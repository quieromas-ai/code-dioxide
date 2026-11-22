"""Submit: input validation, the new ``tasks`` row, dedupe + insert in one transaction."""

import asyncio
import hashlib
import json
import os
import re
import subprocess
import time
import unicodedata
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite

from src import engine_sweep, engine_watch
from src.database import write_tx
from src.dedupe import find_duplicate, fingerprint
from src.runstore import render_brief
from src.tasks import KIND_LIFECYCLE, KIND_SINGLE, TERMINAL, Task

if TYPE_CHECKING:
    from src.engine import TaskEngine

MIN_PRIORITY, MAX_PRIORITY = 1, 5
MAX_RETRIES_LIMIT = 10


class Rejected(Exception):
    """A submit that is refused with a clean message."""


def _kebab(text: str, limit: int = 40) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug[:limit].strip("-") or "task"


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_numbers(priority: Any, max_retries: Any) -> None:
    if not _is_int(priority) or not MIN_PRIORITY <= priority <= MAX_PRIORITY:
        raise Rejected(f"priority must be an integer {MIN_PRIORITY}..{MAX_PRIORITY}")
    if not _is_int(max_retries) or not 0 <= max_retries <= MAX_RETRIES_LIMIT:
        raise Rejected(f"max_retries must be an integer 0..{MAX_RETRIES_LIMIT}")


async def position(db: aiosqlite.Connection, task_id: str) -> int:
    """Queued tasks the scheduler would start before this one; 0 unless it is queued."""
    async with db.execute(
        "SELECT COUNT(*) AS n FROM tasks t JOIN tasks o ON o.status = 'queued' AND o.id != t.id "
        "WHERE t.id = ? AND t.status = 'queued' AND (o.priority > t.priority OR "
        "(o.priority = t.priority AND (o.created_at < t.created_at OR "
        "(o.created_at = t.created_at AND o.id < t.id))))",
        (task_id,),
    ) as cur:
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


def _current_branch(cwd: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    branch = out.stdout.strip()
    return branch if out.returncode == 0 and branch and branch != "HEAD" else None


async def submit(
    engine: "TaskEngine",
    agent: str,
    description: str,
    input_context: Any,
    priority: int,
    max_retries: int,
    *,
    kind_override: str | None = None,
    dedupe: bool = True,
) -> dict[str, Any]:
    try:
        _check_numbers(priority, max_retries)
        row, context = await new_row(
            engine, agent, description, input_context, kind_override, dedupe
        )
    except Rejected as exc:
        return {"error": str(exc), "status": "rejected"}
    row.update(priority=priority, max_retries=max_retries)
    task_id = row["id"]
    # Inputs first: once the row commits the scheduler may launch the task at any moment.
    pinned = context.get("approved_plan") if row["kind"] == KIND_LIFECYCLE else None
    try:
        engine.runstore.write_inputs(task_id, render_brief(description, context), pinned)
    except Exception as exc:
        engine.runstore.remove(task_id)
        error = f"could not write the task inputs: {type(exc).__name__}: {exc}"
        return {"error": error, "status": "rejected"}
    try:
        result = await _insert(engine, row, dedupe)
    except Exception:  # rolled back: no row (a cancellation keeps the dir; prune removes it)
        engine.runstore.remove(task_id)
        raise
    if result.get("task_id") != task_id:  # dedupe hit or queue full: no row was inserted
        engine.runstore.remove(task_id)
    else:
        engine._wake.set()
    return result


async def _insert(engine: "TaskEngine", row: dict[str, Any], dedupe: bool) -> dict[str, Any]:
    """Dedupe + queue cap + insert in one transaction; the submit result."""
    async with write_tx() as db:
        if dedupe and row["fingerprint"]:
            window = float(engine.settings.dedupe_window_s)
            existing = await find_duplicate(db, row["fingerprint"], window, row["created_at"])
            if existing is not None:
                return {
                    "task_id": existing,
                    "status": await engine._column(db, existing, "status"),
                    "position": await position(db, existing),
                    "deduplicated": True,
                }
        depth = await engine._count(db, "queued")
        if depth >= engine.settings.task_queue_size:
            return {"error": f"queue is full ({depth} queued)", "status": "rejected"}
        cols = ", ".join(row)
        marks = ", ".join("?" * len(row))
        await db.execute(f"INSERT INTO tasks ({cols}) VALUES ({marks})", list(row.values()))
        await engine._event(db, row["id"], "submitted", {"agent": row["agent"]})
        if row["fingerprint"]:
            similar = await engine._similar(db, row["fingerprint"], row["id"])
            if similar is not None:
                await engine._event(db, row["id"], "similar_active_task", {"similar": similar})
        ahead = await position(db, row["id"])
    return {"task_id": row["id"], "status": "queued", "position": ahead, "deduplicated": False}


async def new_row(
    engine: "TaskEngine",
    agent: str,
    description: str,
    ctx: Any,
    kind_override: str | None,
    dedupe: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the request and build the ``tasks`` row (git runs here, outside any tx)."""
    if not isinstance(description, str) or not description.strip():
        raise Rejected("description is required")
    if ctx is not None and not isinstance(ctx, dict):
        raise Rejected("input_context must be a JSON object")
    context: dict[str, Any] = ctx or {}
    try:
        context_json = json.dumps(context, ensure_ascii=False, allow_nan=False)
        (description + context_json).encode("utf-8")
    except (TypeError, ValueError) as exc:  # UnicodeEncodeError: a lone surrogate
        raise Rejected(f"input is not valid JSON text: {exc}") from None
    try:
        slug = engine.resolve(agent)
    except KeyError:
        raise Rejected(f"unknown agent '{agent}'") from None
    roles = engine.roles
    lifecycle_agent = roles.lifecycle_agent if roles is not None else None
    kind = kind_override or (KIND_LIFECYCLE if slug == lifecycle_agent else KIND_SINGLE)
    raw_dir = context.get("project_dir") or engine.settings.default_project_dir
    if not isinstance(raw_dir, str | Path):
        raise Rejected("project_dir must be a path (input_context or default_project_dir)")
    cwd = os.path.realpath(os.path.expanduser(raw_dir))
    if not os.path.isdir(cwd):
        raise Rejected(f"project_dir {cwd} is not a directory")
    base = context.get("base_branch")
    if not isinstance(base, str) or not base:
        base = await asyncio.to_thread(_current_branch, cwd)
    task_id = str(uuid.uuid4())
    branch = worktree = None
    if kind == KIND_LIFECYCLE:
        if base is None:
            raise Rejected(f"project_dir {cwd} is not a git repository on a branch")
        template = roles.branch_template if roles is not None else ""
        branch = template.format(slug=_kebab(description), id8=task_id[:8])
        cwd_hash = hashlib.sha1(cwd.encode()).hexdigest()[:8]
        worktree = str(engine.settings.worktrees_dir / cwd_hash / task_id[:8])
    now = time.time()
    args = {"task_id": task_id, "run_dir": str(engine.runstore.task_dir(task_id))}
    row = {
        "id": task_id,
        "agent": slug,
        "kind": kind,
        "description": description,
        "input_context": context_json,
        "status": "queued",
        "fingerprint": fingerprint(slug, description, context, cwd) if dedupe else None,
        "cwd": cwd,
        "base_branch": base,
        "branch": branch,
        "worktree": worktree,
        "args_json": json.dumps(args),
        "created_at": now,
        "updated_at": now,
    }
    return row, context


async def cancel(engine: "TaskEngine", task_id: str) -> dict[str, Any]:
    """Cancel writes ``cancelled`` directly; a running attempt's kill goes through the one
    kill-per-attempt path (``engine_watch.request_kill``), which the watchdog retries."""
    now = time.time()
    async with write_tx() as db:
        row = await engine._fetch(db, task_id)
        if row is None:
            return {"task_id": task_id, "error": f"task {task_id} not found"}
        task = Task.from_row(row)
        if task.status in TERMINAL:
            error = f"task is {task.status}"
            return {"task_id": task_id, "status": task.status, "error": error}
        await db.execute(
            "UPDATE tasks SET kill_reason = 'cancel', status = 'cancelled', completed_at = ?, "
            "updated_at = ? WHERE id = ?",
            (now, now, task_id),
        )
        await engine._event(db, task_id, "cancelled", {"from": task.status})
        in_flight = task_id in engine._attempts or task_id in engine._launching
        sweep_idle = not in_flight and await engine_sweep.cancel_idle(engine, db, task)
        if not in_flight:
            # No waiter (e.g. after a restart): close the open attempt row here, or it would
            # hold its concurrency slot forever.
            await db.execute(
                "UPDATE attempts SET ended_at = ?, outcome = 'cancelled', reason = 'cancel', "
                "running_s = ? - COALESCE(started_at, ?) "
                "WHERE task_id = ? AND n = ? AND ended_at IS NULL",
                (now, now, now, task_id, task.attempt),
            )
    if task.status == "running" and task.pid is not None:
        # Never block the caller for the kill grace; the waiter still closes the attempt.
        # The same one-kill-per-attempt path as every kill (retried by the watchdog).
        key = (task_id, task.attempt)
        engine_watch.request_kill(engine, key, task.pid, task.pid_start, "cancel")
    elif sweep_idle:  # no attempt in flight and the last one ended unclean
        own = engine_sweep.own_claude(task.pid, task.pid_start)
        engine._background(engine._sweep_leftovers(task_id, "cancel", own))
    engine._wake.set()
    return {"task_id": task_id, "status": "cancelled"}
