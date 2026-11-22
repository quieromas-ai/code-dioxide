"""Read side: task status, keyset-paged list, busy agents, task counts."""

import asyncio
import base64
import binascii
import json
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from src import engine_lifecycle, recovery, stream
from src.database import get_db
from src.logpager import iso
from src.tasks import Task

if TYPE_CHECKING:
    from src.engine import TaskEngine

MAX_LIST_LIMIT = 100


def iso_or_none(ts: float | None) -> str | None:
    return None if ts is None else iso(ts)


def _encode_cursor(created_at: float, task_id: str) -> str:
    return base64.urlsafe_b64encode(json.dumps([created_at, task_id]).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[float, str]:
    try:
        created_at, task_id = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except (ValueError, TypeError, binascii.Error) as exc:
        raise ValueError("invalid cursor") from exc
    if not isinstance(created_at, int | float) or not isinstance(task_id, str):
        raise ValueError("invalid cursor")
    return float(created_at), task_id


async def status(engine: "TaskEngine", task_id: str) -> dict[str, Any]:
    db = get_db()
    row = await engine._fetch(db, task_id)
    if row is None:
        return {"task_id": task_id, "status": "not_found", "error": f"task {task_id} not found"}
    task = Task.from_row(row)
    cost, tokens, partial = await _usage(engine, task)
    steps = (
        await engine_lifecycle.step_list(
            task_id,
            task.status,
            engine_lifecycle.step_source_of(engine.runstore, task, engine.settings.claude_home),
        )
        if task.is_lifecycle
        else []
    )
    waiting = await _waiting(engine, task)
    ended_wait = await _ended_during_rate_limit_wait(task)
    return {
        "task_id": task.id,
        "status": task.status,
        "output": json.loads(task.output_json) if task.output_json else None,
        "error": task.error,
        "started_at": iso_or_none(task.started_at),
        "completed_at": iso_or_none(task.completed_at),
        "stage": engine_lifecycle.current_stage(task, steps) if task.is_lifecycle else None,
        "steps": steps,
        "running_steps": [s["label"] for s in steps if s["status"] == "running"],
        "resumes": task.resumes,
        "session_id": task.session_id,
        "run_id": task.run_id,
        "question": json.loads(task.question_json) if task.question_json else None,
        "cost_usd": cost,
        "cost_partial": partial,
        "tokens_used": tokens,
        "waiting": waiting,
        "kill_reason": task.kill_reason,
        "recovery": _recovery(engine, task, waiting, ended_wait=ended_wait),
    }


async def _usage(engine: "TaskEngine", task: Task) -> tuple[float, int | None, bool]:
    """Cost and tokens of a task so far: its finished attempts (written at attempt end) plus, for
    an attempt still running, the last interim ``result`` record of its stream. ``partial`` is true
    while the task runs an attempt: the figures can still grow. An open attempt row on a task that
    is not running (a leftover) adds nothing."""
    task_id = task.id
    db = get_db()
    async with db.execute(
        "SELECT SUM(cost_usd) AS cost, SUM(tokens) AS tokens FROM attempts WHERE task_id = ?",
        (task_id,),
    ) as cur:
        sums = await cur.fetchone()
    async with db.execute(
        "SELECT MAX(n) AS n FROM attempts WHERE task_id = ? AND ended_at IS NULL", (task_id,)
    ) as cur:
        open_attempt = (await cur.fetchone() or {"n": None})["n"]
    cost = float(sums["cost"] or 0.0) if sums else 0.0
    tokens = sums["tokens"] if sums else None
    if open_attempt is None or task.status != "running":
        return cost, tokens, False
    path = engine.runstore.attempt_paths(task_id, open_attempt)[0]
    interim = await asyncio.to_thread(stream.last_interim_result, path)
    if interim is not None:
        cost += interim.cost_usd or 0.0
        tokens = (tokens or 0) + interim.tokens
    return cost, tokens, True


def _budget(used: float, limit: float) -> dict[str, float]:
    return {"used": used, "left": max(0, limit - used)}


def _rate_limit_wait(
    engine: "TaskEngine", task: Task, waiting: dict[str, Any] | None, ended_wait: bool
) -> dict[str, float]:
    """The rate-limit wait budget. The decider charges a wait up front, so while the task sits
    queued for it the charged seconds split into ``used`` (waited so far) and ``scheduled``. A
    task that ended during such a wait (``ended_wait``: cancelled, say) never spends the rest:
    it stays ``scheduled``, counted against ``completed_at`` instead of now."""
    limit = engine.settings.rate_limit_max_wait_s
    charged = task.rate_limit_waited_s
    at = task.next_attempt_at
    scheduled = 0.0
    if waiting is not None and waiting["why"] == "rate_limit" and at is not None:
        scheduled = min(max(0.0, at - engine._clock()), charged)
    elif ended_wait and at is not None and task.completed_at is not None:
        scheduled = min(max(0.0, at - task.completed_at), charged)
    return {"used": charged - scheduled, "scheduled": scheduled, "left": max(0, limit - charged)}


def _recovery(
    engine: "TaskEngine", task: Task, waiting: dict[str, Any] | None, *, ended_wait: bool = False
) -> dict[str, Any]:
    """The recovery counters, each with what is left of its budget (``recovery.next_action``)."""
    settings = engine.settings
    return {
        "resumes": _budget(task.resumes, settings.max_resumes + task.max_retries),
        "call_retries": _budget(task.call_retries, settings.call_mismatch_retries),
        "stage_retries": _budget(task.stage_retries, recovery.STAGE_FAILED_RESUMES),
        "rate_limit_waited_s": _rate_limit_wait(engine, task, waiting, ended_wait),
    }


async def _ended_during_rate_limit_wait(task: Task) -> bool:
    """Whether a task that is no longer queued or running last scheduled a rate-limit wait (its
    ``next_attempt_at`` is kept on cancel), so the wait may have been cut short by its end."""
    if (
        task.status in ("queued", "running")
        or task.completed_at is None
        or task.next_attempt_at is None
        or not task.rate_limit_waited_s
    ):
        return False
    async with get_db().execute(
        "SELECT data_json FROM events WHERE task_id = ? AND type = 'retry_scheduled' "
        "ORDER BY id DESC LIMIT 1",
        (task.id,),
    ) as cur:
        row = await cur.fetchone()
    return row is not None and json.loads(row["data_json"]).get("why") == recovery.RATE_LIMITED


async def _waiting(engine: "TaskEngine", task: Task) -> dict[str, Any] | None:
    """A task queued for a later time: until when, and why (from the event that scheduled it:
    a decider's ``retry_scheduled`` or a pre-launch sweep's ``sweep_requeued``)."""
    at = task.next_attempt_at
    if task.status != "queued" or at is None or at <= engine._clock():
        return None
    async with get_db().execute(
        "SELECT type, data_json FROM events WHERE task_id = ? "
        "AND type IN ('retry_scheduled', 'sweep_requeued') ORDER BY id DESC LIMIT 1",
        (task.id,),
    ) as cur:
        row = await cur.fetchone()
    data = json.loads(row["data_json"]) if row is not None else {}
    if row is not None and row["type"] == "sweep_requeued":
        why = "sweep_unconfirmed" if "streak" in data else "sweep"
    else:
        why = "rate_limit" if data.get("why") == recovery.RATE_LIMITED else "backoff"
    return {"until": iso(at), "why": why}


async def list_tasks(
    engine: "TaskEngine", status: str | None, agent: str | None, limit: int, cursor: str | None
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if agent:
        with suppress(KeyError):
            agent = engine.resolve(agent)
        clauses.append("agent = ?")
        params.append(agent)
    if cursor:
        try:
            created_at, last_id = _decode_cursor(cursor)
        except ValueError as exc:
            return {"error": str(exc)}
        clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
        params += [created_at, created_at, last_id]
    limit = max(1, min(limit, MAX_LIST_LIMIT))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    async with get_db().execute(
        f"SELECT * FROM tasks{where} ORDER BY created_at DESC, id DESC LIMIT ?",
        [*params, limit + 1],
    ) as cur:
        found = [Task.from_row(r) for r in await cur.fetchall()]
    page = found[:limit]
    costs, steps = await _page_costs_and_steps([t.id for t in page])
    tasks = [
        {
            "task_id": t.id, "agent": t.agent, "kind": t.kind, "status": t.status,
            "description": (t.description or "")[:200], "priority": t.priority,
            "created_at": iso_or_none(t.created_at), "started_at": iso_or_none(t.started_at),
            "completed_at": iso_or_none(t.completed_at), "error": t.error,
            "branch": t.branch, "worktree": t.worktree,
            "stage": _stage(t, steps.get(t.id, [])),
            "cost_usd": costs.get(t.id, 0.0),
        }
        for t in page
    ]  # fmt: skip
    more = len(found) > limit
    next_cursor = _encode_cursor(page[-1].created_at, page[-1].id) if more else None
    return {"tasks": tasks, "count": len(tasks), "next_cursor": next_cursor}


def _stage(task: Task, step_rows: list[Any]) -> str | None:
    if not task.is_lifecycle:
        return None
    return engine_lifecycle.current_stage(task, engine_lifecycle.step_items(step_rows, task.status))


async def _page_costs_and_steps(
    ids: list[str],
) -> tuple[dict[str, float], dict[str, list[Any]]]:
    """Finished-attempt cost and mirrored step rows for a page of tasks: one grouped query each,
    whatever the page size."""
    if not ids:
        return {}, {}
    db = get_db()
    marks = ",".join("?" * len(ids))
    async with db.execute(
        f"SELECT task_id, SUM(cost_usd) AS cost FROM attempts WHERE task_id IN ({marks}) "
        "GROUP BY task_id",
        ids,
    ) as cur:
        costs = {r["task_id"]: float(r["cost"] or 0.0) for r in await cur.fetchall()}
    steps: dict[str, list[Any]] = {}
    async with db.execute(
        f"SELECT * FROM steps WHERE task_id IN ({marks}) ORDER BY started_at, rowid", ids
    ) as cur:
        for r in await cur.fetchall():
            steps.setdefault(r["task_id"], []).append(r)
    return costs, steps


async def busy_agents() -> set[str]:
    async with get_db().execute(
        "SELECT DISTINCT agent FROM tasks WHERE status = 'running' AND agent IS NOT NULL"
    ) as cur:
        return {r["agent"] for r in await cur.fetchall()}


async def task_counts(engine: "TaskEngine") -> dict[str, int]:
    db = get_db()
    return {s: await engine._count(db, s) for s in ("queued", "running", "needs_human")}
