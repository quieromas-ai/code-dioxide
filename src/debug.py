"""Remote debugging read side: task log sources, the paged logs, the debug view.

Sources of a task: ``engine`` (its ``events`` rows as JSON lines), ``server`` (``server.log``),
``attempt-<n>`` / ``stderr-<n>``, ``journal`` and ``step:<label>`` (live under ``claude_home``
while running, the run-store copy afterwards), ``script`` and ``inputs`` (snapshot files
concatenated, a header line per file). Without a source: the merged timeline of the timestamped
sources (engine, attempt streams, this task's server lines). Everything served is redacted.
"""

import asyncio
import dataclasses
import io
import json
import os
import re
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from src import journal
from src.config import ENV_PREFIX
from src.engine import TaskEngine
from src.engine_lifecycle import served_step_status
from src.engine_read import iso_or_none
from src.launcher import call_matches, expected_call
from src.logpager import (
    BadCursor,
    Cursor,
    CursorTooLarge,
    Page,
    Source,
    decode_cursor,
    encode_cursor,
    iso,
    load_key,
)
from src.logpager import read_page as _read_page
from src.redact import mask_leaves
from src.runlocator import (
    RunLocator,
    RunRef,
    TaskView,
    event_json,
    load_view,
    stream_events,
)
from src.stream import WorkflowCall
from src.tasks import TERMINAL

VERSION_TIMEOUT_S = 10.0
_ATTEMPT = re.compile(r"(attempt|stderr)-([1-9][0-9]*)")
_STEP_PREFIX = "step:"
VERSION_RETRY_S = 60.0  # a failed `--version` is retried after this long
_versions: dict[str, tuple[str, float | None]] = {}  # bin -> (text, failed at)
_version_lock = threading.Lock()
PRUNED = "pruned"  # the ``run_store`` marker of a task whose run store retention deleted
_CREDENTIAL_BLOCKS = frozenset({"env", "headers"})  # MCP server blocks with arbitrary key names


def not_found(task_id: str) -> dict[str, Any]:
    return {"task_id": task_id, "status": "not_found", "error": f"task {task_id} not found"}


def pruned(engine: TaskEngine, view: TaskView) -> bool:
    """Retention deleted the run store: a terminal task whose run directory is gone."""
    return view.task.status in TERMINAL and not engine.runstore.task_dir(view.task.id).is_dir()


def bundle_url(task_id: str) -> str:
    return f"/api/v1/tasks/{task_id}/bundle.tar.gz"


# -- raw line helpers -------------------------------------------------------------------------
def _obj(raw: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _epoch(value: Any) -> float | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).timestamp()
    return None


def _stamp_key(key: str) -> Any:
    def stamp(raw: bytes) -> float | None:
        obj = _obj(raw)
        return None if obj is None else _epoch(obj.get(key))

    return stamp


def _keep_task(task_id: str) -> Any:
    def keep(raw: bytes) -> bool:
        obj = _obj(raw)
        return obj is not None and obj.get("task_id") == task_id

    return keep


def _file(path: Path) -> Any:
    def opener() -> IO[bytes] | None:
        try:
            return path.open("rb")
        except OSError:
            return None

    return opener


def _blob(data: bytes) -> Any:
    return lambda: io.BytesIO(data)


def _concat(root: Path, files: list[Path]) -> bytes:
    """Snapshot files as one stream: a ``==> <path> <==`` header line before each file."""
    out = bytearray()
    for path in files:
        try:
            data = path.read_bytes()
        except OSError:
            continue
        out += f"==> {path.relative_to(root)} <==\n".encode()
        out += data if data.endswith(b"\n") or not data else data + b"\n"
    return bytes(out)


def _snapshot_files(root: Path) -> list[Path]:
    """Files under ``root``; ``answers/<k>.md`` last in k order, so new answers append."""
    files = [Path(b, n) for b, _d, names in os.walk(root) for n in names]

    def order(path: Path) -> tuple[int, int, str]:
        rel = path.relative_to(root)
        if rel.parts[0] == "answers" and rel.stem.isdigit():
            return 1, int(rel.stem), str(rel)
        return 0, 0, str(rel)

    return sorted(files, key=order)


# -- task context ---------------------------------------------------------------------------
def available_sources(engine: TaskEngine, view: TaskView, run: RunLocator) -> list[str]:
    names = ["engine", "server"]
    for attempt in view.attempts:
        names += [f"attempt-{attempt['n']}", f"stderr-{attempt['n']}"]
    if run.journal_path() is not None:
        names.append("journal")
    names += [f"{_STEP_PREFIX}{step.label}" for step in run.steps()]
    paths = engine.runstore.paths(view.task.id)
    names += [
        n for n, d in (("script", paths.script_dir), ("inputs", paths.inputs_dir)) if d.is_dir()
    ]
    return names


def _source(engine: TaskEngine, view: TaskView, run: RunLocator, name: str) -> Source | None:
    task_id = view.task.id
    paths = engine.runstore.paths(task_id)
    if name == "engine":
        lines = "".join(json.dumps(event_json(e), ensure_ascii=False) + "\n" for e in view.events)
        return Source(name, _blob(lines.encode()), _stamp_key("at"))
    if name == "server":
        return server_source(engine)  # still written: never final
    final = view.final  # the task's own files are complete
    if match := _ATTEMPT.fullmatch(name):
        stream, stderr = engine.runstore.attempt_paths(task_id, int(match.group(2)))
        if match.group(1) == "attempt":
            return Source(name, _file(stream), _stamp_key("timestamp"), final=final)
        return Source(name, _file(stderr), final=final)
    if name == "journal":
        path = run.journal_path()
        return Source(name, _file(path) if path else _blob(b""), final=final)
    if name.startswith(_STEP_PREFIX) and len(name) > len(_STEP_PREFIX):
        path = run.step_path(name[len(_STEP_PREFIX) :])
        return Source(name, _file(path), _stamp_key("timestamp"), final=final)
    if name in ("script", "inputs"):
        root = paths.script_dir if name == "script" else paths.inputs_dir
        files = _snapshot_files(root) if root.is_dir() else []
        return Source(name, _blob(_concat(root, files)))
    return None


def server_source(engine: TaskEngine) -> Source:
    """The live ``server.log`` only (rotated backups are in the bundle); a rotation restarts the
    source at 0 through the inode in the cursor."""
    path = engine.settings.server_log_path
    return Source("server", _file(path), _stamp_key("ts"), rotating=True)


def _key(engine: TaskEngine) -> bytes:
    return load_key(engine.settings.cursor_key_path)


def _scope(task_id: str | None) -> str:
    return "server" if task_id is None else f"task:{task_id}"


def _result(
    engine: TaskEngine,
    page: Page,
    done: bool,
    task_id: str | None,
    sources: list[str] | None = None,
) -> dict[str, Any]:
    try:
        next_cursor = encode_cursor(page.cursor, _key(engine), _scope(task_id))
    except CursorTooLarge:
        return {"error": "cursor too large: page one source at a time"}
    out: dict[str, Any] = {"entries": page.entries, "next_cursor": next_cursor, "done": done}
    if sources is not None:
        out["available_sources"] = sources
    return out


def _caps(engine: TaskEngine, full: bool) -> int:
    s = engine.settings
    return s.log_full_page_bytes if full else s.log_page_chars


def _known(view: TaskView, run: RunLocator, source: str) -> bool:
    """Attempt / stderr / step sources must exist; the others may be empty."""
    if match := _ATTEMPT.fullmatch(source):
        return any(a["n"] == int(match.group(2)) for a in view.attempts)
    if source.startswith(_STEP_PREFIX):
        return any(s.label == source[len(_STEP_PREFIX) :] for s in run.steps())
    return True


def _task_page(
    engine: TaskEngine,
    view: TaskView,
    run: RunLocator,
    source: str | None,
    cursor: Cursor,
    limit: int,
    full: bool,
    with_sources: bool = True,
) -> dict[str, Any]:
    names: list[str] | None = None
    if with_sources:
        names = engine.redactor.redact_json(available_sources(engine, view, run))
    if source is None:
        merged = [
            _source(engine, view, run, "engine"),
            *(_source(engine, view, run, f"attempt-{a['n']}") for a in view.attempts),
            *(_source(engine, view, run, f"{_STEP_PREFIX}{s.label}") for s in run.steps()),
        ]
        merged.append(dataclasses.replace(server_source(engine), keep=_keep_task(view.task.id)))
        sources = [s for s in merged if s is not None]
    else:
        single = _source(engine, view, run, source) if _known(view, run, source) else None
        if single is None:
            error: dict[str, Any] = {"error": f"unknown source {source!r}"}
            if names is not None:
                error["available_sources"] = names
            return error
        sources = [single]
    page = _read_page(
        sources, cursor, engine.redactor, cap=_caps(engine, full), limit=limit, full=full,
        merged=source is None,
    )  # fmt: skip
    return _result(engine, page, page.exhausted and view.final, view.task.id, names)


async def task_logs(
    engine: TaskEngine,
    task_id: str,
    source: str | None = None,
    cursor: str | None = None,
    limit: int = 200,
    full: bool = False,
) -> dict[str, Any]:
    try:
        position = decode_cursor(cursor, _key(engine), _scope(task_id))
    except BadCursor:
        return {"error": "bad cursor"}
    view = await load_view(task_id)
    if view is None:
        return not_found(task_id)

    def page() -> dict[str, Any]:
        run = RunLocator(engine, view)
        result = _task_page(engine, view, run, source, position, limit, full)
        if pruned(engine, view):
            result["run_store"] = PRUNED
        return result

    return await asyncio.to_thread(page)


class LogFollower:
    """Pages one task's logs repeatedly (SSE): the workflow run is resolved once per attempt
    count and reused, and ``available_sources`` is not computed."""

    def __init__(self, engine: TaskEngine, task_id: str) -> None:
        self.engine, self.task_id = engine, task_id
        self._ref: tuple[int, RunRef | None] | None = None

    async def page(self, source: str | None, cursor: str | None, limit: int) -> dict[str, Any]:
        engine = self.engine
        try:
            position = decode_cursor(cursor, _key(engine), _scope(self.task_id))
        except BadCursor:
            return {"error": "bad cursor"}
        view = await load_view(self.task_id)
        if view is None:
            return not_found(self.task_id)

        def page() -> dict[str, Any]:
            cached = self._ref
            if cached is not None and cached[0] == len(view.attempts) and cached[1] is not None:
                run = RunLocator(engine, view, cached[1])
            else:
                run = RunLocator(engine, view)
                self._ref = (len(view.attempts), run.ref)
            return _task_page(engine, view, run, source, position, limit, False, False)

        return await asyncio.to_thread(page)


async def server_logs(
    engine: TaskEngine, cursor: str | None = None, limit: int = 200, full: bool = False
) -> dict[str, Any]:
    try:
        position = decode_cursor(cursor, _key(engine), _scope(None))
    except BadCursor:
        return {"error": "bad cursor"}

    def page() -> dict[str, Any]:
        result = _read_page(
            [server_source(engine)], position, engine.redactor, cap=_caps(engine, full),
            limit=limit, full=full,
        )  # fmt: skip
        return _result(engine, result, result.exhausted, None)

    return await asyncio.to_thread(page)


# -- debug view -----------------------------------------------------------------------------
def _version_of(claude_bin: str) -> str:
    try:
        proc = subprocess.run(
            [claude_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=VERSION_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
            check=False,
            env={k: v for k, v in os.environ.items() if not k.startswith(ENV_PREFIX)},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"error: {type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()[-200:]
        return f"error: exit {proc.returncode}: {detail}"
    return proc.stdout.strip()


def _cached_version(claude_bin: str) -> str:
    with _version_lock:
        cached = _versions.get(claude_bin)
        failed_at = cached[1] if cached else None
        if cached is None or (
            failed_at is not None and time.monotonic() - failed_at >= VERSION_RETRY_S
        ):
            text = _version_of(claude_bin)
            failed = text.startswith("error: ")
            cached = (text, time.monotonic() if failed else None)
            _versions[claude_bin] = cached
        return cached[0]


async def claude_version(claude_bin: str) -> str:
    """``<claude_bin> --version``, run once per binary for the process lifetime (a failure is
    retried after ``VERSION_RETRY_S``)."""
    return await asyncio.to_thread(_cached_version, claude_bin)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _attempt_agents(engine: TaskEngine, view: TaskView) -> Path:
    """What the latest attempt really ran with; the shared file for an attempt that predates the
    per-attempt files (or none yet)."""
    if view.attempts:
        own = engine.runstore.attempt_agents_path(view.task.id, view.attempts[-1]["n"])
        if own.is_file():
            return own
    return engine.settings.data_dir / "agents.json"


def _mask_credential_blocks(value: Any) -> Any:
    """Every string leaf under an ``env`` or ``headers`` object masked, at any depth: MCP
    configs, settings and agents' inline ``mcpServers`` hold secrets there under arbitrary
    names."""
    if isinstance(value, dict):
        return {
            k: mask_leaves(v)
            if isinstance(k, str) and k.lower() in _CREDENTIAL_BLOCKS and isinstance(v, dict)
            else _mask_credential_blocks(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_mask_credential_blocks(item) for item in value]
    return value


def _first_call(engine: TaskEngine, view: TaskView) -> dict[str, Any] | None:
    if not view.attempts:
        return None
    stream, _ = engine.runstore.attempt_paths(view.task.id, view.attempts[-1]["n"])
    calls = [e for e in stream_events(stream) if isinstance(e, WorkflowCall)]
    return calls[0].input if calls else None


def _call(engine: TaskEngine, view: TaskView) -> dict[str, Any]:
    task = view.task
    if not task.is_lifecycle:
        return {"expected": None, "passed": None, "mismatch": None}
    mode = task.resume_mode if task.resume_mode in ("first", "resume", "fresh") else "first"
    try:
        expected: dict[str, Any] | None = expected_call(task, mode)  # type: ignore[arg-type]
    except ValueError:
        expected = None
    passed = _first_call(engine, view)
    mismatch = None if expected is None or passed is None else not call_matches(expected, passed)
    return {"expected": expected, "passed": passed, "mismatch": mismatch}


def _cached(row: dict[str, Any] | None, step: Any, progress: list[dict[str, Any]]) -> bool:
    """The step mirror's value (it knows earlier attempts' counts); the run file otherwise."""
    return bool(row["cached"]) if row is not None else journal.cached_label(0, step, progress)


def _git(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None or (row["git_branch"] is None and row["git_head"] is None):
        return None
    return {"branch": row["git_branch"], "head": row["git_head"]}


def _history(engine: TaskEngine, task_id: str) -> list[dict[str, Any]]:
    """Runs a fresh start replaced (``history/<run>/``): file names only, contents stay in the
    bundle."""
    root = engine.runstore.task_dir(task_id) / "history"
    if not root.is_dir():
        return []
    return [
        {
            "run": run.name,
            "entries": sorted(p.relative_to(run).as_posix() for p in run.rglob("*") if p.is_file()),
        }
        for run in sorted(root.iterdir())
        if run.is_dir()
    ]


def _debug(engine: TaskEngine, view: TaskView, version: str) -> dict[str, Any]:
    task = view.task
    run = RunLocator(engine, view)
    progress = run.progress()
    events = [event_json(e) for e in view.events]
    spawns = [e for e in events if e["type"] == "spawned"]
    mirrored = {s["label"]: s for s in view.steps}
    data = engine.settings.data_dir
    return {
        "task_id": task.id,
        "status": task.status,
        "kind": task.kind,
        "agent": task.agent,
        "session_id": task.session_id,
        "run_id": run.ref.run_id if run.ref else None,
        "attempts": [
            {
                "n": a["n"],
                "argv": json.loads(a["argv_json"]) if a["argv_json"] else None,
                "started": iso_or_none(a["started_at"]),
                "ended": iso_or_none(a["ended_at"]),
                "exit_code": a["exit_code"],
                "outcome": a["outcome"],
                "reason": a["reason"],
                "detail": a["detail"],
                "cost_usd": a["cost_usd"],
            }
            for a in view.attempts
        ],
        "steps": [
            {
                "label": s.label,
                "phase": s.phase,
                "status": served_step_status(s.status, task.status),
                "cached": _cached(mirrored.get(s.label), s, progress),
                "started_count": s.started_count,
                "agent_id": s.agent_id,
                "git": _git(mirrored.get(s.label)),
                "timing": journal.step_timing(
                    journal.step_source(
                        task.cwd or ".",
                        task.session_id,
                        run.ref.run_id if run.ref else None,
                        engine.settings.claude_home,
                        s.agent_id,
                        engine.runstore.step_copy_path(task.id, s.label),
                        run.ref.transcript_dir if run.ref else None,
                    )
                ),
            }
            for s in run.steps()
        ],  # fmt: skip
        "call": _call(engine, view),
        "launch": {
            "path_env": spawns[-1]["data"].get("path_env") if spawns else None,
            # Names only: the child env file's values are never recorded.
            "child_env": spawns[-1]["data"].get("child_env") if spawns else None,
            "claude_version": version,
            "agents_json": _mask_credential_blocks(_read_json(_attempt_agents(engine, view))),
            "mcp_config": _mask_credential_blocks(_read_json(data / "mcp.json")),
            "settings": _mask_credential_blocks(_read_json(data / "settings.json")),
        },
        "timeline": [{**e, "at": iso(e["at"])} for e in events],
        "bundle_url": bundle_url(task.id),
        "history": _history(engine, task.id),
        **({"run_store": PRUNED} if pruned(engine, view) else {}),
    }


async def task_debug(engine: TaskEngine, task_id: str) -> dict[str, Any]:
    view = await load_view(task_id)
    if view is None:
        return not_found(task_id)
    version = await claude_version(engine.settings.claude_bin)
    debug = await asyncio.to_thread(_debug, engine, view, version)
    redacted: dict[str, Any] = engine.redactor.redact_json(debug)
    return redacted
