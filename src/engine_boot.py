"""Engine start: what a previous engine left behind, rebuilt from the database rows, the stream
file, the run file and the process table (nothing in memory survives a restart).

* A ``running`` task's open attempt is rebuilt from its ``attempts`` row (argv, stream, start
  time) and the task row (pid, pid start, session, run). Its process still the one started
  (pid + start time) -> re-attached: watched like a launched attempt, the stream read again from
  offset 0. Otherwise (dead, pid reused, no pid, no start time) the process is never signalled
  and the attempt ends at once through the same waiter: the whole stream and the run file are
  classified (a finished workflow or a result decides; nothing -> ``transient``), the decider
  picks what follows, a clean exit keeps its leftovers and an unclean end is swept.
* An open attempt row of any other task (terminal, ``needs_human``, ``queued``) is closed (an
  unclean end) and its leftovers are killed; its status stays as it is.
* ``queued`` tasks are not touched: the scheduler keeps honouring ``next_attempt_at``.

Runs before the scheduler starts. Idempotent: every change is a transaction on rows that are
still open, so a crash during boot followed by another boot gives the same result.
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from src import engine_recovery, engine_sweep, procs
from src.database import get_db, write_tx
from src.engine_watch import Attempt
from src.launcher import Mode, call_in_prompt
from src.serverlog import LOGGER_NAME
from src.tasks import Task

if TYPE_CHECKING:
    from src.engine import TaskEngine

BOOT_TRIGGER = "boot"
_MODES = ("first", "resume", "fresh")
_log = logging.getLogger(LOGGER_NAME)


_STATES = {"same": "alive", "dead": "dead", "other": "pid_reused"}
LIVENESS_TRIES = 5
LIVENESS_RETRY_S = 0.5
# Total time one task's liveness may take. A ps can hang for procs.PS_TIMEOUT_S (10 s), so a
# retry starts only while at least that much budget is left: the bound holds.
LIVENESS_BUDGET_S = 15.0
LIVENESS_CONCURRENCY = 8  # running tasks checked at once


def process_state(
    pid: int | None,
    start: float | None,
    clock: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
) -> str:
    """``alive`` (still the process started at ``start``), ``pid_reused``, ``dead``, ``no_pid``,
    ``no_start_time`` (identity can never be checked: never re-attached, never signalled), or
    ``unknown``: the process table stayed unreadable over ``LIVENESS_TRIES`` reads. ``unknown`` is
    handled as alive (re-attached), never as gone: a live claude must not be resumed beside
    itself. Blocking (``ps``, pauses): call off the event loop. ``clock`` / ``pause``: test
    seams for the budget."""
    if not pid:
        return "no_pid"
    if start is None:
        return "no_start_time"
    deadline = clock() + LIVENESS_BUDGET_S
    for n in range(LIVENESS_TRIES):
        if n:
            if deadline - (clock() + LIVENESS_RETRY_S) < procs.PS_TIMEOUT_S:
                break  # no room for one more ps: unknown (re-attached), boot goes on
            pause(LIVENESS_RETRY_S)
        state = procs.liveness(pid, start)
        if state != "unknown":
            return _STATES[state]
    return "unknown"


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


async def boot(engine: "TaskEngine") -> None:
    db = get_db()
    async with db.execute(
        "SELECT a.task_id, a.n FROM attempts a JOIN tasks t ON t.id = a.task_id "
        "WHERE a.ended_at IS NULL AND NOT (t.status = 'running' AND a.n = t.attempt) "
        "ORDER BY a.task_id, a.n"
    ) as cur:
        stray = [(str(r["task_id"]), int(r["n"])) for r in await cur.fetchall()]
    async with db.execute("SELECT * FROM tasks WHERE status = 'running' ORDER BY id") as cur:
        running = [Task.from_row(r) for r in await cur.fetchall()]
    for task_id, n in stray:
        try:
            await _close_stray(engine, task_id, n)
        except Exception:
            _log.exception("could not close an open attempt at boot", extra={"task_id": task_id})
    states = await _states(running)
    for task in running:
        try:
            await _recover(engine, task, states[task.id])
        except Exception as exc:  # never leave a running task without a job or an outcome
            _log.exception("could not recover a running task at boot", extra={"task_id": task.id})
            if task.id not in engine._live:
                await _end_unrecoverable(engine, task, f"{type(exc).__name__}: {exc}")


async def _states(running: list[Task]) -> dict[str, str]:
    """Every running task's process state, checked concurrently (bounded): one slow or hanging
    ``ps`` costs boot at most ``LIVENESS_BUDGET_S`` overall per batch, not per task in a row."""
    gate = asyncio.Semaphore(LIVENESS_CONCURRENCY)

    async def one(task: Task) -> str:
        async with gate:
            return await asyncio.to_thread(process_state, task.pid, task.pid_start)

    found = await asyncio.gather(*(one(t) for t in running), return_exceptions=True)
    # A check that raised is unknown: re-attached, never classified as gone.
    return {
        t.id: s if isinstance(s, str) else "unknown" for t, s in zip(running, found, strict=True)
    }


def _paths(engine: "TaskEngine", row: Any) -> tuple[Path, Path]:
    """The attempt's stream and stderr files (the run store's names when the row has none)."""
    stream, stderr = engine.runstore.attempt_paths(str(row["task_id"]), int(row["n"]))
    return (
        Path(row["stream_path"]) if row["stream_path"] else stream,
        Path(row["stderr_path"]) if row["stderr_path"] else stderr,
    )


async def _close_stray(engine: "TaskEngine", task_id: str, n: int) -> None:
    """An open attempt of a task that is no longer running it (e.g. cancelled while the engine
    went down): closed as an unclean end, its process killed if it is still the one started,
    the task's leftovers swept. The task's status is not changed."""
    async with get_db().execute(
        "SELECT * FROM attempts WHERE task_id = ? AND n = ?", (task_id, n)
    ) as cur:
        row = await cur.fetchone()
    if row is None or row["ended_at"] is not None:
        return
    last = await asyncio.to_thread(_mtime, _paths(engine, row)[0])  # never under the lock
    started = float(row["started_at"])
    now = time.time()
    async with write_tx() as db:
        task = Task.from_row(await engine._fetch(db, task_id))
        outcome = "cancelled" if task.status == "cancelled" else "transient"
        reason = task.kill_reason or BOOT_TRIGGER
        cur = await db.execute(
            "UPDATE attempts SET ended_at = ?, outcome = ?, reason = ?, running_s = ? "
            "WHERE task_id = ? AND n = ? AND ended_at IS NULL",
            (now, outcome, reason, max(0.0, min(now, last or started) - started), task_id, n),
        )
        if not cur.rowcount:
            return
        data = {"n": n, "status": task.status, "outcome": outcome, "reason": reason}
        await engine._event(db, task_id, "attempt_closed_at_boot", data)
    if task.status == "running" and n != task.attempt:
        return  # an older row of a running task: the current attempt's own end sweeps
    own = engine_sweep.own_claude(task.pid, task.pid_start) if n == task.attempt else {}
    if own:
        pid, start = next(iter(own.items()))
        await engine._kill(task_id, pid, start, BOOT_TRIGGER)  # checks pid + start first
    else:
        await engine._sweep_leftovers(task_id, BOOT_TRIGGER, own)


async def _attempt_row(task: Task) -> Any:
    async with get_db().execute(
        "SELECT * FROM attempts WHERE task_id = ? AND n = ?", (task.id, task.attempt)
    ) as cur:
        return await cur.fetchone()


def _rebuild(engine: "TaskEngine", task: Task, row: Any, state: str, last: float | None) -> Attempt:
    argv = json.loads(row["argv_json"]) if row["argv_json"] else []
    argv = [str(a) for a in argv] if isinstance(argv, list) else []
    prompt = argv[argv.index("-p") + 1] if "-p" in argv[:-1] else ""
    mode = cast(Mode, task.resume_mode if task.resume_mode in _MODES else "first")
    started = float(row["started_at"])
    gone = state not in ("alive", "unknown")
    stream, stderr = _paths(engine, row)
    return Attempt(
        task=task, mode=mode, n=task.attempt, argv=argv, stream_path=stream,
        stderr_path=stderr, started_at=started, pid=task.pid or 0,
        start=task.pid_start, call=call_in_prompt(prompt) if task.is_lifecycle else None,
        boot=True, gone=gone, gone_at=max(started, last or started) if gone else None,
    )  # fmt: skip


async def _recover(engine: "TaskEngine", task: Task, state: str) -> None:
    row = await _attempt_row(task)
    if row is None or row["ended_at"] is not None:  # no open attempt: not a state we write
        await _end_unrecoverable(engine, task, "running task without an open attempt")
        return
    last = await asyncio.to_thread(_mtime, _paths(engine, row)[0])
    attempt = _rebuild(engine, task, row, state, last)
    action = "reattach" if not attempt.gone else "classify"
    if state == "unknown":
        _log.warning("process table unreadable at boot: re-attached", extra={"task_id": task.id})
    await engine._note(
        task.id, "boot_recovered",
        {"n": attempt.n, "pid": task.pid, "process": state, "action": action},
    )  # fmt: skip
    engine._adopt(attempt)
    if not attempt.gone and task.kill_reason is not None:
        # A kill requested before the restart and not finished: requested again (no new event).
        await engine._kill_for(attempt, task.kill_reason)


async def _end_unrecoverable(engine: "TaskEngine", task: Task, why: str) -> None:
    """A ``running`` task boot cannot rebuild (no open attempt, an unreadable row): an engine
    error, i.e. a ``transient`` end through the decider; its process (if still the one started)
    is killed and its leftovers swept. The task never stays ``running`` without a job."""
    now = time.time()
    attempt = Attempt(task, "first", task.attempt, [], Path(), Path(), now, gone=True, boot=True)
    error = f"engine error at engine start: {why}"
    if task.pid and task.pid_start is not None:
        await engine._kill(task.id, task.pid, task.pid_start, BOOT_TRIGGER)  # pid + start checked
    try:
        await engine_recovery.close_with_decision(engine, attempt, error)
    except Exception:
        _log.exception("could not end a running task at boot", extra={"task_id": task.id})
    own = engine_sweep.own_claude(task.pid, task.pid_start)
    await engine._sweep_leftovers(task.id, BOOT_TRIGGER, own)
