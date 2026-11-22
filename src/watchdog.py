"""Watchdog: every ``watchdog_interval_s`` it checks each live attempt (launched or re-attached,
lifecycle or single) and kills a stalled one or one over the task timeout. It only records the
kill reason and kills through ``kill_for`` (one kill per attempt); the attempt's waiter then
decides what follows (``recovery.next_action``: ``stall`` -> resume within the budget,
``timeout`` -> failed), and the end is unclean (swept).

* Timeout: the task's running time = the sum of ``running_s`` of its closed attempts plus the
  current attempt's ``now - started_at``. Time spent waiting between attempts (``needs_human``,
  a queued backoff, a rate-limit wait) is never counted. Wall clock (``engine._clock``).
* Stall: no activity for ``stall_timeout_s``. Activity = the newest mtime of the attempt's
  stream file and, for a lifecycle task, of any file of its session (journal, run file, step
  transcripts: ``journal.last_activity``). A single task: the stream only. At most one
  activity read per attempt is outstanding (a hung file system never takes more threads).
  Also activity: the attempt's descendant processes (never its claude) used at least
  ``CPU_ACTIVITY_S`` of CPU time since the previous pass (a long command inside a stage writes
  no file). ``task_timeout_s`` stays the hard bound.
* An attempt whose run already reported completion with a valid return: still bounded, by the
  same limits and by ``exit_after_completion_s`` after the completion, but killed with
  ``exit_after_completion`` (in ``RETURN_DECIDES``): the return decides, the end is unclean.
* A terminal task's leftover sweep hit an unreadable process table: it is retried after a
  growing delay until a sweep is confirmed (``engine_sweep.retry_sweeps``).
* A kill was given up (the process table was unreadable) and no kill is in flight: it is
  requested again after a growing delay while the attempt is open, also for a task cancelled
  while running (kill reason ``cancel``).
"""

import asyncio
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from src import engine_lifecycle, engine_sweep, engine_watch, journal, recovery, retention
from src.database import get_db
from src.engine_watch import Attempt
from src.serverlog import LOGGER_NAME
from src.tasks import EXIT_AFTER_COMPLETION

if TYPE_CHECKING:
    from src.engine import TaskEngine

ACTIVITY_TIMEOUT_S = 10.0  # a file-system read that hangs longer skips this pass's stall check
CPU_ACTIVITY_S = 1.0  # descendants' CPU time grown by this much since the last pass: activity
_log = logging.getLogger(LOGGER_NAME)


async def run(engine: "TaskEngine") -> None:
    while True:
        await asyncio.sleep(engine.settings.watchdog_interval_s)
        try:
            await check(engine)
        except Exception:
            _log.exception("watchdog pass failed")
        await retention.maybe_prune(engine)  # once a day; logs its own failures
        engine._watchdog_ticks += 1


async def check(engine: "TaskEngine") -> None:
    engine_sweep.retry_sweeps(engine)  # terminal tasks whose leftover sweep was unconfirmed
    for attempt in list(engine._attempts.values()):
        if attempt.pid and not attempt.gone:
            await _check_attempt(engine, attempt)


def last_activity(engine: "TaskEngine", attempt: Attempt) -> float | None:
    """Newest sign of work (mtime); None when nothing can be read. Blocking: off the loop."""
    times: list[float] = []
    with suppress(OSError):
        times.append(attempt.stream_path.stat().st_mtime)
    task = attempt.task
    if task.is_lifecycle and task.session_id:
        found = journal.last_activity(task.cwd or ".", task.session_id, engine.settings.claude_home)
        if found is not None:
            times.append(found)
    return max(times) if times else None


def _read_done(
    engine: "TaskEngine", key: tuple[str, int], done: "asyncio.Future[float | None]"
) -> None:
    if engine.book.activity_reads.get(key) is done:
        del engine.book.activity_reads[key]
    if not done.cancelled() and done.exception() is not None:
        _log.error("activity read failed", exc_info=done.exception())


async def _check_attempt(engine: "TaskEngine", attempt: Attempt) -> None:
    task_id, n = attempt.task.id, attempt.n
    key = (task_id, n)
    db = get_db()
    async with db.execute(
        "SELECT status, attempt, kill_reason FROM tasks WHERE id = ?", (task_id,)
    ) as cur:
        row = await cur.fetchone()
    if row is None or row["attempt"] != n:
        return  # not this attempt's to judge any more
    now = engine._clock()
    if row["status"] == "cancelled" and row["kill_reason"] == recovery.CANCEL:
        # Cancelled while running: its attempt stays open until the process is gone; a cancel
        # kill that was given up is retried like any other.
        await _rerequest_kill(engine, attempt, recovery.CANCEL, now)
        return
    if row["status"] != "running":
        return
    if row["kill_reason"]:
        await _rerequest_kill(engine, attempt, str(row["kill_reason"]), now)
        return
    # After the run reported completion with a valid return the parent has nothing left to do:
    # every limit still applies, under a kill reason that lets the return decide the task.
    done_at = engine.book.run_done.get(key)
    finished = done_at is not None and await engine_lifecycle.has_valid_return(engine, attempt, {})
    if finished and done_at is not None:
        waited = now - done_at
        if waited > engine.settings.exit_after_completion_s:
            limit_after = engine.settings.exit_after_completion_s
            data: dict[str, Any] = {"n": n, "after_s": round(waited, 3), "limit_s": limit_after}
            await engine._kill_for(attempt, EXIT_AFTER_COMPLETION, data)
            return
    async with db.execute(
        "SELECT COALESCE(SUM(running_s), 0) AS s FROM attempts "
        "WHERE task_id = ? AND n != ? AND ended_at IS NOT NULL",
        (task_id, n),
    ) as cur:
        total = await cur.fetchone()
    running_s = float(total["s"] if total else 0) + max(0.0, now - attempt.started_at)
    limit = engine.settings.task_timeout_s
    if running_s > limit:
        data = {"n": n, "running_s": round(running_s, 3), "limit_s": limit}
        await engine._kill_for(
            attempt, EXIT_AFTER_COMPLETION if finished else recovery.TIMEOUT, data
        )
        return
    if busy_processes(engine, key):
        engine.book.last_busy[key] = now  # activity now, remembered for the quiet passes
        return  # the stage's processes are working (e.g. one long command)
    pending = engine.book.activity_reads.get(key)
    if pending is not None and not pending.done():
        return  # the previous read still hangs: never a second thread for this attempt
    read = asyncio.ensure_future(asyncio.to_thread(last_activity, engine, attempt))
    engine.book.activity_reads[key] = read
    read.add_done_callback(lambda done: _read_done(engine, key, done))
    try:
        last = await asyncio.wait_for(asyncio.shield(read), ACTIVITY_TIMEOUT_S)
    except TimeoutError:
        _log.warning("activity read timed out", extra={"task_id": task_id})
        return
    except Exception:
        return  # logged by ``_read_done``; never a stall on a failed read
    busy_at = engine.book.last_busy.get(key)
    if busy_at is not None:
        last = busy_at if last is None else max(last, busy_at)
    if last is None:
        return  # nothing readable: never a stall on missing evidence
    idle_s = now - last
    if idle_s > engine.settings.stall_timeout_s:
        data = {"n": n, "idle_s": round(idle_s, 3), "limit_s": engine.settings.stall_timeout_s}
        await engine._kill_for(attempt, EXIT_AFTER_COMPLETION if finished else recovery.STALL, data)


def busy_processes(engine: "TaskEngine", key: tuple[str, int]) -> bool:
    """The attempt's descendants (never its claude) used at least ``CPU_ACTIVITY_S`` of CPU
    time since the watchdog's previous pass. From the watch's latest process snapshot; a pass
    with no figures to compare (none yet, or the listing failed) is not activity."""
    now = engine.book.cpu.get(key)
    before = engine.book.cpu_seen.get(key)
    if now is None:
        return False
    engine.book.cpu_seen[key] = now
    if before is None:
        return False
    used = 0.0
    for pid, (start, cpu) in now.items():
        seen = before.get(pid)
        used += max(0.0, cpu - seen[1]) if seen is not None and seen[0] == start else cpu
    return used >= CPU_ACTIVITY_S


async def _rerequest_kill(engine: "TaskEngine", attempt: Attempt, reason: str, now: float) -> None:
    """A kill reason is set: while no kill is in flight and the attempt is still open, a kill
    that was given up is requested again once its (growing) delay is over."""
    key = (attempt.task.id, attempt.n)
    if key in engine.book.killing:
        return  # a kill is in flight
    retry = engine.book.kill_retry.get(key)
    if retry is None or now < retry[1]:
        return  # no kill was given up (or its delay is not over): nothing to retry
    async with get_db().execute(
        "SELECT ended_at FROM attempts WHERE task_id = ? AND n = ?", key
    ) as cur:
        row = await cur.fetchone()
    if row is None or row["ended_at"] is not None:
        return
    await engine._note(
        attempt.task.id, "kill_rerequested",
        {"n": attempt.n, "reason": reason, "given_up": retry[0]},
    )  # fmt: skip
    trigger = "cancel" if reason == recovery.CANCEL else "attempt_end"
    engine_watch.request_kill(engine, key, attempt.pid, attempt.start, trigger)
