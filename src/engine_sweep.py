"""Sweep policy: kill what a task left running only after an unclean attempt.

A clean exit (``tasks.is_clean_exit``) sweeps nothing, neither at the attempt's end nor before
the launch that follows it. Once it is committed, the task's processes alive at that moment are
kept (``RunStore.keep_procs``, each tagged with the attempt that left it) and the recorded
descendant set restarts empty. A kept entry is honoured only while its attempt's row says
``clean_exit`` and the pid is still the same process (pid + start time). Every other end of an
attempt is unclean: the task's leftovers (recorded set, marker processes) are swept at its
terminal status and before the relaunch that follows; a relaunch never starts over a survivor.
Whether the previous attempt was clean is read from its ``attempts`` row (an open or missing row
is unclean), so it holds across a server restart. Each decision writes one ``sweep`` event.
"""

import asyncio
import json
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import aiosqlite

from src import procs
from src.database import get_db, write_tx
from src.serverlog import LOGGER_NAME
from src.tasks import CLEAN_EXIT, TERMINAL, Task, attempt_was_clean

if TYPE_CHECKING:
    from src.engine import TaskEngine
    from src.engine_watch import Attempt

SWEEP_GRACE_S = 10.0
SWEEP_RETRY_S = 30.0
UNCONFIRMED_RETRY_MAX_S = 600.0
UNCONFIRMED_LOG_EVERY = 10
KILL_TRIES = 5  # a kill whose target cannot be identified (ps failing) is retried this often
KILL_RETRY_S = 1.0
SWEPT = "swept"
SKIPPED_CLEAN_EXIT = "skipped_clean_exit"
_log = logging.getLogger(LOGGER_NAME)


def sweep_data(
    policy: str,
    trigger: str,
    killed: Sequence[int] = (),
    exempted: Sequence[tuple[int, str]] = (),
    survivors: Sequence[int] = (),
) -> dict[str, Any]:
    """Payload of the ``sweep`` event (trigger: attempt_end, pre_launch or cancel)."""
    return {
        "policy": policy,
        "trigger": trigger,
        "killed": sorted(killed),
        "exempted": [{"pid": pid, "command": command} for pid, command in sorted(exempted)],
        "survivors": sorted(survivors),
    }


def own_claude(pid: int | None, start: float | None) -> dict[int, float]:
    """The task's ``claude`` (pid -> start), which no exemption covers."""
    return {pid: start} if pid and start is not None else {}


async def clean_attempts(task_id: str) -> set[int]:
    """Numbers of the task's attempts whose persisted row records a clean exit."""
    async with get_db().execute(
        "SELECT n FROM attempts WHERE task_id = ? AND ended_at IS NOT NULL AND reason = ?",
        (task_id, CLEAN_EXIT),
    ) as cur:
        return {int(row["n"]) for row in await cur.fetchall()}


def _spare(
    engine: "TaskEngine", task_id: str, own: dict[int, float], clean: set[int]
) -> procs.Spare:
    kept = engine.runstore.read_kept(task_id)
    honoured = {pid: start for pid, (start, n) in kept.items() if n in clean}
    return procs.Spare(tuple(engine.settings.sweep_exempt), honoured, own)


def _sweep(
    engine: "TaskEngine", task_id: str, grace_s: float, own: dict[int, float], clean: set[int]
) -> tuple[procs.Swept, list[int]]:
    recorded = engine.runstore.read_procs(task_id)
    spare = _spare(engine, task_id, own, clean)
    swept = procs.sweep(task_id, recorded, grace_s, spare)
    return swept, procs.survivors(task_id, recorded, spare)


def _kill_tree(
    engine: "TaskEngine", task_id: str, pid: int, start: float, clean: set[int]
) -> tuple[procs.Swept, list[int] | None]:
    """The kill and its survivors; survivors None when the listing after the signals failed
    (the signals were sent; whether anything survived is unknown)."""
    recorded = engine.runstore.read_procs(task_id)
    spare = _spare(engine, task_id, {pid: start}, clean)
    swept = procs.kill_tree(pid, start, task_id, recorded=recorded, spare=spare)
    try:
        return swept, procs.survivors(task_id, recorded, spare)
    except procs.ListingError:
        return swept, None


def live_procs(engine: "TaskEngine", task_id: str) -> dict[int, float] | None:
    """The task's live processes (recorded, kept, marker); None when they cannot be read."""
    try:
        kept = {pid: start for pid, (start, _n) in engine.runstore.read_kept(task_id).items()}
        return procs.task_procs(task_id, {**engine.runstore.read_procs(task_id), **kept})
    except Exception:
        _log.exception("could not list task processes", extra={"task_id": task_id})
        return None


def keep(engine: "TaskEngine", task_id: str, n: int, live: dict[int, float] | None) -> None:
    """After attempt ``n``'s clean exit committed: keep what was alive, tagged with the attempt
    that left it (an entry kept earlier keeps its tag), and restart the recorded set. Skipped
    when the listing failed; bookkeeping only, a failure is logged."""
    if live is None:
        return
    try:
        old = engine.runstore.read_kept(task_id)
        kept = {
            pid: old[pid] if pid in old and old[pid][0] == start else (start, n)
            for pid, start in live.items()
        }
        engine.runstore.keep_procs(task_id, kept)
    except Exception:
        _log.exception("could not record kept processes", extra={"task_id": task_id})


async def previous_attempt_clean(task_id: str, n: int) -> bool:
    async with get_db().execute(
        "SELECT ended_at, reason FROM attempts WHERE task_id = ? AND n = ?", (task_id, n)
    ) as cur:
        row = await cur.fetchone()
    return attempt_was_clean(row)


async def cancel_idle(engine: "TaskEngine", db: aiosqlite.Connection, task: Task) -> bool:
    """Cancel of a ``queued`` (new, or waiting for a retry the decider scheduled) /
    ``needs_human`` task with no attempt in flight (inside the cancel transaction): True when
    its last attempt ended unclean and its leftovers are to be swept (trigger ``cancel``);
    after a clean one nothing is killed and the skip is recorded."""
    if task.status not in ("queued", "needs_human"):
        return False
    async with db.execute(
        "SELECT ended_at, reason FROM attempts WHERE task_id = ? AND n = ?", (task.id, task.attempt)
    ) as cur:
        row = await cur.fetchone()
    if not attempt_was_clean(row):
        return True
    await engine._event(db, task.id, "sweep", sweep_data(SKIPPED_CLEAN_EXIT, "cancel"))
    return False


async def sweep_clear(engine: "TaskEngine", attempt: "Attempt", own: dict[int, float]) -> bool:
    """Before a launch: after an unclean previous attempt, kill its leftovers; never launch over
    a survivor. After a clean one, sweep nothing.

    Survivors get one SIGKILL-only retry. If anything still survives, the attempt is closed
    (outcome ``sweep_survivors``) and the task goes back to ``queued`` for another try after
    ``SWEEP_RETRY_S`` (a stuck orphan never fails the task). An unreadable process table is
    handled the same way, without end (never launch over a process that may still run), but
    with a growing delay (``SWEEP_RETRY_S`` doubling, capped at ``UNCONFIRMED_RETRY_MAX_S``), a
    streak count in the events and a server-log error when a streak starts and every
    ``UNCONFIRMED_LOG_EVERY`` rounds. None of it counts against the resume budget.
    """
    task_id = attempt.task.id
    if await previous_attempt_clean(task_id, attempt.n - 1):
        engine.book.unconfirmed.pop(task_id, None)
        async with write_tx() as db:
            await engine._event(db, task_id, "sweep", sweep_data(SKIPPED_CLEAN_EXIT, "pre_launch"))
        return True
    killed: list[int] = []
    exempted: dict[int, str] = {}
    survivors: list[int] = []
    unconfirmed = False
    clean = await clean_attempts(task_id)
    for grace_s in (SWEEP_GRACE_S, 0.0):
        try:
            swept, survivors = await asyncio.to_thread(_sweep, engine, task_id, grace_s, own, clean)
        except procs.ListingError:
            # The process table could not be read: nothing can be confirmed gone, so this is
            # handled as survivors (never launch over a process that may still run).
            unconfirmed = True
            streak = engine.book.unconfirmed.get(task_id, 0) + 1
            data = {"trigger": "pre_launch", "streak": streak}
            await engine._note(task_id, "sweep_unconfirmed", data)
            continue
        unconfirmed = False
        engine.book.unconfirmed.pop(task_id, None)
        killed += swept.killed
        exempted.update(swept.exempted)
        data = sweep_data(SWEPT, "pre_launch", killed, list(exempted.items()), survivors)
        async with write_tx() as db:
            if survivors:
                await engine._event(db, task_id, "sweep_survivors", {"pids": survivors})
            else:
                await engine._event(db, task_id, "sweep", data)
                return True
    now = engine._clock()
    delay = SWEEP_RETRY_S
    data = sweep_data(SWEPT, "pre_launch", killed, list(exempted.items()), survivors)
    data["unconfirmed"] = unconfirmed
    if unconfirmed:
        streak = engine.book.unconfirmed[task_id] = engine.book.unconfirmed.get(task_id, 0) + 1
        engine.book.streak_round.add(task_id)  # this job's end keeps the streak
        delay = min(SWEEP_RETRY_S * 2 ** (streak - 1), UNCONFIRMED_RETRY_MAX_S)
        data["streak"] = streak
        if streak == 1 or streak % UNCONFIRMED_LOG_EVERY == 0:
            _log.error(
                "pre-launch sweep unconfirmed: process table unreadable, launch held back",
                extra={"task_id": task_id, "streak": streak, "retry_s": delay},
            )
    async with write_tx() as db:
        await engine._event(db, task_id, "sweep", data)
        await db.execute(
            "UPDATE attempts SET ended_at = ?, outcome = 'sweep_survivors', detail = ?, "
            "running_s = 0 WHERE task_id = ? AND n = ?",
            (now, json.dumps({"unconfirmed": True} if unconfirmed else survivors), task_id,
             attempt.n),
        )  # fmt: skip
        cur = await db.execute(
            "UPDATE tasks SET status = 'queued', next_attempt_at = ?, updated_at = ? "
            "WHERE id = ? AND attempt = ? AND status = 'running'",
            (now + delay, now, task_id, attempt.n),
        )
        if cur.rowcount:
            data = {"pids": survivors, "next_attempt_at": now + delay}
            if unconfirmed:
                data["streak"] = engine.book.unconfirmed[task_id]
            await engine._event(db, task_id, "sweep_requeued", data)
    return False


async def sweep_leftovers(
    engine: "TaskEngine", task_id: str, trigger: str, own: dict[int, float]
) -> None:
    """Bookkeeping after an unclean end: kill what is left of the task (recorded set, marker
    processes; start time checked, so a reused pid is never hit). Failures are logged.

    An unreadable process table confirms nothing. A task that is not terminal gets its
    pre-launch sweep before any relaunch; a terminal one gets none, so its sweep stays on the
    watchdog's retry list (``book.sweep_retry``, a growing delay) until one is confirmed."""
    try:
        clean = await clean_attempts(task_id)
        swept, survivors = await asyncio.to_thread(
            _sweep, engine, task_id, SWEEP_GRACE_S, own, clean
        )
    except procs.ListingError:
        await engine._note(task_id, "sweep_unconfirmed", {"trigger": trigger})
        await _retry_if_terminal(engine, task_id, trigger, own)
        return
    except Exception:
        _log.exception("could not sweep task processes", extra={"task_id": task_id})
        return
    engine.book.sweep_retry.pop(task_id, None)
    data = sweep_data(SWEPT, trigger, swept.killed, swept.exempted, survivors)
    await engine._note(task_id, "sweep", data)


async def _retry_if_terminal(
    engine: "TaskEngine", task_id: str, trigger: str, own: dict[int, float]
) -> None:
    try:
        status = await engine._column(get_db(), task_id, "status")
    except Exception:
        _log.exception("could not read the task status", extra={"task_id": task_id})
        status = "failed"  # unknown: retried (a sweep of a terminal task's leftovers is safe)
    if status not in TERMINAL:
        return  # the pre-launch sweep runs before any relaunch
    tries = engine.book.sweep_retry.get(task_id, ({}, trigger, 0, 0.0))[2] + 1
    delay = min(SWEEP_RETRY_S * 2 ** (tries - 1), UNCONFIRMED_RETRY_MAX_S)
    engine.book.sweep_retry[task_id] = (own, trigger, tries, engine._clock() + delay)


def retry_sweeps(engine: "TaskEngine") -> None:
    """The watchdog's pass: start every due retry of an unconfirmed terminal sweep (in the
    background, awaited by ``stop()``; one in flight per task: the entry is taken out first)."""
    now = engine._clock()
    for task_id, (own, trigger, _tries, due) in list(engine.book.sweep_retry.items()):
        if now >= due:
            del engine.book.sweep_retry[task_id]
            engine._background(_retry_sweep(engine, task_id, trigger, own, _tries))


async def _retry_sweep(
    engine: "TaskEngine", task_id: str, trigger: str, own: dict[int, float], tries: int
) -> None:
    # Restored first, so a failure of this retry grows the delay from the count so far.
    engine.book.sweep_retry.setdefault(task_id, (own, trigger, tries, float("inf")))
    await sweep_leftovers(engine, task_id, trigger, own)
    entry = engine.book.sweep_retry.get(task_id)
    if entry is not None and entry[3] == float("inf"):
        # Neither confirmed nor rescheduled: the sweep failed otherwise (logged by
        # ``sweep_leftovers``). Kept, and retried on the next watchdog pass.
        engine.book.sweep_retry[task_id] = (own, trigger, tries, engine._clock())


async def kill(
    engine: "TaskEngine", task_id: str, pid: int, start: float | None, trigger: str
) -> bool:
    """Kill the task's tree if ``pid`` is still the process started at ``start``. False when
    the kill was given up (the process table stayed unreadable); True otherwise.

    A skipped kill is observable: event ``kill_skipped_no_start_time`` (plus a server-log
    warning) or ``kill_skipped`` when the pid is gone or now belongs to another process. The
    parent's survivors (recorded set, marker processes) are then swept instead. A process this
    kill already signalled and that is gone at the re-check is recorded as such (``sweep`` with
    the pids signalled), never as "not running".
    """
    if start is None:
        _log.warning("kill skipped: no start time", extra={"task_id": task_id, "pid": pid})
        await engine._note(task_id, "kill_skipped_no_start_time", {"pid": pid})
        await sweep_leftovers(engine, task_id, trigger, own_claude(pid, start))
        return True
    signalled: list[int] = []
    exempted: list[tuple[int, str]] = []
    for n in range(KILL_TRIES):  # an unreadable process table is retried, never taken as gone
        if n:
            await asyncio.sleep(KILL_RETRY_S)
        state = await asyncio.to_thread(procs.liveness, pid, start)
        if state in ("dead", "other"):
            if signalled:  # it died of this kill: the listing failed before that was seen
                data = sweep_data(SWEPT, trigger, signalled, exempted)
                data["survivors_unconfirmed"] = True
                await engine._note(task_id, "sweep", data)
            else:
                data = {"pid": pid, "reason": "not running"}
                await engine._note(task_id, "kill_skipped", data)
            await sweep_leftovers(engine, task_id, trigger, own_claude(pid, start))
            return True
        if state == "unknown":
            continue
        clean = await clean_attempts(task_id)
        try:
            swept, survivors = await asyncio.to_thread(
                _kill_tree, engine, task_id, pid, start, clean
            )
        except procs.ListingError:
            continue  # nothing was signalled: the target could not be identified
        signalled = sorted({*signalled, *swept.killed})
        exempted = swept.exempted
        if survivors is None:
            continue  # signals sent; whether they took is checked on the next try
        data = sweep_data(SWEPT, trigger, signalled, exempted, survivors)
        await engine._note(task_id, "sweep", data)
        return True
    _log.error("kill not confirmed: process table unreadable", extra={"task_id": task_id})
    data = {"pid": pid, "tries": KILL_TRIES, "signalled": signalled}
    await engine._note(task_id, "kill_unconfirmed", data)
    return False
