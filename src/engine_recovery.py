"""Applying the recovery decider in the engine: the decision for an ended attempt, its columns
written in the transaction that closes the attempt, and the engine's own failures (an engine
error is a ``transient`` end; a spawn or launch failure fails the task).

The policy itself is ``recovery.next_action`` (pure)."""

import asyncio
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

import aiosqlite

from src import engine_sweep, journal, recovery, usage
from src.classify import Outcome
from src.database import write_tx
from src.serverlog import LOGGER_NAME
from src.tasks import Task

if TYPE_CHECKING:
    from src.engine import TaskEngine
    from src.engine_watch import Attempt

_log = logging.getLogger(LOGGER_NAME)
FRESH_NOTE = (
    "a fresh start begins a new workflow run without the earlier answers; guidance given in "
    "them is not carried over"
)


def decide(
    engine: "TaskEngine",
    task: Task,
    outcome: Outcome,
    reset_at: float | None,
    pending_answer: int | None = None,
) -> tuple[recovery.Action, dict[str, Any]]:
    """The decider's action for the ended attempt and the task columns it implies. ``now`` is
    the engine clock: the scheduler compares ``next_attempt_at`` against the same clock."""
    now = engine._clock()
    models = frozenset(agent.model for agent in engine.agents.values() if agent.model)
    action = recovery.next_action(
        outcome, task, engine.settings, now, reset_at, pending_answer, models
    )
    return action, recovery.task_update(action, task, now, str(uuid.uuid4()))


# -- fresh start: the plan ---------------------------------------------------------------------
def _run_steps(engine: "TaskEngine", task: Task) -> list[journal.JournalStep]:
    """The run's journal steps; else those of the run-store copy, only when that copy is of the
    same run (a newer run refused before its first copy must not read the previous run's)."""
    if not task.run_id or not task.session_id:
        return []
    home = engine.settings.claude_home
    steps = journal.read_steps(task.cwd or ".", task.session_id, task.run_id, home)
    if steps or engine.runstore.copied_run_id(task.id) != task.run_id:
        return steps
    return journal.steps_in(engine.runstore.task_dir(task.id))


async def _read_run(engine: "TaskEngine", task: Task) -> list[journal.JournalStep] | None:
    """The run's journal steps; None when nothing of it could be read (no journal of this run
    found, an empty one, or a read error)."""
    try:
        steps = await asyncio.to_thread(_run_steps, engine, task)
    except Exception:
        _log.exception("could not read the run's journal", extra={"task_id": task.id})
        return None
    return steps or None


async def plan_to_pin(engine: "TaskEngine", task: Task) -> recovery.PlanPin:
    """Read before the deciding transaction (never under the write lock): what a fresh start
    of ``task``'s run does with the plan (``recovery.plan_pin``)."""
    return (await refused_inputs(engine, task))[0]


async def refused_inputs(engine: "TaskEngine", task: Task) -> tuple[recovery.PlanPin, int | None]:
    """Read before the deciding transaction of a refused resume (never under the write lock):
    the plan a fresh start would pin and the run's pending answer (``recovery.pending_answer``:
    an answer the run never consumed, or any answer when the journal cannot be read)."""
    if not task.is_lifecycle:
        return recovery.NO_PLAN_READ, None
    steps = await _read_run(engine, task)
    if steps is None:
        return recovery.NO_PLAN_READ, recovery.pending_answer(task.args_dict, None)
    pin = recovery.plan_pin(steps, task.args_dict.get("plan_gate") is True)
    return pin, recovery.pending_answer(task.args_dict, [(s.label, s.status) for s in steps])


async def fresh_start(
    engine: "TaskEngine", db: aiosqlite.Connection, task: Task, pin: recovery.PlanPin
) -> tuple[dict[str, Any], int]:
    """Inside the deciding transaction: the columns a fresh lifecycle run starts from (args with
    no answers, ``plan_pinned`` per ``recovery.fresh_args``) and the events. ``pin`` was read for
    this same run (the caller compares run and session). A newly pinned plan is written to
    ``inputs/plan.json``; a pin that is no longer current is moved out of ``inputs/``. Both
    happen before the commit. Also returns the number of answers dropped."""
    if not task.is_lifecycle:
        return {}, 0
    await _archive_run(engine, db, task)
    args_json, dropped = recovery.fresh_args(task.args_json, pin)
    if pin.plan is not None:
        label, value = pin.plan
        await asyncio.to_thread(engine.runstore.write_inputs, task.id, None, value)
        await engine._event(db, task.id, "plan_pinned", {"label": label, "run_id": task.run_id})
    elif task.args_dict.get("plan_pinned") is True and not json.loads(args_json)["plan_pinned"]:
        moved = await asyncio.to_thread(engine.runstore.retire_plan, task.id, task.attempt)
        data = {"run_id": task.run_id, "moved_to": str(moved) if moved else None}
        await engine._event(db, task.id, "plan_unpinned", data)
    return {"args_json": args_json}, dropped


async def _archive_run(engine: "TaskEngine", db: aiosqlite.Connection, task: Task) -> None:
    """The previous run's run-store copies, answers, ``out/`` files and step rows go to
    ``history/<run id>/`` (``RunStore.archive_run``); its ``steps`` rows are removed, so the new
    run's step mirror starts empty (no stale label, no inherited start time)."""
    sql = "SELECT * FROM steps WHERE task_id = ? ORDER BY rowid"
    async with db.execute(sql, (task.id,)) as cur:
        step_rows = [dict(r) for r in await cur.fetchall()]
    run_id = engine.runstore.copied_run_id(task.id) or task.run_id or f"attempt-{task.attempt}"
    moved = await asyncio.to_thread(engine.runstore.archive_run, task.id, run_id, step_rows)
    await db.execute("DELETE FROM steps WHERE task_id = ?", (task.id,))
    if moved is not None:
        await engine._event(db, task.id, "run_archived", {"run_id": run_id, "moved_to": str(moved)})


async def apply_decision(
    engine: "TaskEngine",
    db: aiosqlite.Connection,
    task: Task,
    action: recovery.Action,
    update: dict[str, Any],
    pin: recovery.PlanPin = recovery.NO_PLAN_READ,
) -> None:
    """Write the decider's columns for the running attempt ``task.attempt`` (inside the
    caller's transaction). A fresh start applies ``pin`` (read beforehand for ``task``'s run) and
    starts a run without answers; a relaunch is recorded as an event."""
    dropped = 0
    if isinstance(action, recovery.Fresh):
        columns, dropped = await fresh_start(engine, db, task, pin)
        update.update(columns)
    update["updated_at"] = time.time()
    sets = ", ".join(f"{col} = ?" for col in update)
    await db.execute(
        f"UPDATE tasks SET {sets} WHERE id = ? AND attempt = ? AND status = 'running'",
        [*update.values(), task.id, task.attempt],
    )
    described = recovery.describe(action)
    if described is None:
        return
    name, data = described
    if isinstance(action, recovery.Fail):  # an answer a refused resume left unapplied
        data = {**data, "branch": task.branch, "worktree": task.worktree}
    if isinstance(action, recovery.Fresh):
        data = {
            **data, "session_id": update["session_id"], "previous": task.session_id,
            "previous_run_id": task.run_id, "plan_pinned": pin.plan is not None,
            "dropped_answers": dropped, "note": FRESH_NOTE if dropped else None,
        }  # fmt: skip
    await engine._event(db, task.id, name, {"n": task.attempt, **data})
    if isinstance(action, recovery.Resume) and action.model_fallback:
        data = {"n": task.attempt, "from_to": action.model_fallback}
        await engine._event(db, task.id, "model_fallback", data)


async def engine_error(engine: "TaskEngine", task: Task, exc: Exception) -> None:
    """An internal failure: kill the attempt's process and let the decider treat it as a
    ``transient`` end (the error text stays in the attempt's detail); frees the slot. With no
    policy for the row (e.g. an unknown kill reason) the task fails: never left running."""
    error = f"engine error: {type(exc).__name__}: {exc}"
    attempt = engine._attempts.get(task.id)
    try:
        if attempt is None:
            await fail_unlaunched(engine, task, error)
            return
        if attempt.pid:
            await engine._kill(task.id, attempt.pid, attempt.start, "attempt_end")
        await close_with_decision(engine, attempt, error)
    except Exception:
        _log.exception("could not record an engine error", extra={"task_id": task.id})
    own = engine_sweep.own_claude(attempt.pid, attempt.start) if attempt else {}
    await engine._sweep_leftovers(task.id, "attempt_end", own)
    engine._wake.set()


async def close_with_decision(engine: "TaskEngine", attempt: "Attempt", error: str) -> None:
    task_id, now = attempt.task.id, time.time()
    async with write_tx() as db:
        await db.execute(
            "UPDATE attempts SET ended_at = ?, outcome = ?, reason = 'engine_error', "
            "detail = ?, running_s = ? WHERE task_id = ? AND n = ? AND ended_at IS NULL",
            (now, Outcome.transient.value, error, now - attempt.started_at, task_id, attempt.n),
        )
        await engine._event(db, task_id, "engine_error", {"error": error})
        row = await engine._fetch(db, task_id)
        current = Task.from_row(row) if row is not None else None
        if current is None or current.status != "running" or current.attempt != attempt.n:
            return
        try:
            action, update = decide(engine, current, Outcome.transient, None)
        except ValueError:  # no policy (e.g. an unknown kill reason): fail
            action = recovery.Fail(error)
            update = recovery.task_update(action, current, now, "")
        if isinstance(action, recovery.Fail):
            update["error"] = error
        await apply_decision(engine, db, current, action, update)


async def fail_unlaunched(engine: "TaskEngine", task: Task, error: str) -> None:
    now = time.time()
    async with write_tx() as db:
        cur = await db.execute(
            "UPDATE tasks SET status = 'failed', error = ?, completed_at = ?, updated_at = ? "
            "WHERE id = ? AND status = ? AND attempt = ?",
            (error, now, now, task.id, task.status, task.attempt),
        )
        if cur.rowcount:
            await engine._event(db, task.id, "launch_failed", {"error": error})


async def spawn_failed(engine: "TaskEngine", attempt: "Attempt", exc: Exception) -> None:
    now = time.time()
    error = f"spawn failed: {exc}"
    _log.error(error, extra={"task_id": attempt.task.id})
    async with write_tx() as db:
        await db.execute(
            "UPDATE attempts SET ended_at = ?, outcome = ?, reason = ?, "
            "detail = ?, running_s = 0 WHERE task_id = ? AND n = ?",
            (now, Outcome.fatal.value, usage.SPAWN_FAILED, error, attempt.task.id, attempt.n),
        )
        await db.execute(
            "UPDATE tasks SET status = 'failed', error = ?, completed_at = ?, updated_at = ? "
            "WHERE id = ? AND attempt = ? AND status = 'running'",
            (error, now, now, attempt.task.id, attempt.n),
        )
        await engine._event(db, attempt.task.id, "spawn_failed", {"error": str(exc)})
