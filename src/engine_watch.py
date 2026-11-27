"""Watch one attempt: tail the stream, enforce the parent restriction, and the waiter."""

import asyncio
import dataclasses
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src import (
    engine_lifecycle,
    engine_recovery,
    engine_sweep,
    model_usage,
    procs,
    recovery,
    usage,
)
from src.classify import Outcome, auth_message, classify
from src.config import CHILD_ENV_NAME
from src.database import after_commit, get_db, write_tx
from src.launcher import Mode, call_matches, expected_call
from src.serverlog import LOGGER_NAME
from src.stream import (
    Ev,
    ParentToolUse,
    PermissionDenied,
    Result,
    StreamWatcher,
    WorkflowAck,
    WorkflowCall,
    WorkflowDone,
)
from src.tasks import (
    CLEAN_EXIT,
    LATE_PARENT_TOOL,
    LATE_WORKFLOW_CALL,
    RETURN_DECIDES,
    Task,
    is_clean_exit,
)

if TYPE_CHECKING:
    from src.engine import TaskEngine

FAST_POLL_S = 0.25  # until the first Workflow call (lifecycle) / first event (single)
SLOW_POLL_S = 2.0
EXIT_CHECK_S = 0.25
STDERR_TAIL_BYTES = 4096
ERROR_DETAIL_CHARS = 500
KILL_REREQUEST_S = 30.0  # first wait before a given-up kill is requested again (doubling)
KILL_REREQUEST_MAX_S = 600.0
_VIOLATION_EVENT = {"parent_violation": "parent_tool_violation"}
AUTH_FAILED_ERROR = (
    "claude is not authenticated for this service: {message}. Set credentials in {path} "
    "(see README: Credentials) or log in as the service user."
)
_log = logging.getLogger(LOGGER_NAME)


@dataclasses.dataclass(frozen=True)
class Attempt:
    task: Task  # the row as the compare-and-set left it
    mode: Mode
    n: int
    argv: list[str]
    stream_path: Path
    stderr_path: Path
    started_at: float
    pid: int = 0
    start: float | None = None
    # The Workflow input the launch prompt asked for (lifecycle); None: derived from the task.
    call: dict[str, Any] | None = None
    # Rebuilt at engine start from the attempts row (no Popen: liveness by pid + start time).
    boot: bool = False
    # Rebuilt at engine start with no live process of its own: the stream and the run file
    # decide; ``gone_at`` is the last sign of it running (the stream's mtime).
    gone: bool = False
    gone_at: float | None = None


def _tail(path: Path) -> str:
    try:
        with path.open("rb") as fh:
            fh.seek(max(0, path.stat().st_size - STDERR_TAIL_BYTES))
            return fh.read().decode("utf-8", "replace")
    except OSError:
        return ""


async def watch(engine: "TaskEngine", attempt: Attempt) -> None:
    """Tail the stream until the process ends. Lifecycle: the step mirror runs periodically,
    and the workflow's return is read (after a final mirror) before the attempt is decided."""
    task = attempt.task
    watcher = StreamWatcher(attempt.stream_path)
    events: list[Ev] = []
    # Rebuilt by replaying the whole stream through ``inspect`` (a re-attached attempt reads it
    # from offset 0): the first call, the run, its completion, kills already requested.
    state: dict[str, Any] = {"calls": 0, "killed": False, "denied": set()}
    if attempt.boot:
        state["denied"] = await _denied_before(task.id, attempt.started_at)
    try:
        await _watch(engine, attempt, watcher, events, state)
    finally:
        engine.activity.drop(task.id, attempt.n)


async def _watch(
    engine: "TaskEngine",
    attempt: Attempt,
    watcher: StreamWatcher,
    events: list[Ev],
    state: dict[str, Any],
) -> None:
    task = attempt.task
    # Every read of the stream runs off the event loop (a re-attached attempt replays the
    # whole stream); one poll at a time, in order (each one is awaited before the next).
    while True:
        new = await asyncio.to_thread(watcher.poll)
        events += new
        engine.activity.update(task.id, attempt.n, new, time.time())
        await engine._inspect(attempt, new, state)
        await _record_descendants(engine, attempt)
        if task.is_lifecycle:
            await engine_lifecycle.maybe_mirror(engine, attempt, state)
            fast = not any(isinstance(e, WorkflowCall) for e in events)
        else:
            fast = not events
        ended, exit_code = await engine._wait_exit(attempt, FAST_POLL_S if fast else SLOW_POLL_S)
        if ended:
            break
    new = await asyncio.to_thread(watcher.poll)
    await engine._inspect(attempt, new, state)
    workflow_return = None
    if task.is_lifecycle:
        workflow_return = await engine_lifecycle.finish(engine, attempt, state)
    await engine._finish_attempt(attempt, events + new, exit_code, workflow_return)


async def _denied_before(task_id: str, since: float) -> set[str]:
    """Tools whose denial this attempt already recorded (before a restart): not recorded again.
    Denials of earlier attempts do not count (a new one in this attempt is recorded)."""
    async with get_db().execute(
        "SELECT data_json FROM events WHERE task_id = ? AND type = 'permission_denied' AND at >= ?",
        (task_id, since),
    ) as cur:
        found = [json.loads(row["data_json"]).get("tool") for row in await cur.fetchall()]
    return {name for name in found if isinstance(name, str)}


def _record(engine: "TaskEngine", attempt: Attempt) -> None:
    """Snapshot the attempt's descendants (blocking: off the loop): recorded for the sweep, and
    their CPU times kept for the watchdog (a long command in a stage is activity). Checked in
    the same snapshot as the claude's own identity: never a reused pid's children."""
    if attempt.gone or not attempt.pid:
        return
    if attempt.start is None and not procs.tracked(attempt.pid):
        return
    kids, cpu = procs.descendants_with_cpu(attempt.pid, attempt.start)
    engine.book.cpu[(attempt.task.id, attempt.n)] = cpu
    if kids:
        engine.runstore.write_procs(attempt.task.id, kids)


async def _record_descendants(engine: "TaskEngine", attempt: Attempt) -> None:
    """Bookkeeping only: a failure is logged and never stops the watch."""
    try:
        await asyncio.to_thread(_record, engine, attempt)
    except procs.ListingError:
        return  # this poll's snapshot failed; the next one records them
    except Exception:
        _log.exception("could not record descendants", extra={"task_id": attempt.task.id})


def _ended(attempt: Attempt) -> bool:
    """Liveness of a pid this engine holds no Popen for (``ps``; called off the event loop).

    An attempt rebuilt at engine start without a live process of its own (``gone``), or without
    a pid, has ended. Without a start time it falls back to ``is_running(pid)``: the watch ends
    once that pid is gone (a reused pid keeps it open until the watchdog's limits end it)."""
    if attempt.gone or not attempt.pid:
        return True
    if attempt.start is None:
        return not procs.is_running(attempt.pid)
    # Only a positive answer ends it: an unreadable process table (``unknown``) never does.
    return procs.liveness(attempt.pid, attempt.start) in ("dead", "other")


async def wait_exit(attempt: Attempt, interval: float) -> tuple[bool, int | None]:
    """The engine's own child is decided by its Popen (``reap``, no ``ps``), checked every
    ``EXIT_CHECK_S``; any other pid (re-attached after a restart) once per ``interval`` by
    ``_ended`` in a worker thread, so a slow or hung ``ps`` never blocks the loop."""
    deadline = time.monotonic() + interval
    while True:
        if attempt.gone:  # before ``tracked``: a later child may reuse a gone attempt's pid
            return True, None
        if procs.tracked(attempt.pid):
            code = procs.reap(attempt.pid)
            if code is not None:
                return True, code
        else:
            if await asyncio.to_thread(_ended, attempt):
                return True, None
            await asyncio.sleep(max(0.0, deadline - time.monotonic()))
            return False, None
        if time.monotonic() >= deadline:
            return False, None
        await asyncio.sleep(EXIT_CHECK_S)


async def inspect(
    engine: "TaskEngine", attempt: Attempt, new: list[Ev], state: dict[str, Any]
) -> None:
    task = attempt.task
    for ev in new:
        if isinstance(ev, WorkflowAck) and task.is_lifecycle:
            # Only the launch of the attempt's first, matching call names the task's run.
            if state.get("call_ok") and ev.tool_use_id == state.get("first_call"):
                if not ev.is_error:
                    await engine_lifecycle.record_run(engine, attempt, ev, state)
            continue
        if isinstance(ev, WorkflowDone) and task.is_lifecycle:
            if ev.tool_use_id == state.get("first_call") and ev.status == "completed":
                state["run_completed"] = True
                # The watchdog gives the parent exit_after_completion_s to exit from here.
                engine.book.run_done.setdefault((task.id, attempt.n), engine._clock())
            continue
        if isinstance(ev, PermissionDenied):
            await engine._denied(task.id, ev.tool_name, ev.message, state["denied"])
        elif isinstance(ev, Result):
            for denial in ev.permission_denials:
                name = denial.get("tool_name") if isinstance(denial, dict) else None
                await engine._denied(task.id, name, None, state["denied"])
        if not task.is_lifecycle or state["killed"]:
            continue  # a single task works in the parent session: tool use is normal
        reason = None
        data: dict[str, Any] | None = None
        if isinstance(ev, ParentToolUse):
            # Killed either way. After this run reported completion with a valid return, the
            # return still decides the task (the kill reason says so); before, the task fails.
            reason = "parent_violation"
            if state.get("run_completed") and await engine_lifecycle.has_valid_return(
                engine, attempt, state
            ):
                reason, data = LATE_PARENT_TOOL, {"tool": ev.name}
        elif isinstance(ev, WorkflowCall):
            state["calls"] += 1
            if state["calls"] > 1:
                # As a late parent tool call: after a completed run with a valid return, the
                # return decides; before, it is a call mismatch (corrective resume).
                reason = "second_workflow_call"
                if state.get("run_completed") and await engine_lifecycle.has_valid_return(
                    engine, attempt, state
                ):
                    reason, data = LATE_WORKFLOW_CALL, {"tool": "Workflow"}
            elif not call_matches(attempt.call or expected_call(task, attempt.mode), ev.input):
                reason = "call_mismatch"
            else:
                state["first_call"], state["call_ok"] = ev.tool_use_id, True
        if reason is not None:
            state["killed"] = True
            await engine._kill_for(attempt, reason, data)


async def denied(
    engine: "TaskEngine", task_id: str, tool: Any, message: str | None, seen: set[str]
) -> None:
    name = tool if isinstance(tool, str) else "unknown"
    if name in seen:
        return
    seen.add(name)
    await engine._note(task_id, "permission_denied", {"tool": name, "message": message})


async def kill_for(
    engine: "TaskEngine", attempt: Attempt, reason: str, data: dict[str, Any] | None = None
) -> None:
    """Record the kill reason, then kill in the background: the watch keeps tailing the stream
    meanwhile (the kill grace never blocks it); ``stop()`` awaits the kill. One kill per
    attempt per engine (a re-attached attempt's replay, the watchdog and boot may all ask)."""
    task = attempt.task
    key = (task.id, attempt.n)
    async with write_tx() as db:
        cur = await db.execute(
            "UPDATE tasks SET kill_reason = ?, updated_at = ? WHERE id = ? AND attempt = ? "
            "AND status = 'running' AND kill_reason IS NULL",
            (reason, time.time(), task.id, attempt.n),
        )
        if cur.rowcount:
            await engine._event(db, task.id, _VIOLATION_EVENT.get(reason, reason), data or {})
        else:
            async with db.execute(
                "SELECT ended_at FROM attempts WHERE task_id = ? AND n = ?", (task.id, attempt.n)
            ) as rows:
                row = await rows.fetchone()
            if row is None or row["ended_at"] is not None:
                return  # the attempt is already closed: nothing of it to kill or sweep
    request_kill(engine, key, attempt.pid, attempt.start, "attempt_end")


def request_kill(
    engine: "TaskEngine", key: tuple[str, int], pid: int, start: float | None, trigger: str
) -> None:
    """Start the attempt's one kill in flight (cancel and every ``kill_for`` come here); a
    request while one is in flight starts nothing."""
    if key in engine.book.killing:
        return
    engine.book.killing.add(key)
    engine._background(_kill_once(engine, key, pid, start, trigger))


async def _kill_once(
    engine: "TaskEngine", key: tuple[str, int], pid: int, start: float | None, trigger: str
) -> None:
    """Given up (process table unreadable): the attempt may be killed again; the watchdog
    re-requests it after a growing delay (``KILL_REREQUEST_S`` doubling, capped at
    ``KILL_REREQUEST_MAX_S``) while the kill reason stays set and the attempt is open."""
    done = await engine._kill(key[0], pid, start, trigger)
    book = engine.book
    if done is False and key in book.killing:
        book.killing.discard(key)
        given_up = book.kill_retry.get(key, (0, 0.0))[0] + 1
        delay = min(KILL_REREQUEST_S * 2 ** (given_up - 1), KILL_REREQUEST_MAX_S)
        book.kill_retry[key] = (given_up, engine._clock() + delay)


def _failure_text(
    engine: "TaskEngine", base: str, exit_code: int | None, stderr_tail: str, last: Result | None
) -> str:
    """``<outcome>[: kill_reason] (exit N): <redacted stderr tail or result error>``."""
    text = base if exit_code is None else f"{base} (exit {exit_code})"
    detail = stderr_tail.strip()
    if not detail and last is not None and last.is_error:
        detail = "; ".join(last.errors) or (last.text or "")
    detail = detail[-ERROR_DETAIL_CHARS:].strip()
    return f"{text}: {engine.redactor.redact_text(detail)}" if detail else text


def _auth_failure(
    engine: "TaskEngine", events: list[Ev], records: list[dict[str, Any]]
) -> tuple[str, str]:
    """Claude's message (redacted) and the task error for an ``auth_failed`` attempt."""
    message = auth_message(events, records).strip()[-ERROR_DETAIL_CHARS:].strip().rstrip(".")
    message = engine.redactor.redact_text(message)
    path = engine.settings.child_env_file or engine.settings.home / CHILD_ENV_NAME
    return message, AUTH_FAILED_ERROR.format(message=message, path=path)


async def finish_attempt(
    engine: "TaskEngine",
    attempt: Attempt,
    events: list[Ev],
    exit_code: int | None,
    workflow_return: Any = None,
) -> None:
    """The waiter: the only place that moves a running task to its next status.

    ``workflow_return``: a lifecycle run's return value (``engine_lifecycle.finish``) or None.
    """
    task_id = attempt.task.id
    stderr_tail = await asyncio.to_thread(_tail, attempt.stderr_path)
    records: list[dict[str, Any]] = []
    if attempt.task.is_lifecycle:  # a stage that hit a usage limit (structured records only)
        records = await engine_lifecycle.stage_error_records(engine, attempt, {}, workflow_return)
    outcome, reset_at = classify(
        exit_code=exit_code,
        events=events,
        workflow_return=workflow_return,
        stderr_tail=stderr_tail,
        stage_error_records=records,
        kind=attempt.task.kind,
    )
    results = [e for e in events if isinstance(e, Result)]
    last = results[-1] if results else None
    # Listed before the transaction (``ps`` off the loop) when the exit may be clean.
    row = await engine._fetch(get_db(), task_id)
    maybe_clean = row is not None and row["kill_reason"] is None and row["status"] == "running"
    live = None
    if maybe_clean:
        live = await asyncio.to_thread(engine_sweep.live_procs, engine, task_id)
    # A fresh start's plan is read here, never under the write lock (re-checked below).
    # A refused resume also reads whether the run consumed its latest answer (a fresh start
    # must never drop a human decision the run did not apply).
    read = Task.from_row(row) if row is not None else None
    pin, pending = recovery.NO_PLAN_READ, None
    if read is not None and outcome is Outcome.resume_refused:
        pin, pending = await engine_recovery.refused_inputs(engine, read)
    now = time.time()
    async with write_tx() as db:
        current = Task.from_row(await engine._fetch(db, task_id))
        status = current.status  # cancelled meanwhile: cancel already wrote the terminal status
        # The attempt's own share: a resumed session's report includes its earlier attempts.
        prior = await usage.session_usage_before(db, task_id, attempt.n)
        share = usage.attempt_usage(last, prior)
        totals = await usage.task_usage(db, task_id, attempt.n, share)
        update: dict[str, Any] | None = None
        action: recovery.Action | None = None
        explained = False
        same_run = read is not None and (read.session_id, read.run_id) == (
            current.session_id, current.run_id,
        )  # fmt: skip
        if not same_run:  # what was read is of another run: nothing about this one is known
            pin, pending = recovery.NO_PLAN_READ, recovery.pending_answer(current.args_dict, None)
        if current.status == "running" and current.attempt == attempt.n:
            action, update = engine._decide(current, outcome, reset_at, pending)
            return_decides = current.kill_reason is None or current.kill_reason in RETURN_DECIDES
            if attempt.task.is_lifecycle and return_decides:
                # May turn an unanswerable needs_human into failed: before `clean` is decided.
                explained = engine_lifecycle.apply_return(
                    engine, update, workflow_return, totals, outcome
                )
                failure = engine_lifecycle.failure_event(engine, workflow_return, outcome)
                if failure is not None and update["status"] == "failed":
                    await engine._event(db, task_id, *failure)
            if return_decides and outcome is Outcome.auth_failed and update["status"] == "failed":
                message, update["error"] = _auth_failure(engine, events, records)
                explained = True
                data = {"n": attempt.n, "message": message}
                await engine._event(db, task_id, "auth_failed", data)
            status = update["status"]
        clean = update is not None and is_clean_exit(status, current.kill_reason)
        models = model_usage.model_shares(
            last, prior, await model_usage.prior_models(db, task_id, attempt.n)
        )
        ended = await db.execute(
            "UPDATE attempts SET ended_at = ?, exit_code = ?, outcome = ?, reason = ?, "
            "cost_usd = ?, tokens = ?, running_s = ? "
            "WHERE task_id = ? AND n = ? AND ended_at IS NULL",
            (
                now, exit_code, outcome.value, CLEAN_EXIT if clean else current.kill_reason,
                share.cost_usd, share.tokens,
                (attempt.gone_at or now) - attempt.started_at, task_id, attempt.n,
            ),
        )  # fmt: skip
        if ended.rowcount and models is not None:
            await model_usage.write_models(db, task_id, attempt.n, models)
        if update is not None:
            if (
                not attempt.task.is_lifecycle
                and update["status"] == "completed"
                and last is not None
            ):
                output = {
                    "text": last.text, "tokens_used": totals.tokens, "cost_usd": totals.cost_usd,
                }  # fmt: skip
                update["output_json"] = json.dumps(output)
            if update["status"] == "failed" and not explained:
                error = _failure_text(engine, update["error"], exit_code, stderr_tail, last)
                update["error"] = error
            assert action is not None
            await engine_recovery.apply_decision(engine, db, current, action, update, pin)
            data = {"n": attempt.n, "outcome": outcome.value, "status": status}
            await engine._event(db, task_id, "attempt_finished", data)
        if clean:  # what a clean attempt left running stays running
            # File writes are not transactional: kept only once the clean exit is committed
            # (after_commit runs right after COMMIT, before any other coroutine; dropped on
            # rollback, so the attempt stays unclean and its recorded set intact).
            after_commit(lambda: engine_sweep.keep(engine, task_id, attempt.n, live))
            data = engine_sweep.sweep_data(engine_sweep.SKIPPED_CLEAN_EXIT, "attempt_end")
            await engine._event(db, task_id, "sweep", data)
    # Every unclean end, whatever comes next (a retry waits in queued meanwhile): e.g. Bash-tool
    # shells re-parented to PID 1. Unconfirmed (process table unreadable): a relaunch is still
    # preceded by the pre-launch sweep; a terminal task's sweep is retried by the watchdog.
    if not clean:
        own = engine_sweep.own_claude(attempt.pid, attempt.start)
        await engine._sweep_leftovers(task_id, "attempt_end", own)
