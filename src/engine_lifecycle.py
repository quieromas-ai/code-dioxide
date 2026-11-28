"""Lifecycle tasks in the engine: the first-launch preparation (script snapshot, frozen args),
the run id stored live, the step mirror, and the workflow's return mapped to task state.

The workflow's return value exists only in ``<session>/workflows/<runId>.json``; it is read after
the process exits and before the attempt is classified. The step mirror reads the journal and
the run file every ``MIRROR_INTERVAL_S`` while an attempt runs (and once more at its end) and
feeds the ``steps`` table (``status()``, the debug view) and the run-store copies (log sources).
"""

import asyncio
import dataclasses
import inspect
import json
import logging
import subprocess
import time
from collections import deque
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite

from src import journal, stage_measure
from src.classify import Outcome, ScriptFailure, valid_return
from src.database import get_db, write_tx
from src.launcher import SCRIPT_NAME
from src.lifecycle_args import build_lifecycle_args
from src.logpager import iso
from src.serverlog import LOGGER_NAME
from src.stages import stage_numbers
from src.stream import WorkflowAck
from src.tasks import Task
from src.usage import Usage

if TYPE_CHECKING:
    from src.engine import TaskEngine
    from src.engine_watch import Attempt
    from src.runstore import RunStore

# Where the workflow tree lives: inside the installed package (the wheel force-includes
# `workflows/` as `src/workflows/`), else in the source tree next to `src/`.
PACKAGE_DIR = Path(__file__).resolve().parent
# Snapshot files made executable regardless of the installed file mode.
EXECUTABLES = ("stage/*.sh",)
MIRROR_INTERVAL_S = 5.0
GIT_TIMEOUT_S = 10.0
ERROR_DETAIL_CHARS = 500
# Gate asks (the asking stage stays cached, a new interpret step reads the answer) take an
# answer with mode "after"; a stage-raised question (kind "question") re-runs that stage.
GATE_KINDS = frozenset({"plan_approval", "rounds_exhausted", "merge_blocked"})
KINDS = GATE_KINDS | {"question"}
# Phase title per label base (feature-lifecycle.js meta.phases); interpret steps take the phase
# of the ask point they interpret.
_PHASES = (
    ("plan", "Plan"),
    ("setup", "Setup"),
    ("implement", "Implement"),
    ("review-", "Review"),
    ("fix-", "Review"),
    ("qa-", "QA"),
    ("deploy", "Deploy"),
    ("merge", "Merge"),
)
_INTERPRET = "interpret-"
_log = logging.getLogger(LOGGER_NAME)


class LaunchProblem(Exception):
    """A lifecycle task that cannot be launched; the message becomes the task error."""


def workflows_dir(package_dir: Path | None = None) -> Path | None:
    """The workflow tree holding ``feature-lifecycle.js``: packaged first, source tree second."""
    base = PACKAGE_DIR if package_dir is None else package_dir
    for candidate in (base / "workflows", base.parent / "workflows"):
        if (candidate / SCRIPT_NAME).is_file():
            return candidate
    return None


def answer_mode(kind: Any) -> str:
    return "after" if kind in GATE_KINDS else "rerun"


def phase_of(label: str) -> str | None:
    base = label.split("@", 1)[0]
    if base.startswith(_INTERPRET):
        base = base[len(_INTERPRET) :]
    for prefix, title in _PHASES:
        if prefix.endswith("-"):
            if base.startswith(prefix):
                return title
        elif base == prefix or base.startswith(prefix + "-"):  # interpret-plan-1 -> plan-1
            return title
    return None


# -- first launch ------------------------------------------------------------------------------
async def prepare(engine: "TaskEngine", task: Task) -> Task | None:
    """Before the first claim: snapshot ``workflows/`` into ``<run>/script/`` and freeze the
    full lifecycle args into the row (compare-and-set). Both happen once; a later launch (a
    resume after an answer) reuses them unchanged. Before every launch: ``<run>/out/`` exists,
    writable (the stages' only writable run dir, passed with ``--add-dir``).

    None when the row moved meanwhile (the launch is dropped; the persisted args are never
    replaced by unpersisted ones)."""
    if not task.is_lifecycle:
        return task
    try:
        await asyncio.to_thread(engine.runstore.ensure_out, task.id)
    except OSError as exc:
        raise LaunchProblem(f"could not prepare the lifecycle launch: {exc}") from None
    if "roles" in task.args_dict:
        return task
    roles = engine.roles
    if roles is None:
        raise LaunchProblem("a lifecycle task needs roles (roles.yaml)")
    source = workflows_dir()
    if source is None:
        raise LaunchProblem(f"workflow script {SCRIPT_NAME} not found (package or source tree)")
    try:
        args = build_lifecycle_args(task, engine.settings, roles)
        script = await asyncio.to_thread(
            engine.runstore.snapshot_script, task.id, source, EXECUTABLES
        )
    except (OSError, ValueError) as exc:
        raise LaunchProblem(f"could not prepare the lifecycle launch: {exc}") from None
    args_json = json.dumps(args, ensure_ascii=False)
    async with write_tx() as db:
        cur = await db.execute(
            "UPDATE tasks SET args_json = ?, updated_at = ? "
            "WHERE id = ? AND status = ? AND attempt = ? AND args_json IS ?",
            (args_json, time.time(), task.id, task.status, task.attempt, task.args_json),
        )
        if not cur.rowcount:
            return None
        await engine._event(db, task.id, "lifecycle_prepared", {"script_dir": str(script)})
    return dataclasses.replace(task, args_json=args_json)


async def record_run(
    engine: "TaskEngine", attempt: "Attempt", ack: WorkflowAck, state: dict[str, Any]
) -> None:
    """The first ok launch ack of the attempt's (matching) call: its run id goes to the row now,
    while the attempt runs (the resume call, ``status()`` and the run locator read it there)."""
    if state.get("run") is not None or not ack.run_id:
        return
    state["run"] = (ack.run_id, ack.transcript_dir)
    task = attempt.task
    async with write_tx() as db:
        cur = await db.execute(
            "UPDATE tasks SET run_id = ?, updated_at = ? WHERE id = ? AND attempt = ? "
            "AND run_id IS NOT ?",
            (ack.run_id, time.time(), task.id, attempt.n, ack.run_id),
        )
        if cur.rowcount:
            await engine._event(db, task.id, "run_started", {"run_id": ack.run_id})


async def _run_of(attempt: "Attempt", state: dict[str, Any]) -> tuple[str, Path | None] | None:
    run = state.get("run")
    if run is not None:
        run_id, tdir = run
        return run_id, Path(tdir) if tdir else None
    async with get_db().execute("SELECT run_id FROM tasks WHERE id = ?", (attempt.task.id,)) as cur:
        row = await cur.fetchone()
    return (row["run_id"], None) if row is not None and row["run_id"] else None


# -- stage rate limits -----------------------------------------------------------------------------
STAGE_RECORD_LINES = 200  # the tail of a step transcript searched for API error records


def api_error_records(value: Any) -> list[dict[str, Any]]:
    """Every object marked ``isApiErrorMessage: true`` anywhere inside ``value``."""
    found: list[dict[str, Any]] = []
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if item.get("isApiErrorMessage") is True:
                found.append(item)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return found


def _tail_records(path: Path, lines: int) -> list[dict[str, Any]]:
    """API error records the step ended with: those after its last successful assistant turn
    (an earlier 429 the step recovered from does not count), within the last ``lines`` lines."""
    try:
        with path.open("rb") as fh:
            tail = deque(fh, maxlen=lines)
    except OSError:
        return []
    records: list[dict[str, Any]] = []
    for raw in tail:
        try:
            line = json.loads(raw)
        except ValueError:
            continue
        found = api_error_records(line)
        if found:
            records += found
        elif isinstance(line, dict) and line.get("type") == "assistant":
            records = []  # a later successful turn: the step recovered from earlier errors
    return records


def _failed_steps(
    steps: list[journal.JournalStep], workflow_return: Any
) -> list[journal.JournalStep]:
    """The steps that may have hit a limit: the one a ``stage_failed`` return names and every
    step that ended without a result."""
    named = None
    if isinstance(workflow_return, dict) and workflow_return.get("status") == "stage_failed":
        named = workflow_return.get("stage")
    return [s for s in steps if s.label == named or s.status != "completed" or s.result is None]


def _stage_records(
    engine: "TaskEngine",
    task: Task,
    run: tuple[str, Path | None],
    workflow_return: Any,
    started: set[str],
) -> list[dict[str, Any]]:
    cwd, session, home = task.cwd or ".", task.session_id or "", engine.settings.claude_home
    run_id, tdir = run
    records: list[dict[str, Any]] = []
    for step in _failed_steps(
        journal.read_steps(cwd, session, run_id, home, tdir), workflow_return
    ):
        if step.agent_id and step.label in started:
            path = journal.step_transcript_path(cwd, session, run_id, home, step.agent_id, tdir)
            records += _tail_records(path, STAGE_RECORD_LINES)
    return records


async def stage_error_records(
    engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any], workflow_return: Any
) -> list[dict[str, Any]]:
    """Structured API error records in the transcripts of the run's failed or unfinished steps
    that were started in this attempt (read off the loop, bounded); ``classify`` takes a 429 /
    usage-limit one as a rate limit. A step replayed from cache keeps the record of an earlier
    attempt's limit: it never counts again (the outcome stays ``stage_failed``)."""
    task = attempt.task
    run = await _run_of(attempt, state)
    if run is None or not task.session_id:
        return []
    started = await started_in_attempt(task.id, attempt.n)
    try:
        return await asyncio.to_thread(_stage_records, engine, task, run, workflow_return, started)
    except Exception:
        _log.exception("could not read stage transcripts", extra={"task_id": task.id})
        return []


async def started_in_attempt(task_id: str, n: int) -> set[str]:
    """Labels of the steps whose started count grew in attempt ``n`` (the step mirror's rows:
    ``prev_started_count`` is the count before the attempt)."""
    async with get_db().execute(
        "SELECT label FROM steps WHERE task_id = ? AND attempt = ? "
        "AND started_count > prev_started_count",
        (task_id, n),
    ) as cur:
        return {str(r["label"]) for r in await cur.fetchall()}


# -- step mirror -------------------------------------------------------------------------------
def _git(worktree: str | None) -> tuple[str | None, str | None]:
    if not worktree or not Path(worktree).is_dir():
        return None, None
    out: list[str | None] = []
    for args in (["rev-parse", "--abbrev-ref", "HEAD"], ["rev-parse", "HEAD"]):
        try:
            done = subprocess.run(
                ["git", "-C", worktree, *args], capture_output=True, text=True,
                timeout=GIT_TIMEOUT_S, check=False,
            )  # fmt: skip
        except (OSError, subprocess.SubprocessError):
            return None, None
        out.append((done.stdout.strip() or None) if done.returncode == 0 else None)
    return out[0], out[1]


async def maybe_mirror(engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any]) -> None:
    now = time.monotonic()
    if now - state.get("mirrored_at", 0.0) >= MIRROR_INTERVAL_S:
        state["mirrored_at"] = now
        await mirror(engine, attempt, state)


async def mirror(
    engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any], final: bool = False
) -> None:
    """Bookkeeping only: a failure is logged and never changes the task outcome."""
    try:
        await _mirror(engine, attempt, state, final)
    except Exception:
        _log.exception("step mirror failed", extra={"task_id": attempt.task.id})


async def _mirror(
    engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any], final: bool
) -> None:
    task = attempt.task
    run = await _run_of(attempt, state)
    if run is None or not task.session_id:
        return
    run_id, tdir = run
    cwd, session, home = task.cwd or ".", task.session_id, engine.settings.claude_home
    steps = await asyncio.to_thread(journal.read_steps, cwd, session, run_id, home, tdir)
    progress = await asyncio.to_thread(journal.read_progress, cwd, session, run_id, home, tdir)
    async with get_db().execute("SELECT * FROM steps WHERE task_id = ?", (task.id,)) as cur:
        rows = {r["label"]: dict(r) for r in await cur.fetchall()}
    done = [s for s in steps if s.status == "completed"]
    newly_done = [s for s in done if (rows.get(s.label) or {}).get("status") != "completed"]
    git = await asyncio.to_thread(_git, task.worktree) if newly_done else (None, None)
    where = _RunPlace(cwd, session, run_id, home, tdir)
    measured = await asyncio.to_thread(
        _measured, engine.runstore, task.id, steps, rows, where, final
    )
    changed = await asyncio.to_thread(_files_changed, task.worktree, newly_done, rows, git[1])
    async with write_tx() as db:
        for step in steps:
            found = measured.get(step.label, Measured((None, None), None, None))
            row = rows.get(step.label)
            written = await _upsert(
                db, task.id, attempt.n, step, row, progress, git, found.times
            )  # fmt: skip
            for type_, data in stage_events(step, row, written, found.times[0]):
                await engine._event(db, task.id, type_, data)
            if found.usage is not None and step.agent_id is not None:
                n = await stage_measure.attempt_at(db, task.id, found.ended, attempt.n)
                at = stage_measure.StepRun(task.id, step.label, run_id, n)
                await stage_measure.write_usage(db, at, step.agent_id, found.usage)
        for label, files in changed.items():
            await db.execute(
                "UPDATE steps SET files_changed_json = ? WHERE task_id = ? AND label = ?",
                (None if files is None else json.dumps(files), task.id, label),
            )
    if final or len(done) > state.get("copied", 0):
        state["copied"] = len(done)
        await asyncio.to_thread(
            engine.runstore.copy_transcripts, task.id, cwd, session, run_id, home, tdir, final
        )
    await _phase_changed(engine, task.id, steps, state)


@dataclasses.dataclass(frozen=True)
class Measured:
    """A step's transcript readings: real ``times`` (``(None, None)`` when already final),
    billed ``usage`` once completed, and the transcript's last record time (``ended``), which
    places the agent run in an attempt."""

    times: tuple[float | None, float | None]
    usage: dict[str, journal.StageModelUsage] | None
    ended: float | None


@dataclasses.dataclass(frozen=True)
class _RunPlace:
    """Where a workflow run's transcripts are (``journal.step_source``)."""

    cwd: str
    session: str
    run_id: str
    home: Path
    tdir: Path | None


def _measured(
    runstore: "RunStore",
    task_id: str,
    steps: list[journal.JournalStep],
    rows: dict[str, dict[str, Any]],
    where: _RunPlace,
    final_mirror: bool,
) -> dict[str, Measured]:
    """Per step, its real ``(start, end)`` and, once completed, its billed usage per model, from
    its transcript (see ``journal.step_source``: live first, the run-store copy as fallback).
    A step already mirrored as completed by the same agent keeps its times and usage (final: its
    transcript is not reopened), except on the attempt's final mirror, which re-reads a
    completed step's usage once: the journal can mark a step completed before its transcript's
    last turn is written."""
    out: dict[str, Measured] = {}
    for step in steps:
        row = rows.get(step.label)
        final = (
            row is not None
            and row["status"] == "completed"
            and row["finished_at"] is not None
            and step.status == "completed"
            and row["agent_id"] == step.agent_id
        )
        if step.agent_id is None or (final and not final_mirror):
            continue
        source = journal.step_source(
            where.cwd, where.session, where.run_id, where.home, step.agent_id,
            runstore.step_copy_path(task_id, step.label), where.tdir,
        )  # fmt: skip
        read = journal.step_times(source)
        usage = journal.step_usage(source, step.agent_id) if step.status == "completed" else None
        out[step.label] = Measured((None, None) if final else read, usage, read[1])
    return out


def _files_changed(
    worktree: str | None,
    newly_done: list[journal.JournalStep],
    rows: dict[str, dict[str, Any]],
    head: str | None,
) -> dict[str, Any]:
    """Per newly completed step, the files the worktree head moved by since the latest earlier
    completed step with a recorded head (by ``finished_at``): a diff of heads between mirror
    observations, not of the step's own commits (steps completing in one tick, or parallel ones,
    share the diff); None when not knowable (the first completed step has no earlier head)."""
    if not newly_done:
        return {}
    earlier = sorted(
        (r for r in rows.values() if r["status"] == "completed" and r["git_head"]),
        key=lambda r: r["finished_at"] or 0.0,
    )
    before = earlier[-1]["git_head"] if earlier else None
    files = stage_measure.files_changed(worktree, before, head)
    return {step.label: files for step in newly_done}


async def _upsert(
    db: aiosqlite.Connection,
    task_id: str,
    n: int,
    step: journal.JournalStep,
    row: dict[str, Any] | None,
    progress: list[dict[str, Any]],
    git: tuple[str | None, str | None],
    measured: tuple[float | None, float | None] = (None, None),
) -> dict[str, Any]:
    """Upsert one step row; returns what the row now says (``cached``, ``started_at``,
    ``finished_at``, ``agent_type``, ``model``) for the stage events."""
    # The started count left by earlier attempts is persisted, so `cached` stays right across
    # an engine restart: a completed step whose count did not grow in this attempt was replayed.
    if row is None:
        prev = 0
    elif row["attempt"] != n:
        prev = int(row["started_count"])
    else:
        prev = int(row["prev_started_count"])
    cached = journal.cached_label(prev, step, progress)
    now = time.time()
    completed = step.status == "completed"
    # Transcript times are real; the observation time is the fallback (the mirror runs every
    # MIRROR_INTERVAL_S, so a fast step observed that way can start and end in the same tick).
    measured_start, measured_end = measured
    started_at = measured_start or (row["started_at"] if row is not None else now)
    finished_at = None
    if completed:
        was_done = row is not None and row["status"] == "completed"
        observed_end = row["finished_at"] if was_done and row is not None else now
        finished_at = measured_end or observed_end
    branch, head = (
        git
        if completed and (row is None or row["status"] != "completed")
        else ((row["git_branch"], row["git_head"]) if row is not None else (None, None))
    )
    # A cached (replayed) entry or a missing number keeps the stored one: it belongs to the run
    # that measured the stage.
    entry = next((e for e in progress if e.get("label") == step.label), None)
    numbers = stage_numbers(entry)
    await db.execute(
        "INSERT INTO steps (task_id, label, phase, status, started_count, cached, agent_id, "
        "git_branch, git_head, started_at, finished_at, attempt, prev_started_count, "
        "agent_type, model, stage_tokens, tool_calls, last_tool, duration_s, last_progress_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (task_id, label) DO UPDATE SET "
        "phase = excluded.phase, status = excluded.status, started_count = excluded.started_count, "
        "cached = excluded.cached, agent_id = excluded.agent_id, git_branch = excluded.git_branch, "
        "git_head = excluded.git_head, started_at = excluded.started_at, "
        "finished_at = excluded.finished_at, "
        "attempt = excluded.attempt, prev_started_count = excluded.prev_started_count, "
        "agent_type = coalesce(excluded.agent_type, steps.agent_type), "
        "model = coalesce(excluded.model, steps.model), "
        "stage_tokens = coalesce(excluded.stage_tokens, steps.stage_tokens), "
        "tool_calls = coalesce(excluded.tool_calls, steps.tool_calls), "
        "last_tool = coalesce(excluded.last_tool, steps.last_tool), "
        "duration_s = coalesce(excluded.duration_s, steps.duration_s), "
        "last_progress_at = coalesce(excluded.last_progress_at, steps.last_progress_at)",
        (
            task_id, step.label, step.phase or phase_of(step.label), step.status,
            step.started_count, int(cached), step.agent_id, branch, head,
            started_at, finished_at, n, prev, numbers.agent_type, numbers.model,
            numbers.stage_tokens, numbers.tool_calls, numbers.last_tool, numbers.duration_s,
            numbers.last_progress_at,
        ),
    )  # fmt: skip
    return {
        "cached": cached, "started_at": started_at, "finished_at": finished_at,
        "agent_type": numbers.agent_type or (row or {}).get("agent_type"),
        "model": numbers.model or (row or {}).get("model"),
    }  # fmt: skip


def stage_events(
    step: journal.JournalStep,
    row: dict[str, Any] | None,
    written: dict[str, Any],
    measured_start: float | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """``stage_started`` / ``stage_finished`` for a real status transition of ``step`` (its row
    before this mirror tick is ``row``): a new label or a restart (its started count grew)
    started; a step newly completed, or restarted and completed within one tick, finished. A
    replay served from cache did not run: none. A restart's ``started_at`` is its transcript's
    (``measured_start``), null until readable: the row still holds the previous run's."""
    if written["cached"]:
        return []
    base = {"label": step.label, "agent_type": written["agent_type"], "model": written["model"]}
    restarted = row is not None and step.started_count > int(row["started_count"])
    started_at = measured_start if restarted else written["started_at"]
    found: list[tuple[str, dict[str, Any]]] = []
    if row is None or restarted:
        found.append(("stage_started", {**base, "started_at": started_at}))
    if step.status == "completed" and (row is None or restarted or row["status"] != "completed"):
        finished = {"status": step.status, "started_at": started_at,
                    "finished_at": written["finished_at"]}  # fmt: skip
        found.append(("stage_finished", {**base, **finished}))
    return found


def _current(steps: list[journal.JournalStep]) -> journal.JournalStep | None:
    running = [s for s in steps if s.status == "running"]
    return running[-1] if running else (steps[-1] if steps else None)


async def _phase_changed(
    engine: "TaskEngine", task_id: str, steps: list[journal.JournalStep], state: dict[str, Any]
) -> None:
    step = _current(steps)
    phase = (step.phase or phase_of(step.label)) if step is not None else None
    if phase is None or phase == state.get("phase"):
        return
    state["phase"] = phase
    notifier = engine.notifier
    if notifier is None:
        return
    try:
        result = notifier.phase(task_id, phase)
        if inspect.isawaitable(result):
            await result
    except Exception:
        _log.exception("notifier phase failed", extra={"task_id": task_id})


# -- the attempt's end ---------------------------------------------------------------------------
async def finish(engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any]) -> Any:
    """Final mirror (every transcript copied) and the workflow's return value, a
    ``ScriptFailure`` (the run file says the script failed) or None."""
    await mirror(engine, attempt, state, final=True)
    value = await read_return(engine, attempt, state)
    if value is not None:
        return value
    run = await _run_of(attempt, state)
    task = attempt.task
    if run is None or not task.session_id:
        return None
    try:
        failure = await asyncio.to_thread(
            journal.read_failure, task.cwd or ".", task.session_id, run[0],
            engine.settings.claude_home, run[1], attempt.started_at,
        )  # fmt: skip
    except Exception:
        _log.exception("could not read the workflow run file", extra={"task_id": task.id})
        return None
    return None if failure is None else ScriptFailure(*failure)


async def read_return(engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any]) -> Any:
    """The return value in this attempt's run file, or None (also on a read error)."""
    run = await _run_of(attempt, state)
    task = attempt.task
    if run is None or not task.session_id:
        return None
    try:
        # Only a run file written during this attempt holds its return: on a resume the file
        # still carries the previous attempt's return until the workflow finishes again.
        return await asyncio.to_thread(
            journal.read_return, task.cwd or ".", task.session_id, run[0],
            engine.settings.claude_home, run[1], attempt.started_at,
        )  # fmt: skip
    except Exception:
        _log.exception("could not read the workflow return", extra={"task_id": task.id})
        return None


async def has_valid_return(engine: "TaskEngine", attempt: "Attempt", state: dict[str, Any]) -> bool:
    return valid_return(await read_return(engine, attempt, state))


def _output(text: str, value: dict[str, Any], usage: Usage | None) -> str:
    """The task output; ``usage`` is the task's (every attempt), None when unknown."""
    tokens = usage.tokens if usage else None
    cost = usage.cost_usd if usage else None
    out = {"text": text, "tokens_used": tokens, "cost_usd": cost, "return": value}
    return json.dumps(out, ensure_ascii=False)


def _detail(engine: "TaskEngine", text: str) -> str:
    """Redacted first, then bounded (a cut never leaves a partial secret unmasked)."""
    return engine.redactor.redact_text(text)[:ERROR_DETAIL_CHARS]


def failure_event(
    engine: "TaskEngine", workflow_return: Any, outcome: Outcome | None
) -> tuple[str, dict[str, Any]] | None:
    """The event explaining a fatal outcome caused by the run itself (script exception, or a
    return without a known status), redacted; None otherwise."""
    if outcome is not Outcome.fatal:  # e.g. a violation or a rate limit explains it better
        return None
    if isinstance(workflow_return, ScriptFailure):
        data = {"status": workflow_return.status, "error": _detail(engine, workflow_return.error)}
        return "workflow_script_failed", data
    if workflow_return is not None and not valid_return(workflow_return):
        status = workflow_return.get("status") if isinstance(workflow_return, dict) else None
        return "invalid_workflow_return", {"status": _detail(engine, repr(status))}
    return None


def apply_return(
    engine: "TaskEngine",
    update: dict[str, Any],
    workflow_return: Any,
    usage: Usage | None,
    outcome: Outcome | None = None,
) -> bool:
    """Fill the task update from the workflow return: output (completed, cancelled,
    needs_human), the question (needs_human), the stage failure, the script failure or the
    invalid return (failed). ``usage`` is the task's usage, every attempt (None: unknown). True
    when the update's error was set here."""
    if update["status"] == "failed":
        event = failure_event(engine, workflow_return, outcome)
        if event is not None:
            name, data = event
            if name == "workflow_script_failed":
                update["error"] = f"workflow script failed (run status {data['status']!r}): " + (
                    data["error"] or "no error text in the run file"
                )
            else:
                update["error"] = f"invalid workflow return (status={data['status']})"
            return True
    if not isinstance(workflow_return, dict):
        return False
    status = update["status"]
    if status == "needs_human":
        kind, stage = workflow_return.get("kind"), workflow_return.get("stage")
        if kind not in KINDS or not isinstance(stage, str) or not stage.strip():
            # Nobody could answer it: no ask point to attach the answer to.
            update.update(status="failed", completed_at=time.time())
            shown = engine.redactor.redact_text(f"stage={stage!r}, kind={kind!r}")
            update["error"] = f"invalid needs_human return ({shown[:ERROR_DETAIL_CHARS]})"
            update["output_json"] = _output("", workflow_return, usage)
            return True
        text = workflow_return.get("question")
        text = text if isinstance(text, str) else ""
        question = {
            "stage": workflow_return.get("stage"),
            "kind": kind,
            "mode": answer_mode(kind),
            "question": text,
        }
        update["question_json"] = json.dumps(question, ensure_ascii=False)
        update["output_json"] = _output(text, workflow_return, usage)
    elif status in ("completed", "cancelled"):
        text = json.dumps(workflow_return, ensure_ascii=False, sort_keys=True)
        update["output_json"] = _output(text, workflow_return, usage)
    elif status == "failed" and workflow_return.get("status") == "stage_failed":
        stage = workflow_return.get("stage")
        detail = workflow_return.get("error")
        detail = detail if isinstance(detail, str) and detail else "stage returned no result"
        detail = engine.redactor.redact_text(detail[-ERROR_DETAIL_CHARS:])
        update["error"] = f"stage_failed at {stage}: {detail}"
        return True
    return False


# -- read side ---------------------------------------------------------------------------------
def _iso(ts: float | None) -> str | None:
    return None if ts is None else iso(ts)


def served_step_status(step_status: str, task_status: str) -> str:
    """A step's status as served (status, list, debug): one still ``running`` while the task is
    not running (queued for a retry, between launches) is ``interrupted``."""
    if step_status == "running" and task_status != "running":
        return "interrupted"
    return step_status


def step_items(
    found: list[Any], task_status: str, timing: Mapping[str, str] | None = None
) -> list[dict[str, Any]]:
    """Mirrored step rows as served, the status by ``served_step_status``; the row is unchanged.
    ``timing`` maps a label to ``measured``/``observed`` (``journal.step_timing``); a label not
    in it is ``observed``."""
    timing = timing or {}
    return [
        {
            "label": r["label"], "phase": r["phase"],
            "status": served_step_status(r["status"], task_status),
            "started_count": r["started_count"], "cached": bool(r["cached"]),
            "agent_id": r["agent_id"], "git_branch": r["git_branch"], "git_head": r["git_head"],
            "started_at": _iso(r["started_at"]),
            "finished_at": _iso(r["finished_at"]),
            "timing": timing.get(r["label"], "observed"),
        }
        for r in found
    ]  # fmt: skip


def step_source_of(runstore: "RunStore", task: Task, home: Path) -> Callable[[Any], Path]:
    """Maps a mirrored step row to its transcript (``journal.step_source``) for ``task``."""
    return lambda r: journal.step_source(
        task.cwd or ".", task.session_id, task.run_id, home, r["agent_id"],
        runstore.step_copy_path(task.id, r["label"]),
    )  # fmt: skip


async def step_list(
    task_id: str, task_status: str, source_of: Callable[[Any], Path] | None = None
) -> list[dict[str, Any]]:
    """The task's steps as served; with ``source_of`` each step carries its ``timing``."""
    async with get_db().execute(
        "SELECT * FROM steps WHERE task_id = ? ORDER BY started_at, rowid", (task_id,)
    ) as cur:
        found = list(await cur.fetchall())
    timing = await asyncio.to_thread(_timings, source_of, found) if source_of else None
    return step_items(found, task_status, timing)


def _timings(source_of: Callable[[Any], Path], found: list[Any]) -> dict[str, str]:
    return {r["label"]: journal.step_timing(source_of(r)) for r in found}


def _round_of(label: str) -> str | None:
    """The ``r<n>`` token of a step label (``review-r1-x`` -> ``1``)."""
    for token in label.split("@", 1)[0].split("-"):
        if len(token) > 1 and token[0] == "r" and token[1:].isdigit():
            return token[1:]
    return None


def current_stage(task: Task, steps: list[dict[str, Any]]) -> str | None:
    if task.status == "needs_human" and task.question_json:
        question = json.loads(task.question_json)
        if isinstance(question, dict) and isinstance(question.get("stage"), str):
            return str(question["stage"])
    # Unfinished steps: running, or interrupted while the task waits between attempts.
    active = [s for s in steps if s["status"] in ("running", "interrupted")]
    if len(active) > 1 and len({s["phase"] for s in active}) == 1 and active[0]["phase"]:
        rounds = {_round_of(s["label"]) for s in active}  # parallel steps: the phase, not one
        round_ = rounds.pop() if len(rounds) == 1 else None
        return f"{active[0]['phase']} r{round_}" if round_ else str(active[0]["phase"])
    if active:
        return str(active[-1]["label"])
    return str(steps[-1]["label"]) if steps else None
