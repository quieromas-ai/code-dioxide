"""The single decider: what happens to a task after one of its attempts ended.

``next_action`` is pure (no clock, database or filesystem: ``now`` is a parameter) so the policy
can be tested as a table. The engine applies the columns ``task_update`` derives from the action
in the same write transaction that closes the attempt.

Policy (the kill reason is read first, then the attempt's outcome):

* kill ``cancel`` -> ``Done("cancelled")``; kill ``timeout`` -> ``Fail``; kill ``stall`` -> as
  ``transient``; a call kill (``call_mismatch``, ``second_workflow_call``, ``parent_violation``,
  a dropped resume id is a ``call_mismatch``) -> corrective resume; a kill in ``RETURN_DECIDES``
  -> the outcome decides, as without a kill.
* ``success`` -> ``Done("completed")``; ``needs_human`` -> ``WaitHuman``;
  ``cancelled_by_workflow`` -> ``Done("cancelled")``; ``fatal`` (a workflow script exception or
  an unknown return) and ``auth_failed`` (the session, or a stage, could not authenticate) ->
  ``Fail`` at once.
* ``transient`` -> resume after an exponential backoff while ``resumes < max_resumes +
  max_retries``; ``rate_limited`` -> resume at the reset time (absent or past: the default wait),
  every wait at least ``rate_limit_min_wait_s`` and charged to ``rate_limit_waited_s``, not
  counted as a resume; a wait beyond what is left of ``rate_limit_max_wait_s`` fails at once.
  Before that, once per task: when ``model_fallback`` maps a model the agents render with, the
  task resumes after ``rate_limit_min_wait_s`` (not charged) with those models replaced;
  ``stage_failed`` -> one resume, then ``Fail``; ``call_mismatch`` / ``parent_violation`` ->
  resume with the corrective prompt while ``call_retries < call_mismatch_retries``;
  ``resume_refused`` -> ``Fresh`` (a new session and a new run with no answers; counted as a
  resume), except while the run's latest answer is still pending (``pending_answer``: the run
  never consumed it): a fresh run would drop that human decision, so the task fails instead
  (``Fail`` with ``answer_k``; branch and worktree are kept).

An outcome or kill reason the policy does not know raises ``ValueError``: nothing falls through.
"""

import dataclasses
import json
import re
from dataclasses import dataclass, field
from typing import Any, assert_never

from src.classify import Outcome
from src.config import Settings
from src.journal import JournalStep
from src.tasks import CAUSE_FRESH, RETURN_DECIDES, Task

CANCEL = "cancel"
STALL = "stall"  # set by the watchdog (no activity for stall_timeout_s)
TIMEOUT = "timeout"  # set by the watchdog (attempt over task_timeout_s)
CALL_KILLS = frozenset({"call_mismatch", "second_workflow_call", "parent_violation"})
KILL_REASONS = frozenset({CANCEL, STALL, TIMEOUT, *CALL_KILLS, *RETURN_DECIDES})
STAGE_FAILED_RESUMES = 1

# Why a task is resumed (the ``retry_scheduled`` event).
TRANSIENT = "transient"
RATE_LIMITED = "rate_limited"
RATE_LIMITED_FALLBACK = "rate_limited_fallback"  # resumed at once on the user-mapped models
STAGE_FAILED = "stage_failed"
CORRECTIVE = "corrective"


@dataclass(frozen=True)
class Counters:
    """The task's recovery counters (columns; they survive a restart)."""

    resumes: int
    call_retries: int
    stage_retries: int
    rate_limit_waited_s: float

    @classmethod
    def of(cls, task: Task) -> "Counters":
        return cls(task.resumes, task.call_retries, task.stage_retries, task.rate_limit_waited_s)


@dataclass(frozen=True)
class Resume:
    """Relaunch in the same session (and the same workflow run) at ``at`` (epoch s)."""

    at: float
    why: str
    counters: Counters
    corrective: bool = False
    # The models replaced from this relaunch on (``model_fallback``); empty: none.
    model_fallback: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Fresh:
    """Relaunch now in a new session; the plan is pinned first. Branch, worktree, base stay."""

    counters: Counters


@dataclass(frozen=True)
class Fail:
    reason: str
    answer_k: int | None = None  # a refused resume left this answer unapplied


@dataclass(frozen=True)
class Done:
    status: str  # completed | cancelled


@dataclass(frozen=True)
class WaitHuman:
    pass


Action = Resume | Fresh | Fail | Done | WaitHuman


def backoff_s(settings: Settings, resumes: int) -> float:
    """``min(base * 2^resumes, max)``: 30 s, 60 s, 120 s ... capped at 600 s by default."""
    delay = settings.retry_backoff_s * 2.0 ** min(resumes, 32)
    return float(min(delay, settings.retry_backoff_max_s))


def _base(outcome: Outcome, kill_reason: str | None) -> str:
    return f"{outcome.value}: {kill_reason}" if kill_reason else outcome.value


def _budget(task: Task, settings: Settings) -> int:
    return settings.max_resumes + task.max_retries


def _transient(task: Task, settings: Settings, now: float, error: str) -> Action:
    counters = Counters.of(task)
    if counters.resumes >= _budget(task, settings):
        return Fail(error)
    nxt = dataclasses.replace(counters, resumes=counters.resumes + 1)
    return Resume(now + backoff_s(settings, counters.resumes), TRANSIENT, nxt)


def fallback_for(settings: Settings, models: frozenset[str]) -> dict[str, str]:
    """The part of ``model_fallback`` that applies to agents rendered with ``models``."""
    return {m: r for m, r in settings.model_fallback.items() if m in models and r != m}


def _rate_limited(
    task: Task, settings: Settings, now: float, reset_at: float | None, models: frozenset[str]
) -> Action:
    counters = Counters.of(task)
    swap = {} if task.model_fallback_applied else fallback_for(settings, models)
    if swap:  # the mapped models have limits of their own: no reset to wait for, no charge
        at = now + settings.rate_limit_min_wait_s
        return Resume(at, RATE_LIMITED_FALLBACK, counters, model_fallback=swap)
    remaining = settings.rate_limit_max_wait_s - counters.rate_limit_waited_s
    known = reset_at is not None and reset_at > now
    wanted = (
        reset_at - now if known and reset_at is not None else settings.rate_limit_default_wait_s
    )
    wait = max(float(wanted), settings.rate_limit_min_wait_s)
    if wait > remaining:  # never relaunch before the reset: the limit would hit again
        waited = f"{counters.rate_limit_waited_s:.0f}"
        return Fail(
            f"rate_limited: a wait of {wait:.0f}s exceeds the wait budget "
            f"(waited {waited}s of {settings.rate_limit_max_wait_s}s)"
        )
    nxt = dataclasses.replace(counters, rate_limit_waited_s=counters.rate_limit_waited_s + wait)
    return Resume(now + wait, RATE_LIMITED, nxt)


def _stage_failed(task: Task, now: float, error: str) -> Action:
    counters = Counters.of(task)
    if counters.stage_retries >= STAGE_FAILED_RESUMES:
        return Fail(error)
    nxt = dataclasses.replace(counters, stage_retries=counters.stage_retries + 1)
    return Resume(now, STAGE_FAILED, nxt)


def _corrective(task: Task, settings: Settings, now: float, error: str) -> Action:
    counters = Counters.of(task)
    if counters.call_retries >= settings.call_mismatch_retries:
        return Fail(error)
    nxt = dataclasses.replace(counters, call_retries=counters.call_retries + 1)
    return Resume(now, CORRECTIVE, nxt, corrective=True)


def _fresh(task: Task, settings: Settings, error: str) -> Action:
    counters = Counters.of(task)
    if counters.resumes >= _budget(task, settings):
        return Fail(error)
    return Fresh(dataclasses.replace(counters, resumes=counters.resumes + 1))


def _unapplied(task: Task, k: int) -> Action:
    return Fail(
        f"resume refused while answer k={k} was pending; it could not be applied (branch "
        f"{task.branch}, worktree {task.worktree} are kept)",
        answer_k=k,
    )


def next_action(
    outcome: Outcome,
    task: Task,
    settings: Settings,
    now: float,
    reset_at: float | None,
    pending_answer: int | None = None,
    models: frozenset[str] = frozenset(),
) -> Action:
    """What happens after the attempt that ended with ``outcome`` (``task``: the current row,
    with its kill reason and counters; ``reset_at``: a structured rate-limit reset, epoch s;
    ``pending_answer``: the k of the run's latest answer when the run has not consumed it,
    computed by the engine from the run's journal, ``pending_answer()``; ``models``: the models
    the task's agents are rendered with, before any fallback)."""
    kill = task.kill_reason
    error = _base(outcome, kill)
    if kill is not None and kill not in KILL_REASONS:
        raise ValueError(f"no recovery policy for kill reason {kill!r}")
    if kill == CANCEL:
        return Done("cancelled")
    if kill == TIMEOUT:
        return Fail(error)
    if kill == STALL:
        return _transient(task, settings, now, error)
    if kill in CALL_KILLS:
        return _corrective(task, settings, now, error)
    match outcome:  # no kill reason, or one after which the workflow's return decides
        case Outcome.success:
            return Done("completed")
        case Outcome.needs_human:
            return WaitHuman()
        case Outcome.cancelled_by_workflow:
            return Done("cancelled")
        case Outcome.rate_limited:
            return _rate_limited(task, settings, now, reset_at, models)
        case Outcome.transient:
            return _transient(task, settings, now, error)
        case Outcome.stage_failed:
            return _stage_failed(task, now, error)
        case Outcome.resume_refused:
            if pending_answer is not None:
                return _unapplied(task, pending_answer)
            return _fresh(task, settings, error)
        case Outcome.call_mismatch | Outcome.parent_violation:
            return _corrective(task, settings, now, error)
        case Outcome.fatal | Outcome.auth_failed:
            return Fail(error)
        case _:
            assert_never(outcome)


def _counter_columns(counters: Counters) -> dict[str, Any]:
    return {
        "resumes": counters.resumes,
        "call_retries": counters.call_retries,
        "stage_retries": counters.stage_retries,
        "rate_limit_waited_s": counters.rate_limit_waited_s,
    }


def task_update(action: Action, task: Task, now: float, new_session_id: str) -> dict[str, Any]:
    """The ``tasks`` columns for ``action`` (``new_session_id`` is used only by ``Fresh``).

    A resume of an attempt that never spawned (no pid stored) repeats that attempt's mode: no
    session exists to resume yet. Its cause is this action's ``why`` whenever the relaunch
    resumes (so ``corrective`` and the cause agree); a repeated first or fresh launch keeps its
    own."""
    if isinstance(action, Done):
        return {"status": action.status, "error": None, "completed_at": now, "corrective": 0}
    if isinstance(action, Fail):
        return {"status": "failed", "error": action.reason, "completed_at": now, "corrective": 0}
    if isinstance(action, WaitHuman):
        return {"status": "needs_human", "error": None, "corrective": 0}
    if isinstance(action, Resume):
        spawned = task.pid is not None
        mode = "resume" if spawned else (task.resume_mode or "first")
        update = {
            "status": "queued", "error": None, "resume_mode": mode,
            "resume_cause": action.why if mode == "resume" else task.resume_cause,
            "next_attempt_at": action.at, "corrective": int(action.corrective),
            **_counter_columns(action.counters),
        }  # fmt: skip
        if action.model_fallback:
            update["model_fallback_applied"] = 1
        return update
    if isinstance(action, Fresh):
        return {
            "status": "queued", "error": None, "resume_mode": "fresh", "resume_cause": CAUSE_FRESH,
            "session_id": new_session_id, "run_id": None, "next_attempt_at": None,
            "corrective": 0, **_counter_columns(action.counters),
        }  # fmt: skip
    assert_never(action)


def describe(action: Action) -> tuple[str, dict[str, Any]] | None:
    """The event recording a relaunch decision or an answer a refused resume left unapplied;
    None for the other actions (a fresh start's event, ``fresh_start``, is written with the
    dropped answers by the engine)."""
    if isinstance(action, Fail):
        return None if action.answer_k is None else ("answer_not_applied", {"k": action.answer_k})
    if isinstance(action, Resume):
        data = {"why": action.why, "at": action.at, "corrective": action.corrective}
        return "retry_scheduled", {**data, **_counter_columns(action.counters)}
    if isinstance(action, Fresh):
        return "fresh_start", _counter_columns(action.counters)
    return None


# -- a refused resume: was the latest answer consumed? -------------------------------------------
def answer_consumed(steps: list[tuple[str, str]], k: int) -> bool:
    """The run consumed answer ``k`` (``steps``: the journal's ``(label, status)``): a
    *completed* step ``interpret-<ask>-<k>`` (a gate answer; one that only started applied
    nothing) or any step in its epoch, ``<label>@a<k>`` (a rerun answer, and every step after
    any consumption)."""
    interpret = re.compile(rf"interpret-.+-{k}")
    epoch = re.compile(rf".+@a{k}")
    return any(
        (interpret.fullmatch(label) and status == "completed") or epoch.fullmatch(label)
        for label, status in steps
    )


def pending_answer(args: dict[str, Any], steps: list[tuple[str, str]] | None) -> int | None:
    """The k of the run's latest answer (``args.answers``) when the run has not consumed it;
    ``steps``: the run's journal steps as ``(label, status)``, None when the journal could not
    be read (then any answer counts as pending). None: no answer, or the latest one was
    consumed."""
    answers = args.get("answers") or []
    if not answers or not isinstance(answers[-1], dict):
        return None
    k = int(answers[-1]["k"])
    if steps is not None and answer_consumed(steps, k):
        return None
    return k


# -- fresh start: the run's args and the plan to pin ---------------------------------------------
_PLAN = re.compile(r"plan(?:@a\d+)?")
_SETUP = re.compile(r"setup(?:@a\d+)?")


def _plan_shaped(result: Any) -> bool:
    """A planner result the script accepts as a plan: non-empty ``steps``, no question."""
    if not isinstance(result, dict) or result.get("needs_human"):
        return False
    steps = result.get("steps")
    return isinstance(steps, list) and len(steps) > 0


@dataclass(frozen=True)
class PlanPin:
    """What a fresh start does with the plan: ``plan`` (label, result) to pin, or None; and
    whether the run holds any plan step (it planned, rather than running from a pin)."""

    plan: tuple[str, Any] | None
    planned: bool
    read: bool = True  # False: the run's plan state could not be read (``NO_PLAN_READ``)


NO_PLAN_READ = PlanPin(None, False, read=False)  # nothing known about the run's plan


def pinnable_plan(steps: list[JournalStep], plan_gate: bool) -> tuple[str, Any] | None:
    """The plan a fresh start may pin, as ``(label, result)``, or None.

    Only the run's latest plan step (journal order) is considered: an older plan was superseded
    by a re-plan (an ``amend``) and is never pinned in its place. It qualifies when it is
    plan-shaped (non-empty ``steps``, no ``needs_human``) and the run accepted it: the plan gate
    was off, or a completed ``setup`` step comes after it (the run moved past the gate)."""
    plans = [i for i, step in enumerate(steps) if _PLAN.fullmatch(step.label)]
    if not plans:
        return None
    i = plans[-1]
    step = steps[i]
    if step.status != "completed" or not _plan_shaped(step.result):
        return None
    accepted = not plan_gate or any(
        _SETUP.fullmatch(later.label) and later.status == "completed" for later in steps[i + 1 :]
    )
    return (step.label, step.result) if accepted else None


def plan_pin(steps: list[JournalStep], plan_gate: bool) -> PlanPin:
    planned = any(_PLAN.fullmatch(step.label) for step in steps)
    return PlanPin(pinnable_plan(steps, plan_gate), planned)


def fresh_args(args_json: str | None, pin: PlanPin) -> tuple[str, int]:
    """The args of a fresh run: no answers (they belong to the asks of the run that raised them;
    their files and rows stay as history) and ``plan_pinned``: true when a plan is pinned; false
    when the run planned but nothing is pinnable (the fresh run plans again, and asks again with
    the gate on); unchanged when the run never planned (it ran from the pin).

    When the plan state could not be read (``NO_PLAN_READ``): with no answers the run cannot
    have re-planned, so a pin is kept; with answers it may have (an amend), so the fresh run
    plans again (``plan_pinned`` false) rather than implement a superseded plan.
    Also returns the number of answers dropped."""
    args = json.loads(args_json) if args_json else {}
    dropped = len(args.get("answers") or [])
    fresh = {**args, "answers": []}
    if pin.plan is not None:
        fresh["plan_pinned"] = True
    elif pin.planned or (not pin.read and dropped):
        fresh["plan_pinned"] = False
    return json.dumps(fresh, ensure_ascii=False), dropped
