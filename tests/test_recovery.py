"""The single decider: the policy table (pure) and its wiring into the engine (stub-driven)."""

import dataclasses
import json
import shutil
import time
from pathlib import Path
from typing import Any

import pytest

from src import engine_lifecycle, engine_read, engine_recovery, engine_watch, recovery
from src.classify import Outcome
from src.config import Settings
from src.database import write_tx
from src.engine_lifecycle import api_error_records
from src.journal import JournalStep
from src.launcher import CORRECTIVE, SCRIPT_NAME
from src.logpager import iso
from src.mcp_server import ToolProvider
from src.recovery import (
    CALL_KILLS,
    KILL_REASONS,
    NO_PLAN_READ,
    RATE_LIMITED,
    RATE_LIMITED_FALLBACK,
    Counters,
    Done,
    Fail,
    Fresh,
    PlanPin,
    Resume,
    WaitHuman,
    backoff_s,
    describe,
    fresh_args,
    next_action,
    pending_answer,
    pinnable_plan,
    plan_pin,
    task_update,
)
from src.tasks import LATE_PARENT_TOOL, RESUME_CAUSES, Task
from tests.helpers import Env, rows, stub_log, submit, task_row, wait_for, wait_status
from tests.test_engine_lifecycle import (
    ENDED,
    flag,
    journal_started,
    launches,
    lifecycle,
    passed_call,
    steps_by_label,
)
from tests.test_lifecycle_contract import GO
from tests.workflow.run import run_workflow
from tests.workflow.test_feature_lifecycle import (
    CHANGES,
    PLAN,
    RUN_DIR,
    SCRIPT,
    _args,
    _responses,
    _with_epoch,
)

NOW = 1_000_000.0


def table_task(**overrides: Any) -> Task:
    values: dict[str, Any] = {
        "id": "t1", "agent": "lead", "kind": "lifecycle", "description": "d",
        "input_context": None, "priority": 0, "max_retries": 0, "status": "running",
        "fingerprint": None, "cwd": "/repo", "base_branch": "main", "branch": "b",
        "worktree": "/wt", "session_id": "s1", "run_id": "wf_1", "args_json": None,
        "attempt": 1, "resumes": 0, "call_retries": 0, "stage_retries": 0,
        "rate_limit_waited_s": 0.0, "corrective": 0, "resume_mode": "first", "pid": 42,
        "pid_start": 1.0, "kill_reason": None, "output_json": None, "error": None,
        "question_json": None,
        "next_attempt_at": None, "created_at": 1.0, "started_at": 1.0, "completed_at": None,
        "updated_at": 1.0,
    }  # fmt: skip
    values.update(overrides)
    return Task(**values)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(home=tmp_path / "home")


def decide(settings: Settings, outcome: Outcome, reset_at: float | None = None, **task: Any) -> Any:
    return next_action(outcome, table_task(**task), settings, NOW, reset_at)


def env_settings(env: Env, **values: Any) -> None:
    for name, value in values.items():
        setattr(env.settings, name, value)


async def event_data(task_id: str, type_: str) -> list[dict[str, Any]]:
    found = await rows(
        "SELECT data_json FROM events WHERE task_id = ? AND type = ? ORDER BY id", task_id, type_
    )
    return [json.loads(r["data_json"]) for r in found]


def prompt_of(entry: dict[str, Any]) -> str:
    argv = entry["argv"]
    return str(argv[argv.index("-p") + 1])


# -- the policy table ----------------------------------------------------------------------------
def test_every_outcome_and_kill_reason_has_a_policy(settings: Settings) -> None:
    for kill in [None, *sorted(KILL_REASONS)]:
        for outcome in Outcome:
            action = decide(settings, outcome, kill_reason=kill)
            assert isinstance(action, Resume | Fresh | Fail | Done | WaitHuman), (kill, outcome)
    with pytest.raises(ValueError, match="no recovery policy for kill reason 'mystery'"):
        decide(settings, Outcome.transient, kill_reason="mystery")


def test_outcome_table_without_a_kill(settings: Settings) -> None:
    assert decide(settings, Outcome.success) == Done("completed")
    assert decide(settings, Outcome.needs_human) == WaitHuman()
    assert decide(settings, Outcome.cancelled_by_workflow) == Done("cancelled")
    assert decide(settings, Outcome.fatal) == Fail("fatal")  # e.g. a workflow script exception
    assert isinstance(decide(settings, Outcome.transient), Resume)
    assert isinstance(decide(settings, Outcome.resume_refused), Fresh)
    for outcome in (Outcome.call_mismatch, Outcome.parent_violation):
        action = decide(settings, outcome)
        assert isinstance(action, Resume) and action.corrective and action.at == NOW
    # A late parent tool call after a valid return: the return decides, as without a kill.
    late = {"kill_reason": LATE_PARENT_TOOL}
    assert decide(settings, Outcome.success, **late) == Done("completed")
    assert decide(settings, Outcome.needs_human, **late) == WaitHuman()


def test_transient_backoff_cap(settings: Settings) -> None:
    settings.max_resumes = 8
    delays = []
    for n in range(8):
        action = decide(settings, Outcome.transient, resumes=n)
        assert isinstance(action, Resume) and not action.corrective
        assert action.counters == Counters(n + 1, 0, 0, 0.0)
        delays.append(action.at - NOW)
    assert delays == [30, 60, 120, 240, 480, 600, 600, 600]  # doubling from 30 s, capped 600 s
    assert backoff_s(settings, 1000) == 600
    # The watchdog's stall kill is a transient end; the budget is the same.
    action = decide(settings, Outcome.transient, resumes=2, kill_reason="stall")
    assert isinstance(action, Resume) and action.at == NOW + 120
    assert decide(settings, Outcome.transient, resumes=8) == Fail("transient")
    assert decide(settings, Outcome.transient, resumes=8, kill_reason="stall") == Fail(
        "transient: stall"
    )
    update = task_update(decide(settings, Outcome.transient, resumes=1), table_task(), NOW, "n")
    assert update == {
        "status": "queued", "error": None, "resume_mode": "resume", "resume_cause": "transient",
        "next_attempt_at": NOW + 60, "corrective": 0, "resumes": 2, "call_retries": 0,
        "stage_retries": 0, "rate_limit_waited_s": 0.0,
    }  # fmt: skip
    # An attempt that never spawned repeats its own mode (no session to resume yet); a resumed
    # relaunch takes this action's cause, a repeated first launch keeps none.
    unspawned = table_task(pid=None, resume_mode="first")
    first = task_update(decide(settings, Outcome.transient), unspawned, NOW, "n")
    assert (first["resume_mode"], first["resume_cause"]) == ("first", None)
    answered = table_task(pid=None, resume_mode="resume", resume_cause="answer")
    again = task_update(decide(settings, Outcome.transient), answered, NOW, "n")
    assert (again["resume_mode"], again["resume_cause"]) == ("resume", "transient")


def test_rate_limit_not_counted_capped(settings: Settings) -> None:
    settings.rate_limit_default_wait_s = 900
    settings.rate_limit_max_wait_s = 5000
    action = decide(settings, Outcome.rate_limited, NOW + 600, resumes=3)
    assert isinstance(action, Resume) and action.at == NOW + 600
    assert action.counters == Counters(3, 0, 0, 600.0)  # resumes not counted (budget is 3)
    action = decide(settings, Outcome.rate_limited, None, rate_limit_waited_s=600.0)
    assert isinstance(action, Resume) and action.at == NOW + 900  # no reset time: the default
    assert action.counters.rate_limit_waited_s == 1500.0
    # A reset already past: the default wait, charged like any other.
    past = decide(settings, Outcome.rate_limited, NOW - 60)
    assert isinstance(past, Resume) and past.at == NOW + 900
    assert past.counters.rate_limit_waited_s == 900.0
    # Never shorter than the floor (30 s by default), and always charged.
    soon = decide(settings, Outcome.rate_limited, NOW + 5)
    assert isinstance(soon, Resume) and soon.at == NOW + 30
    assert soon.counters.rate_limit_waited_s == 30.0
    # A reset beyond what is left of the wait budget fails at once (no early relaunch).
    assert decide(settings, Outcome.rate_limited, NOW + 1001, rate_limit_waited_s=4000.0) == Fail(
        "rate_limited: a wait of 1001s exceeds the wait budget (waited 4000s of 5000s)"
    )
    action = decide(settings, Outcome.rate_limited, NOW + 1000, rate_limit_waited_s=4000.0)
    assert isinstance(action, Resume) and action.counters.rate_limit_waited_s == 5000.0
    assert isinstance(
        decide(settings, Outcome.rate_limited, NOW + 60, rate_limit_waited_s=5000.0), Fail
    )


async def test_rate_limit_with_past_reset_terminates(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every run reports a reset already past: each wait is floored and charged, so the task
    fails after a bounded number of launches."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_past")
    env_settings(
        env, rate_limit_min_wait_s=0.25, rate_limit_default_wait_s=0, rate_limit_max_wait_s=1
    )
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"].startswith("rate_limited: a wait of 0s exceeds the wait budget"), st
    task = await task_row(sub["task_id"])
    assert task.rate_limit_waited_s == pytest.approx(1.0) and task.resumes == 0
    assert len(launches(env)) == 5  # 4 floored waits of 0.25 s fill the 1 s budget


async def test_rate_limit_resumes_at_reset_without_using_the_budget(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_past,single_success")
    # The reset already passed: the default wait (0), floored to 0.25 s and charged.
    env_settings(env, max_resumes=0, rate_limit_min_wait_s=0.25, rate_limit_default_wait_s=0)
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    task = await task_row(sub["task_id"])
    assert (task.resumes, task.rate_limit_waited_s, task.attempt) == (0, 0.25, 2)
    (scheduled,) = await event_data(task.id, "retry_scheduled")
    assert scheduled["why"] == "rate_limited" and scheduled["rate_limit_waited_s"] == 0.25
    _, second = [e for e in stub_log(env.log) if "argv" in e]
    assert flag(second, "--resume") == task.session_id


def test_fallback_applied_once_then_waits(settings: Settings) -> None:
    settings.model_fallback = {"fable": "opus", "absent": "haiku"}
    settings.rate_limit_default_wait_s = 900
    models = frozenset({"fable", "sonnet"})
    first = next_action(
        Outcome.rate_limited, table_task(resumes=2), settings, NOW, NOW + 3600, models=models
    )
    assert first == Resume(
        NOW + settings.rate_limit_min_wait_s, RATE_LIMITED_FALLBACK, Counters(2, 0, 0, 0.0),
        model_fallback={"fable": "opus"},
    )  # fmt: skip
    update = task_update(first, table_task(), NOW, "n")
    assert update["model_fallback_applied"] == 1 and update["rate_limit_waited_s"] == 0.0
    assert describe(first) == (
        "retry_scheduled", {
            "why": RATE_LIMITED_FALLBACK, "at": first.at, "corrective": False, "resumes": 2,
            "call_retries": 0, "stage_retries": 0, "rate_limit_waited_s": 0.0,
        },
    )  # fmt: skip
    # Once applied, the next rate limit waits for the reset as without a map.
    applied = table_task(model_fallback_applied=1)
    second = next_action(Outcome.rate_limited, applied, settings, NOW, NOW + 600, models=models)
    assert second == Resume(NOW + 600, RATE_LIMITED, Counters(0, 0, 0, 600.0))
    assert "model_fallback_applied" not in task_update(second, applied, NOW, "n")


def test_fallback_off_by_default(settings: Settings) -> None:
    assert settings.model_fallback == {}
    models = frozenset({"fable"})
    task = table_task()
    action = next_action(Outcome.rate_limited, task, settings, NOW, NOW + 600, models=models)
    assert action == Resume(NOW + 600, RATE_LIMITED, Counters(0, 0, 0, 600.0))
    # Only a rate limit falls back: any other outcome is decided as before.
    settings.model_fallback = {"fable": "opus"}
    transient = next_action(Outcome.transient, table_task(), settings, NOW, None, models=models)
    assert isinstance(transient, Resume) and transient.why == "transient"
    assert transient.model_fallback == {}


def test_fallback_skipped_when_no_mapped_model(settings: Settings) -> None:
    settings.model_fallback = {"fable": "opus"}
    for models in (frozenset({"opus"}), frozenset()):
        action = next_action(
            Outcome.rate_limited, table_task(), settings, NOW, NOW + 600, models=models
        )
        assert action == Resume(NOW + 600, RATE_LIMITED, Counters(0, 0, 0, 600.0))
    # A fallback is never charged, but a wait beyond the budget still fails without one.
    settings.rate_limit_max_wait_s = 100
    action = next_action(Outcome.rate_limited, table_task(), settings, NOW, NOW + 600)
    assert isinstance(action, Fail)


def _with_model(env: Env, slug: str, model: str) -> None:
    path = env.settings.agents_dir / f"{slug}.md"
    path.write_text(f"---\ndescription: {slug}\nmodel: {model}\n---\nYou are {slug}.\n")


def _attempt_models(env: Env, task_id: str, n: int) -> dict[str, str | None]:
    path = env.engine.runstore.attempt_agents_path(task_id, n)
    agents = json.loads(path.read_text())
    return {slug: entry.get("model") for slug, entry in agents.items()}


async def test_fallback_resumes_at_once_then_waits_on_the_engine(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mapped model is swapped in the agents file of every later attempt; the first rate
    limit relaunches after the floor without a charge, the second waits for the reset."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_past,rate_limit_past,single_success")
    env_settings(
        env, rate_limit_min_wait_s=0.25, rate_limit_default_wait_s=0,
        model_fallback={"fable": "opus"},
    )  # fmt: skip
    _with_model(env, "implementer", "fable")
    _with_model(env, "reviewer", "sonnet")
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    task = await task_row(sub["task_id"])
    assert (task.attempt, task.resumes, task.model_fallback_applied) == (3, 0, 1)
    assert task.rate_limit_waited_s == 0.25  # only the second wait is charged
    scheduled = await event_data(task.id, "retry_scheduled")
    assert [e["why"] for e in scheduled] == [RATE_LIMITED_FALLBACK, RATE_LIMITED]
    assert [e["rate_limit_waited_s"] for e in scheduled] == [0.0, 0.25]
    assert await event_data(task.id, "model_fallback") == [{"n": 1, "from_to": {"fable": "opus"}}]
    first, second, third = (_attempt_models(env, task.id, n) for n in (1, 2, 3))
    assert (first["implementer"], first["reviewer"]) == ("fable", "sonnet")
    assert (second["implementer"], second["reviewer"]) == ("opus", "sonnet")
    assert third == second
    for entry in launches(env)[1:]:
        assert flag(entry, "--resume") == task.session_id  # same session: a resume, not fresh


def test_stage_failed_one_retry_table(settings: Settings) -> None:
    action = decide(settings, Outcome.stage_failed)
    assert isinstance(action, Resume) and action.at == NOW and not action.corrective
    assert action.counters == Counters(0, 0, 1, 0.0)
    assert decide(settings, Outcome.stage_failed, stage_retries=1) == Fail("stage_failed")


async def test_stage_failed_one_retry(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "stage_failed")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    assert st["error"] == "stage_failed at merge: merge.sh: failed"
    task = await task_row(task_id)
    assert (task.attempt, task.stage_retries, task.resumes) == (2, 1, 0)
    first, second = launches(env)
    assert flag(second, "--resume") == task.session_id
    assert passed_call(second) == {**passed_call(first), "resumeFromRunId": task.run_id}
    (scheduled,) = await event_data(task_id, "retry_scheduled")
    assert scheduled["why"] == "stage_failed"


def test_call_mismatch_retries_table(settings: Settings) -> None:
    settings.call_mismatch_retries = 2
    for kill in sorted(CALL_KILLS):
        for n in range(2):
            action = decide(settings, Outcome.transient, call_retries=n, kill_reason=kill)
            assert isinstance(action, Resume) and action.corrective and action.at == NOW
            assert action.counters == Counters(0, n + 1, 0, 0.0)
        failed = decide(settings, Outcome.transient, call_retries=2, kill_reason=kill)
        assert failed == Fail(f"transient: {kill}")
    assert decide(settings, Outcome.call_mismatch, call_retries=2) == Fail("call_mismatch")


async def test_call_mismatch_two_retries_corrective_prompt(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "call_mutated,call_mutated,success")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st
    task = await task_row(task_id)
    assert (task.attempt, task.call_retries, task.resumes, task.corrective) == (3, 2, 0, 0)
    first, second, third = launches(env)
    assert not prompt_of(first).startswith(CORRECTIVE)
    for entry in (second, third):
        assert prompt_of(entry).startswith(CORRECTIVE)
        # The mismatch was the task's first call (no run stored): a new first call in the same
        # session, without resumeFromRunId.
        assert flag(entry, "--resume") == task.session_id and "--session-id" not in entry["argv"]
        assert passed_call(entry) == passed_call(first)
        assert "resumeFromRunId" not in passed_call(entry)
    assert [e["why"] for e in await event_data(task_id, "retry_scheduled")] == ["corrective"] * 2
    started = await event_data(task_id, "attempt_started")
    assert [e["corrective"] for e in started] == [False, True, True]
    assert [e["cause"] for e in started] == [None, "corrective", "corrective"]


async def kill_in_implement(env: Env, task_id: str) -> Task:
    """Wait until the run's ``implement`` stage runs, then kill it as the watchdog would."""
    return await kill_in_stage(env, task_id, "implement")


async def kill_in_stage(env: Env, task_id: str, label: str) -> Task:
    """Wait until the run's ``label`` step runs, then kill it as the watchdog would."""

    async def in_implement() -> Any:
        task = await task_row(task_id)
        if not (task.run_id and task.session_id and task.pid):
            return None
        started = journal_started(env, task_id, task.run_id, task.session_id)
        return env.engine._attempts.get(task_id) if label in started else None

    attempt = await wait_for(in_implement)
    killed = await task_row(task_id)
    await env.engine._kill_for(attempt, "stall")
    return killed


async def test_fresh_pins_plan_same_branch_new_session(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage,refuse_resume,success")
    monkeypatch.setenv("STUB_CLAUDE_PLAN", json.dumps(PLAN))
    env_settings(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    before = await kill_in_implement(env, task_id)
    plan_file = before.run_dir / "inputs" / "plan.json"
    assert before.args_dict["plan_pinned"] is False and before.args_dict["plan_gate"] is False
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st

    task = await task_row(task_id)
    assert task.session_id and task.session_id != before.session_id
    assert task.run_id and task.run_id != before.run_id
    assert (task.branch, task.worktree, task.base_branch, task.cwd) == (
        before.branch, before.worktree, before.base_branch, before.cwd,
    )  # fmt: skip
    assert json.loads(plan_file.read_text()) == PLAN
    assert oct(plan_file.stat().st_mode & 0o777) == oct(0o444)
    args = task.args_dict
    assert args == {**before.args_dict, "plan_pinned": True, "answers": []}
    assert await event_data(task_id, "plan_pinned") == [{"label": "plan", "run_id": before.run_id}]
    (fresh,) = await event_data(task_id, "fresh_start")
    assert (fresh["previous"], fresh["session_id"]) == (before.session_id, task.session_id)
    assert (fresh["previous_run_id"], fresh["plan_pinned"]) == (before.run_id, True)
    assert (fresh["dropped_answers"], fresh["note"]) == (0, None)
    assert task.resumes == 2  # the stall resume and the fresh start
    # Budget used (2) = transient resumes (1) + fresh restarts (1); mode=resume launches: 1.
    started = await event_data(task_id, "attempt_started")
    assert [(e["mode"], e["cause"]) for e in started] == [
        ("first", None), ("resume", "transient"), ("fresh", "fresh"),
    ]  # fmt: skip
    assert task.resume_cause == "fresh"
    assert st["attempts_by_mode"] == {"first": 1, "resume": 1, "fresh": 1}

    _, refused, relaunched = launches(env)
    assert flag(refused, "--resume") == before.session_id
    assert flag(relaunched, "--session-id") == task.session_id
    assert "--resume" not in relaunched["argv"]
    call = passed_call(relaunched)
    assert call == {"scriptPath": str(task.run_dir / "script" / SCRIPT_NAME), "args": args}
    (attempt,) = await rows("SELECT session_id FROM attempts WHERE task_id = ? AND n = 3", task_id)
    assert attempt["session_id"] == task.session_id

    # The previous run's copies and step rows are history; the mirror holds the new run only.
    history = task.run_dir / "history" / str(before.run_id)
    old_rows = json.loads((history / "steps.json").read_text())
    assert [(r["label"], r["status"]) for r in old_rows] == [
        ("plan", "completed"), ("implement", "running"),
    ]  # fmt: skip
    assert "implement" in journal_labels(history / "journal.jsonl")
    assert (history / "steps" / "implement.jsonl").is_file()
    assert json.loads((task.run_dir / "run.json").read_text())["runId"] == task.run_id
    (archived,) = await event_data(task_id, "run_archived")
    assert archived == {"run_id": before.run_id, "moved_to": str(history)}
    st = await env.engine.status(task_id)
    assert [(s["label"], s["status"]) for s in st["steps"]] == [
        ("plan", "completed"), ("implement", "completed"),
    ]  # fmt: skip
    started = await rows("SELECT started_at FROM steps WHERE task_id = ?", task_id)
    fresh_at = (await rows("SELECT at FROM events WHERE type = 'fresh_start'"))[0]["at"]
    assert min(r["started_at"] for r in started) >= fresh_at  # nothing inherited


def journal_labels(path: Path) -> list[str]:
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return [r["label"] for r in records if r.get("type") == "started"]


async def test_fresh_start_drops_answers_and_never_reuses_k(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fresh after an answered gate whose answer the run consumed (interpret-plan-1 ran, then
    implement was killed; the stub's plan is not plan-shaped: nothing is pinned): the new run
    starts without answers and asks again; the next answer gets a new k, file and row, the
    earlier ones are history."""
    monkeypatch.setenv(
        "STUB_CLAUDE_SCENARIO",
        "needs_human_gate,needs_human_gate_hang,refuse_resume,needs_human_gate",
    )
    env_settings(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    assert (await env.engine.answer(task_id, "approved", "mcp"))["k"] == 1
    before = await kill_in_implement(env, task_id)
    st = await wait_status(env.engine, task_id, "needs_human", "failed", "completed")
    assert st["status"] == "needs_human", st  # the fresh run asked again
    task = await task_row(task_id)
    assert task.args_dict["answers"] == [] and task.args_dict["plan_pinned"] is False
    assert await event_data(task_id, "plan_pinned") == []
    assert await event_data(task_id, "answer_not_applied") == []
    (fresh,) = await event_data(task_id, "fresh_start")
    assert (fresh["dropped_answers"], fresh["plan_pinned"]) == (1, False)
    assert "not carried over" in fresh["note"]
    # Only the new run's steps: the old run's interpret and killed implement are history.
    assert [s["label"] for s in st["steps"]] == ["plan"]
    history = task.run_dir / "history" / str(before.run_id)
    answers_dir = task.run_dir / "inputs" / "answers"
    assert (history / "answers" / "1.md").read_text() == "approved"
    assert not (answers_dir / "1.md").exists()

    assert (await env.engine.answer(task_id, "approved again", "mcp"))["k"] == 2
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    assert (answers_dir / "2.md").read_text() == "approved again"
    found = await rows("SELECT k, answer FROM answers WHERE task_id = ? ORDER BY k", task_id)
    assert [(r["k"], r["answer"]) for r in found] == [(1, "approved"), (2, "approved again")]
    *_, last = launches(env)
    assert passed_call(last)["args"]["answers"] == [{"k": 2, "label": "plan", "mode": "after"}]
    assert "interpret-plan-2" in await steps_by_label(env, task_id)


@pytest.mark.parametrize(
    ("scenario", "stage"), [("needs_human_rounds", "review-r1"), ("needs_human_gate", "plan")]
)
async def test_refused_resume_with_a_pending_answer_fails_without_a_launch(
    env: Env, monkeypatch: pytest.MonkeyPatch, scenario: str, stage: str
) -> None:
    """Ask -> answer -> the resume is refused before the run consumed the answer: a fresh run
    would drop the human's decision (it may be an abort), so the task fails; nothing more is
    launched, branch and worktree are kept and named."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", f"{scenario},refuse_resume,success")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    assert (await env.engine.answer(task_id, "abort", "mcp"))["k"] == 1
    st = await wait_status(env.engine, task_id, "failed", "completed", "needs_human")
    assert st["status"] == "failed", st
    task = await task_row(task_id)
    assert "resume refused while answer k=1 was pending; it could not be applied" in st["error"]
    assert f"branch {task.branch}" in st["error"] and f"worktree {task.worktree}" in st["error"]
    (event,) = await event_data(task_id, "answer_not_applied")
    assert event == {"n": 2, "k": 1, "branch": task.branch, "worktree": task.worktree}
    assert await event_data(task_id, "fresh_start") == []
    assert task.args_dict["answers"] == [{"k": 1, "label": stage, "mode": "after"}]
    assert task.resumes == 0
    await env.engine.stop()  # nothing may launch afterwards
    assert len(launches(env)) == 2


def test_refused_resume_with_a_pending_answer_never_starts_fresh(settings: Settings) -> None:
    for resumes in (0, 99):  # with or without resume budget left
        action = decide(settings, Outcome.resume_refused, resumes=resumes)
        assert isinstance(action, Fresh if resumes == 0 else Fail)
        action = next_action(
            Outcome.resume_refused, table_task(resumes=resumes), settings, NOW, None, 3
        )
        assert isinstance(action, Fail) and action.answer_k == 3, action
        assert action.reason.startswith("resume refused while answer k=3 was pending")
        assert "branch b, worktree /wt are kept" in action.reason
    for outcome in Outcome:  # only a refused resume reads the pending answer
        if outcome is not Outcome.resume_refused:
            plain = decide(settings, outcome)
            assert next_action(outcome, table_task(), settings, NOW, None, 3) == plain


def _done(*labels: str) -> list[tuple[str, str]]:
    return [(label, "completed") for label in labels]


def test_pending_answer_from_the_journal_labels() -> None:
    args = {"answers": [{"k": 1, "label": "plan", "mode": "after"}, {"k": 3, "label": "qa-r1"}]}
    assert pending_answer(args, _done("plan", "interpret-plan-1", "setup@a1")) == 3
    assert pending_answer(args, _done("interpret-qa-r1@a1-3")) is None  # a gate answer
    assert pending_answer(args, _done("implement@a3")) is None  # a rerun answer (its epoch)
    assert pending_answer(args, _done("interpret-plan-13", "x@a13", "x@a3b")) == 3  # not k=3
    # An interpret step that only started applied nothing: the answer is still pending.
    assert pending_answer(args, [("interpret-qa-r1@a1-3", "running")]) == 3
    assert pending_answer(args, [("implement@a3", "running")]) is None  # the epoch began
    assert pending_answer(args, None) == 3  # the journal could not be read
    assert pending_answer({"answers": []}, None) is None
    assert pending_answer({}, []) is None


async def test_refused_resume_with_an_interpret_in_flight_fails_without_a_launch(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The answer's interpret step started but was killed before its decision: nothing was
    applied, so the refused resume fails the task instead of starting fresh over it."""
    monkeypatch.setenv(
        "STUB_CLAUDE_SCENARIO", "needs_human_gate,needs_human_gate_interpret_hang,refuse_resume"
    )
    env_settings(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    assert (await env.engine.answer(task_id, "abort", "mcp"))["k"] == 1
    await kill_in_stage(env, task_id, "interpret-plan-1")
    st = await wait_status(env.engine, task_id, "failed", "completed", "needs_human")
    assert st["status"] == "failed", st
    assert "resume refused while answer k=1 was pending" in st["error"]
    task = await task_row(task_id)
    (event,) = await event_data(task_id, "answer_not_applied")
    assert event == {"n": 3, "k": 1, "branch": task.branch, "worktree": task.worktree}
    assert await event_data(task_id, "fresh_start") == []
    await env.engine.stop()
    assert len(launches(env)) == 3


def test_cancel_overrides(settings: Settings) -> None:
    for outcome in Outcome:
        for counters in ({}, {"resumes": 99, "call_retries": 99, "stage_retries": 99}):
            action = decide(settings, outcome, kill_reason="cancel", **counters)
            assert action == Done("cancelled"), outcome
    update = task_update(Done("cancelled"), table_task(), NOW, "n")
    assert update == {"status": "cancelled", "error": None, "completed_at": NOW, "corrective": 0}


async def test_cancel_while_waiting_for_a_retry_sweeps(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1")
    env_settings(env, retry_backoff_s=3600.0, retry_backoff_max_s=3600.0)
    sub = await submit(env)
    await env.engine.start()

    async def waiting() -> Task | None:
        task = await task_row(sub["task_id"])
        return task if task.status == "queued" and task.next_attempt_at else None

    task = await wait_for(waiting)
    assert task.next_attempt_at is not None and task.next_attempt_at > task.updated_at + 3000

    async def job_done() -> bool:  # the attempt's own end (and its sweep) is over
        return task.id not in env.engine._live

    await wait_for(job_done)
    assert (await env.engine.cancel(task.id))["status"] == "cancelled"

    async def swept() -> list[dict[str, Any]]:
        found = await event_data(task.id, "sweep")
        return [e for e in found if e["trigger"] == "cancel" and e["policy"] == "swept"]

    await wait_for(swept)
    assert len(launches(env)) == 1


def test_timeout_fails(settings: Settings) -> None:
    for outcome in Outcome:
        assert decide(settings, outcome, kill_reason="timeout") == Fail(f"{outcome.value}: timeout")


async def test_timeout_kill_fails_without_relaunch(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    env_settings(env, retry_backoff_s=0.0)
    sub = await submit(env)
    await env.engine.start()

    async def live() -> Any:  # spawned, and the stub has started playing
        started = (await task_row(sub["task_id"])).pid and launches(env)
        return env.engine._attempts.get(sub["task_id"]) if started else None

    attempt = await wait_for(live)
    await env.engine._kill_for(attempt, "timeout")
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"].startswith("transient: timeout"), st
    assert len(launches(env)) == 1


def test_max_retries_extends(settings: Settings) -> None:
    settings.max_resumes = 1
    for n in range(3):
        action = decide(settings, Outcome.transient, resumes=n, max_retries=2)
        assert isinstance(action, Resume) and action.counters.resumes == n + 1
    assert decide(settings, Outcome.transient, resumes=3, max_retries=2) == Fail("transient")
    assert decide(settings, Outcome.transient, resumes=1, max_retries=0) == Fail("transient")
    assert isinstance(decide(settings, Outcome.resume_refused, resumes=2, max_retries=2), Fresh)
    assert decide(settings, Outcome.resume_refused, resumes=3, max_retries=2) == Fail(
        "resume_refused"
    )


async def test_transient_resumes_same_session_then_completes(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1,exit1,single_success")
    env_settings(env, retry_backoff_s=0.0, max_resumes=2)
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    task = await task_row(sub["task_id"])
    assert (task.attempt, task.resumes, task.error) == (3, 2, None)
    first, *resumed = launches(env)
    assert flag(first, "--session-id") == task.session_id
    assert [flag(e, "--resume") for e in resumed] == [task.session_id] * 2
    scheduled = await event_data(task.id, "retry_scheduled")
    assert [(e["why"], e["resumes"]) for e in scheduled] == [("transient", 1), ("transient", 2)]
    pre = [e for e in await event_data(task.id, "sweep") if e["trigger"] == "pre_launch"]
    # Every launch sweeps first (the first has no clean predecessor; the others follow
    # unclean ends).
    assert [e["policy"] for e in pre] == ["swept"] * 3


async def test_engine_error_retried_as_transient(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    real = engine_watch.classify
    calls: list[int] = []

    def fails_once(**kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("classifier broke")
        return real(**kwargs)

    monkeypatch.setattr(engine_watch, "classify", fails_once)
    env_settings(env, retry_backoff_s=0.0)
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    first, second = await rows(
        "SELECT * FROM attempts WHERE task_id = ? ORDER BY n", sub["task_id"]
    )
    assert (first["outcome"], first["reason"]) == ("transient", "engine_error")
    assert first["detail"] == "engine error: ValueError: classifier broke"
    assert second["reason"] == "clean_exit"
    assert (await task_row(sub["task_id"])).resumes == 1


async def test_kill_mid_stage_resume_cached_prefix(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage,success")
    env_settings(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    killed = await kill_in_implement(env, task_id)  # as the watchdog does
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st

    task = await task_row(task_id)
    assert (task.session_id, task.run_id) == (killed.session_id, killed.run_id)
    assert (task.attempt, task.resumes) == (2, 1)
    first, resumed = launches(env)
    assert flag(resumed, "--resume") == task.session_id
    assert passed_call(resumed) == {**passed_call(first), "resumeFromRunId": task.run_id}
    assert task.run_id and task.session_id
    # Only the interrupted stage ran again; the finished one was replayed from cache.
    assert journal_started(env, task_id, task.run_id, task.session_id) == [
        "plan", "implement", "implement",
    ]  # fmt: skip
    steps = await steps_by_label(env, task_id)
    assert (steps["plan"]["cached"], steps["plan"]["started_count"]) == (True, 1)
    assert (steps["implement"]["cached"], steps["implement"]["started_count"]) == (False, 2)
    # Never two live jobs: the killed attempt was closed (unclean) and swept before the relaunch.
    one, two = await rows("SELECT * FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    assert one["reason"] == "stall" and one["ended_at"] <= two["started_at"]
    pre = [e for e in await event_data(task_id, "sweep") if e["trigger"] == "pre_launch"]
    assert [e["policy"] for e in pre] == ["swept", "swept"]  # one per launch
    assert dataclasses.asdict(task)["kill_reason"] is None


async def test_unknown_kill_reason_fails_instead_of_hanging(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No policy for a kill reason: the waiter's decision raises, the engine-error path fails
    the task (never left running with an open attempt holding its slot)."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    sub = await submit(env)
    await env.engine.start()

    async def live() -> Any:
        started = (await task_row(sub["task_id"])).pid and launches(env)
        return env.engine._attempts.get(sub["task_id"]) if started else None

    attempt = await wait_for(live)
    await env.engine._kill_for(attempt, "mystery")
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"] == "engine error: ValueError: no recovery policy for kill reason 'mystery'"
    (row,) = await rows("SELECT ended_at, reason FROM attempts WHERE task_id = ?", sub["task_id"])
    assert row["ended_at"] is not None and row["reason"] == "engine_error"
    assert len(launches(env)) == 1


# -- stage rate limits -------------------------------------------------------------------------
def test_api_error_records_found_wherever_they_sit() -> None:
    record = {
        "isApiErrorMessage": True,
        "apiErrorStatus": 429,
        "apiError": "usage_limit_reached",
        "apiErrorParams": {"rate_limit_info": {"resetsAt": 1700000000}},
    }
    older = {"isApiErrorMessage": True, "apiErrorStatus": 429}  # older shape: fewer fields
    line = {"type": "assistant", "message": {"content": [{"meta": [record]}]}, "x": older}
    assert sorted(map(len, api_error_records(line))) == [2, 4]
    assert api_error_records({"isApiErrorMessage": "true"}) == []
    assert api_error_records([1, "a", None]) == []


async def test_stage_rate_limit_waits_for_the_reset_not_a_stage_retry(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_stage")
    task_id = await lifecycle(env)
    await env.engine.start()

    async def waiting() -> Task | None:
        task = await task_row(task_id)
        return task if task.status == "queued" and task.next_attempt_at else None

    task = await wait_for(waiting)
    assert (task.stage_retries, task.resumes) == (0, 0)
    assert task.next_attempt_at is not None
    wait = task.next_attempt_at - task.updated_at
    assert 3500 < wait <= 3600 and task.rate_limit_waited_s == pytest.approx(wait, abs=1)
    (scheduled,) = await event_data(task_id, "retry_scheduled")
    assert scheduled["why"] == "rate_limited"
    (attempt,) = await rows("SELECT outcome FROM attempts WHERE task_id = ?", task_id)
    assert attempt["outcome"] == "rate_limited"

    async def job_done() -> bool:  # the attempt's own end (and its sweep) is over
        return task_id not in env.engine._live

    await wait_for(job_done)
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"


# -- fresh start with the real feature-lifecycle.js (Node harness) -------------------------------
needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is required")


def journal_of(out: dict[str, Any], responses: dict[str, list[Any]], running: str = "") -> Any:
    """Journal steps as the runtime would record this harness run (call order; the result each
    call was served). ``running`` names a step interrupted before its result."""
    served: dict[str, int] = {}
    steps = []
    for call in out["calls"]:
        label = call["label"]
        i = served.get(label, 0)
        served[label] = i + 1
        queue = responses.get(label) or []
        result = queue[i] if i < len(queue) else None
        status = "running" if label == running else "completed"
        steps.append(JournalStep(label, label, None, status, result, 1, "a" + label))
    return steps


def fresh_run_args(args: dict[str, Any], steps: Any) -> tuple[dict[str, Any], Any]:
    pin = plan_pin(steps, args["plan_gate"])
    fresh, _dropped = fresh_args(json.dumps(args), pin)
    return json.loads(fresh), pin.plan


@needs_node
def test_real_script_fresh_after_approved_gate_pins_and_completes() -> None:
    first = run_workflow(SCRIPT, _args(plan_gate=True), _responses())
    assert first["return"]["kind"] == "plan_approval"
    one = [{"k": 1, "label": "plan", "mode": "after"}]
    responses = _with_epoch(_responses(), 1, skip=("plan",)) | {"interpret-plan-1": [GO]}
    responses.pop("implement@a1")  # interrupted: implement never returned
    args = _args(plan_gate=True, answers=one)
    second = run_workflow(SCRIPT, args, responses)
    assert [c["label"] for c in second["calls"]][:4] == [
        "plan", "interpret-plan-1", "setup@a1", "implement@a1",
    ]  # fmt: skip
    fresh, pinned = fresh_run_args(args, journal_of(second, responses, running="implement@a1"))
    assert pinned == ("plan", PLAN)
    assert fresh == {**args, "answers": [], "plan_pinned": True}
    third = run_workflow(SCRIPT, fresh, _responses())
    labels = [c["label"] for c in third["calls"]]
    assert "plan" not in labels and labels[0] == "setup"
    assert third["return"]["status"] == "completed", third["return"]
    assert "not consumed" not in json.dumps(third["return"])


@needs_node
def test_real_script_fresh_with_unanswered_gate_asks_again() -> None:
    args = _args(plan_gate=True)
    first = run_workflow(SCRIPT, args, _responses())
    fresh, pinned = fresh_run_args(args, journal_of(first, _responses()))
    assert pinned is None and fresh == args
    again = run_workflow(SCRIPT, fresh, _responses())
    ret = again["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == ("needs_human", "plan", "plan_approval")


@needs_node
def test_real_script_fresh_after_unapproved_amend_asks_again() -> None:
    one = [{"k": 1, "label": "plan", "mode": "after"}]
    amend = {"action": "amend", "extra_rounds": 0, "guidance": "split the second step"}
    responses = _responses() | {"interpret-plan-1": [amend], "plan@a1": [PLAN]}
    args = _args(plan_gate=True, answers=one)
    second = run_workflow(SCRIPT, args, responses)
    assert second["return"]["stage"] == "plan@a1"  # the amended plan awaits approval
    fresh, pinned = fresh_run_args(args, journal_of(second, responses))
    assert pinned is None  # neither the superseded plan nor the unapproved one
    assert fresh["answers"] == [] and fresh["plan_pinned"] is False
    again = run_workflow(SCRIPT, fresh, _responses())
    assert (again["return"]["stage"], again["return"]["kind"]) == ("plan", "plan_approval")


@needs_node
def test_real_script_needs_human_plan_never_pinned() -> None:
    asking = PLAN | {"needs_human": {"question": "Which storage backend?"}}
    responses = _responses(plan=[asking])
    for gate in (False, True):
        args = _args(plan_gate=gate)
        out = run_workflow(SCRIPT, args, responses)
        assert (out["return"]["stage"], out["return"]["kind"]) == ("plan", "question")
        assert fresh_run_args(args, journal_of(out, responses))[1] is None
    empty = JournalStep("plan", "plan", None, "completed", PLAN | {"steps": []}, 1, "a")
    assert pinnable_plan([empty], plan_gate=False) is None
    assert pinnable_plan([], plan_gate=False) is None


@needs_node
async def test_real_script_consumes_an_answer_given_after_a_fresh_start(env: Env) -> None:
    """The engine numbers the post-fresh answer k=2 (k=1 is history); the real script consumes
    it (answer file 2.md, interpret-plan-2, epoch @a2) and the earlier file stays."""
    task_id = await lifecycle(env)
    store = env.engine.runstore
    store.write_answer(task_id, 1, "first answer, before the fresh start")
    question = {"stage": "plan", "kind": "plan_approval", "mode": "after", "question": "ok?"}
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO answers (task_id, k, label, mode, answer, source, at) "
            "VALUES (?, 1, 'plan', 'after', 'first answer, before the fresh start', 'mcp', 1)",
            (task_id,),
        )
        await db.execute(
            "UPDATE tasks SET status = 'needs_human', question_json = ?, attempt = 2, "
            "args_json = ? WHERE id = ?",
            (json.dumps(question), json.dumps(_args(plan_gate=True)), task_id),
        )
    assert (await env.engine.answer(task_id, "approved", "mcp"))["k"] == 2
    answers = (await task_row(task_id)).args_dict["answers"]
    assert answers == [{"k": 2, "label": "plan", "mode": "after"}]
    answers_dir = store.paths(task_id).inputs_dir / "answers"
    assert (answers_dir / "1.md").read_text() == "first answer, before the fresh start"
    assert (answers_dir / "2.md").read_text() == "approved"

    responses = _with_epoch(_responses(), 2, skip=("plan",)) | {"interpret-plan-2": [GO]}
    out = run_workflow(SCRIPT, _args(plan_gate=True, answers=answers), responses)
    labels = [c["label"] for c in out["calls"]]
    assert labels[:3] == ["plan", "interpret-plan-2", "setup@a2"]
    assert f"{RUN_DIR}/inputs/answers/2.md" in out["calls"][1]["prompt"]
    assert out["return"]["status"] == "completed", out["return"]
    assert "not consumed" not in json.dumps(out["return"])


_QUESTION_PLAN = PLAN | {"needs_human": {"question": "Which storage backend?"}}
_PENDING_CASES = {
    "plan_approval": (
        _args(plan_gate=True), _responses(), {"k": 1, "label": "plan", "mode": "after"},
        _with_epoch(_responses(), 1, skip=("plan",)) | {"interpret-plan-1": [GO]},
    ),
    "rounds_exhausted": (
        _args(limits={"review_rounds": 1, "qa_rounds": 1}),
        _responses(**{"review-r1-rev-a": [CHANGES]}),
        {"k": 1, "label": "review-r1", "mode": "after"},
        _responses(**{"review-r1-rev-a": [CHANGES]}) | {"interpret-review-r1-1": [GO]},
    ),
    "question": (
        _args(), _responses(plan=[_QUESTION_PLAN]), {"k": 1, "label": "plan", "mode": "rerun"},
        _with_epoch(_responses(), 1, skip=()),
    ),
}  # fmt: skip


@needs_node
@pytest.mark.parametrize("kind", sorted(_PENDING_CASES))
def test_real_script_answer_pending_until_the_run_consumes_it(kind: str) -> None:
    """The journal of the asking run (before the resume) leaves the answer pending; the run
    resumed with it consumes it (the labels the real script gives the consuming step)."""
    args, responses, answer, after = _PENDING_CASES[kind]
    asked = run_workflow(SCRIPT, args, responses)
    assert (asked["return"]["stage"], asked["return"]["kind"]) == (answer["label"], kind)
    answered = {**args, "answers": [answer]}
    assert pending_answer(answered, _done(*(c["label"] for c in asked["calls"]))) == 1
    resumed = run_workflow(SCRIPT, answered, after)
    labels = [c["label"] for c in resumed["calls"]]
    assert pending_answer(answered, _done(*labels)) is None, labels


def test_fresh_args_plan_pinned_follows_the_run() -> None:
    pinned = json.dumps({"plan_pinned": True, "answers": [{"k": 1}]})
    unpinned = json.dumps({"plan_pinned": False, "answers": []})
    pin = PlanPin(("plan@a1", PLAN), True)
    assert json.loads(fresh_args(unpinned, pin)[0])["plan_pinned"] is True
    # The run re-planned (e.g. after an amend) and nothing is pinnable: plan again.
    assert json.loads(fresh_args(pinned, PlanPin(None, True))[0]) == {
        "plan_pinned": False, "answers": [],
    }  # fmt: skip
    # The run never planned (it ran from the pin): keep the pin.
    assert json.loads(fresh_args(pinned, PlanPin(None, False))[0])["plan_pinned"] is True


def test_fresh_args_unreadable_plan_state_keeps_the_pin_only_without_answers() -> None:
    """Nothing could be read: without answers the run cannot have re-planned (the pin stays);
    with answers it may have (an amend), so the fresh run plans again."""
    no_answers = json.dumps({"plan_pinned": True, "answers": []})
    answered = json.dumps({"plan_pinned": True, "answers": [{"k": 1}]})
    assert fresh_args(no_answers, NO_PLAN_READ) == (
        json.dumps({"plan_pinned": True, "answers": []}), 0,
    )  # fmt: skip
    args, dropped = fresh_args(answered, NO_PLAN_READ)
    assert (json.loads(args), dropped) == ({"plan_pinned": False, "answers": []}, 1)
    # Never pinned: nothing to unpin either way.
    unpinned = json.dumps({"plan_pinned": False, "answers": [{"k": 1}]})
    assert json.loads(fresh_args(unpinned, NO_PLAN_READ)[0])["plan_pinned"] is False


@needs_node
def test_real_script_fresh_after_amend_of_a_pinned_plan_plans_again() -> None:
    """A run from a pinned plan; an amend re-plans (plan@a1), unanswered at the gate; the resume
    is refused: the fresh run must plan again and ask, not implement the old plan.json."""
    args = _args(plan_pinned=True, plan_gate=True, limits={"review_rounds": 1, "qa_rounds": 1})
    responses = _responses(**{"review-r1-rev-a": [CHANGES]})
    first = run_workflow(SCRIPT, args, responses)
    asked = first["return"]
    assert (asked["stage"], asked["kind"]) == ("review-r1", "rounds_exhausted")
    one = [{"k": 1, "label": "review-r1", "mode": "after"}]
    amend = {"action": "amend", "extra_rounds": 0, "guidance": "use a queue instead"}
    responses |= {"interpret-review-r1-1": [amend], "plan@a1": [PLAN]}
    second = run_workflow(SCRIPT, {**args, "answers": one}, responses)
    assert (second["return"]["stage"], second["return"]["kind"]) == ("plan@a1", "plan_approval")
    pin = plan_pin(journal_of(second, responses), plan_gate=True)
    assert pin == PlanPin(None, True)
    fresh = json.loads(fresh_args(json.dumps({**args, "answers": one}), pin)[0])
    assert fresh["plan_pinned"] is False and fresh["answers"] == []
    again = run_workflow(SCRIPT, fresh, _responses())
    assert [c["label"] for c in again["calls"]] == ["plan"]  # plans again ...
    ret = again["return"]
    assert (ret["stage"], ret["kind"]) == ("plan", "plan_approval")  # ... and asks again
    assert f"{RUN_DIR}/inputs/plan.json" not in again["calls"][0]["prompt"]


async def test_fresh_start_retires_a_stale_pinned_plan(env: Env) -> None:
    task_id = await lifecycle(env)
    store = env.engine.runstore
    store.write_inputs(task_id, None, {"steps": ["old"]})
    args = json.dumps(_args(plan_pinned=True, plan_gate=True))
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET args_json = ?, attempt = 4, run_id = 'wf_x' WHERE id = ?",
            (args, task_id),
        )
    task = await task_row(task_id)
    async with write_tx() as db:
        columns, _ = await engine_recovery.fresh_start(env.engine, db, task, PlanPin(None, True))
    assert json.loads(columns["args_json"])["plan_pinned"] is False
    paths = store.paths(task_id)
    assert not (paths.inputs_dir / "plan.json").exists()
    moved = paths.run_dir / "superseded" / "plan-4.json"
    assert json.loads(moved.read_text()) == {"steps": ["old"]}
    (event,) = await event_data(task_id, "plan_unpinned")
    assert event == {"run_id": "wf_x", "moved_to": str(moved)}


async def test_fresh_start_unreadable_plan_with_answers_retires_the_pin(env: Env) -> None:
    task_id = await lifecycle(env)
    store = env.engine.runstore
    args = _args(plan_pinned=True, plan_gate=True)
    for answers, kept in (([], True), ([{"k": 1, "label": "review-r1", "mode": "after"}], False)):
        store.write_inputs(task_id, None, {"steps": ["old"]})
        async with write_tx() as db:
            await db.execute(
                "UPDATE tasks SET args_json = ?, attempt = 2 WHERE id = ?",
                (json.dumps({**args, "answers": answers}), task_id),
            )
        task = await task_row(task_id)
        async with write_tx() as db:
            columns, _ = await engine_recovery.fresh_start(env.engine, db, task, NO_PLAN_READ)
        assert json.loads(columns["args_json"])["plan_pinned"] is kept
        assert (store.paths(task_id).inputs_dir / "plan.json").exists() is kept
    assert len(await event_data(task_id, "plan_unpinned")) == 1


async def test_run_store_journal_copy_used_only_for_the_same_run(env: Env) -> None:
    task_id = await lifecycle(env)
    store = env.engine.runstore
    run_dir = store.task_dir(task_id)
    records = [
        {"type": "started", "key": "k1", "agentId": "a1", "label": "plan"},
        {"type": "result", "key": "k1", "agentId": "a1", "result": PLAN},
    ]
    (run_dir / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET session_id = 'gone', run_id = 'wf_new' WHERE id = ?", (task_id,)
        )
    task = await task_row(task_id)
    (run_dir / "run.json").write_text(json.dumps({"runId": "wf_old"}))
    assert await engine_recovery.plan_to_pin(env.engine, task) == NO_PLAN_READ  # another run
    (run_dir / "run.json").write_text(json.dumps({"runId": "wf_new"}))
    assert await engine_recovery.plan_to_pin(env.engine, task) == PlanPin(("plan", PLAN), True)


def test_stage_records_only_those_the_step_ended_with(tmp_path: Path) -> None:
    limit = {"type": "assistant", "isApiErrorMessage": True, "apiErrorStatus": 429}
    ok = {"type": "assistant", "message": "went on"}
    user = {"type": "user", "message": "tool result"}
    path = tmp_path / "agent.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in (user, limit, ok, user)))
    assert engine_lifecycle._tail_records(path, 200) == []  # recovered from the early 429
    path.write_text("".join(json.dumps(r) + "\n" for r in (user, ok, limit)) + "{broken\n")
    assert engine_lifecycle._tail_records(path, 200) == [limit]


async def test_stage_limit_recovered_then_failed_is_stage_failed(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "stage_limit_recovered")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and st["error"] == "stage_failed at implement: tests failed"
    found = await rows("SELECT outcome FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    assert [r["outcome"] for r in found] == ["stage_failed", "stage_failed"]
    task = await task_row(task_id)
    assert (task.stage_retries, task.rate_limit_waited_s) == (1, 0.0)


async def test_replayed_stage_limit_is_not_another_rate_limit_wait(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failing step's 429 record stays in its transcript; after the wait the resumed run
    replays that step from cache, so the record is not this attempt's: ``stage_failed`` (one
    retry, then failed), never a second wait for a reset."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_stage")
    env_settings(env, watchdog_interval_s=3600.0)  # no watchdog pass on the shifted clock
    shift = [0.0]
    env.engine._clock = lambda: time.time() + shift[0]
    task_id = await lifecycle(env)
    await env.engine.start()

    async def waiting() -> Task | None:
        task = await task_row(task_id)
        return task if task.status == "queued" and task.next_attempt_at else None

    task = await wait_for(waiting)
    assert task.next_attempt_at is not None
    shift[0] = task.next_attempt_at - time.time() + 1.0  # the engine clock is past the reset
    env.engine._wake.set()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and st["error"].startswith("stage_failed at implement"), st
    found = await rows("SELECT outcome FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    assert [r["outcome"] for r in found] == ["rate_limited", "stage_failed", "stage_failed"]
    task = await task_row(task_id)
    assert task.stage_retries == 1
    assert [e["why"] for e in await event_data(task_id, "retry_scheduled")] == [
        "rate_limited", "stage_failed",
    ]  # fmt: skip


async def test_retry_time_is_on_the_engine_clock(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """``next_attempt_at`` is written and compared on one clock: an engine clock behind the
    wall clock still relaunches an immediate retry at once."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "stage_failed")
    env_settings(env, watchdog_interval_s=3600.0)
    env.engine._clock = lambda: time.time() - 1000.0
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    (scheduled,) = await event_data(task_id, "retry_scheduled")
    assert scheduled["at"] < time.time() - 900.0  # the engine clock's now
    assert (await task_row(task_id)).attempt == 2


async def test_status_shows_waiting_kill_reason_and_budgets(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """check_task_status: a task queued for later says until when and why; the kill reason and
    every recovery counter with what is left of its budget are shown (served redacted)."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_stage")
    env_settings(env, watchdog_interval_s=3600.0, max_resumes=2, call_mismatch_retries=1)
    task_id = await lifecycle(env)
    await env.engine.start()

    async def waiting() -> dict[str, Any] | None:
        st = await env.engine.status(task_id)
        return st if st["waiting"] else None

    await wait_for(waiting)
    frozen = env.engine._clock()
    env.engine._clock = lambda: frozen  # the scheduled wait counts down: pin it
    st = await env.engine.status(task_id)
    task = await task_row(task_id)
    assert task.next_attempt_at is not None
    assert st["waiting"] == {"until": iso(task.next_attempt_at), "why": "rate_limit"}
    assert st["waiting"]["until"].endswith("+00:00")
    assert st["kill_reason"] is None
    waited = task.rate_limit_waited_s
    assert st["recovery"] == {
        "resumes": {"used": 0, "left": 2},
        "call_retries": {"used": 0, "left": 1},
        "stage_retries": {"used": 0, "left": 1},
        "rate_limit_waited_s": {
            "used": waited - st["recovery"]["rate_limit_waited_s"]["scheduled"],
            "scheduled": st["recovery"]["rate_limit_waited_s"]["scheduled"],
            "left": 86400 - waited,
        },
    }
    assert 0 < st["recovery"]["rate_limit_waited_s"]["scheduled"] <= waited
    provider = ToolProvider()
    provider.attach(env.engine)
    served = await provider.check_task_status(task_id)
    assert served["waiting"] == st["waiting"] and served["recovery"] == st["recovery"]

    for event, data, why in (
        ("retry_scheduled", {"why": "transient"}, "backoff"),
        ("sweep_requeued", {"pids": [1234]}, "sweep"),
        ("sweep_requeued", {"pids": [], "streak": 2}, "sweep_unconfirmed"),
    ):
        async with write_tx() as db:
            await env.engine._event(db, task_id, event, data)
            await db.execute(
                "UPDATE tasks SET kill_reason = 'stall', resumes = 1 WHERE id = ?", (task_id,)
            )
        st = await env.engine.status(task_id)
        assert st["waiting"]["why"] == why, (event, data)
        assert st["kill_reason"] == "stall"
        assert st["recovery"]["resumes"] == {"used": 1, "left": 1}
    env.engine._clock = lambda: (task.next_attempt_at or 0) + 1.0  # due: no longer waiting
    assert (await env.engine.status(task_id))["waiting"] is None
    await env.engine.cancel(task_id)


async def test_recovery_view_splits_scheduled_wait(env: Env) -> None:
    """While queued for a rate-limit wait the charged seconds split into waited-so-far and the
    scheduled wait still ahead; the budget left is unchanged."""
    now = env.engine._clock()
    task = table_task(
        status="queued", rate_limit_waited_s=37401.0, next_attempt_at=now + 37000, resumes=0
    )
    waiting = {"until": iso(now + 37000), "why": "rate_limit"}
    view = engine_read._recovery(env.engine, task, waiting)["rate_limit_waited_s"]
    assert view["scheduled"] == pytest.approx(37000, abs=1)
    assert view["used"] == pytest.approx(401, abs=1)
    assert view["left"] == 86400 - 37401
    other = engine_read._recovery(env.engine, task, {"until": waiting["until"], "why": "backoff"})
    assert other["rate_limit_waited_s"] == {
        "used": 37401.0,
        "scheduled": 0.0,
        "left": 86400 - 37401,
    }
    idle = engine_read._recovery(env.engine, task, None)
    assert idle["rate_limit_waited_s"]["scheduled"] == 0.0


async def test_cancel_during_rate_limit_wait_reports_only_the_spent_wait(env: Env) -> None:
    """A task cancelled 401 s into a 37000 s rate-limit wait (37401 s charged) reports 401 s
    used, the rest scheduled but never spent: counted against ``completed_at``, not now."""
    task_id = (await submit(env))["task_id"]
    start = env.engine._clock()
    async with write_tx() as db:
        await env.engine._event(db, task_id, "retry_scheduled", {"why": RATE_LIMITED})
        await db.execute(
            "UPDATE tasks SET status = 'cancelled', kill_reason = 'cancel', "
            "rate_limit_waited_s = 37401.0, next_attempt_at = ?, completed_at = ? WHERE id = ?",
            (start + 37000, start, task_id),
        )
    env.engine._clock = lambda: start + 5000  # long after the cancel
    view = (await env.engine.status(task_id))["recovery"]["rate_limit_waited_s"]
    assert view["used"] == pytest.approx(401, abs=1)
    assert view["scheduled"] == pytest.approx(37000, abs=1)
    assert view["left"] == 86400 - 37401
    async with write_tx() as db:  # the last retry was not a rate-limit wait: all charged is used
        await env.engine._event(db, task_id, "retry_scheduled", {"why": "transient"})
    view = (await env.engine.status(task_id))["recovery"]["rate_limit_waited_s"]
    assert view == {"used": 37401.0, "scheduled": 0.0, "left": 86400 - 37401}


# -- authentication failures ---------------------------------------------------------------------
AUTH_TEXT = "Failed to authenticate: OAuth session expired and could not be refreshed"


def test_auth_failed_fails_at_once(settings: Settings) -> None:
    for counters in ({}, {"resumes": 0, "call_retries": 0, "stage_retries": 0}):
        assert decide(settings, Outcome.auth_failed, **counters) == Fail("auth_failed")


@pytest.mark.parametrize(
    ("kind", "scenario"),
    [("single", "auth_failed"), ("lifecycle", "auth_failed"), ("lifecycle", "auth_failed_stage")],
)
async def test_auth_failure_is_terminal_one_attempt(
    env: Env, monkeypatch: pytest.MonkeyPatch, kind: str, scenario: str
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", scenario)
    env_settings(env, retry_backoff_s=0.0)
    if kind == "lifecycle":
        task_id = await lifecycle(env)
    else:
        task_id = (await submit(env))["task_id"]
    await env.engine.start()
    status = await wait_status(env.engine, task_id, "failed", "completed", "needs_human")
    assert status["status"] == "failed"
    assert status["error"] == (
        f"claude is not authenticated for this service: {AUTH_TEXT}. Set credentials in "
        f"{env.settings.home}/child.env (see docs/credentials.md) or log in as the service user."
    )
    (auth,) = await event_data(task_id, "auth_failed")
    assert auth == {"n": 1, "message": AUTH_TEXT}
    assert await event_data(task_id, "retry_scheduled") == []
    assert await event_data(task_id, "fresh_start") == []
    (attempt,) = await rows("SELECT outcome FROM attempts WHERE task_id = ?", task_id)
    assert attempt["outcome"] == "auth_failed"
    await env.engine.stop()
    assert len(launches(env)) == 1


def test_every_resume_why_is_a_resume_cause() -> None:
    whys = {recovery.TRANSIENT, recovery.RATE_LIMITED, recovery.RATE_LIMITED_FALLBACK,
            recovery.STAGE_FAILED, recovery.CORRECTIVE}  # fmt: skip
    assert whys | {"answer", "fresh"} == set(RESUME_CAUSES)
