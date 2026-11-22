import asyncio
import dataclasses
import json
import os
import stat
import tomllib
from pathlib import Path
from typing import Any

import pytest

from src import engine_lifecycle, journal
from src.database import close_db, get_db, init_db, write_tx
from src.engine import TaskEngine
from src.journal import JournalStep, slug, step_transcript_path
from src.launcher import CALL_PREFIX, SCRIPT_NAME
from src.mcp_server import ToolProvider
from src.runstore import RunStore
from src.tasks import LATE_WORKFLOW_CALL
from tests.helpers import (
    Env,
    rows,
    stub_log,
    submit,
    task_row,
    wait_for,
    wait_status,
)

pytestmark = pytest.mark.usefixtures("spawned")  # every test: no stub process left running
ENDED = ("completed", "failed", "cancelled", "needs_human")


def launches(env: Env) -> list[dict[str, Any]]:
    return [entry for entry in stub_log(env.log) if "argv" in entry]


def passed_call(entry: dict[str, Any]) -> dict[str, Any]:
    """The Workflow input the launch prompt asks for."""
    argv = entry["argv"]
    prompt: str = argv[argv.index("-p") + 1]
    start = prompt.index(CALL_PREFIX) + len(CALL_PREFIX)
    value, _end = json.JSONDecoder().raw_decode(prompt[start:])
    assert isinstance(value, dict)
    return value


def flag(entry: dict[str, Any], name: str) -> str | None:
    argv = entry["argv"]
    return argv[argv.index(name) + 1] if name in argv else None


async def lifecycle(env: Env, description: str = "Build it") -> str:
    sub = await submit(env, description, agent="team-leader")
    return str(sub["task_id"])


async def steps_by_label(env: Env, task_id: str) -> dict[str, dict[str, Any]]:
    status = await env.engine.status(task_id)
    return {step["label"]: step for step in status["steps"]}


def journal_started(env: Env, task_id: str, run_id: str, session_id: str) -> list[str]:
    path = (
        env.settings.claude_home / "projects" / slug(str(env.repo))
        / session_id / "subagents" / "workflows" / run_id / "journal.jsonl"
    )  # fmt: skip
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return [r["label"] for r in records if r.get("type") == "started"]


def acked_runs(stream: Path) -> list[str]:
    lines = [json.loads(line) for line in stream.read_text().splitlines() if line.strip()]
    return [
        line["tool_use_result"]["runId"]
        for line in lines
        if isinstance(line.get("tool_use_result"), dict)
        and line["tool_use_result"].get("status") == "async_launched"
    ]


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


# -- the brief's tests ------------------------------------------------------------------------
async def test_gate_answer_resumes_same_session_full_call(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "needs_human", st
    question = {"stage": "plan", "kind": "plan_approval", "mode": "after"}
    assert st["question"] == {**question, "question": "Which colour?"}
    assert st["output"]["text"] == "Which colour?" and st["stage"] == "plan"
    first = await task_row(task_id)
    assert first.run_id is not None and first.run_id.startswith("wf_")
    script_dir = first.run_dir / "script"
    (launch,) = launches(env)
    assert passed_call(launch) == {
        "scriptPath": str(script_dir / SCRIPT_NAME),
        "args": first.args_dict,
    }
    args = first.args_dict
    assert args["answers"] == [] and args["roles"]["planner"] == "planner"
    assert args["branch"] == first.branch and args["worktree"] == first.worktree
    for name in ("setup.sh", "merge.sh"):
        assert os.access(script_dir / "stage" / name, os.X_OK)
    assert mode(script_dir / SCRIPT_NAME) == 0o444

    provider = ToolProvider()
    provider.attach(env.engine)
    answered = await provider.answer_task(task_id, "Approved, use blue")
    assert answered == {"task_id": task_id, "status": "queued", "k": 1}
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st

    _, resumed = launches(env)
    assert flag(resumed, "--resume") == first.session_id and "--session-id" not in resumed["argv"]
    answer = {"k": 1, "label": "plan", "mode": "after"}
    assert passed_call(resumed) == {
        "scriptPath": str(script_dir / SCRIPT_NAME),
        "args": {**args, "answers": [answer]},
        "resumeFromRunId": first.run_id,
    }
    final = await task_row(task_id)
    assert (final.session_id, final.run_id) == (first.session_id, first.run_id)
    assert final.question_json is None and final.args_dict["answers"] == [answer]
    answer_file = first.run_dir / "inputs" / "answers" / "1.md"
    assert answer_file.read_text() == "Approved, use blue" and mode(answer_file) == 0o444
    (row,) = await rows("SELECT * FROM answers WHERE task_id = ?", task_id)
    assert (row["k"], row["label"], row["mode"], row["source"]) == (1, "plan", "after", "mcp")
    assert row["question"] == "Which colour?" and row["answer"] == "Approved, use blue"
    steps = await steps_by_label(env, task_id)
    assert list(steps) == ["plan", "interpret-plan-1", "implement"]
    assert (steps["plan"]["cached"], steps["plan"]["started_count"]) == (True, 1)
    assert steps["interpret-plan-1"]["cached"] is False
    assert all(step["status"] == "completed" for step in steps.values())
    sweeps = await rows(
        "SELECT json_extract(data_json, '$.policy') AS policy, "
        "json_extract(data_json, '$.trigger') AS trigger FROM events "
        "WHERE task_id = ? AND type = 'sweep' ORDER BY id",
        task_id,
    )
    # needs_human by itself is a clean exit: nothing swept, also not before the relaunch
    assert [(r["policy"], r["trigger"]) for r in sweeps] == [
        ("swept", "pre_launch"),  # the first launch (no earlier attempt)
        ("skipped_clean_exit", "attempt_end"),
        ("skipped_clean_exit", "pre_launch"),
        ("skipped_clean_exit", "attempt_end"),
    ]


async def test_rerun_answer_mode(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_rerun")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "needs_human", st
    assert (st["question"]["kind"], st["question"]["mode"]) == ("question", "rerun")
    assert st["question"]["question"] == "NEEDS_HUMAN: which colour?"
    assert await env.engine.answer(task_id, "blue", "rest") == {
        "task_id": task_id,
        "status": "queued",
        "k": 1,
    }
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    _, resumed = launches(env)
    assert passed_call(resumed)["args"]["answers"] == [{"k": 1, "label": "plan", "mode": "rerun"}]
    assert st["output"]["return"]["result"] == {"colour": "blue"}
    steps = await steps_by_label(env, task_id)
    assert (steps["plan@a1"]["started_count"], steps["plan@a1"]["cached"]) == (1, False)
    (row,) = await rows("SELECT mode, source FROM answers WHERE task_id = ?", task_id)
    assert (row["mode"], row["source"]) == ("rerun", "rest")


async def test_answer_race_single_relaunch(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    results = await asyncio.gather(
        env.engine.answer(task_id, "first", "mcp"),
        env.engine._schedule_once(),
        env.engine.answer(task_id, "second", "rest"),
        env.engine._schedule_once(),
    )
    answers = [r for r in results if isinstance(r, dict)]
    assert sorted("error" in r for r in answers) == [False, True]
    rejected = next(r for r in answers if "error" in r)
    assert "it will ask when it needs you" in rejected["error"]
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    await env.engine.stop()  # nothing else may launch afterwards
    assert len(launches(env)) == 2
    assert len(await rows("SELECT n FROM attempts WHERE task_id = ?", task_id)) == 2
    assert len(await rows("SELECT k FROM answers WHERE task_id = ?", task_id)) == 1
    assert len((await task_row(task_id)).args_dict["answers"]) == 1


async def test_answer_running_rejected(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    queued = await lifecycle(env, "Queued one")
    rejected = await env.engine.answer(queued, "too early", "mcp")
    assert rejected["error"] == "task is queued — it will ask when it needs you"
    await env.engine.start()

    async def running() -> bool:
        return (await task_row(queued)).status == "running"

    await wait_for(running)
    rejected = await env.engine.answer(queued, "still too early", "mcp")
    assert rejected["error"] == "task is running — it will ask when it needs you"
    assert (await env.engine.answer("nope", "x", "mcp"))["error"] == "task nope not found"
    assert "answer must be" in (await env.engine.answer(queued, "  ", "mcp"))["error"]
    await env.engine.cancel(queued)
    await wait_status(env.engine, queued, "cancelled")
    assert (await env.engine.answer(queued, "late", "mcp"))["error"] == "task is cancelled"
    assert await rows("SELECT k FROM answers WHERE task_id = ?", queued) == []
    assert not (RunStore(env.settings.runs_dir).paths(queued).inputs_dir / "answers").exists()


async def test_call_mutated_killed_before_stage(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "call_mutated")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "call_mismatch" in st["error"], st
    task = await task_row(task_id)
    assert task.kill_reason == "call_mismatch" and task.session_id
    assert task.run_id is None  # the run of a mismatched call is never the task's run
    (run_id,) = acked_runs(task.run_dir / "attempt-1.stream.jsonl")
    assert journal_started(env, task_id, run_id, task.session_id) == []
    assert st["steps"] == []
    types = [r["type"] for r in await rows("SELECT type FROM events WHERE task_id = ?", task_id)]
    assert "call_mismatch" in types


async def test_drop_resume_id_killed(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate,drop_resume_id")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    first = await task_row(task_id)
    assert first.run_id and first.session_id
    before = journal_started(env, task_id, first.run_id, first.session_id)
    assert "error" not in await env.engine.answer(task_id, "go", "mcp")
    st = await wait_status(env.engine, task_id, "failed", "completed")
    assert st["status"] == "failed" and "call_mismatch" in st["error"], st
    task = await task_row(task_id)
    assert task.kill_reason == "call_mismatch" and task.run_id == first.run_id
    _, resumed = launches(env)
    assert flag(resumed, "--resume") == first.session_id
    assert passed_call(resumed)["resumeFromRunId"] == first.run_id  # the prompt asked for it
    assert journal_started(env, task_id, first.run_id, first.session_id) == before

    async def swept() -> list[Any]:  # a killed lifecycle attempt is unclean: it is swept
        return await rows(
            "SELECT data_json FROM events WHERE task_id = ? AND type = 'sweep' "
            "AND json_extract(data_json, '$.trigger') = 'attempt_end' "
            "AND json_extract(data_json, '$.policy') = 'swept'",
            task_id,
        )

    await wait_for(swept)  # written after the status commit (the sweep runs off the waiter)


async def test_second_call_killed(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "second_call")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "second_workflow_call" in st["error"], st
    task = await task_row(task_id)
    assert task.kill_reason == "second_workflow_call"
    (launch,) = launches(env)
    first_run, second_run = acked_runs(task.run_dir / "attempt-1.stream.jsonl")
    assert task.run_id == first_run != second_run  # the first call's run stays the task's
    types = [r["type"] for r in await rows("SELECT type FROM events WHERE task_id = ?", task_id)]
    assert "second_workflow_call" in types


async def test_background_return_mapped(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "background_return")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st
    task = await task_row(task_id)
    run_file = json.loads((task.run_dir / "run.json").read_text())
    workflow_return = run_file["result"]
    assert workflow_return["status"] == "completed"
    assert st["output"]["return"] == workflow_return
    assert json.loads(st["output"]["text"]) == workflow_return  # never the last result's text
    assert st["output"]["cost_usd"] == pytest.approx(0.0125)
    assert st["run_id"] == run_file["runId"] == task.run_id
    assert st["question"] is None and st["error"] is None
    steps = st["steps"]
    assert [s["label"] for s in steps] == ["plan", "implement"]
    for step in steps:
        assert (step["status"], step["cached"], step["started_count"]) == ("completed", False, 1)
        assert step["agent_id"].startswith("a")
    assert {s["phase"] for s in steps} == {"Plan", "Implement"}


async def test_status_steps_cached_after_restart(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    # No workflowProgress in the run file: `cached` must come from the persisted step counts.
    monkeypatch.setenv("STUB_CLAUDE_NO_PROGRESS", "1")
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    before = await steps_by_label(env, task_id)
    assert (before["plan"]["cached"], before["plan"]["started_count"]) == (False, 1)

    await env.engine.stop()  # engine restart: new process state, same database
    await close_db()
    await init_db(env.settings.db_path)
    old = env.engine
    engine = TaskEngine(env.settings, old.roles, old.agents, old.runstore, old.redactor)
    env.engine = engine  # the fixture stops this one
    await engine.start()
    assert (await engine.answer(task_id, "approved", "mcp"))["status"] == "queued"
    st = await wait_status(engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    steps = await steps_by_label(env, task_id)
    assert (steps["plan"]["cached"], steps["plan"]["started_count"]) == (True, 1)
    assert steps["interpret-plan-1"]["cached"] is False
    assert steps["implement"]["cached"] is False


async def seed_step(task_id: str, label: str, phase: str, status: str = "running") -> None:
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO steps (task_id, label, phase, status, started_count, started_at) "
            "VALUES (?, ?, ?, ?, 1, 1.0)",
            (task_id, label, phase, status),
        )


async def test_step_interrupted_when_task_not_running(env: Env) -> None:
    task_id = await lifecycle(env)  # queued: the engine is not started
    await seed_step(task_id, "plan", "Plan")
    assert (await engine_lifecycle.step_list(task_id, "queued"))[0]["status"] == "interrupted"
    assert (await engine_lifecycle.step_list(task_id, "running"))[0]["status"] == "running"
    (row,) = await rows("SELECT status FROM steps WHERE task_id = ?", task_id)
    assert row["status"] == "running"  # the mirror row is unchanged
    st = await env.engine.status(task_id)
    assert (st["status"], st["steps"][0]["status"], st["running_steps"]) == (
        "queued", "interrupted", [],
    )  # fmt: skip


async def test_stage_parallel_reviews_reports_phase(env: Env) -> None:
    task_id = await lifecycle(env)
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,))
    labels = ["review-r1-dx-reviewer-correctness", "review-r1-dx-reviewer-maintainability"]
    for label in labels:
        await seed_step(task_id, label, "Review")
    st = await env.engine.status(task_id)
    assert (st["stage"], st["running_steps"]) == ("Review r1", labels)
    # No shared round: the phase alone. No shared phase: the last running label, as before.
    await seed_step(task_id, "review-r2-dx-reviewer-style", "Review")
    assert (await env.engine.status(task_id))["stage"] == "Review"
    await seed_step(task_id, "implement", "Implement")
    assert (await env.engine.status(task_id))["stage"] == "implement"


async def test_stage_parallel_reviews_reports_phase_while_waiting(env: Env) -> None:
    """Between attempts (queued) the parallel reviews are interrupted, not running: the stage
    still names the phase and round, not one reviewer."""
    task_id = await lifecycle(env)  # queued: the engine is not started
    labels = ["review-r1-dx-reviewer-correctness", "review-r1-dx-reviewer-maintainability"]
    for label in labels:
        await seed_step(task_id, label, "Review")
    st = await env.engine.status(task_id)
    assert [s["status"] for s in st["steps"]] == ["interrupted", "interrupted"]
    assert (st["stage"], st["running_steps"]) == ("Review r1", [])
    (item,) = (await env.engine.list(None, None, 10, None))["tasks"]
    assert item["stage"] == "Review r1"


async def test_list_tasks_includes_branch_stage_cost(env: Env) -> None:
    env.settings.task_queue_size = 100
    ids = [await lifecycle(env, f"Build {i}") for i in range(50)]
    target = ids[-1]
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'running', started_at = 5.0, branch = 'feat/x', "
            "worktree = '/wt/x' WHERE id = ?",
            (target,),
        )
        for n, cost in ((1, 1.0), (2, 0.5)):
            await db.execute(
                "INSERT INTO attempts (task_id, n, started_at, ended_at, cost_usd) "
                "VALUES (?, ?, 1.0, 2.0, ?)",
                (target, n, cost),
            )
    await seed_step(target, "plan", "Plan", "completed")
    await seed_step(target, "implement", "Implement")
    seen: list[str] = []
    await get_db().set_trace_callback(lambda sql: seen.append(sql))
    listed = await env.engine.list(None, None, 50, None)
    await get_db().set_trace_callback(None)
    assert len([sql for sql in seen if sql.lstrip().upper().startswith("SELECT")]) <= 4
    by_id = {t["task_id"]: t for t in listed["tasks"]}
    assert len(by_id) == 50
    item = by_id[target]
    assert item["stage"] == "implement" and item["cost_usd"] == pytest.approx(1.5)
    assert (item["branch"], item["worktree"]) == ("feat/x", "/wt/x")
    assert item["started_at"] == "1970-01-01T00:00:05+00:00"
    idle = by_id[ids[0]]
    assert (idle["stage"], idle["cost_usd"], idle["started_at"]) == (None, 0.0, None)


# -- additions -------------------------------------------------------------------------------
async def test_stage_failed_return_surfaces_reason(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "answer_mismatch")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    assert st["error"].startswith("stage_failed at implement: answer None")
    assert "was not consumed" in st["error"]


async def test_workflow_abort_cancels_task(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "workflow_abort")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "cancelled" and st["completed_at"], st
    assert st["output"]["return"]["stage"] == "interpret-plan-1"


async def test_step_mirror_runs_during_the_watch(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engine_lifecycle, "MIRROR_INTERVAL_S", 0.1)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    phases: list[tuple[str, str]] = []

    class Notifier:
        async def phase(self, task_id: str, phase: str) -> None:
            phases.append((task_id, phase))

    env.engine.notifier = Notifier()
    task_id = await lifecycle(env)
    await env.engine.start()

    async def mirrored() -> dict[str, dict[str, Any]] | None:
        steps = await steps_by_label(env, task_id)
        return steps if "implement" in steps else None

    steps = await wait_for(mirrored)
    task = await task_row(task_id)
    assert task.status == "running" and task.run_id is not None  # run id stored live
    assert steps["plan"]["status"] == "completed" and steps["implement"]["status"] == "running"
    assert (await env.engine.status(task_id))["stage"] == "implement"
    store = env.engine.runstore
    assert store.step_copy_path(task_id, "plan").is_file()  # a finished step is copied live
    assert not store.step_copy_path(task_id, "implement").exists()
    assert phases[-1] == (task_id, "Implement")
    assert all(a != b for a, b in zip(phases, phases[1:], strict=False))  # changes only
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")


async def test_needs_human_inputs_sealed_and_subject_location_writable(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    paths = env.engine.runstore.paths(task_id)
    assert mode(paths.inputs_dir) == 0o555 and mode(paths.script_dir) == 0o555
    with pytest.raises(PermissionError):
        (paths.inputs_dir / "extra.md").write_text("x")
    assert mode(paths.out_dir) == 0o700  # out/merge-subject.txt: the only writable run dir


# -- fix round 1 ---------------------------------------------------------------------------------
REPO = Path(__file__).resolve().parent.parent


def test_workflows_packaged_and_resolved(tmp_path: Path) -> None:
    # The wheel ships the tree as src/workflows (no wheel is built here: the build backend is
    # not installed offline; the resolver is tested against the simulated packaged layout).
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text())
    wheel = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert wheel["force-include"] == {
        "workflows": "src/workflows",
        "templates": "cli/templates",  # the CLI's data dirs (tests/test_cli.py)
        "examples": "cli/examples",
    }
    assert engine_lifecycle.workflows_dir() == REPO / "workflows"  # this checkout

    site = tmp_path / "site"
    package = site / "src"
    (package / "workflows" / "stage").mkdir(parents=True)
    (package / "workflows" / SCRIPT_NAME).write_text("// packaged")
    (site / "workflows").mkdir()
    (site / "workflows" / SCRIPT_NAME).write_text("// source tree")
    assert engine_lifecycle.workflows_dir(package) == package / "workflows"  # packaged first
    (package / "workflows" / SCRIPT_NAME).unlink()
    assert engine_lifecycle.workflows_dir(package) == site / "workflows"
    (site / "workflows" / SCRIPT_NAME).unlink()
    assert engine_lifecycle.workflows_dir(package) is None


async def test_stage_scripts_executable_whatever_the_installed_mode(
    env: Env, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package = tmp_path / "pkg" / "src"
    installed = package / "workflows"
    (installed / "stage").mkdir(parents=True)
    for name in (SCRIPT_NAME, "stage/setup.sh", "stage/merge.sh"):
        (installed / name).write_bytes((REPO / "workflows" / name).read_bytes())
        (installed / name).chmod(0o644)  # an install that dropped the execute bit
    monkeypatch.setattr(engine_lifecycle, "PACKAGE_DIR", package)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    task_id = await lifecycle(env)
    await env.engine.start()
    assert (await wait_status(env.engine, task_id, *ENDED))["status"] == "completed"
    script = env.engine.runstore.paths(task_id).script_dir
    assert mode(script / "stage" / "setup.sh") == mode(script / "stage" / "merge.sh") == 0o555
    assert mode(script / SCRIPT_NAME) == 0o444


async def test_resume_with_stale_run_file_does_not_reask(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate,stale_run_file")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    assert "error" not in await env.engine.answer(task_id, "approved", "mcp")
    st = await wait_status(env.engine, task_id, "failed", "completed", "needs_human")
    # The run file still holds attempt 1's needs_human return: it is not this attempt's.
    assert st["status"] == "failed" and st["error"].startswith("transient"), st
    assert st["question"] is None
    (_, second) = await rows("SELECT outcome FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    assert second["outcome"] == "transient"


async def test_needs_human_without_stage_fails(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_no_stage")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and st["completed_at"], st
    assert st["error"] == "invalid needs_human return (stage='', kind='question')"
    assert st["question"] is None


async def test_answer_too_large_or_unstorable_is_an_error(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    provider = ToolProvider()
    provider.attach(env.engine)
    big = await provider.answer_task(task_id, "é" * 32769)  # 65538 bytes
    assert big["error"] == "answer is 65538 bytes; the limit is 65536 bytes (UTF-8)"

    def broken(task_id: str, k: int, text: str) -> Path:
        raise OSError(28, "No space left on device")

    real = env.engine.runstore.write_answer
    monkeypatch.setattr(env.engine.runstore, "write_answer", broken)
    failed = await provider.answer_task(task_id, "approved")
    assert failed["error"].startswith("could not store the answer: OSError")
    assert (await task_row(task_id)).status == "needs_human"
    assert await rows("SELECT k FROM answers WHERE task_id = ?", task_id) == []
    monkeypatch.setattr(env.engine.runstore, "write_answer", real)  # the store works again
    assert (await provider.answer_task(task_id, "a" * 65536))["status"] == "queued"
    assert (await wait_status(env.engine, task_id, "completed", "failed"))["status"] == "completed"


async def test_prepare_returns_none_when_the_row_moved(env: Env) -> None:
    task_id = await lifecycle(env)
    stale = await task_row(task_id)
    await env.engine.cancel(task_id)
    assert await engine_lifecycle.prepare(env.engine, stale) is None
    assert "roles" not in (await task_row(task_id)).args_dict  # nothing unpersisted is used


async def test_stop_awaits_kills_requested_while_stopping(env: Env) -> None:
    engine = env.engine
    finished: list[bool] = []

    async def slow_kill() -> None:
        await asyncio.sleep(0.2)
        finished.append(True)

    async def job() -> None:
        try:
            await asyncio.sleep(3600)
        finally:  # e.g. a watch that requests a kill as it is cancelled
            engine._background(slow_kill())

    engine._jobs.add(asyncio.create_task(job()))
    await asyncio.sleep(0)
    await engine.stop()
    assert finished == [True] and not engine._kills


# -- phase 3 fix wave --------------------------------------------------------------------------
def add_dirs(entry: dict[str, Any]) -> list[str]:
    argv = entry["argv"]
    i = argv.index("--add-dir") + 1
    end = next(j for j in range(i, len(argv)) if argv[j].startswith("--"))
    return list(argv[i:end])


async def events_of(task_id: str, type_: str) -> list[dict[str, Any]]:
    found = await rows(
        "SELECT data_json FROM events WHERE task_id = ? AND type = ? ORDER BY id", task_id, type_
    )
    return [json.loads(r["data_json"]) for r in found]


async def test_out_dir_created_before_launch_and_passed_to_every_launch(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    paths = env.engine.runstore.paths(task_id)
    assert mode(paths.out_dir) == 0o700
    (paths.out_dir / "merge-subject.txt").write_text("feat: x\n")  # writable by the stages
    assert mode(paths.inputs_dir) == mode(paths.script_dir) == 0o555
    assert "error" not in await env.engine.answer(task_id, "approved", "mcp")
    assert (await wait_status(env.engine, task_id, "completed", "failed"))["status"] == "completed"
    first, resumed = launches(env)
    expected = [str(paths.script_dir), str(paths.inputs_dir), str(env.settings.worktrees_dir)]
    for entry in (first, resumed):
        assert add_dirs(entry) == [*expected, str(paths.out_dir)]
        assert entry["argv"].count("--allowedTools") == 1
    assert (paths.out_dir / "merge-subject.txt").read_text() == "feat: x\n"


async def test_script_exception_fails_with_run_file_error(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "script_exception")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    assert st["error"] == (
        "workflow script failed (run status 'failed'): TypeError: plan.steps is not iterable"
    )
    (attempt,) = await rows("SELECT outcome FROM attempts WHERE task_id = ?", task_id)
    assert attempt["outcome"] == "fatal"
    assert await events_of(task_id, "workflow_script_failed") == [
        {"status": "failed", "error": "TypeError: plan.steps is not iterable"}
    ]


async def test_unknown_return_status_fails_with_detail(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "unknown_status")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    assert st["error"] == "invalid workflow return (status='finished-ish')"
    assert await events_of(task_id, "invalid_workflow_return") == [{"status": "'finished-ish'"}]


LATE = "parent_tool_after_completion"


async def assert_late_call_killed_and_swept(task_id: str) -> None:
    """The late parent call killed the process (an unclean end: reason stored on the attempt,
    the attempt's leftovers swept) and is recorded once."""
    task = await task_row(task_id)
    assert task.kill_reason == LATE
    (attempt,) = await rows(
        "SELECT reason, outcome, exit_code FROM attempts WHERE task_id = ? AND n = ?",
        task_id,
        task.attempt,
    )
    assert attempt["reason"] == LATE and attempt["exit_code"] != 0, dict(attempt)
    assert await events_of(task_id, LATE) == [{"tool": "Bash"}]
    assert await events_of(task_id, "parent_tool_violation") == []

    async def swept() -> list[Any]:
        return await rows(
            "SELECT data_json FROM events WHERE task_id = ? AND type = 'sweep' "
            "AND json_extract(data_json, '$.trigger') = 'attempt_end' "
            "AND json_extract(data_json, '$.policy') = 'swept'",
            task_id,
        )

    await wait_for(swept)


async def test_parent_tool_after_completed_return_killed_return_decides(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success_late_bash")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)  # the stub hangs: only a kill ends it
    assert st["status"] == "completed" and st["error"] is None, st
    task = await task_row(task_id)
    run_file = json.loads((task.run_dir / "run.json").read_text())
    assert st["output"]["return"] == run_file["result"]
    assert run_file["result"]["status"] == "completed"
    await assert_late_call_killed_and_swept(task_id)


async def test_parent_tool_after_needs_human_return_killed_question_kept(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate_late_bash,needs_human_gate")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "needs_human" and st["error"] is None, st
    question = {"stage": "plan", "kind": "plan_approval", "mode": "after"}
    assert st["question"] == {**question, "question": "Which colour?"}
    await assert_late_call_killed_and_swept(task_id)

    results = await asyncio.gather(
        env.engine.answer(task_id, "approved", "mcp"),
        env.engine._schedule_once(),
        env.engine._schedule_once(),
    )
    assert results[0] == {"task_id": task_id, "status": "queued", "k": 1}
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    await env.engine.stop()  # nothing else may launch afterwards
    assert len(launches(env)) == 2
    assert len(await rows("SELECT n FROM attempts WHERE task_id = ?", task_id)) == 2


async def test_parent_tool_during_the_run_still_fails(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "parent_bash_mid_run")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "parent_violation" in st["error"], st
    assert (await task_row(task_id)).kill_reason == "parent_violation"
    assert len(await events_of(task_id, "parent_tool_violation")) == 1
    assert await events_of(task_id, LATE) == []


async def test_parent_tool_before_completion_still_kills(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "parent_bash")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "parent_violation" in st["error"], st
    assert (await task_row(task_id)).kill_reason == "parent_violation"
    assert len(await events_of(task_id, "parent_tool_violation")) == 1
    assert await events_of(task_id, "parent_tool_after_completion") == []


@pytest.mark.parametrize(
    ("scenario", "stage", "kind"),
    [
        ("needs_human_rounds", "review-r1", "rounds_exhausted"),
        ("needs_human_merge_blocked", "merge", "merge_blocked"),
    ],
)
async def test_gate_kinds_answer_end_to_end(
    env: Env, monkeypatch: pytest.MonkeyPatch, scenario: str, stage: str, kind: str
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", scenario)
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "needs_human", st
    assert {k: st["question"][k] for k in ("stage", "kind", "mode")} == {
        "stage": stage,
        "kind": kind,
        "mode": "after",
    }
    assert st["stage"] == stage
    assert (await env.engine.answer(task_id, "continue", "mcp"))["status"] == "queued"
    st = await wait_status(env.engine, task_id, "completed", "failed")
    assert st["status"] == "completed", st
    _, resumed = launches(env)
    assert passed_call(resumed)["args"]["answers"] == [{"k": 1, "label": stage, "mode": "after"}]
    steps = await steps_by_label(env, task_id)
    assert steps[f"interpret-{stage}-1"]["cached"] is False
    assert steps["plan"]["cached"] is True


async def test_second_workflow_call_after_completed_return_killed_return_decides(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """As a late parent tool call: killed (an unclean end), the completed return decides; no
    corrective resume, no call retry used."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success_late_workflow")
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)  # the stub hangs: only a kill ends it
    assert st["status"] == "completed" and st["error"] is None, st
    task = await task_row(task_id)
    assert (task.kill_reason, task.call_retries) == (LATE_WORKFLOW_CALL, 0)
    assert await events_of(task_id, LATE_WORKFLOW_CALL) == [{"tool": "Workflow"}]
    assert await events_of(task_id, "second_workflow_call") == []
    (attempt,) = await rows("SELECT outcome, reason FROM attempts WHERE task_id = ?", task_id)
    assert (attempt["outcome"], attempt["reason"]) == ("success", LATE_WORKFLOW_CALL)
    await env.engine.stop()
    assert len(launches(env)) == 1


def _transcript(path: Path, first: str, last: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [{"type": "user", "timestamp": first}, {"type": "result", "timestamp": last}]
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


async def test_upsert_prefers_measured_times(env: Env) -> None:
    task_id = await lifecycle(env)
    home, cwd, session, run_id = env.settings.claude_home, str(env.repo), "sess", "wf_run"
    store = env.engine.runstore
    task = dataclasses.replace(await task_row(task_id), session_id=session, run_id=run_id)
    steps = [
        JournalStep("k1", "setup", "Setup", "completed", {}, 1, "asetup"),
        JournalStep("k2", "plan", "Plan", "completed", {}, 1, "aplan"),
        JournalStep("k3", "implement", "Implement", "running", None, 1, "aimpl"),
    ]
    # `setup` (stamped 8 s apart) and the running `implement` have only a live transcript; `plan`
    # has none, only an empty run-store copy.
    for agent in ("asetup", "aimpl"):
        live = step_transcript_path(cwd, session, run_id, home, agent)
        _transcript(live, "2026-01-02T19:40:01Z", "2026-01-02T19:40:09Z")
    store.step_copy_path(task_id, "plan").parent.mkdir(parents=True, exist_ok=True)
    store.step_copy_path(task_id, "plan").write_text("", encoding="utf-8")
    times = engine_lifecycle._measured_times(
        store, task_id, steps, {}, cwd, session, run_id, home, None
    )
    assert times["plan"] == (None, None)
    async with write_tx() as db:
        for step in steps:
            await engine_lifecycle._upsert(
                db, task_id, 1, step, None, [], (None, None), times[step.label]
            )
    source_of = engine_lifecycle.step_source_of(store, task, home)
    by_label = {
        s["label"]: s for s in await engine_lifecycle.step_list(task_id, "running", source_of)
    }
    assert {k: v["timing"] for k, v in by_label.items()} == {
        "setup": "measured", "plan": "observed", "implement": "measured",
    }  # fmt: skip
    (row,) = await rows("SELECT started_at, finished_at FROM steps WHERE label = 'setup'")
    assert row["finished_at"] - row["started_at"] == 8
    (impl,) = await rows("SELECT started_at, finished_at FROM steps WHERE label = 'implement'")
    assert impl["started_at"] == row["started_at"] and impl["finished_at"] is None
    (plan,) = await rows("SELECT started_at, finished_at FROM steps WHERE label = 'plan'")
    assert plan["finished_at"] >= plan["started_at"]  # observation times: no readable transcript
    # Without a transcript at all (live gone, no copy) the status is observed.
    live_setup = step_transcript_path(cwd, session, run_id, home, "asetup")
    live_setup.unlink()
    again = {s["label"]: s for s in await engine_lifecycle.step_list(task_id, "running", source_of)}
    assert again["setup"]["timing"] == "observed"


async def test_measured_times_not_reread_for_finished_steps(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    task_id = await lifecycle(env)
    home, cwd, session, run_id = env.settings.claude_home, str(env.repo), "sess", "wf_run"
    steps = [
        JournalStep("k1", "setup", "Setup", "completed", {}, 1, "asetup"),
        JournalStep("k2", "implement", "Implement", "running", None, 1, "aimpl"),
    ]
    for agent in ("asetup", "aimpl"):
        live = step_transcript_path(cwd, session, run_id, home, agent)
        _transcript(live, "2026-01-02T19:40:01Z", "2026-01-02T19:40:09Z")
    opened: list[str] = []
    real = journal.step_times

    def spy(path: Path) -> tuple[float | None, float | None]:
        opened.append(path.name)
        return real(path)

    monkeypatch.setattr(journal, "step_times", spy)
    store = env.engine.runstore
    args = (store, task_id, steps)
    times = engine_lifecycle._measured_times(*args, {}, cwd, session, run_id, home, None)
    assert sorted(opened) == ["agent-aimpl.jsonl", "agent-asetup.jsonl"]
    async with write_tx() as db:
        for step in steps:
            await engine_lifecycle._upsert(
                db, task_id, 1, step, None, [], (None, None), times[step.label]
            )
    found = {
        r["label"]: dict(r) for r in await rows("SELECT * FROM steps WHERE task_id = ?", task_id)
    }
    opened.clear()
    # The next mirror: `setup` is final in the DB (same agent); only the running step is read.
    later = engine_lifecycle._measured_times(*args, found, cwd, session, run_id, home, None)
    assert opened == ["agent-aimpl.jsonl"] and set(later) == {"implement"}
    # A restarted step (new agent id) is measured again.
    restarted = [JournalStep("k1", "setup", "Setup", "completed", {}, 2, "anewsetup"), steps[1]]
    opened.clear()
    engine_lifecycle._measured_times(
        store, task_id, restarted, found, cwd, session, run_id, home, None
    )
    assert len(opened) == 2  # `setup` (its new agent) is read again, with `implement`


async def test_upsert_running_step_takes_measured_start_only(env: Env) -> None:
    task_id = await lifecycle(env)
    step = JournalStep("k1", "implement", "Implement", "running", None, 1, "aimpl")
    start = 1_767_382_801.0
    async with write_tx() as db:
        await engine_lifecycle._upsert(
            db, task_id, 1, step, None, [], (None, None), (start, start + 9)
        )
    (row,) = await rows("SELECT started_at, finished_at FROM steps WHERE label = 'implement'")
    assert (row["started_at"], row["finished_at"]) == (start, None)
    # A later observation without a transcript keeps the measured start.
    async with write_tx() as db:
        await engine_lifecycle._upsert(db, task_id, 1, step, dict(row) | {
            "status": "running", "started_count": 1, "attempt": 1, "prev_started_count": 0,
            "git_branch": None, "git_head": None,
        }, [], (None, None), (None, None))  # fmt: skip
    (row,) = await rows("SELECT started_at FROM steps WHERE label = 'implement'")
    assert row["started_at"] == start


async def test_stage_rate_limit_falls_back_to_the_mapped_model(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lifecycle stage hits a usage limit (journaled as Claude Code does: started, no
    result): with a matching map the same run is resumed at once (no reset wait, no charge) on
    an agents file with the mapped models. The completed plan replays from cache; the limited
    stage runs again, on the mapped model, and the task completes."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "rate_limit_stage_unjournaled,success")
    env.settings.model_fallback = {"fable": "opus"}
    env.settings.rate_limit_min_wait_s = 0.25
    agents = env.settings.agents_dir
    (agents / "implementer.md").write_text(
        "---\ndescription: implementer\nmodel: fable\n---\nYou are implementer.\n"
    )
    task_id = await lifecycle(env)
    await env.engine.start()
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st
    found = await rows("SELECT outcome FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    assert [r["outcome"] for r in found] == ["rate_limited", "success"]
    task = await task_row(task_id)
    assert (task.model_fallback_applied, task.rate_limit_waited_s, task.resumes) == (1, 0.0, 0)
    whys = await rows(
        "SELECT type, data_json FROM events WHERE task_id = ? AND type IN "
        "('retry_scheduled', 'model_fallback') ORDER BY id",
        task_id,
    )
    assert [(r["type"], json.loads(r["data_json"]).get("why")) for r in whys] == [
        ("retry_scheduled", "rate_limited_fallback"), ("model_fallback", None),
    ]  # fmt: skip
    models = [
        json.loads(env.engine.runstore.attempt_agents_path(task_id, n).read_text()) for n in (1, 2)
    ]
    assert [m["implementer"]["model"] for m in models] == ["fable", "opus"]
    assert all("model" not in m["planner"] for m in models)
    first, second = launches(env)
    assert flag(second, "--resume") == task.session_id
    assert flag(second, "--agents") == str(env.engine.runstore.attempt_agents_path(task_id, 2))
    assert passed_call(second) == {**passed_call(first), "resumeFromRunId": task.run_id}
    assert task.run_id is not None and task.session_id is not None
    started = journal_started(env, task_id, task.run_id, task.session_id)
    assert started == ["plan", "implement", "implement"]  # plan cached, implement re-run
    steps = await steps_by_label(env, task_id)
    assert steps["plan"]["cached"] is True and steps["implement"]["cached"] is False
    counts = await rows("SELECT label, started_count FROM steps WHERE task_id = ?", task_id)
    assert {r["label"]: r["started_count"] for r in counts} == {"plan": 1, "implement": 2}
