import asyncio
import itertools
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from src import engine_watch, procs
from src.database import write_tx
from src.engine import Attempt, _next_mode
from src.mcp_server import RETURN_KEYS, ToolProvider
from src.runstore import RunStore
from src.serverlog import LOGGER_NAME
from src.stream import PermissionDenied, Result
from tests.helpers import (
    TIMEOUT_S,
    Env,
    rows,
    running_with_pid,
    stub_log,
    submit,
    task_row,
    wait_for,
    wait_status,
)
from tests.stub_claude import RUN_MODEL_TOKENS

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "orcai_tools.json"
pytestmark = pytest.mark.usefixtures("spawned")  # every test: no stub process left running


async def test_single_completes_with_cost_and_attempt_row(env: Env) -> None:
    sub = await submit(env)
    assert sub["status"] == "queued" and sub["deduplicated"] is False
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    assert st["output"] == {"text": "Done.", "tokens_used": 21104, "cost_usd": 0.0042}
    assert st["cost_usd"] == pytest.approx(0.0042)
    assert st["steps"] == [] and st["stage"] is None and st["completed_at"]
    task = await task_row(sub["task_id"])
    assert task.attempt == 1 and task.session_id and task.kill_reason is None
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["n"] == 1 and attempt["exit_code"] == 0 and attempt["outcome"] == "success"
    assert attempt["cost_usd"] == pytest.approx(0.0042) and attempt["tokens"] == 21104
    assert attempt["ended_at"] >= attempt["started_at"] and attempt["running_s"] >= 0
    assert attempt["session_id"] == task.session_id
    argv = stub_log(env.log)[0]["argv"]
    assert argv[argv.index("--agent") + 1] == "implementer" and "--model" not in argv
    assert argv[argv.index("--session-id") + 1] == task.session_id
    for name in ("agents.json", "mcp.json"):
        assert (env.settings.data_dir / name).is_file()
    assert not (env.settings.data_dir / "settings.json").exists()


async def test_concurrent_identical_submits_one_task(env: Env) -> None:
    results = await asyncio.gather(*(submit(env, "Same work") for _ in range(5)))
    assert len({r["task_id"] for r in results}) == 1
    assert sorted(r["deduplicated"] for r in results) == [False, True, True, True, True]
    assert len(await rows("SELECT id FROM tasks")) == 1


async def test_prompt_agent_lifecycle_agent_runs_single_not_deduped(env: Env) -> None:
    await env.engine.start()
    provider = ToolProvider(prompt_wait_s=TIMEOUT_S)
    provider.attach(env.engine)
    first = await provider.prompt_agent("team-leader", "Quick question", None, True)
    second = await provider.prompt_agent("team-leader", "Quick question", None, True)
    assert first["status"] == second["status"] == "completed"
    assert first["response"] == "Done." and first["tokens_used"] == 21104
    assert first["task_id"] != second["task_id"]
    for task_id in (first["task_id"], second["task_id"]):
        task = await task_row(task_id)
        assert (task.kind, task.agent, task.priority) == ("single", "lead", 4)
        assert task.branch is None and task.worktree is None
    assert all("--agent" in entry["argv"] for entry in stub_log(env.log) if "argv" in entry)


async def test_cancel_during_backoff_kills_nothing(env: Env) -> None:
    sub = await submit(env)
    sleeper = subprocess.Popen(["/bin/sleep", "300"])
    try:
        start = procs.proc_start_time(sleeper.pid)
        assert start is not None
        async with write_tx() as db:
            await db.execute(
                "UPDATE tasks SET attempt = 1, pid = ?, pid_start = ?, next_attempt_at = ? "
                "WHERE id = ?",
                (sleeper.pid, start - 100, time.time() + 600, sub["task_id"]),
            )
        result = await env.engine.cancel(sub["task_id"])
        assert result == {"task_id": sub["task_id"], "status": "cancelled"}
        assert sleeper.poll() is None and procs.is_running(sleeper.pid)
        task = await task_row(sub["task_id"])
        assert (task.status, task.kill_reason) == ("cancelled", "cancel")
        assert task.completed_at is not None
        assert (await env.engine.cancel(sub["task_id"]))["error"] == "task is cancelled"
    finally:
        sleeper.kill()
        sleeper.wait()


async def test_cancel_in_spawn_window_kills_new_process(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    seen: list[tuple[int, float | None]] = []

    async def cancel_now(task_id: str, pid: int, start: float | None) -> None:
        seen.append((pid, start))
        assert (await env.engine.cancel(task_id))["status"] == "cancelled"
        assert (await task_row(task_id)).pid is None  # the pid is not stored yet

    env.engine._after_spawn = cancel_now
    sub = await submit(env)
    await env.engine.start()

    async def new_process_dead() -> bool:
        return bool(seen) and not procs.alive_and_same(*seen[0])

    await wait_for(new_process_dead)
    task = await task_row(sub["task_id"])
    assert (task.status, task.kill_reason, task.pid) == ("cancelled", "cancel", seen[0][0])

    async def attempt_closed() -> bool:
        found = await rows("SELECT ended_at FROM attempts WHERE task_id = ?", sub["task_id"])
        return bool(found) and found[0]["ended_at"] is not None

    await wait_for(attempt_closed)
    assert (await task_row(sub["task_id"])).status == "cancelled"


async def test_cas_clears_stale_kill_reason(env: Env) -> None:
    sub = await submit(env)
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET attempt = 1, kill_reason = 'call_mismatch', "
            "session_id = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee' WHERE id = ?",
            (sub["task_id"],),
        )
    queued = await task_row(sub["task_id"])
    stale = replace(queued, attempt=0)
    assert await env.engine._launch(stale, "resume") is None  # loses the compare-and-set
    assert stub_log(env.log) == []

    attempt = await env.engine._launch(queued, "resume")
    assert attempt is not None
    task = await task_row(sub["task_id"])
    assert (task.status, task.attempt, task.kill_reason) == ("running", 2, None)
    assert task.session_id == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    await env.engine._watch(attempt)
    task = await task_row(sub["task_id"])
    assert task.status == "completed"
    argv = stub_log(env.log)[0]["argv"]
    assert argv[argv.index("--resume") + 1] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


async def test_parent_tool_violation_killed(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "parent_bash_hang")
    env.settings.call_mismatch_retries = 0  # no corrective resume: the first kill is final
    sub = await submit(env, "Build the thing", agent="team-leader")
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "failed" and "parent_violation" in st["error"]
    task = await task_row(sub["task_id"])
    assert task.kill_reason == "parent_violation"
    types = [r["type"] for r in await rows("SELECT type FROM events WHERE task_id = ?", task.id)]
    assert "parent_tool_violation" in types


async def test_single_parent_tool_use_is_normal(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "parent_bash")
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert (await task_row(sub["task_id"])).kill_reason is None


async def test_busy_counts(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    provider = ToolProvider()
    provider.attach(env.engine)
    assert await env.engine.busy_agents() == set()
    sub = await submit(env)
    await submit(env, "Second", agent="qa")
    assert (await provider.task_counts())["queued"] == 2
    env.settings.max_concurrent_tasks = 1
    await env.engine.start()
    await running_with_pid(env.engine, sub["task_id"])
    assert await env.engine.busy_agents() == {"implementer"}
    active = await provider.get_active_agents()
    assert [a["id"] for a in active["agents"]] == ["implementer"]
    assert active["active_count"] == 1 and active["queue_depth"] == 1
    assert await provider.task_counts() == {"queued": 1, "running": 1, "needs_human": 0}
    busy = await provider.get_agents(role=None, status="busy")
    assert [a["id"] for a in busy["agents"]] == ["implementer"]
    await env.engine.cancel(sub["task_id"])
    (second,) = await rows("SELECT id FROM tasks WHERE agent = 'qa'")
    await running_with_pid(env.engine, second["id"])
    assert await env.engine.busy_agents() == {"qa"}
    await env.engine.cancel(second["id"])
    assert await env.engine.busy_agents() == set()


async def running_ids(env: Env) -> list[str]:
    return [r["id"] for r in await rows("SELECT id FROM tasks WHERE status = 'running'")]


async def test_concurrency_cap(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    env.settings.max_concurrent_tasks = 2
    ids = [(await submit(env, f"Work {i}"))["task_id"] for i in range(3)]
    await env.engine.start()

    async def two_running() -> list[str] | None:
        found = await running_ids(env)
        return found if len(found) == 2 else None

    first = await wait_for(two_running)
    await env.engine._schedule_once()
    await env.engine._schedule_once()
    assert sorted(await running_ids(env)) == sorted(first)
    (waiting,) = set(ids) - set(first)
    assert (await task_row(waiting)).status == "queued"
    await env.engine.cancel(first[0])
    await running_with_pid(env.engine, waiting)
    for task_id in await running_ids(env):
        await env.engine.cancel(task_id)


async def test_stop_detaches_process_alive(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    sub = await submit(env)
    await env.engine.start()
    task = await running_with_pid(env.engine, sub["task_id"])
    await env.engine.stop()
    assert procs.alive_and_same(task.pid or 0, task.pid_start)
    assert (await task_row(sub["task_id"])).status == "running"
    procs.kill_tree(task.pid or 0, task.pid_start, task.id, grace_s=2.0)
    assert not procs.alive_and_same(task.pid or 0, task.pid_start)


async def test_list_tasks_keyset_stable_under_inserts(env: Env) -> None:
    ids = [(await submit(env, f"Task {i}"))["task_id"] for i in range(5)]
    page1 = await env.engine.list(None, None, 2, None)
    assert [t["task_id"] for t in page1["tasks"]] == ids[::-1][:2]
    for i in range(3):
        await submit(env, f"Later {i}")
    page2 = await env.engine.list(None, None, 2, page1["next_cursor"])
    page3 = await env.engine.list(None, None, 2, page2["next_cursor"])
    seen = [t["task_id"] for t in page1["tasks"] + page2["tasks"] + page3["tasks"]]
    assert seen == ids[::-1] and page3["next_cursor"] is None
    filtered = await env.engine.list("queued", "dev", 50, None)
    assert len(filtered["tasks"]) == 8
    assert (await env.engine.list(None, "qa", 50, None))["tasks"] == []
    assert "error" in await env.engine.list(None, None, 2, "not-a-cursor")


async def test_submit_rejects_bad_input(env: Env, tmp_path: Path) -> None:
    cases: list[tuple[str, str, Any]] = [
        ("nobody", "Do it", None),
        ("implementer", "Do it", {"project_dir": str(tmp_path / "missing")}),
        ("implementer", "Do it", ["not", "an", "object"]),
        ("implementer", "Bad \ud800 text", None),
        ("implementer", "Do it", {"note": "bad \udfff"}),
        ("implementer", "Do it", {"value": float("nan")}),
        ("implementer", "Do it", {"value": object()}),
    ]
    for agent, description, context in cases:
        result = await env.engine.submit(agent, description, context, 3, 0)
        assert result["status"] == "rejected" and result["error"], (agent, description)
    assert await rows("SELECT id FROM tasks") == []


async def test_submit_rejects_when_queue_full(env: Env) -> None:
    env.settings.task_queue_size = 1
    assert (await submit(env, "One"))["status"] == "queued"
    assert (await submit(env, "Two"))["status"] == "rejected"


async def test_similar_active_task_event(env: Env) -> None:
    first = await submit(env, "Fix the bug")
    async with write_tx() as db:
        sql = "UPDATE tasks SET status = 'needs_human' WHERE id = ?"
        await db.execute(sql, (first["task_id"],))
    second = await submit(env, "Fix the bug")
    assert second["task_id"] != first["task_id"] and second["deduplicated"] is False
    events = await rows("SELECT data_json FROM events WHERE type = 'similar_active_task'")
    assert [json.loads(e["data_json"])["similar"] for e in events] == [first["task_id"]]


async def test_spawn_failure_fails_task(env: Env, tmp_path: Path) -> None:
    env.settings.claude_bin = str(tmp_path / "no-such-claude")
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["status"] == "failed" and "spawn" in st["error"]
    (attempt,) = await rows("SELECT outcome FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["outcome"] == "fatal"
    events = await rows("SELECT type FROM events WHERE task_id = ?", st["task_id"])
    types = [r["type"] for r in events]
    assert "spawn_failed" in types


async def test_lifecycle_task_stored_with_branch_and_worktree(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, "Add a Health endpoint, quickly!", agent="team-leader")
    task = await task_row(sub["task_id"])
    id8 = task.id[:8]
    assert (task.kind, task.agent, task.base_branch) == ("lifecycle", "lead", "main")
    assert task.branch == f"dioxide/add-a-health-endpoint-quickly-{id8}"
    assert task.cwd == os.path.realpath(env.repo)
    assert task.worktree is not None and Path(task.worktree).name == id8
    assert Path(task.worktree).parent.parent == env.settings.worktrees_dir
    assert task.args_dict == {"task_id": task.id, "run_dir": str(task.run_dir)}
    brief = RunStore(env.settings.runs_dir).paths(task.id).brief_path
    assert brief.read_text().startswith("Add a Health endpoint, quickly!")
    await env.engine.start()
    st = await wait_status(env.engine, task.id, "failed", "completed")
    assert st["status"] == "completed", st
    frozen = (await task_row(task.id)).args_dict  # full args frozen before the first claim
    assert frozen["task_id"] == task.id and frozen["branch"] == task.branch
    assert frozen["answers"] == [] and frozen["merge_strategy"] == "squash"
    assert len(stub_log(env.log)) == 1


async def test_lifecycle_rejected_outside_git(env: Env, tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    context = {"project_dir": str(plain)}
    rejected = await submit(env, "Build", agent="team-leader", input_context=context)
    assert rejected["status"] == "rejected"
    single = await submit(env, "Build", input_context=context)
    assert (await task_row(single["task_id"])).base_branch is None


async def test_descendants_recorded_during_run(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "orphan_bash_group")
    sub = await submit(env, "Build", agent="team-leader")
    await env.engine.start()

    def child_pid() -> int | None:
        return next((e["bg_child_pid"] for e in stub_log(env.log) if "bg_child_pid" in e), None)

    async def recorded() -> int | None:
        pid = child_pid()
        store = RunStore(env.settings.runs_dir)
        return pid if pid is not None and pid in store.read_procs(sub["task_id"]) else None

    child = await wait_for(recorded)
    await env.engine.cancel(sub["task_id"])
    await wait_for(lambda: _gone(child))


async def _gone(pid: int) -> bool:
    return not procs.is_running(pid)


def sleeper_dead_at_spawn(
    monkeypatch: pytest.MonkeyPatch, sleeper: subprocess.Popen[bytes]
) -> list[bool]:
    """Record, at every spawn, whether the recorded leftover was already gone."""
    seen: list[bool] = []
    inner = procs.spawn

    def checking(*args: Any, **kwargs: Any) -> tuple[int, float | None]:
        seen.append(sleeper.poll() is not None)
        return inner(*args, **kwargs)

    monkeypatch.setattr(procs, "spawn", checking)
    return seen


async def test_sweep_before_launch(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    sub = await submit(env)
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        start_new_session=True,
    )
    dead_at_spawn = sleeper_dead_at_spawn(monkeypatch, sleeper)
    try:
        start = procs.proc_start_time(sleeper.pid)
        assert start is not None
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await env.engine.start()
        await wait_status(env.engine, sub["task_id"], "completed", "failed")
        assert dead_at_spawn == [True]
        events = await rows("SELECT type FROM events WHERE task_id = ? ORDER BY id", sub["task_id"])
        types = [e["type"] for e in events]
        # M1: the sweep runs after the claim (CAS won) and before the spawn.
        assert types.index("attempt_started") < types.index("sweep")
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
        sleeper.wait()


def flat_keys(payload: dict[str, Any]) -> set[str]:
    keys = set()
    for key, value in payload.items():
        keys.add(key)
        items = value if isinstance(value, list) else [value]
        for item in items:
            if isinstance(item, dict):
                keys |= {f"{key}.{sub}" for sub in item}
    return keys


async def test_tool_output_keys_within_return_keys(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    orcai = json.loads(FIXTURE.read_text())["return_keys"]
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "single_success,hang")
    await env.engine.start()
    provider = ToolProvider(prompt_wait_s=TIMEOUT_S)
    provider.attach(env.engine)
    prompted = await provider.prompt_agent("dev", "Say done", None, True)
    delegated = await provider.delegate_task("dev", "Keep going", None, 3, 0)
    await running_with_pid(env.engine, delegated["task_id"])
    outputs = {
        "prompt_agent": prompted,
        "delegate_task": delegated,
        "check_task_status": await provider.check_task_status(prompted["task_id"]),
        "get_agents": await provider.get_agents(None, None),
        "get_active_agents": await provider.get_active_agents(),
    }
    for tool, payload in outputs.items():
        keys = flat_keys(payload)
        assert keys <= RETURN_KEYS[tool], (tool, keys - RETURN_KEYS[tool])
        assert keys | {"error"} >= set(orcai[tool]), (tool, set(orcai[tool]) - keys)
    await provider.cancel_task(delegated["task_id"])


async def test_permission_denials_recorded_once_per_tool(env: Env) -> None:
    sub = await submit(env)
    task = await task_row(sub["task_id"])
    attempt = Attempt(task, "first", 1, [], env.tmp / "s", env.tmp / "e", time.time())
    denials = [{"tool_name": "Bash", "tool_use_id": "t1", "tool_input": {}}]
    result = Result(False, "success", None, None, "ok", 0.0, 0, None, [], denials, 0)
    new = [PermissionDenied("Bash", "needs approval"), PermissionDenied("Edit", None), result]
    await env.engine._inspect(attempt, new, {"calls": 0, "killed": False, "denied": set()})
    events = await rows("SELECT data_json FROM events WHERE type = 'permission_denied'")
    assert sorted(json.loads(e["data_json"])["tool"] for e in events) == ["Bash", "Edit"]


def start_sleeper() -> tuple[subprocess.Popen[bytes], float]:
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"], start_new_session=True
    )
    start = procs.proc_start_time(sleeper.pid)
    assert start is not None
    return sleeper, start


async def event_types(task_id: str) -> list[str]:
    found = await rows("SELECT type FROM events WHERE task_id = ? ORDER BY id", task_id)
    return [e["type"] for e in found]


async def test_sweep_survivors_retried_with_sigkill(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()
    graces: list[float] = []
    real_sweep = procs.sweep

    def survives_once(
        task_id: str, recorded: Any, grace_s: float = 10.0, spare: Any = None
    ) -> procs.Swept:
        graces.append(grace_s)
        if len(graces) == 1:
            return procs.Swept([], [])
        return real_sweep(task_id, recorded, grace_s, spare)

    monkeypatch.setattr(procs, "sweep", survives_once)
    dead_at_spawn = sleeper_dead_at_spawn(monkeypatch, sleeper)
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await env.engine.start()
        st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
        assert st["status"] == "completed" and graces[:2] == [10.0, 0.0]
        assert dead_at_spawn == [True]
        types = await event_types(sub["task_id"])
        assert types.index("attempt_started") < types.index("sweep_survivors")
        assert types.index("sweep_survivors") < types.index("sweep")
        survivors = await rows("SELECT data_json FROM events WHERE type = 'sweep_survivors'")
        assert json.loads(survivors[0]["data_json"])["pids"] == [sleeper.pid]
        sweeps = await rows("SELECT data_json FROM events WHERE type = 'sweep' ORDER BY id")
        data = json.loads(sweeps[0]["data_json"])
        assert (data["policy"], data["trigger"]) == ("swept", "pre_launch")
        assert data["killed"] == [sleeper.pid] and data["survivors"] == []
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
        sleeper.wait()


async def test_sweep_survivors_requeue_never_launch(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()
    monkeypatch.setattr(procs, "sweep", lambda *args, **kwargs: procs.Swept([], []))
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        before = time.time()
        assert await env.engine._launch(await task_row(sub["task_id"]), "first") is None
        task = await task_row(sub["task_id"])
        assert (task.status, task.attempt, task.pid) == ("queued", 1, None)
        assert task.next_attempt_at is not None and task.next_attempt_at >= before + 30
        assert stub_log(env.log) == [] and sleeper.poll() is None
        (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", sub["task_id"])
        assert attempt["outcome"] == "sweep_survivors" and attempt["ended_at"] is not None
        assert attempt["reason"] is None and json.loads(attempt["detail"]) == [sleeper.pid]
        types = await event_types(sub["task_id"])
        assert types.count("sweep_survivors") == 2 and "sweep_requeued" in types
        assert "launch_failed" not in types
        await env.engine._schedule_once()  # not due yet: nothing is launched over the orphan
        assert (await task_row(sub["task_id"])).status == "queued"
        assert _next_mode(task) == "first"  # it never spawned
    finally:
        sleeper.kill()
        sleeper.wait()


async def test_bookkeeping_failures_do_not_abort_watch(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(env.engine.runstore, "write_procs", broken)
    monkeypatch.setattr(env.engine.runstore, "append_event", broken)
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    assert "attempt_finished" in await event_types(sub["task_id"])  # DB events still written


async def test_classify_error_fails_task_and_frees_slot(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = engine_watch.classify
    calls: list[int] = []

    def fails_once(**kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("classifier broke")
        return real(**kwargs)

    monkeypatch.setattr(engine_watch, "classify", fails_once)
    env.settings.max_concurrent_tasks = 1
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    first = await submit(env, "First")
    second = await submit(env, "Second")
    await env.engine.start()
    st = await wait_status(env.engine, first["task_id"], "completed", "failed")
    assert st["status"] == "failed"
    assert st["error"] == "engine error: ValueError: classifier broke"
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", first["task_id"])
    # The decider treats an engine error as a transient end; with no budget left it fails.
    assert attempt["ended_at"] is not None and attempt["outcome"] == "transient"
    assert (attempt["reason"], attempt["detail"]) == ("engine_error", st["error"])
    assert "engine_error" in await event_types(first["task_id"])
    assert (await wait_status(env.engine, second["task_id"], "completed"))["status"] == "completed"


async def test_engine_error_kills_live_process(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")

    async def broken_wait(attempt: Attempt, interval: float) -> tuple[bool, int | None]:
        raise RuntimeError("watch broke")

    env.engine._wait_exit = broken_wait  # type: ignore[method-assign]
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"] == "engine error: RuntimeError: watch broke"
    task = await task_row(sub["task_id"])
    assert task.pid is not None and not procs.alive_and_same(task.pid, task.pid_start)


async def test_spawn_non_oserror_fails_task(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken_env(*args: Any, **kwargs: Any) -> dict[str, str]:
        raise RuntimeError("bad environment")

    monkeypatch.setattr("src.engine.launch_env", broken_env)
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"] == "spawn failed: bad environment"
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", st["task_id"])
    assert attempt["outcome"] == "fatal" and attempt["ended_at"] is not None
    assert (attempt["reason"], attempt["detail"]) == ("spawn_failed", st["error"])


async def set_running(task_id: str, pid: int, pid_start: float | None) -> None:
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'running', attempt = 1, pid = ?, pid_start = ? WHERE id = ?",
            (pid, pid_start, task_id),
        )


async def wait_event(task_id: str, type_: str) -> None:
    async def seen() -> bool:
        return type_ in await event_types(task_id)

    await wait_for(seen)


async def test_cancel_running_stale_start_kills_nothing(env: Env) -> None:
    sub = await submit(env)
    sleeper = subprocess.Popen(["/bin/sleep", "300"])
    try:
        start = procs.proc_start_time(sleeper.pid)
        assert start is not None
        await set_running(sub["task_id"], sleeper.pid, start - 100)
        assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
        await wait_event(sub["task_id"], "kill_skipped")
        assert sleeper.poll() is None
    finally:
        sleeper.kill()
        sleeper.wait()


async def test_kill_skipped_without_start_time(env: Env) -> None:
    sub = await submit(env)
    sleeper = subprocess.Popen(["/bin/sleep", "300"])
    try:
        await set_running(sub["task_id"], sleeper.pid, None)
        await env.engine.cancel(sub["task_id"])
        await wait_event(sub["task_id"], "kill_skipped_no_start_time")
        assert sleeper.poll() is None
    finally:
        sleeper.kill()
        sleeper.wait()


async def test_wait_exit_without_start_time_ends_when_pid_gone(env: Env) -> None:
    gone = subprocess.Popen(["/bin/sleep", "0"])
    gone.wait()
    task = await task_row((await submit(env))["task_id"])
    attempt = Attempt(task, "first", 1, [], env.tmp / "s", env.tmp / "e", time.time(), gone.pid)
    assert await engine_watch.wait_exit(attempt, 5.0) == (True, None)


async def test_cancel_returns_before_kill_and_keeps_slot(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    release = threading.Event()
    real_kill = procs.kill_tree

    def slow_kill(*args: Any, **kwargs: Any) -> procs.Swept:
        release.wait(TIMEOUT_S)
        return real_kill(*args, **kwargs)

    monkeypatch.setattr(procs, "kill_tree", slow_kill)
    env.settings.max_concurrent_tasks = 1
    first = await submit(env, "First")
    second = await submit(env, "Second")
    await env.engine.start()
    task = await running_with_pid(env.engine, first["task_id"])
    assert (await env.engine.cancel(first["task_id"]))["status"] == "cancelled"
    assert not release.is_set()  # cancel returned while the kill is still blocked
    await env.engine._schedule_once()
    assert (await task_row(second["task_id"])).status == "queued"  # the dying task keeps its slot
    release.set()
    await running_with_pid(env.engine, second["task_id"])
    assert not procs.alive_and_same(task.pid or 0, task.pid_start)
    await env.engine.cancel(second["task_id"])


async def test_cancel_after_exit_sends_no_signal(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    killed: list[Any] = []
    signals: list[tuple[int, int]] = []
    real_os_kill = os.kill
    real_wait = env.engine._wait_exit

    def spy_kill_tree(*args: Any, **kwargs: Any) -> None:
        killed.append(args)

    def spy_os_kill(pid: int, sig: int) -> None:
        if sig != 0:
            signals.append((pid, sig))
        real_os_kill(pid, sig)

    async def cancel_after_exit(attempt: Attempt, interval: float) -> tuple[bool, int | None]:
        ended, code = await real_wait(attempt, interval)
        if ended:
            assert (await env.engine.cancel(attempt.task.id))["status"] == "cancelled"
        return ended, code

    monkeypatch.setattr(procs, "kill_tree", spy_kill_tree)
    monkeypatch.setattr(os, "kill", spy_os_kill)
    env.engine._wait_exit = cancel_after_exit  # type: ignore[method-assign]
    sub = await submit(env)
    await env.engine.start()
    await wait_event(sub["task_id"], "kill_skipped")

    async def attempt_closed() -> bool:
        found = await rows("SELECT ended_at FROM attempts WHERE task_id = ?", sub["task_id"])
        return bool(found) and found[0]["ended_at"] is not None

    await wait_for(attempt_closed)
    task = await task_row(sub["task_id"])
    assert task.status == "cancelled"
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["ended_at"] is not None and attempt["exit_code"] == 0
    assert killed == [] and not [s for s in signals if s[0] == task.pid]


async def test_failed_error_has_exit_code_and_stderr(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["error"] == "transient (exit 1): Error: stub failure"


async def test_get_active_agents_redacted(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    secret = "sk-ant-" + "a1" * 12
    agent = env.engine.agents["implementer"]
    env.engine.agents["implementer"] = replace(agent, description=f"uses {secret}")
    provider = ToolProvider()
    provider.attach(env.engine)
    sub = await submit(env)
    await env.engine.start()
    await running_with_pid(env.engine, sub["task_id"])
    active = await provider.get_active_agents()
    assert active["agents"][0]["description"] == "uses ****"
    await env.engine.cancel(sub["task_id"])


async def test_submit_validates_priority_and_retries(env: Env) -> None:
    for priority, retries in [(0, 0), (6, 0), (True, 0), ("3", 0), (3, -1), (3, 11), (3, 1.5)]:
        result = await env.engine.submit("implementer", "Do it", None, priority, retries)
        assert result["status"] == "rejected", (priority, retries)
    assert (await env.engine.submit("implementer", "Do it", None, 5, 10))["status"] == "queued"


async def test_position_counts_tasks_scheduled_first(env: Env) -> None:
    a = await env.engine.submit("implementer", "A", None, 3, 0)
    b = await env.engine.submit("implementer", "B", None, 3, 0)
    c = await env.engine.submit("implementer", "C", None, 5, 0)
    assert [a["position"], b["position"], c["position"]] == [0, 1, 0]
    again = await env.engine.submit("implementer", "B", None, 3, 0)
    assert again["deduplicated"] and again["position"] == 2  # C (priority 5) and A first
    await env.engine.cancel(a["task_id"])
    assert (await env.engine.submit("implementer", "A", None, 3, 0))["position"] == 2


async def test_inputs_write_failure_rejects_without_row(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = env.engine.runstore.write_inputs

    def broken(task_id: str, *args: Any, **kwargs: Any) -> None:
        real(task_id, "partial")  # a half-written run dir is removed too
        raise OSError("read-only file system")

    monkeypatch.setattr(env.engine.runstore, "write_inputs", broken)
    provider = ToolProvider()
    provider.attach(env.engine)
    result = await provider.delegate_task("implementer", "Do it", None, 3, 0)
    assert result["status"] == "rejected" and "read-only file system" in result["error"]
    assert await rows("SELECT id FROM tasks") == [] and await rows("SELECT id FROM events") == []
    assert run_dirs(env) == []


def run_dirs(env: Env) -> list[str]:
    runs = env.settings.runs_dir
    return sorted(p.name for p in runs.iterdir()) if runs.is_dir() else []


async def test_inputs_written_before_row_and_no_stray_run_dir(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = env.engine.runstore.write_inputs
    rows_at_write: list[int] = []

    def checking(task_id: str, *args: Any, **kwargs: Any) -> None:
        rows_at_write.append(len(get_db_rows_sync(env, task_id)))
        real(task_id, *args, **kwargs)

    monkeypatch.setattr(env.engine.runstore, "write_inputs", checking)
    first = await submit(env, "Same work")
    again = await submit(env, "Same work")  # dedupe hit
    assert again["deduplicated"] and again["task_id"] == first["task_id"]
    assert rows_at_write == [0, 0]  # inputs are on disk before the row exists
    assert run_dirs(env) == [first["task_id"]]
    env.settings.task_queue_size = 1
    assert (await submit(env, "Other work"))["status"] == "rejected"  # queue full
    assert run_dirs(env) == [first["task_id"]]

    async def failing_insert(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("insert broke")

    env.settings.task_queue_size = 20
    monkeypatch.setattr(env.engine, "_similar", failing_insert)
    with pytest.raises(RuntimeError):
        await submit(env, "Third work")
    assert run_dirs(env) == [first["task_id"]]
    assert len(await rows("SELECT id FROM tasks")) == 1


def get_db_rows_sync(env: Env, task_id: str) -> list[Any]:
    with sqlite3.connect(env.settings.db_path) as conn:
        return conn.execute("SELECT id FROM tasks WHERE id = ?", (task_id,)).fetchall()


async def test_wait_exit_ps_off_the_loop(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeper, start = start_sleeper()  # not spawned by the engine: liveness needs `ps`
    try:
        task = await task_row((await submit(env))["task_id"])
        attempt = Attempt(task, "first", 1, [], env.tmp / "s", env.tmp / "e", time.time())
        attempt = replace(attempt, pid=sleeper.pid, start=start)

        def slow_ps(cmd: list[str]) -> str:
            time.sleep(0.5)
            return ""

        monkeypatch.setattr(procs, "_run", slow_ps)
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.02)
                ticks += 1

        job = asyncio.create_task(ticker())
        try:
            await engine_watch.wait_exit(attempt, 2.0)
        finally:
            job.cancel()
        assert ticks >= 10  # the loop kept running while `ps` hung
    finally:
        sleeper.kill()
        sleeper.wait()


async def test_wait_exit_trusts_reap_for_own_child(
    env: Env, monkeypatch: pytest.MonkeyPatch, spawned: list[tuple[int, float | None]]
) -> None:
    pid, start = procs.spawn(
        [sys.executable, "-c", "import time; time.sleep(0.6)"],
        env.tmp, dict(os.environ), env.tmp / "o", env.tmp / "e",
    )  # fmt: skip
    task = await task_row((await submit(env))["task_id"])
    attempt = Attempt(task, "first", 1, [], env.tmp / "s", env.tmp / "e", time.time(), pid, start)
    calls: list[list[str]] = []
    real_run = procs._run
    monkeypatch.setattr(procs, "_run", lambda cmd: calls.append(cmd) or real_run(cmd))
    assert await engine_watch.wait_exit(attempt, 0.3) == (False, None)
    assert await engine_watch.wait_exit(attempt, 5.0) == (True, 0)
    assert calls == []  # the Popen decides; no `ps`


async def test_cancel_with_parent_gone_sweeps_recorded(env: Env) -> None:
    sub = await submit(env)
    dead = subprocess.Popen(["/bin/sleep", "0"])
    dead.wait()
    sleeper, start = start_sleeper()
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await set_running(sub["task_id"], dead.pid, 1.0)
        assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
        await wait_event(sub["task_id"], "kill_skipped")

        async def sleeper_dead() -> bool:
            return sleeper.poll() is not None

        await wait_for(sleeper_dead)
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
        sleeper.wait()


async def test_cancel_without_waiter_closes_attempt_and_frees_slot(env: Env) -> None:
    env.settings.max_concurrent_tasks = 1
    stale = await submit(env, "Left running by a previous process")
    now = time.time()
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'running', attempt = 1, started_at = ? WHERE id = ?",
            (now - 5, stale["task_id"]),
        )
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 1, ?)",
            (stale["task_id"], now - 5),
        )
    assert (await env.engine.cancel(stale["task_id"]))["status"] == "cancelled"
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", stale["task_id"])
    assert attempt["ended_at"] is not None and attempt["outcome"] == "cancelled"
    assert attempt["reason"] == "cancel" and attempt["running_s"] >= 5
    fresh = await submit(env, "New work")
    await env.engine._schedule_once()
    assert fresh["task_id"] in env.engine._launching or env.engine._jobs
    st = await wait_status(env.engine, fresh["task_id"], "completed", "failed")
    assert st["status"] == "completed"


async def test_cancel_during_sweep_never_spawns(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    env.settings.max_concurrent_tasks = 1
    first = await submit(env, "First")
    loop = asyncio.get_running_loop()
    real_sweep = procs.sweep
    cancelled: list[str] = []

    def cancelling_sweep(
        task_id: str, recorded: Any, grace_s: float = 10.0, spare: Any = None
    ) -> procs.Swept:
        if task_id == first["task_id"] and not cancelled:
            cancelled.append(task_id)
            future = asyncio.run_coroutine_threadsafe(env.engine.cancel(task_id), loop)
            assert future.result(TIMEOUT_S)["status"] == "cancelled"
        return real_sweep(task_id, recorded, grace_s, spare)

    monkeypatch.setattr(procs, "sweep", cancelling_sweep)
    assert await env.engine._launch(await task_row(first["task_id"]), "first") is None
    assert cancelled and stub_log(env.log) == []  # nothing was spawned
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", first["task_id"])
    assert attempt["ended_at"] is not None and attempt["outcome"] == "cancelled"
    assert attempt["running_s"] == 0
    assert (await task_row(first["task_id"])).status == "cancelled"
    second = await submit(env, "Second")
    await env.engine._schedule_once()  # the slot is free
    st = await wait_status(env.engine, second["task_id"], "completed", "failed")
    assert st["status"] == "completed"


async def test_run_finally_keeps_newer_attempt_entry(env: Env, tmp_path: Path) -> None:
    sub = await submit(env, "Overlapping jobs")
    task = await task_row(sub["task_id"])
    engine = env.engine

    def attempt(n: int) -> Attempt:
        paths = (tmp_path / f"{n}.jsonl", tmp_path / f"{n}.err")
        return Attempt(replace(task, attempt=n), "first", n, [], *paths, time.time())

    old, newer = attempt(1), attempt(2)

    async def launch(t: Any, mode: Any, owned: list[Attempt]) -> Attempt:
        engine._register(old, owned)
        return old

    async def watch(a: Attempt) -> None:  # a newer job registers while the old one finishes
        engine._attempts[a.task.id] = newer

    engine._launch = launch  # type: ignore[method-assign]
    engine._watch = watch  # type: ignore[method-assign]
    await engine._run(task, "first")
    assert engine._attempts[task.id] is newer
    engine._watch = lambda a: asyncio.sleep(0)  # type: ignore[method-assign,assignment,return-value]
    engine._attempts[task.id] = old
    await engine._run(task, "first")
    assert task.id not in engine._attempts  # its own entry is still removed


async def test_finish_attempt_keeps_row_closed_by_cancel(env: Env, tmp_path: Path) -> None:
    sub = await submit(env, "Closed by cancel")
    now = time.time()
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'cancelled', attempt = 1, kill_reason = 'cancel' "
            "WHERE id = ?",
            (sub["task_id"],),
        )
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at, ended_at, outcome, reason, running_s) "
            "VALUES (?, 1, ?, ?, 'cancelled', 'cancel', 3)",
            (sub["task_id"], now - 5, now - 2),
        )
    task = await task_row(sub["task_id"])
    paths = (tmp_path / "1.jsonl", tmp_path / "1.err")
    attempt = Attempt(task, "first", 1, [], *paths, now - 5)
    await engine_watch.finish_attempt(env.engine, attempt, [], 0)
    (row,) = await rows("SELECT * FROM attempts WHERE task_id = ?", sub["task_id"])
    assert row["ended_at"] == now - 2 and row["outcome"] == "cancelled"
    assert row["exit_code"] is None and row["cost_usd"] is None and row["running_s"] == 3


async def test_stream_poll_runs_off_the_event_loop(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stream read that blocks (a large replay, a slow disk) never blocks the event loop:
    while the first poll is held, another coroutine runs and releases it. Polls of one watcher
    never overlap."""
    entered, gate = threading.Event(), threading.Event()
    released: list[bool] = []
    flight = [0, 0]  # polls in flight now, most at once
    lock = threading.Lock()

    class HeldWatcher(engine_watch.StreamWatcher):
        def poll(self) -> list[Any]:
            with lock:
                flight[0] += 1
                flight[1] = max(flight)
            try:
                if not entered.is_set():
                    entered.set()
                    # Released by the test coroutine only if the loop keeps running meanwhile.
                    released.append(gate.wait(timeout=5.0))
                return super().poll()
            finally:
                with lock:
                    flight[0] -= 1

    monkeypatch.setattr(engine_watch, "StreamWatcher", HeldWatcher)
    sub = await submit(env)
    await env.engine.start()

    async def held() -> bool:
        return entered.is_set()

    await wait_for(held)
    assert (await env.engine.status(sub["task_id"]))["status"] == "running"  # the loop runs
    gate.set()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    assert released == [True]
    assert flight[1] == 1


def _result_line(cost: float, index: int, tokens: int) -> str:
    record = {
        "type": "result", "total_cost_usd": cost, "result_index": index,
        "usage": {"input_tokens": tokens, "output_tokens": 0},
    }  # fmt: skip
    return json.dumps(record) + "\n"


async def test_status_cost_partial_from_interim_result(env: Env) -> None:
    task_id = (await submit(env))["task_id"]
    stream_path = env.engine.runstore.attempt_paths(task_id, 2)[0]
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    # Two turns so far: the last result record is the running total, not the first one.
    stream_path.write_text(
        '{"type": "system", "subtype": "init"}\n'
        + _result_line(0.05, 0, 40)
        + '{"type": "assistant", "message": {}}\n'
        + _result_line(0.17, 1, 70)
        + '{"type": "assistant", "mess'  # a half-written line at the tail
    )
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at, ended_at, cost_usd, tokens) "
            "VALUES (?, 1, 1.0, 2.0, 0.19, 30)",
            (task_id,),
        )
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 2, 3.0)", (task_id,)
        )
    st = await env.engine.status(task_id)
    assert st["cost_usd"] == pytest.approx(0.36) and st["cost_partial"] is True
    assert st["tokens_used"] == 100
    async with write_tx() as db:
        await db.execute(
            "UPDATE attempts SET ended_at = 4.0, cost_usd = 0.2, tokens = 80 WHERE n = 2"
        )
    st = await env.engine.status(task_id)
    assert st["cost_usd"] == pytest.approx(0.39) and st["cost_partial"] is False
    assert st["tokens_used"] == 110


async def test_status_cost_partial_without_result_yet(env: Env) -> None:
    task_id = (await submit(env))["task_id"]
    st = await env.engine.status(task_id)
    assert (st["cost_usd"], st["cost_partial"], st["tokens_used"]) == (0.0, False, None)
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 1, 1.0)", (task_id,)
        )
    st = await env.engine.status(task_id)  # running attempt, no stream file yet
    assert (st["cost_usd"], st["cost_partial"], st["tokens_used"]) == (0.0, True, None)
    assert {"cost_partial", "tokens_used"} <= RETURN_KEYS["check_task_status"]


async def test_status_orphan_open_attempt_is_not_partial(env: Env) -> None:
    task_id = (await submit(env))["task_id"]  # queued, not running
    stream_path = env.engine.runstore.attempt_paths(task_id, 1)[0]
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    stream_path.write_text(_result_line(0.5, 0, 10))
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 1, 1.0)", (task_id,)
        )
    st = await env.engine.status(task_id)
    assert (st["cost_usd"], st["cost_partial"], st["tokens_used"]) == (0.0, False, None)


# -- hot agent reload + per-attempt agents file ----------------------------------------------
def agents_file_of(argv: list[str]) -> Path:
    return Path(argv[argv.index("--agents") + 1])


_EDITS = itertools.count(1)


def write_edit(path: Path, text: str) -> None:
    """An edit that always moves the mtime, whatever the file system's resolution."""
    path.write_text(text)
    stamp = time.time_ns() + next(_EDITS) * 10**9
    os.utime(path, ns=(stamp, stamp))


def edit_agent(env: Env, slug: str, text: str) -> None:
    write_edit(env.settings.agents_dir / f"{slug}.md", text)


async def finished_argv(env: Env, description: str) -> list[str]:
    sub = await submit(env, description)
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    (attempt,) = await rows("SELECT argv_json FROM attempts WHERE task_id = ?", sub["task_id"])
    argv: list[str] = json.loads(attempt["argv_json"])
    return argv


async def test_agent_model_edit_applies_next_launch(env: Env) -> None:
    await env.engine.start()
    edit_agent(env, "implementer", "---\ndescription: implementer\nmodel: fable\n---\nYou.\n")
    first = agents_file_of(await finished_argv(env, "one"))
    edit_agent(env, "implementer", "---\ndescription: implementer\nmodel: opus\n---\nYou.\n")
    second = agents_file_of(await finished_argv(env, "two"))
    assert first != second
    assert json.loads(first.read_text())["implementer"]["model"] == "fable"
    assert json.loads(second.read_text())["implementer"]["model"] == "opus"
    assert (
        json.loads((env.settings.data_dir / "agents.json").read_text())["implementer"]["model"]
        == "opus"
    )
    (event,) = await rows(
        "SELECT data_json FROM events WHERE type = 'agents_reloaded' LIMIT 1 OFFSET 1"
    )
    assert json.loads(event["data_json"]) == {"changed": ["implementer"]}


async def test_reload_is_logged_with_the_changed_names(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    """The reload event is global (no task): the server log line is what surfaces it."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
            await env.engine.start()
            edit_agent(env, "implementer", "---\ndescription: implementer\nmodel: opus\n---\nY.\n")
            await finished_argv(env, "one")
    finally:
        logger.removeHandler(caplog.handler)
    reloads = [r for r in caplog.records if r.getMessage() == "agents reloaded"]
    assert reloads and reloads[-1].levelno == logging.INFO
    assert getattr(reloads[-1], "changed") == ["implementer"]


async def test_reload_keeps_last_good_on_parse_error(env: Env) -> None:
    await env.engine.start()
    before = agents_file_of(await finished_argv(env, "one"))
    edit_agent(env, "implementer", "no frontmatter at all\n")
    argv = await finished_argv(env, "two")
    assert json.loads(agents_file_of(argv).read_text()) == json.loads(before.read_text())
    (event,) = await rows("SELECT data_json FROM events WHERE type = 'agents_reload_failed'")
    assert "implementer.md" in json.loads(event["data_json"])["error"]
    await finished_argv(env, "three")  # the same broken file: no second failure event
    assert len(await rows("SELECT id FROM events WHERE type = 'agents_reload_failed'")) == 1
    edit_agent(env, "implementer", "---\ndescription: implementer\nmodel: opus\n---\nYou.\n")
    fixed = agents_file_of(await finished_argv(env, "four"))
    assert json.loads(fixed.read_text())["implementer"]["model"] == "opus"


async def test_each_attempt_gets_own_agents_file(env: Env) -> None:
    await env.engine.start()
    argvs = [await finished_argv(env, "one"), await finished_argv(env, "two")]
    paths = [agents_file_of(argv) for argv in argvs]
    assert len(set(paths)) == 2
    for path in paths:
        assert path.name == "attempt-1.agents.json" and path.parent.parent == env.settings.runs_dir
        assert path.is_file()


async def test_removed_agent_fails_the_queued_task_not_the_engine(env: Env) -> None:
    sub = await submit(env, agent="qa")
    (env.settings.agents_dir / "qa.md").unlink()
    (env.settings.roles_path).unlink()  # roles must not name the removed agent
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["status"] == "failed" and "unknown agent" in st["error"]
    assert await finished_argv(env, "after")  # the engine keeps launching


async def test_reload_swaps_roles_with_agents_or_not_at_all(env: Env) -> None:
    await env.engine.start()
    await finished_argv(env, "one")
    roles_path = env.settings.roles_path
    write_edit(roles_path, roles_path.read_text() + "aliases2: {}\n")  # an unknown key: invalid
    bad = env.engine.roles
    await finished_argv(env, "two")
    assert env.engine.roles is bad
    fixed = (
        roles_path.read_text().replace("aliases2: {}\n", "").replace("dev: implementer", "dev: qa")
    )
    write_edit(roles_path, fixed)
    await finished_argv(env, "three")
    assert env.engine.resolve("dev") == "qa"
    (event,) = await rows("SELECT data_json FROM events WHERE type = 'agents_reloaded'")
    assert json.loads(event["data_json"]) == {"changed": ["roles.yaml"]}


async def test_submit_and_get_agents_see_a_new_agent_on_an_idle_engine(env: Env) -> None:
    provider = ToolProvider()
    provider.attach(env.engine)  # the engine is never started: no launch can reload
    edit_agent(env, "newcomer", "---\ndescription: newcomer\n---\nYou.\n")
    listed = await provider.get_agents(None, None)
    assert "newcomer" in [a["id"] for a in listed["agents"]]
    edit_agent(env, "another", "---\ndescription: another\n---\nYou.\n")
    sub = await submit(env, agent="another")
    assert sub["status"] == "queued"
    await env.engine.list(None, "another", 10, None)  # also resolves a caller's agent name


async def test_reload_error_never_carries_the_source_line_and_is_global(env: Env) -> None:
    planted = "s3cr3tvalue-planted"
    await env.engine.start()
    edit_agent(
        env,
        "implementer",
        f"---\ndescription: implementer\ntoken: {planted}\nbad: [unclosed\n---\nYou.\n",
    )
    sub = await submit(env, "one")
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed"
    (event,) = await rows("SELECT * FROM events WHERE type = 'agents_reload_failed'")
    assert event["task_id"] is None
    assert "implementer.md" in event["data_json"] and "line" in event["data_json"]
    every = await rows("SELECT * FROM events")
    assert planted not in json.dumps([dict(r) for r in every])
    for task_event in await rows("SELECT type FROM events WHERE task_id = ?", sub["task_id"]):
        assert not task_event["type"].startswith("agents_reload")
    events_file = env.engine.runstore.task_dir(sub["task_id"]) / "events.jsonl"
    assert "agents_reload" not in events_file.read_text()


async def test_single_tokens_cover_every_model(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    # modelUsage counts subagents and every model; top-level usage is the parent loop only.
    monkeypatch.setenv("STUB_CLAUDE_SESSION_USAGE", "1")
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed", st
    assert st["tokens_used"] == st["output"]["tokens_used"] == RUN_MODEL_TOKENS
    assert st["cost_usd"] == pytest.approx(0.0042)


async def test_status_interim_result_of_resumed_attempt_counted_once(env: Env) -> None:
    task_id = (await submit(env))["task_id"]
    stream_path = env.engine.runstore.attempt_paths(task_id, 2)[0]
    stream_path.parent.mkdir(parents=True, exist_ok=True)
    # The resumed attempt's running total already includes attempt 1 (same session).
    record = {
        "type": "result", "total_cost_usd": 0.5, "result_index": 0,
        "usage": {"input_tokens": 20, "output_tokens": 0},
        "modelUsage": {"m": {"inputTokens": 800, "costUSD": 0.5}},
    }  # fmt: skip
    stream_path.write_text(json.dumps(record) + "\n")
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, session_id, started_at, ended_at, cost_usd, tokens) "
            "VALUES (?, 1, 's1', 1.0, 2.0, 0.19, 300)",
            (task_id,),
        )
        await db.execute(
            "INSERT INTO attempts (task_id, n, session_id, started_at) VALUES (?, 2, 's1', 3.0)",
            (task_id,),
        )
    st = await env.engine.status(task_id)
    assert st["cost_usd"] == pytest.approx(0.5) and st["tokens_used"] == 800
    assert st["cost_partial"] is True
