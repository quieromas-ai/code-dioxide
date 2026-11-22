"""Sweep policy: only an unclean attempt's leftover processes are killed; exempt commands never."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import sysconfig
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from src import engine_watch, procs
from src.database import write_tx
from src.engine_watch import Attempt
from src.recovery import WaitHuman
from src.runstore import RunStore
from src.tasks import Task
from tests.helpers import (
    Env,
    rows,
    running_with_pid,
    stub_log,
    submit,
    task_row,
    wait_for,
    wait_status,
)

pytestmark = pytest.mark.usefixtures("spawned")  # every test: no stub process left running
SWEEP_KEYS = {"policy", "trigger", "killed", "exempted", "survivors"}
FAKE_COMMAND = "fake-daemon"


def bg_children(env: Env) -> list[int]:
    return [e["bg_child_pid"] for e in stub_log(env.log) if "bg_child_pid" in e]


@pytest.fixture
def children(env: Env) -> Iterator[None]:
    """Kill every background child the stub started (they outlive it by design)."""
    yield
    for pid in bg_children(env):
        if procs.is_running(pid):
            os.kill(pid, signal.SIGKILL)


async def wait_children(env: Env, n: int) -> list[int]:
    async def check() -> list[int] | None:
        found = bg_children(env)
        return found if len(found) >= n else None

    result: list[int] = await wait_for(check)
    return result


async def sweeps(task_id: str) -> list[dict[str, Any]]:
    found = await rows(
        "SELECT data_json FROM events WHERE task_id = ? AND type = 'sweep' ORDER BY id", task_id
    )
    return [json.loads(r["data_json"]) for r in found]


async def wait_sweeps(task_id: str, trigger: str, n: int = 1) -> list[dict[str, Any]]:
    async def check() -> list[dict[str, Any]] | None:
        found = [d for d in await sweeps(task_id) if d["trigger"] == trigger]
        return found if len(found) >= n else None

    result: list[dict[str, Any]] = await wait_for(check)
    return result


async def idle(env: Env) -> None:
    """The engine's jobs (waiter, attempt-end sweep) and kills are all done."""

    async def check() -> bool:
        return not env.engine._jobs and not env.engine._kills

    await wait_for(check)


async def wait_gone(pid: int) -> None:
    async def check() -> bool:
        return not procs.is_running(pid)

    await wait_for(check)


async def wait_attempt_closed(task_id: str, n: int) -> Any:
    async def check() -> Any:
        found = await rows("SELECT * FROM attempts WHERE task_id = ? AND n = ?", task_id, n)
        return found[0] if found and found[0]["ended_at"] is not None else None

    return await wait_for(check)


async def requeue(env: Env, task_id: str) -> None:
    """What an answer (or a retry) does: the task goes back to the queue."""
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'queued', completed_at = NULL WHERE id = ?", (task_id,)
        )
    env.engine._wake.set()


def start_sleeper() -> tuple[subprocess.Popen[bytes], float]:
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"], start_new_session=True
    )
    start = procs.proc_start_time(sleeper.pid)
    assert start is not None
    return sleeper, start


def dead_at_spawn(monkeypatch: pytest.MonkeyPatch, sleeper: subprocess.Popen[bytes]) -> list[bool]:
    """Record, at every spawn, whether the planted leftover was already gone."""
    seen: list[bool] = []
    inner = procs.spawn

    def checking(*args: Any, **kwargs: Any) -> tuple[int, float | None]:
        seen.append(sleeper.poll() is not None)
        return inner(*args, **kwargs)

    monkeypatch.setattr(procs, "spawn", checking)
    return seen


def stop_sleeper(sleeper: subprocess.Popen[bytes]) -> None:
    if sleeper.poll() is None:
        sleeper.kill()
    sleeper.wait()


def interpreter_link(tmp: Path) -> Path:
    """A link to the interpreter binary under a neutral name (argv[0] = the link).

    A macOS framework build's ``bin/python3.x`` re-execs ``Python.app`` (argv[0] then names that
    binary), so the link points at the app binary itself; the caller checks the name ``ps``
    reports."""
    binary = os.path.realpath(sys.executable)
    framework = sysconfig.get_config_var("PYTHONFRAMEWORK")
    if sys.platform == "darwin" and framework:
        app = Path(sys.base_prefix, "Resources", "Python.app", "Contents", "MacOS", framework)
        binary = str(app) if app.is_file() else binary
    link = tmp / "bin-fake" / FAKE_COMMAND
    link.parent.mkdir()
    link.symlink_to(binary)
    return link


async def test_completed_task_leaves_background_process_running(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed"
    await idle(env)
    (child,) = bg_children(env)
    assert procs.is_running(child)
    (attempt,) = await rows("SELECT reason FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["reason"] == "clean_exit"
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "skipped_clean_exit" and end["killed"] == []
    store = RunStore(env.settings.runs_dir)
    assert child in store.read_kept(sub["task_id"])
    assert store.read_procs(sub["task_id"]) == {}  # the recorded set restarts empty


async def test_needs_human_leaves_background_process_running(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    real_decide = env.engine._decide

    def needs_human(
        task: Task, outcome: Any, reset_at: float | None, pending: int | None = None
    ) -> tuple[Any, Any]:
        action, update = real_decide(task, outcome, reset_at, pending)
        if update["status"] == "completed":
            return WaitHuman(), {"status": "needs_human"}
        return action, update

    env.engine._decide = needs_human  # type: ignore[method-assign]
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "needs_human")
    await idle(env)
    (child,) = bg_children(env)
    assert procs.is_running(child)
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "skipped_clean_exit"


async def test_failed_task_sweeps_background_process(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["status"] == "failed"
    (child,) = await wait_children(env, 1)
    await wait_gone(child)
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "swept" and child in end["killed"] and end["survivors"] == []
    (attempt,) = await rows("SELECT reason FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["reason"] is None


async def test_cancel_sweeps_background_process(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    sub = await submit(env)
    await env.engine.start()
    task = await running_with_pid(env.engine, sub["task_id"])
    (child,) = await wait_children(env, 1)
    assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
    await wait_gone(child)
    (cancel,) = await wait_sweeps(sub["task_id"], "cancel")
    assert cancel["policy"] == "swept"
    assert {child, task.pid} <= set(cancel["killed"]) and cancel["survivors"] == []
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "swept"


async def test_relaunch_after_unclean_exit_sweeps(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1,single_success")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "failed")
    await idle(env)
    sleeper, start = start_sleeper()  # a leftover of the failed attempt the sweep missed
    seen = dead_at_spawn(monkeypatch, sleeper)
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await requeue(env, sub["task_id"])
        await wait_attempt_closed(sub["task_id"], 2)
        assert seen == [True]  # killed before the relaunch spawned
        pre = (await wait_sweeps(sub["task_id"], "pre_launch", 2))[-1]
        assert pre["policy"] == "swept" and sleeper.pid in pre["killed"]
        assert pre["survivors"] == []
    finally:
        stop_sleeper(sleeper)


async def test_relaunch_after_clean_exit_does_not_sweep(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "single_success,exit1")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed")
    await idle(env)
    (first,) = bg_children(env)
    await requeue(env, sub["task_id"])
    await wait_attempt_closed(sub["task_id"], 2)
    await idle(env)
    first_child, second_child = bg_children(env)
    assert first_child == first
    pre = (await wait_sweeps(sub["task_id"], "pre_launch", 2))[-1]
    assert pre["policy"] == "skipped_clean_exit" and pre["killed"] == []
    # The second attempt failed: its own child is swept, the first (clean) attempt's is kept.
    await wait_gone(second_child)
    assert procs.is_running(first_child)
    end = (await wait_sweeps(sub["task_id"], "attempt_end", 2))[-1]
    assert end["policy"] == "swept"
    assert second_child in end["killed"] and first_child not in end["killed"]
    assert first_child not in end["survivors"]


async def test_restart_with_open_attempt_counts_as_unclean(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()
    seen = dead_at_spawn(monkeypatch, sleeper)
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        async with write_tx() as db:  # left open by a server that stopped mid-attempt
            await db.execute("UPDATE tasks SET attempt = 1 WHERE id = ?", (sub["task_id"],))
            await db.execute(
                "INSERT INTO attempts (task_id, n, started_at, reason) "
                "VALUES (?, 1, 1.0, 'clean_exit')",
                (sub["task_id"],),
            )
        await env.engine.start()
        await wait_status(env.engine, sub["task_id"], "completed", "failed")
        assert seen == [True]
        # Engine start closes the open row (an unclean end) and sweeps at once; the pre-launch
        # sweep then finds nothing left.
        (boot,) = await wait_sweeps(sub["task_id"], "boot")
        assert boot["policy"] == "swept" and boot["killed"] == [sleeper.pid]
        (pre,) = await wait_sweeps(sub["task_id"], "pre_launch")
        assert pre["policy"] == "swept" and pre["killed"] == []
        first = (await rows("SELECT * FROM attempts WHERE task_id = ? AND n = 1", sub["task_id"]))[
            0
        ]
        assert first["ended_at"] is not None and first["reason"] == "boot"
        assert (await task_row(sub["task_id"])).attempt == 2
    finally:
        stop_sleeper(sleeper)


async def test_sweep_exempt_command_survives_cancel_and_is_not_a_survivor(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang,single_success")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", str(interpreter_link(env.tmp)))
    env.settings.sweep_exempt = [FAKE_COMMAND]
    sub = await submit(env)
    await env.engine.start()
    await running_with_pid(env.engine, sub["task_id"])
    (child,) = await wait_children(env, 1)
    assert procs._snapshot()[child].command == FAKE_COMMAND
    assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
    await wait_attempt_closed(sub["task_id"], 1)
    await idle(env)
    assert procs.is_running(child)
    expected = [{"pid": child, "command": FAKE_COMMAND}]
    (cancel,) = await wait_sweeps(sub["task_id"], "cancel")
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    for data in (cancel, end):
        assert data["exempted"] == expected and data["survivors"] == []
        assert child not in data["killed"]
    # The relaunch is not held back by the exempt process: it is no survivor.
    await requeue(env, sub["task_id"])
    st = await wait_status(env.engine, sub["task_id"], "completed")
    assert st["status"] == "completed"
    assert procs.is_running(child)
    pre = (await wait_sweeps(sub["task_id"], "pre_launch", 2))[-1]
    assert pre["policy"] == "swept" and {"pid": child, "command": FAKE_COMMAND} in pre["exempted"]
    assert pre["survivors"] == []
    found = await rows("SELECT type FROM events WHERE task_id = ?", sub["task_id"])
    types = [r["type"] for r in found]
    assert "sweep_survivors" not in types


async def test_sweep_exempt_never_protects_claude(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    sub = await submit(env)
    await env.engine.start()
    task = await running_with_pid(env.engine, sub["task_id"])
    (child,) = await wait_children(env, 1)
    table = procs._snapshot()
    claude_command = table[task.pid or 0].command
    assert claude_command and table[child].command == claude_command
    env.settings.sweep_exempt = [claude_command]
    assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
    await wait_gone(task.pid or 0)
    await idle(env)
    assert procs.is_running(child)  # same command name, not the task's claude: exempt
    (cancel,) = await wait_sweeps(sub["task_id"], "cancel")
    assert task.pid in cancel["killed"] and child not in cancel["killed"]
    assert cancel["exempted"] == [{"pid": child, "command": claude_command}]


async def test_sweep_event_recorded_for_each_policy(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "single_success,exit1")
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed")
    await idle(env)
    await requeue(env, sub["task_id"])
    await wait_attempt_closed(sub["task_id"], 2)
    await wait_sweeps(sub["task_id"], "attempt_end", 2)
    await idle(env)
    found = await sweeps(sub["task_id"])
    assert [(d["policy"], d["trigger"]) for d in found] == [
        ("swept", "pre_launch"),  # first launch: no previous attempt row counts as unclean
        ("skipped_clean_exit", "attempt_end"),
        ("skipped_clean_exit", "pre_launch"),
        ("swept", "attempt_end"),
    ]
    for data in found:
        assert set(data) == SWEEP_KEYS
        assert all(isinstance(pid, int) for pid in data["killed"] + data["survivors"])
        assert all(set(e) == {"pid", "command"} for e in data["exempted"])
    mirrored = (env.settings.runs_dir / sub["task_id"] / "events.jsonl").read_text()
    assert mirrored.count('"type": "sweep"') == len(found)


def alive_at_spawn(monkeypatch: pytest.MonkeyPatch, pid: int, start: float) -> list[bool]:
    """Record, at every spawn, whether ``pid`` was still the process started at ``start``."""
    seen: list[bool] = []
    inner = procs.spawn

    def checking(*args: Any, **kwargs: Any) -> tuple[int, float | None]:
        seen.append(procs.alive_and_same(pid, start))
        return inner(*args, **kwargs)

    monkeypatch.setattr(procs, "spawn", checking)
    return seen


async def set_last_attempt(task_id: str, status: str, reason: str | None) -> None:
    """The task is ``status`` with a closed attempt 1 (``reason`` as persisted)."""
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = ?, attempt = 1 WHERE id = ?", (status, task_id))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at, ended_at, outcome, reason) "
            "VALUES (?, 1, 1.0, 2.0, 'success', ?)",
            (task_id, reason),
        )


async def test_rolled_back_clean_exit_counts_as_unclean(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    real_event = env.engine._event
    failed: list[str] = []

    async def failing(db: Any, task_id: str, type_: str, data: dict[str, Any]) -> None:
        if type_ == "sweep" and data["trigger"] == "attempt_end" and not failed:
            failed.append(task_id)  # after the keep step: e.g. stop() cancels the waiter
            raise asyncio.CancelledError
        await real_event(db, task_id, type_, data)

    env.engine._event = failing  # type: ignore[method-assign]
    sub = await submit(env)
    await env.engine.start()

    async def rolled_back() -> bool:
        return bool(failed)

    await wait_for(rolled_back)
    await idle(env)
    env.engine._event = real_event  # type: ignore[method-assign]
    (attempt,) = await rows("SELECT * FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["ended_at"] is None and attempt["reason"] is None  # still open: unclean
    (child,) = bg_children(env)
    start = procs.proc_start_time(child)
    assert start is not None
    store = RunStore(env.settings.runs_dir)
    assert store.read_kept(sub["task_id"]) == {} and store.read_procs(sub["task_id"])
    # A kept entry of an attempt whose row is not clean_exit is not honoured either.
    kept = {str(child): {"start": start, "n": 1}}
    (env.settings.runs_dir / sub["task_id"] / "kept.json").write_text(json.dumps(kept))
    seen = alive_at_spawn(monkeypatch, child, start)
    await requeue(env, sub["task_id"])
    await wait_attempt_closed(sub["task_id"], 2)
    assert seen == [False]  # the open attempt's process was killed before the relaunch
    pre = (await wait_sweeps(sub["task_id"], "pre_launch", 2))[-1]
    assert pre["policy"] == "swept" and child in pre["killed"]


async def test_unclean_needs_human_sweeps_background_process(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")

    def needs_human(
        task: Task, outcome: Any, reset_at: float | None, pending: int | None = None
    ) -> tuple[Any, Any]:
        return WaitHuman(), {"status": "needs_human"}

    env.engine._decide = needs_human  # type: ignore[method-assign]
    sub = await submit(env)
    await env.engine.start()
    task = await running_with_pid(env.engine, sub["task_id"])
    (child,) = await wait_children(env, 1)
    async with write_tx() as db:  # an engine kill, e.g. a watchdog: the exit is not clean
        await db.execute("UPDATE tasks SET kill_reason = 'stall' WHERE id = ?", (task.id,))
    os.kill(task.pid or 0, signal.SIGKILL)
    await wait_status(env.engine, sub["task_id"], "needs_human")
    await wait_gone(child)
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "swept" and child in end["killed"]
    (attempt,) = await rows("SELECT reason FROM attempts WHERE task_id = ?", sub["task_id"])
    assert attempt["reason"] == "stall"


async def test_cancel_queued_after_unclean_attempt_sweeps(env: Env) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await set_last_attempt(sub["task_id"], "queued", None)  # e.g. a sweep_survivors requeue
        assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"

        async def sleeper_dead() -> bool:
            return sleeper.poll() is not None

        await wait_for(sleeper_dead)
        (cancel,) = await wait_sweeps(sub["task_id"], "cancel")
        assert cancel["policy"] == "swept" and cancel["killed"] == [sleeper.pid]
    finally:
        stop_sleeper(sleeper)


async def test_cancel_needs_human_after_clean_exit_kills_nothing(env: Env) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()
    try:
        RunStore(env.settings.runs_dir).write_procs(sub["task_id"], {sleeper.pid: start})
        await set_last_attempt(sub["task_id"], "needs_human", "clean_exit")
        assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
        await idle(env)
        assert procs.alive_and_same(sleeper.pid, start)
        assert await sweeps(sub["task_id"]) == [
            {
                "policy": "skipped_clean_exit",
                "trigger": "cancel",
                "killed": [],
                "exempted": [],
                "survivors": [],
            }
        ]
    finally:
        stop_sleeper(sleeper)


async def test_kill_for_closed_attempt_kills_nothing(env: Env) -> None:
    sub = await submit(env)
    sleeper, start = start_sleeper()  # stands in for the attempt's claude
    try:
        await set_last_attempt(sub["task_id"], "cancelled", "cancel")
        task = await task_row(sub["task_id"])
        paths = (env.tmp / "1.jsonl", env.tmp / "1.err")
        attempt = Attempt(task, "first", 1, [], *paths, 1.0, sleeper.pid, start)
        await engine_watch.kill_for(env.engine, attempt, "parent_violation")
        await idle(env)
        assert procs.alive_and_same(sleeper.pid, start)
        found = await rows("SELECT type FROM events WHERE task_id = ?", sub["task_id"])
        types = {r["type"] for r in found}
        assert not types & {"sweep", "parent_tool_violation", "kill_skipped"}
    finally:
        stop_sleeper(sleeper)


async def test_engine_error_sweeps_with_own_claude(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    spawned: list[tuple[int, float]] = []
    swept: list[tuple[str, str, dict[int, float]]] = []
    real_leftovers = env.engine._sweep_leftovers

    async def broken_inspect(attempt: Attempt, new: list[Any], state: dict[str, Any]) -> None:
        assert attempt.start is not None
        spawned.append((attempt.pid, attempt.start))
        # The claude's own command name is exempt: an engine error still kills it.
        env.settings.sweep_exempt = [procs._snapshot()[attempt.pid].command]
        raise RuntimeError("inspect broke")

    async def spy(task_id: str, trigger: str, own: dict[int, float]) -> None:
        swept.append((task_id, trigger, own))
        await real_leftovers(task_id, trigger, own)

    env.engine._inspect = broken_inspect  # type: ignore[method-assign]
    env.engine._sweep_leftovers = spy  # type: ignore[method-assign]
    env.settings.max_resumes = 0  # no recovery budget: the first unclean end is final
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed")
    assert "inspect broke" in st["error"]
    await idle(env)
    ((pid, start),) = spawned
    await wait_gone(pid)
    assert swept and swept[-1] == (sub["task_id"], "attempt_end", {pid: start})
    ends = await wait_sweeps(sub["task_id"], "attempt_end")
    assert pid in ends[0]["killed"] and all(d["policy"] == "swept" for d in ends)
    assert all(d["exempted"] == [] for d in ends)


async def test_keep_skipped_when_listing_fails(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")

    def broken(*args: Any, **kwargs: Any) -> dict[int, float]:
        raise OSError("ps unavailable")

    monkeypatch.setattr(procs, "task_procs", broken)
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed")
    await idle(env)
    (child,) = bg_children(env)
    assert procs.is_running(child)  # still a clean exit: nothing swept
    store = RunStore(env.settings.runs_dir)
    assert store.read_kept(sub["task_id"]) == {}
    assert store.read_procs(sub["task_id"])  # not reset without a kept set
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "skipped_clean_exit"


async def test_unclean_end_before_a_retry_swept_at_once(
    env: Env, monkeypatch: pytest.MonkeyPatch, children: None
) -> None:
    """With a retry budget, an unclean end goes back to queued for the backoff: its leftover
    processes are killed at the attempt's end, not only before the relaunch."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1")
    monkeypatch.setenv("STUB_CLAUDE_BG_CHILD", "1")
    env.settings.retry_backoff_s = 3600.0  # the relaunch is an hour away
    sub = await submit(env)
    await env.engine.start()
    (child,) = await wait_children(env, 1)
    await wait_gone(child)
    (end,) = await wait_sweeps(sub["task_id"], "attempt_end")
    assert end["policy"] == "swept" and child in end["killed"]
    task = await task_row(sub["task_id"])
    assert task.status == "queued" and task.next_attempt_at and task.resumes == 1
    assert len(bg_children(env)) == 1  # nothing relaunched meanwhile

    async def job_done() -> bool:  # the attempt's own end (and its sweep) is over
        return sub["task_id"] not in env.engine._live

    await wait_for(job_done)
    assert (await env.engine.cancel(sub["task_id"]))["status"] == "cancelled"
