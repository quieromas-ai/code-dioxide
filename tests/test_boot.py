"""Server and engine start: the registry and lifespan checks; running attempts re-attached or
ended from what a previous engine left (rows, stream, run file, process table); other open
attempt rows closed; waiting states kept."""

import json
import logging
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from cli.main import cli
from src import engine_boot, engine_sweep, engine_watch, journal, procs
from src.config import Settings, acquire_home_lock
from src.database import write_tx
from src.engine_boot import process_state
from src.engine_watch import Attempt
from src.main import create_app, load_registry
from tests.helpers import (
    Env,
    restart,
    rows,
    running_with_pid,
    stub_log,
    submit,
    task_row,
    wait_for,
    wait_status,
)
from tests.test_engine_lifecycle import lifecycle
from tests.test_engine_sweep import start_sleeper, stop_sleeper
from tests.test_recovery import event_data, table_task

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
RUN_ID = "wf_b00t0001-abc"
QUESTION = {"status": "needs_human", "stage": "plan", "kind": "plan_approval", "question": "Ok?"}
RETURNS: dict[str, dict[str, Any]] = {
    "completed": {"status": "completed", "stage": "merge", "result": {}},
    "needs_human": QUESTION,
    "cancelled": {"status": "cancelled", "stage": "plan", "result": {}},
    "stage_failed": {"status": "stage_failed", "stage": "merge", "error": "merge.sh: failed"},
}


@pytest.fixture
def sleepers() -> Iterator[list[subprocess.Popen[bytes]]]:
    """Plain processes standing in for a task's claude (or a stranger with a reused pid)."""
    started: list[subprocess.Popen[bytes]] = []
    yield started
    for sleeper in started:
        stop_sleeper(sleeper)


def launch_count(env: Env, task_id: str) -> int:
    return sum(1 for e in stub_log(env.log) if e.get("env", {}).get("DIOXIDE_TASK_ID") == task_id)


async def attempts_of(task_id: str) -> list[tuple[str | None, str | None]]:
    found = await rows("SELECT outcome, reason FROM attempts WHERE task_id = ? ORDER BY n", task_id)
    return [(r["outcome"], r["reason"]) for r in found]


async def sweeps_of(task_id: str) -> list[tuple[str, str]]:
    return [(e["policy"], e["trigger"]) for e in await event_data(task_id, "sweep")]


async def idle(env: Env, task_id: str) -> None:
    """The task's job (re-attached, ended at boot or launched) has finished."""

    async def done() -> bool:
        return task_id not in env.engine._live and task_id not in env.engine._launching

    await wait_for(done)


# -- the server's own start ----------------------------------------------------------------
def settings_for(home: Path) -> Settings:
    return Settings(home=home, auth_disabled=True)


def test_missing_agents_dir_without_roles_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="test-boot"):
        agents, roles = load_registry(settings_for(tmp_path), logging.getLogger("test-boot"))
    assert (agents, roles) == ({}, None)
    assert "empty registry" in caplog.text


def test_missing_agents_dir_with_roles_fails(tmp_path: Path) -> None:
    shutil.copy(EXAMPLES / "roles.yaml", tmp_path / "roles.yaml")
    with pytest.raises(ValueError, match="agents dir .* is missing"):
        load_registry(settings_for(tmp_path), logging.getLogger("test-boot"))


def test_agents_without_roles_load(tmp_path: Path) -> None:
    shutil.copytree(EXAMPLES / "agents", tmp_path / "agents")
    agents, roles = load_registry(settings_for(tmp_path), logging.getLogger("test-boot"))
    assert len(agents) == 6 and roles is None


async def test_lifespan_fails_fast_on_bad_roles(tmp_path: Path) -> None:
    shutil.copytree(EXAMPLES / "agents", tmp_path / "agents")
    (tmp_path / "roles.yaml").write_text("planner: planner\naliases: {qa: planner}\n")
    settings = settings_for(tmp_path)
    app = create_app(settings)
    with pytest.raises(ValueError, match="roles.yaml"):
        async with app.router.lifespan_context(app):
            pytest.fail("must not start")
    log = [json.loads(line) for line in settings.server_log_path.read_text().splitlines()]
    assert any(entry["msg"] == "invalid configuration" for entry in log)


async def test_lifespan_writes_engine_files(tmp_path: Path) -> None:
    shutil.copytree(EXAMPLES / "agents", tmp_path / "agents")
    shutil.copy(EXAMPLES / "roles.yaml", tmp_path / "roles.yaml")
    settings = Settings(home=tmp_path, auth_disabled=True, extra_settings={"model": "x"})
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        rendered = json.loads((settings.data_dir / "agents.json").read_text())
        assert set(rendered) == {
            "planner",
            "implementer",
            "reviewer",
            "qa",
            "deployer",
            "coordinator",
        }
        assert json.loads((settings.data_dir / "mcp.json").read_text()) == {"mcpServers": {}}
        assert json.loads((settings.data_dir / "settings.json").read_text()) == {"model": "x"}


# -- what a previous engine left: built by claiming through the engine, then files by hand -----
def _line(kind: str, **fields: Any) -> dict[str, Any]:
    return {"type": kind, "session_id": "s", **fields}


def _result(index: int, **extra: Any) -> dict[str, Any]:
    line = _line("result", subtype="success", is_error=False, result="Done.", result_index=index)
    line.update(total_cost_usd=0.01, usage={"input_tokens": 1, "output_tokens": 2}, **extra)
    return line


def stream_for(attempt: Attempt, content: str, tdir: Path) -> list[dict[str, Any]]:
    """Stream lines in the shapes ``tests/stub_claude.py`` writes (Claude Code 2.1.328)."""
    lines = [_line("system", subtype="init", tools=["Workflow"], mcp_servers=[])]
    if content == "nothing":
        return lines
    if content == "result":  # one result line and nothing else
        return [*lines, _result(0)]
    if content == "rate_limit":
        info = {"status": "rejected", "resetsAt": int(time.time()) + 3600}
        limited = _result(0, is_error=True, api_error_status=429, api_error="usage_limit_reached")
        return [*lines, _line("rate_limit_event", rate_limit_info=info), limited]
    call = {"type": "tool_use", "id": "toolu_1", "name": "Workflow", "input": attempt.call}
    ack_item = {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok", "is_error": False}
    meta = {"status": "async_launched", "runId": RUN_ID, "taskId": "w1", "transcriptDir": str(tdir)}
    lines += [
        _line("assistant", message={"content": [call]}, parent_tool_use_id=None),
        _line("user", message={"content": [ack_item]}, parent_tool_use_id=None,
              tool_use_result=meta),
    ]  # fmt: skip
    if content == "first_result":  # the first of a workflow session's two result lines only
        return [*lines, _result(0)]
    done = _line("system", subtype="task_notification", tool_use_id="toolu_1", status="completed")
    return [*lines, done, _result(0), _result(1)]


@dataclass
class Case:
    name: str
    kind: str  # lifecycle | single
    status: str  # the task's status when the engine stopped
    process: str  # alive | dead | pid_reused | no_pid
    content: str  # what the stream and run file show
    # expected after boot (and after the alive process exits):
    final: str
    attempts: list[tuple[str | None, str | None]]
    launches: int
    sweeps: list[tuple[str, str]]
    boot: str | None  # the boot_recovered action (None: no such event)
    open_row: bool = True  # the attempt row was left open
    task_id: str = ""
    sleeper: subprocess.Popen[bytes] | None = None
    start: float | None = None


SWEPT_END = [("swept", "attempt_end")]
CLEAN_END = [("skipped_clean_exit", "attempt_end")]
TRANSIENT = [("transient", None)]
CASES = [
    # running, process alive and the same: re-attached; it then ends (here: we stop it).
    Case("alive-nothing", "lifecycle", "running", "alive", "nothing",
         "queued", TRANSIENT, 0, SWEPT_END, "reattach"),
    Case("alive-completed", "lifecycle", "running", "alive", "completed",
         "completed", [("success", "clean_exit")], 0, CLEAN_END, "reattach"),
    # running, process dead: the whole stream and the run file decide.
    Case("dead-nothing", "lifecycle", "running", "dead", "nothing",
         "queued", TRANSIENT, 0, SWEPT_END, "classify"),
    Case("dead-first-result", "lifecycle", "running", "dead", "first_result",
         "queued", TRANSIENT, 0, SWEPT_END, "classify"),
    Case("dead-result-single", "single", "running", "dead", "result",
         "completed", [("success", "clean_exit")], 0, CLEAN_END, "classify"),
    Case("dead-completed", "lifecycle", "running", "dead", "completed",
         "completed", [("success", "clean_exit")], 0, CLEAN_END, "classify"),
    Case("dead-needs-human", "lifecycle", "running", "dead", "needs_human",
         "needs_human", [("needs_human", "clean_exit")], 0, CLEAN_END, "classify"),
    Case("dead-cancelled", "lifecycle", "running", "dead", "cancelled",
         "cancelled", [("cancelled_by_workflow", None)], 0, SWEPT_END, "classify"),
    Case("dead-stage-failed", "lifecycle", "running", "dead", "stage_failed",
         "queued", [("stage_failed", None), ("transient", None)], 1,
         [*SWEPT_END, ("swept", "pre_launch"), *SWEPT_END], "classify"),
    Case("dead-rate-limit", "lifecycle", "running", "dead", "rate_limit",
         "queued", [("rate_limited", None)], 0, SWEPT_END, "classify"),
    # running, pid alive but another process (start time differs): never re-attached/signalled.
    Case("reused-completed", "lifecycle", "running", "pid_reused", "completed",
         "completed", [("success", "clean_exit")], 0, CLEAN_END, "classify"),
    Case("reused-nothing", "single", "running", "pid_reused", "nothing",
         "queued", TRANSIENT, 0, SWEPT_END, "classify"),
    # running, claimed but no pid stored (stopped inside the spawn window).
    Case("no-pid-nothing", "lifecycle", "running", "no_pid", "nothing",
         "queued", TRANSIENT, 0, SWEPT_END, "classify"),
    # not running: left as they are.
    Case("needs-human-waiting", "lifecycle", "needs_human", "dead", "needs_human",
         "needs_human", [("needs_human", "clean_exit")], 0, [], None, open_row=False),
    Case("queued-backoff", "lifecycle", "queued", "dead", "nothing",
         "queued", TRANSIENT, 0, [], None, open_row=False),
    # not running but its attempt row left open: closed (unclean) and swept at boot.
    Case("cancelled-open-alive", "lifecycle", "cancelled", "alive", "nothing",
         "cancelled", [("cancelled", "cancel")], 0, [("swept", "boot")], None),
    Case("queued-open-dead", "lifecycle", "queued", "dead", "nothing",
         "queued", [("transient", "boot")], 0, [("swept", "boot")], None),
]  # fmt: skip


async def leave_behind(env: Env, case: Case, sleepers: list[subprocess.Popen[bytes]]) -> None:
    """Claim through the (not started) engine, then write what the stopped engine left."""
    engine = env.engine
    if case.kind == "lifecycle":
        case.task_id = await lifecycle(env, f"Boot {case.name}")
    else:
        case.task_id = str((await submit(env, f"Boot {case.name}"))["task_id"])
    task = await task_row(case.task_id)
    prepared = await engine._prepare_launch(task, "first")
    assert prepared is not None
    attempt = await engine._claim(prepared, "first")
    assert attempt is not None
    claimed = await task_row(case.task_id)
    assert claimed.session_id and claimed.cwd
    cwd, session, home = claimed.cwd, claimed.session_id, env.settings.claude_home
    tdir = journal.workflow_run_dir(cwd, session, RUN_ID, home, None)
    lines = stream_for(attempt, case.content, tdir)
    attempt.stream_path.write_text("".join(json.dumps(line) + "\n" for line in lines))
    if case.content in RETURNS:
        run_file = journal.run_file_path(cwd, session, RUN_ID, home)
        run_file.parent.mkdir(parents=True, exist_ok=True)
        run = {"runId": RUN_ID, "status": "completed", "result": RETURNS[case.content]}
        run_file.write_text(json.dumps(run))

    pid: int | None = None
    start: float | None = None
    if case.process != "no_pid":
        sleeper, start = start_sleeper()
        sleepers.append(sleeper)
        pid = sleeper.pid
        if case.process == "dead":
            stop_sleeper(sleeper)
        elif case.process == "pid_reused":
            case.sleeper, case.start = sleeper, start
            start -= 1000.0  # the recorded process started long before this one
        else:
            case.sleeper, case.start = sleeper, start
    now = time.time()
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET pid = ?, pid_start = ? WHERE id = ?", (pid, start, case.task_id)
        )
        if case.status == "running":
            return
        kill_reason = "cancel" if case.status == "cancelled" else None
        await db.execute(
            "UPDATE tasks SET status = ?, kill_reason = ?, next_attempt_at = ?, "
            "question_json = ? WHERE id = ?",
            (
                case.status, kill_reason,
                now + 3600 if case.status == "queued" else None,
                json.dumps(QUESTION) if case.status == "needs_human" else None, case.task_id,
            ),
        )  # fmt: skip
        if not case.open_row:
            outcome, reason = case.attempts[0]
            await db.execute(
                "UPDATE attempts SET ended_at = ?, outcome = ?, reason = ?, running_s = 1 "
                "WHERE task_id = ?",
                (now, outcome, reason, case.task_id),
            )


async def test_boot_recovery_matrix(
    env: Env, monkeypatch: pytest.MonkeyPatch, sleepers: list[subprocess.Popen[bytes]]
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "exit1")  # any relaunch ends transient
    env.settings.retry_backoff_s = 3600.0  # a transient end waits an hour: no relaunch here
    env.settings.max_concurrent_tasks = 50
    for case in CASES:
        await leave_behind(env, case, sleepers)
    before = {c.task_id: await task_row(c.task_id) for c in CASES}

    await restart(env)
    boots = {c.task_id: await event_data(c.task_id, "boot_recovered") for c in CASES}
    for case in CASES:
        if case.boot == "reattach":  # still running, watched by the new engine
            task = await task_row(case.task_id)
            assert task.status == "running" and case.task_id in env.engine._live, case.name
            assert case.sleeper is not None
            case.sleeper.send_signal(signal.SIGTERM)  # the process ends by itself
    for case in CASES:
        await idle(env, case.task_id)
        if case.launches:
            await wait_for(lambda c=case: _attempts_closed(c.task_id, len(c.attempts)))
            await idle(env, case.task_id)

    for case in CASES:
        task = await task_row(case.task_id)
        got = {
            "final": task.status,
            "attempts": await attempts_of(case.task_id),
            "launches": launch_count(env, case.task_id),
            "sweeps": await sweeps_of(case.task_id),
            "boot": [(b["process"], b["action"]) for b in boots[case.task_id]],
        }
        want_boot = [(case.process, case.boot)] if case.boot else []
        want = {
            "final": case.final, "attempts": case.attempts, "launches": case.launches,
            "sweeps": case.sweeps, "boot": want_boot,
        }  # fmt: skip
        assert got == want, case.name
        if case.final == "queued":  # a waiting state: never launched early
            assert task.next_attempt_at and task.next_attempt_at > time.time() + 60, case.name
        if case.final == "needs_human":
            assert json.loads(task.question_json or "{}")["kind"] == "plan_approval", case.name
        if case.process == "pid_reused":  # never signalled, never taken for the task's
            assert case.sleeper is not None and case.start is not None
            assert procs.alive_and_same(case.sleeper.pid, case.start), case.name
            assert case.sleeper.pid not in env.engine.runstore.read_procs(case.task_id)
        if case.name == "cancelled-open-alive":  # the cancelled attempt's process is killed
            assert case.sleeper is not None and case.sleeper.wait(timeout=10) is not None
            (end,) = await event_data(case.task_id, "sweep")
            assert case.sleeper.pid in end["killed"]
        if case.name == "no-pid-nothing":  # never spawned: the relaunch repeats its mode
            assert task.resume_mode == "first" and task.resumes == 1
        if case.boot is None and case.open_row:
            (closed,) = await event_data(case.task_id, "attempt_closed_at_boot")
            assert closed["status"] == case.status
        unchanged = before[case.task_id]
        if case.boot is None and not case.open_row:  # nothing at all happened to it
            assert task == unchanged, case.name


async def _attempts_closed(task_id: str, n: int) -> bool:
    found = await rows("SELECT ended_at FROM attempts WHERE task_id = ?", task_id)
    return len(found) == n and all(r["ended_at"] is not None for r in found)


async def test_boot_is_idempotent(
    env: Env, monkeypatch: pytest.MonkeyPatch, sleepers: list[subprocess.Popen[bytes]]
) -> None:
    """A second boot over the same state (e.g. after a crash during the first) ends the same."""
    env.settings.retry_backoff_s = 3600.0
    cases = [c for c in CASES if c.name in ("dead-completed", "dead-nothing", "queued-open-dead")]
    cases = [replace(c) for c in cases]
    for case in cases:
        await leave_behind(env, case, sleepers)
    for _ in range(2):
        await restart(env)
        for case in cases:
            await idle(env, case.task_id)
    for case in cases:
        task = await task_row(case.task_id)
        assert (task.status, await attempts_of(case.task_id)) == (case.final, case.attempts)
        assert len(await event_data(case.task_id, "boot_recovered")) == (1 if case.boot else 0)
        assert launch_count(env, case.task_id) == 0


# -- with the stub: stop the engine while the attempt runs, start a new one --------------------
async def wait_run_started(task_id: str) -> Any:
    async def check() -> Any:
        task = await task_row(task_id)
        return task if task.status == "running" and task.pid and task.run_id else None

    return await wait_for(check)


def wait_process_gone(pid: int, start: float | None) -> None:
    deadline = time.monotonic() + 20
    while procs.alive_and_same(pid, start):
        assert time.monotonic() < deadline, "the stub did not finish"
        time.sleep(0.05)


async def test_finished_while_detached_no_resume(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    release = env.tmp / "release"
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "finish_while_detached")
    monkeypatch.setenv("STUB_CLAUDE_DETACHED_RETURN", "needs_human")
    monkeypatch.setenv("STUB_CLAUDE_RELEASE", str(release))
    task_id = await lifecycle(env)
    await env.engine.start()
    running = await wait_run_started(task_id)
    await restart(env, start=False)  # the engine is down while the run finishes
    release.touch()
    wait_process_gone(running.pid, running.pid_start)

    await env.engine.start()
    st = await wait_status(env.engine, task_id, "needs_human", "completed", "failed", "queued")
    assert st["status"] == "needs_human", st
    await idle(env, task_id)
    task = await task_row(task_id)
    assert json.loads(task.question_json or "{}")["kind"] == "plan_approval"
    (boot,) = await event_data(task_id, "boot_recovered")
    assert (boot["process"], boot["action"]) == ("dead", "classify")
    # Posted once, decided once, never resumed: one launch, one clean attempt, nothing swept.
    finished = await event_data(task_id, "attempt_finished")
    assert [f["status"] for f in finished] == ["needs_human"]
    assert await event_data(task_id, "retry_scheduled") == []
    assert len(stub_log(env.log)) == 1
    assert await attempts_of(task_id) == [("needs_human", "clean_exit")]
    assert await sweeps_of(task_id) == [("swept", "pre_launch"), *CLEAN_END]  # the launch's


async def test_reattach_completes_one_attempt(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    release = env.tmp / "release"
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "finish_while_detached")
    monkeypatch.setenv("STUB_CLAUDE_RELEASE", str(release))
    task_id = await lifecycle(env)
    await env.engine.start()
    running = await wait_run_started(task_id)
    await restart(env)
    (boot,) = await event_data(task_id, "boot_recovered")
    assert (boot["process"], boot["action"]) == ("alive", "reattach")
    assert task_id in env.engine._live and task_id in env.engine._attempts
    assert procs.alive_and_same(running.pid, running.pid_start)  # stop() detached, never killed
    release.touch()
    st = await wait_status(env.engine, task_id, "completed", "failed", "queued", "needs_human")
    assert st["status"] == "completed", st
    await idle(env, task_id)
    assert len(stub_log(env.log)) == 1  # the re-attached process finished the task alone
    assert await attempts_of(task_id) == [("success", "clean_exit")]
    assert len(await event_data(task_id, "attempt_started")) == 1
    assert len(await event_data(task_id, "run_started")) == 1  # the replay names no new run
    task = await task_row(task_id)
    assert task.run_id == running.run_id and json.loads(task.output_json or "{}")["return"]


async def test_pid_reuse_not_reattached(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    env.settings.retry_backoff_s = 3600.0
    sub = await submit(env)
    task_id = str(sub["task_id"])
    await env.engine.start()
    running = await running_with_pid(env.engine, task_id)

    async def launched() -> bool:
        return len(stub_log(env.log)) == 1

    await wait_for(launched)
    await restart(env, start=False)
    assert running.pid is not None
    os.kill(running.pid, signal.SIGKILL)  # the claude process died while the engine was down
    wait_process_gone(running.pid, running.pid_start)
    stranger, start = start_sleeper()  # a new process now holds the recorded pid
    try:
        async with write_tx() as db:
            await db.execute(
                "UPDATE tasks SET pid = ?, pid_start = ? WHERE id = ?",
                (stranger.pid, start - 1000.0, task_id),
            )
        assert process_state(stranger.pid, start - 1000.0) == "pid_reused"
        await env.engine.start()
        await idle(env, task_id)
        (boot,) = await event_data(task_id, "boot_recovered")
        assert (boot["process"], boot["action"]) == ("pid_reused", "classify")
        task = await task_row(task_id)
        assert task.status == "queued" and await attempts_of(task_id) == TRANSIENT
        # The attempt was treated as gone (ended at once from its stream, not watched) ...
        closed = await rows("SELECT * FROM attempts WHERE task_id = ?", task_id)
        assert len(closed) == 1 and closed[0]["ended_at"] is not None
        assert task_id not in env.engine._attempts
        # ... and the stranger holding the pid was never signalled: still alive, same process.
        assert stranger.poll() is None and procs.alive_and_same(stranger.pid, start)
        for sweep in await event_data(task_id, "sweep"):
            assert stranger.pid not in sweep["killed"]
        assert await event_data(task_id, "kill_skipped") == []
        assert len(stub_log(env.log)) == 1
    finally:
        stop_sleeper(stranger)


async def test_reattached_watch_state_judges_a_late_parent_call(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first call and its run, seen before the restart, are rebuilt from the stream: a
    parent tool call after the run's completion (seen only after the restart) is judged as
    late (the return decides), not as a violation during the run."""
    release = env.tmp / "release"
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "finish_while_detached_late_bash")
    monkeypatch.setenv("STUB_CLAUDE_RELEASE", str(release))
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_run_started(task_id)
    await restart(env)
    release.touch()
    st = await wait_status(env.engine, task_id, "completed", "failed", "queued", "needs_human")
    assert st["status"] == "completed", st
    await idle(env, task_id)
    assert await attempts_of(task_id) == [("success", "parent_tool_after_completion")]
    assert len(await event_data(task_id, "parent_tool_after_completion")) == 1
    assert await event_data(task_id, "parent_tool_violation") == []
    assert len(stub_log(env.log)) == 1


# -- an unreadable process table is "unknown", never "dead" -------------------------------------
@contextmanager
def ps_failing(calls: list[int] | None = None) -> Iterator[None]:
    """Every process listing fails (as a ``ps`` that times out or errors: empty output)."""
    saved = (procs._run, procs._linux_stat, procs._read_table)

    def failed(cmd: list[str]) -> str:
        if calls is not None:
            calls.append(1)
        return ""

    def failed_stat(pid: int) -> None:  # linux reads /proc instead of running ps
        if calls is not None:
            calls.append(1)

    procs._run = failed  # type: ignore[assignment]
    procs._linux_stat = failed_stat  # type: ignore[assignment]
    procs._read_table = dict  # type: ignore[assignment]
    try:
        yield
    finally:
        procs._run, procs._linux_stat, procs._read_table = saved  # type: ignore[assignment]


async def start_hanging(env: Env, monkeypatch: pytest.MonkeyPatch) -> tuple[str, Any]:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    task_id = str((await submit(env))["task_id"])
    await env.engine.start()
    running = await running_with_pid(env.engine, task_id)

    async def launched() -> bool:
        return len(stub_log(env.log)) == 1 and running_stream(env, task_id).stat().st_size > 0

    await wait_for(launched)
    return task_id, running


def running_stream(env: Env, task_id: str) -> Path:
    return env.engine.runstore.attempt_paths(task_id, 1)[0]


async def cancel_and_wait(env: Env, task_id: str) -> None:
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"
    await wait_for(lambda: _attempts_closed(task_id, 1))
    await idle(env, task_id)


def test_liveness_unreadable_table_is_unknown() -> None:
    sleeper, start = start_sleeper()
    try:
        assert procs.liveness(sleeper.pid, start) == "same"
        with ps_failing():
            assert procs.liveness(sleeper.pid, start) == "unknown"
            assert procs.is_running(sleeper.pid)  # unknown is never taken as gone
            assert not procs.alive_and_same(sleeper.pid, start)  # nor as safe to signal
            with pytest.raises(procs.ListingError):
                procs.survivors("t", {sleeper.pid: start})
        assert procs.liveness(sleeper.pid, start + 5) == "other"
    finally:
        stop_sleeper(sleeper)
    assert procs.liveness(sleeper.pid, start) == "dead"


async def test_boot_with_ps_failing_reattaches_a_live_claude(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(engine_boot, "LIVENESS_RETRY_S", 0.01)
    task_id, running = await start_hanging(env, monkeypatch)
    await restart(env, start=False)
    with ps_failing():
        await env.engine.start()
        (boot,) = await event_data(task_id, "boot_recovered")
        assert (boot["process"], boot["action"]) == ("unknown", "reattach")
        assert task_id in env.engine._live
        attempt = env.engine._attempts[task_id]
        assert not attempt.gone
    task = await task_row(task_id)
    assert task.status == "running" and task.attempt == 1  # not resumed beside itself
    assert procs.alive_and_same(running.pid, running.pid_start)
    assert len(stub_log(env.log)) == 1
    await cancel_and_wait(env, task_id)


async def test_reattached_watch_survives_ps_failures(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(engine_watch, "SLOW_POLL_S", 0.05)
    monkeypatch.setattr(engine_watch, "FAST_POLL_S", 0.05)
    task_id, running = await start_hanging(env, monkeypatch)
    await restart(env)
    calls: list[int] = []
    with ps_failing(calls):

        async def polled() -> bool:
            return len(calls) >= 10

        await wait_for(polled)
    task = await task_row(task_id)
    assert task.status == "running" and task_id in env.engine._live
    assert (
        await rows("SELECT * FROM attempts WHERE task_id = ? AND ended_at IS NOT NULL", task_id)
        == []
    )
    assert procs.alive_and_same(running.pid, running.pid_start)
    await cancel_and_wait(env, task_id)


async def test_pre_launch_sweep_with_ps_failing_does_not_launch(env: Env) -> None:
    sub = await submit(env)
    task_id = str(sub["task_id"])
    async with write_tx() as db:  # an earlier attempt ended unclean: the relaunch sweeps first
        await db.execute("UPDATE tasks SET attempt = 1 WHERE id = ?", (task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at, ended_at, outcome) "
            "VALUES (?, 1, 1.0, 2.0, 'transient')",
            (task_id,),
        )
    with ps_failing():
        await env.engine.start()

        async def requeued() -> Any:
            task = await task_row(task_id)
            return task if task.status == "queued" and task.next_attempt_at else None

        task = await wait_for(requeued, timeout=60)
    assert task.next_attempt_at and task.next_attempt_at > time.time() + 10
    assert len(await event_data(task_id, "sweep_unconfirmed")) == 2
    (sweep,) = await event_data(task_id, "sweep")
    assert sweep["trigger"] == "pre_launch" and sweep["unconfirmed"] is True
    second = (await rows("SELECT * FROM attempts WHERE task_id = ? AND n = 2", task_id))[0]
    assert second["outcome"] == "sweep_survivors"
    assert stub_log(env.log) == []  # nothing launched over a process that may still run
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"


async def test_older_open_row_of_a_running_task_not_swept_at_boot(
    env: Env, sleepers: list[subprocess.Popen[bytes]]
) -> None:
    case = replace(next(c for c in CASES if c.name == "alive-nothing"))
    await leave_behind(env, case, sleepers)
    async with write_tx() as db:  # the current attempt is 2; an older row 1 was left open
        await db.execute("UPDATE attempts SET n = 2 WHERE task_id = ?", (case.task_id,))
        await db.execute("UPDATE tasks SET attempt = 2 WHERE id = ?", (case.task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 1, 1.0)", (case.task_id,)
        )
    await restart(env)
    (closed,) = await event_data(case.task_id, "attempt_closed_at_boot")
    assert closed["n"] == 1
    assert await sweeps_of(case.task_id) == []  # the live current attempt is not swept
    assert case.sleeper is not None and case.sleeper.poll() is None
    assert case.task_id in env.engine._live
    case.sleeper.send_signal(signal.SIGTERM)
    await idle(env, case.task_id)


async def test_reattached_denials_only_of_this_attempt(env: Env) -> None:
    task_id = str((await submit(env))["task_id"])
    for tool, at in (("Bash", 10.0), ("Edit", 20.0), ("Write", 30.0)):
        async with write_tx() as db:
            await db.execute(
                "INSERT INTO events (task_id, type, data_json, at) "
                "VALUES (?, 'permission_denied', ?, ?)",
                (task_id, json.dumps({"tool": tool}), at),
            )
    assert await engine_watch._denied_before(task_id, 20.0) == {"Edit", "Write"}


async def test_unconfirmed_pre_launch_sweeps_back_off_without_end(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    """Unreadable process table at every pre-launch sweep: never launched, re-queued with a
    growing delay (30 s doubling), a streak in the events, one server-log error for the
    streak's start (and every 10th round), never charged to the resume budget."""
    sub = await submit(env)
    task_id = str(sub["task_id"])
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET attempt = 1 WHERE id = ?", (task_id,))
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at, ended_at, outcome) "
            "VALUES (?, 1, 1.0, 2.0, 'transient')",
            (task_id,),
        )
    offset = [0.0]
    env.engine._clock = lambda: time.time() + offset[0]
    delays: list[float] = []
    with ps_failing(), caplog.at_level(logging.ERROR, logger="dioxide"):
        await env.engine.start()
        for round_ in (1, 2, 3):

            async def requeued(n: int = round_) -> Any:
                found = await event_data(task_id, "sweep_requeued")
                return found if len(found) >= n else None

            found = await wait_for(requeued, timeout=60)
            event = found[round_ - 1]
            assert event["streak"] == round_
            delays.append(event["next_attempt_at"] - env.engine._clock())
            offset[0] = event["next_attempt_at"] - time.time() + 1  # the delay is over
        found = await wait_for(lambda: requeued(4), timeout=60)
    assert [round(d / 10) * 10 for d in delays] == [30, 60, 120]
    task = await task_row(task_id)
    assert task.resumes == 0 and stub_log(env.log) == []
    errors = [r for r in caplog.records if "pre-launch sweep unconfirmed" in r.getMessage()]
    assert len(errors) == 1  # the streak's start only (rounds 2-4)
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"


async def test_signalled_then_unreadable_is_recorded_as_this_kill(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A kill that sent its signals and then hit an unreadable table: the process found gone
    at the re-check is recorded as killed by this kill, not as 'not running'."""
    task_id = str((await submit(env))["task_id"])
    states = iter(["same", "dead"])
    monkeypatch.setattr(procs, "liveness", lambda pid, start: next(states))
    monkeypatch.setattr(engine_sweep, "_kill_tree", lambda *a: (procs.Swept([4242], []), None))
    monkeypatch.setattr(engine_sweep, "KILL_RETRY_S", 0.0)
    monkeypatch.setattr(engine_sweep, "_sweep", lambda *a: (procs.Swept([], []), []))
    assert await engine_sweep.kill(env.engine, task_id, 4242, 1.0, "attempt_end") is True
    assert await event_data(task_id, "kill_skipped") == []
    first = (await event_data(task_id, "sweep"))[0]
    assert first["killed"] == [4242] and first["survivors_unconfirmed"] is True


async def test_boot_liveness_checks_run_concurrently_within_a_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int] = []
    lock = threading.Lock()
    flight = [0, 0]  # in flight now, most at once

    def hanging(pid: int, start: float | None) -> str:
        with lock:
            calls.append(pid)
            flight[0] += 1
            flight[1] = max(flight[1], flight[0])
        time.sleep(0.2)  # a slow ps
        with lock:
            flight[0] -= 1
        return "unknown"

    monkeypatch.setattr(procs, "liveness", hanging)
    monkeypatch.setattr(procs, "PS_TIMEOUT_S", 0.2)
    monkeypatch.setattr(engine_boot, "LIVENESS_BUDGET_S", 0.75)
    monkeypatch.setattr(engine_boot, "LIVENESS_RETRY_S", 0.05)
    tasks = [table_task(id=f"t{i}", pid=100 + i, pid_start=1.0) for i in range(4)]
    states = await engine_boot._states(tasks)
    assert states == {t.id: "unknown" for t in tasks}
    # Each task: a retry only while one more ps fits in the budget, so never the full tries.
    assert all(1 <= calls.count(t.pid or 0) < engine_boot.LIVENESS_TRIES for t in tasks), calls
    assert flight[1] >= 2  # checked concurrently, not one task after another


def test_liveness_retries_until_the_budget_cuts_them_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fake clock: every ps takes 4 s and says ``unknown``. Budget 15 s, ps bound 10 s, pause
    0.5 s: the first read ends at 4 s, a retry fits (4.5 + 10 <= 15) and ends at 8.5 s, the
    next would not (9 + 10 > 15). Exactly 2 reads: a retry happened, the budget stopped the
    rest of the ``LIVENESS_TRIES``."""
    now = [0.0]
    calls: list[float] = []
    ps_s = [4.0]

    def slow(pid: int, start: float | None) -> str:
        calls.append(now[0])
        now[0] += ps_s[0]
        return "unknown"

    def pause(seconds: float) -> None:
        now[0] += seconds

    monkeypatch.setattr(procs, "liveness", slow)
    assert (engine_boot.LIVENESS_BUDGET_S, procs.PS_TIMEOUT_S) == (15.0, 10.0)
    assert engine_boot.LIVENESS_RETRY_S == 0.5 and engine_boot.LIVENESS_TRIES > 2
    assert process_state(42, 1.0, clock=lambda: now[0], pause=pause) == "unknown"
    assert calls == [0.0, 4.5]
    calls.clear()
    now[0], ps_s[0] = 0.0, 1.0  # a quick ps (each 1 s): every try fits the budget
    monkeypatch.setattr(procs, "PS_TIMEOUT_S", 1.0)
    process_state(42, 1.0, clock=lambda: now[0], pause=pause)
    assert len(calls) == engine_boot.LIVENESS_TRIES


# -- clean startup failures -----------------------------------------------------------------
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _unusable_key(home: Path) -> None:
    (home / "data" / "cursor.key").mkdir(parents=True)  # a dir: unreadable as a key


def _bad_roles(home: Path) -> None:
    shutil.copytree(EXAMPLES / "agents", home / "agents")
    (home / "roles.yaml").write_text("planner: planner\naliases: {qa: planner}\n")


def _future_db(home: Path) -> None:
    (home / "data").mkdir(parents=True)
    with sqlite3.connect(home / "data" / "dioxide.db") as conn:
        conn.execute("PRAGMA user_version = 99")


def _loose_child_env(home: Path) -> None:
    (home / "child.env").write_text("CLAUDE_CODE_OAUTH_TOKEN=placeholder-oauth-value-0001\n")
    (home / "child.env").chmod(0o644)


def _dioxide_in_child_env(home: Path) -> None:
    (home / "child.env").write_text("DIOXIDE_PORT=1\n")
    (home / "child.env").chmod(0o600)


@pytest.mark.parametrize(
    ("plant", "said", "logged"),
    [
        (_unusable_key, "cursor key", "cursor key unusable"),
        (_bad_roles, "invalid configuration", "invalid configuration"),
        (_future_db, "newer than this code", "database unusable"),
        (None, "already running", None),
        (_loose_child_env, "no group/other permission bits", "child env file unusable"),
        (_dioxide_in_child_env, "DIOXIDE_PORT not allowed", "child env file unusable"),
    ],
    ids=["cursor-key", "config", "database", "already-running", "child-env-mode", "child-env-name"],
)
def test_startup_failure_one_line_no_traceback(
    tmp_path: Path, plant: Any, said: str, logged: str | None
) -> None:
    """``dioxide up`` on a known startup failure: one clear line, a non-zero exit, no
    traceback; the server log (when this instance owns it) records the same failure."""
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "DIOXIDE_HOME": str(home), "DIOXIDE_AUTH_TOKEN": "t" * 24,
           "DIOXIDE_PORT": str(_free_port())}  # fmt: skip
    lock = None
    if plant is None:
        lock = acquire_home_lock(home / "data" / ".lock")  # another engine holds this home
    else:
        plant(home)
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "cli.main", "up"], env=env, capture_output=True, text=True,
            timeout=30, cwd=Path(__file__).resolve().parent.parent,
        )  # fmt: skip
    finally:
        if lock is not None:
            lock.release()
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output
    assert "Traceback" not in output, output
    (line,) = [ln for ln in output.splitlines() if "dioxide: startup failed:" in ln]
    assert said in line
    if logged is not None:
        log = [json.loads(x) for x in (home / "data" / "server.log").read_text().splitlines()]
        assert [e["msg"] for e in log if e["level"] == "ERROR"] == [logged]


def test_up_invalid_settings_one_line(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))  # no token, auth not disabled
    result = CliRunner().invoke(cli, ["up"])
    assert result.exit_code == 1 and "Traceback" not in result.output
    assert "invalid configuration" in result.output and "auth_token" in result.output
