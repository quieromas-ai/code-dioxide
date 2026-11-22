"""Watchdog: a stalled attempt or one over the task timeout is killed once (``kill_for``); the
waiter decides what follows. Timing is driven by file mtimes and the engine's clock seam, with
short polling intervals and deadlines, never by a stage finishing within a time window."""

import asyncio
import json
import os
import signal
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import pytest

from src import engine_sweep, engine_watch, procs, watchdog
from src.database import write_tx
from src.journal import slug
from src.tasks import Task
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
from tests.test_boot import ps_failing
from tests.test_engine_lifecycle import ENDED, journal_started, lifecycle
from tests.test_engine_sweep import start_sleeper, stop_sleeper
from tests.test_recovery import event_data

HOUR = 3600.0


def watch_fast(env: Env, **values: Any) -> None:
    env.settings.watchdog_interval_s = 0.05
    env.settings.stall_timeout_s = 600  # nothing ages by itself within a test
    for name, value in values.items():
        setattr(env.settings, name, value)


def session_dir(env: Env, session_id: str) -> Path:
    return env.settings.claude_home / "projects" / slug(str(env.repo)) / session_id


def age(paths: list[Path], seconds: float = HOUR) -> None:
    """Make files look untouched for ``seconds`` (what the watchdog reads is their mtime)."""
    then = time.time() - seconds
    for path in paths:
        os.utime(path, (then, then))


def session_files(env: Env, session_id: str) -> list[Path]:
    sdir = session_dir(env, session_id)
    files = [p for p in sdir.rglob("*") if p.is_file()]
    sibling = sdir.with_name(f"{sdir.name}.jsonl")
    return [*files, sibling] if sibling.exists() else files


def _progress_started(stream: Path, label: str) -> bool:
    """The stream holds the ``task_progress`` line of ``label``'s start: the stub writes it after
    the step's journal record, transcript and meta file, right before the stage body runs."""
    try:
        lines = stream.read_text().splitlines()
    except OSError:
        return False
    for raw in lines:
        try:
            line = json.loads(raw)
        except ValueError:
            continue
        if line.get("subtype") == "task_progress" and line.get("description") == label:
            return True
    return False


async def in_implement(env: Env, task_id: str) -> Task:
    """The run's ``implement`` stage body runs (all its files written; the stub stays in it)."""

    async def check() -> Task | None:
        task = await task_row(task_id)
        attempt = env.engine._attempts.get(task_id)
        if not (task.run_id and task.session_id and task.pid and attempt):
            return None
        if "implement" not in journal_started(env, task_id, task.run_id, task.session_id):
            return None
        return task if _progress_started(attempt.stream_path, "implement") else None

    result: Task = await wait_for(check)
    return result


def stage_transcript(env: Env, session_id: str, label: str) -> Path:
    """The step transcript of ``label``, found by its meta file's description."""
    for meta in session_dir(env, session_id).rglob("agent-*.meta.json"):
        if json.loads(meta.read_text()).get("description") == label:
            return meta.with_name(meta.name.removesuffix(".meta.json") + ".jsonl")
    raise AssertionError(f"no transcript for {label}")


async def ticks(env: Env, n: int) -> None:
    """Wait for ``n`` more complete watchdog passes."""
    target = env.engine._watchdog_ticks + n

    async def done() -> bool:
        return env.engine._watchdog_ticks >= target

    await wait_for(done)


def count_kills(env: Env) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    inner = env.engine._kill

    async def counting(task_id: str, pid: int, start: float | None, trigger: str) -> bool:
        calls.append((task_id, trigger))
        return await inner(task_id, pid, start, trigger)

    env.engine._kill = counting  # type: ignore[method-assign]
    return calls


async def stub_writing(env: Env, task_id: str) -> None:
    """The stub runs and has written its first stream line (later aging then sticks)."""
    attempt = env.engine._attempts[task_id]

    async def started() -> bool:
        path = attempt.stream_path
        return bool(stub_log(env.log)) and path.exists() and path.stat().st_size > 0

    await wait_for(started)


async def attempts_of(task_id: str) -> list[Any]:
    return await rows("SELECT * FROM attempts WHERE task_id = ? ORDER BY n", task_id)


async def test_watchdog_one_spawn(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """An idle stage: killed once for the stall however many passes see it, then one resume."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage,success")
    watch_fast(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    kills = count_kills(env)
    task = await in_implement(env, task_id)
    assert task.session_id
    attempt = env.engine._attempts[task_id]
    age([attempt.stream_path, *session_files(env, task.session_id)])
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st

    (stall,) = await event_data(task_id, "stall")
    assert stall["n"] == 1 and stall["idle_s"] > HOUR - 60 and stall["limit_s"] == 600
    assert kills == [(task_id, "attempt_end")]  # one kill for the stalled attempt
    assert len(stub_log(env.log)) == 2  # one spawn for the resume
    one, two = await attempts_of(task_id)
    assert (one["outcome"], one["reason"]) == ("transient", "stall")
    assert two["reason"] == "clean_exit" and one["ended_at"] <= two["started_at"]
    assert (await task_row(task_id)).resumes == 1


async def test_long_stage_with_activity_not_stalled(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stage that keeps writing its transcript is not stalled, though the stream is silent;
    once it goes quiet, it is."""
    quiet = env.tmp / "quiet"
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "long_stage,success")
    monkeypatch.setenv("STUB_CLAUDE_QUIET", str(quiet))
    watch_fast(env, retry_backoff_s=0.0)
    task_id = await lifecycle(env)
    await env.engine.start()
    task = await in_implement(env, task_id)
    assert task.session_id and task.run_id
    attempt = env.engine._attempts[task_id]
    files = session_files(env, task.session_id)
    transcript = stage_transcript(env, task.session_id, "implement")
    age([attempt.stream_path, *(p for p in files if p != transcript)])
    await ticks(env, 20)
    task = await task_row(task_id)
    assert (task.status, task.kill_reason) == ("running", None)
    assert await event_data(task_id, "stall") == []

    quiet.touch()  # the stage stops writing and idles

    async def still() -> bool:
        size = transcript.stat().st_size
        await asyncio.sleep(0.3)
        return transcript.stat().st_size == size

    await wait_for(still)
    age([transcript])
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st
    (stall,) = await event_data(task_id, "stall")
    assert stall["n"] == 1
    assert [a["reason"] for a in await attempts_of(task_id)] == ["stall", "clean_exit"]


async def test_timeout_excludes_waiting(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """The task timeout sums the attempts' running time: a long needs_human wait between them
    does not count; running past the limit does."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "needs_human_gate,hang")
    watch_fast(env, task_timeout_s=1000, stall_timeout_s=10**6)
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_status(env.engine, task_id, "needs_human")
    async with write_tx() as db:  # the human answered after 5000 s of waiting
        await db.execute(
            "UPDATE attempts SET started_at = started_at - 5000, ended_at = ended_at - 5000 "
            "WHERE task_id = ?",
            (task_id,),
        )
        await db.execute(
            "UPDATE tasks SET started_at = started_at - 5000, created_at = created_at - 5000 "
            "WHERE id = ?",
            (task_id,),
        )
    (first,) = await attempts_of(task_id)
    assert first["running_s"] < 100
    assert (await env.engine.answer(task_id, "approved", "mcp"))["status"] == "queued"
    second = await running_with_pid(env.engine, task_id)
    assert second.attempt == 2
    await ticks(env, 10)
    task = await task_row(task_id)
    assert (task.status, task.kill_reason) == ("running", None)  # 5000 s of waiting not counted

    real = env.engine._clock
    env.engine._clock = lambda: real() + 1000  # the second attempt has now run for 1000 s
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "timeout" in (st.get("error") or ""), st
    (timeout,) = await event_data(task_id, "timeout")
    assert timeout["n"] == 2 and 1000 < timeout["running_s"] < 1000 + 100 + first["running_s"]
    assert [a["reason"] for a in await attempts_of(task_id)] == ["clean_exit", "timeout"]
    assert len(stub_log(env.log)) == 2  # a timeout is final: no relaunch


async def test_single_task_stall_killed(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """Single tasks get the same enforcement (activity = their stream)."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    watch_fast(env, max_resumes=0)
    sub = await submit(env)
    task_id = str(sub["task_id"])
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    await ticks(env, 5)
    assert (await task_row(task_id)).kill_reason is None
    age([env.engine._attempts[task_id].stream_path])
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "stall" in (st.get("error") or ""), st
    assert [a["reason"] for a in await attempts_of(task_id)] == ["stall"]


async def test_watchdog_covers_a_reattached_attempt(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    watch_fast(env, max_resumes=0)
    sub = await submit(env)
    task_id = str(sub["task_id"])
    await env.engine.start()
    running = await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    await restart(env)
    attempt = env.engine._attempts[task_id]
    assert attempt.boot and not attempt.gone and not procs.tracked(attempt.pid)
    age([attempt.stream_path])
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "stall" in (st.get("error") or ""), st
    assert not procs.alive_and_same(running.pid or 0, running.pid_start)  # killed
    assert len(stub_log(env.log)) == 1
    assert [a["reason"] for a in await attempts_of(task_id)] == ["stall"]


async def wait_run_done(env: Env, task_id: str, n: int) -> None:
    async def run_done() -> bool:
        return (task_id, n) in env.engine.book.run_done

    await wait_for(run_done)


async def test_parent_hanging_after_completion_killed_return_decides(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The run reported completion with a valid return, the parent does not exit: the watchdog
    kills it after exit_after_completion_s (reason in RETURN_DECIDES), the return decides and
    the end is unclean (swept)."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success_then_hang")
    watch_fast(env, task_timeout_s=1000)
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_run_done(env, task_id, 1)
    await ticks(env, 5)
    assert (await task_row(task_id)).kill_reason is None  # within the 120 s after completion
    real = env.engine._clock
    env.engine._clock = lambda: real() + 200
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "completed", st

    async def job_done() -> bool:  # the waiter's end-of-attempt sweep and the kill are over
        return task_id not in env.engine._live and not env.engine._kills

    await wait_for(job_done)
    (killed,) = await event_data(task_id, "exit_after_completion")
    assert killed["n"] == 1 and killed["after_s"] > 120 and killed["limit_s"] == 120
    (one,) = await attempts_of(task_id)
    assert (one["outcome"], one["reason"]) == ("success", "exit_after_completion")
    ends = [e for e in await event_data(task_id, "sweep") if e["trigger"] == "attempt_end"]
    assert ends and all(e["policy"] == "swept" for e in ends)  # unclean: swept
    assert await event_data(task_id, "timeout") == [] and await event_data(task_id, "stall") == []
    assert len(stub_log(env.log)) == 1


async def test_parent_hanging_after_needs_human_killed_question_answerable_once(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success_then_hang,success")
    monkeypatch.setenv("STUB_CLAUDE_DETACHED_RETURN", "needs_human")
    watch_fast(env, task_timeout_s=1000)
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_run_done(env, task_id, 1)
    real = env.engine._clock
    env.engine._clock = lambda: real() + 200
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "needs_human", st
    task = await task_row(task_id)
    assert json.loads(task.question_json or "{}")["kind"] == "plan_approval"
    (one,) = await attempts_of(task_id)
    assert (one["outcome"], one["reason"]) == ("needs_human", "exit_after_completion")

    async def idle() -> bool:
        return task_id not in env.engine._live

    await wait_for(idle)
    assert (await env.engine.answer(task_id, "approved", "mcp"))["status"] == "queued"
    again = await env.engine.answer(task_id, "approved again", "mcp")
    assert again.get("error"), again  # answerable once
    st = await wait_status(env.engine, task_id, "completed", "failed", "cancelled")
    assert st["status"] == "completed", st
    assert len(stub_log(env.log)) == 2
    # The killed attempt was unclean: the relaunch after the answer swept first.
    pre = [e for e in await event_data(task_id, "sweep") if e["trigger"] == "pre_launch"]
    assert [e["policy"] for e in pre] == ["swept", "swept"]  # the first launch's, the relaunch's


async def test_given_up_kill_is_requested_again(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """The first kill finds the process table unreadable and gives up; once it is readable
    again the watchdog requests the kill again: the task is decided without cancel/restart."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setattr(engine_sweep, "KILL_RETRY_S", 0.01)
    monkeypatch.setattr(engine_watch, "KILL_REREQUEST_S", 0.2)
    watch_fast(env, max_resumes=0)
    sub = await submit(env)
    task_id = str(sub["task_id"])
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    with ps_failing():
        age([env.engine._attempts[task_id].stream_path])

        async def given_up() -> bool:
            return bool(await event_data(task_id, "kill_unconfirmed"))

        await wait_for(given_up)
        assert (await task_row(task_id)).status == "running"
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed" and "stall" in (st.get("error") or ""), st
    assert len(await event_data(task_id, "stall")) == 1
    assert len(await event_data(task_id, "kill_rerequested")) >= 1
    assert len(stub_log(env.log)) == 1

    async def forgotten() -> bool:  # the job is over: its bookkeeping is dropped
        return task_id not in env.engine._live and not any(
            k[0] == task_id for k in (*env.engine.book.killing, *env.engine.book.kill_retry)
        )

    await wait_for(forgotten)


async def test_one_activity_read_per_attempt(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """A hung file system: the watchdog never starts a second read for the same attempt."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setattr(watchdog, "ACTIVITY_TIMEOUT_S", 0.05)
    watch_fast(env)
    release = threading.Event()
    calls: list[str] = []

    def hung(engine: Any, attempt: Any) -> float | None:
        calls.append(attempt.task.id)
        release.wait(30)
        return None

    monkeypatch.setattr(watchdog, "last_activity", hung)
    sub = await submit(env)
    task_id = str(sub["task_id"])
    try:
        await env.engine.start()
        await running_with_pid(env.engine, task_id)
        await ticks(env, 15)
        assert calls == [task_id]
    finally:
        release.set()

    async def read_again() -> bool:  # the read finished: a new one may start
        return len(calls) >= 2

    await wait_for(read_again)
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"
    await wait_for(lambda: _closed(task_id))


async def _closed(task_id: str) -> bool:
    found = await rows("SELECT ended_at FROM attempts WHERE task_id = ?", task_id)
    return bool(found) and all(r["ended_at"] is not None for r in found)


async def test_reattach_with_a_pending_kill_kills_once(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A kill requested before the restart is requested again at re-attach; a second request
    for the same attempt (the replay, the watchdog) starts no second kill."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    watch_fast(env, max_resumes=0)
    sub = await submit(env)
    task_id = str(sub["task_id"])
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    await restart(env, start=False)
    async with write_tx() as db:  # the old engine recorded the stall, then went down
        await db.execute("UPDATE tasks SET kill_reason = 'stall' WHERE id = ?", (task_id,))
    kills = count_kills(env)
    await env.engine.start()
    await env.engine._kill_for(env.engine._attempts[task_id], "stall")
    st = await wait_status(env.engine, task_id, *ENDED)
    assert st["status"] == "failed", st
    assert kills == [(task_id, "attempt_end")]
    assert [a["reason"] for a in await attempts_of(task_id)] == ["stall"]


async def open_attempts() -> int:
    found = await rows(
        "SELECT COUNT(*) AS n FROM attempts a JOIN tasks t ON a.task_id = t.id "
        "WHERE a.ended_at IS NULL"
    )
    return int(found[0]["n"])


async def test_cancel_kill_given_up_is_requested_again(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancel while the process table is unreadable: the cancel kill gives up; once it is
    readable again the watchdog retries it: process killed, attempt closed, slot freed."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setattr(engine_sweep, "KILL_RETRY_S", 0.01)
    monkeypatch.setattr(engine_watch, "KILL_REREQUEST_S", 0.2)
    watch_fast(env)
    task_id = str((await submit(env))["task_id"])
    await env.engine.start()
    running = await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    with ps_failing():
        assert (await env.engine.cancel(task_id))["status"] == "cancelled"

        async def given_up() -> bool:
            return bool(await event_data(task_id, "kill_unconfirmed"))

        await wait_for(given_up)
        assert await open_attempts() == 1  # the hung process still holds its slot
    rerequest = (await wait_for(lambda: _rerequested(task_id)))[0]
    assert rerequest["reason"] == "cancel" and rerequest["given_up"] == 1

    async def freed() -> bool:
        return await open_attempts() == 0 and task_id not in env.engine._live

    await wait_for(freed)
    assert not procs.alive_and_same(running.pid or 0, running.pid_start)
    (one,) = await attempts_of(task_id)
    assert one["reason"] == "cancel"
    assert (await task_row(task_id)).status == "cancelled"


async def test_cancel_in_spawn_window_kill_given_up_is_requested_again(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancelled inside the spawn window while the process table is unreadable: the new
    process's kill gives up and, once ``ps`` reads again, the watchdog retries it."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    monkeypatch.setattr(engine_sweep, "KILL_RETRY_S", 0.01)
    monkeypatch.setattr(engine_watch, "KILL_REREQUEST_S", 0.2)
    watch_fast(env)
    seen: list[tuple[int, float | None]] = []
    failing = ExitStack()

    async def cancel_now(task_id: str, pid: int, start: float | None) -> None:
        seen.append((pid, start))
        failing.enter_context(ps_failing())
        assert (await env.engine.cancel(task_id))["status"] == "cancelled"

    env.engine._after_spawn = cancel_now
    task_id = str((await submit(env))["task_id"])
    try:
        await env.engine.start()

        async def given_up() -> bool:
            return bool(await event_data(task_id, "kill_unconfirmed"))

        await wait_for(given_up)
        assert await open_attempts() == 1  # the new process still holds its slot
    finally:
        failing.close()
    rerequest = (await wait_for(lambda: _rerequested(task_id)))[0]
    assert rerequest["reason"] == "cancel" and rerequest["given_up"] == 1

    async def freed() -> bool:
        return await open_attempts() == 0 and task_id not in env.engine._live

    await wait_for(freed)
    assert not procs.alive_and_same(*seen[0])
    (one,) = await attempts_of(task_id)
    assert one["reason"] == "cancel"
    assert (await task_row(task_id)).status == "cancelled"


async def _rerequested(task_id: str) -> list[dict[str, Any]]:
    return await event_data(task_id, "kill_rerequested")


async def test_cancel_during_a_stall_kill_one_kill_in_flight(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    watch_fast(env, max_resumes=0)
    task_id = str((await submit(env))["task_id"])
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    await stub_writing(env, task_id)
    gate = asyncio.Event()
    calls: list[str] = []
    inner = env.engine._kill

    async def held(tid: str, pid: int, start: float | None, trigger: str) -> bool:
        calls.append(trigger)
        await gate.wait()  # the stall kill is in flight
        return await inner(tid, pid, start, trigger)

    env.engine._kill = held  # type: ignore[method-assign]
    age([env.engine._attempts[task_id].stream_path])

    async def stall_kill_started() -> bool:
        return calls == ["attempt_end"]

    await wait_for(stall_kill_started)
    assert (await env.engine.cancel(task_id))["status"] == "cancelled"
    await ticks(env, 5)
    assert calls == ["attempt_end"]  # the cancel started no second kill
    gate.set()

    async def freed() -> bool:
        return await open_attempts() == 0 and task_id not in env.engine._live

    await wait_for(freed)
    assert calls == ["attempt_end"]
    assert (await task_row(task_id)).status == "cancelled"


async def test_terminal_unconfirmed_sweep_retried_until_confirmed(env: Env) -> None:
    """The end-of-attempt sweep of a task that is now terminal met an unreadable process table:
    no pre-launch sweep follows, so the watchdog retries it (growing delay) until a sweep is
    confirmed and the leftover is gone. A task that is not terminal is left to its pre-launch
    sweep."""
    clock = [1000.0]
    env.engine._clock = lambda: clock[0]
    task_id = (await submit(env))["task_id"]
    queued = (await submit(env, "Say other"))["task_id"]
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'failed' WHERE id = ?", (task_id,))
    sleeper, start = start_sleeper()
    try:
        env.engine.runstore.write_procs(task_id, {sleeper.pid: start})
        with ps_failing():
            for tid in (task_id, queued):
                await engine_sweep.sweep_leftovers(env.engine, tid, "attempt_end", {})
            assert set(env.engine.book.sweep_retry) == {task_id}
            assert env.engine.book.sweep_retry[task_id][2:] == (1, 1000.0 + 30.0)
            await watchdog.check(env.engine)  # not due yet
            assert env.engine._kills == set()
            clock[0] += 30.0
            await watchdog.check(env.engine)
            await asyncio.gather(*env.engine._kills)
            assert env.engine.book.sweep_retry[task_id][2:] == (2, 1030.0 + 60.0)
        assert sleeper.poll() is None  # nothing was signalled on an unreadable table
        clock[0] += 60.0
        await watchdog.check(env.engine)
        await asyncio.gather(*env.engine._kills)
        assert task_id not in env.engine.book.sweep_retry
        assert sleeper.wait(timeout=20) is not None
        unconfirmed = await event_data(task_id, "sweep_unconfirmed")
        assert unconfirmed == [{"trigger": "attempt_end"}] * 2
        (swept,) = await event_data(task_id, "sweep")
        assert swept["killed"] == [sleeper.pid] and swept["trigger"] == "attempt_end"
    finally:
        stop_sleeper(sleeper)


async def _child_cpu(env: Env, task_id: str) -> tuple[int, float]:
    """The stage's long command as the watch's latest process snapshot saw it: (pid, CPU s)."""
    child = next(e["stage_child_pid"] for e in stub_log(env.log) if "stage_child_pid" in e)
    task = await task_row(task_id)
    seen = env.engine.book.cpu.get((task_id, task.attempt), {})
    return child, seen[child][1] if child in seen else -1.0


@pytest.mark.parametrize(
    ("scenario", "stalled"), [("burn_in_stage", False), ("sleep_in_stage", True)]
)
async def test_long_command_cpu_counts_as_activity(
    env: Env, monkeypatch: pytest.MonkeyPatch, scenario: str, stalled: bool
) -> None:
    """A stage runs one long command and no file changes (the clock seam puts every mtime
    past ``stall_timeout_s``): a command that burns CPU (>= 1 s since the previous pass) is
    activity, one that only sleeps is a stall."""
    children: list[tuple[int, float | None]] = []
    try:
        monkeypatch.setenv("STUB_CLAUDE_SCENARIO", scenario)
        watch_fast(env, watchdog_interval_s=HOUR, max_resumes=0)  # passes are run by the test
        task_id = await lifecycle(env)
        await env.engine.start()
        await in_implement(env, task_id)

        async def child_seen() -> tuple[int, float] | None:
            found = stub_log(env.log)
            if not any("stage_child_pid" in e for e in found):
                return None
            pid, cpu = await _child_cpu(env, task_id)
            return (pid, cpu) if cpu >= 0 else None

        child, baseline = await wait_for(child_seen)
        children.append((child, procs.proc_start_time(child)))
        await watchdog.check(env.engine)  # the first pass: files are fresh, figures recorded
        assert (await task_row(task_id)).kill_reason is None

        if not stalled:

            async def burnt() -> bool:  # the watch's snapshots show 1.2 s more CPU time
                return (await _child_cpu(env, task_id))[1] >= baseline + 1.2

            await wait_for(burnt)
        real = time.time
        env.engine._clock = lambda: real() + 2 * env.settings.stall_timeout_s
        await watchdog.check(env.engine)
        task = await task_row(task_id)
        if stalled:
            assert task.kill_reason == "stall", task
            st = await wait_status(env.engine, task_id, *ENDED)
            assert st["status"] == "failed" and "stall" in st["error"], st
        else:
            assert task.kill_reason is None and task.status == "running", task
            await env.engine.cancel(task_id)
    finally:  # the stage's command is the stub's child: never left running by a failure
        for pid, began in children:
            if procs.alive_and_same(pid, began):
                os.kill(pid, signal.SIGKILL)


async def test_quiet_pass_after_busy_ones_measures_idle_from_the_last_busy_pass(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Busy for longer than ``stall_timeout_s`` (no file written), then one quiet pass: idle
    counts from the last busy pass, not from the files, so no kill; quiet for
    ``stall_timeout_s`` after that busy pass: a stall."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "sleep_in_stage")
    watch_fast(env, watchdog_interval_s=HOUR, max_resumes=0)  # passes are run by the test
    busy = [True]
    monkeypatch.setattr(watchdog, "busy_processes", lambda engine, key: busy[0])
    task_id = await lifecycle(env)
    await env.engine.start()
    await in_implement(env, task_id)
    stall = env.settings.stall_timeout_s
    t0 = time.time()
    clock = [t0]
    env.engine._clock = lambda: clock[0]
    try:
        for at, is_busy, killed in (
            (t0, True, False),
            (t0 + stall + 100, True, False),  # busy for longer than the stall limit
            (t0 + stall + 400, False, False),  # quiet, 300 s after the last busy pass
            (t0 + 2 * stall + 101, False, True),  # quiet for the stall limit after it
        ):
            clock[0], busy[0] = at, is_busy
            await watchdog.check(env.engine)
            reason = (await task_row(task_id)).kill_reason
            assert reason == ("stall" if killed else None), (at - t0, reason)
        st = await wait_status(env.engine, task_id, *ENDED)
        assert st["status"] == "failed" and "stall" in st["error"], st
    finally:  # the stage's command is the stub's child: never left running by a failure
        for entry in stub_log(env.log):
            pid = entry.get("stage_child_pid")
            if pid and procs.is_running(pid):
                os.kill(pid, signal.SIGKILL)


async def test_sweep_retry_kept_when_the_sweep_fails_otherwise(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry whose sweep fails with an error other than an unreadable listing keeps its
    entry (logged) and runs again on the next pass."""
    clock = [1000.0]
    env.engine._clock = lambda: clock[0]
    task_id = (await submit(env))["task_id"]
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'failed' WHERE id = ?", (task_id,))
    env.engine.book.sweep_retry[task_id] = ({}, "attempt_end", 1, 1000.0)
    calls = [0]
    real = engine_sweep._sweep

    def broken(*args: Any) -> Any:
        calls[0] += 1
        if calls[0] == 1:
            raise RuntimeError("disk full")
        return real(*args)

    monkeypatch.setattr(engine_sweep, "_sweep", broken)
    await watchdog.check(env.engine)
    await asyncio.gather(*env.engine._kills)
    assert env.engine.book.sweep_retry[task_id] == ({}, "attempt_end", 1, 1000.0)
    await watchdog.check(env.engine)  # the next pass retries it
    await asyncio.gather(*env.engine._kills)
    assert calls[0] == 2 and task_id not in env.engine.book.sweep_retry
    (swept,) = await event_data(task_id, "sweep")
    assert swept["trigger"] == "attempt_end"
