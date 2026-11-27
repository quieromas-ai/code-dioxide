from typing import Any

import pytest

from src import stream
from src.activity import ActivityBook
from src.stream import ParentToolUse, Result, WorkflowCall
from tests.helpers import Env, restart, submit, wait_for, wait_status
from tests.reports_fixture import insert_attempt, insert_task
from tests.test_boot import wait_run_started
from tests.test_engine_lifecycle import lifecycle

pytestmark = pytest.mark.usefixtures("spawned")


def result(cost: float, tokens: int = 5) -> Result:
    return Result(
        is_error=False, subtype="success", api_error_status=None, api_error=None, text=None,
        cost_usd=cost, tokens=tokens, session_id="s1", errors=[], permission_denials=[],
        result_index=None, session_tokens=tokens,
    )  # fmt: skip


def test_book_keeps_last_result_and_tool() -> None:
    book = ActivityBook()
    assert book.get("t", 1) is None
    book.update(
        "t", 1, [ParentToolUse("u1", "Read"), result(0.1), ParentToolUse("u2", "Bash")], 5.0
    )
    book.update("t", 1, [result(0.2)], 6.0)
    entry = book.get("t", 1)
    assert entry is not None
    assert entry.last_tool == "Bash" and entry.last_result == result(0.2)
    assert entry.last_event_at == 6.0


def test_book_update_without_events_keeps_time() -> None:
    book = ActivityBook()
    book.update("t", 1, [], 4.0)
    entry = book.get("t", 1)
    assert entry is not None and entry.last_event_at is None and entry.last_result is None
    book.update("t", 1, [WorkflowCall("w1", {})], 7.0)
    book.update("t", 1, [], 9.0)
    entry = book.get("t", 1)
    assert entry is not None and entry.last_tool == "Workflow" and entry.last_event_at == 7.0


def test_book_entries_per_attempt_and_drop() -> None:
    book = ActivityBook()
    book.update("t", 1, [ParentToolUse("u", "Read")], 1.0)
    book.update("t", 2, [ParentToolUse("u", "Edit")], 2.0)
    book.drop("t", 1)
    book.drop("t", 9)  # unknown: no error
    assert book.get("t", 1) is None
    entry = book.get("t", 2)
    assert entry is not None and entry.last_tool == "Edit"


async def test_open_attempt_share_uses_cache(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    def unread(path: Any) -> None:
        raise AssertionError("the stream is not read while the cache has the attempt")

    monkeypatch.setattr(stream, "last_interim_result", unread)
    await insert_task("r1", status="running", session_id="s1")
    await insert_attempt("r1", 1, session_id="s1", started_at=1.0)
    env.engine.activity.update("r1", 1, [result(0.3)], 2.0)
    status = await env.engine.status("r1")
    assert status["cost_usd"] == pytest.approx(0.3) and status["cost_partial"] is True


async def test_activity_entry_dropped_at_attempt_end(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[str, int]] = []
    real = ActivityBook.update

    def spy(self: ActivityBook, task_id: str, n: int, *args: Any) -> None:
        seen.append((task_id, n))
        real(self, task_id, n, *args)

    monkeypatch.setattr(ActivityBook, "update", spy)
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed")
    assert (sub["task_id"], 1) in seen
    assert env.engine.activity.get(sub["task_id"], 1) is None


async def test_activity_rebuilt_on_reattach(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    release = env.tmp / "release"
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "finish_while_detached")
    monkeypatch.setenv("STUB_CLAUDE_RELEASE", str(release))
    task_id = await lifecycle(env)
    await env.engine.start()
    await wait_run_started(task_id)
    await restart(env)

    async def replayed() -> bool:
        entry = env.engine.activity.get(task_id, 1)
        return entry is not None and entry.last_tool == "Workflow"

    try:
        await wait_for(replayed)
    finally:
        release.touch()
    await wait_status(env.engine, task_id, "completed", "failed", "queued", "needs_human")
