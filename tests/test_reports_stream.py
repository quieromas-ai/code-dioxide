import asyncio
import time

import pytest

from src.auth import Access
from src.database import write_tx
from tests.helpers import Env
from tests.reports_fixture import (
    insert_attempt,
    insert_event,
    insert_task,
    reports_app,
    sse_events,
    sse_read,
)

pytestmark = pytest.mark.usefixtures("reports")

NOW = time.time()
PATH = "/api/v1/activity/stream"
SECRET = "s3cr3t-text"


def app(env: Env, access: Access = Access.reports) -> object:
    return reports_app(env, access, keepalive_s=0.05, poll_s=0.02)


async def running_task(task_id: str) -> None:
    await insert_task(task_id, status="running", created_at=NOW - 10)
    await insert_attempt(task_id, 1, started_at=NOW - 10)


async def test_stream_sends_snapshot_then_new_events(reports: Env) -> None:
    await running_task("r1")
    await insert_event("r1", "attempt_started", {"n": 1, "mode": "fresh"}, NOW - 10)  # before

    async def plant() -> None:
        await asyncio.sleep(0.1)
        await insert_event("r1", "attempt_started", {"n": 2, "mode": "resume"}, NOW)

    planter = asyncio.create_task(plant())
    status, body = await sse_read(app(reports), PATH, lambda b: '"n": 2' in b)
    await planter
    found = sse_events(body)
    assert status == 200
    # The start position goes first: a client reconnecting before any event resumes there.
    assert found[0]["event"] == "ready" and found[0]["id"] == str(found[0]["data"]["after"])
    assert found[1]["event"] == "progress" and found[1]["data"]["task"] == "r1"
    assert found[1]["id"] is None
    events = [e for e in found if e["event"] == "event"]
    assert len(events) == 1 and events[0]["data"]["data"] == {"n": 2, "mode": "resume"}
    assert events[0]["id"] == str(events[0]["data"]["id"])


async def test_stream_resumes_after_last_event_id(reports: Env) -> None:
    await insert_task("t1")
    for n in (1, 2, 3):
        await insert_event("t1", "attempt_started", {"n": n}, NOW - 10 + n)
    status, body = await sse_read(
        app(reports), PATH, lambda b: b.count("event: event") >= 2 and ": keep-alive" in b,
        headers={"Last-Event-ID": "1"},
    )  # fmt: skip
    ids = [e["id"] for e in sse_events(body) if e["event"] == "event"]
    assert status == 200 and ids == ["2", "3"]
    _, body = await sse_read(
        app(reports), PATH, lambda b: ": keep-alive" in b, query="last_event_id=2"
    )
    assert [e["id"] for e in sse_events(body) if e["event"] == "event"] == ["3"]


@pytest.mark.parametrize(
    ("query", "headers"), [("last_event_id=x", None), ("", {"Last-Event-ID": "1.5"})]
)
async def test_stream_bad_last_event_id_is_400(
    reports: Env, query: str, headers: dict[str, str] | None
) -> None:
    status, body = await sse_read(app(reports), PATH, lambda b: True, query=query, headers=headers)
    assert status == 400 and "last_event_id" in body


async def test_stream_events_use_allowlist(reports: Env) -> None:
    await insert_task("t1")
    await insert_event("t1", "permission_denied", {"tool": "Bash", "message": SECRET}, NOW)
    _, body = await sse_read(
        app(reports), PATH, lambda b: ": keep-alive" in b, headers={"Last-Event-ID": "0"}
    )
    assert SECRET not in body
    (event,) = [e for e in sse_events(body) if e["event"] == "event"]
    assert event["data"]["data"] == {"tool": "Bash"}


async def test_stream_reports_removed_row(reports: Env) -> None:
    await running_task("r1")

    async def finish() -> None:
        await asyncio.sleep(0.1)
        async with write_tx() as db:
            await db.execute("UPDATE tasks SET status = 'completed' WHERE id = 'r1'")

    finisher = asyncio.create_task(finish())
    _, body = await sse_read(app(reports), PATH, lambda b: '"removed": true' in b)
    await finisher
    progress = [e["data"] for e in sse_events(body) if e["event"] == "progress"]
    assert progress[0]["task"] == "r1" and progress[-1] == {"task": "r1", "removed": True}


async def test_stream_filters_apply(reports: Env) -> None:
    await running_task("r1")
    await running_task("r2")
    await insert_event("r1", "submitted", {"agent": "a"}, NOW)
    await insert_event("r2", "submitted", {"agent": "a"}, NOW)
    _, body = await sse_read(
        app(reports), PATH, lambda b: ": keep-alive" in b, query="task=r2",
        headers={"Last-Event-ID": "0"},
    )  # fmt: skip
    assert {e["data"]["task"] for e in sse_events(body) if e["event"] != "ready"} == {"r2"}


async def test_stream_start_id_lets_a_quiet_client_resume(reports: Env) -> None:
    await insert_task("t1")
    await insert_event("t1", "submitted", {"agent": "a"}, NOW - 5)
    _, body = await sse_read(app(reports), PATH, lambda b: ": keep-alive" in b)
    (ready,) = [e for e in sse_events(body) if e["event"] == "ready"]
    await insert_event("t1", "cancelled", {"from": "queued"}, NOW)  # while disconnected
    _, body = await sse_read(
        app(reports), PATH, lambda b: ": keep-alive" in b, headers={"Last-Event-ID": ready["id"]}
    )
    assert [e["data"]["type"] for e in sse_events(body) if e["event"] == "event"] == ["cancelled"]
