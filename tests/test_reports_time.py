import sqlite3
import time

import httpx
import pytest

from src import stream
from src.auth import Access
from src.engine_read import _rate_limit_wait
from src.reports.db import get_read_db
from src.reports.metrics_time import wait_intervals
from src.tasks import Task
from tests.helpers import Env, task_row
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_task

T0 = float(int(time.time() // 86400) * 86400) - 3 * 86400  # a UTC midnight three days ago


def ev(type_: str, at: float, **data: object) -> dict[str, object]:
    return {"type": type_, "at": at, "data": data}


def test_wait_intervals_end_rules() -> None:
    events = [
        ev("attempt_finished", 100, status="needs_human"),
        ev("answered", 160),
        ev("attempt_finished", 200, status="needs_human"),
        ev("cancelled", 230),
    ]
    human = wait_intervals(
        events,
        starts=lambda e: e["type"] == "attempt_finished",
        ends=frozenset({"answered", "cancelled"}),
        completed_at=None,
        now=1000,
    )
    assert human == [(100, 160, False), (200, 230, False)]
    open_wait = wait_intervals(
        [ev("attempt_finished", 100)],
        starts=lambda e: True,
        ends=frozenset({"answered"}),
        completed_at=None,
        now=1000,
    )
    assert open_wait == [(100, 1000, True)]
    ended = wait_intervals(
        [ev("attempt_finished", 100)],
        starts=lambda e: True,
        ends=frozenset({"answered"}),
        completed_at=400,
        now=1000,
    )
    assert ended == [(100, 400, False)]


async def get(env: Env, query: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(f"/api/v1/metrics/time{query}")


async def plant() -> None:
    await insert_task("h", created_at=T0, completed_at=T0 + 500)
    await insert_attempt("h", 1, started_at=T0 + 7, ended_at=T0 + 100, running_s=93.0)
    await insert_event("h", "attempt_finished", {"n": 1, "status": "needs_human"}, T0 + 100)
    await insert_event("h", "answered", {}, T0 + 160)
    await insert_attempt("h", 2, started_at=T0 + 170, ended_at=T0 + 200, running_s=30.0)
    await insert_event("h", "attempt_finished", {"n": 2, "status": "needs_human"}, T0 + 200)
    await insert_event("h", "cancelled", {"from": "needs_human"}, T0 + 230)
    await insert_task(
        "r",
        created_at=T0,
        status="completed",
        completed_at=T0 + 600,
        rate_limit_waited_s=60.0,
        next_attempt_at=T0 + 260,
    )
    await insert_attempt("r", 1, started_at=T0 + 1, ended_at=T0 + 200, running_s=199.0)
    await insert_event("r", "retry_scheduled", {"n": 1, "why": "rate_limited"}, T0 + 200)
    await insert_event("r", "attempt_started", {"n": 2, "mode": "resume"}, T0 + 260)
    await insert_attempt("r", 2, started_at=T0 + 260, ended_at=T0 + 290, running_s=30.0)
    await insert_event("r", "retry_scheduled", {"n": 2, "why": "transient"}, T0 + 300)
    await insert_event("r", "attempt_started", {"n": 3, "mode": "resume"}, T0 + 310)
    await insert_attempt("r", 3, started_at=T0 + 310, ended_at=T0 + 330, running_s=20.0)
    await insert_task("s", created_at=T0, status="cancelled", completed_at=T0 + 405)
    await insert_event("s", "sweep_requeued", {"n": 1}, T0 + 400)
    await insert_event("s", "cancelled", {"from": "queued"}, T0 + 405)


async def test_time_sums(reports: Env) -> None:
    await plant()
    (row,) = (await get(reports, "")).json()["rows"]
    assert row["human_wait_s"] == pytest.approx(60 + 30)
    assert row["rate_limit_wait_s"] == pytest.approx(60)
    assert row["backoff_wait_s"] == pytest.approx(10 + 5)
    assert row["queued_s"] == pytest.approx(7 + 1)
    assert row["running_s"] == pytest.approx(93 + 30 + 199 + 30 + 20)
    assert row["tasks"] == 3 and row["partial"] is False


async def test_rate_limit_wait_matches_status_split(reports: Env) -> None:
    await plant()
    task: Task = await task_row("r")
    used = _rate_limit_wait(reports.engine, task, None, ended_wait=False)["used"]
    (row,) = (await get(reports, "?task=r")).json()["rows"]
    assert row["rate_limit_wait_s"] == pytest.approx(used)


async def test_time_stats_and_bad_stat(reports: Env) -> None:
    await plant()
    (row,) = (await get(reports, "?stat=max")).json()["rows"]
    assert row["running_s"] == pytest.approx(199 + 30 + 20)
    (row,) = (await get(reports, "?stat=p50")).json()["rows"]
    assert row["running_s"] == pytest.approx(93 + 30)
    assert (await get(reports, "?stat=x")).status_code == 400


async def test_ongoing_wait_partial_only_when_window_reaches_now(reports: Env) -> None:
    now = time.time()
    await insert_task("w", status="needs_human", created_at=now - 100)
    await insert_event("w", "attempt_finished", {"n": 1, "status": "needs_human"}, now - 50)
    (row,) = (await get(reports, "")).json()["rows"]
    assert row["partial"] is True and row["human_wait_s"] == pytest.approx(50, abs=5)
    assert (await get(reports, f"?until={now - 60}")).json()["rows"] == []


async def test_wait_ends_at_its_scheduled_time_not_a_late_relaunch(reports: Env) -> None:
    await insert_task("q", created_at=T0, completed_at=T0 + 900)
    await insert_event("q", "retry_scheduled", {"why": "rate_limited", "at": T0 + 160}, T0 + 100)
    await insert_event("q", "attempt_started", {"n": 2, "mode": "resume"}, T0 + 400)  # queued
    await insert_event("q", "retry_scheduled", {"why": "transient", "at": T0 + 510}, T0 + 500)
    await insert_event("q", "attempt_started", {"n": 3, "mode": "resume"}, T0 + 505)  # early
    await insert_event("q", "sweep_requeued", {"next_attempt_at": T0 + 620}, T0 + 600)
    await insert_event("q", "attempt_started", {"n": 4, "mode": "resume"}, T0 + 700)
    (row,) = (await get(reports, "?task=q")).json()["rows"]
    assert row["rate_limit_wait_s"] == pytest.approx(60)
    assert row["backoff_wait_s"] == pytest.approx(5 + 20)


async def test_time_metric_binds_no_variable_per_task(reports: Env) -> None:
    db = get_read_db()
    await db._execute(db._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 100)  # type: ignore[attr-defined]
    for i in range(150):
        await insert_task(f"m{i}", created_at=T0)
        await insert_attempt(f"m{i}", 1, started_at=T0 + 1, ended_at=T0 + 2, running_s=1.0)
        await insert_event(f"m{i}", "sweep_requeued", {"next_attempt_at": T0 + 9}, T0 + 3)
    response = await get(reports, "")
    assert response.status_code == 200, response.text
    (row,) = response.json()["rows"]
    assert row["tasks"] == 150 and row["running_s"] == pytest.approx(150)


async def test_time_and_reliability_do_not_read_streams(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = time.time()
    await insert_task("run", status="running", created_at=now - 100)
    await insert_attempt("run", 1, started_at=now - 50)
    await insert_event("run", "attempt_started", {"n": 1, "mode": "first"}, now - 50)

    def no_reads(_: object) -> None:
        raise AssertionError("stream read")

    monkeypatch.setattr(stream, "last_interim_result", no_reads)
    (row,) = (await get(reports, "")).json()["rows"]
    assert row["running_s"] == pytest.approx(50, abs=5) and row["partial"] is True
    async with client_as(reports, Access.reports) as client:
        (rel,) = (await client.get("/api/v1/metrics/reliability")).json()["rows"]
    assert rel["attempts"] == 1 and rel["partial"] is True


async def test_unknown_query_parameter_is_400(reports: Env) -> None:
    async with client_as(reports, Access.reports) as client:
        for path in ("/api/v1/metrics/time?agnet=x", "/api/v1/metrics/usage?stat=p50",
                     "/api/v1/agents?bucket=day", "/api/v1/projects?group_by=agent",
                     "/api/v1/tasks/x/attempts?since=1"):  # fmt: skip
            response = await client.get(path)
            assert response.status_code == 400, path
            assert "unknown query parameter" in response.json()["error"], path
        assert (await client.get("/api/v1/metrics/time?stat=p50&bucket=day")).status_code == 200
