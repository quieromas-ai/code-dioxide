import time

import pytest

from src.auth import Access
from src.engine_read import iso_or_none
from src.stream import ParentToolUse
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_step, insert_task
from tests.test_activity import result

pytestmark = pytest.mark.usefixtures("reports")

NOW = time.time()


async def plant(env: Env) -> None:
    await insert_task("single", status="running", session_id="s1", description="SECRET-D")
    await insert_attempt("single", 1, started_at=NOW - 60, ended_at=NOW - 50, cost_usd=1.0)
    await insert_attempt("single", 2, started_at=NOW - 30)
    env.engine.activity.update("single", 2, [result(0.25, 7)], NOW - 5)
    env.engine.activity.update("single", 2, [ParentToolUse("u", "Bash")], NOW - 4)
    await insert_task("life", kind="lifecycle", agent="lead", status="running", cwd="/proj/b")
    await insert_attempt("life", 1, started_at=NOW - 100)
    await insert_step("life", "plan@a1", phase="Plan", status="completed", started_at=NOW - 99)
    await insert_step(
        "life", "qa-r1@a1", phase="QA", status="running", started_at=NOW - 20,
        agent_type="dx-qa", model="m", last_tool="Edit", last_progress_at=NOW - 3,
    )  # fmt: skip
    await insert_task("human", status="needs_human")
    await insert_attempt("human", 1, started_at=NOW - 80, ended_at=NOW - 70)
    await insert_task("later", status="queued", next_attempt_at=NOW + 600)
    await insert_event(
        "later", "retry_scheduled", {"why": "rate_limited", "at": NOW + 600}, NOW - 9
    )
    await insert_task("done", status="completed")
    await insert_task("due", status="queued", next_attempt_at=NOW - 1)


async def test_activity_rows(reports: Env) -> None:
    await plant(reports)
    async with client_as(reports, Access.reports) as client:
        response = await client.get("/api/v1/activity")
        single = (await client.get("/api/v1/tasks/single")).json()
    assert "SECRET-D" not in response.text
    rows = {r["task"]: r for r in response.json()["rows"]}
    assert set(rows) == {"single", "life", "human", "later"}
    assert rows["single"]["cost_usd"] == pytest.approx(1.25) == single["cost_usd"]
    assert {k: rows["single"][k] for k in ("status", "stage", "last_tool", "waiting_on")} == {
        "status": "running", "stage": None, "last_tool": "Bash", "waiting_on": None,
    }  # fmt: skip
    assert rows["single"]["since"] == _iso(NOW - 30)
    assert rows["single"]["last_activity_at"] == _iso(NOW - 4)
    life = rows["life"]
    assert (life["stage"], life["stage_agent"], life["stage_model"], life["last_tool"]) == (
        "qa", "dx-qa", "m", "Edit",
    )  # fmt: skip
    assert (life["project"], life["kind"], life["last_activity_at"]) == (
        "/proj/b", "lifecycle", _iso(NOW - 3),
    )  # fmt: skip
    assert (rows["human"]["waiting_on"], rows["human"]["since"]) == ("human", _iso(NOW - 70))
    assert (rows["later"]["waiting_on"], rows["later"]["since"]) == ("rate_limit", _iso(NOW - 9))
    assert set(life) == {
        "task", "agent", "project", "kind", "status", "stage", "stage_agent", "stage_model",
        "last_tool", "last_activity_at", "cost_usd", "tokens", "waiting_on", "since",
    }  # fmt: skip


async def test_activity_filters(reports: Env) -> None:
    await plant(reports)
    async with client_as(reports, Access.reports) as client:
        by_agent = (await client.get("/api/v1/activity?stage_agent=dx-qa")).json()["rows"]
        by_status = (await client.get("/api/v1/activity?status=needs_human")).json()["rows"]
        bad = await client.get("/api/v1/activity?limit=5")
    assert [r["task"] for r in by_agent] == ["life"]
    assert [r["task"] for r in by_status] == ["human"]
    assert bad.status_code == 400


def _iso(ts: float) -> str:
    found = iso_or_none(ts)
    assert found is not None
    return found
