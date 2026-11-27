import time

import httpx
import pytest

from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_step, insert_task

pytestmark = pytest.mark.usefixtures("reports")

DAY = 86400.0
D = float(int(time.time() // DAY) * DAY) - 5 * DAY  # a UTC midnight five days ago


async def get(env: Env, query: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(f"/api/v1/metrics/stages{query}")


async def plant() -> None:
    await insert_task("w1", kind="lifecycle", agent="lead", cwd="/proj/a", created_at=D)
    await insert_task("w2", kind="lifecycle", agent="lead", cwd="/proj/b", created_at=D)
    common = {"status": "completed", "model": "m", "tool_calls": 2, "duration_s": 10.0}
    await insert_step(
        "w1", "implement@a1", agent_type="dx-impl", stage_tokens=100, finished_at=D + 10, **common
    )
    await insert_step(
        "w2", "implement@a1", agent_type="dx-qa", stage_tokens=300, finished_at=D + DAY + 10,
        **common,
    )  # fmt: skip
    # A replay served from cache: counted as a cached run, its numbers belong to the earlier run.
    await insert_step(
        "w2", "plan@a1", agent_type="dx-planner", stage_tokens=999, cached=1,
        finished_at=D + DAY + 5, **common,
    )  # fmt: skip


async def test_stages_grouped_by_stage(reports: Env) -> None:
    await plant()
    body = (await get(reports, "?group_by=stage")).json()
    by_stage = {r["stage"]: r for r in body["rows"]}
    assert {k: by_stage["implement"][k] for k in ("stage_tokens", "runs", "cached_runs")} == {
        "stage_tokens": 400, "runs": 2, "cached_runs": 0,
    }  # fmt: skip
    assert by_stage["implement"]["tool_calls"] == 4 and by_stage["implement"]["tasks"] == 2
    # The cached row still holds the numbers of the run that measured it (one row per label).
    assert {k: by_stage["plan"][k] for k in ("stage_tokens", "runs", "cached_runs")} == {
        "stage_tokens": 999, "runs": 1, "cached_runs": 1,
    }  # fmt: skip


async def test_cached_row_without_numbers_counts_only_the_replay(reports: Env) -> None:
    await insert_task("w9", kind="lifecycle", created_at=D)
    await insert_step("w9", "setup", status="completed", cached=1, finished_at=D + 5)
    (row,) = (await get(reports, "?group_by=stage")).json()["rows"]
    assert (row["runs"], row["cached_runs"], row["stage_tokens"]) == (0, 1, None)


async def test_stages_by_day(reports: Env) -> None:
    await plant()
    rows = (await get(reports, "?group_by=stage&bucket=day")).json()["rows"]
    assert [(r["stage"], r["stage_tokens"]) for r in rows] == [
        ("implement", 100), ("implement", 300), ("plan", 999)
    ]  # fmt: skip


async def test_stage_agent_filter(reports: Env) -> None:
    await plant()
    rows = (await get(reports, "?stage_agent=dx-qa&group_by=stage_agent")).json()["rows"]
    assert [(r["stage_agent"], r["stage_tokens"]) for r in rows] == [("dx-qa", 300)]


async def test_running_stage_counts_now_partial(reports: Env) -> None:
    await insert_task("w3", kind="lifecycle", status="running", created_at=D)
    await insert_step(
        "w3", "qa-r1@a1", status="running", started_at=D, stage_tokens=50, agent_type="dx-qa"
    )
    (row,) = (await get(reports, "?group_by=stage")).json()["rows"]
    assert (row["stage"], row["stage_tokens"], row["partial"]) == ("qa", 50, True)
    past = (await get(reports, f"?group_by=stage&until={D + DAY}")).json()["rows"]
    assert past == []


async def test_stages_bad_params(reports: Env) -> None:
    assert (await get(reports, "?group_by=agent")).status_code == 400
    async with client_as(reports, Access.reports) as client:
        usage = await client.get("/api/v1/metrics/usage?stage_agent=x")
    assert usage.status_code == 400


async def test_ended_stage_without_finish_counts_at_last_progress(reports: Env) -> None:
    """Only completed steps get ``finished_at``; a failed one counts at its last progress."""
    await insert_task("w4", kind="lifecycle", status="failed", created_at=D)
    await insert_step(
        "w4", "fix@a1", status="failed", started_at=D, last_progress_at=D + DAY + 1,
        stage_tokens=7,
    )  # fmt: skip
    rows = (await get(reports, "?group_by=stage&bucket=day")).json()["rows"]
    assert [(r["bucket"], r["stage_tokens"], r["partial"]) for r in rows] == [
        (rows[0]["bucket"], 7, False)
    ]
    assert rows[0]["bucket"].startswith(time.strftime("%Y-%m-%d", time.gmtime(D + DAY)))
