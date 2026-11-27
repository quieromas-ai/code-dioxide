import time

import httpx
import pytest

from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_event, insert_step, insert_task

pytestmark = pytest.mark.usefixtures("reports")

DAY = 86400.0
D = float(int(time.time() // DAY) * DAY) - 5 * DAY


async def get(env: Env, query: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(f"/api/v1/metrics/savings{query}")


async def measured(task_id: str, label: str, tokens: int, at: float) -> None:
    await insert_step(
        task_id, label, status="completed", agent_type="dx-qa", stage_tokens=tokens,
        finished_at=at,
    )  # fmt: skip


async def test_savings_counts_and_estimate(reports: Env) -> None:
    for i, tokens in enumerate((100, 200, 600)):
        await insert_task(f"m{i}", kind="lifecycle", agent="lead", created_at=D)
        await measured(f"m{i}", "qa-r1@a1", tokens, D + i)
    await insert_task("c1", kind="lifecycle", agent="lead", created_at=D)
    await insert_step("c1", "qa-r1@a2", status="completed", cached=1, agent_type="dx-qa",
                      finished_at=D + 10)  # fmt: skip
    await insert_step("c1", "plan", status="completed", cached=1, agent_type="dx-p",
                      finished_at=D + 10)  # fmt: skip
    rows = {r["stage"]: r for r in (await get(reports, "?group_by=stage")).json()["rows"]}
    assert (rows["qa"]["cached_stages"], rows["qa"]["est_stage_tokens_saved"]) == (1, 200)
    assert rows["qa"]["deduplicated_submissions"] == 0


async def test_savings_estimate_needs_three_samples(reports: Env) -> None:
    for i, tokens in enumerate((100, 200)):
        await insert_task(f"m{i}", kind="lifecycle", created_at=D)
        await measured(f"m{i}", "qa", tokens, D + i)
    await insert_task("c1", kind="lifecycle", created_at=D)
    await insert_step("c1", "qa@a1", status="completed", cached=1, agent_type="dx-qa",
                      finished_at=D + 10)  # fmt: skip
    (row,) = (await get(reports, "?group_by=stage")).json()["rows"]
    assert (row["cached_stages"], row["est_stage_tokens_saved"]) == (1, None)


async def test_savings_dedupes_per_agent(reports: Env) -> None:
    await insert_task("t1", agent="implementer", created_at=D)
    await insert_task("t2", agent="lead", created_at=D)
    await insert_event("t1", "deduplicated", {"agent": "implementer"}, D + 1)
    await insert_event("t1", "deduplicated", {"agent": "implementer"}, D + 2)
    await insert_event("t2", "deduplicated", {"agent": "lead"}, D + 3)
    rows = {r["agent"]: r for r in (await get(reports, "?group_by=agent")).json()["rows"]}
    assert rows["implementer"]["deduplicated_submissions"] == 2
    assert rows["lead"]["deduplicated_submissions"] == 1
    assert rows["lead"]["cached_stages"] == 0 and rows["lead"]["est_stage_tokens_saved"] is None


async def test_savings_bad_group_by(reports: Env) -> None:
    assert (await get(reports, "?group_by=model")).status_code == 400
