import time

import httpx

from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_task

T0 = float(int(time.time() // 86400) * 86400) - 3 * 86400

COLUMNS = (
    "submitted", "completed", "failed", "cancelled", "attempts", "call_retries", "stage_retries",
    "resumes", "fresh_restarts", "tasks_with_fallback", "killed", "auth_failed",
    "permission_denied",
)  # fmt: skip


async def get(env: Env, query: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(f"/api/v1/metrics/reliability{query}")


async def plant() -> None:
    await insert_task("a", status="completed", created_at=T0, completed_at=T0 + 50)
    await insert_attempt("a", 1, ended_at=T0 + 10, reason="stall")
    await insert_event("a", "attempt_started", {"n": 1, "mode": "first"}, T0 + 1)
    await insert_event("a", "retry_scheduled", {"n": 1, "why": "corrective"}, T0 + 10)
    await insert_attempt("a", 2, ended_at=T0 + 20, reason="clean_exit")
    await insert_event("a", "attempt_started", {"n": 2, "mode": "resume"}, T0 + 11)
    await insert_event("a", "retry_scheduled", {"n": 2, "why": "stage_failed"}, T0 + 20)
    await insert_attempt("a", 3, ended_at=T0 + 40, reason="clean_exit")
    await insert_event("a", "attempt_started", {"n": 3, "mode": "fresh"}, T0 + 21)
    await insert_event("a", "model_fallback", {"n": 3}, T0 + 21)
    await insert_event("a", "model_fallback", {"n": 3}, T0 + 22)  # still one task
    await insert_task("b", status="failed", created_at=T0, completed_at=T0 + 70)
    await insert_attempt("b", 1, ended_at=T0 + 60)
    await insert_event("b", "attempt_started", {"n": 1, "mode": "first"}, T0 + 2)
    await insert_event("b", "auth_failed", {"n": 1}, T0 + 60)
    await insert_event("b", "permission_denied", {"tool": "Bash"}, T0 + 30)
    await insert_task("c", status="cancelled", created_at=T0, completed_at=T0 + 5)


async def test_reliability_counts(reports: Env) -> None:
    await plant()
    (row,) = (await get(reports, "")).json()["rows"]
    assert {k: row[k] for k in COLUMNS} == {
        "submitted": 3, "completed": 1, "failed": 1, "cancelled": 1, "attempts": 4,
        "call_retries": 1, "stage_retries": 1, "resumes": 1, "fresh_restarts": 1,
        "tasks_with_fallback": 1, "killed": 1, "auth_failed": 1, "permission_denied": 1,
    }  # fmt: skip
    assert (await get(reports, "?group_by=status")).status_code == 400
