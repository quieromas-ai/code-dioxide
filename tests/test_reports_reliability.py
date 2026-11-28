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


CAUSE_COLUMNS = (
    "answer_resumes", "transient_resumes", "corrective_resumes", "stage_failed_resumes",
    "rate_limit_resumes", "unattributed_resumes",
)  # fmt: skip


def _identity_holds(row: dict[str, int]) -> bool:
    return row["resumes"] == sum(row[c] for c in CAUSE_COLUMNS)


async def _attempt(task_id: str, n: int, at: float, started: dict[str, object]) -> None:
    await insert_event(task_id, "attempt_started", {"n": n, **started}, at)
    await insert_attempt(task_id, n, ended_at=at + 1, reason="clean_exit")


async def test_reliability_derives_cause_of_old_events(reports: Env) -> None:
    """Events written before ``cause`` existed: the cause comes from the event before."""
    await plant()
    (row,) = (await get(reports, "")).json()["rows"]
    assert {c: row[c] for c in CAUSE_COLUMNS} == {
        "answer_resumes": 0, "transient_resumes": 0, "corrective_resumes": 1,
        "stage_failed_resumes": 0, "rate_limit_resumes": 0, "unattributed_resumes": 0,
    }  # fmt: skip
    assert _identity_holds(row)


async def test_reliability_resume_causes(reports: Env) -> None:
    await insert_task("t", status="completed", created_at=T0, completed_at=T0 + 100)
    await _attempt("t", 1, T0 + 1, {"mode": "first", "cause": None})
    for n, cause in enumerate(
        ("answer", "transient", "corrective", "stage_failed", "rate_limited",
         "rate_limited_fallback"),
        start=2,
    ):  # fmt: skip
        await _attempt("t", n, T0 + 10 * n, {"mode": "resume", "cause": cause})
    await _attempt("t", 8, T0 + 80, {"mode": "fresh", "cause": "fresh"})
    (row,) = (await get(reports, "")).json()["rows"]
    assert {c: row[c] for c in CAUSE_COLUMNS} == {
        "answer_resumes": 1, "transient_resumes": 1, "corrective_resumes": 1,
        "stage_failed_resumes": 1, "rate_limit_resumes": 2, "unattributed_resumes": 0,
    }  # fmt: skip
    assert (row["resumes"], row["fresh_restarts"], row["attempts"]) == (6, 1, 8)
    assert _identity_holds(row)


async def test_reliability_sweep_requeue_inherits_and_pruned_is_unattributed(reports: Env) -> None:
    await insert_task("s", status="completed", created_at=T0, completed_at=T0 + 100)
    await _attempt("s", 1, T0 + 1, {"mode": "first"})
    await insert_event("s", "answered", {"k": 1, "label": "plan", "mode": "after"}, T0 + 5)
    await _attempt("s", 2, T0 + 10, {"mode": "resume"})  # never spawned: swept, re-queued
    await insert_event("s", "sweep_requeued", {"pids": [1]}, T0 + 12)
    await _attempt("s", 3, T0 + 20, {"mode": "resume"})  # inherits the answer
    # An old resume event with nothing before it to derive a cause from (hand-edited history).
    await insert_task("p", status="completed", created_at=T0, completed_at=T0 + 100)
    await _attempt("p", 2, T0 + 30, {"mode": "resume"})
    (row,) = (await get(reports, "")).json()["rows"]
    assert (row["answer_resumes"], row["unattributed_resumes"], row["resumes"]) == (2, 1, 3)
    assert _identity_holds(row)
    by_task = {}
    for task_id in ("s", "p"):
        (r,) = (await get(reports, f"?task={task_id}")).json()["rows"]
        by_task[task_id] = r["unattributed_resumes"]
    assert by_task == {"s": 0, "p": 1}  # 0 wherever the history survives
