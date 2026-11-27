"""Identities the design promises between reporting figures (spec: Totals and reconciliation)."""

import time
from typing import Any

import pytest

from src.auth import Access
from src.database import write_tx
from src.model_usage import write_models
from src.stream import ModelUsage
from tests.helpers import Env, submit
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_task

DAY = 86400.0
D = float(int(time.time() // DAY) * DAY) - 10 * DAY
ADDITIVE_USAGE = ("cost_usd", "tokens", "attempts", "uncounted_attempts")
ADDITIVE_RELIABILITY = ("submitted", "completed", "attempts", "resumes", "fresh_restarts", "killed")


async def attempt(task: str, n: int, mode: str, ended: float, **cols: Any) -> None:
    await insert_attempt(task, n, ended_at=ended, **cols)
    await insert_event(task, "attempt_started", {"n": n, "mode": mode}, ended - 1)


async def plant() -> None:
    # resumed session: spend of a cut-short attempt comes back with the resume
    await insert_task("res", agent="implementer", cwd="/p/a", created_at=D, completed_at=D + 50)
    await attempt("res", 1, "first", D + 10, session_id="s1", cost_usd=None)
    await attempt("res", 2, "resume", D + 20, session_id="s1", cost_usd=0.7, tokens=70)
    # reset / fresh session after a crash: the first attempt's spend is lost
    await insert_task("fr", agent="lead", cwd="/p/b", kind="lifecycle", created_at=D + DAY,
                      completed_at=D + DAY + 40)  # fmt: skip
    await attempt("fr", 1, "first", D + DAY + 10, session_id="s1", cost_usd=None)
    await attempt("fr", 2, "fresh", D + DAY + 30, session_id="s2", cost_usd=0.4, tokens=40)
    # killed last attempt
    await insert_task("kl", agent="implementer", cwd="/p/b", status="failed", created_at=D + DAY,
                      completed_at=D + DAY + 20)  # fmt: skip
    await attempt("kl", 1, "first", D + DAY + 20, session_id="s1", cost_usd=0.1, tokens=10,
                  reason="timeout")  # fmt: skip
    # straddles the window edge used below (since = D + 2 days)
    await insert_task("edge", agent="lead", cwd="/p/a", created_at=D + DAY,
                      completed_at=D + 3 * DAY)  # fmt: skip
    await attempt("edge", 1, "first", D + DAY + 100, session_id="s1", cost_usd=1.0, tokens=1)
    await attempt("edge", 2, "resume", D + 3 * DAY, session_id="s1", cost_usd=2.0, tokens=2)


async def rows(env: Env, path: str) -> list[dict[str, Any]]:
    async with client_as(env, Access.reports) as client:
        response = await client.get(path)
    assert response.status_code == 200, response.text
    found: list[dict[str, Any]] = response.json()["rows"]
    return found


@pytest.mark.parametrize(
    ("route", "columns"),
    [("usage", ADDITIVE_USAGE), ("reliability", ADDITIVE_RELIABILITY)],
)
async def test_group_rows_sum_to_ungrouped(
    reports: Env, route: str, columns: tuple[str, ...]
) -> None:
    await plant()
    since = f"since={D - DAY}"
    (total,) = await rows(reports, f"/api/v1/metrics/{route}?{since}")
    for key in ("agent", "project", "kind"):
        grouped = await rows(reports, f"/api/v1/metrics/{route}?{since}&group_by={key}")
        assert len(grouped) == 2, key
        for column in columns:
            assert sum(r[column] or 0 for r in grouped) == pytest.approx(total[column] or 0), (
                key, column,
            )  # fmt: skip


async def test_attempts_identity(reports: Env) -> None:
    await plant()
    query = f"since={D - DAY}&bucket=day"
    usage = {r["bucket"]: r for r in await rows(reports, f"/api/v1/metrics/usage?{query}")}
    reliability = await rows(reports, f"/api/v1/metrics/reliability?{query}")
    modes: dict[str, int] = {}
    for task in ("res", "fr", "kl", "edge"):
        for a in await rows(reports, f"/api/v1/tasks/{task}/attempts"):
            modes[a["mode"]] = modes.get(a["mode"], 0) + 1
    firsts = 0
    for row in reliability:
        assert usage[row["bucket"]]["attempts"] == row["attempts"]
        firsts += row["attempts"] - row["resumes"] - row["fresh_restarts"]
    assert firsts == modes["first"]
    assert sum(r["resumes"] for r in reliability) == modes["resume"]
    assert sum(r["fresh_restarts"] for r in reliability) == modes["fresh"]


async def test_task_cost_equals_sum_of_attempt_rows(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        for task in ("res", "fr", "kl", "edge"):
            cost = (await client.get(f"/api/v1/tasks/{task}")).json()["cost_usd"]
            attempts = (await client.get(f"/api/v1/tasks/{task}/attempts")).json()["rows"]
            assert cost == pytest.approx(sum(a["cost_usd"] or 0 for a in attempts)), task


async def test_window_edge_counts_only_inside(reports: Env) -> None:
    await plant()
    (row,) = await rows(reports, f"/api/v1/metrics/usage?task=edge&since={D + 2 * DAY}")
    assert row["cost_usd"] == pytest.approx(2.0) and row["attempts"] == 1
    async with client_as(reports, Access.reports) as client:
        assert (await client.get("/api/v1/tasks/edge")).json()["cost_usd"] == pytest.approx(3.0)


async def test_uncounted_flags_reach_metrics(reports: Env) -> None:
    await plant()
    by_task = {}
    for task in ("res", "fr", "kl"):
        (row,) = await rows(reports, f"/api/v1/metrics/usage?task={task}&since={D - DAY}")
        by_task[task] = (row["uncounted_attempts"], row["cost_complete"])
    assert by_task == {"res": (0, True), "fr": (1, False), "kl": (1, False)}


async def test_model_rows_sum_to_ungrouped(reports: Env) -> None:
    """Two models in one attempt, the rest of the fixture without a breakdown (``model: null``)."""
    await plant()
    async with write_tx() as db:
        await write_models(db, "res", 2, {"A": ModelUsage(40, 0, 0, 0, 0.5),
                                          "B": ModelUsage(30, 0, 0, 0, 0.2)})  # fmt: skip
    window = f"since={D}&until={D + 5 * DAY}"
    (total,) = await rows(reports, f"/api/v1/metrics/usage?{window}")
    by_model = await rows(reports, f"/api/v1/metrics/usage?{window}&group_by=model")
    assert {r["model"] for r in by_model} == {"A", "B", None}
    assert sum(r["cost_usd"] or 0 for r in by_model) == pytest.approx(total["cost_usd"])
    assert sum(r["tokens"] or 0 for r in by_model) == total["tokens"]
    assert total["unattributed_tokens"] == total["tokens"] - 70


async def test_submit_calls_are_submitted_plus_deduplicated(reports: Env) -> None:
    for description in ("one", "two", "one"):
        await submit(reports, description)
    window = "since=0"
    (reliability,) = await rows(reports, f"/api/v1/metrics/reliability?{window}")
    (savings,) = await rows(reports, f"/api/v1/metrics/savings?{window}")
    assert reliability["submitted"] + savings["deduplicated_submissions"] == 3
