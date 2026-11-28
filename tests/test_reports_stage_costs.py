"""Per-stage billed usage and estimated cost on the reporting routes; synthetic data with the
shape of a measured lifecycle run (three attempts, three models)."""

import json
from typing import Any

import httpx
import pytest

from src.auth import Access
from src.database import write_tx
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_step, insert_task
from tests.test_reports_content import plant_lifecycle

pytestmark = pytest.mark.usefixtures("reports")
T0 = 1_000_000.0
# (attempt n, model, input, output, cache read, cache write, cost): the run's attempt_models.
ATTEMPT_MODELS = [
    (1, "claude-opus-5-5", 34, 8439, 506409, 72212, 0.7092008),
    (3, "claude-haiku-5-5", 10, 1693, 69695, 48554, 0.0076137),
    (3, "claude-sonnet-5-5", 16, 3609, 302712, 49639, 0.1904907),
]
# (label, agent, attempt n, model, input, output, cache read, 5m writes, turns)
STAGES = [
    ("plan", "abb", 1, "claude-opus-5-5", 26, 7820, 428316, 46231, 13),
    ("setup@a2", "a02", 3, "claude-haiku-5-5", 4, 397, 22614, 23053, 2),
    ("merge@a2", "a84", 3, "claude-haiku-5-5", 6, 1296, 47081, 25501, 3),
    ("qa-r1@a2", "a06", 3, "claude-sonnet-5-5", 16, 3609, 302712, 49639, 8),
]


async def plant() -> None:
    await insert_task("d12", kind="lifecycle", agent="lead", status="completed", created_at=T0,
                      completed_at=T0 + 900, run_id="wf")  # fmt: skip
    for n in (1, 2, 3):
        await insert_attempt("d12", n, started_at=T0 + n, ended_at=T0 + 100 * n, cost_usd=1.0)
    async with write_tx() as db:
        for n, model, i, o, r, w, cost in ATTEMPT_MODELS:
            await db.execute(
                "INSERT INTO attempt_models (task_id, n, model, input_tokens, output_tokens, "
                "cache_read_tokens, cache_write_tokens, cost_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("d12", n, model, i, o, r, w, cost),
            )
        for label, agent, n, model, i, o, r, w5, turns in STAGES:
            await db.execute(
                "INSERT INTO step_models (task_id, label, agent_id, run_id, attempt_n, model, "
                "input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, "
                "cache_write_1h_tokens, turns) VALUES (?, ?, ?, 'wf', ?, ?, ?, ?, ?, ?, 0, ?)",
                ("d12", label, agent, n, model, i, o, r, w5, turns),
            )
    for k, (label, _agent, n, _model, i, o, r, w5, turns) in enumerate(STAGES):
        await insert_step(
            "d12", label, status="completed", agent_type="dx", model="m", started_at=T0 + 10 * k,
            finished_at=T0 + 10 * k + 5, billed_input_tokens=i, billed_output_tokens=o,
            billed_cache_read_tokens=r, billed_cache_write_tokens=w5, turns=turns,
            files_changed_json=json.dumps([{"status": "M", "path": "a.py"}]) if k == 0 else None,
        )  # fmt: skip
    # A stage of attempt 2, whose attempt has no per-model breakdown: its cost is unknown.
    await insert_step("d12", "interpret-plan-1", status="completed", agent_type=None,
                      started_at=T0 + 200, finished_at=T0 + 201)  # fmt: skip
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO step_models VALUES ('d12', 'interpret-plan-1', 'ae9', 'wf', 2, "
            "'claude-opus-5-5', 4, 362, 29683, 30060, 0, 2)"
        )


async def get(env: Env, path: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(path)


async def rows_by(env: Env, path: str, key: str) -> dict[Any, dict[str, Any]]:
    return {r[key]: r for r in (await get(env, path)).json()["rows"]}


WINDOW = f"since={T0 - 1}&until={T0 + 1000}"


async def test_stage_rows_carry_billed_usage_and_estimate(reports: Env) -> None:
    await plant()
    rows = {r["label"]: r for r in (await get(reports, "/api/v1/tasks/d12/stages")).json()["rows"]}
    plan, qa = rows["plan"], rows["qa-r1@a2"]
    assert (plan["billed_output_tokens"], plan["turns"]) == (7820, 13)
    assert plan["cost_basis"] == "allocated" and 0 < plan["estimated_cost_usd"] < 0.7092008
    assert (qa["cost_basis"], qa["estimated_cost_usd"]) == ("exact", 0.1904907)
    assert {rows[x]["cost_basis"] for x in ("setup@a2", "merge@a2")} == {"allocated"}
    assert rows["interpret-plan-1"]["estimated_cost_usd"] is None
    assert rows["interpret-plan-1"]["cost_basis"] is None
    assert "cost_usd" not in plan  # only exact figures are called cost_usd


async def test_reconciliation_per_model_and_overhead(reports: Env) -> None:
    """Stage sums + parent overhead = the attempt's modelUsage, tokens and cost, per model."""
    await plant()
    stages = (await get(reports, "/api/v1/tasks/d12/stages")).json()["rows"]
    attempts = await rows_by(reports, "/api/v1/tasks/d12/attempts", "n")
    by_label = {r["label"]: r for r in stages}
    cost = {n: sum(a[6] for a in ATTEMPT_MODELS if a[0] == n) for n in (1, 3)}
    for n in (1, 3):
        labels = [s[0] for s in STAGES if s[2] == n]
        staged = sum(by_label[label]["estimated_cost_usd"] for label in labels)
        assert staged + attempts[n]["parent_overhead_estimated_cost_usd"] == pytest.approx(
            cost[n], abs=1e-6
        )
    tokens_1 = sum(ATTEMPT_MODELS[0][2:6])
    plan_tokens = sum(STAGES[0][4:8])
    assert attempts[1]["parent_overhead_tokens"] == tokens_1 - plan_tokens
    assert attempts[3]["parent_overhead_tokens"] == 0  # haiku and sonnet: stages only
    assert attempts[2]["parent_overhead_tokens"] is None  # no breakdown for attempt 2
    assert attempts[2]["parent_overhead_estimated_cost_usd"] is None


async def test_metrics_stages_sums_and_basis(reports: Env) -> None:
    await plant()
    rows = await rows_by(reports, f"/api/v1/metrics/stages?{WINDOW}&group_by=stage", "stage")
    assert rows["qa"]["cost_basis"] == "exact" and rows["qa"]["estimated_cost_usd"] == 0.1904907
    assert rows["qa"]["billed_output_tokens"] == 3609 and rows["qa"]["turns"] == 8
    assert rows["plan"]["cost_basis"] == "allocated"
    assert rows["interpret-plan-1"]["cost_basis"] is None
    assert rows["interpret-plan-1"]["estimated_cost_usd"] is None
    total = (await get(reports, f"/api/v1/metrics/stages?{WINDOW}")).json()["rows"]
    assert total[0]["cost_basis"] is None  # one unknown stage makes the sum unknown


async def test_cached_replay_keeps_measured_numbers(reports: Env) -> None:
    """A step replayed from cache (one row per label, flipped to cached) keeps its measured
    run's billed numbers and cost."""
    await plant()
    async with write_tx() as db:
        await db.execute("UPDATE steps SET cached = 1 WHERE task_id = 'd12' AND label = 'plan'")
    rows = await rows_by(reports, f"/api/v1/metrics/stages?{WINDOW}&group_by=stage", "stage")
    plan: dict[str, Any] = rows["plan"]
    assert (plan["cached_runs"], plan["billed_output_tokens"]) == (1, 7820)
    assert plan["cost_basis"] == "allocated" and plan["estimated_cost_usd"] > 0


async def test_fresh_start_run_gets_no_old_run_estimate(reports: Env) -> None:
    """After a fresh start the old run's step usage stays (its attempt's split) but is not the
    current steps' cost."""
    await plant()
    async with write_tx() as db:
        await db.execute("UPDATE tasks SET run_id = 'wf_new' WHERE id = 'd12'")
    rows = await rows_by(reports, "/api/v1/tasks/d12/stages", "label")
    assert all(r["estimated_cost_usd"] is None for r in rows.values())
    attempts = await rows_by(reports, "/api/v1/tasks/d12/attempts", "n")
    assert attempts[1]["parent_overhead_tokens"] == sum(ATTEMPT_MODELS[0][2:6]) - sum(
        STAGES[0][4:8]
    )
    # The old run's stage shares stay with their attempt: overhead = the attempt's whole cost.
    assert attempts[1]["parent_overhead_estimated_cost_usd"] == pytest.approx(0.7092008, abs=1e-9)
    assert attempts[3]["parent_overhead_estimated_cost_usd"] == pytest.approx(
        0.0076137 + 0.1904907, abs=1e-9
    )


async def test_stage_result_serves_files_changed(reports: Env) -> None:
    await plant_lifecycle(reports)
    async with write_tx() as db:
        await db.execute(
            "UPDATE steps SET files_changed_json = ? WHERE task_id = 'w1' "
            "AND label = 'implement@a1'",
            (json.dumps([{"status": "M", "path": "a.py"}]),),
        )
    async with client_as(reports, Access.content) as client:
        result = (await client.get("/api/v1/tasks/w1/stages/implement@a1/result")).json()
    assert result["files_changed"] == [{"status": "M", "path": "a.py"}]
    assert result["result"]["ok"] is True  # the stage's own result is untouched
