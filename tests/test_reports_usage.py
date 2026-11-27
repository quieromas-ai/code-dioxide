import json
import time

import httpx
import pytest

from src.auth import Access
from src.database import write_tx
from src.model_usage import write_models
from src.stream import ModelUsage
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_task

DAY = 86400.0
D = float(int(time.time() // DAY) * DAY) - 5 * DAY  # a UTC midnight five days ago


async def get(env: Env, query: str) -> httpx.Response:
    async with client_as(env, Access.reports) as client:
        return await client.get(f"/api/v1/metrics/usage{query}")


async def plant() -> None:
    await insert_task("r", agent="implementer", created_at=D)
    await insert_attempt("r", 1, session_id="s1", ended_at=D + 10, cost_usd=1.0, tokens=10)
    await insert_attempt("r", 2, session_id="s1", ended_at=D + 20, cost_usd=0.5, tokens=5)
    await insert_task("k", agent="lead", cwd="/proj/b", created_at=D + DAY)
    await insert_attempt("k", 1, ended_at=D + DAY + 5, cost_usd=0.2, tokens=2, reason="stall")


async def test_usage_by_day(reports: Env) -> None:
    await plant()
    body = (await get(reports, "?bucket=day")).json()
    assert body["bucket"] == "day" and body["group_by"] == []
    assert set(body["window"]) == {"since", "until"}
    first, second = body["rows"]
    assert {k: first[k] for k in ("cost_usd", "tokens", "attempts", "tasks")} == {
        "cost_usd": 1.5, "tokens": 15, "attempts": 2, "tasks": 1,
    }  # fmt: skip
    assert (first["uncounted_attempts"], first["cost_complete"], first["partial"]) == (
        0,
        True,
        False,
    )
    assert second["cost_usd"] == pytest.approx(0.2)
    assert (second["uncounted_attempts"], second["cost_complete"]) == (1, False)


async def test_usage_group_by_and_filters(reports: Env) -> None:
    await plant()
    rows = (await get(reports, "?group_by=agent,project")).json()["rows"]
    assert [(r["agent"], r["project"], r["attempts"]) for r in rows] == [
        ("implementer", "/proj/a", 2), ("lead", "/proj/b", 1),
    ]  # fmt: skip
    (row,) = (await get(reports, "?agent=dev")).json()["rows"]  # alias of implementer
    assert row["cost_usd"] == 1.5
    response = await get(reports, "?group_by=bogus")
    assert (
        response.status_code == 400
        and "agent, model, project, kind, status" in response.json()["error"]
    )


def _result_line(cost: float, tokens: int) -> str:
    record = {"type": "result", "total_cost_usd": cost, "usage": {"input_tokens": tokens}}
    return json.dumps(record) + "\n"


async def test_open_attempt_partial_and_excluded_from_past_window(reports: Env) -> None:
    now = time.time()
    await insert_task("o", status="running", created_at=now - 100)
    await insert_attempt("o", 1, session_id="s1", started_at=now - 100, ended_at=now - 90,
                         cost_usd=0.1, tokens=1, reason="clean_exit")  # fmt: skip
    await insert_attempt("o", 2, session_id=None, started_at=now - 50)
    stream = reports.engine.runstore.attempt_paths("o", 2)[0]
    stream.parent.mkdir(parents=True, exist_ok=True)
    stream.write_text(_result_line(0.3, 7))
    rows = (await get(reports, "")).json()["rows"]
    assert len(rows) == 1 and rows[0]["partial"] is True
    assert rows[0]["cost_usd"] == pytest.approx(0.4) and rows[0]["attempts"] == 2
    async with client_as(reports, Access.reports) as client:
        task_cost = (await client.get("/api/v1/tasks/o")).json()["cost_usd"]
    assert task_cost == pytest.approx(rows[0]["cost_usd"])
    past = (await get(reports, f"?until={now - 60}")).json()["rows"]
    assert len(past) == 1 and past[0]["partial"] is False
    assert past[0]["cost_usd"] == pytest.approx(0.1)


async def test_usage_bad_bucket_is_400(reports: Env) -> None:
    assert (await get(reports, "?bucket=week")).status_code == 400


BREAKDOWN = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens")


async def plant_models() -> None:
    """``m``: attempt 1 split over models A and B; ``x``: one attempt with no breakdown."""
    await insert_task("m", agent="implementer", created_at=D)
    await insert_attempt("m", 1, ended_at=D + 10, cost_usd=1.5, tokens=150)
    async with write_tx() as db:
        await write_models(db, "m", 1, {"A": ModelUsage(10, 20, 30, 40, 1.0),
                                        "B": ModelUsage(5, 5, 20, 20, 0.5)})  # fmt: skip
    await insert_task("x", agent="lead", created_at=D)
    await insert_attempt("x", 1, ended_at=D + 20, cost_usd=0.2, tokens=9)


async def test_usage_breakdown_columns(reports: Env) -> None:
    await plant_models()
    rows = {r["agent"]: r for r in (await get(reports, "?group_by=agent")).json()["rows"]}
    m, x = rows["implementer"], rows["lead"]
    assert [m[c] for c in BREAKDOWN] == [15, 25, 50, 60] and m["unattributed_tokens"] == 0
    assert [x[c] for c in BREAKDOWN] == [None] * 4 and x["unattributed_tokens"] == 9


async def test_group_by_model_sums_to_total(reports: Env) -> None:
    await plant_models()
    (total,) = (await get(reports, "")).json()["rows"]
    rows = {r["model"]: r for r in (await get(reports, "?group_by=model")).json()["rows"]}
    assert set(rows) == {"A", "B", None}
    assert (rows["A"]["cost_usd"], rows["A"]["tokens"]) == (1.0, 100)
    assert (rows[None]["cost_usd"], rows[None]["tokens"]) == (0.2, 9)
    assert sum(r["cost_usd"] for r in rows.values()) == pytest.approx(total["cost_usd"])
    assert sum(r["tokens"] for r in rows.values()) == total["tokens"] == 159
    assert total["unattributed_tokens"] == 9


async def test_group_by_model_cost_residual_goes_to_null(reports: Env) -> None:
    await insert_task("r", created_at=D)
    await insert_attempt("r", 1, ended_at=D + 10, cost_usd=1.0, tokens=4)
    async with write_tx() as db:
        await write_models(db, "r", 1, {"A": ModelUsage(1, 1, 1, 1, 0.75)})
    rows = {r["model"]: r for r in (await get(reports, "?group_by=model")).json()["rows"]}
    assert rows["A"]["cost_usd"] == 0.75 and rows[None]["cost_usd"] == pytest.approx(0.25)
    assert rows[None]["tokens"] in (0, None)


async def test_group_by_model_open_attempt_partial(reports: Env) -> None:
    now = time.time()
    await insert_task("o", status="running", created_at=now - 100)
    await insert_attempt("o", 1, session_id="s1", started_at=now - 50)
    stream = reports.engine.runstore.attempt_paths("o", 1)[0]
    stream.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "type": "result", "total_cost_usd": 0.3, "session_id": "s1",
        "modelUsage": {"A": {"inputTokens": 7, "costUSD": 0.3}},
    }  # fmt: skip
    stream.write_text(json.dumps(record) + "\n")
    (row,) = (await get(reports, "?group_by=model")).json()["rows"]
    assert (row["model"], row["cost_usd"], row["input_tokens"], row["partial"]) == (
        "A", 0.3, 7, True,
    )  # fmt: skip
