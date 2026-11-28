import time
from datetime import datetime

import httpx
from fastapi import FastAPI

from src.auth import Access
from src.database import write_tx
from src.mcp_server import ToolProvider
from src.model_usage import write_models
from src.reports.routes import install_reports
from src.stream import ModelUsage
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_step, insert_task

NOW = time.time()
DAY = 86400.0


async def plant() -> None:
    await insert_task(
        "t1",
        agent="implementer",
        cwd="/proj/a",
        created_at=NOW - 3 * DAY,
        started_at=NOW - 3 * DAY,
        completed_at=NOW - 2 * DAY,
    )
    await insert_task(
        "t2",
        agent="implementer",
        cwd="/proj/b",
        status="running",
        created_at=NOW - DAY,
        started_at=NOW - DAY,
    )
    await insert_task(
        "t3",
        agent="lead",
        kind="lifecycle",
        cwd="/proj/a",
        created_at=NOW - 40 * DAY,
        completed_at=NOW - 39 * DAY,
    )
    await insert_attempt(
        "t1",
        1,
        session_id="s1",
        started_at=NOW - 3 * DAY,
        ended_at=NOW - 3 * DAY + 10,
        outcome="transient",
        cost_usd=None,
    )
    await insert_attempt(
        "t1",
        2,
        session_id="s1",
        started_at=NOW - 3 * DAY + 20,
        ended_at=NOW - 2 * DAY,
        outcome="success",
        reason="clean_exit",
        cost_usd=1.5,
        tokens=100,
        running_s=50.0,
    )
    await insert_event("t1", "attempt_started", {"n": 1, "mode": "first"}, NOW - 3 * DAY)
    await insert_event("t1", "attempt_started", {"n": 2, "mode": "resume"}, NOW - 3 * DAY + 20)


async def get(env: Env, path: str, access: Access = Access.reports) -> httpx.Response:
    async with client_as(env, access) as client:
        return await client.get(path)


async def test_attempt_rows(reports: Env) -> None:
    await plant()
    response = await get(reports, "/api/v1/tasks/t1/attempts")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [(r["n"], r["mode"], r["uncounted"]) for r in body["rows"]] == [
        (1, "first", False),
        (2, "resume", False),
    ]
    second = body["rows"][1]
    assert (second["cost_usd"], second["tokens"], second["running_s"]) == (1.5, 100, 50.0)
    assert second["outcome"] == "success" and second["ended_at"].endswith("+00:00")
    assert (await get(reports, "/api/v1/tasks/nope/attempts")).status_code == 404


async def test_agent_rows(reports: Env) -> None:
    await plant()
    body = (await get(reports, "/api/v1/agents")).json()
    rows = {r["id"]: r for r in body["rows"]}
    assert set(rows) == {"planner", "implementer", "reviewer", "qa", "lead"}
    impl = rows["implementer"]
    assert (impl["status"], impl["running_tasks"], impl["tasks_total"]) == ("busy", 1, 2)
    assert impl["role"] == "implementer" and "description" not in impl
    assert rows["lead"]["tasks_total"] == 0  # its task is outside the default 30-day window
    lead_all = (await get(reports, f"/api/v1/agents?since={NOW - 50 * DAY}")).json()
    assert {r["id"]: r for r in lead_all["rows"]}["lead"]["tasks_total"] == 1
    one = (await get(reports, "/api/v1/agents/dev")).json()  # alias resolves
    assert one["id"] == "implementer"
    assert one["tasks_by_status"] == {"completed": 1, "running": 1}
    assert (await get(reports, "/api/v1/agents/nope")).status_code == 404


async def test_project_rows_and_paging(reports: Env) -> None:
    await plant()
    first = (await get(reports, "/api/v1/projects?limit=1")).json()
    (row,) = first["rows"]
    assert (row["project"], row["tasks_total"], row["running"]) == ("/proj/a", 1, 0)
    assert first["next_cursor"] is not None
    second = (await get(reports, f"/api/v1/projects?limit=1&cursor={first['next_cursor']}")).json()
    assert [(r["project"], r["running"]) for r in second["rows"]] == [("/proj/b", 1)]
    assert second["next_cursor"] is None
    only_lead = (await get(reports, f"/api/v1/projects?agent=lead&since={NOW - 50 * DAY}")).json()
    assert [(r["project"], r["tasks_total"]) for r in only_lead["rows"]] == [("/proj/a", 1)]
    assert (await get(reports, "/api/v1/projects?cursor=!!")).status_code == 400


async def test_detached_engine_is_503() -> None:
    app = FastAPI()
    install_reports(app, ToolProvider())
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 50000))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/api/v1/agents")
    assert response.status_code == 503
    assert response.json() == {"error": "engine not started"}


async def test_cursor_of_wrong_type_is_400(reports: Env) -> None:
    await plant()
    for path in ("/api/v1/tasks/t1/attempts?cursor=WyJhIl0=", "/api/v1/projects?cursor=WzFd"):
        response = await get(reports, path)
        assert response.status_code == 400, path
        assert response.json() == {"error": "invalid cursor"}


async def test_attempt_rows_carry_breakdown(reports: Env) -> None:
    await insert_task("b1")
    await insert_attempt("b1", 1, ended_at=1.0, cost_usd=1.0, tokens=10)
    await insert_attempt("b1", 2, ended_at=2.0, cost_usd=1.0, tokens=10)
    async with write_tx() as db:
        await write_models(db, "b1", 1, {"A": ModelUsage(1, 2, 3, 0, 0.5),
                                         "B": ModelUsage(1, 1, 1, 1, 0.5)})  # fmt: skip
    async with client_as(reports, Access.reports) as client:
        rows = (await client.get("/api/v1/tasks/b1/attempts")).json()["rows"]
    keys = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens")
    assert [rows[0][k] for k in keys] == [2, 3, 4, 1]
    assert [rows[1][k] for k in keys] == [None] * 4


async def plant_stages() -> None:
    """A lifecycle task submitted to ``lead`` whose stages ran on other agents."""
    await insert_task(
        "L1", agent="lead", kind="lifecycle", cwd="/proj/a", status="running",
        created_at=NOW - 2 * DAY, started_at=NOW - 2 * DAY,
    )  # fmt: skip
    await insert_step(
        "L1", "plan", status="completed", agent_type="planner",
        started_at=NOW - 2 * DAY, finished_at=NOW - 2 * DAY + 60,
    )  # fmt: skip
    await insert_step(
        "L1", "implement", status="running", agent_type="implementer",
        started_at=NOW - 60,
    )  # fmt: skip
    await insert_step("L1", "setup", status="completed", agent_type=None, started_at=NOW - 30)
    await insert_task(
        "L2", agent="lead", kind="lifecycle", cwd="/proj/b", status="failed",
        created_at=NOW - DAY, completed_at=NOW - DAY + 100,
    )  # fmt: skip
    await insert_step(
        "L2", "implement", status="running", agent_type="implementer",  # stale: task failed
        started_at=NOW - DAY,
    )  # fmt: skip


async def test_agent_rows_count_stage_work(reports: Env) -> None:
    await plant_stages()
    rows = {r["id"]: r for r in (await get(reports, "/api/v1/agents")).json()["rows"]}
    planner, impl, lead = rows["planner"], rows["implementer"], rows["lead"]
    # Stage-only activity: last_active_at is the latest stage timestamp; tasks_total unchanged.
    assert (planner["tasks_total"], planner["stages_total"], planner["stage_tasks_total"]) == (
        0, 1, 1,
    )  # fmt: skip
    assert planner["last_active_at"] is not None
    assert abs(_epoch(planner["last_active_at"]) - (NOW - 2 * DAY + 60)) < 1
    assert (planner["status"], planner["running_stages"]) == ("idle", 0)
    # Running stages count only status running of running tasks; busy from a stage alone.
    assert (impl["tasks_total"], impl["stages_total"], impl["stage_tasks_total"]) == (0, 2, 2)
    assert (impl["running_stages"], impl["running_tasks"], impl["status"]) == (1, 0, "busy")
    # The lifecycle agent: its own tasks, no stage rows (script stages are nobody's).
    assert (lead["tasks_total"], lead["stages_total"], lead["running_stages"]) == (2, 0, 0)


async def test_agent_last_active_is_later_of_task_and_stage(reports: Env) -> None:
    await plant()  # implementer: task t2 started NOW - DAY
    await plant_stages()  # implementer: stage started NOW - 60
    rows = {r["id"]: r for r in (await get(reports, "/api/v1/agents")).json()["rows"]}
    impl = rows["implementer"]
    assert impl["tasks_total"] == 2  # unchanged by stage rows
    assert abs(_epoch(impl["last_active_at"]) - (NOW - 60)) < 1


async def test_agent_filter_matches_stage_agent(reports: Env) -> None:
    await plant_stages()
    one = (await get(reports, "/api/v1/agents/implementer")).json()
    assert (one["stages_total"], one["running_stages"], one["status"]) == (2, 1, "busy")
    filtered = (await get(reports, "/api/v1/agents?agent=planner")).json()["rows"]
    by_id = {r["id"]: r for r in filtered}
    assert by_id["planner"]["stages_total"] == 1 and by_id["implementer"]["stages_total"] == 0


def _epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


async def test_agent_stage_figures_window_alias_and_script_stages(reports: Env) -> None:
    await plant_stages()
    await insert_task(
        "OLD", agent="lead", kind="lifecycle", status="completed",
        created_at=NOW - 40 * DAY, completed_at=NOW - 39 * DAY,
    )  # fmt: skip
    await insert_step(
        "OLD", "plan", status="completed", agent_type="planner",
        started_at=NOW - 40 * DAY, finished_at=NOW - 40 * DAY + 5,
    )  # fmt: skip
    rows = {r["id"]: r for r in (await get(reports, "/api/v1/agents")).json()["rows"]}
    assert rows["planner"]["stages_total"] == 1  # OLD's stage is outside the 30-day window
    wide = (await get(reports, f"/api/v1/agents?since={NOW - 50 * DAY}")).json()["rows"]
    assert {r["id"]: r for r in wide}["planner"]["stages_total"] == 2
    # The setup step (no agent, NOW - 30) is nobody's activity: lead's stays its task's start.
    assert abs(_epoch(rows["lead"]["last_active_at"]) - (NOW - DAY + 100)) < 1
    aliased = (await get(reports, "/api/v1/agents/dev")).json()  # dev -> implementer
    assert (aliased["id"], aliased["stages_total"]) == ("implementer", 2)
    by_alias = (await get(reports, "/api/v1/agents?agent=dev")).json()["rows"]
    assert {r["id"]: r for r in by_alias}["implementer"]["running_stages"] == 1
