import pytest

from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_step, insert_task

pytestmark = pytest.mark.usefixtures("reports")

STEPS = [
    ("plan@a1", "Plan", "completed", 10.0, 20.0, "dx-planner", 100),
    ("implement@a1", "Implement", "failed", 30.0, 40.0, "dx-impl", 200),
    ("implement@a2", "Implement", "running", 50.0, None, "dx-impl", None),
    ("review-r1-x@a1", "Review", "completed", 60.0, 70.0, "dx-rev", 300),
    ("review-r1-y@a1", "Review", "completed", 61.0, 71.0, "dx-rev", 400),
]


async def plant(status: str = "running") -> None:
    await insert_task("w1", kind="lifecycle", agent="lead", status=status)
    for label, phase, step_status, started, finished, agent_type, tokens in STEPS:
        await insert_step(
            "w1", label, phase=phase, status=step_status, started_at=started,
            finished_at=finished, agent_type=agent_type, model="m", stage_tokens=tokens,
            tool_calls=3, last_tool="Bash", duration_s=1.5, started_count=1,
        )  # fmt: skip


async def test_stages_rows(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        body = (await client.get("/api/v1/tasks/w1/stages")).json()
    assert [r["label"] for r in body["rows"]] == [s[0] for s in STEPS]
    assert body["next_cursor"] is None
    review = body["rows"][3]
    assert review == {
        "label": "review-r1-x@a1", "stage": "review-x", "phase": "Review", "status": "completed",
        "stage_agent": "dx-rev", "model": "m", "started_count": 1, "cached": False,
        "started_at": "1970-01-01T00:01:00+00:00", "finished_at": "1970-01-01T00:01:10+00:00",
        "duration_s": 1.5, "stage_tokens": 300, "tool_calls": 3, "last_tool": "Bash",
    }  # fmt: skip
    assert body["rows"][2]["stage"] == "implement"


async def test_stage_running_while_task_queued_is_interrupted(reports: Env) -> None:
    await plant(status="queued")
    async with client_as(reports, Access.reports) as client:
        body = (await client.get("/api/v1/tasks/w1/stages")).json()
    assert body["rows"][2]["status"] == "interrupted"


async def test_stages_paged_by_cursor(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        first = (await client.get("/api/v1/tasks/w1/stages?limit=2")).json()
        rest = (
            await client.get(f"/api/v1/tasks/w1/stages?limit=10&cursor={first['next_cursor']}")
        ).json()
        bad = await client.get("/api/v1/tasks/w1/stages?cursor=garbage")
    assert [r["label"] for r in first["rows"] + rest["rows"]] == [s[0] for s in STEPS]
    assert bad.status_code == 400


async def test_unknown_task_is_404(reports: Env) -> None:
    async with client_as(reports, Access.reports) as client:
        assert (await client.get("/api/v1/tasks/nope/stages")).status_code == 404
        assert (await client.get("/api/v1/tasks/nope/graph")).status_code == 404


async def test_graph_nodes_and_edges(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        response = await client.get("/api/v1/tasks/w1/graph")
        bad = await client.get("/api/v1/tasks/w1/graph?limit=1")
    assert bad.status_code == 400
    body = response.json()
    assert [n["id"] for n in body["nodes"]] == [s[0] for s in STEPS]
    assert body["nodes"][0] == {
        "id": "plan@a1", "title": "plan", "subTitle": "dx-planner", "mainStat": "completed",
        "secondaryStat": "1.5s", "phase": "Plan",
    }  # fmt: skip
    edges = {(e["source"], e["target"], e["mainStat"]) for e in body["edges"]}
    assert edges == {
        ("plan@a1", "implement@a1", "next"), ("plan@a1", "implement@a2", "next"),
        ("implement@a1", "review-r1-x@a1", "next"), ("implement@a1", "review-r1-y@a1", "next"),
        ("implement@a2", "review-r1-x@a1", "next"), ("implement@a2", "review-r1-y@a1", "next"),
        ("implement@a1", "implement@a2", "retry"),
    }  # fmt: skip
    assert all(e["id"] == f"{e['source']}->{e['target']}" for e in body["edges"])
    assert len(body["edges"]) == len(edges)


async def test_graph_of_task_without_steps(reports: Env) -> None:
    await insert_task("s1")
    async with client_as(reports, Access.reports) as client:
        graph = (await client.get("/api/v1/tasks/s1/graph")).json()
        stages = (await client.get("/api/v1/tasks/s1/stages")).json()
    assert graph == {"nodes": [], "edges": []}
    assert stages == {"rows": [], "next_cursor": None}


async def test_graph_node_without_numbers(reports: Env) -> None:
    await insert_task("w2", kind="lifecycle")
    await insert_step("w2", "setup", phase=None, status="running", started_at=1.0)
    async with client_as(reports, Access.reports) as client:
        (node,) = (await client.get("/api/v1/tasks/w2/graph")).json()["nodes"]
    assert (node["subTitle"], node["secondaryStat"], node["mainStat"]) == ("", "", "interrupted")


async def test_graph_of_review_rounds_follows_each_round(reports: Env) -> None:
    await insert_task("w3", kind="lifecycle")
    for label, phase, started in [
        ("implement", "Implement", 1.0), ("review-r1-x", "Review", 2.0), ("fix-r1", "Fix", 3.0),
        ("review-r2-x", "Review", 4.0),
    ]:  # fmt: skip
        await insert_step("w3", label, phase=phase, status="completed", started_at=started)
    async with client_as(reports, Access.reports) as client:
        edges = (await client.get("/api/v1/tasks/w3/graph")).json()["edges"]
    assert {(e["source"], e["target"]) for e in edges} == {
        ("implement", "review-r1-x"), ("review-r1-x", "fix-r1"), ("fix-r1", "review-r2-x"),
    }  # fmt: skip


async def test_graph_retry_edge_from_unsuffixed_first_run(reports: Env) -> None:
    """Real labels: a step's first run has no suffix; a re-run after answer 1 is ``X@a1``."""
    await insert_task("w4", kind="lifecycle")
    await insert_step("w4", "plan", phase="Plan", status="completed", started_at=1.0)
    await insert_step("w4", "plan@a1", phase="Plan", status="completed", started_at=2.0)
    async with client_as(reports, Access.reports) as client:
        edges = (await client.get("/api/v1/tasks/w4/graph")).json()["edges"]
    assert [(e["source"], e["target"], e["mainStat"]) for e in edges] == [
        ("plan", "plan@a1", "retry")
    ]
