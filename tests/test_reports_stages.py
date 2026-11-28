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
        "billed_input_tokens": None, "billed_output_tokens": None,
        "billed_cache_read_tokens": None, "billed_cache_write_tokens": None, "turns": None,
        "estimated_cost_usd": None, "cost_basis": None,
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
    # Waves: [plan@a1], [implement@a1], [implement@a2 (running: open-ended), both reviews]; the
    # retry edge replaces the next edge between the two implement runs.
    assert edges == {
        ("plan@a1", "implement@a1", "next"),
        ("implement@a1", "review-r1-x@a1", "next"), ("implement@a1", "review-r1-y@a1", "next"),
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


async def _graph_edges(
    reports: Env,
    task_id: str,
    steps: list[tuple[str, float | None, float | None, str]],
    status: str = "completed",
) -> set[tuple[str, str, str]]:
    await insert_task(task_id, kind="lifecycle", status=status)
    for label, started, finished, step_status in steps:
        await insert_step(
            task_id, label, phase="P", status=step_status, started_at=started,
            finished_at=finished,
        )  # fmt: skip
    async with client_as(reports, Access.reports) as client:
        body = (await client.get(f"/api/v1/tasks/{task_id}/graph")).json()
    ids = [e["id"] for e in body["edges"]]
    assert len(ids) == len(set(ids)), ids  # Grafana needs unique edge ids
    return {(e["source"], e["target"], e["mainStat"]) for e in body["edges"]}


async def test_graph_parallel_reviews_share_a_wave(reports: Env) -> None:
    edges = await _graph_edges(reports, "g1", [
        ("implement", 1.0, 10.0, "completed"),
        ("review-r1-a", 11.0, 20.0, "completed"), ("review-r1-b", 12.0, 18.0, "completed"),
        ("qa-r1", 21.0, 30.0, "completed"),
    ])  # fmt: skip
    assert edges == {
        ("implement", "review-r1-a", "next"), ("implement", "review-r1-b", "next"),
        ("review-r1-a", "qa-r1", "next"), ("review-r1-b", "qa-r1", "next"),
    }  # fmt: skip


async def test_graph_sequential_same_phase_and_touching_are_waves(reports: Env) -> None:
    edges = await _graph_edges(reports, "g2", [
        ("setup", 1.0, 5.0, "completed"), ("implement", 5.0, 9.0, "completed"),  # touching
        ("deploy", 9.5, 12.0, "completed"),
    ])  # fmt: skip
    assert edges == {("setup", "implement", "next"), ("implement", "deploy", "next")}


async def test_graph_running_step_absorbs_later_starts(reports: Env) -> None:
    edges = await _graph_edges(reports, "g3", [
        ("implement", 1.0, 5.0, "completed"), ("review-r1-a", 6.0, None, "running"),
        ("review-r1-b", 100.0, 110.0, "completed"),
    ], status="running")  # fmt: skip
    assert edges == {
        ("implement", "review-r1-a", "next"), ("implement", "review-r1-b", "next"),
    }  # fmt: skip
    # The same step interrupted (task not running) spans only its start.
    edges = await _graph_edges(reports, "g4", [
        ("implement", 1.0, 5.0, "completed"), ("review-r1-a", 6.0, None, "running"),
        ("review-r1-b", 100.0, 110.0, "completed"),
    ], status="failed")  # fmt: skip
    assert ("review-r1-a", "review-r1-b", "next") in edges


async def test_graph_gate_edges_replace_next(reports: Env) -> None:
    edges = await _graph_edges(reports, "g5", [
        ("plan", 1.0, 5.0, "completed"), ("interpret-plan-1", 10.0, 11.0, "completed"),
        ("interpret-plan-2", 20.0, 21.0, "completed"), ("plan@a2", 22.0, 30.0, "completed"),
        ("interpret-plan@a2-3", 40.0, 41.0, "completed"),
    ])  # fmt: skip
    assert edges == {
        ("plan", "interpret-plan-1", "gate"), ("plan", "interpret-plan-2", "gate"),
        ("interpret-plan-1", "interpret-plan-2", "next"),
        ("interpret-plan-2", "plan@a2", "next"),
        ("plan@a2", "interpret-plan@a2-3", "gate"),
    }  # fmt: skip


async def test_graph_step_without_start_is_a_last_wave(reports: Env) -> None:
    edges = await _graph_edges(reports, "g6", [
        ("plan", 1.0, 5.0, "completed"), ("implement", None, None, "pending"),
    ])  # fmt: skip
    assert edges == {("plan", "implement", "next")}


async def test_graph_interrupted_parallel_steps_stay_one_wave(reports: Env) -> None:
    await insert_task("g7", kind="lifecycle", status="failed")
    await insert_step("g7", "implement", phase="P", status="completed", started_at=1.0,
                      finished_at=5.0)  # fmt: skip
    for label, started in (("review-r1-a", 11.0), ("review-r1-b", 12.0), ("review-r1-c", 13.0)):
        await insert_step("g7", label, phase="P", status="running", started_at=started,
                          last_progress_at=30.0)  # fmt: skip
    await insert_step("g7", "review-r1-d", phase="P", status="running", started_at=40.0)
    async with client_as(reports, Access.reports) as client:
        body = (await client.get("/api/v1/tasks/g7/graph")).json()
    edges = {(e["source"], e["target"]) for e in body["edges"]}
    assert edges == {
        ("implement", "review-r1-a"), ("implement", "review-r1-b"), ("implement", "review-r1-c"),
        ("review-r1-a", "review-r1-d"), ("review-r1-b", "review-r1-d"),
        ("review-r1-c", "review-r1-d"),
    }  # fmt: skip


async def test_graph_review_round_gate_links_every_reviewer(reports: Env) -> None:
    edges = await _graph_edges(reports, "g8", [
        ("review-r1-a", 1.0, 5.0, "completed"), ("review-r1-b", 1.5, 6.0, "completed"),
        ("interpret-review-r1-1", 10.0, 11.0, "completed"),
        ("review-r2-a@a1", 12.0, 15.0, "completed"),
        ("interpret-review-r2@a1-2", 20.0, 21.0, "completed"),
    ])  # fmt: skip
    gates = {(s, t) for s, t, kind in edges if kind == "gate"}
    assert gates == {
        ("review-r1-a", "interpret-review-r1-1"), ("review-r1-b", "interpret-review-r1-1"),
        ("review-r2-a@a1", "interpret-review-r2@a1-2"),
    }  # fmt: skip


async def test_graph_start_tie_broken_by_label(reports: Env) -> None:
    edges = await _graph_edges(reports, "g9", [
        ("b-step", 1.0, 2.0, "completed"), ("a-step", 1.0, 1.0, "completed"),
        ("c-step", 3.0, 4.0, "completed"),
    ])  # fmt: skip
    # a-step (zero length, first by label) is its own wave; b-step starts at its end: a new wave.
    assert edges == {("a-step", "b-step", "next"), ("b-step", "c-step", "next")}
