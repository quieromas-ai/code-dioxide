"""stage_started / stage_finished: emitted on real step transitions only, public fields only."""

import json
from typing import Any

import pytest

from src import engine_lifecycle
from src.auth import Access
from src.database import write_tx
from src.journal import JournalStep
from src.reports.events import EVENT_FIELDS, public_data
from tests.helpers import Env, rows
from tests.reports_fixture import client_as, insert_event, insert_task
from tests.test_engine_lifecycle import lifecycle

WRITTEN = {"cached": False, "started_at": 10.0, "finished_at": None, "agent_type": "dx-impl",
           "model": "opus"}  # fmt: skip


def _step(status: str, started_count: int = 1) -> JournalStep:
    return JournalStep("k", "implement", "Implement", status, None, started_count, "a1")


def _row(status: str, started_count: int = 1) -> dict[str, Any]:
    return {"status": status, "started_count": started_count}


def test_new_running_step_started_once() -> None:
    (event,) = engine_lifecycle.stage_events(_step("running"), None, WRITTEN)
    assert event == ("stage_started", {
        "label": "implement", "agent_type": "dx-impl", "model": "opus", "started_at": 10.0,
    })  # fmt: skip
    # The next tick, nothing moved: no event.
    assert engine_lifecycle.stage_events(_step("running"), _row("running"), WRITTEN) == []


def test_completion_finishes_once_and_a_fast_step_starts_and_finishes() -> None:
    done = WRITTEN | {"finished_at": 20.0}
    (finished,) = engine_lifecycle.stage_events(_step("completed"), _row("running"), done)
    assert finished[0] == "stage_finished"
    assert finished[1] | {} == {
        "label": "implement", "agent_type": "dx-impl", "model": "opus", "status": "completed",
        "started_at": 10.0, "finished_at": 20.0,
    }  # fmt: skip
    assert engine_lifecycle.stage_events(_step("completed"), _row("completed"), done) == []
    both = engine_lifecycle.stage_events(_step("completed"), None, done)
    assert [t for t, _ in both] == ["stage_started", "stage_finished"]


def test_restart_starts_again_and_a_cached_replay_emits_none() -> None:
    restarted = engine_lifecycle.stage_events(_step("running", 2), _row("running", 1), WRITTEN)
    assert [t for t, _ in restarted] == ["stage_started"]
    cached = WRITTEN | {"cached": True}
    assert engine_lifecycle.stage_events(_step("completed"), None, cached) == []


async def test_mirror_writes_one_event_per_transition(env: Env) -> None:
    task_id = await lifecycle(env)
    step = _step("running")
    async with write_tx() as db:
        for _tick in range(2):  # two ticks, one transition
            row = {
                r["label"]: dict(r)
                for r in await rows("SELECT * FROM steps WHERE task_id = ?", task_id)
            }.get("implement")
            written = await engine_lifecycle._upsert(db, task_id, 1, step, row, [], (None, None))
            for type_, data in engine_lifecycle.stage_events(step, row, written):
                await env.engine._event(db, task_id, type_, data)
    found = await rows("SELECT type FROM events WHERE task_id = ? AND type LIKE 'stage_%'", task_id)
    assert [r["type"] for r in found] == ["stage_started"]


@pytest.mark.usefixtures("reports")
async def test_reports_scope_sees_stage_events_with_public_fields_only(reports: Env) -> None:
    await insert_task("t1")
    data = {"label": "implement", "agent_type": "dx", "model": "opus", "status": "completed",
            "started_at": 1.0, "finished_at": 2.0, "agent_id": "secret-agent"}  # fmt: skip
    await insert_event("t1", "stage_finished", data, 1e9)
    await insert_event("t1", "attempt_finished", {"n": 1, "outcome": "success",
                       "status": "completed", "cost_usd": 1.5, "tokens": 10}, 1e9 + 1)  # fmt: skip
    async with client_as(reports, Access.reports) as client:
        body = (await client.get("/api/v1/events?task=t1&since=0")).json()
    by_type = {e["type"]: e["data"] for e in body["rows"]}
    assert "agent_id" not in by_type["stage_finished"]
    assert by_type["stage_finished"]["label"] == "implement"
    assert by_type["attempt_finished"] | {} == {
        "n": 1, "outcome": "success", "status": "completed", "cost_usd": 1.5, "tokens": 10,
    }  # fmt: skip
    assert public_data("stage_started", {"label": "x", "prompt": "p"}) == {"label": "x"}
    assert EVENT_FIELDS["stage_started"] <= EVENT_FIELDS["stage_finished"]
    assert json.dumps(body).count("secret-agent") == 0


def test_restart_completed_within_one_tick_finishes_again() -> None:
    done = WRITTEN | {"finished_at": 30.0, "started_at": 5.0}  # the row's old start
    events = engine_lifecycle.stage_events(_step("completed", 2), _row("completed", 1), done, 25.0)
    assert [t for t, _ in events] == ["stage_started", "stage_finished"]
    assert all(d["started_at"] == 25.0 for _, d in events)  # the new run's start, not the old
    unread = engine_lifecycle.stage_events(_step("running", 2), _row("completed", 1), WRITTEN)
    assert unread == [("stage_started", {
        "label": "implement", "agent_type": "dx-impl", "model": "opus", "started_at": None,
    })]  # fmt: skip
