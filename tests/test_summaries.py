import asyncio
import json
import time
from typing import Any

import pytest

from src import summaries
from src.auth import Access
from src.config import Settings
from src.database import write_tx
from src.reports.entities import NotFound
from src.stream import Result
from tests.helpers import Env, rows, stub_log, wait_for
from tests.reports_fixture import (
    client_as,
    insert_attempt,
    insert_event,
    insert_step,
    insert_task,
)
from tests.test_debug import SK

pytestmark = pytest.mark.usefixtures("reports", "spawned")
DAY = 86400.0
D = float(int(time.time() // DAY) * DAY) - 5 * DAY


def done(text: str = "It went fine.", cost: float = 0.01) -> Result:
    return Result(
        is_error=False, subtype="success", api_error_status=None, api_error=None, text=text,
        cost_usd=cost, tokens=50, session_id=None, errors=[], permission_denials=[],
        result_index=0,
    )  # fmt: skip


def on(env: Env, **overrides: Any) -> None:
    env.settings.summaries = True
    for key, value in overrides.items():
        setattr(env.settings, key, value)


def test_settings_defaults(tmp_path: Any) -> None:
    settings = Settings(home=tmp_path, auth_token="t")
    assert (settings.summaries, settings.summary_model, settings.summary_min_interval_s) == (
        False, "haiku", 300.0,
    )  # fmt: skip


async def plant_lifecycle(env: Env) -> None:
    await insert_task("w1", kind="lifecycle", status="running", updated_at=100.0)
    await insert_step("w1", "plan", phase="Plan", status="completed", last_tool="Read")
    await insert_step("w1", "implement@a1", phase="Implement", status="running", last_tool="Edit")


async def test_summary_input_lists_stages_and_tools(reports: Env) -> None:
    await plant_lifecycle(reports)
    text = await summaries.summary_input(reports.engine, "w1")
    assert "plan" in text and "implement@a1" in text and "Edit" in text and "running" in text
    assert len(text) <= summaries.SUMMARY_INPUT_CHARS


async def test_summary_input_redacted_and_capped(reports: Env) -> None:
    await insert_task("s1", status="completed", output_json=json.dumps(
        {"text": f"result {SK} " + "x" * 50_000}
    ))  # fmt: skip
    text = await summaries.summary_input(reports.engine, "s1")
    assert SK not in text and len(text) <= summaries.SUMMARY_INPUT_CHARS


async def test_latest_progress(reports: Env) -> None:
    await plant_lifecycle(reports)
    assert await summaries.latest_progress("w1") == 100.0
    await insert_event("w1", "submitted", {"agent": "a"}, 150.0)
    await insert_step("w1", "qa", status="running", last_progress_at=170.0)
    assert await summaries.latest_progress("w1") == 170.0


async def test_off_and_unknown(reports: Env) -> None:
    with pytest.raises(summaries.SummariesOff):
        await summaries.get_summary(reports.engine, "w1")
    on(reports)
    with pytest.raises(NotFound):
        await summaries.get_summary(reports.engine, "nope")


async def test_summary_generated_by_claude_and_stored(reports: Env) -> None:
    on(reports)
    await plant_lifecycle(reports)
    body = await summaries.get_summary(reports.engine, "w1")
    assert body["summary"] == "Done." and body["cached"] is False
    assert body["cost_usd"] == 0.0042 and body["basis_at"] == "1970-01-01T00:01:40+00:00"
    (launch,) = [e for e in stub_log(reports.log) if "argv" in e]
    argv = launch["argv"]
    assert argv[argv.index("--model") + 1] == "haiku"
    assert launch["env"]["DIOXIDE_TASK_ID"] is None and launch["dioxide_vars"] == []
    # Isolated from the user's Claude config: no tools, MCP servers, settings or saved session.
    assert argv[argv.index("--tools") + 1] == "" and "--strict-mcp-config" in argv
    assert "--no-session-persistence" in argv
    assert argv[argv.index("--setting-sources") + 1] == ""
    (run,) = await rows("SELECT task_id, cost_usd FROM summary_runs")
    assert (run["task_id"], run["cost_usd"]) == ("w1", 0.0042)
    assert reports.engine.summaries_running == 0


async def test_concurrent_requests_share_one_run(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports)
    await plant_lifecycle(reports)
    calls: list[str] = []

    async def fake(engine: Any, prompt: str) -> Result:
        calls.append(prompt)
        await asyncio.sleep(0.1)
        return done()

    monkeypatch.setattr(summaries, "run_claude", fake)
    first, second = await asyncio.gather(
        summaries.get_summary(reports.engine, "w1"), summaries.get_summary(reports.engine, "w1")
    )
    assert len(calls) == 1 and first["summary"] == second["summary"] == "It went fine."


async def test_no_free_slot_is_refused(reports: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    on(reports, max_concurrent_tasks=1)
    await insert_task("busy", status="running", attempt=1)
    await insert_attempt("busy", 1, started_at=1.0)  # the one slot: busy's open attempt
    await plant_lifecycle(reports)
    monkeypatch.setattr(summaries, "run_claude", lambda *a: pytest.fail("must not run"))
    with pytest.raises(summaries.NoSlot) as raised:
        await summaries.get_summary(reports.engine, "w1")
    assert raised.value.retry_after == 30


async def test_summary_slot_counts_for_the_scheduler(reports: Env) -> None:
    on(reports, max_concurrent_tasks=2)
    assert await reports.engine.free_slots() == 2
    reports.engine.summaries_running = 1
    assert await reports.engine.free_slots() == 1


async def test_freshness_rules(reports: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    on(reports, summary_min_interval_s=60.0)
    await plant_lifecycle(reports)
    clock = [1000.0]
    monkeypatch.setattr(summaries, "_now", lambda: clock[0])
    texts = iter(["one", "two"])

    async def fake(engine: Any, prompt: str) -> Result:
        return done(next(texts))

    monkeypatch.setattr(summaries, "run_claude", fake)
    assert (await summaries.get_summary(reports.engine, "w1"))["summary"] == "one"
    assert (await summaries.get_summary(reports.engine, "w1"))["cached"] is True  # no progress
    await insert_event("w1", "submitted", {"agent": "a"}, 500.0)  # newer than basis (100)
    clock[0] = 1030.0  # inside the interval: still cached
    again = await summaries.get_summary(reports.engine, "w1")
    assert (again["summary"], again["cached"]) == ("one", True)
    clock[0] = 1100.0  # past the interval, progress newer: regenerated
    assert (await summaries.get_summary(reports.engine, "w1"))["summary"] == "two"


async def test_failed_run_is_an_error_and_stores_nothing(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports)
    await plant_lifecycle(reports)

    async def fake(engine: Any, prompt: str) -> Result | None:
        return None

    monkeypatch.setattr(summaries, "run_claude", fake)
    with pytest.raises(summaries.SummaryFailed):
        await summaries.get_summary(reports.engine, "w1")
    assert await rows("SELECT * FROM summaries") == []
    assert reports.engine.summaries_running == 0


async def test_summary_input_redacted(reports: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    on(reports)
    await plant_lifecycle(reports)
    await insert_task("s2", status="completed", output_json=json.dumps({"text": f"key {SK}"}))
    seen: list[str] = []

    async def fake(engine: Any, prompt: str) -> Result:
        seen.append(prompt)
        return done(f"leaked {SK}")

    monkeypatch.setattr(summaries, "run_claude", fake)
    body = await summaries.get_summary(reports.engine, "s2")
    assert SK not in seen[0] and SK not in body["summary"]


# -- routes ------------------------------------------------------------------------------------
async def test_summary_route_statuses(reports: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await plant_lifecycle(reports)
    async with client_as(reports, Access.content) as client:
        off = await client.get("/api/v1/tasks/w1/summary")
        on(reports)

        async def fake(engine: Any, prompt: str) -> Result:
            return done()

        monkeypatch.setattr(summaries, "run_claude", fake)
        ok = await client.get("/api/v1/tasks/w1/summary")
        missing = await client.get("/api/v1/tasks/nope/summary")
        bad = await client.get("/api/v1/tasks/w1/summary?x=1")
    assert off.status_code == 404 and "DIOXIDE_SUMMARIES" in off.json()["error"]
    assert ok.status_code == 200 and ok.json()["summary"] == "It went fine."
    assert missing.status_code == 404 and bad.status_code == 400


async def test_summary_route_busy_and_failed(reports: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    on(reports)
    await plant_lifecycle(reports)

    async def no_slot() -> int:
        return 0

    monkeypatch.setattr(reports.engine, "free_slots", no_slot)
    async with client_as(reports, Access.content) as client:
        busy = await client.get("/api/v1/tasks/w1/summary")
        monkeypatch.undo()
        on(reports)

        async def failing(engine: Any, prompt: str) -> None:
            return None

        monkeypatch.setattr(summaries, "run_claude", failing)
        failed = await client.get("/api/v1/tasks/w1/summary")
    assert busy.status_code == 503 and busy.headers["retry-after"] == "30"
    assert busy.json() == {"error": "no free slot", "retry_after": 30}
    assert failed.status_code == 502 and failed.json() == {"error": "summary failed"}


async def plant_runs() -> None:
    async with write_tx() as db:
        for task_id, at, cost, tokens in (("w1", D + 10, 0.01, 50), ("w1", D + DAY, 0.02, 70),
                                          (None, D + DAY + 5, 0.03, 10)):  # fmt: skip
            await db.execute(
                "INSERT INTO summary_runs (task_id, at, cost_usd, tokens) VALUES (?, ?, ?, ?)",
                (task_id, at, cost, tokens),
            )


async def test_overhead_metrics(reports: Env) -> None:
    await plant_lifecycle(reports)
    await plant_runs()
    async with client_as(reports, Access.reports) as client:
        (total,) = (await client.get("/api/v1/metrics/overhead")).json()["rows"]
        days = (await client.get("/api/v1/metrics/overhead?bucket=day")).json()["rows"]
        one = (await client.get("/api/v1/metrics/overhead?task=w1")).json()["rows"]
        bad = await client.get("/api/v1/metrics/overhead?group_by=agent")
    assert (total["summaries"], total["cost_usd"], total["tokens"]) == (3, pytest.approx(0.06), 130)
    assert [d["summaries"] for d in days] == [1, 2]
    assert one[0]["summaries"] == 2 and bad.status_code == 400


async def test_overhead_not_in_task_cost(reports: Env) -> None:
    await insert_task("w1", created_at=D)
    await insert_attempt("w1", 1, ended_at=D + 5, cost_usd=1.0, tokens=10)
    await plant_runs()
    async with client_as(reports, Access.reports) as client:
        task = (await client.get("/api/v1/tasks/w1")).json()
        (usage,) = (await client.get("/api/v1/metrics/usage")).json()["rows"]
        (overhead,) = (await client.get("/api/v1/metrics/overhead")).json()["rows"]
    assert task["cost_usd"] == 1.0 and usage["cost_usd"] == 1.0
    total_spend = usage["cost_usd"] + overhead["cost_usd"]  # spec: total dioxide spend
    assert total_spend == pytest.approx(1.06)


# -- review fixes ------------------------------------------------------------------------------
async def test_summary_and_scheduler_never_share_the_last_slot(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports, max_concurrent_tasks=1)
    await insert_task("q1", status="queued", created_at=1.0)
    await plant_lifecycle(reports)
    engine = reports.engine
    real = engine.free_slots

    async def slow() -> int:
        free = await real()
        await asyncio.sleep(0.05)  # both deciders would see the same free slot
        return free

    monkeypatch.setattr(engine, "free_slots", slow)

    async def fake(engine: Any, prompt: str) -> Result:
        await asyncio.sleep(0.2)
        return done()

    monkeypatch.setattr(summaries, "run_claude", fake)

    async def summary() -> str:
        try:
            await summaries.get_summary(engine, "w1")
            return "ran"
        except summaries.NoSlot:
            return "refused"

    started: list[str] = []

    def start_job(task_id: str, work: Any) -> None:
        started.append(task_id)
        work.close()  # the launch itself is not under test

    monkeypatch.setattr(engine, "_start_job", start_job)
    _, outcome = await asyncio.gather(engine._schedule_once(), summary())
    assert (started == ["q1"]) != (outcome == "ran")  # exactly one of them got the slot


async def test_secret_cut_by_the_cap_is_redacted_first(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports)
    secret = "plainsecretvalue123456"  # a child-env value: matched as a whole value only
    reports.engine.redactor.add_values([secret])
    filler = "x" * (summaries.RESULT_CHARS - 10)
    await insert_task("s3", status="completed", output_json=json.dumps({"text": filler + secret}))
    text = await summaries.summary_input(reports.engine, "s3")
    assert secret[:10] not in text


async def test_failed_run_spend_counted_and_cooled_down(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports, summary_min_interval_s=60.0)
    await plant_lifecycle(reports)
    clock = [1000.0]
    monkeypatch.setattr(summaries, "_now", lambda: clock[0])
    calls: list[str] = []

    async def errored(engine: Any, prompt: str) -> Result:
        calls.append(prompt)
        return Result(
            is_error=True, subtype="error_during_execution", api_error_status=None,
            api_error=None, text=None, cost_usd=0.02, tokens=7, session_id=None, errors=[],
            permission_denials=[], result_index=0,
        )  # fmt: skip

    monkeypatch.setattr(summaries, "run_claude", errored)
    with pytest.raises(summaries.SummaryFailed):
        await summaries.get_summary(reports.engine, "w1")
    (run,) = await rows("SELECT cost_usd FROM summary_runs")
    assert run["cost_usd"] == 0.02
    clock[0] = 1030.0
    with pytest.raises(summaries.SummaryFailed):  # inside the cool-down: no new run
        await summaries.get_summary(reports.engine, "w1")
    assert len(calls) == 1
    clock[0] = 1100.0
    with pytest.raises(summaries.SummaryFailed):
        await summaries.get_summary(reports.engine, "w1")
    assert len(calls) == 2


async def test_spawn_error_is_a_failed_summary(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    on(reports, claude_bin=str(reports.tmp / "no-such-claude"))
    await plant_lifecycle(reports)
    with pytest.raises(summaries.SummaryFailed):
        await summaries.get_summary(reports.engine, "w1")
    assert reports.engine.summaries_running == 0 and reports.engine.summary_jobs == {}


async def test_cancelled_run_kills_its_process(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = reports.tmp / "slow-claude"
    script.write_text("#!/bin/sh\nexec sleep 30\n")
    script.chmod(0o755)
    on(reports, claude_bin=str(script))
    procs: list[asyncio.subprocess.Process] = []
    real = asyncio.create_subprocess_exec

    async def spy(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        proc = await real(*args, **kwargs)
        procs.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)
    job = asyncio.ensure_future(summaries.run_claude(reports.engine, "hi"))
    await wait_for(lambda: asyncio.sleep(0, result=bool(procs)))
    job.cancel()
    with pytest.raises(asyncio.CancelledError):
        await job
    assert procs[0].returncode is not None  # killed and reaped
