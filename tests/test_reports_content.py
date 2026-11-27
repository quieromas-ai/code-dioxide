import json
import shutil
import threading
from pathlib import Path

import pytest

from src import runlocator
from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_event, insert_step, insert_task
from tests.test_debug import SK

pytestmark = pytest.mark.usefixtures("reports")

SCRIPT = "export const meta = { name: 'w' }\nphase('One')"
STAGE_PROMPT = f"Implement it. token {SK}"


def jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


async def plant_lifecycle(env: Env) -> Path:
    await insert_task(
        "w1", kind="lifecycle", agent="lead", status="completed", session_id="s1",
        run_id="wf_1", description=f"Build it {SK}", input_context=json.dumps({"k": 1}),
        args_json=json.dumps({"plan_pinned": False}),
    )  # fmt: skip
    await insert_attempt("w1", 1, ended_at=1.0)
    await insert_step("w1", "implement@a1", phase="Implement", status="completed")
    store = env.engine.runstore
    run_dir = store.task_dir("w1")
    jsonl(store.attempt_paths("w1", 1)[0], [
        {"type": "system", "subtype": "init"},
        {"type": "system", "subtype": "task_started", "prompt": SCRIPT},
    ])  # fmt: skip
    store.attempt_agents_path("w1", 1).write_text(json.dumps({"dx-impl": {"prompt": "be good"}}))
    (run_dir / "run.json").write_text(json.dumps({"workflowProgress": [
        {"type": "workflow_agent", "label": "implement@a1", "promptPreview": "Implement…",
         "resultPreview": "done…"},
    ]}))  # fmt: skip
    jsonl(run_dir / "journal.jsonl", [
        {"type": "started", "key": "k1", "label": "implement@a1", "agentId": "a1",
         "phase": "Implement"},
        {"type": "result", "key": "k1", "agentId": "a1", "result": {"ok": True, "secret": SK}},
    ])  # fmt: skip
    jsonl(store.step_copy_path("w1", "implement@a1"), [
        {"type": "user", "message": {"role": "user", "content": STAGE_PROMPT},
         "timestamp": "2026-01-02T19:40:01Z"},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "ok"}]},
         "timestamp": "2026-01-02T19:40:05Z"},
    ])  # fmt: skip
    return run_dir


CONTENT_PATHS = [
    "/api/v1/agents/implementer/prompt", "/api/v1/tasks/w1/prompts", "/api/v1/tasks/w1/events",
    "/api/v1/tasks/w1/stages/implement@a1/result",
    "/api/v1/tasks/w1/stages/implement@a1/transcript",
]  # fmt: skip


async def test_task_prompts(reports: Env) -> None:
    await plant_lifecycle(reports)
    async with client_as(reports, Access.content) as client:
        body = (await client.get("/api/v1/tasks/w1/prompts")).json()
    assert body["workflow_script"] == SCRIPT
    assert body["input_context"] == {"k": 1} and body["args"] == {"plan_pinned": False}
    assert body["agents"] == [{"n": 1, "definitions": {"dx-impl": {"prompt": "be good"}}}]
    (stage,) = body["stages"]
    assert (stage["label"], stage["prompt_preview"]) == ("implement@a1", "Implement…")
    assert stage["prompt"].startswith("Implement it. token ") and SK not in stage["prompt"]
    assert SK not in body["description"]


async def test_prompts_of_pruned_task(reports: Env) -> None:
    run_dir = await plant_lifecycle(reports)
    shutil.rmtree(run_dir)
    async with client_as(reports, Access.content) as client:
        response = await client.get("/api/v1/tasks/w1/prompts")
    body = response.json()
    pruned = {"available": False, "reason": "pruned"}
    assert response.status_code == 200 and body["description"].startswith("Build it")
    assert body["workflow_script"] == pruned
    assert body["agents"] == [{"n": 1, "definitions": pruned}]


async def test_single_task_prompts(reports: Env) -> None:
    await insert_task("s1", description="Say done")
    async with client_as(reports, Access.content) as client:
        body = (await client.get("/api/v1/tasks/s1/prompts")).json()
        result = await client.get("/api/v1/tasks/s1/stages/plan/result")
    assert (body["workflow_script"], body["stages"], body["agents"]) == (None, [], [])
    assert result.status_code == 404


async def test_stage_result_and_transcript(reports: Env) -> None:
    await plant_lifecycle(reports)
    async with client_as(reports, Access.content) as client:
        result = (await client.get("/api/v1/tasks/w1/stages/implement@a1/result")).json()
        transcript = (
            await client.get("/api/v1/tasks/w1/stages/implement@a1/transcript?limit=1")
        ).json()
    assert result["result_preview"] == "done…" and result["result"]["ok"] is True
    assert SK not in json.dumps(result)
    assert len(transcript["entries"]) == 1 and transcript["next_cursor"]
    assert SK not in json.dumps(transcript)


@pytest.mark.parametrize("label", ["..%2Fx", "nope", "implement"])
async def test_stage_label_is_matched_not_joined(reports: Env, label: str) -> None:
    await plant_lifecycle(reports)
    async with client_as(reports, Access.content) as client:
        for leaf in ("result", "transcript"):
            response = await client.get(f"/api/v1/tasks/w1/stages/{label}/{leaf}")
            assert response.status_code == 404, (label, leaf)


async def test_task_events_full_data(reports: Env) -> None:
    await plant_lifecycle(reports)
    await insert_event("w1", "permission_denied", {"tool": "Bash", "message": f"no {SK}"}, 5.0)
    async with client_as(reports, Access.content) as client:
        rows = (await client.get("/api/v1/tasks/w1/events")).json()["rows"]
        missing = await client.get("/api/v1/tasks/nope/events")
    (row,) = rows
    assert row["data"]["tool"] == "Bash" and row["data"]["message"].startswith("no ")
    assert SK not in row["data"]["message"]
    assert missing.status_code == 404


async def test_agent_prompt(reports: Env) -> None:
    async with client_as(reports, Access.content) as client:
        body = (await client.get("/api/v1/agents/implementer/prompt")).json()
        alias = await client.get("/api/v1/agents/dev/prompt")
        missing = await client.get("/api/v1/agents/nobody/prompt")
    assert body["id"] == "implementer" and isinstance(body["system_prompt"], str)
    assert set(body) == {"id", "model", "effort", "tools", "disallowed_tools", "system_prompt"}
    assert alias.json()["id"] == "implementer" and missing.status_code == 404


async def test_content_redacted_everywhere(reports: Env) -> None:
    await plant_lifecycle(reports)
    await insert_event("w1", "spawn_failed", {"error": SK}, 6.0)
    async with client_as(reports, Access.content) as client:
        for path in CONTENT_PATHS:
            response = await client.get(path)
            assert response.status_code == 200, path
            assert SK not in response.text, path


PRUNED = {"available": False, "reason": "pruned"}


async def test_transcript_of_pruned_task_is_the_marker(reports: Env) -> None:
    run_dir = await plant_lifecycle(reports)
    shutil.rmtree(run_dir)
    async with client_as(reports, Access.content) as client:
        response = await client.get("/api/v1/tasks/w1/stages/implement@a1/transcript")
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["entries"], body["next_cursor"], body["transcript"]) == ([], None, PRUNED)


async def test_transcript_of_stage_not_yet_journaled(reports: Env) -> None:
    await plant_lifecycle(reports)
    await insert_step("w1", "qa@a1", phase="QA", status="running")
    async with client_as(reports, Access.content) as client:
        response = await client.get("/api/v1/tasks/w1/stages/qa@a1/transcript")
    assert response.status_code == 200, response.text
    assert (response.json()["entries"], response.json()["transcript"]) == ([], None)


async def test_prompts_of_pruned_task_keep_the_stages(reports: Env) -> None:
    run_dir = await plant_lifecycle(reports)
    shutil.rmtree(run_dir)
    async with client_as(reports, Access.content) as client:
        body = (await client.get("/api/v1/tasks/w1/prompts")).json()
    assert body["stages"] == [{"label": "implement@a1", "prompt_preview": PRUNED, "prompt": PRUNED}]


async def test_missing_file_of_a_live_task_is_not_pruned(reports: Env) -> None:
    await plant_lifecycle(reports)
    reports.engine.runstore.attempt_agents_path("w1", 1).unlink()
    reports.engine.runstore.step_copy_path("w1", "implement@a1").unlink()
    async with client_as(reports, Access.content) as client:
        body = (await client.get("/api/v1/tasks/w1/prompts")).json()
    assert body["agents"] == [{"n": 1, "definitions": None}]
    assert body["stages"][0]["prompt"] is None


async def test_transcript_route_parses_streams_off_the_loop(
    reports: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await plant_lifecycle(reports)
    loop_thread = threading.get_ident()
    seen: list[bool] = []
    real = runlocator.stream_events

    def spy(path: Path) -> list[object]:
        seen.append(threading.get_ident() == loop_thread)
        return real(path)

    monkeypatch.setattr(runlocator, "stream_events", spy)
    async with client_as(reports, Access.content) as client:
        await client.get("/api/v1/tasks/w1/stages/implement@a1/transcript")
        await client.get("/api/v1/tasks/w1/prompts")
    assert seen and not any(seen)
