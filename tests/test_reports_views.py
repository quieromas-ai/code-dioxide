import json
from typing import Any

from src.auth import Access
from src.reports.views import LIST_ROW_KEYS, STATUS_KEYS, STEP_KEYS
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_attempt, insert_step, insert_task

SECRET = "PLANTED-SECRET-XYZ"


async def plant(task_id: str = "t1") -> str:
    output = {"text": SECRET, "return": {"x": SECRET}, "tokens_used": 5, "cost_usd": 0.1}
    await insert_task(
        task_id, kind="lifecycle", description=SECRET, error=SECRET, status="needs_human",
        output_json=json.dumps(output), question_json=json.dumps({"q": SECRET}),
        branch=SECRET, worktree=f"/w/{SECRET}", created_at=1.0,
    )  # fmt: skip
    await insert_step(task_id, "plan", phase="Plan", git_branch=SECRET, git_head="abc")
    await insert_attempt(task_id, 1, ended_at=2.0, cost_usd=0.1, reason="clean_exit")
    return task_id


async def test_reports_scope_strips_free_text(env: Env) -> None:
    task_id = await plant()
    async with client_as(env, Access.reports) as client:
        for path in ("/api/v1/tasks", f"/api/v1/tasks/{task_id}"):
            response = await client.get(path)
            assert response.status_code == 200, path
            assert SECRET not in response.text, path
        listed = (await client.get("/api/v1/tasks")).json()
        status = (await client.get(f"/api/v1/tasks/{task_id}")).json()
    assert set(listed) == {"tasks", "count", "next_cursor"}
    (row,) = listed["tasks"]
    assert set(row) <= set(LIST_ROW_KEYS) and row["cost_usd"] == 0.1
    assert set(status) <= set(STATUS_KEYS)
    assert status["output"] == {"tokens_used": 5, "cost_usd": 0.1}
    assert status["status"] == "needs_human" and status["cost_complete"] is True
    (step,) = status["steps"]
    assert set(step) <= set(STEP_KEYS) and step["label"] == "plan"


async def test_admin_and_content_see_unchanged_task_routes(env: Env) -> None:
    task_id = await plant()
    expected: dict[str, Any] = {
        "/api/v1/tasks": await env.engine.list(None, None, 20, None),
        f"/api/v1/tasks/{task_id}": await env.engine.status(task_id),
    }
    for access in (Access.admin, Access.content):
        async with client_as(env, access) as client:
            for path, payload in expected.items():
                body = (await client.get(path)).json()
                assert body == json.loads(json.dumps(payload)), (access, path)
                assert SECRET in json.dumps(body)
