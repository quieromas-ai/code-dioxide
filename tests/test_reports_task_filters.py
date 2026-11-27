from src.auth import Access
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_task


async def plant() -> None:
    await insert_task("a1", cwd="/proj/a", kind="single", created_at=100.0)
    await insert_task("a2", cwd="/proj/a", kind="lifecycle", created_at=200.0)
    await insert_task("b1", cwd="/proj/b", kind="single", created_at=300.0)


async def ids(env: Env, query: str) -> list[str]:
    async with client_as(env, Access.admin) as client:
        response = await client.get(f"/api/v1/tasks{query}")
    assert response.status_code == 200, response.text
    return [row["task_id"] for row in response.json()["tasks"]]


async def test_task_list_shared_filters(env: Env) -> None:
    await plant()
    assert await ids(env, "") == ["b1", "a2", "a1"]
    assert await ids(env, "?project=/proj/a") == ["a2", "a1"]
    assert await ids(env, "?kind=lifecycle") == ["a2"]
    assert await ids(env, "?since=150") == ["b1", "a2"]
    assert await ids(env, "?until=250") == ["a2", "a1"]
    assert await ids(env, "?since=1970-01-01T00:02:30Z&until=250") == ["a2"]


async def test_task_list_bad_filter_is_400(env: Env) -> None:
    async with client_as(env, Access.reports) as client:
        for query in ("?kind=x", "?since=x", "?since=9&until=5"):
            response = await client.get(f"/api/v1/tasks{query}")
            assert response.status_code == 400, query
            assert "error" in response.json()
