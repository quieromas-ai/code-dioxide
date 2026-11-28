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


async def test_task_list_task_filter_returns_one_row(env: Env) -> None:
    await plant()
    assert await ids(env, "?task=a2") == ["a2"]
    assert await ids(env, "?task=a2&kind=single") == []
    assert await ids(env, "?task=nope") == []


async def test_task_list_status_filter_validated(env: Env) -> None:
    await plant()
    assert await ids(env, "?status=completed") == ["b1", "a2", "a1"]
    assert await ids(env, "?status=queued") == []


async def test_task_list_task_filter_pages_with_cursor(env: Env) -> None:
    await plant()
    async with client_as(env, Access.admin) as client:
        first = (await client.get("/api/v1/tasks?project=/proj/a&limit=1")).json()
        assert [r["task_id"] for r in first["tasks"]] == ["a2"]
        cursor = first["next_cursor"]
        page = await client.get(f"/api/v1/tasks?task=a1&limit=1&cursor={cursor}")
        assert page.status_code == 200, page.text
        assert [r["task_id"] for r in page.json()["tasks"]] == ["a1"]
        past = await client.get(f"/api/v1/tasks?task=a2&limit=1&cursor={cursor}")
        assert [r["task_id"] for r in past.json()["tasks"]] == []


async def test_task_list_unknown_or_bad_param_is_400(env: Env) -> None:
    await plant()
    for access in (Access.admin, Access.reports):
        async with client_as(env, access) as client:
            for query in ("?tsk=a1", "?status=bogus", "?task=a1&junk=1"):
                response = await client.get(f"/api/v1/tasks{query}")
                assert response.status_code == 400, (access, query)
                assert "error" in response.json()


async def test_task_list_empty_value_is_no_filter(env: Env) -> None:
    await plant()
    assert await ids(env, "?status=&task=&project=") == ["b1", "a2", "a1"]
