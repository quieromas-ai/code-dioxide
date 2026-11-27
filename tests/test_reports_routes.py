import re
from pathlib import Path

import httpx
from fastapi import FastAPI

from src.config import Settings
from src.main import create_app
from src.reports.routes import FAMILIES

TOKEN = "test-token-value"
BASE = "http://127.0.0.1:8500"


def family_paths(app: FastAPI) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {family: set() for family in FAMILIES}
    for path, operations in app.openapi()["paths"].items():
        for operation in operations.values():
            for tag in operation.get("tags", []):
                if tag in found:
                    found[tag].add(path)
    return found


def client(app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 50000))
    return httpx.AsyncClient(
        transport=transport, base_url=BASE, headers={"Authorization": f"Bearer {TOKEN}"}
    )


async def test_index_lists_every_family_route(tmp_path: Path) -> None:
    app = create_app(Settings(home=tmp_path, auth_token=TOKEN))
    async with client(app) as c:
        index = (await c.get("/api/v1")).json()
    assert set(index["families"]) == set(FAMILIES)
    expected = family_paths(app)
    for family, entry in index["families"].items():
        assert entry["description"] == FAMILIES[family]
        assert {r["path"] for r in entry["routes"]} == expected[family], family
        assert all(r["method"] == "GET" for r in entry["routes"])
    assert {"/api/v1/tasks", "/api/v1/tasks/{task_id}"} <= expected["entities"]
    listed = {r["path"] for entry in index["families"].values() for r in entry["routes"]}
    assert not listed & {"/api/v1/server/logs", "/api/v1/tasks/{task_id}/logs", "/health"}
    assert expected["activity"] == {
        "/api/v1/activity",
        "/api/v1/activity/stream",
        "/api/v1/events",
    } and expected["content"] == {
        "/api/v1/agents/{agent_id}/prompt",
        "/api/v1/tasks/{task_id}/prompts",
        "/api/v1/tasks/{task_id}/events",
        "/api/v1/tasks/{task_id}/stages/{label}/result",
        "/api/v1/tasks/{task_id}/stages/{label}/transcript",
        "/api/v1/tasks/{task_id}/summary",
    }


async def test_openapi_served_with_family_tags(tmp_path: Path) -> None:
    app = create_app(Settings(home=tmp_path, auth_token=TOKEN))
    async with client(app) as c:
        response = await c.get("/api/v1/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    for family, routes in family_paths(app).items():
        for path in routes:
            assert family in paths[path]["get"]["tags"], path


def test_docs_routes_exist(tmp_path: Path) -> None:
    docs = Path(__file__).resolve().parent.parent / "docs"
    text = "".join(
        (docs / name).read_text()
        for name in ("reporting-api.md", "reporting-api-reference.md", "dashboards.md")
    )
    named = {re.sub(r"\{[^}]+\}", "{x}", p) for p in re.findall(r"`(/api/v1[^`?]*)`", text)}
    app = create_app(Settings(home=tmp_path, auth_token=TOKEN))
    served = {re.sub(r"\{[^}]+\}", "{x}", p) for p in app.openapi()["paths"]}
    served |= {"/api/v1", "/api/v1/openapi.json"}
    assert named and named <= served, named - served
