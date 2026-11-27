"""Rows planted straight into the database for the reporting tests."""

import asyncio
import json
from collections.abc import Callable
from typing import Any

import httpx
from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

from src.auth import Access
from src.database import write_tx
from src.mcp_server import ToolProvider
from src.reports.routes import install_reports
from src.rest_api import build_router
from tests.helpers import Env

TASK_DEFAULTS: dict[str, Any] = {
    "agent": "implementer",
    "kind": "single",
    "description": "d",
    "status": "completed",
    "cwd": "/proj/a",
    "created_at": 0.0,
    "updated_at": 0.0,
}


async def insert_task(task_id: str, **cols: Any) -> None:
    row = {"id": task_id, **TASK_DEFAULTS, **cols}
    names = ", ".join(row)
    marks = ", ".join("?" * len(row))
    async with write_tx() as db:
        await db.execute(f"INSERT INTO tasks ({names}) VALUES ({marks})", list(row.values()))


async def insert_attempt(task_id: str, n: int, **cols: Any) -> None:
    row = {"task_id": task_id, "n": n, "session_id": "s1", **cols}
    names = ", ".join(row)
    marks = ", ".join("?" * len(row))
    async with write_tx() as db:
        await db.execute(f"INSERT INTO attempts ({names}) VALUES ({marks})", list(row.values()))


async def insert_event(task_id: str | None, type_: str, data: dict[str, Any], at: float) -> None:
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO events (task_id, type, data_json, at) VALUES (?, ?, ?, ?)",
            (task_id, type_, json.dumps(data), at),
        )


async def insert_step(task_id: str, label: str, **cols: Any) -> None:
    row = {"task_id": task_id, "label": label, "status": "done", **cols}
    names = ", ".join(row)
    marks = ", ".join("?" * len(row))
    async with write_tx() as db:
        await db.execute(f"INSERT INTO steps ({names}) VALUES ({marks})", list(row.values()))


class AsAccess:
    def __init__(self, app: ASGIApp, access: Access) -> None:
        self.app, self.access = app, access

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope.setdefault("state", {})["access"] = self.access
        await self.app(scope, receive, send)


def client_as(env: Env, access: Access) -> httpx.AsyncClient:
    provider = ToolProvider()
    provider.attach(env.engine)
    app = FastAPI()
    install_reports(app, provider)
    app.include_router(build_router(provider))
    transport = httpx.ASGITransport(app=AsAccess(app, access), client=("127.0.0.1", 50000))
    return httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8500")


def reports_app(env: Env, access: Access, **router: Any) -> AsAccess:
    """The reporting routes alone (``router``: ``keepalive_s``, ``poll_s``), as ``access``."""
    provider = ToolProvider()
    provider.attach(env.engine)
    app = FastAPI()
    install_reports(app, provider, **router)
    return AsAccess(app, access)


async def sse_read(
    app: Any,
    path: str,
    stop: Callable[[str], bool],
    *,
    query: str = "",
    headers: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> tuple[int, str]:
    """Raw ASGI GET of an endless stream: the client goes away once ``stop(body so far)``.
    Returns the status and the body."""
    requested, gone = False, asyncio.Event()
    status, body = 0, bytearray()

    async def receive() -> dict[str, Any]:
        nonlocal requested
        if not requested:
            requested = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await gone.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = message["status"]
        body.extend(message.get("body", b""))
        if stop(body.decode()) or not message.get("more_body", True):
            gone.set()

    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": query.encode(), "headers": [(b"host", b"test"), *raw],
        "client": ("127.0.0.1", 50000), "server": ("test", 80), "root_path": "",
    }  # fmt: skip
    await asyncio.wait_for(app(scope, receive, send), timeout)
    return status, body.decode()


def sse_events(body: str) -> list[dict[str, Any]]:
    """The body's events as ``{"event", "id", "data"}`` (keep-alive comments skipped)."""
    found = []
    for block in body.split("\n\n"):
        fields: dict[str, Any] = {"event": None, "id": None, "data": None}
        for line in block.splitlines():
            key, _, value = line.partition(": ")
            if key in ("event", "id"):
                fields[key] = value
            elif key == "data":
                fields["data"] = json.loads(value)
        if fields["data"] is not None:
            found.append(fields)
    return found
