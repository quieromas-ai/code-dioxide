import asyncio
import json
import os
import stat
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import httpx
import httpx2
import pytest
import uvicorn
from fastapi import FastAPI
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

import src.main
from src.config import AlreadyRunning, Settings, acquire_home_lock
from src.database import write_tx
from src.logpager import MAX_CURSOR_CHARS
from src.main import create_app, run

TOKEN = "test-token-value"
LOOPBACK = ("127.0.0.1", 50000)
REMOTE = ("203.0.113.9", 4321)
BASE_URL = "http://127.0.0.1:8500"
TEN_TOOLS = {
    "delegate_task",
    "check_task_status",
    "prompt_agent",
    "get_agents",
    "get_active_agents",
    "answer_task",
    "cancel_task",
    "list_tasks",
    "get_task_logs",
    "get_task_debug",
}


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {"home": tmp_path, "auth_token": TOKEN}
    values.update(overrides)
    return Settings(**values)


def client_for(app: FastAPI, client: tuple[str, int]) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=client)
    return httpx.AsyncClient(transport=transport, base_url=BASE_URL)


def bearer(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[FastAPI]:
    async with app.router.lifespan_context(app):
        yield app


async def test_token_required_on_loopback_by_default(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path))
    async with client_for(app, LOOPBACK) as client:
        for path in ("/health", "/mcp", "/api/anything"):
            denied = await client.get(path)
            assert denied.status_code == 401, path
            assert denied.json() == {"error": "unauthorized"}
            assert denied.headers["www-authenticate"] == "Bearer"
        assert (await client.get("/health", headers=bearer("wrong"))).status_code == 401
        allowed = await client.get("/health", headers=bearer())
    assert allowed.status_code == 200
    assert allowed.json() == {"status": "ok", "queued": 0, "running": 0, "needs_human": 0}


async def test_trusted_loopback_without_token(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path, trust_loopback=True))
    async with client_for(app, LOOPBACK) as client:
        assert (await client.get("/health")).status_code == 200
    async with client_for(app, ("::1", 50000)) as client:
        assert (await client.get("/health")).status_code == 200
    async with client_for(app, REMOTE) as client:
        assert (await client.get("/health")).status_code == 401


@pytest.mark.parametrize(
    "header",
    ["X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-IP"],
)
async def test_forwarded_header_never_exempt(tmp_path: Path, header: str) -> None:
    app = create_app(make_settings(tmp_path, trust_loopback=True))
    async with client_for(app, LOOPBACK) as client:
        denied = await client.get("/health", headers={header: "127.0.0.1"})
        allowed = await client.get("/health", headers={header: "127.0.0.1", **bearer()})
    assert denied.status_code == 401
    assert allowed.status_code == 200


async def test_auth_disabled_bypasses(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path, auth_token="", auth_disabled=True))
    async with client_for(app, REMOTE) as client:
        assert (await client.get("/health")).status_code == 200


async def test_empty_token_never_matches(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path, auth_token=""))
    async with client_for(app, LOOPBACK) as client:
        response = await client.get("/health", headers={"Authorization": "Bearer "})
    assert response.status_code == 401


async def test_proxy_headers_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(app: Any, **kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", TOKEN)
    monkeypatch.setattr(src.main.uvicorn, "run", fake_run)
    run()
    assert len(calls) == 1
    assert calls[0]["proxy_headers"] is False
    assert (calls[0]["host"], calls[0]["port"]) == ("127.0.0.1", 8500)

    app = create_app(make_settings(tmp_path, trust_loopback=True))
    async with client_for(app, REMOTE) as client:
        spoofed = await client.get("/health", headers={"X-Forwarded-For": "127.0.0.1"})
    assert spoofed.status_code == 401


def test_run_refuses_missing_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setattr(src.main.uvicorn, "run", lambda *a, **k: pytest.fail("must not serve"))
    with pytest.raises(ValueError, match="auth_token"):
        run()


async def test_lifespan_holds_and_releases_home_lock(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    app = create_app(settings)
    async with running(app):
        assert settings.db_path.is_file()
        assert settings.server_log_path.parent.is_dir()
        with pytest.raises(AlreadyRunning, match="another dioxide engine"):
            acquire_home_lock(settings.lock_path)
    acquire_home_lock(settings.lock_path).release()


async def test_mcp_endpoint_lists_ten_tools_with_token(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path))
    async with running(app):
        async with client_for(app, LOOPBACK) as client:
            denied = await client.post(
                "/mcp",
                content=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
                headers={"Content-Type": "application/json"},
            )
        assert denied.status_code == 401

        http_client = httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app, client=LOOPBACK), headers=bearer()
        )
        async with http_client:
            transport = streamable_http_client(f"{BASE_URL}/mcp", http_client=http_client)
            async with Client(transport, mode="legacy", cache=None) as mcp_client:
                result = await mcp_client.list_tools()
    assert {tool.name for tool in result.tools} == TEN_TOOLS


def mode_of(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


async def test_lifespan_creates_private_files_and_dirs(tmp_path: Path) -> None:
    settings = make_settings(tmp_path / "home")
    async with running(create_app(settings)):
        for directory in (
            settings.home,
            settings.data_dir,
            settings.runs_dir,
            settings.worktrees_dir,
        ):
            assert mode_of(directory) == 0o700, directory
        assert mode_of(settings.db_path) == 0o600
        assert mode_of(settings.server_log_path) == 0o600
        for sidecar in (Path(f"{settings.db_path}-wal"), Path(f"{settings.db_path}-shm")):
            if sidecar.exists():
                assert mode_of(sidecar) == 0o600, sidecar


async def test_lifespan_tightens_existing_wide_modes(tmp_path: Path) -> None:
    settings = make_settings(tmp_path / "home")
    for directory in (settings.home, settings.data_dir, settings.runs_dir, settings.worktrees_dir):
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o755)
    settings.server_log_path.write_text("")
    settings.server_log_path.chmod(0o644)
    async with running(create_app(settings)):
        for directory in (
            settings.home,
            settings.data_dir,
            settings.runs_dir,
            settings.worktrees_dir,
        ):
            assert mode_of(directory) == 0o700, directory
        assert mode_of(settings.db_path) == 0o600
        assert mode_of(settings.server_log_path) == 0o600


def test_run_sets_restrictive_umask(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", TOKEN)
    monkeypatch.setattr(src.main.uvicorn, "run", lambda *a, **k: None)
    previous = os.umask(0o022)
    try:
        run()
        assert os.umask(previous) == 0o077
    finally:
        os.umask(previous)


async def test_losing_instance_refuses_and_leaves_winner_log_alone(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    app = create_app(settings)
    async with running(app):
        before = settings.server_log_path.read_bytes()
        with pytest.raises(AlreadyRunning, match="already running"):
            async with running(create_app(settings)):
                pytest.fail("second instance must not start")
        assert settings.server_log_path.read_bytes() == before


async def test_losing_instance_does_not_touch_log_before_lock(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    settings.data_dir.mkdir(parents=True)
    with acquire_home_lock(settings.lock_path):
        with pytest.raises(AlreadyRunning, match="already running"):
            async with running(create_app(settings)):
                pytest.fail("must not start")
    assert not settings.server_log_path.exists()
    assert not settings.db_path.exists()


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
async def test_fastapi_docs_not_exposed(tmp_path: Path, path: str) -> None:
    app = create_app(make_settings(tmp_path))
    async with client_for(app, LOOPBACK) as client:
        response = await client.get(path, headers=bearer())
    assert response.status_code == 404


MCP_INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "t", "version": "0"},
    },
}
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


async def post_initialize(app: FastAPI, host: str) -> httpx.Response:
    async with client_for(app, REMOTE) as client:
        return await client.post(
            "/mcp",
            content=json.dumps(MCP_INIT),
            headers={**MCP_HEADERS, **bearer(), "Host": host},
        )


async def test_mcp_foreign_host_rejected_by_default(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path))
    async with running(app):
        response = await post_initialize(app, "mcp.example.test")
    assert response.status_code == 421


async def test_allowed_hosts_admit_configured_host(tmp_path: Path) -> None:
    app = create_app(make_settings(tmp_path, allowed_hosts=["mcp.example.test"]))
    async with running(app):
        configured = await post_initialize(app, "mcp.example.test")
        other = await post_initialize(app, "evil.example.test")
        loopback = await post_initialize(app, "127.0.0.1:8500")
        async with client_for(app, REMOTE) as client:
            health = await client.get("/health", headers={**bearer(), "Host": "evil.example.test"})
    assert configured.status_code == 200
    assert other.status_code == 421
    assert loopback.status_code == 200
    assert health.status_code == 200


# -- request head limit vs log cursors ------------------------------------------------------
def test_run_request_head_fits_max_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", TOKEN)
    monkeypatch.setattr(src.main.uvicorn, "run", lambda app, **kw: calls.append(kw))
    run()
    (options,) = calls
    assert options["http"] == "h11"  # the limit below is h11's; httptools would ignore it
    assert options["h11_max_incomplete_event_size"] == src.main.MAX_REQUEST_HEAD_BYTES
    assert src.main.MAX_REQUEST_HEAD_BYTES >= MAX_CURSOR_CHARS + 16384


@asynccontextmanager
async def served(app: Any, settings: Settings, **override: Any) -> AsyncIterator[str]:
    """``app`` behind a real uvicorn with dioxide's server options, on a free loopback port."""
    options = {**src.main.server_options(settings), "port": 0, **override}
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="critical", **options))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


async def raw_get(url: str, target: str) -> int | None:
    """The status of one raw HTTP/1.1 GET (httpx refuses URLs over 64 KiB, so raw bytes); None
    when the server reset the connection (it refused the head while it was still arriving)."""
    host, port = url.removeprefix("http://").split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    head = f"GET {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    try:
        writer.write(head.encode())
        status_line = await reader.readline()
        await reader.read()
    except ConnectionResetError:
        return None
    finally:
        writer.close()
        with suppress(ConnectionResetError):
            await writer.wait_closed()
    return int(status_line.split()[1]) if status_line else None


def refused(status: int | None) -> bool:
    return status is None or (400 <= status < 500 and status != 404)


async def test_max_cursor_passes_server_oversize_head_refused(tmp_path: Path) -> None:
    """A cursor at the pager's cap reaches the app (h11's default limit refuses it); a request
    head well beyond the server limit is refused by h11 before any handler runs. h11 enforces
    the limit on the buffered incomplete head, so the oversize case uses twice the limit."""
    echo = FastAPI()
    seen: list[int] = []

    @echo.get("/api/v1/tasks/{task_id}/logs")
    async def logs(task_id: str, cursor: str = "") -> dict[str, int]:
        seen.append(len(cursor))
        return {"len": len(cursor)}

    target = "/api/v1/tasks/t/logs?cursor="
    async with served(echo, make_settings(tmp_path)) as url:
        assert await raw_get(url, target + "A" * MAX_CURSOR_CHARS) == 200
        oversize = target + "A" * (2 * src.main.MAX_REQUEST_HEAD_BYTES)
        assert refused(await raw_get(url, oversize))
    assert seen == [MAX_CURSOR_CHARS]
    settings = make_settings(tmp_path)
    async with served(echo, settings, h11_max_incomplete_event_size=None) as url:  # default
        assert refused(await raw_get(url, target + "A" * MAX_CURSOR_CHARS))
    assert seen == [MAX_CURSOR_CHARS]


def test_server_options_bound_shutdown_and_quiet_access_log(tmp_path: Path) -> None:
    options = src.main.server_options(make_settings(tmp_path))
    assert options["timeout_graceful_shutdown"] == src.main.GRACEFUL_SHUTDOWN_S == 5
    assert options["access_log"] is False


async def test_stop_with_open_sse_stream_is_bounded(tmp_path: Path) -> None:
    """An open SSE stream cannot hold a stop past the graceful-shutdown bound; the lifespan
    shutdown (engine stop, database close) still runs."""
    settings = make_settings(tmp_path)
    app = create_app(settings)
    options = {**src.main.server_options(settings), "port": 0}
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", **options))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO tasks (id, status, created_at, updated_at) VALUES ('t-1', 'running', 0, 0)"
        )
    url = f"http://127.0.0.1:{port}"
    bound = src.main.GRACEFUL_SHUTDOWN_S + 5
    # No client timeout: the client never gives up first (httpx defaults to 5 s).
    async with httpx.AsyncClient(base_url=url, headers=bearer(), timeout=None) as client:
        async with client.stream("GET", "/api/v1/tasks/t-1/stream") as response:
            assert response.status_code == 200
            started = time.monotonic()
            server.should_exit = True
            done, _ = await asyncio.wait({serving}, timeout=bound)
            elapsed = time.monotonic() - started
            if not done:  # a regression: force the server down so the test fails, not hangs
                server.force_exit = True
                for task in asyncio.all_tasks():
                    if task is not asyncio.current_task() and task is not serving:
                        task.cancel()
                with suppress(BaseException):
                    await asyncio.wait_for(serving, 5)
            assert done, f"stop still blocked after {elapsed:.1f}s"
    assert elapsed < bound
    assert "dioxide stopped" in settings.server_log_path.read_text()
