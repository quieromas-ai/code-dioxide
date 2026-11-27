import os
import socket
import sqlite3
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI
from mcp.server.transport_security import TransportSecuritySettings
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from uvicorn.config import STARTUP_FAILURE

from src import bundle, childenv, procs, serverlog
from src.auth import BearerAuthMiddleware
from src.cf_access import CfAccessVerifier
from src.childenv import ChildEnvError
from src.config import AlreadyRunning, Settings, acquire_home_lock
from src.database import close_db, init_db
from src.engine import TaskEngine
from src.logpager import MAX_CURSOR_CHARS, load_key
from src.mcp_server import ToolProvider, build_mcp
from src.redact import Redactor
from src.reports.db import close_read_db, open_read_db
from src.reports.routes import install_reports
from src.rest_api import build_router
from src.roles import load_registry
from src.runstore import RunStore

PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
# The SDK's own DNS-rebinding allowlist for a loopback bind; kept when `allowed_hosts` is set.
_LOOPBACK_HOSTS = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
_LOOPBACK_ORIGINS = ["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"]
SECRET_FIELDS = {"auth_token", "reports_token", "content_token"}
# REST log cursors travel in the query string (`?cursor=`). A realistic merged-view cursor is a
# few KiB (about 1-3 KiB for 20-75 sources), but the pager accepts up to MAX_CURSOR_CHARS, far
# above h11's 16 KiB default for the request head. The limit covers a cap-size cursor plus the
# rest of the request line and the headers; a bigger head is refused by h11 (HTTP 4xx) before
# any handler runs. h11 is pinned: httptools (uvicorn[standard]) would ignore this setting.
MAX_REQUEST_HEAD_BYTES = MAX_CURSOR_CHARS + 64 * 1024
STARTUP_FAILED = "dioxide: startup failed: "
# A stop waits this long for open requests (an SSE or MCP stream never ends by itself), then
# cancels them; the lifespan shutdown (engine stop, database close) runs after that.
GRACEFUL_SHUTDOWN_S = 5


class _StartupFailureMessage:
    """For a known startup failure, replaces the traceback Starlette reports to the server
    with one line (``STARTUP_FAILED`` + reason). Unexpected failures keep their traceback."""

    def __init__(self, app: ASGIApp, failures: list[str]) -> None:
        self.app = app
        self.failures = failures

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await self.app(scope, receive, send)
            return

        async def reporting(message: Message) -> None:
            if message["type"] == "lifespan.startup.failed" and self.failures:
                reason = " ".join(self.failures[-1].split())  # one line, whatever the error
                message = {**message, "message": STARTUP_FAILED + reason}
            await send(message)

        await self.app(scope, receive, reporting)


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIR_MODE)
    path.chmod(PRIVATE_DIR_MODE)


def _private_file(path: Path) -> None:
    """Create `path` if missing and force mode 0600 (SQLite -wal/-shm inherit the db's mode)."""
    os.close(os.open(path, os.O_RDWR | os.O_CREAT, PRIVATE_FILE_MODE))
    path.chmod(PRIVATE_FILE_MODE)


def _transport_security(settings: Settings) -> TransportSecuritySettings | None:
    """None keeps the SDK default (loopback-only Host check); extra hosts extend it."""
    if not settings.allowed_hosts:
        return None
    origins = [
        f"{scheme}://{host}" for host in settings.allowed_hosts for scheme in ("http", "https")
    ]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[*_LOOPBACK_HOSTS, *settings.allowed_hosts],
        allowed_origins=[*_LOOPBACK_ORIGINS, *origins],
    )


def _cf_verifier(settings: Settings) -> CfAccessVerifier | None:
    policy = settings.cf_access_policy()
    return CfAccessVerifier(policy) if policy is not None else None


def create_app(settings: Settings) -> FastAPI:
    """The ASGI app: REST routes and the MCP endpoint at /mcp, all behind bearer auth."""
    provider = ToolProvider()
    failures: list[str] = []  # known startup failures, reported as one line
    mcp = build_mcp(provider)
    mcp_app = mcp.streamable_http_app(
        host=settings.host, transport_security=_transport_security(settings)
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        for directory in (
            settings.home,
            settings.data_dir,
            settings.runs_dir,
            settings.worktrees_dir,
        ):
            _private_dir(directory)
        # Lock before logging: a losing instance must not open (or write to) the winner's log.
        try:
            lock = acquire_home_lock(settings.lock_path)
        except AlreadyRunning as exc:
            failures.append(
                f"dioxide is already running for home {settings.home} (lock {settings.lock_path})"
            )
            raise AlreadyRunning(failures[-1]) from exc
        with lock:
            _private_file(settings.server_log_path)
            logger = serverlog.setup(settings.server_log_path)
            if settings.child_env_file is not None:  # checked again at every launch
                try:
                    childenv.read(settings.child_env_file)
                except ChildEnvError as exc:
                    logger.error("child env file unusable", extra={"error": str(exc)})
                    failures.append(str(exc))
                    raise
            redactor = Redactor.from_settings(settings)  # warns here on unreadable redact files
            config = settings.model_dump(mode="json", exclude=SECRET_FIELDS)
            logger.info("config at start", extra={"config": redactor.redact_json(config)})
            try:
                agents, roles = load_registry(settings, logger)
            except (OSError, ValueError) as exc:
                logger.error("invalid configuration", extra={"error": str(exc)})
                failures.append(f"invalid configuration: {exc}")
                raise
            try:
                _private_file(settings.db_path)
                await init_db(settings.db_path)
                await open_read_db(settings.db_path)
            except (OSError, sqlite3.Error, RuntimeError) as exc:
                logger.error("database unusable", extra={"error": str(exc)})
                failures.append(f"database {settings.db_path}: {exc}")
                await close_read_db()  # an open connection's thread would keep the process alive
                await close_db()
                raise
            try:
                load_key(settings.cursor_key_path)  # created once (0600) before logs are paged
            except RuntimeError as exc:
                logger.error("cursor key unusable", extra={"error": str(exc)})
                failures.append(str(exc))
                await close_read_db()
                await close_db()
                raise
            if stale := bundle.sweep_stale(settings.data_dir):
                logger.info("removed stale bundle temp dirs", extra={"dirs": stale})
            engine = TaskEngine(settings, roles, agents, RunStore(settings.runs_dir), redactor)
            try:
                await engine.start()
                provider.attach(engine)
                async with mcp.session_manager.run():
                    logger.info("dioxide started", extra={"host": settings.host})
                    yield
            finally:
                provider.detach()
                await engine.stop()
                await close_read_db()
                await close_db()
                logger.info("dioxide stopped")

    app = FastAPI(
        title="code-dioxide",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url="/api/v1/openapi.json",  # interactive docs stay off
    )
    install_reports(app, provider)
    app.include_router(build_router(provider))
    # The MCP sub-app serves its own /mcp route; FastAPI routes above take precedence.
    app.mount("/", mcp_app)
    app.add_middleware(
        BearerAuthMiddleware,
        token=settings.auth_token,
        reports_token=settings.reports_token,
        content_token=settings.content_token,
        auth_disabled=settings.auth_disabled,
        trust_loopback=settings.trust_loopback,
        tunnel_port=settings.tunnel_port,
        cf_verifier=_cf_verifier(settings),
    )
    app.add_middleware(_StartupFailureMessage, failures=failures)
    return app


def load_settings() -> Settings:
    """Settings from the environment and the home's ``.env``; ValueError when invalid."""
    settings = Settings()
    settings.validate_security()
    return settings


def server_options(settings: Settings) -> dict[str, Any]:
    # proxy_headers=False: auth must see the real peer, never a client-supplied X-Forwarded-For.
    # access_log=False: one line per request (SSE polls included) would flood the service's
    # stdout log, which launchd never rotates; server.log rotates and logs what matters.
    return {
        "host": settings.host,
        "port": settings.port,
        "proxy_headers": False,
        "http": "h11",
        "h11_max_incomplete_event_size": MAX_REQUEST_HEAD_BYTES,
        "timeout_graceful_shutdown": GRACEFUL_SHUTDOWN_S,
        "access_log": False,
    }


def _bind_listener(host: str, port: int) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except OSError:
        sock.close()
        raise
    sock.set_inheritable(True)
    return sock


def serve(settings: Settings) -> None:
    # Every file the process creates (db, -wal/-shm, logs, run output) is private; spawned
    # children get the umask the server started with back.
    procs.set_child_umask(os.umask(0o077))
    app = create_app(settings)
    if not settings.tunnel_port:
        uvicorn.run(app, **server_options(settings))
        return
    # Two listeners, one app: the main one and the Cloudflare Tunnel one (src.auth tells them
    # apart by the connection's local port).
    sockets: list[socket.socket] = []
    try:
        for host, port in (
            (settings.host, settings.port),
            (settings.tunnel_host, settings.tunnel_port),
        ):
            sockets.append(_bind_listener(host, port))
        server = uvicorn.Server(uvicorn.Config(app, **server_options(settings)))
        server.run(sockets=sockets)
    finally:
        for sock in sockets:
            sock.close()
    if not server.started:  # what uvicorn.run does: a failed startup is not a clean exit
        sys.exit(STARTUP_FAILURE)


def run() -> None:
    serve(load_settings())
