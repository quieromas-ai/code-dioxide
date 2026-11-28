"""REST routes (included before the MCP mount; all behind the bearer-auth middleware).

Handlers stay thin: they call the same `ToolProvider` methods as the MCP tools.
"""

import asyncio
import time
from collections.abc import AsyncGenerator, AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

from src import bundle
from src.auth import Access, access_of
from src.mcp_server import ToolProvider
from src.reports.query import QueryError, check_params, parse_filters
from src.reports.views import reports_list, reports_status
from src.sse import KEEPALIVE, sse

KEEPALIVE_S = 15.0
POLL_S = 1.0
STREAM_LIMIT = 200
BUNDLE_CHUNK = 1 << 16
_UNAVAILABLE = {"error": "engine not started", "status": "unavailable"}

# Every parameter GET /api/v1/tasks accepts: any other one is refused (400), never ignored.
TASK_LIST_PARAMS = frozenset(
    {"status", "agent", "limit", "cursor", "project", "kind", "since", "until", "task"}
)


class AnswerBody(BaseModel):
    answer: str


async def _file_chunks(path: Path) -> AsyncGenerator[bytes]:
    with path.open("rb") as fh:
        while chunk := await asyncio.to_thread(fh.read, BUNDLE_CHUNK):
            yield chunk


class _TempFileResponse(StreamingResponse):
    """Streams a file from a temp dir and removes the dir on every exit path (done, client
    disconnect on either ASGI path, error), closing the body generator explicitly."""

    def __init__(self, path: Path, tmp: Path, **kwargs: Any) -> None:
        self._chunks = _file_chunks(path)
        self._tmp = tmp
        super().__init__(self._chunks, **kwargs)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._chunks.aclose()
            await asyncio.to_thread(bundle.cleanup, self._tmp)


def build_router(
    provider: ToolProvider, keepalive_s: float = KEEPALIVE_S, poll_s: float = POLL_S
) -> APIRouter:
    """REST API over `provider`; `keepalive_s` / `poll_s` pace the SSE stream."""
    router = APIRouter()

    async def missing(task_id: str) -> JSONResponse | None:
        if provider.engine is None:
            return JSONResponse(_UNAVAILABLE, status_code=503)
        if not await provider.task_exists(task_id):
            return JSONResponse({"error": f"task {task_id} not found"}, status_code=404)
        return None

    def payload(result: dict[str, Any]) -> JSONResponse:
        if result.get("status") == "unavailable":
            return JSONResponse(result, status_code=503)
        return JSONResponse(result, status_code=400 if "error" in result else 200)

    @router.get("/health", tags=["service"])
    async def health() -> dict[str, Any]:
        return {"status": "ok", **await provider.task_counts()}

    @router.get("/api/v1/tasks", tags=["entities"], summary="tasks, newest first")
    async def list_tasks(
        request: Request,
        status: str | None = None,
        agent: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
        project: str | None = None,
        kind: str | None = None,
        since: str | None = None,
        until: str | None = None,
        task: str | None = None,
    ) -> Response:
        given = {
            "status": status, "task": task, "project": project, "kind": kind,
            "since": since, "until": until,
        }  # fmt: skip
        try:
            check_params(request.query_params, TASK_LIST_PARAMS)
            # An empty value (`?status=` from an unset dropdown) means no filter, as for `agent`.
            filters = parse_filters(
                {k: v for k, v in given.items() if v}, now=time.time(), windowed=False
            )
        except QueryError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        result = await provider.list_tasks(filters.status, agent, limit, cursor, filters)
        if access_of(request.scope) is Access.reports:
            result = reports_list(result)
        return payload(result)

    @router.get("/api/v1/tasks/{task_id}", tags=["entities"], summary="one task")
    async def get_task(request: Request, task_id: str) -> Response:
        if (gone := await missing(task_id)) is not None:
            return gone
        result = await provider.check_task_status(task_id)
        if access_of(request.scope) is Access.reports:
            result = reports_status(result)
        return JSONResponse(result)

    @router.post("/api/v1/tasks/{task_id}/cancel", tags=["admin"])
    async def cancel_task(task_id: str) -> Response:
        return await missing(task_id) or JSONResponse(await provider.cancel_task(task_id))

    @router.post("/api/v1/tasks/{task_id}/answer", tags=["admin"])
    async def answer_task(task_id: str, body: AnswerBody) -> Response:
        return await missing(task_id) or JSONResponse(
            await provider.answer_task(task_id, body.answer, source="rest")
        )

    @router.get("/api/v1/tasks/{task_id}/logs", tags=["admin"])
    async def task_logs(
        task_id: str,
        source: str | None = None,
        cursor: str | None = None,
        limit: int = 200,
        full: bool = False,
    ) -> Response:
        if (gone := await missing(task_id)) is not None:
            return gone
        return payload(await provider.get_task_logs(task_id, source, cursor, limit, full))

    @router.get("/api/v1/tasks/{task_id}/debug", tags=["admin"])
    async def task_debug(task_id: str) -> Response:
        return await missing(task_id) or JSONResponse(await provider.get_task_debug(task_id))

    @router.get("/api/v1/tasks/{task_id}/stream", tags=["admin"])
    async def task_stream(
        request: Request, task_id: str, source: str | None = None, cursor: str | None = None
    ) -> Response:
        """SSE: one `data:` event per entry; `event: done` once the pager reports done.

        The last entry of each page carries `id: <next_cursor>`: a client whose stream dropped
        reconnects with `?cursor=<last id>` and misses nothing."""
        if (gone := await missing(task_id)) is not None:
            return gone

        follower = provider.log_follower(task_id)
        if follower is None:
            return JSONResponse(_UNAVAILABLE, status_code=503)

        async def events() -> AsyncIterator[str]:
            position = cursor
            last_sent = time.monotonic()
            while not await request.is_disconnected():
                page = await follower.page(source, position, STREAM_LIMIT)
                if "error" in page:
                    yield sse(page, "error")
                    return
                position = page["next_cursor"]
                last = len(page["entries"]) - 1
                for n, entry in enumerate(page["entries"]):
                    yield sse(entry, event_id=position if n == last else None)
                if page["done"]:
                    yield sse({"next_cursor": position}, "done")
                    return
                if page["entries"]:
                    last_sent = time.monotonic()
                    continue
                await asyncio.sleep(poll_s)
                if time.monotonic() - last_sent >= keepalive_s:
                    last_sent = time.monotonic()
                    yield KEEPALIVE

        headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        return StreamingResponse(events(), media_type="text/event-stream", headers=headers)

    @router.get("/api/v1/tasks/{task_id}/bundle.tar.gz", tags=["admin"])
    async def task_bundle(task_id: str) -> Response:
        if (gone := await missing(task_id)) is not None:
            return gone
        built = await provider.task_bundle(task_id)
        if built is None:
            return JSONResponse({"error": f"task {task_id} not found"}, status_code=404)
        tmp, archive = built
        return _TempFileResponse(
            archive,
            tmp,
            media_type="application/gzip",
            headers={"Content-Disposition": f'attachment; filename="{task_id}.tar.gz"'},
        )

    @router.get("/api/v1/server/logs", tags=["admin"])
    async def server_logs(
        cursor: str | None = None, limit: int = 200, full: bool = False
    ) -> Response:
        return payload(await provider.server_logs(cursor, limit, full))

    return router
