"""The reporting routes under ``/api/v1``, grouped by family (each route's OpenAPI tag).

``GET /api/v1`` lists the families and their routes, generated from the app's own routes, so
the index cannot drift from what is served.
"""

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from src.auth import Access, access_of
from src.engine import TaskEngine
from src.engine_read import iso_or_none
from src.mcp_server import ToolProvider
from src.reports import (
    activity,
    content,
    entities,
    events,
    metrics_overhead,
    metrics_reliability,
    metrics_savings,
    metrics_stages,
    metrics_time,
    metrics_usage,
    stages,
)
from src.reports.aggregate import parse_stat
from src.reports.db import get_read_db
from src.reports.entities import NotFound
from src.reports.query import (
    Bucket,
    Filters,
    QueryError,
    check_params,
    parse_bucket,
    parse_filters,
    parse_group_by,
    parse_limit,
)
from src.sse import KEEPALIVE, sse
from src.summaries import NoSlot, SummariesOff, SummaryFailed, get_summary

FAMILIES = {
    "entities": "what exists: tasks, attempts, agents, projects",
    "metrics": "numbers over time: usage, time, reliability",
    "activity": "what is happening now",
    "content": "text: prompts, results, transcripts, summaries",
}
_UNAVAILABLE = {"error": "engine not started"}
KEEPALIVE_S = 15.0
POLL_S = 1.0
STREAM_BATCH = 500


def unavailable() -> JSONResponse:
    return JSONResponse(_UNAVAILABLE, status_code=503)


def redacted(engine: TaskEngine, payload: dict[str, Any]) -> JSONResponse:
    out: dict[str, Any] = engine.redactor.redact_json(payload)
    return JSONResponse(out)


async def _query_error(_: Request, exc: Exception) -> Response:
    return JSONResponse({"error": str(exc)}, status_code=400)


async def _not_found(_: Request, exc: Exception) -> Response:
    return JSONResponse({"error": str(exc)}, status_code=404)


def _params(request: Request) -> dict[str, str]:
    return dict(request.query_params)


FILTER_PARAMS = frozenset({"agent", "task", "project", "status", "kind", "since", "until",
                           "stage_agent"})  # fmt: skip
LIST_PARAMS = frozenset({"limit", "cursor"})
METRIC_PARAMS = FILTER_PARAMS | {"group_by", "bucket"}


def _index(app: FastAPI) -> dict[str, Any]:
    families: dict[str, Any] = {
        family: {"description": description, "routes": []}
        for family, description in FAMILIES.items()
    }
    for path, operations in app.openapi()["paths"].items():
        for method, operation in operations.items():
            for tag in operation.get("tags", []):
                if tag in families:
                    families[tag]["routes"].append(
                        {"method": method.upper(), "path": path,
                         "summary": operation.get("summary", "")}
                    )  # fmt: skip
    return {"families": families}


def metric_body(
    rows: list[dict[str, Any]], filters: Filters, group_by: tuple[str, ...], bucket: Bucket
) -> dict[str, Any]:
    window = {"since": iso_or_none(filters.since), "until": iso_or_none(filters.until)}
    return {"rows": rows, "window": window, "group_by": list(group_by), "bucket": bucket}


def start_event_id(request: Request) -> int | None:
    """Where an activity stream resumes: the ``Last-Event-ID`` header (an EventSource
    reconnecting) or ``?last_event_id=``; None when neither is given."""
    raw = request.headers.get("last-event-id", request.query_params.get("last_event_id"))
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        raise QueryError(f"invalid last_event_id {raw!r}: an event id (integer)") from None


def build_reports_router(
    provider: ToolProvider, keepalive_s: float = KEEPALIVE_S, poll_s: float = POLL_S
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1", summary="families and their routes")
    async def index(request: Request) -> dict[str, Any]:
        return _index(request.app)

    def filters(
        request: Request,
        engine: TaskEngine,
        *,
        windowed: bool,
        now: float | None = None,
        stage_agent: bool = False,
    ) -> Filters:
        at = time.time() if now is None else now
        found = parse_filters(_params(request), now=at, windowed=windowed, stage_agent=stage_agent)
        if found.agent is not None:
            found = replace(found, agent=entities.resolve_agent(engine, found.agent))
        return found

    @router.get("/api/v1/tasks/{task_id}/attempts", tags=["entities"], summary="a task's attempts")
    async def task_attempts(request: Request, task_id: str) -> Response:
        check_params(request.query_params, LIST_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await entities.attempts(get_read_db(), task_id, _params(request)))

    @router.get("/api/v1/tasks/{task_id}/stages", tags=["entities"], summary="a task's stages")
    async def task_stages(request: Request, task_id: str) -> Response:
        check_params(request.query_params, LIST_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await stages.stages(get_read_db(), task_id, _params(request)))

    @router.get(
        "/api/v1/tasks/{task_id}/graph", tags=["entities"], summary="a task's workflow graph"
    )
    async def task_graph(request: Request, task_id: str) -> Response:
        check_params(request.query_params, frozenset())
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await stages.graph(get_read_db(), task_id))

    @router.get("/api/v1/agents", tags=["entities"], summary="agents and their task counts")
    async def agents(request: Request) -> Response:
        check_params(request.query_params, FILTER_PARAMS | LIST_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        found = filters(request, engine, windowed=True)
        return redacted(
            engine, await entities.agents(get_read_db(), engine, found, _params(request))
        )

    @router.get("/api/v1/agents/{agent_id}", tags=["entities"], summary="one agent")
    async def agent(request: Request, agent_id: str) -> Response:
        check_params(request.query_params, FILTER_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        found = filters(request, engine, windowed=True)
        return redacted(engine, await entities.agent(get_read_db(), engine, agent_id, found))

    @router.get("/api/v1/projects", tags=["entities"], summary="projects (working directories)")
    async def projects(request: Request) -> Response:
        check_params(request.query_params, FILTER_PARAMS | LIST_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        found = filters(request, engine, windowed=True)
        return redacted(engine, await entities.projects(get_read_db(), found, _params(request)))

    @router.get(
        "/api/v1/metrics/usage", tags=["metrics"], summary="cost and tokens by attempt end time"
    )
    async def metrics_usage_route(request: Request) -> Response:
        check_params(request.query_params, METRIC_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        now = time.time()  # one instant: the window's default end and the open attempts' time
        found = filters(request, engine, windowed=True, now=now)
        group_by = parse_group_by(params.get("group_by"), metrics_usage.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        rows = await metrics_usage.usage_rows(
            get_read_db(), engine, found, group_by, bucket, now=now
        )
        return redacted(engine, metric_body(rows, found, group_by, bucket))

    @router.get(
        "/api/v1/metrics/time", tags=["metrics"], summary="queued, running and waiting seconds"
    )
    async def metrics_time_route(request: Request) -> Response:
        check_params(request.query_params, METRIC_PARAMS | {"stat"})
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        now = time.time()
        found = filters(request, engine, windowed=True, now=now)
        group_by = parse_group_by(params.get("group_by"), metrics_time.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        stat = parse_stat(params.get("stat"))
        rows = await metrics_time.time_rows(
            get_read_db(), engine, found, group_by, bucket, stat, now=now
        )
        body = metric_body(rows, found, group_by, bucket)
        return redacted(engine, {**body, "stat": stat})

    @router.get(
        "/api/v1/metrics/reliability", tags=["metrics"], summary="outcomes, retries and failures"
    )
    async def metrics_reliability_route(request: Request) -> Response:
        check_params(request.query_params, METRIC_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        now = time.time()
        found = filters(request, engine, windowed=True, now=now)
        group_by = parse_group_by(params.get("group_by"), metrics_reliability.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        rows = await metrics_reliability.reliability_rows(
            get_read_db(), engine, found, group_by, bucket, now=now
        )
        return redacted(engine, metric_body(rows, found, group_by, bucket))

    @router.get(
        "/api/v1/metrics/stages", tags=["metrics"], summary="stage tokens, tool calls, duration"
    )
    async def metrics_stages_route(request: Request) -> Response:
        check_params(request.query_params, METRIC_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        now = time.time()
        found = filters(request, engine, windowed=True, now=now, stage_agent=True)
        group_by = parse_group_by(params.get("group_by"), metrics_stages.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        rows = await metrics_stages.stage_rows(get_read_db(), found, group_by, bucket, now=now)
        return redacted(engine, metric_body(rows, found, group_by, bucket))

    @router.get(
        "/api/v1/metrics/savings", tags=["metrics"], summary="cached stages and deduplicated calls"
    )
    async def metrics_savings_route(request: Request) -> Response:
        check_params(request.query_params, METRIC_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        found = filters(request, engine, windowed=True, stage_agent=True)
        group_by = parse_group_by(params.get("group_by"), metrics_savings.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        rows = await metrics_savings.savings_rows(get_read_db(), found, group_by, bucket)
        return redacted(engine, metric_body(rows, found, group_by, bucket))

    @router.get("/api/v1/activity", tags=["activity"], summary="running and waiting tasks now")
    async def activity_now(request: Request) -> Response:
        check_params(request.query_params, FILTER_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        now = time.time()
        found = filters(request, engine, windowed=False, now=now, stage_agent=True)
        rows = await activity.activity_rows(get_read_db(), engine, found, now)
        return redacted(engine, {"rows": rows})

    @router.get(
        "/api/v1/activity/stream", tags=["activity"], summary="activity and events over SSE"
    )
    async def activity_stream(request: Request) -> Response:
        """SSE: first ``ready`` (``id:`` the start position), one ``progress`` per activity
        row, then each new event (``event: event``, ``id:`` its id) and a ``progress`` per
        changed row (``{"task", "removed": true}`` for a row gone). A reconnect with
        ``Last-Event-ID`` misses no event and repeats none."""
        check_params(request.query_params, FILTER_PARAMS | {"last_event_id"})
        if (engine := provider.engine) is None:
            return unavailable()
        start = start_event_id(request)
        found = filters(request, engine, windowed=False, stage_agent=True)
        full = access_of(request.scope) is Access.admin
        db = get_read_db()
        after = await events.max_event_id(db) if start is None else start
        redact = engine.redactor.redact_json

        async def stream() -> AsyncIterator[str]:
            # The start position as the first id: a client that reconnects before any event
            # arrived sends it back as Last-Event-ID and misses nothing written meanwhile.
            yield sse({"after": after}, "ready", str(after))
            last = after
            shown: dict[str, dict[str, Any]] = {}
            last_sent = time.monotonic()
            while not await request.is_disconnected():
                sent = False
                rows = await activity.activity_rows(db, engine, found, time.time())
                current = {r["task"]: r for r in rows}
                for task_id, row in current.items():
                    if shown.get(task_id) != row:
                        yield sse(redact(row), "progress")
                        sent = True
                for task_id in shown.keys() - current.keys():
                    yield sse({"task": task_id, "removed": True}, "progress")
                    sent = True
                shown = current
                while batch := await events.select_events(
                    db, found, after=last, limit=STREAM_BATCH, full=full
                ):
                    for event in batch:
                        yield sse(redact(event), "event", str(event["id"]))
                    last = batch[-1]["id"]
                    sent = True
                if sent:
                    last_sent = time.monotonic()
                elif time.monotonic() - last_sent >= keepalive_s:
                    last_sent = time.monotonic()
                    yield KEEPALIVE
                await asyncio.sleep(poll_s)

        headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        return StreamingResponse(stream(), media_type="text/event-stream", headers=headers)

    @router.get("/api/v1/events", tags=["activity"], summary="the event log")
    async def event_log(request: Request) -> Response:
        check_params(request.query_params, FILTER_PARAMS | LIST_PARAMS | {"type"})
        if (engine := provider.engine) is None:
            return unavailable()
        found = filters(request, engine, windowed=True)
        types = events.parse_types(request.query_params.get("type"))
        full = access_of(request.scope) is Access.admin
        body = await events.events(get_read_db(), found, types, _params(request), full=full)
        return redacted(engine, body)

    @router.get("/api/v1/agents/{agent_id}/prompt", tags=["content"], summary="an agent's prompt")
    async def agent_prompt(request: Request, agent_id: str) -> Response:
        check_params(request.query_params, frozenset())
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await content.agent_prompt(engine, agent_id))

    @router.get("/api/v1/tasks/{task_id}/prompts", tags=["content"], summary="a task's prompts")
    async def task_prompts(request: Request, task_id: str) -> Response:
        check_params(request.query_params, frozenset())
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await content.task_prompts(engine, get_read_db(), task_id))

    @router.get(
        "/api/v1/tasks/{task_id}/events", tags=["content"], summary="a task's events, full data"
    )
    async def task_events(request: Request, task_id: str) -> Response:
        check_params(request.query_params, LIST_PARAMS | {"type"})
        if (engine := provider.engine) is None:
            return unavailable()
        types = events.parse_types(request.query_params.get("type"))
        body = await content.task_events(get_read_db(), task_id, types, _params(request))
        return redacted(engine, body)

    @router.get(
        "/api/v1/tasks/{task_id}/stages/{label}/result", tags=["content"],
        summary="a stage's result",
    )  # fmt: skip
    async def stage_result(request: Request, task_id: str, label: str) -> Response:
        check_params(request.query_params, frozenset())
        if (engine := provider.engine) is None:
            return unavailable()
        return redacted(engine, await content.stage_result(engine, get_read_db(), task_id, label))

    @router.get(
        "/api/v1/tasks/{task_id}/stages/{label}/transcript", tags=["content"],
        summary="a stage's transcript, paged",
    )  # fmt: skip
    async def stage_transcript(request: Request, task_id: str, label: str) -> Response:
        check_params(request.query_params, LIST_PARAMS)
        if (engine := provider.engine) is None:
            return unavailable()
        limit = parse_limit(request.query_params.get("limit"))
        cursor = request.query_params.get("cursor")
        page = await content.stage_transcript(engine, get_read_db(), task_id, label, cursor, limit)
        return redacted(engine, page)

    @router.get("/api/v1/tasks/{task_id}/summary", tags=["content"], summary="a task summary")
    async def task_summary(request: Request, task_id: str) -> Response:
        check_params(request.query_params, frozenset())
        if (engine := provider.engine) is None:
            return unavailable()
        try:
            body = await get_summary(engine, task_id)
        except SummariesOff as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except NoSlot as exc:
            return JSONResponse(
                {"error": "no free slot", "retry_after": exc.retry_after}, status_code=503,
                headers={"Retry-After": str(exc.retry_after)},
            )  # fmt: skip
        except SummaryFailed:
            return JSONResponse({"error": "summary failed"}, status_code=502)
        return redacted(engine, body)

    @router.get(
        "/api/v1/metrics/overhead", tags=["metrics"], summary="dioxide's own spend (summaries)"
    )
    async def metrics_overhead_route(request: Request) -> Response:
        check_params(
            request.query_params, frozenset({"task", "since", "until", "bucket", "group_by"})
        )
        if (engine := provider.engine) is None:
            return unavailable()
        params = _params(request)
        found = filters(request, engine, windowed=True)
        group_by = parse_group_by(params.get("group_by"), metrics_overhead.GROUP_BY)
        bucket = parse_bucket(params.get("bucket"))
        rows = await metrics_overhead.overhead_rows(get_read_db(), found, bucket)
        return redacted(engine, metric_body(rows, found, group_by, bucket))

    return router


def install_reports(app: FastAPI, provider: ToolProvider, **router: Any) -> None:
    """The reporting router (``router``: ``build_reports_router`` options) and its 400/404
    handlers."""
    app.include_router(build_reports_router(provider, **router))
    app.add_exception_handler(QueryError, _query_error)
    app.add_exception_handler(NotFound, _not_found)
