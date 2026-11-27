"""Entities: what exists (attempts of a task, agents, projects), as flat paged rows."""

from collections.abc import Mapping
from contextlib import suppress
from typing import Any

import aiosqlite

from src import usage
from src.engine import TaskEngine
from src.engine_read import iso_or_none
from src.mcp_server import roles_by_agent
from src.reports.query import (
    Filters,
    QueryError,
    decode_cursor,
    encode_cursor,
    parse_limit,
    where,
)


class NotFound(LookupError):
    """The route's subject does not exist (404)."""


def page(
    rows: list[dict[str, Any]], key: str, key_type: type, params: Mapping[str, str]
) -> dict[str, Any]:
    """Keyset page over ``rows`` sorted by ``key`` (unique, of ``key_type``): the rows after
    the cursor's key."""
    limit = parse_limit(params.get("limit"))
    if (cursor := params.get("cursor")) is not None:
        key_values = decode_cursor(cursor)
        if len(key_values) != 1 or type(key_values[0]) is not key_type:
            raise QueryError("invalid cursor")
        rows = [r for r in rows if r[key] > key_values[0]]
    more = len(rows) > limit
    rows = rows[:limit]
    return {"rows": rows, "next_cursor": encode_cursor([rows[-1][key]]) if more else None}


def resolve_agent(engine: TaskEngine, name: str) -> str:
    with suppress(KeyError):
        return engine.resolve(name)
    return name


async def attempts(
    db: aiosqlite.Connection, task_id: str, params: Mapping[str, str]
) -> dict[str, Any]:
    async with db.execute("SELECT 1 FROM tasks WHERE id = ?", (task_id,)) as cur:
        if await cur.fetchone() is None:
            raise NotFound(f"task {task_id} not found")
    async with db.execute(
        "SELECT a.n, a.session_id, a.started_at, a.ended_at, a.outcome, a.reason, a.cost_usd, "
        "a.tokens, a.running_s, (SELECT json_extract(e.data_json, '$.mode') FROM events e "
        "WHERE e.task_id = a.task_id AND e.type = 'attempt_started' "
        "AND json_extract(e.data_json, '$.n') = a.n ORDER BY e.id DESC LIMIT 1) AS mode, "
        "m.input_tokens, m.output_tokens, m.cache_read_tokens, m.cache_write_tokens "
        "FROM attempts a LEFT JOIN (SELECT task_id, n, sum(input_tokens) AS input_tokens, "
        "sum(output_tokens) AS output_tokens, sum(cache_read_tokens) AS cache_read_tokens, "
        "sum(cache_write_tokens) AS cache_write_tokens FROM attempt_models WHERE task_id = ? "
        "GROUP BY task_id, n) m ON m.task_id = a.task_id AND m.n = a.n "
        "WHERE a.task_id = ? ORDER BY a.n",
        (task_id, task_id),
    ) as cur:
        found = await cur.fetchall()
    uncounted = await usage.uncounted_attempt_keys(db, [task_id])
    rows = [
        {
            "n": r["n"], "session_id": r["session_id"], "mode": r["mode"],
            "started_at": iso_or_none(r["started_at"]), "ended_at": iso_or_none(r["ended_at"]),
            "outcome": r["outcome"], "reason": r["reason"], "cost_usd": r["cost_usd"],
            "tokens": r["tokens"], "input_tokens": r["input_tokens"],
            "output_tokens": r["output_tokens"], "cache_read_tokens": r["cache_read_tokens"],
            "cache_write_tokens": r["cache_write_tokens"], "running_s": r["running_s"],
            "uncounted": (task_id, r["n"]) in uncounted,
        }
        for r in found
    ]  # fmt: skip
    return page(rows, "n", int, params)


async def _task_counts(
    db: aiosqlite.Connection, filters: Filters, group: str
) -> dict[str, dict[str, Any]]:
    """Per ``group`` column value: tasks created in the window, their last activity and last
    creation time, tasks per status."""
    cond, args = where(filters, time_column="t.created_at")
    async with db.execute(
        f"SELECT {group} AS k, t.status, count(*) AS n, "
        "max(max(coalesce(t.started_at, 0), coalesce(t.completed_at, 0))) AS last, "
        "max(t.created_at) AS created "
        f"FROM tasks t WHERE {cond} AND {group} IS NOT NULL GROUP BY {group}, t.status",
        args,
    ) as cur:
        found = await cur.fetchall()
    by_key: dict[str, dict[str, Any]] = {}
    for r in found:
        entry = by_key.setdefault(
            r["k"], {"total": 0, "last": 0.0, "created": 0.0, "by_status": {}}
        )
        entry["created"] = max(entry["created"], r["created"] or 0.0)
        entry["total"] += r["n"]
        entry["last"] = max(entry["last"], r["last"] or 0.0)
        entry["by_status"][r["status"]] = r["n"]
    return by_key


async def _running(db: aiosqlite.Connection, filters: Filters, group: str) -> dict[str, int]:
    """Running tasks per ``group`` value now (no window: running is a current state)."""
    current = Filters(agent=filters.agent, project=filters.project, kind=filters.kind)
    cond, args = where(current, time_column=None)
    async with db.execute(
        f"SELECT {group} AS k, count(*) AS n FROM tasks t "
        f"WHERE {cond} AND t.status = 'running' AND {group} IS NOT NULL GROUP BY {group}",
        args,
    ) as cur:
        return {r["k"]: int(r["n"]) for r in await cur.fetchall()}


async def _agent_rows(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters
) -> list[dict[str, Any]]:
    await engine.refresh_agents()
    roles = roles_by_agent(engine.roles)
    counts = await _task_counts(db, filters, "t.agent")
    running = await _running(db, filters, "t.agent")
    rows = []
    for slug in sorted(engine.agents):
        entry = counts.get(slug, {"total": 0, "last": 0.0, "by_status": {}})
        rows.append(
            {
                "id": slug,
                "role": ",".join(roles.get(slug, [])),
                "model": engine.agents[slug].model,
                "status": "busy" if running.get(slug) else "idle",
                "running_tasks": running.get(slug, 0),
                "tasks_total": entry["total"],
                "last_active_at": iso_or_none(entry["last"] or None),
                "tasks_by_status": entry["by_status"],
            }
        )
    return rows


async def agents(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters, params: Mapping[str, str]
) -> dict[str, Any]:
    rows = await _agent_rows(db, engine, filters)
    for row in rows:
        del row["tasks_by_status"]
    return page(rows, "id", str, params)


async def agent(
    db: aiosqlite.Connection, engine: TaskEngine, name: str, filters: Filters
) -> dict[str, Any]:
    slug = resolve_agent(engine, name)
    for row in await _agent_rows(db, engine, filters):
        if row["id"] == slug:
            return row
    raise NotFound(f"agent {name} not found")


async def projects(
    db: aiosqlite.Connection, filters: Filters, params: Mapping[str, str]
) -> dict[str, Any]:
    counts = await _task_counts(db, filters, "t.cwd")
    running = await _running(db, filters, "t.cwd")
    rows = [
        {
            "project": project,
            "tasks_total": entry["total"],
            "running": running.get(project, 0),
            "last_task_at": iso_or_none(entry["created"] or None),
        }
        for project, entry in sorted(counts.items())
    ]
    return page(rows, "project", str, params)
