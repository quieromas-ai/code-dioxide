"""The event log (``/events``) and the fields of event data a reports or content token sees.

Event data mixes ids and counts with free text (error messages, paths, the child's PATH, tool
denial messages). Outside admin scope each type shows only its listed fields: ids, numbers,
enums, tool names. A type with no entry shows ``{}``.
"""

import json
from collections.abc import Mapping
from typing import Any

import aiosqlite

from src.engine_read import iso_or_none
from src.reports.query import (
    Filters,
    QueryError,
    decode_cursor,
    encode_cursor,
    parse_limit,
    where,
)

_COUNTERS = frozenset({"resumes", "call_retries", "stage_retries", "rate_limit_waited_s"})
_KILL = frozenset({"n", "after_s", "running_s", "idle_s", "limit_s", "tool"})

EVENT_FIELDS: dict[str, frozenset[str]] = {
    "agents_reload_failed": frozenset(),
    "agents_reloaded": frozenset({"changed"}),
    "answer_not_applied": frozenset({"n", "k"}),
    "answered": frozenset({"k", "label", "mode", "source"}),
    "attempt_closed_at_boot": frozenset({"n", "status", "outcome", "reason"}),
    "attempt_finished": frozenset({"n", "outcome", "status", "cost_usd", "tokens"}),
    "attempt_started": frozenset({"n", "mode", "corrective", "cause"}),
    "auth_failed": frozenset({"n"}),
    "boot_recovered": frozenset({"n", "process", "action"}),
    "cancelled": frozenset({"from"}),
    "deduplicated": frozenset({"agent"}),
    "engine_error": frozenset(),
    "fresh_start": frozenset(
        {"n", "session_id", "previous", "previous_run_id", "plan_pinned", "dropped_answers"}
        | _COUNTERS
    ),
    "invalid_workflow_return": frozenset(),
    "kill_rerequested": frozenset({"n", "reason", "given_up"}),
    "kill_skipped": frozenset({"reason"}),
    "kill_skipped_no_start_time": frozenset(),
    "kill_unconfirmed": frozenset({"tries"}),
    "launch_failed": frozenset(),
    "lifecycle_prepared": frozenset(),
    "model_fallback": frozenset({"n", "from_to"}),
    "permission_denied": frozenset({"tool"}),
    "plan_pinned": frozenset({"label", "run_id"}),
    "plan_unpinned": frozenset({"run_id"}),
    "retry_scheduled": frozenset({"n", "why", "at", "corrective"} | _COUNTERS),
    "run_archived": frozenset({"run_id"}),
    "run_started": frozenset({"run_id"}),
    "similar_active_task": frozenset({"similar"}),
    "spawn_failed": frozenset(),
    "spawned": frozenset({"n"}),
    "stage_finished": frozenset(
        {"label", "agent_type", "model", "status", "started_at", "finished_at"}
    ),
    "stage_started": frozenset({"label", "agent_type", "model", "started_at"}),
    "submitted": frozenset({"agent"}),
    "sweep": frozenset({"policy", "trigger", "survivors_unconfirmed"}),
    "sweep_requeued": frozenset({"next_attempt_at", "streak"}),
    "sweep_survivors": frozenset(),
    "sweep_unconfirmed": frozenset({"trigger", "streak"}),
    "workflow_script_failed": frozenset({"status"}),
    # kills, named by their reason
    "cancel": _KILL,
    "stall": _KILL,
    "timeout": _KILL,
    "call_mismatch": _KILL,
    "second_workflow_call": _KILL,
    "parent_tool_violation": _KILL,
    "parent_tool_after_completion": _KILL,
    "exit_after_completion": _KILL,
    "workflow_call_after_completion": _KILL,
}


def public_data(type_: str, data: Mapping[str, Any]) -> dict[str, Any]:
    """``data`` reduced to the fields listed for ``type_`` (none for an unlisted type)."""
    fields = EVENT_FIELDS.get(type_, frozenset())
    return {k: v for k, v in data.items() if k in fields}


def parse_types(value: str | None) -> list[str] | None:
    if value is None:
        return None
    types = [t.strip() for t in value.split(",") if t.strip()]
    if not types:
        raise QueryError("type must name at least one event type")
    return types


async def select_events(
    db: aiosqlite.Connection,
    filters: Filters,
    *,
    types: list[str] | None = None,
    after: int | None = None,
    limit: int,
    full: bool,
) -> list[dict[str, Any]]:
    """Up to ``limit`` events after id ``after``, ascending by id, in the filters' window;
    ``full`` (admin) keeps all of the data, else ``public_data``."""
    cond, args = where(filters, time_column="e.at")
    if types is not None:
        cond += f" AND e.type IN ({','.join('?' * len(types))})"
        args += types
    if after is not None:
        cond += " AND e.id > ?"
        args.append(after)
    # Tasks are joined only when a task column is filtered: engine events have no task.
    task_filtered = any(
        v is not None
        for v in (filters.agent, filters.task, filters.project, filters.status, filters.kind)
    )
    join = "JOIN tasks t ON t.id = e.task_id " if task_filtered else ""
    async with db.execute(
        f"SELECT e.id, e.at, e.task_id, e.type, e.data_json FROM events e {join}WHERE {cond} "
        "ORDER BY e.id LIMIT ?",
        [*args, limit],
    ) as cur:
        found = await cur.fetchall()
    rows = []
    for r in found:
        data = _loads(r["data_json"])
        rows.append(
            {"id": r["id"], "at": iso_or_none(r["at"]), "task": r["task_id"], "type": r["type"],
             "data": data if full else public_data(r["type"], data)}
        )  # fmt: skip
    return rows


async def max_event_id(db: aiosqlite.Connection) -> int:
    async with db.execute("SELECT coalesce(max(id), 0) AS id FROM events") as cur:
        row = await cur.fetchone()
    return int(row["id"]) if row is not None else 0


async def events(
    db: aiosqlite.Connection,
    filters: Filters,
    types: list[str] | None,
    params: Mapping[str, str],
    *,
    full: bool,
) -> dict[str, Any]:
    """``/events``: a page of ``select_events``; the cursor is the last id."""
    limit = parse_limit(params.get("limit"))
    after = None
    if (cursor := params.get("cursor")) is not None:
        found = decode_cursor(cursor)
        if len(found) != 1 or type(found[0]) is not int:
            raise QueryError("invalid cursor")
        after = found[0]
    rows = await select_events(db, filters, types=types, after=after, limit=limit + 1, full=full)
    more = len(rows) > limit
    rows = rows[:limit]
    return {"rows": rows, "next_cursor": encode_cursor([rows[-1]["id"]]) if more else None}


def _loads(raw: str | None) -> dict[str, Any]:
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}
