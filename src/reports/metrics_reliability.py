"""``/metrics/reliability``: how tasks and attempts ended, and what recovery it took.

Each figure counts at the time of its own fact: ``submitted`` at task creation, outcomes at
``completed_at``, per-attempt figures (``attempts``, ``resumes``, ``fresh_restarts``,
``killed``) at the attempt's end (the open attempt now, ``partial``), event counts at the event.
So per bucket ``attempts`` = first attempts + ``resumes`` + ``fresh_restarts``, and equals the
``attempts`` of ``/metrics/usage``.
"""

import time
from typing import Any

import aiosqlite

from src.engine import TaskEngine
from src.recovery import CORRECTIVE, KILL_REASONS, STAGE_FAILED
from src.reports.aggregate import Contribution, aggregate
from src.reports.metrics_usage import open_attempt_rows, task_keys
from src.reports.query import Bucket, Filters, where

GROUP_BY = ("agent", "project", "kind")
COLUMNS = (
    "submitted", "completed", "failed", "cancelled", "attempts", "call_retries", "stage_retries",
    "resumes", "fresh_restarts", "tasks_with_fallback", "killed", "auth_failed",
    "permission_denied",
)  # fmt: skip
_OUTCOMES = ("completed", "failed", "cancelled")
_EVENT_COLUMNS = {"auth_failed": "auth_failed", "permission_denied": "permission_denied"}
_RETRY_COLUMNS = {CORRECTIVE: "call_retries", STAGE_FAILED: "stage_retries"}
_MODE_COLUMNS = {"resume": "resumes", "fresh": "fresh_restarts"}
_TASK_COLUMNS = "t.agent, t.cwd, t.kind, t.status"


def _attempt_values(mode: str | None, reason: str | None) -> dict[str, int]:
    values = {"attempts": 1, "killed": int(reason in KILL_REASONS)}
    if mode in _MODE_COLUMNS:
        values[_MODE_COLUMNS[mode]] = 1
    return values


async def contributions(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters, now: float
) -> list[Contribution]:
    found: list[Contribution] = []
    cond, args = where(filters, time_column="t.created_at")
    async with db.execute(
        f"SELECT t.id, t.created_at, {_TASK_COLUMNS} FROM tasks t WHERE {cond}", args
    ) as cur:
        for r in await cur.fetchall():
            found.append(Contribution(r["created_at"], r["id"], task_keys(r), {"submitted": 1}))
    cond, args = where(filters, time_column="t.completed_at")
    outcomes = ",".join("?" * len(_OUTCOMES))
    async with db.execute(
        f"SELECT t.id, t.completed_at, {_TASK_COLUMNS} FROM tasks t "
        f"WHERE t.status IN ({outcomes}) AND t.completed_at IS NOT NULL AND {cond}",
        [*_OUTCOMES, *args],
    ) as cur:
        for r in await cur.fetchall():
            found.append(Contribution(r["completed_at"], r["id"], task_keys(r), {r["status"]: 1}))
    cond, args = where(filters, time_column="a.ended_at")
    async with db.execute(
        f"SELECT a.task_id, a.ended_at, a.reason, {_TASK_COLUMNS}, "
        "(SELECT json_extract(e.data_json, '$.mode') FROM events e WHERE e.task_id = a.task_id "
        "AND e.type = 'attempt_started' AND json_extract(e.data_json, '$.n') = a.n "
        "ORDER BY e.id DESC LIMIT 1) AS mode "
        f"FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.ended_at IS NOT NULL AND {cond}",
        args,
    ) as cur:
        for r in await cur.fetchall():
            values = _attempt_values(r["mode"], r["reason"])
            found.append(Contribution(r["ended_at"], r["task_id"], task_keys(r), values))
    for row in await open_attempt_rows(db, filters, now):
        values = _attempt_values(row["mode"], None)
        found.append(Contribution(now, row["id"], task_keys(row), values, partial=True))
    found += await _event_contributions(db, filters)
    return found


async def _event_contributions(db: aiosqlite.Connection, filters: Filters) -> list[Contribution]:
    found: list[Contribution] = []
    cond, args = where(filters, time_column="e.at")
    types = ["retry_scheduled", *_EVENT_COLUMNS]
    async with db.execute(
        f"SELECT e.task_id, e.type, e.at, json_extract(e.data_json, '$.why') AS why, "
        f"{_TASK_COLUMNS} FROM events e JOIN tasks t ON t.id = e.task_id "
        f"WHERE e.type IN ({','.join('?' * len(types))}) AND {cond}",
        [*types, *args],
    ) as cur:
        for r in await cur.fetchall():
            column = (
                _RETRY_COLUMNS.get(r["why"])
                if r["type"] == "retry_scheduled"
                else _EVENT_COLUMNS[r["type"]]
            )
            if column is not None:
                found.append(Contribution(r["at"], r["task_id"], task_keys(r), {column: 1}))
    # A task counts once, at its first fallback (which may lie before the window: then not here).
    cond, args = where(filters, time_column=None)
    async with db.execute(
        f"SELECT e.task_id, min(e.at) AS at, {_TASK_COLUMNS} FROM events e "
        f"JOIN tasks t ON t.id = e.task_id WHERE e.type = 'model_fallback' AND {cond} "
        "GROUP BY e.task_id",
        args,
    ) as cur:
        for r in await cur.fetchall():
            if (filters.since is None or r["at"] >= filters.since) and (
                filters.until is None or r["at"] < filters.until
            ):
                keys = task_keys(r)
                found.append(Contribution(r["at"], r["task_id"], keys, {"tasks_with_fallback": 1}))
    return found


async def reliability_rows(
    db: aiosqlite.Connection,
    engine: TaskEngine,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
    now: float | None = None,
) -> list[dict[str, Any]]:
    at = time.time() if now is None else now
    rows = aggregate(
        await contributions(db, engine, filters, at),
        group_by=group_by, bucket=bucket, columns=COLUMNS,
    )  # fmt: skip
    for row in rows:
        for column in COLUMNS:
            row[column] = row[column] or 0  # a count, never null
    return rows
