"""``/metrics/time``: where a task's time went — queued, running, and three kinds of wait.

Waits are intervals between events: each starts at an event and ends at the first later event of
the task whose type ends it, else at the task's ``completed_at``, else now (ongoing, ``partial``,
counted only when the window reaches now). A wait counts at the time it ended, so a closed bucket
does not change afterwards. Rate-limit wait is measured the same way: seconds actually waited,
not the seconds the decider charged when it scheduled the wait.
"""

import json
import time
from collections.abc import Callable, Sequence
from typing import Any

import aiosqlite

from src.engine import TaskEngine
from src.recovery import RATE_LIMITED, RATE_LIMITED_FALLBACK
from src.reports.aggregate import Contribution, Stat, aggregate
from src.reports.metrics_usage import open_attempt_rows, task_keys
from src.reports.query import Bucket, Filters, where

GROUP_BY = ("agent", "project", "kind", "status")
COLUMNS = ("queued_s", "running_s", "human_wait_s", "rate_limit_wait_s", "backoff_wait_s")
_RATE_LIMIT_WHY = frozenset({RATE_LIMITED, RATE_LIMITED_FALLBACK})
_RELAUNCH_ENDS = frozenset({"attempt_started", "cancelled"})
_HUMAN_ENDS = frozenset({"answered", "cancelled"})
_EVENT_TYPES = (
    "attempt_finished", "answered", "cancelled", "retry_scheduled", "sweep_requeued",
    "attempt_started",
)  # fmt: skip

Event = dict[str, Any]


def wait_intervals(
    events: Sequence[Event],
    *,
    starts: Callable[[Event], bool],
    ends: frozenset[str],
    completed_at: float | None,
    now: float,
    scheduled_end: Callable[[Event], float | None] | None = None,
) -> list[tuple[float, float, bool]]:
    """``(start, end, ongoing)`` per start event of one task's ``events`` (in order). A wait
    with a ``scheduled_end`` (when the relaunch was due) ends there at the latest: a later
    relaunch waited for a free slot or a restart, not for the reason the wait was scheduled."""
    found = []
    for i, event in enumerate(events):
        if not starts(event):
            continue
        end = next((e["at"] for e in events[i + 1 :] if e["type"] in ends), None)
        if end is None and completed_at is not None:
            end = max(completed_at, event["at"])
        due = scheduled_end(event) if scheduled_end is not None else None
        if due is not None:
            due = max(due, event["at"])
            if end is None and due <= now:
                end = due
            elif end is not None:
                end = min(end, due)
        if end is None:
            found.append((event["at"], now, True))
        else:
            found.append((event["at"], end, False))
    return found


def _is_human(event: Event) -> bool:
    return bool(
        event["type"] == "attempt_finished" and event["data"].get("status") == "needs_human"
    )


def _is_rate_limit(event: Event) -> bool:
    return event["type"] == "retry_scheduled" and event["data"].get("why") in _RATE_LIMIT_WHY


def _is_backoff(event: Event) -> bool:
    return event["type"] == "sweep_requeued" or (
        event["type"] == "retry_scheduled" and event["data"].get("why") not in _RATE_LIMIT_WHY
    )


def _due(event: Event) -> float | None:
    """When a scheduled relaunch was due: ``retry_scheduled.at``, or ``sweep_requeued``'s
    ``next_attempt_at``."""
    due = event["data"].get("at", event["data"].get("next_attempt_at"))
    return float(due) if isinstance(due, int | float) else None


_Due = Callable[[Event], float | None] | None
_WAITS: tuple[tuple[str, Callable[[Event], bool], frozenset[str], _Due], ...] = (
    ("human_wait_s", _is_human, _HUMAN_ENDS, None),
    ("rate_limit_wait_s", _is_rate_limit, _RELAUNCH_ENDS, _due),
    ("backoff_wait_s", _is_backoff, _RELAUNCH_ENDS, _due),
)


def _inside(filters: Filters, at: float) -> bool:
    return (filters.since is None or at >= filters.since) and (
        filters.until is None or at < filters.until
    )


def _scope(filters: Filters) -> tuple[str, list[Any]]:
    """Condition on ``tasks t``: tasks that can have time in the window (created before its
    end, not finished before its start). Joined into each query, so no query binds one
    variable per task."""
    cond, args = where(filters, time_column=None)
    clauses = [cond]
    if filters.until is not None:
        clauses.append("t.created_at < ?")
        args.append(filters.until)
    if filters.since is not None:
        clauses.append("(t.completed_at IS NULL OR t.completed_at >= ?)")
        args.append(filters.since)
    return " AND ".join(clauses), args


async def _tasks(db: aiosqlite.Connection, filters: Filters) -> dict[str, Any]:
    scope, args = _scope(filters)
    async with db.execute(
        "SELECT t.id, t.agent, t.cwd, t.kind, t.status, t.created_at, t.completed_at "
        f"FROM tasks t WHERE {scope}",
        args,
    ) as cur:
        return {r["id"]: r for r in await cur.fetchall()}


async def _events(db: aiosqlite.Connection, filters: Filters) -> dict[str, list[Event]]:
    scope, args = _scope(filters)
    types = ",".join("?" * len(_EVENT_TYPES))
    found: dict[str, list[Event]] = {}
    async with db.execute(
        f"SELECT e.task_id, e.type, e.data_json, e.at FROM events e "
        f"JOIN tasks t ON t.id = e.task_id WHERE e.type IN ({types}) AND {scope} "
        "ORDER BY e.task_id, e.at, e.id",
        [*_EVENT_TYPES, *args],
    ) as cur:
        for r in await cur.fetchall():
            data = json.loads(r["data_json"]) if r["data_json"] else {}
            found.setdefault(r["task_id"], []).append(
                {"type": r["type"], "at": r["at"], "data": data}
            )
    return found


async def contributions(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters, now: float
) -> list[Contribution]:
    tasks = await _tasks(db, filters)
    found: list[Contribution] = []
    reaches_now = filters.until is None or now <= filters.until
    for task_id, events in (await _events(db, filters)).items():
        task = tasks[task_id]
        keys = task_keys(task)
        for column, starts, ends, due in _WAITS:
            for start, end, ongoing in wait_intervals(
                events,
                starts=starts,
                ends=ends,
                completed_at=task["completed_at"],
                now=now,
                scheduled_end=due,
            ):
                if ongoing and reaches_now:
                    found.append(Contribution(now, task_id, keys, {column: end - start}, True))
                elif not ongoing and _inside(filters, end):
                    found.append(Contribution(end, task_id, keys, {column: end - start}))
    scope, scope_args = _scope(filters)
    async with db.execute(
        "SELECT a.task_id, min(a.started_at) AS first_start FROM attempts a "
        f"JOIN tasks t ON t.id = a.task_id WHERE a.started_at IS NOT NULL AND {scope} "
        "GROUP BY a.task_id",
        scope_args,
    ) as cur:
        for r in await cur.fetchall():
            task = tasks[r["task_id"]]
            if _inside(filters, r["first_start"]):
                queued = r["first_start"] - task["created_at"]
                found.append(
                    Contribution(
                        r["first_start"], task["id"], task_keys(task), {"queued_s": queued}
                    )
                )
    cond, args = where(filters, time_column="a.ended_at")
    async with db.execute(
        "SELECT a.task_id, a.ended_at, a.running_s, t.agent, t.cwd, t.kind, t.status "
        f"FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.ended_at IS NOT NULL AND {cond}",
        args,
    ) as cur:
        for r in await cur.fetchall():
            found.append(
                Contribution(
                    r["ended_at"], r["task_id"], task_keys(r), {"running_s": r["running_s"]}
                )
            )
    for row in await open_attempt_rows(db, filters, now):
        if row["started_at"] is not None:
            running = now - row["started_at"]
            found.append(Contribution(now, row["id"], task_keys(row), {"running_s": running}, True))
    return found


async def time_rows(
    db: aiosqlite.Connection,
    engine: TaskEngine,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
    stat: Stat,
    now: float | None = None,
) -> list[dict[str, Any]]:
    at = time.time() if now is None else now
    return aggregate(
        await contributions(db, engine, filters, at),
        group_by=group_by, bucket=bucket, columns=COLUMNS, stat=stat, count_tasks=True,
    )  # fmt: skip
