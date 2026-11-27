"""``/metrics/stages``: the stages' own numbers (context tokens, tool calls, duration) per stage.

One figure per step row: a completed stage at ``finished_at``, another ended one (failed, never
given a finish) at its last progress or start, a running stage of a running task now
(``partial``, only when the window reaches now). A row replayed from cache counts in
``cached_runs``; it keeps the numbers of the run that measured it, which counts in ``runs``
(a cached row without numbers, mirrored before they were recorded, adds only
``cached_runs``). ``stage_tokens`` is the stage's context size, not billed tokens: never add
it to ``/metrics/usage`` ``tokens``.
"""

import time
from typing import Any

import aiosqlite

from src.reports.aggregate import Contribution, aggregate
from src.reports.metrics_usage import in_window
from src.reports.query import Bucket, Filters, where
from src.stages import normalize_stage

GROUP_BY = ("stage", "stage_agent", "model", "project")
COLUMNS = ("stage_tokens", "tool_calls", "duration_s", "runs", "cached_runs")
_COUNTS = ("runs", "cached_runs")


async def contributions(
    db: aiosqlite.Connection, filters: Filters, now: float
) -> list[Contribution]:
    cond, args = where(filters, time_column=None)
    if filters.stage_agent is not None:
        cond += " AND s.agent_type = ?"
        args.append(filters.stage_agent)
    async with db.execute(
        "SELECT s.task_id, s.label, s.status, s.cached, s.agent_type, s.model, s.stage_tokens, "
        "s.tool_calls, s.duration_s, t.cwd, t.status AS task_status, "
        "coalesce(s.finished_at, s.last_progress_at, s.started_at) AS ended_at "
        f"FROM steps s JOIN tasks t ON t.id = s.task_id WHERE {cond}",
        args,
    ) as cur:
        found = await cur.fetchall()
    out: list[Contribution] = []
    for r in found:
        running = r["status"] == "running" and r["task_status"] == "running"
        at = now if running else r["ended_at"]
        if at is None or not in_window(filters, at) or (not running and at == filters.until):
            continue
        keys = {
            "stage": normalize_stage(r["label"]), "stage_agent": r["agent_type"],
            "model": r["model"], "project": r["cwd"],
        }  # fmt: skip
        # One row per label: a replay served from cache keeps the numbers of the run that
        # measured the stage, so that run still counts; numbers never come from the replay.
        numbers = {
            "stage_tokens": r["stage_tokens"], "tool_calls": r["tool_calls"],
            "duration_s": r["duration_s"],
        }  # fmt: skip
        measured = not r["cached"] or any(v is not None for v in numbers.values())
        values: dict[str, float | int | None] = {"cached_runs": int(bool(r["cached"]))}
        if measured:
            values |= {"runs": 1, **numbers}
        out.append(Contribution(at, r["task_id"], keys, values, partial=running))
    return out


async def stage_rows(
    db: aiosqlite.Connection,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
    now: float | None = None,
) -> list[dict[str, Any]]:
    at = time.time() if now is None else now
    rows = aggregate(
        await contributions(db, filters, at),
        group_by=group_by, bucket=bucket, columns=COLUMNS, count_tasks=True,
    )  # fmt: skip
    for row in rows:
        for column in _COUNTS:
            row[column] = row[column] or 0  # a count, never null
    return rows
