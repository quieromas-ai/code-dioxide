"""``/metrics/savings``: work dioxide did not redo.

``cached_stages``: stages a resumed run served from cache, at the stage's time (as in
``/metrics/stages``). ``deduplicated_submissions``: ``delegate_task`` calls answered with an
existing task, at the ``deduplicated`` event (counted from the release that writes it).
``est_stage_tokens_saved``: per cached stage, the median ``stage_tokens`` of measured runs of
the same ``stage`` and stage agent in the window; null with fewer than 3 of them. No cost
estimate: cost per stage is not known.
"""

import statistics
from typing import Any

import aiosqlite

from src.reports.aggregate import Contribution, aggregate
from src.reports.metrics_usage import in_window
from src.reports.query import Bucket, Filters, where
from src.stages import normalize_stage

GROUP_BY = ("agent", "project", "stage")
COLUMNS = ("cached_stages", "deduplicated_submissions", "est_stage_tokens_saved")
MIN_SAMPLES = 3
_COUNTS = ("cached_stages", "deduplicated_submissions")


async def _stage_contributions(db: aiosqlite.Connection, filters: Filters) -> list[Contribution]:
    cond, args = where(filters, time_column=None)
    if filters.stage_agent is not None:
        cond += " AND s.agent_type = ?"
        args.append(filters.stage_agent)
    async with db.execute(
        "SELECT s.task_id, s.label, s.cached, s.agent_type, s.stage_tokens, t.agent, t.cwd, "
        "coalesce(s.finished_at, s.last_progress_at, s.started_at) AS at "
        f"FROM steps s JOIN tasks t ON t.id = s.task_id WHERE {cond} AND s.status != 'running'",
        args,
    ) as cur:
        found = [
            r for r in await cur.fetchall()
            if r["at"] is not None and in_window(filters, r["at"]) and r["at"] != filters.until
        ]  # fmt: skip
    samples: dict[tuple[str, str | None], list[int]] = {}
    for r in found:
        if r["stage_tokens"] is not None:
            samples.setdefault((normalize_stage(r["label"]), r["agent_type"]), []).append(
                r["stage_tokens"]
            )
    out = []
    for r in found:
        if not r["cached"]:
            continue
        stage = normalize_stage(r["label"])
        measured = samples.get((stage, r["agent_type"]), [])
        estimate = statistics.median(measured) if len(measured) >= MIN_SAMPLES else None
        keys = {"agent": r["agent"], "project": r["cwd"], "stage": stage}
        values = {"cached_stages": 1, "est_stage_tokens_saved": estimate}
        out.append(Contribution(r["at"], r["task_id"], keys, values))
    return out


async def _dedupe_contributions(db: aiosqlite.Connection, filters: Filters) -> list[Contribution]:
    if filters.stage_agent is not None:
        return []  # a dedupe has no stage
    cond, args = where(filters, time_column="e.at")
    async with db.execute(
        "SELECT e.task_id, e.at, t.agent, t.cwd FROM events e JOIN tasks t ON t.id = e.task_id "
        f"WHERE e.type = 'deduplicated' AND {cond}",
        args,
    ) as cur:
        return [
            Contribution(
                r["at"],
                r["task_id"],
                {"agent": r["agent"], "project": r["cwd"], "stage": None},
                {"deduplicated_submissions": 1},
            )
            for r in await cur.fetchall()
        ]


async def savings_rows(
    db: aiosqlite.Connection,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
) -> list[dict[str, Any]]:
    """No open figures: a cached stage or a dedupe is final when recorded."""
    found = await _stage_contributions(db, filters) + await _dedupe_contributions(db, filters)
    rows = aggregate(found, group_by=group_by, bucket=bucket, columns=COLUMNS)
    for row in rows:
        for column in _COUNTS:
            row[column] = row[column] or 0
    return rows
