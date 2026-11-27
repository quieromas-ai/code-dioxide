"""``/metrics/overhead``: dioxide's own spend (task summaries), by the time of each run. Never
part of a task's cost; total dioxide spend = Σ ``/metrics/usage`` + Σ this."""

from typing import Any

import aiosqlite

from src.reports.aggregate import Contribution, aggregate
from src.reports.query import Bucket, Filters

GROUP_BY: tuple[str, ...] = ()
COLUMNS = ("summaries", "cost_usd", "tokens")


async def overhead_rows(
    db: aiosqlite.Connection, filters: Filters, bucket: Bucket
) -> list[dict[str, Any]]:
    clauses: list[str] = ["1"]
    args: list[Any] = []
    if filters.task is not None:
        clauses.append("task_id = ?")
        args.append(filters.task)
    if filters.since is not None:
        clauses.append("at >= ?")
        args.append(filters.since)
    if filters.until is not None:
        clauses.append("at < ?")
        args.append(filters.until)
    async with db.execute(
        f"SELECT id, task_id, at, cost_usd, tokens FROM summary_runs WHERE {' AND '.join(clauses)}",
        args,
    ) as cur:
        found = [
            Contribution(
                r["at"], r["task_id"] or f"run-{r['id']}", {},
                {"summaries": 1, "cost_usd": r["cost_usd"], "tokens": r["tokens"]},
            )
            for r in await cur.fetchall()
        ]  # fmt: skip
    rows = aggregate(found, group_by=(), bucket=bucket, columns=COLUMNS)
    for row in rows:
        row["summaries"] = row["summaries"] or 0
    return rows
