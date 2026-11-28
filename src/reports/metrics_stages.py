"""``/metrics/stages``: the stages' own numbers (context tokens, tool calls, duration) per stage.

One figure per step row: a completed stage at ``finished_at``, another ended one (failed, never
given a finish) at its last progress or start, a running stage of a running task now
(``partial``, only when the window reaches now). A row replayed from cache counts in
``cached_runs``; it keeps the numbers of the run that measured it, which counts in ``runs``
(a cached row without numbers, mirrored before they were recorded, adds only
``cached_runs``). ``stage_tokens`` is the stage's context size, not billed tokens: never add
it to ``/metrics/usage`` ``tokens``. The billed columns (``billed_*_tokens``, ``turns``) come
from the stage transcripts; ``estimated_cost_usd`` is ``src/stage_cost.py``'s split, its
``cost_basis`` ``exact`` only when every summed stage is exact, null when any is unknown.
"""

import time
from collections.abc import Mapping
from typing import Any

import aiosqlite

from src.reports.aggregate import Contribution, aggregate
from src.reports.metrics_usage import in_window
from src.reports.query import Bucket, Filters, where
from src.reports.stage_costs import task_costs
from src.stages import normalize_stage

GROUP_BY = ("stage", "stage_agent", "model", "project")
BILLED = (
    "billed_input_tokens", "billed_output_tokens", "billed_cache_read_tokens",
    "billed_cache_write_tokens", "turns",
)  # fmt: skip
_BASIS_COUNTS = ("_exact", "_allocated", "_unknown")
COLUMNS = (
    "stage_tokens", "tool_calls", "duration_s", "runs", "cached_runs", *BILLED,
    "estimated_cost_usd", *_BASIS_COUNTS,
)  # fmt: skip
_COUNTS = ("runs", "cached_runs")


async def contributions(
    db: aiosqlite.Connection,
    filters: Filters,
    now: float,
    cost_ratios: Mapping[str, Mapping[str, float]],
) -> list[Contribution]:
    cond, args = where(filters, time_column=None)
    if filters.stage_agent is not None:
        cond += " AND s.agent_type = ?"
        args.append(filters.stage_agent)
    async with db.execute(
        "SELECT s.task_id, s.label, s.status, s.cached, s.agent_type, s.model, s.stage_tokens, "
        "s.tool_calls, s.duration_s, t.cwd, t.status AS task_status, "
        f"{', '.join('s.' + c for c in BILLED)}, "
        "coalesce(s.finished_at, s.last_progress_at, s.started_at) AS ended_at "
        f"FROM steps s JOIN tasks t ON t.id = s.task_id WHERE {cond}",
        args,
    ) as cur:
        found = await cur.fetchall()
    windowed = []
    for r in found:
        running = r["status"] == "running" and r["task_status"] == "running"
        at = now if running else r["ended_at"]
        if at is None or not in_window(filters, at) or (not running and at == filters.until):
            continue
        windowed.append((r, running, at))
    costs = (await task_costs(db, sorted({r["task_id"] for r, *_ in windowed}), cost_ratios)).stages
    out: list[Contribution] = []
    for r, running, at in windowed:
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
        # Billed numbers and cost are the measured runs' (step_models), never the replay's own:
        # a row replayed from cache keeps them, as it keeps stage_tokens.
        cost = costs.get((r["task_id"], r["label"]))
        values |= {c: r[c] for c in BILLED}
        if cost is not None:
            values["estimated_cost_usd"] = cost.estimated_cost_usd
            values[f"_{cost.cost_basis or 'unknown'}"] = 1
        elif r["status"] == "completed":  # finished but never measured (before v11)
            values["_unknown"] = 1
        out.append(Contribution(at, r["task_id"], keys, values, partial=running))
    return out


async def stage_rows(
    db: aiosqlite.Connection,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
    cost_ratios: Mapping[str, Mapping[str, float]],
    now: float | None = None,
) -> list[dict[str, Any]]:
    at = time.time() if now is None else now
    rows = aggregate(
        await contributions(db, filters, at, cost_ratios),
        group_by=group_by, bucket=bucket, columns=COLUMNS, count_tasks=True,
    )  # fmt: skip
    for row in rows:
        for column in _COUNTS:
            row[column] = row[column] or 0  # a count, never null
        exact, allocated, unknown = (row.pop(c) or 0 for c in _BASIS_COUNTS)
        row["cost_basis"] = _basis(exact, allocated, unknown)
        if row["cost_basis"] is None:
            row["estimated_cost_usd"] = None
    return rows


def _basis(exact: int, allocated: int, unknown: int) -> str | None:
    """The summed stages' basis: null when any is unknown (or none is costed), ``exact`` only when
    all are."""
    if unknown or not (exact or allocated):
        return None
    return "allocated" if allocated else "exact"
