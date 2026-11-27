"""One aggregation for every metric route: contributions -> rows per bucket and group keys.

A metric source yields one ``Contribution`` per fact (an attempt's cost, a wait, an outcome),
stamped with the time it belongs to and the keys of its task. ``aggregate`` sums them per
(bucket, group keys), or takes a per-task statistic: the task's sum first, then the nearest-rank
percentile (or max) over the tasks of the row.
"""

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from src.engine_read import iso_or_none
from src.reports.query import MAX_METRIC_ROWS, QueryError, bucket_start

Stat = Literal["sum", "p50", "p90", "max"]
STATS: tuple[Stat, ...] = ("sum", "p50", "p90", "max")
_RANK = {"p50": 0.5, "p90": 0.9, "max": 1.0}
Number = float | int


@dataclass(frozen=True)
class Contribution:
    at: float
    task_id: str
    keys: Mapping[str, str | None]
    values: Mapping[str, Number | None]
    partial: bool = False


def _add(a: Number | None, b: Number | None) -> Number | None:
    if a is None:
        return b
    return a if b is None else a + b


def _nearest_rank(values: list[Number], rank: float) -> Number | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(1, math.ceil(rank * len(ordered))) - 1]


def aggregate(
    contribs: Iterable[Contribution],
    *,
    group_by: tuple[str, ...],
    bucket: str,
    columns: tuple[str, ...],
    stat: Stat = "sum",
    count_tasks: bool = False,
) -> list[dict[str, Any]]:
    """Rows sorted by bucket then group keys: ``bucket`` (when bucketed), the group keys, each
    column, ``tasks`` (distinct, when ``count_tasks``) and ``partial`` (any open figure)."""
    rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for item in contribs:
        start = bucket_start(item.at, bucket)
        row_key = (start, *(item.keys.get(k) for k in group_by))
        row = rows.get(row_key)
        if row is None:
            if len(rows) >= MAX_METRIC_ROWS:
                raise QueryError("too many rows: narrow the window or use a coarser bucket")
            row = rows[row_key] = {"start": start, "per_task": {}, "partial": False}
        per_task = row["per_task"].setdefault(item.task_id, {})
        for column in columns:
            per_task[column] = _add(per_task.get(column), item.values.get(column))
        row["partial"] = row["partial"] or item.partial
    out = []
    for row_key in sorted(rows, key=lambda k: tuple("" if v is None else v for v in k)):
        row = rows[row_key]
        result: dict[str, Any] = {}
        if bucket != "none":
            result["bucket"] = iso_or_none(row["start"])
        result.update(zip(group_by, row_key[1:], strict=True))
        tasks = row["per_task"].values()
        for column in columns:
            values = [t[column] for t in tasks if t.get(column) is not None]
            if stat == "sum":
                total: Number | None = None
                for value in values:
                    total = _add(total, value)
                result[column] = total
            else:
                result[column] = _nearest_rank(values, _RANK[stat])
        if count_tasks:
            result["tasks"] = len(row["per_task"])
        result["partial"] = row["partial"]
        out.append(result)
    return out


def parse_stat(value: str | None) -> Stat:
    for stat in STATS:
        if value == stat or (value is None and stat == "sum"):
            return stat
    raise QueryError(f"invalid stat {value!r}: one of {', '.join(STATS)}")
