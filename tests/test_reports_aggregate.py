import pytest

from src.reports.aggregate import Contribution, aggregate
from src.reports.query import MAX_METRIC_ROWS, QueryError

HOUR = 1795219200.0  # a UTC midnight


def c(at: float, task: str, agent: str, cost: float | None, partial: bool = False) -> Contribution:
    return Contribution(at, task, {"agent": agent}, {"cost_usd": cost}, partial)


def test_sum_within_bucket_and_groups() -> None:
    contribs = [
        c(HOUR + 1, "t1", "a", 1.0),
        c(HOUR + 2, "t2", "b", 2.0),
        c(HOUR + 3, "t1", "a", None),
    ]
    (row,) = aggregate(
        contribs, group_by=(), bucket="hour", columns=("cost_usd",), count_tasks=True
    )
    assert row == {
        "bucket": "2026-11-21T00:00:00+00:00", "cost_usd": 3.0, "tasks": 2, "partial": False,
    }  # fmt: skip
    rows = aggregate(contribs, group_by=("agent",), bucket="none", columns=("cost_usd",))
    assert rows == [
        {"agent": "a", "cost_usd": 1.0, "partial": False},
        {"agent": "b", "cost_usd": 2.0, "partial": False},
    ]


def test_all_null_column_is_null_and_partial_propagates() -> None:
    rows = aggregate([c(HOUR, "t1", "a", None, partial=True)], group_by=(), bucket="none",
                     columns=("cost_usd",))  # fmt: skip
    assert rows == [{"cost_usd": None, "partial": True}]
    assert aggregate([], group_by=(), bucket="none", columns=("cost_usd",)) == []


def test_per_task_stats_nearest_rank() -> None:
    contribs = [c(HOUR, f"t{v}", "a", float(v)) for v in (1, 2, 3)]
    contribs += [c(HOUR, "t10", "a", 4.0), c(HOUR, "t10", "a", 6.0)]  # one task, summed first
    stat = {s: aggregate(contribs, group_by=(), bucket="none", columns=("cost_usd",), stat=s)
            for s in ("p50", "p90", "max")}  # fmt: skip
    assert [stat[s][0]["cost_usd"] for s in ("p50", "p90", "max")] == [2.0, 10.0, 10.0]


def test_row_cap() -> None:
    contribs = [c(HOUR + 3600 * i, "t", "a", 1.0) for i in range(MAX_METRIC_ROWS + 1)]
    with pytest.raises(QueryError, match="too many rows"):
        aggregate(contribs, group_by=(), bucket="hour", columns=("cost_usd",))
