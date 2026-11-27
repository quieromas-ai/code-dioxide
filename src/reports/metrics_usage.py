"""``/metrics/usage``: cost and tokens of attempts, by the time each attempt ended.

An ended attempt counts at its ``ended_at``; the open attempt of a running task counts its
interim share now (``partial``), only when the window reaches now. ``uncounted_attempts`` names
the attempts whose spend is missing (``usage.uncounted_condition``), so a row with any is
``cost_complete: false``. The token breakdown comes from ``attempt_models``; an attempt without
one has null breakdown columns and its tokens in ``unattributed_tokens``. With
``group_by=model`` an attempt counts per model, and what has no breakdown in a ``model: null``
row, so the model rows sum to the ungrouped figures.
"""

import time
from collections.abc import Mapping
from typing import Any

import aiosqlite

from src import engine_read, model_usage, usage
from src.engine import TaskEngine
from src.reports.aggregate import Contribution, aggregate
from src.reports.query import Bucket, Filters, where
from src.stream import ModelUsage
from src.tasks import Task

GROUP_BY = ("agent", "model", "project", "kind", "status")
BREAKDOWN = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens")
COLUMNS = (
    "cost_usd", "tokens", *BREAKDOWN, "unattributed_tokens", "attempts", "uncounted_attempts",
)  # fmt: skip


def task_keys(row: Any) -> dict[str, str | None]:
    """The group keys of a task row (``agent``, ``cwd``, ``kind``, ``status`` columns)."""
    return {"agent": row["agent"], "project": row["cwd"], "kind": row["kind"],
            "status": row["status"]}  # fmt: skip


def in_window(filters: Filters, at: float) -> bool:
    return (filters.since is None or at >= filters.since) and (
        filters.until is None or at <= filters.until
    )


async def open_attempts(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters, now: float
) -> list[tuple[Task, int, usage.Usage | None, dict[str, ModelUsage] | None]]:
    """Running tasks' open attempts with their interim share and its per-model split; none when
    the window ends before now."""
    if not in_window(filters, now):
        return []
    cond, args = where(filters, time_column=None)
    async with db.execute(
        f"SELECT t.* FROM tasks t WHERE {cond} AND t.status = 'running'", args
    ) as cur:
        tasks = [Task.from_row(r) for r in await cur.fetchall()]
    found = []
    for task in tasks:
        interim = await engine_read.open_attempt_interim(engine, task, db)
        if interim is None:
            continue
        n, result = interim
        prior = await usage.session_usage_before(db, task.id, n)
        share = usage.attempt_usage(result, prior) if result is not None else None
        models = model_usage.model_shares(
            result, prior, await model_usage.prior_models(db, task.id, n)
        )
        found.append((task, n, share, models))
    return found


async def attempt_breakdowns(
    db: aiosqlite.Connection, filters: Filters
) -> dict[tuple[str, int], dict[str, ModelUsage]]:
    """``attempt_models`` of the attempts that ended in the window, per ``(task_id, n)``."""
    cond, args = where(filters, time_column="a.ended_at")
    async with db.execute(
        "SELECT m.task_id, m.n, m.model, m.input_tokens, m.output_tokens, m.cache_read_tokens, "
        "m.cache_write_tokens, m.cost_usd FROM attempt_models m "
        "JOIN attempts a ON a.task_id = m.task_id AND a.n = m.n "
        f"JOIN tasks t ON t.id = a.task_id WHERE a.ended_at IS NOT NULL AND {cond}",
        args,
    ) as cur:
        found: dict[tuple[str, int], dict[str, ModelUsage]] = {}
        for r in await cur.fetchall():
            found.setdefault((r["task_id"], r["n"]), {})[r["model"]] = ModelUsage(
                r["input_tokens"] or 0, r["output_tokens"] or 0, r["cache_read_tokens"] or 0,
                r["cache_write_tokens"] or 0, r["cost_usd"],
            )  # fmt: skip
    return found


def _values(
    cost: float | None, tokens: int | None, models: Mapping[str, ModelUsage] | None
) -> dict[str, Any]:
    """An attempt's columns: the breakdown summed over its models (None without one), and the
    tokens the breakdown does not cover."""
    values: dict[str, Any] = {"cost_usd": cost, "tokens": tokens}
    if models:
        sums = [sum(getattr(m, column) for m in models.values()) for column in BREAKDOWN]
        values |= dict(zip(BREAKDOWN, sums, strict=True))
        values["unattributed_tokens"] = None if tokens is None else tokens - sum(sums)
    else:
        values |= dict.fromkeys(BREAKDOWN)
        values["unattributed_tokens"] = tokens
    return values


def _split(
    at: float,
    task_id: str,
    keys: Mapping[str, str | None],
    counts: Mapping[str, int],
    cost: float | None,
    tokens: int | None,
    models: Mapping[str, ModelUsage] | None,
    *,
    by_model: bool,
    partial: bool = False,
) -> list[Contribution]:
    """One attempt's contributions: one row, or with ``by_model`` one per model plus a
    ``model: null`` row for what has no breakdown (the whole attempt, or a cost residual), so
    model rows sum to the attempt. ``attempts`` count in each model row of the attempt."""
    if not by_model or not models:
        values = _values(cost, tokens, models) | dict(counts)
        return [Contribution(at, task_id, {**keys, "model": None}, values, partial)]
    found = [
        Contribution(
            at, task_id, {**keys, "model": model},
            _values(m.cost_usd, m.tokens, {model: m}) | dict(counts), partial,
        )
        for model, m in sorted(models.items())
    ]  # fmt: skip
    attributed = sum(m.cost_usd or 0.0 for m in models.values())
    if cost is not None and abs(cost - attributed) > usage.FLOAT_NOISE:
        residual = {"cost_usd": cost - attributed, "attempts": 0, "uncounted_attempts": 0}
        found.append(Contribution(at, task_id, {**keys, "model": None}, residual, partial))
    return found


async def open_attempt_rows(db: aiosqlite.Connection, filters: Filters, now: float) -> list[Any]:
    """Running tasks' open attempts (task keys, ``n``, ``started_at``, ``mode``) without reading
    their streams; none when the window ends before now."""
    if not in_window(filters, now):
        return []
    cond, args = where(filters, time_column=None)
    async with db.execute(
        "SELECT t.id, t.agent, t.cwd, t.kind, t.status, a.n, a.started_at, "
        "(SELECT json_extract(e.data_json, '$.mode') FROM events e WHERE e.task_id = a.task_id "
        "AND e.type = 'attempt_started' AND json_extract(e.data_json, '$.n') = a.n "
        "ORDER BY e.id DESC LIMIT 1) AS mode "
        "FROM tasks t JOIN attempts a ON a.task_id = t.id AND a.ended_at IS NULL "
        "AND a.n = (SELECT max(n) FROM attempts o WHERE o.task_id = t.id AND o.ended_at IS NULL) "
        f"WHERE {cond} AND t.status = 'running'",
        args,
    ) as cur:
        return list(await cur.fetchall())


async def contributions(
    db: aiosqlite.Connection,
    engine: TaskEngine,
    filters: Filters,
    now: float,
    *,
    by_model: bool = False,
) -> list[Contribution]:
    cond, args = where(filters, time_column="a.ended_at")
    uncounted, uncounted_args = usage.uncounted_condition("a")
    async with db.execute(
        f"SELECT a.task_id, a.n, a.ended_at, a.cost_usd, a.tokens, {uncounted} AS uncounted, "
        "t.agent, t.cwd, t.kind, t.status FROM attempts a JOIN tasks t ON t.id = a.task_id "
        f"WHERE a.ended_at IS NOT NULL AND {cond}",
        [*uncounted_args, *args],
    ) as cur:
        rows = await cur.fetchall()
    breakdowns = await attempt_breakdowns(db, filters)
    found: list[Contribution] = []
    for r in rows:
        counts = {"attempts": 1, "uncounted_attempts": int(r["uncounted"])}
        found += _split(
            r["ended_at"], r["task_id"], task_keys(r), counts, r["cost_usd"], r["tokens"],
            breakdowns.get((r["task_id"], r["n"])), by_model=by_model,
        )  # fmt: skip
    for task, _, share, models in await open_attempts(db, engine, filters, now):
        keys = {"agent": task.agent, "project": task.cwd, "kind": task.kind, "status": task.status}
        found += _split(
            now, task.id, keys, {"attempts": 1, "uncounted_attempts": 0},
            share.cost_usd if share else None, share.tokens if share else None, models,
            by_model=by_model, partial=True,
        )  # fmt: skip
    return found


async def usage_rows(
    db: aiosqlite.Connection,
    engine: TaskEngine,
    filters: Filters,
    group_by: tuple[str, ...],
    bucket: Bucket,
    now: float | None = None,
) -> list[dict[str, Any]]:
    at = time.time() if now is None else now
    rows = aggregate(
        await contributions(db, engine, filters, at, by_model="model" in group_by),
        group_by=group_by, bucket=bucket, columns=COLUMNS, count_tasks=True,
    )  # fmt: skip
    for row in rows:
        row["cost_complete"] = not row["uncounted_attempts"]
    return rows
