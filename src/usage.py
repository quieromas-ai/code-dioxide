"""Per-attempt usage: the cost and tokens an attempt itself spent.

A result record reports the session's totals so far. Since Claude Code 2.1.277 a resumed session
restores its earlier spend, so a resumed attempt's report includes every earlier attempt of the
same session. An attempt's own share is that report minus what those attempts already account
for. Rows hold the share, so summing a task's attempts counts each dollar and token once.

Tokens come from ``modelUsage`` (every model, subagents included). Without it the top-level
``usage`` is used. That figure is per run, not cumulative, so it is taken as is.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import aiosqlite

from src.recovery import KILL_REASONS
from src.stream import Result


@dataclass(frozen=True)
class Usage:
    """Cost (USD) and tokens; None when not known."""

    cost_usd: float | None
    tokens: int | None


# Summed float shares drift a few ulps from the session total: a total this close below the
# prior is the same amount (nothing spent), not a restart.
FLOAT_NOISE = 1e-9


def _share[N: (int, float)](total: N, prior: N | None) -> N:
    """``total`` minus ``prior``; a total below the prior means the session's totals started
    over (a pre-2.1.277 resume, a crash result with zeroed totals), so all of it is new."""
    counted = prior or 0
    if total < counted - FLOAT_NOISE:
        return total
    return max(total - counted, total * 0)  # float noise never makes a negative share


def attempt_usage(result: Result | None, prior: Usage) -> Usage:
    """The attempt's own usage from its last result record. ``prior`` is what earlier attempts
    of the same session already account for (``session_usage_before``)."""
    if result is None:
        return Usage(cost_usd=None, tokens=None)
    cost = None if result.cost_usd is None else _share(result.cost_usd, prior.cost_usd)
    if result.session_tokens is None:
        return Usage(cost_usd=cost, tokens=result.tokens)
    return Usage(cost_usd=cost, tokens=_share(result.session_tokens, prior.tokens))


async def session_usage_before(db: aiosqlite.Connection, task_id: str, n: int) -> Usage:
    """What the task's attempts before ``n`` in attempt ``n``'s session already account for.
    Nothing when attempt ``n`` is unknown or has no session recorded."""
    async with db.execute(
        "SELECT SUM(a.cost_usd) AS cost, SUM(a.tokens) AS tokens FROM attempts a "
        "JOIN attempts cur ON cur.task_id = a.task_id AND cur.session_id = a.session_id "
        "WHERE a.task_id = ? AND cur.n = ? AND a.n < ?",
        (task_id, n, n),
    ) as cur:
        row = await cur.fetchone()
    return _from_sums(row)


def _from_sums(row: aiosqlite.Row | None) -> Usage:
    """A ``SUM(cost_usd) AS cost, SUM(tokens) AS tokens`` row; NULL sums are unknown."""
    if row is None:
        return Usage(cost_usd=None, tokens=None)
    cost, tokens = row["cost"], row["tokens"]
    return Usage(
        cost_usd=None if cost is None else float(cost),
        tokens=None if tokens is None else int(tokens),
    )


def _plus[N: (int, float)](a: N | None, b: N | None) -> N | None:
    if a is None:
        return b
    return a if b is None else a + b


async def task_usage(db: aiosqlite.Connection, task_id: str, n: int, own: Usage) -> Usage:
    """The task's usage with attempt ``n`` counted as ``own`` (its row may not hold it yet)."""
    async with db.execute(
        "SELECT SUM(cost_usd) AS cost, SUM(tokens) AS tokens FROM attempts "
        "WHERE task_id = ? AND n != ?",
        (task_id, n),
    ) as cur:
        row = await cur.fetchone()
    others = _from_sums(row)
    return Usage(
        cost_usd=_plus(others.cost_usd, own.cost_usd), tokens=_plus(others.tokens, own.tokens)
    )


# An attempt whose process never started (``engine_recovery.spawn_failed``): nothing was spent.
SPAWN_FAILED = "spawn_failed"


def uncounted_condition(alias: str) -> tuple[str, list[str]]:
    """SQL (and its parameters) true for an ended attempt ``alias`` whose spend is not (fully)
    in the figures: cut short (no result record, or killed) and never followed by an attempt
    of the same session that has a result (a resumed session's total brings the earlier spend
    back in). A failed spawn spent nothing and is never uncounted."""
    kills = sorted(KILL_REASONS)
    sql = (
        f"({alias}.ended_at IS NOT NULL AND coalesce({alias}.reason, '') != '{SPAWN_FAILED}' "
        f"AND ({alias}.cost_usd IS NULL "
        f"OR coalesce({alias}.reason, '') IN ({','.join('?' * len(kills))})) "
        f"AND NOT EXISTS (SELECT 1 FROM attempts later WHERE later.task_id = {alias}.task_id "
        f"AND later.n > {alias}.n AND later.session_id = {alias}.session_id "
        "AND later.cost_usd IS NOT NULL))"
    )
    return sql, kills


async def uncounted_attempt_keys(
    db: aiosqlite.Connection, task_ids: Sequence[str]
) -> set[tuple[str, int]]:
    """``(task_id, n)`` of the attempts ``uncounted_condition`` holds for."""
    if not task_ids:
        return set()
    ids = list(task_ids)
    condition, params = uncounted_condition("a")
    async with db.execute(
        f"SELECT a.task_id, a.n FROM attempts a "
        f"WHERE a.task_id IN ({','.join('?' * len(ids))}) AND {condition}",
        [*ids, *params],
    ) as cur:
        return {(r["task_id"], int(r["n"])) for r in await cur.fetchall()}


async def uncounted_attempts(db: aiosqlite.Connection, task_ids: Sequence[str]) -> dict[str, int]:
    """Per task, how many of its attempts ``uncounted_attempt_keys`` names; tasks with none are
    absent."""
    counts: dict[str, int] = {}
    for task_id, _ in await uncounted_attempt_keys(db, task_ids):
        counts[task_id] = counts.get(task_id, 0) + 1
    return counts
