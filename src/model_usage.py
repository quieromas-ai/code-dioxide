"""Per-model attempt shares (``attempt_models``): what an attempt spent on each model.

A result record's ``modelUsage`` holds the session's per-model totals so far. A model's share is
its total minus that model's share in the session's earlier attempts, with the attempt's own
reset decision (``usage.attempt_usage``): when the session's cost (tokens) started over, every
model takes its whole cost (tokens). So the model shares add up to the attempt share. When the
split is not knowable (no ``modelUsage``, an earlier attempt of the session without a breakdown,
a model below its prior while the session grew) the attempt has no breakdown.
"""

from collections.abc import Mapping

import aiosqlite

from src.stream import ModelUsage, Result
from src.usage import FLOAT_NOISE, Usage

_ZERO = ModelUsage(0, 0, 0, 0, 0.0)


def _reset(total: float | None, prior: float | None) -> bool:
    return total is not None and total < (prior or 0) - FLOAT_NOISE


def _minus(total: float | None, prior: float | None) -> float | None:
    """``total − prior``; None when unknown; NaN marks a negative share (no breakdown)."""
    if total is None:
        return None
    share = total - (prior or 0.0)
    if share < -FLOAT_NOISE:
        return float("nan")
    return max(share, 0.0)


def model_shares(
    result: Result | None, prior: Usage, prior_models: Mapping[str, ModelUsage] | None
) -> dict[str, ModelUsage] | None:
    """The attempt's share per model; ``prior`` is the session's earlier attempts' usage and
    ``prior_models`` their per-model shares (None: one of them has no breakdown)."""
    if result is None or not result.model_usage or prior_models is None:
        return None
    cost_reset = _reset(result.cost_usd, prior.cost_usd)
    tokens_reset = _reset(result.session_tokens, prior.tokens)
    shares: dict[str, ModelUsage] = {}
    for model in sorted({*result.model_usage, *prior_models}):
        total = result.model_usage.get(model, _ZERO)
        before = prior_models.get(model, _ZERO)
        if tokens_reset:
            counts = [total.input_tokens, total.output_tokens, total.cache_read_tokens,
                      total.cache_write_tokens]  # fmt: skip
        else:
            counts = [
                total.input_tokens - before.input_tokens,
                total.output_tokens - before.output_tokens,
                total.cache_read_tokens - before.cache_read_tokens,
                total.cache_write_tokens - before.cache_write_tokens,
            ]
        cost = total.cost_usd if cost_reset else _minus(total.cost_usd, before.cost_usd)
        if any(c < 0 for c in counts) or (cost is not None and cost != cost):  # NaN: negative
            return None
        inp, out, read, write = counts
        share = ModelUsage(inp, out, read, write, cost)
        if model in result.model_usage or share != _ZERO:
            shares[model] = share
    return shares


async def prior_models(
    db: aiosqlite.Connection, task_id: str, n: int
) -> dict[str, ModelUsage] | None:
    """Per-model sums of the earlier attempts in attempt ``n``'s session; None when one of them
    spent tokens but has no breakdown (pruned, or recorded before per-model shares)."""
    async with db.execute(
        "SELECT count(*) AS n FROM attempts a JOIN attempts cur ON cur.task_id = a.task_id "
        "AND cur.session_id = a.session_id WHERE a.task_id = ? AND cur.n = ? AND a.n < ? "
        "AND a.tokens IS NOT NULL AND NOT EXISTS (SELECT 1 FROM attempt_models m "
        "WHERE m.task_id = a.task_id AND m.n = a.n)",
        (task_id, n, n),
    ) as cur:
        row = await cur.fetchone()
    if row is not None and row["n"]:
        return None
    async with db.execute(
        "SELECT m.model, sum(m.input_tokens) AS i, sum(m.output_tokens) AS o, "
        "sum(m.cache_read_tokens) AS r, sum(m.cache_write_tokens) AS w, sum(m.cost_usd) AS c "
        "FROM attempt_models m JOIN attempts a ON a.task_id = m.task_id AND a.n = m.n "
        "JOIN attempts cur ON cur.task_id = a.task_id AND cur.session_id = a.session_id "
        "WHERE m.task_id = ? AND cur.n = ? AND a.n < ? GROUP BY m.model",
        (task_id, n, n),
    ) as cur:
        return {
            r["model"]: ModelUsage(r["i"] or 0, r["o"] or 0, r["r"] or 0, r["w"] or 0, r["c"])
            for r in await cur.fetchall()
        }


async def write_models(
    db: aiosqlite.Connection, task_id: str, n: int, shares: Mapping[str, ModelUsage]
) -> None:
    for model, share in shares.items():
        await db.execute(
            "INSERT OR REPLACE INTO attempt_models (task_id, n, model, input_tokens, "
            "output_tokens, cache_read_tokens, cache_write_tokens, cost_usd) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, n, model, share.input_tokens, share.output_tokens,
             share.cache_read_tokens, share.cache_write_tokens, share.cost_usd),
        )  # fmt: skip
