"""Estimated cost per lifecycle stage: an attempt's known per-model cost (``attempt_models``)
split across the stages that used that model in the attempt and the parent session's own
overhead. No price list: only the attempt's cost is in dollars; the split uses token-class
weights relative to base input (``cost_ratios``).

Per (attempt, model):

* exact - one stage used the model and the parent session used none of it: that stage takes the
  model's whole cost.
* allocated - otherwise each participant (each stage, plus the parent overhead) gets
  ``cost * w / sum(w)``, ``w = Σ ratio(class) * tokens(class)``; the overhead takes the residual,
  so stage costs plus overhead equal the attempt's cost exactly.

Default ratios (Anthropic prompt caching docs,
https://platform.claude.com/docs/en/build-with-claude/prompt-caching): a 5-minute cache write
costs 1.25x base input, a 1-hour write 2x, a cache read about 0.1x (it varies by model, e.g. 0.05x
for Opus 5.5); output is 5x on current models. The parent session's cache writes are not split
by duration in ``modelUsage``: they weigh ``parent_cache_write`` (default 2x: dioxide's parent
sessions observed writing 1-hour entries). Every ratio is overridable per model (prefix match).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

RATIO_KEYS = (
    "input", "output", "cache_read", "cache_write_5m", "cache_write_1h", "parent_cache_write",
)  # fmt: skip
DEFAULT_RATIOS: Mapping[str, float] = {
    "input": 1.0, "output": 5.0, "cache_read": 0.1, "cache_write_5m": 1.25,
    "cache_write_1h": 2.0, "parent_cache_write": 2.0,
}  # fmt: skip
Basis = Literal["exact", "allocated"]
_DIGITS = 6


@dataclass(frozen=True)
class Tokens:
    """Billed tokens of one participant on one model."""

    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write_5m: int = 0
    cache_write_1h: int = 0

    @property
    def total(self) -> int:
        return (
            self.input + self.output + self.cache_read + self.cache_write_5m + self.cache_write_1h
        )


@dataclass(frozen=True)
class AttemptModel:
    """An attempt's tokens and cost on one model (``attempt_models``); cache writes unsplit."""

    input: int
    output: int
    cache_read: int
    cache_write: int
    cost_usd: float


@dataclass(frozen=True)
class Split:
    """One (attempt, model) cost split: per stage label, and the parent overhead."""

    stages: dict[str, float] = field(default_factory=dict)
    basis: Basis = "allocated"
    overhead_cost_usd: float = 0.0
    overhead: Tokens = Tokens()


def ratios_for(model: str, overrides: Mapping[str, Mapping[str, float]]) -> dict[str, float]:
    """The defaults updated by the longest ``overrides`` key that prefixes ``model``."""
    keys = [k for k in overrides if model.startswith(k)]
    chosen = overrides[max(keys, key=len)] if keys else {}
    return {**DEFAULT_RATIOS, **chosen}


def _weight(tokens: Tokens, ratios: Mapping[str, float], *, parent: bool) -> float:
    writes = (
        ratios["parent_cache_write"] * (tokens.cache_write_5m + tokens.cache_write_1h)
        if parent
        else ratios["cache_write_5m"] * tokens.cache_write_5m
        + ratios["cache_write_1h"] * tokens.cache_write_1h
    )
    return (
        ratios["input"] * tokens.input + ratios["output"] * tokens.output
        + ratios["cache_read"] * tokens.cache_read + writes
    )  # fmt: skip


def overhead_of(attempt: AttemptModel, stages: Mapping[str, Tokens]) -> Tokens:
    """The parent session's own tokens: the attempt's minus its stages' (never below 0); its
    cache writes are kept in ``cache_write_5m`` (duration unknown, see ``parent_cache_write``)."""
    return Tokens(
        input=max(0, attempt.input - sum(t.input for t in stages.values())),
        output=max(0, attempt.output - sum(t.output for t in stages.values())),
        cache_read=max(0, attempt.cache_read - sum(t.cache_read for t in stages.values())),
        cache_write_5m=max(
            0,
            attempt.cache_write - sum(t.cache_write_5m + t.cache_write_1h for t in stages.values()),
        ),
    )


def split(
    attempt: AttemptModel, stages: Mapping[str, Tokens], ratios: Mapping[str, float]
) -> Split:
    """The attempt's cost on one model across ``stages`` (label -> tokens) and the overhead."""
    overhead = overhead_of(attempt, stages)
    used = {label: t for label, t in stages.items() if t.total > 0}
    if len(used) == 1 and overhead.total == 0:
        (label,) = used
        costs = {name: 0.0 for name in stages} | {label: attempt.cost_usd}
        return Split(costs, "exact", 0.0, overhead)
    weights = {label: _weight(t, ratios, parent=False) for label, t in stages.items()}
    total = sum(weights.values()) + _weight(overhead, ratios, parent=True)
    costs = {
        label: round(attempt.cost_usd * w / total, _DIGITS) if total > 0 else 0.0
        for label, w in weights.items()
    }
    residual = attempt.cost_usd - sum(costs.values())
    if overhead.total == 0 and costs:  # no parent usage: the rounding residual is a stage's
        heaviest = max(costs, key=lambda label: (weights[label], label))
        costs[heaviest] += residual
        residual = 0.0
    return Split(costs, "allocated", residual, overhead)
