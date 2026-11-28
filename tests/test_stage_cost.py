"""The per-stage cost split (src/stage_cost.py); fixtures with the shape of a measured run."""

import math

import pytest

from src.stage_cost import DEFAULT_RATIOS, AttemptModel, Tokens, overhead_of, ratios_for, split

OPUS_55 = {"cache_read": 0.05}  # Opus 5.5 list prices: read $0.20 / input $4


def test_single_stage_without_parent_usage_is_exact() -> None:
    attempt = AttemptModel(10, 1693, 69695, 48554, 0.0076137)  # haiku: setup + merge only
    stages = {"setup@a2": Tokens(4, 397, 22614, 23053), "merge@a2": Tokens(6, 1296, 47081, 25501)}
    result = split(attempt, stages, DEFAULT_RATIOS)
    assert result.basis == "allocated"  # two stages share the model
    assert math.isclose(sum(result.stages.values()) + result.overhead_cost_usd, 0.0076137)
    assert result.overhead == Tokens()
    one = split(
        AttemptModel(16, 3609, 302712, 49639, 0.19049),
        {"qa-r1@a2": Tokens(16, 3609, 302712, 49639)},
        DEFAULT_RATIOS,
    )
    assert (one.basis, one.stages, one.overhead_cost_usd) == ("exact", {"qa-r1@a2": 0.19049}, 0.0)


def test_parent_and_stage_on_one_model_is_allocated() -> None:
    # Attempt 1 on opus: one stage plus the parent session.
    attempt = AttemptModel(34, 8439, 506409, 72212, 0.7092008)
    result = split(
        attempt,
        {"plan": Tokens(26, 7820, 428316, 46231)},
        ratios_for("claude-opus-5-5", {"claude-opus": OPUS_55}),
    )
    assert result.basis == "allocated"
    assert result.overhead == Tokens(8, 619, 78093, 25981)
    # Opus 5.5 list prices reproduce the stage's own cost: $0.4734 of the attempt's $0.7092.
    assert result.stages["plan"] == pytest.approx(0.4734, abs=0.001)
    assert result.stages["plan"] + result.overhead_cost_usd == pytest.approx(0.7092008, abs=1e-9)


def test_two_stages_split_by_weight_and_residual_is_exact() -> None:
    attempt = AttemptModel(0, 200, 0, 0, 1.0)
    result = split(attempt, {"a": Tokens(output=100), "b": Tokens(output=100)}, DEFAULT_RATIOS)
    assert result.stages == {"a": 0.5, "b": 0.5} and result.overhead_cost_usd == 0.0
    odd = split(
        AttemptModel(0, 3, 0, 0, 1.0),
        {"a": Tokens(output=1), "b": Tokens(output=1)},
        DEFAULT_RATIOS,
    )
    assert odd.stages == {"a": 0.333333, "b": 0.333333}
    assert sum(odd.stages.values()) + odd.overhead_cost_usd == pytest.approx(1.0, abs=1e-12)


def test_output_heavy_stage_costs_more_than_cache_read_heavy() -> None:
    attempt = AttemptModel(0, 1000, 1000, 0, 1.0)
    result = split(
        attempt, {"out": Tokens(output=1000), "read": Tokens(cache_read=1000)}, DEFAULT_RATIOS
    )
    assert result.stages["out"] > result.stages["read"]
    assert result.stages["out"] / result.stages["read"] == pytest.approx(50, rel=1e-3)  # 5 / 0.1


def test_one_hour_writes_weigh_more_than_five_minute() -> None:
    attempt = AttemptModel(0, 0, 0, 200, 1.0)
    result = split(
        attempt,
        {"5m": Tokens(cache_write_5m=100), "1h": Tokens(cache_write_1h=100)},
        DEFAULT_RATIOS,
    )
    assert result.stages["1h"] / result.stages["5m"] == pytest.approx(2.0 / 1.25, rel=1e-3)


def test_overhead_never_negative() -> None:
    over = overhead_of(AttemptModel(1, 1, 1, 1, 0.1), {"a": Tokens(5, 5, 5, 5, 5)})
    assert over == Tokens()


def test_ratios_longest_prefix_and_partial_override() -> None:
    overrides = {"claude-opus": {"cache_read": 0.07}, "claude-opus-5-5": {"cache_read": 0.05}}
    assert ratios_for("claude-opus-5-5", overrides)["cache_read"] == 0.05
    assert ratios_for("claude-opus-5", overrides)["cache_read"] == 0.07
    assert ratios_for("claude-opus-5-5", overrides)["output"] == DEFAULT_RATIOS["output"]
    assert ratios_for("claude-haiku-5-5", overrides) == dict(DEFAULT_RATIOS)


def test_rounding_residual_without_parent_usage_stays_on_stages() -> None:
    result = split(
        AttemptModel(0, 3, 0, 0, 1.0),
        {"a": Tokens(output=1), "b": Tokens(output=2)},
        DEFAULT_RATIOS,
    )
    assert result.overhead_cost_usd == 0.0
    assert sum(result.stages.values()) == 1.0
