"""Per-attempt usage: an attempt's share of its session's cumulative totals."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from src.database import close_db, get_db, init_db, write_tx
from src.stream import Result
from src.usage import (
    Usage,
    attempt_usage,
    session_usage_before,
    task_usage,
    uncounted_attempts,
)
from tests.helpers import Env
from tests.reports_fixture import insert_attempt, insert_task

NOTHING = Usage(cost_usd=None, tokens=None)
SESSION = "e05e6ac6-0000-4000-8000-000000000000"


def result(cost: float | None, session_tokens: int | None, tokens: int = 0) -> Result:
    return Result(
        is_error=False, subtype="success", api_error_status=None, api_error=None, text="Done.",
        cost_usd=cost, tokens=tokens, session_id=SESSION, errors=[], permission_denials=[],
        result_index=0, session_tokens=session_tokens,
    )  # fmt: skip


def test_resumed_attempt_counts_only_its_own_share() -> None:
    # Task 033df164: the resumed attempt reported the session's whole total (v2.1.277+).
    first = attempt_usage(result(0.605304, 404_263), NOTHING)
    assert first == Usage(cost_usd=pytest.approx(0.605304), tokens=404_263)
    second = attempt_usage(result(8.500052055, 14_024_526), first)
    assert second.tokens == 13_620_263
    assert second.cost_usd == pytest.approx(7.894748055)


def test_no_prior_attempt_takes_the_whole_total() -> None:
    assert attempt_usage(result(0.2, 300), NOTHING) == Usage(cost_usd=0.2, tokens=300)


def test_total_below_prior_is_a_reset_never_negative() -> None:
    # A session that restarted its totals (pre-2.1.277 resume, or a crash result with zeroed
    # totals) reports less than was already counted: the whole report is this attempt's.
    prior = Usage(cost_usd=5.0, tokens=1_000)
    assert attempt_usage(result(1.5, 400), prior) == Usage(cost_usd=1.5, tokens=400)
    assert attempt_usage(result(0.0, 0), prior) == Usage(cost_usd=0.0, tokens=0)


def test_without_model_usage_tokens_fall_back_to_top_level_usage() -> None:
    # Top-level usage is per run, not cumulative: it is never reduced by the prior.
    prior = Usage(cost_usd=0.1, tokens=21_104)
    got = attempt_usage(result(0.3, None, tokens=21_104), prior)
    assert got.tokens == 21_104 and got.cost_usd == pytest.approx(0.2)


def test_no_result_is_nothing_known() -> None:
    assert attempt_usage(None, Usage(cost_usd=1.0, tokens=5)) == NOTHING


def test_result_without_cost_keeps_cost_unknown() -> None:
    assert attempt_usage(result(None, 50), Usage(cost_usd=1.0, tokens=10)) == Usage(None, 40)


@pytest.fixture
async def db(tmp_path: Path) -> AsyncIterator[None]:
    await init_db(tmp_path / "data" / "dioxide.db")
    yield
    await close_db()


async def _attempt(n: int, session: str | None, cost: float | None, tokens: int | None) -> None:
    async with write_tx() as conn:
        await conn.execute(
            "INSERT INTO attempts (task_id, n, session_id, started_at, cost_usd, tokens) "
            "VALUES ('t1', ?, ?, 1.0, ?, ?)",
            (n, session, cost, tokens),
        )


async def test_session_usage_before_sums_earlier_attempts_of_the_same_session(db: None) -> None:
    await _attempt(1, "s-old", 9.0, 900)  # before a fresh start: another session
    await _attempt(2, "s-new", 0.5, 50)
    await _attempt(3, "s-new", None, None)  # killed: no result recorded
    await _attempt(4, "s-new", 1.0, 100)
    await _attempt(5, "s-new", None, None)  # the attempt being finalized
    got = await session_usage_before(get_db(), "t1", 5)
    assert got.tokens == 150 and got.cost_usd == pytest.approx(1.5)


async def test_session_usage_before_is_nothing_for_a_first_or_unknown_session(db: None) -> None:
    await _attempt(1, "s-a", 0.5, 50)
    await _attempt(2, "s-b", None, None)  # fresh start: new session
    await _attempt(3, None, None, None)  # no session recorded
    assert await session_usage_before(get_db(), "t1", 1) == NOTHING
    assert await session_usage_before(get_db(), "t1", 2) == NOTHING
    assert await session_usage_before(get_db(), "t1", 3) == NOTHING
    assert await session_usage_before(get_db(), "t1", 9) == NOTHING  # no such attempt


async def test_task_usage_adds_the_attempt_share_to_the_other_attempts(db: None) -> None:
    await _attempt(1, "s-old", 9.0, 900)
    await _attempt(2, "s-new", None, None)  # killed: nothing known
    await _attempt(3, "s-new", None, None)  # being finalized: its row is not written yet
    got = await task_usage(get_db(), "t1", 3, Usage(cost_usd=0.5, tokens=50))
    assert got.tokens == 950 and got.cost_usd == pytest.approx(9.5)


async def test_task_usage_is_unknown_only_when_nothing_is_known(db: None) -> None:
    await _attempt(1, "s", None, None)
    assert await task_usage(get_db(), "t1", 1, NOTHING) == NOTHING
    assert await task_usage(get_db(), "t1", 1, Usage(cost_usd=None, tokens=7)) == Usage(None, 7)


def test_zero_spend_resume_within_float_noise_is_not_a_reset() -> None:
    # Summed shares land a few ulps above the session total (0.02, 1.2411, 3.7479 sum to
    # 3.7479000000000005): a resume that spends nothing must add nothing, not the whole total.
    counted = NOTHING
    for total in (0.02, 1.2411, 3.7479):
        share = attempt_usage(result(total, None), counted)
        assert share.cost_usd is not None
        counted = Usage(cost_usd=(counted.cost_usd or 0.0) + share.cost_usd, tokens=None)
    idle = attempt_usage(result(3.7479, None), counted)
    assert idle.cost_usd == pytest.approx(0.0, abs=1e-9)


async def _plant_completeness_cases() -> None:
    for task_id in "ABCDE":
        await insert_task(task_id)
    await insert_attempt("A", 1, ended_at=1.0, cost_usd=None)  # resumed with a result: counted
    await insert_attempt("A", 2, ended_at=2.0, cost_usd=1.0)
    await insert_attempt("B", 1, ended_at=1.0, cost_usd=0.5, reason="stall")  # killed, last
    await insert_attempt("C", 1, session_id="s1", ended_at=1.0, cost_usd=None)  # fresh after
    await insert_attempt("C", 2, session_id="s2", ended_at=2.0, cost_usd=1.0)
    await insert_attempt("D", 1, ended_at=None, cost_usd=None)  # still running
    await insert_attempt("E", 1, ended_at=1.0, cost_usd=1.0, reason="clean_exit")


async def test_uncounted_attempt_rules(db: None) -> None:
    await _plant_completeness_cases()
    assert await uncounted_attempts(get_db(), list("ABCDE")) == {"B": 1, "C": 1}
    assert await uncounted_attempts(get_db(), []) == {}


async def test_status_and_list_carry_completeness(env: Env) -> None:
    await _plant_completeness_cases()
    for task_id, uncounted in (("B", 1), ("E", 0)):
        status = await env.engine.status(task_id)
        assert (status["uncounted_attempts"], status["cost_complete"]) == (uncounted, not uncounted)
    rows = {r["task_id"]: r for r in (await env.engine.list(None, None, 20, None))["tasks"]}
    assert (rows["B"]["uncounted_attempts"], rows["B"]["cost_complete"]) == (1, False)
    assert (rows["E"]["uncounted_attempts"], rows["E"]["cost_complete"]) == (0, True)


async def test_failed_spawn_is_not_uncounted(db: None) -> None:
    await insert_task("F", status="failed")
    await insert_attempt("F", 1, ended_at=1.0, cost_usd=None, reason="spawn_failed")
    assert await uncounted_attempts(get_db(), ["F"]) == {}
