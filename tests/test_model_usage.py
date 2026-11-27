import json
from typing import Any

import pytest

from src.database import get_db, write_tx
from src.model_usage import model_shares, prior_models, write_models
from src.stream import ModelUsage, Result, StreamWatcher
from src.usage import Usage
from tests.helpers import Env, rows, submit, wait_status
from tests.reports_fixture import insert_attempt, insert_task

pytestmark = pytest.mark.usefixtures("spawned")


def mu(
    inp: int = 0, out: int = 0, read: int = 0, write: int = 0, cost: float | None = 0.0
) -> ModelUsage:
    return ModelUsage(inp, out, read, write, cost)


def result(models: dict[str, ModelUsage] | None, cost: float | None) -> Result:
    tokens = sum(m.tokens for m in models.values()) if models else None
    return Result(
        is_error=False, subtype="success", api_error_status=None, api_error=None, text=None,
        cost_usd=cost, tokens=5, session_id="s1", errors=[], permission_denials=[],
        result_index=None, session_tokens=tokens, model_usage=models,
    )  # fmt: skip


def test_model_usage_parsed_from_result_line(tmp_path: Any) -> None:
    path = tmp_path / "s.jsonl"
    line = {
        "type": "result", "subtype": "success", "is_error": False, "total_cost_usd": 1.5,
        "modelUsage": {
            "a": {"inputTokens": 1, "outputTokens": 2, "cacheReadInputTokens": 3,
                  "cacheCreationInputTokens": 4, "costUSD": 1.0, "canonicalModel": "x"},
            "b": {"inputTokens": "x", "outputTokens": 5},
            "c": "not a dict",
        },
    }  # fmt: skip
    path.write_text(json.dumps(line) + "\n")
    (found,) = [e for e in StreamWatcher(path).poll() if isinstance(e, Result)]
    assert found.model_usage == {"a": mu(1, 2, 3, 4, 1.0), "b": mu(0, 5, 0, 0, None)}
    assert found.model_usage["a"].tokens == 10


def test_model_shares_sum_to_attempt_share() -> None:
    prior = Usage(1.0, 100)
    later = result({"A": mu(inp=100, cost=1.0), "B": mu(inp=50, cost=0.5)}, 1.5)
    shares = model_shares(later, prior, {"A": mu(inp=100, cost=1.0)})
    assert shares == {"A": mu(cost=0.0), "B": mu(inp=50, cost=0.5)}


def test_model_shares_on_reset() -> None:
    """Totals below the prior: the session started over, every model takes its total."""
    later = result({"A": mu(inp=10, cost=0.1)}, 0.1)
    assert model_shares(later, Usage(1.0, 100), {"A": mu(inp=100, cost=1.0)}) == {
        "A": mu(inp=10, cost=0.1)
    }


def test_model_shares_first_attempt_is_the_total() -> None:
    first = result({"A": mu(1, 2, 3, 4, 0.2)}, 0.2)
    assert model_shares(first, Usage(None, None), {}) == {"A": mu(1, 2, 3, 4, 0.2)}


def test_model_shares_none_without_breakdown_or_prior() -> None:
    assert model_shares(None, Usage(None, None), {}) is None
    assert model_shares(result(None, 1.0), Usage(None, None), {}) is None
    assert model_shares(result({"A": mu(inp=1)}, 0.1), Usage(0.0, 0), None) is None


def test_model_shares_none_when_a_model_share_is_negative() -> None:
    """A model below its prior while the session total grew: the split is not knowable."""
    later = result({"A": mu(inp=50, cost=0.5), "B": mu(inp=100, cost=1.0)}, 1.5)
    assert model_shares(later, Usage(1.0, 100), {"A": mu(inp=100, cost=1.0)}) is None


async def test_prior_models_of_session(env: Env) -> None:
    await insert_task("t1")
    await insert_attempt("t1", 1, session_id="s1", tokens=100, cost_usd=1.0, ended_at=1.0)
    await insert_attempt("t1", 2, session_id="s2", tokens=7, cost_usd=0.1, ended_at=2.0)
    await insert_attempt("t1", 3, session_id="s1")
    async with write_tx() as db:
        await write_models(db, "t1", 1, {"A": mu(inp=100, cost=1.0)})
    assert await prior_models(get_db(), "t1", 3) == {"A": mu(inp=100, cost=1.0)}
    assert await prior_models(get_db(), "t1", 1) == {}
    await insert_attempt("t1", 4, session_id="s2")
    assert await prior_models(get_db(), "t1", 4) is None  # attempt 2 used tokens, no rows


async def test_attempt_models_written_at_attempt_end(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SESSION_USAGE", "1")
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed")
    found = await rows("SELECT * FROM attempt_models WHERE task_id = ?", sub["task_id"])
    (attempt,) = await rows(
        "SELECT cost_usd, tokens FROM attempts WHERE task_id = ?", sub["task_id"]
    )
    assert [r["model"] for r in found] == ["claude-opus-5-5"]
    model = found[0]
    assert model["cost_usd"] == pytest.approx(attempt["cost_usd"])
    total = sum(model[c] for c in ("input_tokens", "output_tokens", "cache_read_tokens",
                                   "cache_write_tokens"))  # fmt: skip
    assert total == attempt["tokens"] and model["cache_write_tokens"] == 1000
