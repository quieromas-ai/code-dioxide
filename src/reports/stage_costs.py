"""Per-stage estimated cost at read time (``src/stage_cost.py``): from ``step_models`` and
``attempt_models``, so a late ``attempt_models`` row or a re-parsed step needs no backfill."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field

import aiosqlite

from src.stage_cost import AttemptModel, Basis, Split, Tokens, ratios_for, split

_Key = tuple[str, int, str]  # (task, attempt n, model)
_CHUNK = 500


@dataclass(frozen=True)
class StageCost:
    """A stage's estimate: None/None when any part is not knowable."""

    estimated_cost_usd: float | None
    cost_basis: Basis | None


@dataclass
class TaskCosts:
    stages: dict[tuple[str, str], StageCost] = field(default_factory=dict)  # (task, label)
    overhead_tokens: dict[tuple[str, int], int | None] = field(default_factory=dict)
    overhead_cost_usd: dict[tuple[str, int], float | None] = field(default_factory=dict)


async def _attempt_models(
    db: aiosqlite.Connection, marks: str, ids: list[str]
) -> dict[_Key, AttemptModel | None]:
    out: dict[_Key, AttemptModel | None] = {}
    async with db.execute(
        "SELECT task_id, n, model, input_tokens, output_tokens, cache_read_tokens, "
        f"cache_write_tokens, cost_usd FROM attempt_models WHERE task_id IN ({marks})",
        ids,
    ) as cur:
        for r in await cur.fetchall():
            key = (r["task_id"], int(r["n"]), r["model"])
            known = r["cost_usd"] is not None and None not in (
                r["input_tokens"], r["output_tokens"], r["cache_read_tokens"],
                r["cache_write_tokens"],
            )  # fmt: skip
            out[key] = (
                AttemptModel(
                    r["input_tokens"],
                    r["output_tokens"],
                    r["cache_read_tokens"],
                    r["cache_write_tokens"],
                    float(r["cost_usd"]),
                )  # fmt: skip
                if known
                else None
            )
    return out


async def _stage_tokens(
    db: aiosqlite.Connection, marks: str, ids: list[str]
) -> tuple[dict[_Key, dict[str, Tokens]], set[tuple[str, str, int]]]:
    """Per (task, attempt, model): each stage's tokens, keyed by label, plus the
    (task, label, attempt) parts of the task's current workflow run. A fresh start leaves the
    old run's usage in place (its attempts' split needs it) but its stages are not the current
    steps, so they get no estimate."""
    out: dict[_Key, dict[str, Tokens]] = defaultdict(dict)
    current: set[tuple[str, str, int]] = set()
    async with db.execute(
        "SELECT m.task_id, m.attempt_n, m.model, m.label, m.run_id = t.run_id AS current, "
        "sum(m.input_tokens) AS i, sum(m.output_tokens) AS o, sum(m.cache_read_tokens) AS r, "
        "sum(m.cache_write_5m_tokens) AS w5, sum(m.cache_write_1h_tokens) AS w1 "
        "FROM step_models m JOIN tasks t ON t.id = m.task_id "
        f"WHERE m.task_id IN ({marks}) GROUP BY m.task_id, m.attempt_n, m.model, m.label, m.run_id",
        ids,
    ) as cur:
        for r in await cur.fetchall():
            n = int(r["attempt_n"])
            key = (r["task_id"], n, r["model"])
            # Two runs can share a label within one attempt only across a fresh start: the parts
            # add up for the attempt's split.
            prior = out[key].get(r["label"], Tokens())
            out[key][r["label"]] = Tokens(
                prior.input + r["i"], prior.output + r["o"], prior.cache_read + r["r"],
                prior.cache_write_5m + r["w5"], prior.cache_write_1h + r["w1"],
            )  # fmt: skip
            if r["current"]:
                current.add((r["task_id"], r["label"], n))
    return out, current


async def task_costs(
    db: aiosqlite.Connection,
    task_ids: list[str],
    overrides: Mapping[str, Mapping[str, float]],
) -> TaskCosts:
    """Stage estimates and parent overhead of ``task_ids``: one ``stage_cost.split`` per
    (attempt, model); a stage sums its parts (exact only if every part is), and is unknown when
    a part's attempt has no known per-model cost. An attempt's overhead is unknown when one of
    its models' cost is."""
    found = TaskCosts()
    attempts: dict[_Key, AttemptModel | None] = {}
    stage_parts: dict[_Key, dict[str, Tokens]] = {}
    current: set[tuple[str, str, int]] = set()
    for start in range(0, len(task_ids), _CHUNK):  # SQLite caps bound variables
        ids = task_ids[start : start + _CHUNK]
        marks = ",".join("?" * len(ids))
        attempts |= await _attempt_models(db, marks, ids)
        parts_of, current_of = await _stage_tokens(db, marks, ids)
        stage_parts |= parts_of
        current |= current_of
    splits: dict[_Key, Split | None] = {
        key: None if attempt is None else split(
            attempt, stage_parts.get(key, {}), ratios_for(key[2], overrides)
        )
        for key, attempt in attempts.items()
    }  # fmt: skip
    parts: dict[tuple[str, str], list[tuple[float, Basis] | None]] = defaultdict(list)
    earlier_runs: dict[tuple[str, int], float] = defaultdict(float)
    for key, stages in stage_parts.items():
        result = splits.get(key)
        for label in stages:
            if (key[0], label, key[1]) not in current:
                # An earlier workflow run's stage (before a fresh start) is not one of the
                # current steps: its share stays with its attempt's overhead, so stages plus
                # overhead still equal the attempt's cost.
                if result is not None:
                    earlier_runs[(key[0], key[1])] += result.stages[label]
                continue
            parts[(key[0], label)].append(
                None if result is None else (result.stages[label], result.basis)
            )
    for stage_key, costs in parts.items():
        known = [c for c in costs if c is not None]
        if len(known) < len(costs):
            found.stages[stage_key] = StageCost(None, None)
            continue
        basis: Basis = "exact" if all(b == "exact" for _, b in known) else "allocated"
        found.stages[stage_key] = StageCost(sum(c for c, _ in known), basis)
    by_attempt: dict[tuple[str, int], list[Split | None]] = defaultdict(list)
    for (task_id, n, _model), result in splits.items():
        by_attempt[(task_id, n)].append(result)
    for attempt_key, results in by_attempt.items():
        costed = [r for r in results if r is not None]
        whole = len(costed) == len(results)  # one model without a known cost: overhead unknown
        found.overhead_tokens[attempt_key] = (
            sum(r.overhead.total for r in costed) if whole else None
        )
        found.overhead_cost_usd[attempt_key] = (
            sum(r.overhead_cost_usd for r in costed) + earlier_runs[attempt_key] if whole else None
        )
    return found
