"""Recompute recorded usage from the stored streams (``dioxide repair-usage``).

Rows written before per-attempt shares (``src.usage``) hold a resumed session's cumulative cost
and the top-level tokens only. Each attempt's share is computed again from its stream's last
result record, in attempt order per session, and a task output's ``tokens_used``/``cost_usd``
become the task's totals. A task with an open attempt or a pruned stream is left as it is.

Attempts recorded before schema v8 get their per-model shares (``attempt_models``) the same
way, from each stream's last result record (``model_usage.model_shares``).

Step rows mirrored before schema v7 have no stage numbers: they are filled from the run-store
copy of the run file (``runs/<task>/run.json``) where it still has the stage's progress entry.
"""

import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from src.database import BACKUP_MODE, STAGE_COLUMNS
from src.model_usage import model_shares
from src.runstore import RunStore
from src.stages import StageNumbers, stage_numbers
from src.stream import ModelUsage, Result, StreamWatcher
from src.usage import FLOAT_NOISE, Usage, attempt_usage


@dataclass(frozen=True)
class AttemptFix:
    n: int
    old: Usage
    new: Usage


@dataclass(frozen=True)
class ModelFix:
    n: int
    shares: dict[str, ModelUsage]


@dataclass(frozen=True)
class TaskFix:
    task_id: str
    attempts: list[AttemptFix]
    output_json: str | None  # the new output, None when it stays
    # attempts whose split differs from the recomputed one (empty shares: the split is removed)
    models: list[ModelFix] = field(default_factory=list)


@dataclass(frozen=True)
class Skipped:
    task_id: str
    reason: str


@dataclass(frozen=True)
class StageFix:
    task_id: str
    label: str
    numbers: StageNumbers


@dataclass
class Plan:
    fixes: list[TaskFix] = field(default_factory=list)
    skipped: list[Skipped] = field(default_factory=list)
    stages: list[StageFix] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (self.fixes or self.stages)


def last_result(path: Path) -> Result | None:
    """The stream's last result record (the whole file is read), or None."""
    results = [e for e in StreamWatcher(path).poll() if isinstance(e, Result)]
    return results[-1] if results else None


def _sum(values: list[float | None]) -> float | None:
    known = [v for v in values if v is not None]
    return sum(known) if known else None  # SQL SUM: NULL when nothing is known


def _same(a: Usage, b: Usage) -> bool:
    """Equal, cost within float noise (the engine sums priors in SQL, the repair in Python)."""
    if a.tokens != b.tokens or (a.cost_usd is None) != (b.cost_usd is None):
        return False
    return a.cost_usd is None or math.isclose(a.cost_usd, b.cost_usd or 0.0, abs_tol=FLOAT_NOISE)


def _new_output(raw: str | None, totals: Usage) -> str | None:
    try:
        out = json.loads(raw) if raw else None
    except ValueError:
        return None
    if not isinstance(out, dict) or not {"tokens_used", "cost_usd"} <= out.keys():
        return None
    tokens, cost = out["tokens_used"], out["cost_usd"]
    numeric = cost is None or (isinstance(cost, int | float) and not isinstance(cost, bool))
    if numeric and _same(Usage(cost, tokens), totals):
        return None
    out["tokens_used"], out["cost_usd"] = totals.tokens, totals.cost_usd
    return json.dumps(out, ensure_ascii=False)


def _task(conn: sqlite3.Connection, task_id: str, raw_output: str | None) -> TaskFix | Skipped:
    attempts = conn.execute(
        "SELECT n, session_id, stream_path, ended_at, cost_usd, tokens FROM attempts "
        "WHERE task_id = ? ORDER BY n",
        (task_id,),
    ).fetchall()
    if any(a["ended_at"] is None for a in attempts):
        return Skipped(task_id, "an attempt is still open")
    missing = [
        a["n"] for a in attempts if not a["stream_path"] or not Path(a["stream_path"]).is_file()
    ]
    if missing:
        return Skipped(task_id, f"stream pruned (attempt {', '.join(map(str, missing))})")
    stored: dict[int, dict[str, ModelUsage]] = {}
    for r in conn.execute(
        "SELECT n, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
        "cost_usd FROM attempt_models WHERE task_id = ?",
        (task_id,),
    ):
        stored.setdefault(r["n"], {})[r["model"]] = ModelUsage(
            r["input_tokens"] or 0, r["output_tokens"] or 0, r["cache_read_tokens"] or 0,
            r["cache_write_tokens"] or 0, r["cost_usd"],
        )  # fmt: skip
    counted: dict[str | None, Usage] = {}
    # Per session, the per-model shares so far; None once an attempt has no breakdown.
    by_model: dict[str | None, dict[str, ModelUsage] | None] = {}
    fixes: list[AttemptFix] = []
    model_fixes: list[ModelFix] = []
    shares: list[Usage] = []
    for a in attempts:
        session = a["session_id"]
        prior = counted.get(session, Usage(None, None)) if session else Usage(None, None)
        result = last_result(Path(a["stream_path"]))
        share = attempt_usage(result, prior)
        prior_models = by_model.get(session, {}) if session else {}
        models = model_shares(result, prior, prior_models)
        if session:
            by_model[session] = _add_models(prior_models, models, share)
        # The split follows the share it is recomputed with: one written against a wrong prior
        # (an earlier attempt closed without usage) is replaced, a stale one removed.
        if not _same_split(stored.get(a["n"], {}), models or {}):
            model_fixes.append(ModelFix(a["n"], models or {}))
        shares.append(share)
        if session:
            counted[session] = Usage(
                _sum([prior.cost_usd, share.cost_usd]),
                None if (t := _sum([prior.tokens, share.tokens])) is None else int(t),
            )
        old = Usage(a["cost_usd"], a["tokens"])
        if not _same(old, share):
            fixes.append(AttemptFix(a["n"], old, share))
    tokens = _sum([s.tokens for s in shares])
    totals = Usage(_sum([s.cost_usd for s in shares]), None if tokens is None else int(tokens))
    return TaskFix(task_id, fixes, _new_output(raw_output, totals), model_fixes)


def _same_split(a: dict[str, ModelUsage], b: dict[str, ModelUsage]) -> bool:
    if a.keys() != b.keys():
        return False
    for model, x in a.items():
        y = b[model]
        if (x.input_tokens, x.output_tokens, x.cache_read_tokens, x.cache_write_tokens) != (
            y.input_tokens, y.output_tokens, y.cache_read_tokens, y.cache_write_tokens,
        ):  # fmt: skip
            return False
        if (x.cost_usd is None) != (y.cost_usd is None) or (
            x.cost_usd is not None
            and not math.isclose(x.cost_usd, y.cost_usd or 0.0, abs_tol=FLOAT_NOISE)
        ):
            return False
    return True


def _add_models(
    prior: dict[str, ModelUsage] | None, models: dict[str, ModelUsage] | None, share: Usage
) -> dict[str, ModelUsage] | None:
    """The session's per-model sums after an attempt; None once one with tokens has no split
    (as ``model_usage.prior_models`` decides from the rows)."""
    if prior is None or (models is None and share.tokens is not None):
        return None
    if models is None:
        return prior
    total = dict(prior)
    for model, m in models.items():
        p = total.get(model, ModelUsage(0, 0, 0, 0, None))
        cost = (
            None
            if p.cost_usd is None and m.cost_usd is None
            else ((p.cost_usd or 0.0) + (m.cost_usd or 0.0))
        )
        total[model] = ModelUsage(
            p.input_tokens + m.input_tokens, p.output_tokens + m.output_tokens,
            p.cache_read_tokens + m.cache_read_tokens, p.cache_write_tokens + m.cache_write_tokens,
            cost,
        )  # fmt: skip
    return total


def _progress(runs: RunStore, task_id: str) -> dict[str, dict[str, object]]:
    """The run-store run file's progress entries by label; empty when missing or unreadable."""
    try:
        run = json.loads((runs.task_dir(task_id) / "run.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):  # ValueError: not JSON, or not a valid task id
        return {}
    progress = run.get("workflowProgress") if isinstance(run, dict) else None
    if not isinstance(progress, list):
        return {}
    return {
        p["label"]: p for p in progress if isinstance(p, dict) and isinstance(p.get("label"), str)
    }


_NO_NUMBERS = " AND ".join(f"{column} IS NULL" for column, _ in STAGE_COLUMNS)


def _stage_fixes(conn: sqlite3.Connection, runs_dir: Path) -> list[StageFix]:
    found: list[StageFix] = []
    progress: dict[str, dict[str, dict[str, object]]] = {}
    for row in conn.execute(
        f"SELECT task_id, label FROM steps WHERE {_NO_NUMBERS} ORDER BY task_id, label"
    ):
        task_id = row["task_id"]
        if task_id not in progress:
            progress[task_id] = _progress(RunStore(runs_dir), task_id)
        numbers = stage_numbers(progress[task_id].get(row["label"]))
        if any(value is not None for value in vars(numbers).values()):
            found.append(StageFix(task_id, row["label"], numbers))
    return found


def plan_repair(conn: sqlite3.Connection, runs_dir: Path | None = None) -> Plan:
    """What a repair would change; reads only. ``conn`` uses ``sqlite3.Row`` rows. Stage
    numbers are planned only with ``runs_dir`` (the run store)."""
    plan = Plan(stages=_stage_fixes(conn, runs_dir) if runs_dir is not None else [])
    for task in conn.execute("SELECT id, output_json FROM tasks ORDER BY created_at, id"):
        found = _task(conn, task["id"], task["output_json"])
        if isinstance(found, Skipped):
            plan.skipped.append(found)
        elif found.attempts or found.output_json is not None or found.models:
            plan.fixes.append(found)
    return plan


def apply_plan(conn: sqlite3.Connection, plan: Plan) -> None:
    """Write ``plan`` in one transaction."""
    with conn:
        for fix in plan.fixes:
            for a in fix.attempts:
                conn.execute(
                    "UPDATE attempts SET cost_usd = ?, tokens = ? WHERE task_id = ? AND n = ?",
                    (a.new.cost_usd, a.new.tokens, fix.task_id, a.n),
                )
            if fix.output_json is not None:
                conn.execute(
                    "UPDATE tasks SET output_json = ? WHERE id = ?", (fix.output_json, fix.task_id)
                )
            for m in fix.models:
                conn.execute(
                    "DELETE FROM attempt_models WHERE task_id = ? AND n = ?", (fix.task_id, m.n)
                )
                for model, share in m.shares.items():
                    conn.execute(
                        "INSERT INTO attempt_models (task_id, n, model, input_tokens, "
                        "output_tokens, cache_read_tokens, cache_write_tokens, cost_usd) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (fix.task_id, m.n, model, share.input_tokens, share.output_tokens,
                         share.cache_read_tokens, share.cache_write_tokens, share.cost_usd),
                    )  # fmt: skip
        for stage in plan.stages:
            n = stage.numbers
            conn.execute(
                "UPDATE steps SET agent_type = ?, model = ?, stage_tokens = ?, tool_calls = ?, "
                f"last_tool = ?, duration_s = ?, last_progress_at = ? WHERE {_NO_NUMBERS} "
                "AND task_id = ? AND label = ?",
                (n.agent_type, n.model, n.stage_tokens, n.tool_calls, n.last_tool, n.duration_s,
                 n.last_progress_at, stage.task_id, stage.label),
            )  # fmt: skip


def backup(conn: sqlite3.Connection, db_path: Path) -> Path:
    """A consistent copy at ``<name>.repair-<unix time>.bak`` (mode 0600)."""
    target = db_path.with_name(f"{db_path.name}.repair-{int(time.time())}.bak")
    partial = target.with_name(f".{target.name}.tmp")
    partial.unlink(missing_ok=True)
    conn.execute("VACUUM INTO ?", (str(partial),))
    partial.chmod(BACKUP_MODE)
    os.replace(partial, target)
    return target
