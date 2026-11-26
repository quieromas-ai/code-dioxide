"""Recompute recorded usage from the stored streams (``dioxide repair-usage``).

Rows written before per-attempt shares (``src.usage``) hold a resumed session's cumulative cost
and the top-level tokens only. Each attempt's share is computed again from its stream's last
result record, in attempt order per session, and a task output's ``tokens_used``/``cost_usd``
become the task's totals. A task with an open attempt or a pruned stream is left as it is.
"""

import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from src.database import BACKUP_MODE
from src.stream import Result, StreamWatcher
from src.usage import FLOAT_NOISE, Usage, attempt_usage


@dataclass(frozen=True)
class AttemptFix:
    n: int
    old: Usage
    new: Usage


@dataclass(frozen=True)
class TaskFix:
    task_id: str
    attempts: list[AttemptFix]
    output_json: str | None  # the new output, None when it stays


@dataclass(frozen=True)
class Skipped:
    task_id: str
    reason: str


@dataclass
class Plan:
    fixes: list[TaskFix] = field(default_factory=list)
    skipped: list[Skipped] = field(default_factory=list)


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
    counted: dict[str | None, Usage] = {}
    fixes: list[AttemptFix] = []
    shares: list[Usage] = []
    for a in attempts:
        session = a["session_id"]
        prior = counted.get(session, Usage(None, None)) if session else Usage(None, None)
        share = attempt_usage(last_result(Path(a["stream_path"])), prior)
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
    return TaskFix(task_id, fixes, _new_output(raw_output, totals))


def plan_repair(conn: sqlite3.Connection) -> Plan:
    """What a repair would change; reads only. ``conn`` uses ``sqlite3.Row`` rows."""
    plan = Plan()
    for task in conn.execute("SELECT id, output_json FROM tasks ORDER BY created_at, id"):
        found = _task(conn, task["id"], task["output_json"])
        if isinstance(found, Skipped):
            plan.skipped.append(found)
        elif found.attempts or found.output_json is not None:
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


def backup(conn: sqlite3.Connection, db_path: Path) -> Path:
    """A consistent copy at ``<name>.repair-<unix time>.bak`` (mode 0600)."""
    target = db_path.with_name(f"{db_path.name}.repair-{int(time.time())}.bak")
    partial = target.with_name(f".{target.name}.tmp")
    partial.unlink(missing_ok=True)
    conn.execute("VACUUM INTO ?", (str(partial),))
    partial.chmod(BACKUP_MODE)
    os.replace(partial, target)
    return target
