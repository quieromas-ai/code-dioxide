"""What a lifecycle stage spent and changed, measured server-side when the step mirror sees it
complete: billed usage per (agent run, model) from its transcript (``step_models``, summed onto
the ``steps`` row) and the files its commits changed (``git diff --name-status``)."""

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

from src.journal import StageModelUsage


@dataclass(frozen=True)
class StepRun:
    """Where a step's measurement belongs: the task, the step, the workflow run, the attempt."""

    task_id: str
    label: str
    run_id: str
    attempt_n: int


GIT_DIFF_TIMEOUT_S = 10.0
_SUM_COLUMNS = (
    ("billed_input_tokens", "input_tokens"),
    ("billed_output_tokens", "output_tokens"),
    ("billed_cache_read_tokens", "cache_read_tokens"),
    ("billed_cache_write_tokens", "cache_write_5m_tokens + cache_write_1h_tokens"),
    ("turns", "turns"),
)


async def write_usage(
    db: aiosqlite.Connection, at: StepRun, agent_id: str, usage: dict[str, StageModelUsage]
) -> None:
    """Store one agent run's usage of ``at.label`` and refresh the step's sums over every agent
    run of that label in the current workflow run (a restarted step adds its new agent's run; a
    fresh start's new run does not inherit the old one's). ``attempt_n`` is written once: a
    re-parse (the final mirror) keeps the attempt that ran the agent."""
    for model, u in usage.items():
        await db.execute(
            "INSERT INTO step_models (task_id, label, agent_id, run_id, attempt_n, model, "
            "input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, "
            "cache_write_1h_tokens, turns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (task_id, label, agent_id, model) DO UPDATE SET "
            "input_tokens = excluded.input_tokens, output_tokens = excluded.output_tokens, "
            "cache_read_tokens = excluded.cache_read_tokens, "
            "cache_write_5m_tokens = excluded.cache_write_5m_tokens, "
            "cache_write_1h_tokens = excluded.cache_write_1h_tokens, turns = excluded.turns",
            (at.task_id, at.label, agent_id, at.run_id, at.attempt_n, model, u.input_tokens,
             u.output_tokens, u.cache_read_tokens, u.cache_write_5m_tokens,
             u.cache_write_1h_tokens, u.turns),
        )  # fmt: skip
    sets = ", ".join(
        f"{column} = (SELECT coalesce(sum({expr}), 0) FROM step_models m "
        "WHERE m.task_id = steps.task_id AND m.label = steps.label AND m.run_id = ?)"
        for column, expr in _SUM_COLUMNS
    )
    await db.execute(
        f"UPDATE steps SET {sets} WHERE task_id = ? AND label = ?",
        [*([at.run_id] * len(_SUM_COLUMNS)), at.task_id, at.label],
    )


async def attempt_at(
    db: aiosqlite.Connection, task_id: str, ended: float | None, observing: int
) -> int:
    """The attempt whose run produced an agent's transcript: the latest attempt started by the
    transcript's last record (a step first seen by a later attempt, after an engine restart or
    an upgrade, still belongs to the one that ran it); ``observing`` when that is unknown."""
    if ended is None:
        return observing
    async with db.execute(
        "SELECT max(n) AS n FROM attempts WHERE task_id = ? AND started_at <= ?", (task_id, ended)
    ) as cur:
        row = await cur.fetchone()
    return int(row["n"]) if row is not None and row["n"] is not None else observing


def files_changed(worktree: str | None, before: str | None, after: str | None) -> Any:
    """``[{"status", "path"}]`` between two commits of the worktree (``git diff --name-status
    --no-renames``: a rename is a ``D`` and an ``A``); ``[]`` when the head did not move; None
    when not knowable (no worktree, a missing head, a failing diff)."""
    if not worktree or not before or not after or not Path(worktree).is_dir():
        return None
    if before == after:
        return []
    try:
        done = subprocess.run(
            ["git", "-C", worktree, "diff", "--name-status", "--no-renames", before, after],
            capture_output=True, text=True, timeout=GIT_DIFF_TIMEOUT_S, check=False,
        )  # fmt: skip
    except (OSError, subprocess.SubprocessError):
        return None
    if done.returncode != 0:
        return None
    found = []
    for line in done.stdout.splitlines():
        status, _, path = line.partition("\t")
        if status and path:
            found.append({"status": status, "path": path})
    return found
