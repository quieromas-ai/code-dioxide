"""``/activity``: what is happening now — one row per running task and per task waiting on a
human or for a scheduled relaunch. No window: it is the current state."""

from typing import Any

import aiosqlite

from src.engine import TaskEngine
from src.engine_read import iso_or_none, open_attempt_share, scheduling_event, task_usage
from src.reports.query import Filters, where
from src.stages import normalize_stage
from src.tasks import Task

COLUMNS = (
    "task", "agent", "project", "kind", "status", "stage", "stage_agent", "stage_model",
    "last_tool", "last_activity_at", "cost_usd", "tokens", "waiting_on", "since",
)  # fmt: skip


async def _running_step(db: aiosqlite.Connection, task_id: str) -> Any:
    async with db.execute(
        "SELECT label, agent_type, model, last_tool, last_progress_at FROM steps "
        "WHERE task_id = ? AND status = 'running' ORDER BY started_at DESC, rowid DESC LIMIT 1",
        (task_id,),
    ) as cur:
        return await cur.fetchone()


async def _last_ended(db: aiosqlite.Connection, task_id: str) -> float | None:
    async with db.execute(
        "SELECT max(ended_at) AS at FROM attempts WHERE task_id = ?", (task_id,)
    ) as cur:
        row = await cur.fetchone()
    return row["at"] if row is not None else None


async def _open_started(db: aiosqlite.Connection, task_id: str, n: int) -> float | None:
    async with db.execute(
        "SELECT started_at FROM attempts WHERE task_id = ? AND n = ?", (task_id, n)
    ) as cur:
        row = await cur.fetchone()
    return row["started_at"] if row is not None else None


async def _row(db: aiosqlite.Connection, engine: TaskEngine, task: Task) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys(COLUMNS)
    row.update(task=task.id, agent=task.agent, project=task.cwd, kind=task.kind, status=task.status)
    cost, tokens, _ = await task_usage(engine, task, db)
    row.update(cost_usd=cost, tokens=tokens)
    if task.status == "running":
        found = await open_attempt_share(engine, task, db)
        if found is not None:
            n, _ = found
            row["since"] = iso_or_none(await _open_started(db, task.id, n))
            if not task.is_lifecycle and (cached := engine.activity.get(task.id, n)) is not None:
                row.update(
                    last_tool=cached.last_tool,
                    last_activity_at=iso_or_none(cached.last_event_at),
                )
        if task.is_lifecycle and (step := await _running_step(db, task.id)) is not None:
            row.update(
                stage=normalize_stage(step["label"]), stage_agent=step["agent_type"],
                stage_model=step["model"], last_tool=step["last_tool"],
                last_activity_at=iso_or_none(step["last_progress_at"]),
            )  # fmt: skip
    elif task.status == "needs_human":
        row.update(waiting_on="human", since=iso_or_none(await _last_ended(db, task.id)))
    else:
        why, at = await scheduling_event(db, task.id)
        row.update(waiting_on=why, since=iso_or_none(at))
    return row


async def activity_rows(
    db: aiosqlite.Connection, engine: TaskEngine, filters: Filters, now: float
) -> list[dict[str, Any]]:
    cond, args = where(filters, time_column=None)
    async with db.execute(
        f"SELECT t.* FROM tasks t WHERE {cond} AND (t.status IN ('running', 'needs_human') "
        "OR (t.status = 'queued' AND t.next_attempt_at > ?)) ORDER BY t.created_at, t.id",
        [*args, now],
    ) as cur:
        tasks = [Task.from_row(r) for r in await cur.fetchall()]
    rows = [await _row(db, engine, task) for task in tasks]
    if filters.stage_agent is not None:
        rows = [r for r in rows if r["stage_agent"] == filters.stage_agent]
    return rows
