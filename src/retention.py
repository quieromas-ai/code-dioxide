"""Run store retention: delete the run dirs of terminal tasks that ended more than
``run_retention_days`` ago (``0`` disables it).

Runs once at boot, after re-attach, and then once a day from the watchdog loop (the engine's
clock). Every task that is not terminal is kept, whatever its age (a ``needs_human`` task may
wait longer than the retention and still needs its ``script/`` and ``inputs/``). A terminal task
is kept while its end time (``completed_at``, else ``updated_at``) is within the limit, and so is
one the engine still works on: a live job, or a leftover sweep the watchdog retries. A run dir
with no task row (a submit that never inserted its row) is aged by its newest mtime. The file
work runs off the event loop; a failure is logged and never stops the engine. The task rows
stay: only the run dirs are deleted.
"""

import asyncio
import logging
from typing import TYPE_CHECKING

from src.database import get_db
from src.runstore import RunStore
from src.serverlog import LOGGER_NAME
from src.tasks import TERMINAL

if TYPE_CHECKING:
    from src.engine import TaskEngine

DAY_S = 86400.0
_log = logging.getLogger(LOGGER_NAME)


def _remove(store: RunStore, days: float, now: float, keep: set[str], expired: set[str]) -> int:
    """Blocking: delete the expired tasks' dirs, then rowless dirs older than the limit."""
    removed = 0
    for task_id in sorted(expired):
        run_dir = store.task_dir(task_id)
        if run_dir.is_dir():
            store.remove(task_id)
            removed += not run_dir.exists()  # only a dir actually gone counts
    return removed + len(store.prune(days, keep | expired, now))


async def prune_runs(engine: "TaskEngine") -> int | None:
    """One retention pass; the number of run dirs deleted, None when disabled or failed."""
    days = engine.settings.run_retention_days
    if days == 0:
        return None
    now = engine._clock()
    cutoff = now - days * DAY_S
    try:
        async with get_db().execute(
            "SELECT id, status, completed_at, updated_at FROM tasks"
        ) as cur:
            rows = await cur.fetchall()
        keep: set[str] = set()
        expired: set[str] = set()
        for row in rows:
            ended = row["completed_at"] if row["completed_at"] is not None else row["updated_at"]
            if row["status"] in TERMINAL and ended < cutoff:
                expired.add(row["id"])
            else:
                keep.add(row["id"])
        busy = {*engine.book.sweep_retry, *engine._attempts, *engine._live}
        keep |= busy
        expired -= busy
        count = await asyncio.to_thread(_remove, engine.runstore, days, now, keep, expired)
    except Exception:
        _log.exception("run store retention pass failed")
        return None
    _log.info("run store retention", extra={"pruned": count, "retention_days": days})
    return count


async def maybe_prune(engine: "TaskEngine") -> None:
    """From the watchdog loop: a retention pass once a day (by the engine's clock)."""
    now = engine._clock()
    if engine._last_prune_at is not None and now - engine._last_prune_at < DAY_S:
        return
    engine._last_prune_at = now
    await prune_runs(engine)
