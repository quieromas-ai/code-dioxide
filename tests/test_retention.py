"""Run store retention: terminal tasks' run dirs go once they ended more than
``run_retention_days`` ago; every other task's dir stays, whatever its age."""

import json
import logging
import os
import tarfile
import time
from collections.abc import Iterator

import pytest

from src import bundle, debug, retention
from src.database import write_tx
from src.runstore import RunStore
from src.serverlog import LOGGER_NAME
from tests.helpers import Env, wait_for

DAY = 86400.0


class Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def server_log() -> Iterator[Records]:
    handler = Records()
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(handler)
    level = logger.level
    logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


async def add_task(env: Env, task_id: str, status: str, ended: float | None, age: float) -> None:
    """A task row and its run dir; ``age``: seconds since creation (also the dir's mtime)."""
    created = time.time() - age
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO tasks (id, status, created_at, updated_at, completed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_id, status, created, ended if ended is not None else created, ended),
        )
    run = env.engine.runstore.task_dir(task_id)
    (run / "inputs").mkdir(parents=True)
    (run / "inputs" / "brief.md").write_text("brief\n")
    for path in (run / "inputs" / "brief.md", run / "inputs", run):
        os.utime(path, (created, created))


def present(env: Env, *task_ids: str) -> set[str]:
    return {t for t in task_ids if env.engine.runstore.task_dir(t).is_dir()}


async def test_old_terminal_run_pruned(env: Env, server_log: Records) -> None:
    now = time.time()
    await add_task(env, "old-done", "completed", now - 91 * DAY, 100 * DAY)
    await add_task(env, "old-failed", "failed", now - 120 * DAY, 120 * DAY)
    await add_task(env, "young-done", "cancelled", now - 1 * DAY, 100 * DAY)  # created long ago
    # Ended long ago, but its dir was touched recently: the end time decides.
    await add_task(env, "touched", "completed", now - 95 * DAY, 0)
    assert await retention.prune_runs(env.engine) == 3
    assert present(env, "old-done", "old-failed", "young-done", "touched") == {"young-done"}
    lines = [r for r in server_log.records if r.getMessage() == "run store retention"]
    assert len(lines) == 1 and lines[0].__dict__["pruned"] == 3


async def test_non_terminal_runs_kept_whatever_age(env: Env) -> None:
    for status in ("queued", "running", "needs_human"):
        await add_task(env, status, status, None, 400 * DAY)
    assert await retention.prune_runs(env.engine) == 0
    assert present(env, "queued", "running", "needs_human") == {"queued", "running", "needs_human"}


async def test_zero_retention_prunes_nothing(env: Env) -> None:
    env.settings.run_retention_days = 0
    await add_task(env, "ancient", "completed", time.time() - 900 * DAY, 900 * DAY)
    assert await retention.prune_runs(env.engine) is None
    assert present(env, "ancient") == {"ancient"}


async def test_prune_failure_logged_engine_unaffected(
    env: Env, server_log: Records, monkeypatch: pytest.MonkeyPatch
) -> None:
    await add_task(env, "old", "completed", time.time() - 91 * DAY, 91 * DAY)

    def broken(self: RunStore, task_id: str) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr(RunStore, "remove", broken)
    assert await retention.prune_runs(env.engine) is None
    assert any(r.getMessage() == "run store retention pass failed" for r in server_log.records)


async def test_prune_at_boot_then_daily(env: Env) -> None:
    env.settings.watchdog_interval_s = 0.05
    now = time.time()
    await add_task(env, "at-boot", "completed", now - 91 * DAY, 91 * DAY)
    clock = [now]
    env.engine._clock = lambda: clock[0]
    await env.engine.start()
    assert present(env, "at-boot") == set()  # pruned during start, before it returns

    await add_task(env, "later", "completed", now - 91 * DAY, 91 * DAY)
    ticks = env.engine._watchdog_ticks

    async def passes() -> bool:
        return env.engine._watchdog_ticks >= ticks + 3

    await wait_for(passes)
    assert present(env, "later") == {"later"}  # less than a day since the last pass

    clock[0] = now + DAY + 1

    async def pruned() -> bool:
        return not present(env, "later")

    await wait_for(pruned)


async def test_retry_and_live_tasks_kept(env: Env) -> None:
    """A terminal task the watchdog still retries a sweep for, or with a live job, keeps its
    run dir however old."""
    old = time.time() - 120 * DAY
    for task_id in ("retrying", "live", "gone"):
        await add_task(env, task_id, "failed", old, 120 * DAY)
    env.engine.book.sweep_retry["retrying"] = ({}, "cancel", 1, 0.0)
    env.engine._attempts["live"] = object()  # type: ignore[assignment]
    try:
        assert await retention.prune_runs(env.engine) == 1
    finally:
        env.engine._attempts.pop("live")
        env.engine.book.sweep_retry.pop("retrying")
    assert present(env, "retrying", "live", "gone") == {"retrying", "live"}


async def test_removed_count_only_dirs_gone(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    old = time.time() - 120 * DAY
    await add_task(env, "stuck", "completed", old, 120 * DAY)
    await add_task(env, "freed", "completed", old, 120 * DAY)
    real = RunStore.remove

    def partial(self: RunStore, task_id: str) -> None:
        if task_id != "stuck":  # a dir that survives its removal (ignore_errors)
            real(self, task_id)

    monkeypatch.setattr(RunStore, "remove", partial)
    assert await retention.prune_runs(env.engine) == 1
    assert present(env, "stuck", "freed") == {"stuck"}


async def test_pruned_run_marked_in_logs_debug_bundle(env: Env) -> None:
    await add_task(env, "pruned", "completed", time.time() - 120 * DAY, 120 * DAY)
    await add_task(env, "kept", "completed", time.time() - DAY, DAY)
    assert await retention.prune_runs(env.engine) == 1
    page = await debug.task_logs(env.engine, "pruned")
    assert page["run_store"] == "pruned" and page["done"] is True
    assert (await debug.task_debug(env.engine, "pruned"))["run_store"] == "pruned"
    assert "run_store" not in await debug.task_logs(env.engine, "kept")
    assert "run_store" not in await debug.task_debug(env.engine, "kept")
    settings = env.settings
    for task_id, marked in (("pruned", True), ("kept", False)):
        built = await bundle.task_bundle(
            task_id, env.engine.runstore.task_dir(task_id), settings.server_log_path,
            env.engine.redactor, settings.data_dir,
        )  # fmt: skip
        assert built is not None
        tmp, archive = built
        try:
            with tarfile.open(archive) as tar:
                member = tar.extractfile(f"{task_id}/task.json")
                assert member is not None
                rows = json.load(member)
        finally:
            bundle.cleanup(tmp)
        assert (rows.get("run_store") == "pruned") is marked
