"""Shared engine-test helpers (stub-driven environment, waits, row access)."""

import asyncio
import json
import subprocess
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import procs
from src.config import Settings
from src.database import close_db, get_db, init_db
from src.engine import TaskEngine
from src.redact import Redactor
from src.runstore import RunStore
from src.tasks import Task

AGENTS = ("planner", "implementer", "reviewer", "qa", "lead")
ROLES = """planner: planner
implementer: implementer
reviewers: [reviewer]
qa: qa
lifecycle_agent: lead
aliases: {team-leader: lead, dev: implementer}
"""
TIMEOUT_S = 20.0
# Popen objects of children an engine "restart" made the engine forget (a new engine process
# holds none); the ``spawned`` fixture takes them back at teardown to reap them.
DETACHED: dict[int, Any] = {}


@dataclass
class Env:
    engine: TaskEngine
    settings: Settings
    repo: Path
    log: Path
    tmp: Path


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "test@example.invalid")
    _git(path, "config", "user.name", "test")
    (path / "README").write_text("x\n")
    _git(path, "add", "README")
    _git(path, "commit", "-q", "-m", "init")
    return path


def make_home(home: Path) -> None:
    agents = home / "agents"
    agents.mkdir(parents=True)
    for slug in AGENTS:
        (agents / f"{slug}.md").write_text(f"---\ndescription: {slug}\n---\nYou are {slug}.\n")
    (home / "roles.yaml").write_text(ROLES)


async def wait_for(check: Callable[[], Awaitable[Any]], timeout: float = TIMEOUT_S) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = await check()
        if value:
            return value
        assert time.monotonic() < deadline, "condition not reached in time"
        await asyncio.sleep(0.05)


async def wait_status(engine: TaskEngine, task_id: str, *statuses: str) -> dict[str, Any]:
    async def check() -> dict[str, Any] | None:
        st = await engine.status(task_id)
        return st if st["status"] in statuses else None

    result: dict[str, Any] = await wait_for(check)
    return result


async def task_row(task_id: str) -> Task:
    async with get_db().execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cur:
        row = await cur.fetchone()
    assert row is not None
    return Task.from_row(row)


async def rows(sql: str, *params: Any) -> list[Any]:
    async with get_db().execute(sql, params) as cur:
        return list(await cur.fetchall())


async def running_with_pid(engine: TaskEngine, task_id: str) -> Task:
    async def check() -> Task | None:
        task = await task_row(task_id)
        return task if task.status == "running" and task.pid is not None else None

    result: Task = await wait_for(check)
    return result


def stub_log(log: Path) -> list[dict[str, Any]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


async def submit(env: Env, description: str = "Say done", **kwargs: Any) -> dict[str, Any]:
    agent = kwargs.pop("agent", "implementer")
    context = kwargs.pop("input_context", None)
    result: dict[str, Any] = await env.engine.submit(agent, description, context, 3, 0, **kwargs)
    return result


async def restart(env: Env, start: bool = True) -> TaskEngine:
    """Stop the engine (its children keep running) and start a new one on the same home and
    database, as a server restart does: the new engine holds no Popen of the old one's children
    (liveness by pid + start time only)."""
    old = env.engine
    await old.stop()
    DETACHED.update(procs._children)
    procs._children.clear()
    await close_db()
    await init_db(env.settings.db_path)
    new = TaskEngine(
        env.settings, old.roles, old.agents, RunStore(env.settings.runs_dir), Redactor()
    )
    env.engine = new
    if start:
        await new.start()
    return new
