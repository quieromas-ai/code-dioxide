import os
import signal
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from src import procs
from src.config import Settings
from src.database import close_db, init_db
from src.engine import TaskEngine
from src.redact import Redactor
from src.registry import load_agents
from src.roles import load_roles
from src.runstore import RunStore
from tests.helpers import DETACHED, Env, make_home, make_repo
from tests.stub_claude import make_launcher


@pytest.fixture(autouse=True)
def _isolate_dioxide_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Keep tests off the real home and ambient DIOXIDE_* variables."""
    for key in list(os.environ):
        if key.startswith("DIOXIDE_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path_factory.mktemp("dioxide-home")))


@pytest.fixture
def spawned(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[int, float | None]]]:
    """Every process the engine spawns; the test fails if one is still alive at teardown."""
    started: list[tuple[int, float | None]] = []
    real = procs.spawn

    def recording(*args: Any, **kwargs: Any) -> tuple[int, float | None]:
        pid, start = real(*args, **kwargs)
        started.append((pid, start))
        return pid, start

    monkeypatch.setattr(procs, "spawn", recording)
    yield started
    procs._children.update(DETACHED)  # children an engine restart detached from
    DETACHED.clear()
    leaked = []
    for pid, start in started:
        procs.reap(pid)
        if procs.alive_and_same(pid, start):
            leaked.append(pid)
            os.kill(pid, signal.SIGKILL)
            procs.reap(pid)
    assert not leaked, f"stub processes left running: {leaked}"


@pytest.fixture
async def env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    spawned: list[tuple[int, float | None]],
) -> AsyncIterator[Env]:
    home = tmp_path / "home"
    make_home(home)
    repo = make_repo(tmp_path / "repo")
    log = tmp_path / "stub.log"
    monkeypatch.setenv("STUB_CLAUDE_HOME", str(tmp_path / "claude-home"))
    monkeypatch.setenv("STUB_CLAUDE_LOG", str(log))
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "single_success")
    settings = Settings(
        home=home,
        claude_bin=str(make_launcher(tmp_path / "bin")),
        claude_home=tmp_path / "claude-home",
        default_project_dir=repo,
        auth_disabled=True,
    )
    await init_db(settings.db_path)
    agents = load_agents(settings.agents_dir)
    roles = load_roles(settings.roles_path, agents)
    engine = TaskEngine(settings, roles, agents, RunStore(settings.runs_dir), Redactor())
    try:
        yield Env(engine, settings, repo, log, tmp_path)
    finally:
        await engine.stop()
        await close_db()
