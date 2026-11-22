"""Fixtures for tests that drive the real ``claude`` CLI through the real engine.

Everything lives under pytest's tmp dirs except what the real CLI itself writes below
``<claude_home>/projects/<slug>/`` for the throwaway project; that is never cleaned up here.
"""

import secrets
import shutil
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src import procs
from src.config import Settings
from src.database import close_db, get_db, init_db
from src.engine import TaskEngine
from src.mcp_server import ToolProvider
from src.redact import Redactor
from src.registry import load_agents
from src.roles import load_roles
from src.runstore import RunStore
from tests.helpers import make_repo

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
LIVE_MODEL = "haiku"  # every stage role
# The orchestrator needs a capable model with a large context window; stages stay cheap.
PARENT_MODEL = "opus"
PARENT_EFFORT = "low"
SCRIPT_MODEL = LIVE_MODEL  # the role-less setup / merge stages


@dataclass
class LiveEnv:
    provider: ToolProvider
    engine: TaskEngine
    settings: Settings
    repo: Path
    token: str = field(repr=False)


@pytest.fixture(scope="session", autouse=True)
def _require_claude() -> None:
    if shutil.which("claude") is None:
        pytest.skip("claude is not on PATH")


@pytest.fixture
def live_repo(tmp_path: Path) -> Path:
    return make_repo(tmp_path / "repo")


@pytest.fixture
def live_home(tmp_path: Path, live_repo: Path) -> Settings:
    """An isolated DIOXIDE_HOME (agents on haiku, example roles) and the settings that use it:
    a lifecycle parent session on opus at low effort, setup and merge on haiku."""
    home = tmp_path / "dioxide-home"
    agents = home / "agents"
    agents.mkdir(parents=True)
    for source in sorted((EXAMPLES / "agents").glob("*.md")):
        lines = source.read_text(encoding="utf-8").splitlines()
        swapped = [f"model: {LIVE_MODEL}" if ln.startswith("model:") else ln for ln in lines]
        (agents / source.name).write_text("\n".join(swapped) + "\n", encoding="utf-8")
    shutil.copy(EXAMPLES / "roles.yaml", home / "roles.yaml")
    claude_bin = shutil.which("claude")
    assert claude_bin is not None
    return Settings(
        home=home,
        auth_token=secrets.token_hex(16),
        claude_bin=claude_bin,
        parent_model=PARENT_MODEL,
        parent_effort=PARENT_EFFORT,
        script_model=SCRIPT_MODEL,
        claude_home=Path("~/.claude").expanduser(),
        default_project_dir=live_repo,
        max_concurrent_tasks=1,
        extra_settings={
            "disableAllHooks": True,
            "permissions": {
                "allow": ["Bash", "Read", "Edit", "Write", "Glob", "Grep"],
                "defaultMode": "acceptEdits",
            },
        },
    )


async def _task_ids() -> list[str]:
    async with get_db().execute("SELECT id FROM tasks") as cur:
        return [row["id"] for row in await cur.fetchall()]


@pytest.fixture
async def live_engine(live_home: Settings, live_repo: Path) -> AsyncIterator[LiveEnv]:
    settings = live_home
    await init_db(settings.db_path)
    agents = load_agents(settings.agents_dir)
    roles = load_roles(settings.roles_path, agents)
    engine = TaskEngine(
        settings, roles, agents, RunStore(settings.runs_dir), Redactor.from_settings(settings)
    )
    provider = ToolProvider()
    provider.attach(engine)
    await engine.start()
    try:
        yield LiveEnv(provider, engine, settings, live_repo, settings.auth_token)
    finally:
        provider.detach()
        await engine.stop()
        leaked = kill_leftovers(engine.runstore, await _task_ids())
        await close_db()
    assert not leaked, f"claude processes left running (killed): {leaked}"


def kill_leftovers(runstore: RunStore, task_ids: list[str]) -> dict[str, list[int]]:
    """Every process a task left running (marker or recorded pid + start), killed; returned by
    task id. For the finalizers: a live test must leave nothing behind."""
    leaked: dict[str, list[int]] = {}
    for task_id in task_ids:
        recorded = runstore.read_procs(task_id)
        alive = procs.marker_pids(task_id) | {
            pid for pid, start in recorded.items() if procs.alive_and_same(pid, start)
        }
        if alive:
            leaked[task_id] = sorted(alive)
            procs.sweep(task_id, recorded, grace_s=3.0)
    return leaked
