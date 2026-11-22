import json
import shutil
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from cli.snapshot_orcai_tools import scrub_schema
from src.config import Settings
from src.database import close_db, init_db
from src.engine import TaskEngine
from src.mcp_server import RETURN_KEYS, ToolProvider, build_mcp
from src.redact import Redactor
from src.registry import load_agents
from src.roles import load_roles
from src.runstore import RunStore

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "orcai_tools.json"
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

TOOL_NAMES = {
    "delegate_task",
    "check_task_status",
    "prompt_agent",
    "get_agents",
    "get_active_agents",
    "answer_task",
    "cancel_task",
    "list_tasks",
    "get_task_logs",
    "get_task_debug",
}
KEPT_TOOLS = (
    "delegate_task",
    "check_task_status",
    "prompt_agent",
    "get_agents",
    "get_active_agents",
)


def load_fixture() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return data


async def dioxide_schemas() -> dict[str, dict[str, Any]]:
    tools = await build_mcp(ToolProvider()).list_tools()
    return {tool.name: tool.input_schema for tool in tools}


def test_fixture_snapshots_all_orcai_tools() -> None:
    fixture = load_fixture()
    assert len(fixture["tools"]) == 12
    assert set(fixture["return_keys"]) == set(KEPT_TOOLS)


async def test_exact_tool_set() -> None:
    assert set(await dioxide_schemas()) == TOOL_NAMES
    assert len(await dioxide_schemas()) == 10


@pytest.mark.parametrize("tool", KEPT_TOOLS)
async def test_kept_tools_params_match_orcai(tool: str) -> None:
    orcai = {t["name"]: t["inputSchema"] for t in load_fixture()["tools"]}[tool]
    ours = scrub_schema((await dioxide_schemas())[tool])
    assert ours.get("properties", {}) == orcai.get("properties", {})
    assert sorted(ours.get("required", [])) == sorted(orcai.get("required", []))
    assert ours.get("type") == orcai.get("type")


@pytest.mark.parametrize("tool", KEPT_TOOLS)
def test_kept_tools_return_keys_superset(tool: str) -> None:
    documented = set(load_fixture()["return_keys"][tool])
    assert documented, f"fixture documents no return keys for {tool}"
    missing = documented - RETURN_KEYS[tool]
    assert not missing, f"{tool} drops orcai return keys {sorted(missing)}"


def test_check_task_status_keeps_nested_output_keys() -> None:
    assert {"output.text", "output.tokens_used"} <= RETURN_KEYS["check_task_status"]


def example_home(tmp_path: Path) -> Settings:
    shutil.copytree(EXAMPLES / "agents", tmp_path / "agents")
    shutil.copy(EXAMPLES / "roles.yaml", tmp_path / "roles.yaml")
    return Settings(home=tmp_path)


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[ToolProvider]:
    settings = example_home(tmp_path)
    await init_db(settings.db_path)
    agents = load_agents(settings.agents_dir)
    roles = load_roles(settings.roles_path, agents)
    engine = TaskEngine(settings, roles, agents, RunStore(settings.runs_dir), Redactor())
    tools = ToolProvider()
    tools.attach(engine)
    try:
        yield tools
    finally:
        await close_db()


async def test_get_agents_lists_registry_with_roles(provider: ToolProvider) -> None:
    listed = await provider.get_agents(role=None, status=None)
    assert listed["total"] == 5
    by_id = {agent["id"]: agent for agent in listed["agents"]}
    assert set(by_id["planner"]) == {"id", "name", "description", "role", "status"}
    assert by_id["planner"]["role"] == "planner,lifecycle_agent"
    assert {agent["status"] for agent in listed["agents"]} == {"idle"}
    reviewers = await provider.get_agents(role="reviewer", status=None)
    assert [agent["id"] for agent in reviewers["agents"]] == ["reviewer"]
    assert (await provider.get_agents(role=None, status="busy"))["total"] == 0


async def test_get_active_agents_empty_when_idle(provider: ToolProvider) -> None:
    active = await provider.get_active_agents()
    assert active == {"agents": [], "active_count": 0, "queue_depth": 0}


async def test_tools_report_engine_not_started() -> None:
    provider = ToolProvider()
    expected = {"error": "engine not started", "status": "unavailable"}
    assert await provider.delegate_task("planner", "do it", None, 3, 0) == expected
    assert await provider.check_task_status("t1") == expected
    assert await provider.get_agents(None, None) == expected
    assert await provider.cancel_task("t1") == expected
    assert await provider.answer_task("t1", "yes") == expected
    assert await provider.get_task_logs("t1", None, None, 200, False) == expected
    assert await provider.get_task_debug("t1") == expected
    assert await provider.task_counts() == {"queued": 0, "running": 0, "needs_human": 0}


async def test_cancel_task_description_covers_needs_human() -> None:
    tools = {tool.name: tool for tool in await build_mcp(ToolProvider()).list_tools()}
    assert "needs_human" in (tools["cancel_task"].description or "")
    assert set(tools["cancel_task"].input_schema["properties"]) == {"task_id"}


def test_check_task_status_adds_recovery_keys_keeping_orcai_ones() -> None:
    orcai = set(load_fixture()["return_keys"]["check_task_status"])
    added = {"waiting", "waiting.until", "waiting.why", "kill_reason", "recovery"}
    added |= {f"recovery.{k}" for k in ("resumes", "call_retries", "stage_retries")}
    added.add("recovery.rate_limit_waited_s")
    assert orcai | added <= RETURN_KEYS["check_task_status"]
    assert not orcai & added  # only additions: no orcai key changes meaning
