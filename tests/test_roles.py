import json
from pathlib import Path

import pytest

from src.registry import AgentDef, load_agents
from src.roles import Roles, load_roles

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
SLUGS = ["planner", "implementer", "reviewer", "qa", "deployer", "lead"]


@pytest.fixture
def agents() -> dict[str, AgentDef]:
    return {
        slug: AgentDef(slug=slug, description=slug, prompt=f"You are {slug}.") for slug in SLUGS
    }


BASE = {
    "planner": "planner",
    "implementer": "implementer",
    "reviewers": ["reviewer"],
    "qa": "qa",
    "deployer": "deployer",
    "lifecycle_agent": "lead",
    "aliases": {"team-leader": "lead"},
}


def write_roles(tmp_path: Path, **overrides: object) -> Path:
    data: dict[str, object] = {**BASE, **overrides}
    lines = []
    for key, value in data.items():
        if value is not None:
            lines.append(f"{key}: {json.dumps(value)}")
    path = tmp_path / "roles.yaml"
    path.write_text("\n".join(lines) + "\n")
    return path


def test_load_roles_ok(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    roles = load_roles(write_roles(tmp_path), agents)
    assert isinstance(roles, Roles)
    assert roles.reviewers == ["reviewer"]
    assert roles.lifecycle_agent == "lead"
    assert roles.branch_template == "dioxide/{slug}-{id8}"


@pytest.mark.parametrize("key", ["planner", "implementer", "qa", "lifecycle_agent"])
def test_roles_missing_agent_raises(tmp_path: Path, agents: dict[str, AgentDef], key: str) -> None:
    path = write_roles(tmp_path, **{key: "ghost"})
    with pytest.raises(ValueError, match="ghost"):
        load_roles(path, agents)


def test_roles_missing_reviewer_agent_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="ghost"):
        load_roles(write_roles(tmp_path, reviewers=["reviewer", "ghost"]), agents)


def test_roles_deployer_optional(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    roles = load_roles(write_roles(tmp_path, deployer=None), agents)
    assert roles.deployer is None


def test_roles_deployer_missing_agent_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="ghost"):
        load_roles(write_roles(tmp_path, deployer="ghost"), agents)


def test_lifecycle_agent_required(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="lifecycle_agent"):
        load_roles(write_roles(tmp_path, lifecycle_agent=None), agents)


def test_empty_reviewers_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="reviewers"):
        load_roles(write_roles(tmp_path, reviewers=[]), agents)


def test_branch_template_requires_id8(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="id8"):
        load_roles(write_roles(tmp_path, branch_template="dioxide/{slug}"), agents)


def test_alias_unknown_agent_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match="ghost"):
        load_roles(write_roles(tmp_path, aliases={"x": "ghost"}), agents)


def test_alias_resolves(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    roles = load_roles(write_roles(tmp_path), agents)
    assert roles.resolve("team-leader") == "lead"
    assert roles.resolve("planner") == "planner"


def test_unknown_agent_keyerror(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    roles = load_roles(write_roles(tmp_path), agents)
    with pytest.raises(KeyError):
        roles.resolve("nobody")


def test_shipped_examples() -> None:
    agents = load_agents(EXAMPLES / "agents")
    roles = load_roles(EXAMPLES / "roles.yaml", agents)
    assert roles.reviewers == ["reviewer"]
    assert roles.lifecycle_agent == "coordinator"

    settings = json.loads((EXAMPLES / "settings.json").read_text())
    assert settings == {
        "permissions": {
            "allow": ["Bash", "Read", "Edit", "Write", "Glob", "Grep"],
            "defaultMode": "acceptEdits",
        }
    }


def test_unknown_roles_key_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match=r"roles\.yaml.*unknown key 'reviewer'"):
        load_roles(write_roles(tmp_path, reviewer="reviewer"), agents)


def test_alias_shadowing_slug_raises(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    with pytest.raises(ValueError, match=r"alias 'qa' shadows an agent slug"):
        load_roles(write_roles(tmp_path, aliases={"qa": "lead"}), agents)


def test_roles_yaml_error_is_value_error(tmp_path: Path, agents: dict[str, AgentDef]) -> None:
    path = tmp_path / "roles.yaml"
    path.write_text("planner: [unclosed\n")
    with pytest.raises(ValueError, match=r"roles\.yaml.*invalid YAML"):
        load_roles(path, agents)


@pytest.mark.parametrize("template", ["x/{slug}-{id8}-{nope}", "x/{0}-{id8}", "x/{id8}-{slug!z}"])
def test_branch_template_unknown_placeholder_raises(
    tmp_path: Path, agents: dict[str, AgentDef], template: str
) -> None:
    with pytest.raises(ValueError, match="branch_template' has an unknown placeholder"):
        load_roles(write_roles(tmp_path, branch_template=template), agents)
