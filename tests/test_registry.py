from pathlib import Path

import pytest

from src.registry import AgentDef, load_agents, render_agents_json

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

FULL = """---
name: custom-slug
description: Does things
model: sonnet
effort: high
tools: Read, Grep
disallowed_tools: [Edit, Write]
skills: [alpha, beta]
mcp_servers:
  - docs
  - {search: {command: srv}}
memory: project
---
You are an agent.
"""


def write(directory: Path, name: str, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(text)
    return path


def test_load_agents_frontmatter(tmp_path: Path) -> None:
    write(tmp_path, "file-stem.md", FULL)
    write(tmp_path, "minimal.md", "---\ndescription: Min\n---\nBody text\n")
    write(tmp_path, "ignored.txt", "not an agent")

    agents = load_agents(tmp_path)

    assert list(agents) == ["custom-slug", "minimal"]
    full = agents["custom-slug"]
    assert full == AgentDef(
        slug="custom-slug",
        description="Does things",
        model="sonnet",
        effort="high",
        tools=["Read", "Grep"],
        disallowed_tools=["Edit", "Write"],
        skills=["alpha", "beta"],
        mcp_servers=["docs", {"search": {"command": "srv"}}],
        memory="project",
        prompt="You are an agent.",
    )
    minimal = agents["minimal"]
    assert minimal.model is None and minimal.tools == [] and minimal.mcp_servers == []
    assert minimal.prompt == "Body text"


def test_load_agents_accepts_native_camelcase(tmp_path: Path) -> None:
    write(
        tmp_path,
        "a.md",
        "---\ndescription: D\ndisallowedTools: Edit\nmcpServers: [x]\n---\nbody\n",
    )
    agent = load_agents(tmp_path)["a"]
    assert agent.disallowed_tools == ["Edit"]
    assert agent.mcp_servers == ["x"]


def test_render_agents_json_shape(tmp_path: Path) -> None:
    write(tmp_path, "full.md", FULL)
    write(tmp_path, "minimal.md", "---\ndescription: Min\n---\nBody\n")

    rendered = render_agents_json(load_agents(tmp_path))

    assert rendered["minimal"] == {"description": "Min", "prompt": "Body"}
    assert rendered["custom-slug"] == {
        "description": "Does things",
        "prompt": "You are an agent.",
        "model": "sonnet",
        "effort": "high",
        "tools": ["Read", "Grep"],
        "disallowedTools": ["Edit", "Write"],
        "skills": ["alpha", "beta"],
        "mcpServers": ["docs", {"search": {"command": "srv"}}],
        "memory": "project",
    }


def test_unknown_frontmatter_key_raises(tmp_path: Path) -> None:
    write(tmp_path, "bad.md", "---\ndescription: D\nrunner: x\n---\nbody\n")
    with pytest.raises(ValueError, match=r"bad\.md.*runner"):
        load_agents(tmp_path)


def test_missing_description_raises(tmp_path: Path) -> None:
    write(tmp_path, "nodesc.md", "---\nmodel: sonnet\n---\nbody\n")
    with pytest.raises(ValueError, match=r"nodesc\.md.*description"):
        load_agents(tmp_path)


def test_empty_prompt_raises(tmp_path: Path) -> None:
    write(tmp_path, "empty.md", "---\ndescription: D\n---\n  \n")
    with pytest.raises(ValueError, match=r"empty\.md.*prompt"):
        load_agents(tmp_path)


def test_mcp_servers_must_be_list(tmp_path: Path) -> None:
    write(tmp_path, "m.md", "---\ndescription: D\nmcp_servers: docs\n---\nbody\n")
    with pytest.raises(ValueError, match=r"m\.md.*mcp"):
        load_agents(tmp_path)


def test_no_frontmatter_raises(tmp_path: Path) -> None:
    write(tmp_path, "plain.md", "just text\n")
    with pytest.raises(ValueError, match=r"plain\.md.*frontmatter"):
        load_agents(tmp_path)


def test_shipped_example_agents() -> None:
    agents = load_agents(EXAMPLES / "agents")
    assert set(agents) == {"planner", "implementer", "reviewer", "qa", "deployer"}
    assert agents["reviewer"].disallowed_tools == ["Edit", "Write"]
    for path in (EXAMPLES / "agents").glob("*.md"):
        assert len(path.read_text().splitlines()) <= 30


def test_duplicate_slug_raises(tmp_path: Path) -> None:
    write(tmp_path, "a.md", "---\nname: same\ndescription: A\n---\nbody\n")
    write(tmp_path, "b.md", "---\nname: same\ndescription: B\n---\nbody\n")
    with pytest.raises(ValueError, match=r"b\.md.*duplicate agent slug 'same'"):
        load_agents(tmp_path)


def test_both_key_spellings_raise(tmp_path: Path) -> None:
    text = "---\ndescription: D\ndisallowed_tools: [Edit]\ndisallowedTools: [Write]\n---\nbody\n"
    write(tmp_path, "both.md", text)
    with pytest.raises(ValueError, match=r"both\.md.*given twice"):
        load_agents(tmp_path)


@pytest.mark.parametrize("tools", ["5", "[Read, 3]", "{a: b}"])
def test_malformed_tools_raise(tmp_path: Path, tools: str) -> None:
    write(tmp_path, "t.md", f"---\ndescription: D\ntools: {tools}\n---\nbody\n")
    with pytest.raises(ValueError, match=r"t\.md.*'tools'"):
        load_agents(tmp_path)


def test_yaml_error_is_value_error_naming_file(tmp_path: Path) -> None:
    write(tmp_path, "broken.md", "---\ndescription: [unclosed\n---\nbody\n")
    with pytest.raises(ValueError, match=r"broken\.md.*invalid YAML"):
        load_agents(tmp_path)


@pytest.mark.parametrize(
    ("value", "error"),
    [("*SECRETALIASxyz", "ComposerError"), ("!custom_s3cr3t x", "ConstructorError")],
)
def test_yaml_error_names_class_and_position_only(tmp_path: Path, value: str, error: str) -> None:
    """The parser's problem text echoes alias and tag names: only the class and position stay."""
    write(tmp_path, "broken.md", f"---\ndescription: D\ntoken: {value}\n---\nbody\n")
    with pytest.raises(ValueError) as caught:
        load_agents(tmp_path)
    message = str(caught.value)
    assert "SECRETALIASxyz" not in message and "s3cr3t" not in message
    assert message.endswith(f"invalid YAML frontmatter: {error} (line 2, column 8)")
