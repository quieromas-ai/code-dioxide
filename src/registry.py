from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Frontmatter keys -> AgentDef field. Claude Code only honours the camelCase keys in the
# `--agents` JSON, so the snake_case spellings are accepted here as aliases and rendered
# in the native form.
_KEY_TO_FIELD = {
    "name": "name",
    "description": "description",
    "model": "model",
    "effort": "effort",
    "tools": "tools",
    "disallowedTools": "disallowed_tools",
    "disallowed_tools": "disallowed_tools",
    "skills": "skills",
    "mcpServers": "mcp_servers",
    "mcp_servers": "mcp_servers",
    "memory": "memory",
}
_LIST_FIELDS = ("tools", "disallowed_tools", "skills")


@dataclass(frozen=True)
class AgentDef:
    slug: str
    description: str
    prompt: str
    model: str | None = None
    effort: str | None = None
    tools: list[str] = field(default_factory=list)
    disallowed_tools: list[str] = field(default_factory=list)
    skills: list[str] = field(default_factory=list)
    mcp_servers: list[Any] = field(default_factory=list)  # native `mcpServers`, as-is
    memory: str | None = None


def yaml_problem(exc: yaml.YAMLError) -> str:
    """The error class and position only: ``str(exc)`` quotes the offending source line, and even
    the parser's problem text echoes alias and tag names, either of which may be a credential."""
    name = type(exc).__name__
    mark = getattr(exc, "problem_mark", None)
    if mark is None:
        return name
    return f"{name} (line {mark.line + 1}, column {mark.column + 1})"


def _split_frontmatter(path: Path, text: str) -> tuple[dict[str, Any], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError(f"{path}: missing YAML frontmatter")
    for end in range(1, len(lines)):
        if lines[end].strip() == "---":
            break
    else:
        raise ValueError(f"{path}: unterminated YAML frontmatter")
    try:
        meta = yaml.safe_load("\n".join(lines[1:end])) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{path}: invalid YAML frontmatter: {yaml_problem(exc)}") from None
    if not isinstance(meta, dict):
        raise ValueError(f"{path}: frontmatter must be a mapping")
    return meta, "\n".join(lines[end + 1 :]).strip()


def _as_str_list(path: Path, key: str, value: Any) -> list[str]:
    if value is None:
        return []
    items = value.split(",") if isinstance(value, str) else value
    if not isinstance(items, list) or not all(isinstance(i, str) for i in items):
        raise ValueError(f"{path}: '{key}' must be a list or comma-separated string")
    return [i.strip() for i in items if i.strip()]


def _optional_str(path: Path, key: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{path}: '{key}' must be a string")
    return value


def _parse_agent(path: Path) -> AgentDef:
    meta, prompt = _split_frontmatter(path, path.read_text(encoding="utf-8"))
    fields: dict[str, Any] = {}
    for key, value in meta.items():
        target = _KEY_TO_FIELD.get(key)
        if target is None:
            raise ValueError(f"{path}: unknown frontmatter key '{key}'")
        if target in fields:
            raise ValueError(f"{path}: frontmatter key '{key}' given twice")
        fields[target] = value

    description = fields.get("description")
    if not isinstance(description, str) or not description.strip():
        raise ValueError(f"{path}: missing 'description'")
    if not prompt:
        raise ValueError(f"{path}: empty prompt body")

    mcp_servers = fields.get("mcp_servers")
    if mcp_servers is None:
        mcp_servers = []
    elif not isinstance(mcp_servers, list):
        raise ValueError(f"{path}: 'mcpServers' (mcp_servers) must be a list")

    return AgentDef(
        slug=str(fields.get("name") or path.stem),
        description=description,
        prompt=prompt,
        model=_optional_str(path, "model", fields.get("model")),
        effort=_optional_str(path, "effort", fields.get("effort")),
        memory=_optional_str(path, "memory", fields.get("memory")),
        mcp_servers=mcp_servers,
        **{f: _as_str_list(path, f, fields.get(f)) for f in _LIST_FIELDS},
    )


def load_agents(directory: Path) -> dict[str, AgentDef]:
    agents: dict[str, AgentDef] = {}
    for path in sorted(directory.glob("*.md")):
        agent = _parse_agent(path)
        if agent.slug in agents:
            raise ValueError(f"{path}: duplicate agent slug '{agent.slug}'")
        agents[agent.slug] = agent
    return dict(sorted(agents.items()))


def render_agents_json(agents: dict[str, AgentDef]) -> dict[str, dict[str, Any]]:
    """The `{slug: {...}}` object for `claude --agents`; camelCase keys only, unset ones omitted."""
    rendered: dict[str, dict[str, Any]] = {}
    for slug, agent in agents.items():
        entry: dict[str, Any] = {"description": agent.description, "prompt": agent.prompt}
        optional: dict[str, Any] = {
            "model": agent.model,
            "effort": agent.effort,
            "tools": agent.tools,
            "disallowedTools": agent.disallowed_tools,
            "skills": agent.skills,
            "mcpServers": agent.mcp_servers,
            "memory": agent.memory,
        }
        entry.update({key: value for key, value in optional.items() if value})
        rendered[slug] = entry
    return rendered
