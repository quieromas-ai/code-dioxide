import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from src.config import Settings
from src.registry import AgentDef, load_agents, yaml_problem

DEFAULT_BRANCH_TEMPLATE = "dioxide/{slug}-{id8}"
_KNOWN_KEYS = {
    "planner",
    "implementer",
    "reviewers",
    "qa",
    "deployer",
    "lifecycle_agent",
    "aliases",
    "branch_template",
}


@dataclass(frozen=True)
class Roles:
    planner: str
    implementer: str
    reviewers: list[str]
    qa: str
    deployer: str | None
    lifecycle_agent: str
    aliases: dict[str, str] = field(default_factory=dict)
    branch_template: str = DEFAULT_BRANCH_TEMPLATE
    known_agents: frozenset[str] = frozenset()

    def resolve(self, name: str) -> str:
        """Alias -> agent slug; an agent slug resolves to itself; anything else is a KeyError."""
        if name in self.aliases:
            return self.aliases[name]
        if name in self.known_agents:
            return name
        raise KeyError(name)


def _require_agent(path: Path, key: str, value: Any, agents: dict[str, AgentDef]) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: '{key}' is required")
    if value not in agents:
        raise ValueError(f"{path}: '{key}' names unknown agent '{value}'")
    return value


def load_roles(path: Path, agents: dict[str, AgentDef]) -> Roles:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{path}: invalid YAML: {yaml_problem(exc)}") from None
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: must be a mapping")
    unknown = sorted(set(raw) - _KNOWN_KEYS)
    if unknown:
        raise ValueError(f"{path}: unknown key '{unknown[0]}'")

    reviewers = raw.get("reviewers")
    if not isinstance(reviewers, list) or not reviewers:
        raise ValueError(f"{path}: 'reviewers' must list at least one agent")
    deployer = raw.get("deployer")
    aliases = raw.get("aliases") or {}
    if not isinstance(aliases, dict):
        raise ValueError(f"{path}: 'aliases' must be a mapping")
    template = raw.get("branch_template", DEFAULT_BRANCH_TEMPLATE)
    if not isinstance(template, str) or "{id8}" not in template:
        raise ValueError(f"{path}: 'branch_template' must contain {{id8}}")
    try:
        template.format(slug="x", id8="00000000")
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(f"{path}: 'branch_template' has an unknown placeholder: {exc}") from None

    for alias, target in aliases.items():
        if alias in agents:
            raise ValueError(f"{path}: alias '{alias}' shadows an agent slug")
        _require_agent(path, f"aliases.{alias}", target, agents)

    return Roles(
        planner=_require_agent(path, "planner", raw.get("planner"), agents),
        implementer=_require_agent(path, "implementer", raw.get("implementer"), agents),
        reviewers=[_require_agent(path, "reviewers", r, agents) for r in reviewers],
        qa=_require_agent(path, "qa", raw.get("qa"), agents),
        deployer=None if deployer is None else _require_agent(path, "deployer", deployer, agents),
        lifecycle_agent=_require_agent(path, "lifecycle_agent", raw.get("lifecycle_agent"), agents),
        aliases={str(k): str(v) for k, v in aliases.items()},
        branch_template=template,
        known_agents=frozenset(agents),
    )


def load_registry(
    settings: Settings, logger: logging.Logger
) -> tuple[dict[str, AgentDef], Roles | None]:
    """Agents + roles, at boot and at every hot reload; any configuration error raises ValueError.

    A missing agents dir is tolerated (empty registry, warning) only when roles.yaml is also
    absent; without roles.yaml every task is a single task.
    """
    has_roles = settings.roles_path.is_file()
    if not settings.agents_dir.is_dir():
        if has_roles:
            raise ValueError(
                f"agents dir {settings.agents_dir} is missing but {settings.roles_path} exists"
            )
        logger.warning("no agents dir; starting with an empty registry",
                       extra={"agents_dir": str(settings.agents_dir)})  # fmt: skip
        return {}, None
    agents = load_agents(settings.agents_dir)
    return agents, load_roles(settings.roles_path, agents) if has_roles else None


def registry_signature(settings: Settings) -> tuple[int, frozenset[str]]:
    """Cheap change detector (stat only): newest mtime of ``agents/*.md`` and roles.yaml, plus
    the file names, so a deleted file changes it too."""
    newest = 0
    names: set[str] = set()
    paths = [settings.roles_path]
    if settings.agents_dir.is_dir():
        paths += settings.agents_dir.glob("*.md")
    for path in paths:
        try:
            newest = max(newest, path.stat().st_mtime_ns)
        except OSError:  # absent (roles.yaml is optional) or gone mid-scan
            continue
        names.add(path.name)
    return newest, frozenset(names)
