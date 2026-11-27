"""Builds the exact ``claude -p`` command, prompt, environment and config files for an attempt."""

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from src.config import ENV_PREFIX, Settings
from src.runstore import RunPaths
from src.tasks import Task

Mode = Literal["first", "resume", "fresh"]

SCRIPT_NAME = "feature-lifecycle.js"
TASK_MARKER = f"{ENV_PREFIX}TASK_ID"
BG_WAIT_VAR = "CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS"
CALL_PREFIX = "Call the Workflow tool exactly once with this input and nothing else: "
CALL_SUFFIX = (
    ". Do not use any other tool. When the workflow ends, reply with one line: its final status. "
    "The engine decides retries and resumes; never say whether it will be resumed."
)
CORRECTIVE = "Your previous attempt did not call the Workflow tool exactly as instructed. "
FRESH_CHECK = "First check what is already done; do not repeat completed side effects."
SINGLE_RESUME = (
    "Continue the previous task; first check what is already done; "
    "do not repeat completed side effects."
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def expected_call(task: Task, mode: Mode) -> dict[str, Any]:
    """The exact Workflow tool input the parent session must send."""
    call: dict[str, Any] = {
        "scriptPath": str(task.run_dir / "script" / SCRIPT_NAME),
        "args": task.args_dict,
    }
    if mode == "resume" and task.run_id:
        call["resumeFromRunId"] = task.run_id
    return call


def call_matches(expected: dict[str, Any], passed: dict[str, Any]) -> bool:
    """Whole-input canonical JSON equality; a JSON-string ``args`` is parsed first."""
    actual = dict(passed)
    if isinstance(actual.get("args"), str):
        try:
            actual["args"] = json.loads(actual["args"])
        except ValueError:
            return False
    return canonical_json(actual) == canonical_json(expected)


def call_in_prompt(prompt: str) -> dict[str, Any] | None:
    """The Workflow input a lifecycle launch prompt (``build_prompt``) asks for; None when the
    prompt holds none (a single task, or an unreadable prompt)."""
    start = prompt.find(CALL_PREFIX)
    if start < 0:
        return None
    try:
        value, _end = json.JSONDecoder().raw_decode(prompt[start + len(CALL_PREFIX) :])
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def build_prompt(task: Task, mode: Mode, corrective: bool = False) -> str:
    if task.is_lifecycle:
        prompt = f"{CALL_PREFIX}{canonical_json(expected_call(task, mode))}{CALL_SUFFIX}"
        return CORRECTIVE + prompt if corrective else prompt
    if mode == "resume":
        return SINGLE_RESUME
    brief = f"Read the task brief at {task.run_dir / 'inputs' / 'brief.md'} and do it."
    if mode == "fresh" and task.attempt > 1:  # a new session in a tree earlier attempts changed
        return f"{brief} {FRESH_CHECK}"
    return brief


def build_command(
    *,
    settings: Settings,
    task: Task,
    mode: Mode,
    session_id: str,
    paths: RunPaths,
    agents_path: Path,
    corrective: bool = False,
) -> list[str]:
    data = settings.data_dir
    cmd = [
        settings.claude_bin,
        "-p",
        build_prompt(task, mode, corrective),
        "--resume" if mode == "resume" else "--session-id",
        session_id,
        "--agents",
        str(agents_path),
        "--mcp-config",
        str(data / "mcp.json"),
        "--strict-mcp-config",
        "--allowedTools",
        "Workflow",
        "--add-dir",
        str(paths.script_dir),
        str(paths.inputs_dir),
        str(settings.worktrees_dir),
        *([str(paths.out_dir)] if task.is_lifecycle else []),  # the stages' only writable run dir
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    if settings.extra_settings:
        cmd += ["--settings", str(data / "settings.json")]
    if task.is_lifecycle:
        if settings.parent_model:
            cmd += ["--model", settings.parent_model]
        if settings.parent_effort:
            cmd += ["--effort", settings.parent_effort]
    else:
        if not task.agent:
            raise ValueError(f"single task {task.id} has no agent")
        cmd += ["--agent", task.agent]
    return cmd


def render_mcp_config(settings: Settings) -> dict[str, Any]:
    """The user's MCP config minus every server named in ``mcp_exclude``."""
    if settings.mcp_config_path is None:
        return {"mcpServers": {}}
    config = json.loads(settings.mcp_config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"{settings.mcp_config_path} must hold a JSON object")
    servers = config.get("mcpServers") or {}
    if not isinstance(servers, dict):
        raise ValueError(f"{settings.mcp_config_path}: mcpServers must be an object")
    excluded = set(settings.mcp_exclude)
    config["mcpServers"] = {k: v for k, v in servers.items() if k not in excluded}
    return config


def render_settings(settings: Settings) -> dict[str, Any] | None:
    """``extra_settings`` verbatim (dioxide adds nothing), or None when unset."""
    return dict(settings.extra_settings) if settings.extra_settings else None


def launch_env(
    settings: Settings,
    task: Task,
    base_env: Mapping[str, str] | None = None,
    child_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Child environment: every ``DIOXIDE_*`` variable removed except the task marker, then the
    child env file's variables (``child_env``: credentials) added; dioxide's own variables (the
    background-wait ceiling, the task marker, ``extra_path``) are applied last and win."""
    env = child_process_env(settings, base_env, child_env)
    env[TASK_MARKER] = task.id
    return env


def child_process_env(
    settings: Settings,
    base_env: Mapping[str, str] | None = None,
    child_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """``launch_env`` without the task marker: for a claude run that belongs to no task (a
    summary), so no task's sweep counts it among the task's processes."""
    source = os.environ if base_env is None else base_env
    env = {k: v for k, v in source.items() if not k.startswith(ENV_PREFIX)}
    env.update(child_env or {})
    env[BG_WAIT_VAR] = "0"
    if settings.extra_path:
        current = env.get("PATH")
        env["PATH"] = os.pathsep.join([*settings.extra_path, *([current] if current else [])])
    return env
