import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.config import Settings
from src.launcher import (
    CALL_SUFFIX,
    Mode,
    build_command,
    build_prompt,
    call_in_prompt,
    call_matches,
    expected_call,
    launch_env,
    render_mcp_config,
    render_settings,
)
from src.runstore import RunStore
from src.tasks import Task

TASK_ID = "4c1f6d2e-0000-4000-8000-000000000001"
AGENTS_FILE = Path("/run/attempt-1.agents.json")


def make_task(run_dir: Path, **overrides: Any) -> Task:
    args = {"task_id": TASK_ID, "run_dir": str(run_dir), "answers": []}
    values: dict[str, Any] = {
        "id": TASK_ID,
        "agent": "dx-team-leader",
        "kind": "lifecycle",
        "description": "Add a health endpoint",
        "input_context": json.dumps({"project_dir": "/repo"}),
        "priority": 0,
        "max_retries": 0,
        "status": "running",
        "fingerprint": None,
        "cwd": "/repo",
        "base_branch": "main",
        "branch": "dx/health-4c1f6d2e",
        "worktree": "/wt/health",
        "session_id": "11111111-2222-3333-4444-555555555555",
        "run_id": None,
        "args_json": json.dumps(args),
        "attempt": 1,
        "resumes": 0,
        "call_retries": 0,
        "stage_retries": 0,
        "rate_limit_waited_s": 0.0,
        "corrective": 0,
        "resume_mode": "first",
        "pid": None,
        "pid_start": None,
        "kill_reason": None,
        "output_json": None,
        "error": None,
        "question_json": None,
        "next_attempt_at": None,
        "created_at": 1.0,
        "started_at": None,
        "completed_at": None,
        "updated_at": 1.0,
    }
    values.update(overrides)
    return Task(**values)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(home=tmp_path / "home", claude_bin="/opt/bin/claude")


@pytest.fixture
def store(settings: Settings) -> RunStore:
    return RunStore(settings.runs_dir)


def value_after(cmd: list[str], flag: str) -> str:
    return cmd[cmd.index(flag) + 1]


def test_build_command_modes_and_flags(settings: Settings, store: RunStore) -> None:
    paths = store.paths(TASK_ID)
    task = make_task(paths.run_dir)
    sid = task.session_id
    assert sid is not None
    data = settings.data_dir

    first = build_command(
        settings=settings,
        task=task,
        mode="first",
        session_id=sid,
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert first == [
        "/opt/bin/claude",
        "-p",
        build_prompt(task, "first"),
        "--session-id",
        sid,
        "--agents",
        str(AGENTS_FILE),
        "--mcp-config",
        str(data / "mcp.json"),
        "--strict-mcp-config",
        "--allowedTools",
        "Workflow",
        "--add-dir",
        str(paths.script_dir),
        str(paths.inputs_dir),
        str(settings.worktrees_dir),
        str(paths.out_dir),
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    fresh = build_command(
        settings=settings,
        task=task,
        mode="fresh",
        session_id=sid,
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert "--session-id" in fresh and "--resume" not in fresh

    resumed_task = make_task(paths.run_dir, run_id="wf_abc-123")
    resume = build_command(
        settings=settings,
        task=resumed_task,
        mode="resume",
        session_id=sid,
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert value_after(resume, "--resume") == sid
    assert "--session-id" not in resume
    assert "--settings" not in resume and "--model" not in resume and "--agent" not in resume

    with_settings = Settings(
        home=settings.home, claude_bin="/opt/bin/claude", extra_settings={"disableAllHooks": True}
    )
    cmd = build_command(
        settings=with_settings,
        task=task,
        mode="first",
        session_id=sid,
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert cmd[-2:] == ["--settings", str(data / "settings.json")]

    single = make_task(paths.run_dir, kind="single", agent="dx-implementer")
    cmd = build_command(
        settings=settings,
        task=single,
        mode="first",
        session_id=sid,
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert cmd[-2:] == ["--agent", "dx-implementer"]
    assert value_after(cmd, "-p") == build_prompt(single, "first")


def test_lifecycle_launch_adds_out_dir(settings: Settings, store: RunStore) -> None:
    paths = store.paths(TASK_ID)
    assert paths.out_dir == paths.run_dir / "out"
    lifecycle = make_task(paths.run_dir, run_id="wf_abc-123")
    for mode in ("first", "resume", "fresh"):
        cmd = build_command(
            settings=settings,
            task=lifecycle,
            mode=mode,
            session_id="s",
            paths=paths,
            agents_path=AGENTS_FILE,
        )
        i = cmd.index("--add-dir")
        assert cmd[i + 1 : i + 5] == [
            str(paths.script_dir),
            str(paths.inputs_dir),
            str(settings.worktrees_dir),
            str(paths.out_dir),
        ]
        assert cmd[i + 5].startswith("--") and cmd.count("--add-dir") == 1
    single = make_task(paths.run_dir, kind="single", agent="dx-implementer")
    cmd = build_command(
        settings=settings,
        task=single,
        mode="first",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert str(paths.out_dir) not in cmd


def test_parent_model_lifecycle_only(settings: Settings, store: RunStore) -> None:
    paths = store.paths(TASK_ID)
    s = Settings(home=settings.home, claude_bin="/opt/bin/claude", parent_model="haiku")
    lifecycle = make_task(paths.run_dir)
    single = make_task(paths.run_dir, kind="single", agent="dx-implementer")

    cmd = build_command(
        settings=s,
        task=lifecycle,
        mode="first",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert value_after(cmd, "--model") == "haiku"
    cmd = build_command(
        settings=s, task=single, mode="first", session_id="s", paths=paths, agents_path=AGENTS_FILE
    )
    assert "--model" not in cmd


def test_parent_effort_lifecycle_only_and_absent_when_unset(
    settings: Settings, store: RunStore
) -> None:
    paths = store.paths(TASK_ID)
    lifecycle = make_task(paths.run_dir)
    single = make_task(paths.run_dir, kind="single", agent="dx-implementer")
    cmd = build_command(
        settings=settings,
        task=lifecycle,
        mode="first",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert "--effort" not in cmd
    s = Settings(home=settings.home, claude_bin="/opt/bin/claude", parent_effort="low")
    cmd = build_command(
        settings=s,
        task=lifecycle,
        mode="resume",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert value_after(cmd, "--effort") == "low"
    cmd = build_command(
        settings=s, task=single, mode="first", session_id="s", paths=paths, agents_path=AGENTS_FILE
    )
    assert "--effort" not in cmd


def test_corrective_prefix_only_when_asked(settings: Settings, store: RunStore) -> None:
    paths = store.paths(TASK_ID)
    task = make_task(paths.run_dir)
    plain = build_command(
        settings=settings,
        task=task,
        mode="resume",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    fixed = build_command(
        settings=settings,
        task=task,
        mode="resume",
        session_id="s",
        paths=paths,
        agents_path=AGENTS_FILE,
        corrective=True,
    )
    prompt = value_after(fixed, "-p")
    assert prompt.startswith(
        "Your previous attempt did not call the Workflow tool exactly as instructed. "
    )
    assert prompt.endswith(value_after(plain, "-p"))
    assert "resumeFromRunId" not in prompt  # no run stored yet: a new first call, same session
    assert value_after(fixed, "--resume") == "s"


def test_single_fresh_start_tells_the_new_session_to_check_first(
    settings: Settings, store: RunStore
) -> None:
    paths = store.paths(TASK_ID)
    brief = f"Read the task brief at {paths.run_dir / 'inputs' / 'brief.md'} and do it."
    first = make_task(paths.run_dir, kind="single", agent="dx-implementer", attempt=1)
    cmd = build_command(
        settings=settings,
        task=first,
        mode="first",
        session_id="n",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert value_after(cmd, "-p") == brief  # the very first launch is unchanged
    later = make_task(paths.run_dir, kind="single", agent="dx-implementer", attempt=3)
    cmd = build_command(
        settings=settings,
        task=later,
        mode="fresh",
        session_id="n",
        paths=paths,
        agents_path=AGENTS_FILE,
    )
    assert value_after(cmd, "-p") == (
        f"{brief} First check what is already done; do not repeat completed side effects."
    )
    assert value_after(cmd, "--session-id") == "n" and "--resume" not in cmd


def test_only_workflow_permission_added(settings: Settings, store: RunStore) -> None:
    paths = store.paths(TASK_ID)
    for task in (make_task(paths.run_dir), make_task(paths.run_dir, kind="single", agent="a")):
        cmd = build_command(
            settings=settings,
            task=task,
            mode="first",
            session_id="s",
            paths=paths,
            agents_path=AGENTS_FILE,
        )
        assert not any(arg.startswith("--permission-mode") for arg in cmd)
        assert "--dangerously-skip-permissions" not in cmd
        assert cmd.count("--allowedTools") == 1
        i = cmd.index("--allowedTools")
        assert cmd[i + 1] == "Workflow"
        assert cmd[i + 2].startswith("--")
        assert "--disallowedTools" not in cmd


def test_env_bg_wait_zero_and_marker(settings: Settings, store: RunStore) -> None:
    s = Settings(home=settings.home, extra_path=["/opt/a", "/opt/b"])
    task = make_task(store.paths(TASK_ID).run_dir)
    env = launch_env(s, task, {"PATH": "/usr/bin", "HOME": "/h"})
    assert env["CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS"] == "0"
    assert env["DIOXIDE_TASK_ID"] == TASK_ID
    assert env["PATH"] == os.pathsep.join(["/opt/a", "/opt/b", "/usr/bin"])
    assert env["HOME"] == "/h"

    env = launch_env(settings, task, {"PATH": "/usr/bin"})
    assert env["PATH"] == "/usr/bin"


def test_launch_env_strips_dioxide_secrets(
    settings: Settings, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    task = make_task(store.paths(TASK_ID).run_dir)
    base = {
        "PATH": "/usr/bin",
        "DIOXIDE_AUTH_TOKEN": "x",
        "DIOXIDE_OTHER_TOKEN": "x",
        "DIOXIDE_HOME": "/h",
        "DIOXIDE_TASK_ID": "someone-else",
        "OTHER_DIOXIDE_X": "kept",
    }
    env = launch_env(settings, task, base)
    assert sorted(k for k in env if k.startswith("DIOXIDE_")) == ["DIOXIDE_TASK_ID"]
    assert env["DIOXIDE_TASK_ID"] == TASK_ID
    assert env["OTHER_DIOXIDE_X"] == "kept"

    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", "x")
    env = launch_env(settings, task)
    assert "DIOXIDE_AUTH_TOKEN" not in env
    assert env["DIOXIDE_TASK_ID"] == TASK_ID


def test_mcp_config_applies_mcp_exclude(tmp_path: Path) -> None:
    user_cfg = tmp_path / "mcp.json"
    user_cfg.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "dioxide": {"type": "http", "url": "http://127.0.0.1:8500/mcp"},
                    "orcai-projects": {"type": "http", "url": "http://127.0.0.1:8200/mcp"},
                    "github": {"command": "gh-mcp"},
                }
            }
        )
    )
    s = Settings(
        home=tmp_path / "home", mcp_config_path=user_cfg, mcp_exclude=["dioxide", "orcai-projects"]
    )
    assert render_mcp_config(s) == {"mcpServers": {"github": {"command": "gh-mcp"}}}

    none = Settings(home=tmp_path / "empty-home")
    assert none.mcp_config_path is None
    assert render_mcp_config(none) == {"mcpServers": {}}


def test_render_settings_verbatim(tmp_path: Path) -> None:
    assert render_settings(Settings(home=tmp_path)) is None
    extra = {"disableAllHooks": True, "permissions": {"allow": ["Bash"]}}
    assert render_settings(Settings(home=tmp_path, extra_settings=extra)) == extra


def test_call_matches_full_input(tmp_path: Path) -> None:
    task = make_task(tmp_path / "run", run_id="wf_abc-123")
    first = expected_call(task, "first")
    assert first == {
        "scriptPath": str(tmp_path / "run" / "script" / "feature-lifecycle.js"),
        "args": task.args_dict,
    }
    assert expected_call(task, "fresh") == first
    resume = expected_call(task, "resume")
    assert resume["resumeFromRunId"] == "wf_abc-123"
    assert expected_call(make_task(tmp_path / "run"), "resume") == first

    assert call_matches(resume, json.loads(json.dumps(resume)))
    stringly = dict(resume, args=json.dumps(resume["args"], indent=2))
    assert call_matches(resume, stringly)

    dropped = {k: v for k, v in resume.items() if k != "resumeFromRunId"}
    assert not call_matches(resume, dropped)
    assert not call_matches(resume, dict(resume, script="x"))
    assert not call_matches(resume, dict(resume, args=dict(resume["args"], extra=1)))
    assert not call_matches(resume, dict(resume, args="{not json"))


def test_build_prompt_variants(tmp_path: Path) -> None:
    task = make_task(tmp_path / "run", run_id="wf_abc-123")
    call = json.dumps(
        expected_call(task, "resume"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    prompt = build_prompt(task, "resume")
    assert prompt == (
        f"Call the Workflow tool exactly once with this input and nothing else: {call}. "
        "Do not use any other tool. When the workflow ends, reply with one line: its final "
        "status. The engine decides retries and resumes; never say whether it will be resumed."
    )
    assert build_prompt(task, "resume", corrective=True) == (
        "Your previous attempt did not call the Workflow tool exactly as instructed. " + prompt
    )

    single = make_task(tmp_path / "run", kind="single", agent="a")
    brief = tmp_path / "run" / "inputs" / "brief.md"
    assert build_prompt(single, "first") == f"Read the task brief at {brief} and do it."
    assert build_prompt(single, "resume") == (
        "Continue the previous task; first check what is already done; "
        "do not repeat completed side effects."
    )


@pytest.mark.parametrize("mode", ["first", "resume", "fresh"])
def test_call_in_prompt_roundtrip_with_new_suffix(tmp_path: Path, mode: Mode) -> None:
    task = make_task(tmp_path / "run", run_id="wf_abc-123")
    expected = expected_call(task, mode)
    assert "never say whether it will be resumed" in CALL_SUFFIX
    assert call_in_prompt(build_prompt(task, mode)) == expected
    assert call_in_prompt(build_prompt(task, mode, corrective=True)) == expected
