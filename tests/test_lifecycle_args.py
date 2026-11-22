import json
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from src.config import Settings
from src.lifecycle_args import build_lifecycle_args
from src.roles import Roles
from src.tasks import KIND_LIFECYCLE, Task

ROLES = Roles(
    planner="planner",
    implementer="implementer",
    reviewers=["reviewer-a", "reviewer-b"],
    qa="qa",
    deployer="deployer",
    lifecycle_agent="planner",
)


def _task(tmp_path: Path, description: str, context: dict[str, Any]) -> Task:
    row: dict[str, Any] = {f.name: None for f in fields(Task)}
    run_dir = tmp_path / "runs" / "t-1"
    row.update(
        id="t-1",
        agent="planner",
        kind=KIND_LIFECYCLE,
        description=description,
        input_context=json.dumps(context),
        priority=0,
        max_retries=0,
        status="queued",
        cwd=str(tmp_path / "repo"),
        base_branch="main",
        branch="dioxide/feature-t1",
        worktree=str(tmp_path / "wt" / "t-1"),
        args_json=json.dumps({"task_id": "t-1", "run_dir": str(run_dir)}),
        attempt=0,
        resumes=0,
        call_retries=0,
        rate_limit_waited_s=0.0,
        created_at=1.0,
        updated_at=1.0,
    )
    return Task.from_row(row)


def _settings(tmp_path: Path) -> Settings:
    return Settings(home=tmp_path / "home", review_rounds=2, qa_rounds=4, merge_strategy="pr")


def test_args_deterministic_and_small(tmp_path: Path) -> None:
    brief = "x" * 20_000
    context = {"approved_plan": {"steps": ["y" * 5_000]}, "plan_gate": True, "notes": "z" * 9_000}
    task = _task(tmp_path, brief, context)
    settings = _settings(tmp_path)

    first = json.dumps(build_lifecycle_args(task, settings, ROLES), sort_keys=True)
    second = json.dumps(build_lifecycle_args(task, settings, ROLES), sort_keys=True)

    assert first == second
    assert len(first.encode()) < 2048
    assert "xxxx" not in first and "yyyy" not in first and "zzzz" not in first


def test_args_shape_from_task_settings_roles(tmp_path: Path) -> None:
    task = _task(tmp_path, "do it", {"approved_plan": {"steps": []}, "plan_gate": True})
    args = build_lifecycle_args(task, _settings(tmp_path), ROLES)
    assert args == {
        "task_id": "t-1",
        "run_dir": str(tmp_path / "runs" / "t-1"),
        "repo": str(tmp_path / "repo"),
        "base_branch": "main",
        "branch": "dioxide/feature-t1",
        "worktree": str(tmp_path / "wt" / "t-1"),
        "roles": {
            "planner": "planner",
            "implementer": "implementer",
            "reviewers": ["reviewer-a", "reviewer-b"],
            "qa": "qa",
            "deployer": "deployer",
        },
        "limits": {"review_rounds": 2, "qa_rounds": 4},
        "merge_strategy": "pr",
        "plan_gate": True,
        "plan_pinned": True,
        "answers": [],
    }


def test_args_plan_flags_default_false(tmp_path: Path) -> None:
    task = _task(tmp_path, "do it", {"plan_gate": "yes", "approved_plan": None})
    args = build_lifecycle_args(task, _settings(tmp_path), ROLES)
    assert args["plan_gate"] is False
    assert args["plan_pinned"] is False


def test_script_model_frozen_only_when_set(tmp_path: Path) -> None:
    task = _task(tmp_path, "brief", {})
    unset = build_lifecycle_args(task, _settings(tmp_path), ROLES)
    assert "script_model" not in unset  # args frozen before the setting: unchanged shape
    cheap = _settings(tmp_path).model_copy(update={"script_model": "haiku"})
    args = build_lifecycle_args(task, cheap, ROLES)
    assert args == {**unset, "script_model": "haiku"}
    assert json.dumps(args) == json.dumps(build_lifecycle_args(task, cheap, ROLES))


def test_script_model_must_not_be_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_SCRIPT_MODEL", "haiku")
    assert Settings(home=tmp_path / "h").script_model == "haiku"
    monkeypatch.delenv("DIOXIDE_SCRIPT_MODEL")
    assert Settings(home=tmp_path / "h").script_model is None
    with pytest.raises(ValueError, match="script_model"):
        Settings(home=tmp_path / "h", script_model="  ")
