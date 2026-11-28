"""Immutable view of one ``tasks`` row."""

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Self

KIND_LIFECYCLE = "lifecycle"
KIND_SINGLE = "single"
TERMINAL = frozenset({"completed", "failed", "cancelled"})
# Why a launch resumes the session or restarts it fresh (``tasks.resume_cause``, the
# ``attempt_started`` event's ``cause``): an answer, a decider ``Resume.why``
# (``recovery.TRANSIENT`` ... ``recovery.CORRECTIVE``) or a ``Fresh`` restart.
CAUSE_ANSWER = "answer"
CAUSE_FRESH = "fresh"
RESUME_CAUSES = (
    CAUSE_ANSWER, "transient", "corrective", "stage_failed", "rate_limited",
    "rate_limited_fallback", CAUSE_FRESH,
)  # fmt: skip
# A caller waiting on a task (prompt_agent, log followers) stops at these: a task waiting in
# needs_human writes nothing more until it is answered and resumed.
WAIT_ENDS = TERMINAL | {"needs_human"}
# Sweep rule: only an unclean attempt's leftover processes are killed. A clean exit (the claude
# process exited by itself and the attempt was classified completed or needs_human) leaves what
# it started running. The attempts row records it as reason CLEAN_EXIT.
CLEAN_EXIT_STATUSES = frozenset({"completed", "needs_human"})
CLEAN_EXIT = "clean_exit"
# Kill reasons after which the workflow's own return still decides the task's status: the
# engine killed the process (an unclean end, swept as such), but the run had already finished
# with a valid return. A parent tool call after the run reported completion is one.
LATE_PARENT_TOOL = "parent_tool_after_completion"
# The watchdog killed a parent still running after its run reported completion with a valid
# return (it has nothing left to do): the return decides, the end is unclean (swept).
EXIT_AFTER_COMPLETION = "exit_after_completion"
# A second Workflow call after this attempt's run reported completion with a valid return: killed,
# the return decides (as for a late parent tool call).
LATE_WORKFLOW_CALL = "workflow_call_after_completion"
RETURN_DECIDES = frozenset({LATE_PARENT_TOOL, EXIT_AFTER_COMPLETION, LATE_WORKFLOW_CALL})


def is_clean_exit(status: str, kill_reason: str | None) -> bool:
    """An attempt that ended in ``status`` exited cleanly: nothing killed it (no kill reason)
    and it was classified ``completed`` or ``needs_human``. Every other end is unclean."""
    return kill_reason is None and status in CLEAN_EXIT_STATUSES


def attempt_was_clean(row: Mapping[str, Any] | sqlite3.Row | None) -> bool:
    """From a persisted ``attempts`` row; an open or missing row counts as unclean."""
    return row is not None and row["ended_at"] is not None and row["reason"] == CLEAN_EXIT


def _json_object(raw: str | None, column: str) -> dict[str, Any]:
    if raw is None or raw == "":
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"tasks.{column} must hold a JSON object")
    return value


@dataclass(frozen=True)
class Task:
    """Mirror of the ``tasks`` table columns (see ``src.database.SCHEMA``)."""

    id: str
    agent: str | None
    kind: str | None
    description: str | None
    input_context: str | None
    priority: int
    max_retries: int
    status: str
    fingerprint: str | None
    cwd: str | None
    base_branch: str | None
    branch: str | None
    worktree: str | None
    session_id: str | None
    run_id: str | None
    args_json: str | None
    attempt: int
    resumes: int
    call_retries: int
    stage_retries: int
    rate_limit_waited_s: float
    corrective: int  # 1: the next launch carries the corrective prompt prefix
    resume_mode: str | None
    pid: int | None
    pid_start: float | None
    kill_reason: str | None
    output_json: str | None
    error: str | None
    question_json: str | None
    next_attempt_at: float | None
    created_at: float
    started_at: float | None
    completed_at: float | None
    updated_at: float
    model_fallback_applied: int = 0  # 1: the task's agents files carry settings.model_fallback
    resume_cause: str | None = None  # why the next launch resumes or restarts (RESUME_CAUSES)

    @classmethod
    def from_row(cls, row: sqlite3.Row | Mapping[str, Any]) -> Self:
        return cls(**{f.name: row[f.name] for f in fields(cls)})

    @property
    def is_lifecycle(self) -> bool:
        return self.kind == KIND_LIFECYCLE

    @property
    def input_context_dict(self) -> dict[str, Any]:
        return _json_object(self.input_context, "input_context")

    @property
    def args_dict(self) -> dict[str, Any]:
        return _json_object(self.args_json, "args_json")

    @property
    def run_dir(self) -> Path:
        """The task's run directory, taken from ``args.run_dir`` (set by the engine at submit)."""
        raw = self.args_dict.get("run_dir")
        if not isinstance(raw, str) or not raw:
            raise ValueError(f"task {self.id} has no args.run_dir")
        return Path(raw)
