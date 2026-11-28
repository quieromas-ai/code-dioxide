"""The reports-scope view of the existing task routes: an allowlist of fields per payload.

Keys not listed are dropped, so a field added to a payload later stays out of this scope until
someone lists it. What is listed carries ids, names, enum values, numbers and timestamps only:
descriptions, errors, output text, return values, questions, answers and branch names (slugs of
the description) never reach a reports caller.
"""

from typing import Any

LIST_KEYS = ("tasks", "count", "next_cursor", "error", "status")
LIST_ROW_KEYS = (
    "task_id", "agent", "kind", "status", "priority", "created_at", "started_at",
    "completed_at", "stage", "cost_usd", "uncounted_attempts", "cost_complete",
)  # fmt: skip
STATUS_KEYS = (
    "task_id", "status", "started_at", "completed_at", "stage", "steps", "running_steps",
    "resumes", "session_id", "run_id", "cost_usd", "cost_partial", "tokens_used", "waiting",
    "kill_reason", "recovery", "attempts_by_mode", "uncounted_attempts", "cost_complete",
    "output",
)  # fmt: skip
OUTPUT_KEYS = ("tokens_used", "cost_usd")
STEP_KEYS = (
    "label", "phase", "status", "started_count", "cached", "agent_id", "started_at",
    "finished_at", "timing",
)  # fmt: skip


def _only(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: payload[key] for key in keys if key in payload}


def reports_list(payload: dict[str, Any]) -> dict[str, Any]:
    view = _only(payload, LIST_KEYS)
    if isinstance(view.get("tasks"), list):
        view["tasks"] = [_only(row, LIST_ROW_KEYS) for row in view["tasks"]]
    return view


def reports_status(payload: dict[str, Any]) -> dict[str, Any]:
    view = _only(payload, STATUS_KEYS)
    if isinstance(view.get("output"), dict):
        view["output"] = _only(view["output"], OUTPUT_KEYS)
    elif "output" in view:
        view["output"] = None
    if isinstance(view.get("steps"), list):
        view["steps"] = [_only(step, STEP_KEYS) for step in view["steps"]]
    return view
