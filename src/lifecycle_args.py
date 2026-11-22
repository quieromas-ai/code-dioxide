"""The ``args`` object of the feature-lifecycle Workflow script.

Only small, engine-built values: ids, paths, roles, limits and the ``answers`` metadata. Large or
human-written content (brief, pinned plan, answers) lives in read-only files under the run's
``inputs/`` directory and is referenced by path from the stage prompts.
"""

from typing import Any

from src.config import Settings
from src.roles import Roles
from src.tasks import Task


def _required(task: Task, name: str) -> str:
    value = getattr(task, name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"task {task.id} has no {name}")
    return value


def build_lifecycle_args(task: Task, settings: Settings, roles: Roles) -> dict[str, Any]:
    """The lifecycle args for ``task``; pure, so equal inputs give byte-identical JSON.

    ``plan_gate`` is ``input_context.plan_gate is True``; ``plan_pinned`` is whether
    ``input_context.approved_plan`` was given (the engine writes it to ``inputs/plan.json``).
    ``answers`` starts empty and only ever grows by appending ``{k, label, mode}``.
    ``script_model`` (the setup and merge stages' model) is present only when the user set it:
    args frozen before it existed keep running unchanged (those stages on the parent's model).
    """
    context = task.input_context_dict
    args: dict[str, Any] = {
        "task_id": task.id,
        "run_dir": str(task.run_dir),
        "repo": _required(task, "cwd"),
        "base_branch": _required(task, "base_branch"),
        "branch": _required(task, "branch"),
        "worktree": _required(task, "worktree"),
        "roles": {
            "planner": roles.planner,
            "implementer": roles.implementer,
            "reviewers": list(roles.reviewers),
            "qa": roles.qa,
            "deployer": roles.deployer,
        },
        "limits": {"review_rounds": settings.review_rounds, "qa_rounds": settings.qa_rounds},
        "merge_strategy": settings.merge_strategy,
        "plan_gate": context.get("plan_gate") is True,
        "plan_pinned": context.get("approved_plan") is not None,
        "answers": [],
    }
    if settings.script_model is not None:
        args["script_model"] = settings.script_model
    return args
