"""Contract between the engine and feature-lifecycle.js: every ask the script can return goes
through the engine's ``apply_return`` and ``answer()``, and the answers the engine appends are
consumed by the script on the resumed run."""

import json
import shutil
from typing import Any

import pytest

from src import engine_lifecycle
from src.database import write_tx
from tests.helpers import Env, submit, task_row
from tests.workflow.run import run_workflow
from tests.workflow.test_feature_lifecycle import (
    APPROVE,
    BLOCKED,
    CHANGES,
    DEPLOYED,
    GREEN,
    IMPL,
    MERGED,
    SCRIPT,
    _args,
    _responses,
    _with_epoch,
)

pytestmark = [
    pytest.mark.skipif(
        shutil.which("node") is None, reason="node is required for the workflow harness tests"
    ),
    pytest.mark.usefixtures("spawned"),
]

GO = {"action": "continue", "extra_rounds": 0, "guidance": ""}
ASKS = {"needs_human": {"question": "Which storage backend?"}}
ONE_ROUND = {"review_rounds": 1, "qa_rounds": 3}

# kind -> (script args, first-run responses, extra responses for the answered run)
CASES: dict[str, tuple[dict[str, Any], dict[str, list[Any]], dict[str, list[Any]]]] = {
    "plan_approval": (
        _args(plan_gate=True),
        _responses(),
        _with_epoch(_responses(), 1, skip=("plan",)) | {"interpret-plan-1": [GO]},
    ),
    "rounds_exhausted": (
        _args(limits=ONE_ROUND),
        _responses(**{"review-r1-rev-a": [CHANGES]}),
        {
            "interpret-review-r1-1": [GO],
            "review-r1-rev-a@a1": [APPROVE],
            "qa-r1@a1": [GREEN],
            "deploy@a1": [DEPLOYED],
            "merge@a1": [MERGED],
        },
    ),
    "merge_blocked": (
        _args(),
        _responses(merge=[BLOCKED]),
        {"interpret-merge-1": [GO], "qa-r1@a1": [GREEN], "merge@a1": [MERGED]},
    ),
    "question": (
        _args(),
        _responses(implement=[IMPL | ASKS]),
        _with_epoch(_responses(), 1, skip=("plan", "setup")),
    ),
}


async def engine_answers(env: Env, workflow_return: dict[str, Any]) -> list[dict[str, Any]]:
    """The answers list the engine builds from this return: apply_return, then answer()."""
    task_id = str((await submit(env, "Build it", agent="team-leader"))["task_id"])
    update: dict[str, Any] = {"status": "needs_human", "error": None}
    assert engine_lifecycle.apply_return(env.engine, update, workflow_return, None) is False
    assert update["status"] == "needs_human", update
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'needs_human', question_json = ?, output_json = ? "
            "WHERE id = ?",
            (update["question_json"], update["output_json"], task_id),
        )
    answered = await env.engine.answer(task_id, "go on", "mcp")
    assert answered["status"] == "queued", answered
    answers = (await task_row(task_id)).args_dict["answers"]
    assert isinstance(answers, list)
    return answers


@pytest.mark.parametrize("kind", list(CASES))
async def test_engine_answer_consumed_by_script(env: Env, kind: str) -> None:
    args, responses, after = CASES[kind]
    first = run_workflow(SCRIPT, args, responses)
    asked = first["return"]
    assert (asked["status"], asked["kind"]) == ("needs_human", kind), asked

    answers = await engine_answers(env, json.loads(json.dumps(asked)))
    assert answers == [
        {"k": 1, "label": asked["stage"], "mode": engine_lifecycle.answer_mode(kind)}
    ]

    second = run_workflow(SCRIPT, args | {"answers": answers}, responses | after)
    ret = second["return"]
    assert ret["status"] == "completed", ret
    assert "not consumed" not in json.dumps(ret)
    assert second["calls"][: len(first["calls"]) - 1] == first["calls"][:-1]


async def _ask(env: Env, task_id: str, workflow_return: dict[str, Any], text: str) -> None:
    update: dict[str, Any] = {"status": "needs_human", "error": None}
    assert engine_lifecycle.apply_return(env.engine, update, workflow_return, None) is False
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'needs_human', question_json = ?, output_json = ? "
            "WHERE id = ?",
            (update["question_json"], update["output_json"], task_id),
        )
    answered = await env.engine.answer(task_id, text, "mcp")
    assert answered["status"] == "queued", answered


async def test_engine_reask_after_unclear_interpret(env: Env) -> None:
    """An unclear answer asks again at the same gate; the engine's second answer
    (k=2, same label and mode) is consumed after interpret-plan-1 replays."""
    unclear = {"action": "unclear", "extra_rounds": 0, "guidance": ""}
    args = _args(plan_gate=True)
    task_id = str((await submit(env, "Build it", agent="team-leader"))["task_id"])
    first = run_workflow(SCRIPT, args, _responses())
    await _ask(env, task_id, json.loads(json.dumps(first["return"])), "hmm")
    one = (await task_row(task_id)).args_dict["answers"]
    responses = _responses() | {"interpret-plan-1": [unclear]}
    second = run_workflow(SCRIPT, args | {"answers": one}, responses)
    reasked = second["return"]
    assert (reasked["status"], reasked["stage"]) == ("needs_human", "plan"), reasked
    await _ask(env, task_id, json.loads(json.dumps(reasked)), "approved")
    two = (await task_row(task_id)).args_dict["answers"]
    assert two == [
        {"k": 1, "label": "plan", "mode": "after"},
        {"k": 2, "label": "plan", "mode": "after"},
    ]
    after = _with_epoch(_responses(), 2, skip=("plan",)) | {
        "interpret-plan-1": [unclear],
        "interpret-plan-2": [GO],
    }
    third = run_workflow(SCRIPT, args | {"answers": two}, after)
    assert third["return"]["status"] == "completed", third["return"]
    assert third["calls"][:2] == second["calls"][:2]
