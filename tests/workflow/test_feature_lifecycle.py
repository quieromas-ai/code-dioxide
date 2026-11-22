import ast
import re
import shutil
from pathlib import Path
from typing import Any

import pytest

from tests.workflow.run import run_workflow

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required for the workflow harness tests"
)

SCRIPT = Path(__file__).resolve().parents[2] / "workflows" / "feature-lifecycle.js"
RUN_DIR = "/srv/dioxide/runs/t-1"
WORKTREE = "/srv/dioxide/worktrees/ab12cd34/t-1"

PLAN = {"steps": ["add the thing"], "acceptance": ["thing works"], "needs_deploy": True}
SETUP = {"head_sha": "base0", "reused": False, "exit_code": 0}
IMPL = {"commits": ["c1"], "tests_green": True, "head_sha": "h-impl"}
FIX = {"commits": ["c2"], "tests_green": True, "head_sha": "h-fix"}
APPROVE = {"verdict": "approve", "findings": []}
CHANGES = {"verdict": "changes", "findings": ["rename foo"]}
GREEN = {"gate": "green", "failures": [], "evidence": "all passed", "head_sha": "h-qa"}
RED = {"gate": "red", "failures": ["test_bar fails"], "evidence": "1 failed", "head_sha": "h-x"}
DEPLOYED = {"deployed": True, "health_checks": ["health ok"]}
MERGED = {"merge_sha": "m1", "already_merged": False, "exit_code": 0}


def _args(**over: Any) -> dict[str, Any]:
    args: dict[str, Any] = {
        "task_id": "t-1",
        "run_dir": RUN_DIR,
        "repo": "/srv/repos/app",
        "base_branch": "main",
        "branch": "dioxide/feature-t1",
        "worktree": WORKTREE,
        "roles": {
            "planner": "lead",
            "implementer": "eng",
            "reviewers": ["rev-a"],
            "qa": "tester",
            "deployer": "ops",
        },
        "limits": {"review_rounds": 3, "qa_rounds": 3},
        "merge_strategy": "squash",
        "plan_gate": False,
        "plan_pinned": False,
        "answers": [],
    }
    args.update(over)
    return args


def _responses(**over: list[Any]) -> dict[str, list[Any]]:
    base: dict[str, list[Any]] = {
        "plan": [PLAN],
        "setup": [SETUP],
        "implement": [IMPL],
        "review-r1-rev-a": [APPROVE],
        "qa-r1": [GREEN],
        "deploy": [DEPLOYED],
        "merge": [MERGED],
    }
    base.update(over)
    return base


def _with_epoch(responses: dict[str, list[Any]], k: int, skip: tuple[str, ...]) -> dict[str, Any]:
    """Responses for labels after the k-th answer (``<label>@a<k>``), except ``skip``."""
    return {
        (label if label in skip else f"{label}@a{k}"): queue for label, queue in responses.items()
    }


def _run(args: dict[str, Any], responses: dict[str, list[Any]]) -> dict[str, Any]:
    """Run the lifecycle script; every run must give each call a unique label."""
    out = run_workflow(SCRIPT, args, responses)
    _assert_unique(out)
    return out


def _labels(out: dict[str, Any]) -> list[str]:
    return [c["label"] for c in out["calls"]]


def _call(out: dict[str, Any], label: str) -> dict[str, Any]:
    matches = [c for c in out["calls"] if c["label"] == label]
    assert len(matches) == 1, f"{label}: {len(matches)} calls"
    return matches[0]


def _assert_unique(out: dict[str, Any]) -> None:
    labels = _labels(out)
    assert len(labels) == len(set(labels)), labels


def test_meta_is_a_pure_literal() -> None:
    """The Workflow tool rejects a script whose ``meta`` holds any non-literal node (a string
    concatenation included) before running it."""
    source = SCRIPT.read_text(encoding="utf-8")
    found = re.search(r"^export const meta = (\{.*?^\});$", source, re.S | re.M)
    assert found, "no meta block"
    quoted = re.sub(r"([{,]\s*)([A-Za-z_]\w*)\s*:", r'\1"\2":', found.group(1))
    meta = ast.literal_eval(quoted)
    assert meta["name"] == "feature-lifecycle"
    assert isinstance(meta["description"], str)


def test_happy_path_order() -> None:
    roles = _args()["roles"] | {"reviewers": ["rev-a", "rev-b"]}
    out = _run(_args(roles=roles), _responses(**{"review-r1-rev-b": [APPROVE]}))
    assert _labels(out) == [
        "plan",
        "setup",
        "implement",
        "review-r1-rev-a",
        "review-r1-rev-b",
        "qa-r1",
        "deploy",
        "merge",
    ]
    assert [c["agentType"] for c in out["calls"]] == [
        "lead",
        None,
        "eng",
        "rev-a",
        "rev-b",
        "tester",
        "ops",
        None,
    ]
    assert all(c["schema"] is not None for c in out["calls"])
    assert out["return"] == {
        "status": "completed",
        "stage": "merge",
        "result": {
            "branch": "dioxide/feature-t1",
            "merge_sha": "m1",
            "gate": "green",
            "review": "approve",
        },
    }
    setup = _call(out, "setup")["prompt"]
    assert (
        f"bash '{RUN_DIR}/script/stage/setup.sh' '/srv/repos/app' 'main' "
        f"'dioxide/feature-t1' '{WORKTREE}'" in setup
    )
    assert "verbatim" in setup
    implement = _call(out, "implement")["prompt"]
    assert f"git -C '{WORKTREE}' log 'main..dioxide/feature-t1'" in implement
    merge = _call(out, "merge")["prompt"]
    assert (
        f"bash '{RUN_DIR}/script/stage/merge.sh' '/srv/repos/app' 'main' 'dioxide/feature-t1' "
        f"'{WORKTREE}' 'h-qa' 'squash' 't-1' '{RUN_DIR}/out/merge-subject.txt'" in merge
    )
    assert "verbatim" in merge
    for prompt in (setup, merge):
        assert "exit_code" in prompt and "makes no decision of its own" in prompt
    assert "are not plan steps" in _call(out, "plan")["prompt"]


def _assert_script_stages_role_less(out: dict[str, Any]) -> None:
    """setup/merge calls (any epoch) carry no agentType key; every other call has one."""
    for call in out["calls"]:
        script_stage = call["label"].split("@")[0] in ("setup", "merge")
        assert ("agentType" in call["optionKeys"]) is not script_stage, call["label"]
        assert (call["agentType"] is None) is script_stage, call["label"]


def test_script_stages_run_without_a_role() -> None:
    _assert_script_stages_role_less(_run(_args(), _responses()))
    answers = [{"k": 1, "label": "plan", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    responses = _with_epoch(_responses(), 1, skip=("plan",))
    responses["interpret-plan-1"] = [go]
    out = _run(_args(plan_gate=True, answers=answers), responses)
    assert "setup@a1" in _labels(out) and "merge@a1" in _labels(out)
    _assert_script_stages_role_less(out)
    blocked = _run(_args(), _responses(merge=[BLOCKED]))
    _assert_script_stages_role_less(blocked)
    answers = [{"k": 1, "label": "merge", "mode": "after"}]
    requalified = _responses(merge=[BLOCKED]) | {
        "interpret-merge-1": [go],
        "qa-r1@a1": [GREEN],
        "merge@a1": [MERGED],
    }
    out = _run(_args(answers=answers), requalified)
    assert _labels(out)[-1] == "merge@a1"
    _assert_script_stages_role_less(out)


def _script_stage_runs(**over: Any) -> list[dict[str, Any]]:
    """Runs reaching setup/merge in several epochs: plain, after a plan gate, merge blocked
    and re-qualified."""
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    gate = _with_epoch(_responses(), 1, skip=("plan",)) | {"interpret-plan-1": [go]}
    requalified = _responses(merge=[BLOCKED]) | {
        "interpret-merge-1": [go], "qa-r1@a1": [GREEN], "merge@a1": [MERGED],
    }  # fmt: skip
    return [
        _run(_args(**over), _responses()),
        _run(_args(plan_gate=True, answers=[{"k": 1, "label": "plan", "mode": "after"}], **over),
             gate),
        _run(_args(answers=[{"k": 1, "label": "merge", "mode": "after"}], **over), requalified),
    ]  # fmt: skip


def test_script_model_only_on_script_stages() -> None:
    """``script_model`` is passed as ``model`` to setup and merge (every epoch) and to nothing
    else; without it no call has a model; prompts and every other option stay the same."""
    plain = _script_stage_runs()
    cheap = _script_stage_runs(script_model="haiku")
    for without, with_model in zip(plain, cheap, strict=True):
        assert _labels(without) == _labels(with_model)
        for a, b in zip(without["calls"], with_model["calls"], strict=True):
            script_stage = a["label"].split("@")[0] in ("setup", "merge")
            assert a["model"] is None and "model" not in a["optionKeys"], a["label"]
            assert b["model"] == ("haiku" if script_stage else None), b["label"]
            assert ("model" in b["optionKeys"]) is script_stage, b["label"]
            assert b["prompt"] == a["prompt"] and b["agentType"] == a["agentType"]
            assert b["schema"] == a["schema"] and b["phase"] == a["phase"]
        assert without["return"] == with_model["return"]
    assert any("merge@a1" in _labels(out) for out in cheap)


def test_script_stages_take_no_guidance() -> None:
    answers = [{"k": 1, "label": "plan", "mode": "after"}]
    interpret = {"action": "continue", "extra_rounds": 0, "guidance": "do not push or merge"}
    responses = _with_epoch(_responses(), 1, skip=("plan",))
    responses["interpret-plan-1"] = [interpret]
    out = _run(_args(plan_gate=True, answers=answers), responses)
    assert out["return"]["status"] == "completed"
    assert "do not push or merge" in _call(out, "implement@a1")["prompt"]
    for label in ("setup@a1", "merge@a1"):
        prompt = _call(out, label)["prompt"]
        assert "do not push or merge" not in prompt, label
        assert "Human guidance" not in prompt and "plan" not in prompt.split("\n\n", 1)[1], label


def test_empty_guidance_adds_no_guidance_block() -> None:
    answers = [{"k": 1, "label": "plan", "mode": "after"}]
    for empty in ("", "  \n "):
        interpret = {"action": "continue", "extra_rounds": 0, "guidance": empty}
        responses = _with_epoch(_responses(), 1, skip=("plan",))
        responses["interpret-plan-1"] = [interpret]
        out = _run(_args(plan_gate=True, answers=answers), responses)
        assert out["return"]["status"] == "completed"
        assert not [c["label"] for c in out["calls"] if "Human guidance" in c["prompt"]]
    interp = _call(out, "interpret-plan-1")["prompt"]
    assert "empty string" in interp and "Never restate" in interp


def test_script_stage_nonzero_exit_stage_failed() -> None:
    setup = {"head_sha": "", "exit_code": 2, "error": "usage: setup.sh"}
    out = _run(_args(), _responses(setup=[setup]))
    assert (out["return"]["status"], out["return"]["stage"]) == ("stage_failed", "setup")
    assert out["return"]["error"] == "usage: setup.sh"

    merge = {"already_merged": False, "exit_code": 1}
    out = _run(_args(), _responses(merge=[merge]))
    ret = out["return"]
    assert (ret["status"], ret["stage"]) == ("stage_failed", "merge")
    assert ret["error"] == "stage script exited 1"


def test_script_stage_without_exit_code_stage_failed() -> None:
    for over in ({"setup": [{"head_sha": "base0"}]}, {"merge": [{"merge_sha": "m1"}]}):
        out = _run(_args(), _responses(**over))
        ret = out["return"]
        assert (ret["status"], ret["stage"]) == ("stage_failed", next(iter(over)))
        assert "did not return the script's result" in ret["error"]


def test_merge_needs_human_without_reason_stage_failed() -> None:
    for refusal in (
        {"already_merged": False, "needs_human": True, "exit_code": 0},
        {"already_merged": False, "needs_human": True, "reason": " ", "exit_code": 0},
        {"already_merged": False, "needs_human": True, "reason": "unsure", "exit_code": 3},
    ):
        out = _run(_args(), _responses(merge=[refusal]))
        ret = out["return"]
        assert (ret["status"], ret["stage"]) == ("stage_failed", "merge"), refusal
        assert "kind" not in ret


def test_no_deploy_without_deployer() -> None:
    roles = _args()["roles"] | {"deployer": None}
    out = _run(_args(roles=roles), _responses())
    assert "deploy" not in _labels(out)
    assert out["return"]["status"] == "completed"

    no_need = _responses(plan=[PLAN | {"needs_deploy": False}])
    out = _run(_args(), no_need)
    assert "deploy" not in _labels(out)
    assert out["return"]["status"] == "completed"


def test_review_loop_then_approve() -> None:
    roles = _args()["roles"] | {"reviewers": ["rev-a", "rev-b"]}
    responses = _responses(
        **{
            "review-r1-rev-a": [APPROVE],
            "review-r1-rev-b": [CHANGES],
            "fix-r1": [FIX],
            "review-r2-rev-a": [APPROVE],
            "review-r2-rev-b": [APPROVE],
        }
    )
    out = _run(_args(roles=roles), responses)
    assert _labels(out)[2:8] == [
        "implement",
        "review-r1-rev-a",
        "review-r1-rev-b",
        "fix-r1",
        "review-r2-rev-a",
        "review-r2-rev-b",
    ]
    fix = _call(out, "fix-r1")
    assert fix["agentType"] == "eng"
    assert "rename foo" in fix["prompt"] and "rev-b" in fix["prompt"]
    assert out["return"]["status"] == "completed"
    assert out["return"]["result"]["review"] == "approve"


def test_rounds_exhausted_gate_ask() -> None:
    limits = {"review_rounds": 2, "qa_rounds": 3}
    responses = _responses(
        **{
            "review-r1-rev-a": [CHANGES],
            "fix-r1": [FIX],
            "review-r2-rev-a": [CHANGES],
            "fix-r2": [FIX],
        }
    )
    first = _run(_args(limits=limits), responses)
    assert _labels(first) == [
        "plan",
        "setup",
        "implement",
        "review-r1-rev-a",
        "fix-r1",
        "review-r2-rev-a",
    ]
    ret = first["return"]
    assert ret["status"] == "needs_human"
    assert ret["stage"] == "review-r2"
    assert ret["kind"] == "rounds_exhausted"
    assert "rename foo" in ret["question"]

    answers = [{"k": 1, "label": "review-r2", "mode": "after"}]
    interpret = {"action": "continue", "extra_rounds": 1, "guidance": "accept the naming"}
    resumed = responses | {
        "interpret-review-r2-1": [interpret],
        "review-r1-rev-a@a1": [APPROVE],
        "qa-r1@a1": [GREEN],
        "deploy@a1": [DEPLOYED],
        "merge@a1": [MERGED],
    }
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][:6] == first["calls"]
    assert _labels(second)[6:] == [
        "interpret-review-r2-1",
        "review-r1-rev-a@a1",
        "qa-r1@a1",
        "deploy@a1",
        "merge@a1",
    ]
    assert f"{RUN_DIR}/inputs/answers/1.md" in second["calls"][6]["prompt"]
    assert "accept the naming" in _call(second, "review-r1-rev-a@a1")["prompt"]
    assert second["return"]["status"] == "completed"

    # extra_rounds bounds the new epoch: one more failed round asks again, without a fix
    again = resumed | {"review-r1-rev-a@a1": [CHANGES], "fix-r1@a1": [FIX]}
    third = _run(_args(limits=limits, answers=answers), again)
    assert _labels(third)[-1] == "review-r1-rev-a@a1"
    assert third["return"]["stage"] == "review-r1@a1"
    assert third["return"]["kind"] == "rounds_exhausted"


def test_qa_red_fix_green() -> None:
    responses = _responses(
        **{"qa-r1": [RED], "qa-fix-r1": [FIX], "qa-r2": [GREEN | {"head_sha": "h-qa2"}]}
    )
    out = _run(_args(), responses)
    assert _labels(out)[4:] == ["qa-r1", "qa-fix-r1", "qa-r2", "deploy", "merge"]
    qa_fix = _call(out, "qa-fix-r1")
    assert qa_fix["agentType"] == "eng"
    assert "test_bar fails" in qa_fix["prompt"]
    assert "'h-qa2' 'squash'" in _call(out, "merge")["prompt"]
    assert out["return"]["result"]["gate"] == "green"


def test_plan_gate_then_answer_plan_prompt_identical() -> None:
    first = _run(_args(plan_gate=True), _responses())
    assert _labels(first) == ["plan"]
    ret = first["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == ("needs_human", "plan", "plan_approval")
    assert "add the thing" in ret["question"]

    answers = [{"k": 1, "label": "plan", "mode": "after"}]
    interpret = {"action": "continue", "extra_rounds": 0, "guidance": "keep it small"}
    responses = _with_epoch(_responses(), 1, skip=("plan",))
    responses["interpret-plan-1"] = [interpret]
    second = _run(_args(plan_gate=True, answers=answers), responses)
    assert second["calls"][0] == first["calls"][0]
    assert _labels(second)[:3] == ["plan", "interpret-plan-1", "setup@a1"]
    interp = second["calls"][1]
    assert interp["agentType"] == "lead"
    assert f"{RUN_DIR}/inputs/answers/1.md" in interp["prompt"]
    assert "keep it small" in _call(second, "implement@a1")["prompt"]
    assert second["return"]["status"] == "completed"
    _assert_unique(second)


def test_rerun_ask_reruns_only_that_stage() -> None:
    asks = {"needs_human": {"question": "Which storage backend?"}}
    first = _run(_args(), _responses(implement=[IMPL | asks]))
    assert _labels(first) == ["plan", "setup", "implement"]
    ret = first["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == ("needs_human", "implement", "question")
    assert ret["question"] == "Which storage backend?"

    answers = [{"k": 1, "label": "implement", "mode": "rerun"}]
    responses = _with_epoch(_responses(), 1, skip=("plan", "setup"))
    second = _run(_args(answers=answers), responses)
    assert second["calls"][:2] == first["calls"][:2]
    assert _labels(second) == [
        "plan",
        "setup",
        "implement@a1",
        "review-r1-rev-a@a1",
        "qa-r1@a1",
        "deploy@a1",
        "merge@a1",
    ]
    rerun = second["calls"][2]["prompt"]
    original = first["calls"][2]["prompt"]
    assert rerun.startswith(original)
    assert f"{RUN_DIR}/inputs/answers/1.md" in rerun[len(original) :]
    assert not any(label.startswith("interpret-") for label in _labels(second))
    assert second["return"]["status"] == "completed"


def test_two_asks_epochs_and_prior_prompts_identical() -> None:
    asks = {"needs_human": {"question": "Which storage backend?"}}
    first = _run(_args(plan_gate=True), _responses())
    assert first["return"]["kind"] == "plan_approval"

    one = [{"k": 1, "label": "plan", "mode": "after"}]
    interpret = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    epoch1 = _with_epoch(_responses(implement=[IMPL | asks]), 1, skip=("plan",))
    epoch1["interpret-plan-1"] = [interpret]
    second = _run(_args(plan_gate=True, answers=one), epoch1)
    assert _labels(second) == ["plan", "interpret-plan-1", "setup@a1", "implement@a1"]
    assert second["return"]["stage"] == "implement@a1"
    assert second["return"]["kind"] == "question"

    two = one + [{"k": 2, "label": "implement@a1", "mode": "rerun"}]
    epoch2 = _with_epoch(_responses(), 2, skip=("plan",))
    epoch2 |= {"interpret-plan-1": [interpret], "setup@a1": [SETUP]}
    third = _run(_args(plan_gate=True, answers=two), epoch2)
    assert second["calls"][0] == first["calls"][0]
    assert third["calls"][:3] == second["calls"][:3]
    assert _labels(third)[3:] == [
        "implement@a2",
        "review-r1-rev-a@a2",
        "qa-r1@a2",
        "deploy@a2",
        "merge@a2",
    ]
    rerun = third["calls"][3]["prompt"]
    assert rerun.startswith(second["calls"][3]["prompt"])
    assert f"{RUN_DIR}/inputs/answers/2.md" in rerun
    assert third["return"]["status"] == "completed"
    _assert_unique(third)


def test_amend_replans_with_epoch_labels_unique() -> None:
    one = [{"k": 1, "label": "plan", "mode": "after"}]
    amend = {"action": "amend", "extra_rounds": 0, "guidance": "split the second step"}
    responses = _responses() | {"interpret-plan-1": [amend], "plan@a1": [PLAN]}
    second = _run(_args(plan_gate=True, answers=one), responses)
    assert _labels(second) == ["plan", "interpret-plan-1", "plan@a1"]
    assert "split the second step" in second["calls"][2]["prompt"]
    assert "split the second step" not in second["calls"][0]["prompt"]
    ret = second["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == ("needs_human", "plan@a1", "plan_approval")

    two = one + [{"k": 2, "label": "plan@a1", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    responses = _with_epoch(_responses(), 2, skip=("plan",))
    responses |= {"interpret-plan-1": [amend], "plan@a1": [PLAN], "interpret-plan@a1-2": [go]}
    third = _run(_args(plan_gate=True, answers=two), responses)
    assert third["calls"][:3] == second["calls"]
    assert _labels(third)[3:5] == ["interpret-plan@a1-2", "setup@a2"]
    assert third["return"]["status"] == "completed"
    _assert_unique(third)


def test_abort_cancelled() -> None:
    one = [{"k": 1, "label": "plan", "mode": "after"}]
    abort = {"action": "abort", "extra_rounds": 0, "guidance": ""}
    out = _run(_args(plan_gate=True, answers=one), _responses() | {"interpret-plan-1": [abort]})
    assert _labels(out) == ["plan", "interpret-plan-1"]
    assert out["return"]["status"] == "cancelled"
    assert out["return"]["stage"] == "interpret-plan-1"


def test_plan_pinned_skips_plan() -> None:
    out = _run(_args(plan_pinned=True, plan_gate=True), _responses())
    assert _labels(out) == ["setup", "implement", "review-r1-rev-a", "qa-r1", "deploy", "merge"]
    plan_ref = f"{RUN_DIR}/inputs/plan.json"
    assert plan_ref in _call(out, "implement")["prompt"]
    assert plan_ref in _call(out, "deploy")["prompt"]
    assert out["return"]["status"] == "completed"


def test_null_agent_stage_failed() -> None:
    out = _run(_args(), _responses(implement=[]))
    assert _labels(out) == ["plan", "setup", "implement"]
    assert out["return"]["status"] == "stage_failed"
    assert out["return"]["stage"] == "implement"

    roles = _args()["roles"] | {"reviewers": ["rev-a", "rev-b"]}
    out = _run(_args(roles=roles), _responses())
    assert out["return"]["status"] == "stage_failed"
    assert out["return"]["stage"] == "review-r1-rev-b"
    assert "qa-r1" not in _labels(out)


def test_mismatched_answer_stage_failed() -> None:
    answers = [{"k": 1, "label": "merge", "mode": "after"}]
    out = _run(_args(plan_gate=True, answers=answers), _responses())
    assert _labels(out) == ["plan"]
    assert out["return"]["status"] == "stage_failed"
    assert out["return"]["stage"] == "plan"


def test_prompts_reference_inputs_not_content() -> None:
    """Inputs reach stages only as paths: args carry no brief/plan/answer text (see
    test_args_deterministic_and_small), so this checks the path references and the preamble."""
    one = [{"k": 1, "label": "plan", "mode": "after"}]
    interpret = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    responses = _with_epoch(_responses(), 1, skip=("plan",))
    responses["interpret-plan-1"] = [interpret]
    out = _run(_args(plan_gate=True, answers=one), responses)
    assert out["return"]["status"] == "completed"
    brief_ref = f"{RUN_DIR}/inputs/brief.md"
    preamble = out["calls"][0]["prompt"].split("\n\n", 1)[0]
    assert WORKTREE in preamble and "t-1" in preamble
    for call in out["calls"]:
        assert call["prompt"].startswith(preamble), call["label"]
    for label in ("plan", "implement@a1", "review-r1-rev-a@a1", "qa-r1@a1"):
        assert brief_ref in _call(out, label)["prompt"], label
    assert f"{RUN_DIR}/inputs/answers/1.md" in _call(out, "interpret-plan-1")["prompt"]

    pinned = _run(_args(plan_pinned=True), _responses())
    for label in ("implement", "review-r1-rev-a", "qa-r1", "deploy"):
        assert f"{RUN_DIR}/inputs/plan.json" in _call(pinned, label)["prompt"], label


def test_setup_failure_stage_failed() -> None:
    failure = {"head_sha": "", "exit_code": 1, "error": "setup.sh: not a git repository"}
    out = _run(_args(), _responses(setup=[failure]))
    assert _labels(out) == ["plan", "setup"]
    ret = out["return"]
    assert (ret["status"], ret["stage"]) == ("stage_failed", "setup")
    assert ret["error"] == "setup.sh: not a git repository"

    out = _run(_args(), _responses(setup=[{"head_sha": "", "exit_code": 0}]))
    assert _labels(out) == ["plan", "setup"]
    assert out["return"]["status"] == "stage_failed"


def test_merge_without_sha_stage_failed() -> None:
    out = _run(_args(), _responses(merge=[{"already_merged": False, "exit_code": 0}]))
    ret = out["return"]
    assert (ret["status"], ret["stage"]) == ("stage_failed", "merge")
    assert "merge_sha" not in ret["result"]

    errored = {
        "already_merged": False,
        "merge_sha": "m1",
        "exit_code": 0,
        "error": "merge.sh: worktree not found",
    }
    out = _run(_args(), _responses(merge=[errored]))
    ret = out["return"]
    assert (ret["status"], ret["error"]) == ("stage_failed", "merge.sh: worktree not found")

    pr_url = {"already_merged": False, "pr_url": "u", "exit_code": 0}
    pr = _run(_args(), _responses(merge=[pr_url]))
    assert pr["return"]["status"] == "completed"
    assert pr["return"]["result"]["pr_url"] == "u"


BLOCKED = {
    "already_merged": False,
    "needs_human": True,
    "reason": "branch tip moved after QA",
    "exit_code": 0,
}


def test_merge_blocked_continue_requalifies_then_merges() -> None:
    first = _run(_args(), _responses(merge=[BLOCKED]))
    assert _labels(first)[-1] == "merge"
    ret = first["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == ("needs_human", "merge", "merge_blocked")
    assert "branch tip moved after QA" in ret["question"]

    answers = [{"k": 1, "label": "merge", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    responses = _responses(merge=[BLOCKED]) | {
        "interpret-merge-1": [go],
        "qa-r1@a1": [GREEN | {"head_sha": "h-qa3"}],
        "merge@a1": [MERGED],
    }
    second = _run(_args(answers=answers), responses)
    assert second["calls"][: len(first["calls"])] == first["calls"]
    assert _labels(second)[len(first["calls"]) :] == ["interpret-merge-1", "qa-r1@a1", "merge@a1"]
    assert "'h-qa3' 'squash'" in _call(second, "merge@a1")["prompt"]
    assert second["return"]["status"] == "completed"
    assert second["return"]["stage"] == "merge@a1"
    _assert_unique(second)


def test_merge_blocked_abort_cancelled() -> None:
    answers = [{"k": 1, "label": "merge", "mode": "after"}]
    abort = {"action": "abort", "extra_rounds": 0, "guidance": ""}
    responses = _responses(merge=[BLOCKED]) | {"interpret-merge-1": [abort]}
    out = _run(_args(answers=answers), responses)
    assert _labels(out)[-2:] == ["merge", "interpret-merge-1"]
    assert (out["return"]["status"], out["return"]["stage"]) == ("cancelled", "interpret-merge-1")


ASKS = {"needs_human": {"question": "Keep the public name?"}}


def test_rerun_ask_inside_loop_restarts_rounds() -> None:
    limits = {"review_rounds": 2, "qa_rounds": 3}
    responses = _responses(**{"review-r1-rev-a": [CHANGES], "fix-r1": [FIX | ASKS]})
    first = _run(_args(limits=limits), responses)
    assert _labels(first)[-1] == "fix-r1"
    assert (first["return"]["stage"], first["return"]["kind"]) == ("fix-r1", "question")

    answers = [{"k": 1, "label": "fix-r1", "mode": "rerun"}]
    resumed = responses | {
        "fix-r1@a1": [FIX],
        "review-r2-rev-a@a1": [CHANGES],
        "fix-r2@a1": [FIX],
    }
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][:4] == first["calls"][:4]
    assert _labels(second)[4:] == ["fix-r1@a1", "review-r2-rev-a@a1"]
    assert second["calls"][4]["prompt"].startswith(first["calls"][4]["prompt"])
    # the re-run fix is round 1 of epoch 1's budget of 2, so r2 exhausts it
    ret = second["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == (
        "needs_human",
        "review-r2@a1",
        "rounds_exhausted",
    )

    approved = resumed | {
        "review-r2-rev-a@a1": [APPROVE],
        "qa-r1@a1": [GREEN],
        "deploy@a1": [DEPLOYED],
        "merge@a1": [MERGED],
    }
    third = _run(_args(limits=limits, answers=answers), approved)
    assert _labels(third)[5:] == ["review-r2-rev-a@a1", "qa-r1@a1", "deploy@a1", "merge@a1"]
    assert third["return"]["status"] == "completed"


def test_rerun_ask_inside_qa_loop_restarts_budget() -> None:
    limits = {"review_rounds": 3, "qa_rounds": 2}
    responses = _responses(**{"qa-r1": [RED], "qa-fix-r1": [FIX | ASKS]})
    first = _run(_args(limits=limits), responses)
    assert (first["return"]["stage"], first["return"]["kind"]) == ("qa-fix-r1", "question")

    answers = [{"k": 1, "label": "qa-fix-r1", "mode": "rerun"}]
    resumed = responses | {"qa-fix-r1@a1": [FIX], "qa-r2@a1": [RED], "qa-fix-r2@a1": [FIX]}
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][:5] == first["calls"][:5]
    assert _labels(second)[5:] == ["qa-fix-r1@a1", "qa-r2@a1"]
    ret = second["return"]
    assert (ret["stage"], ret["kind"]) == ("qa-r2@a1", "rounds_exhausted")


def test_rerun_ask_raised_by_qa_stage() -> None:
    limits = {"review_rounds": 3, "qa_rounds": 2}
    responses = _responses(**{"qa-r1": [RED], "qa-fix-r1": [FIX], "qa-r2": [RED | ASKS]})
    first = _run(_args(limits=limits), responses)
    assert (first["return"]["stage"], first["return"]["kind"]) == ("qa-r2", "question")

    answers = [{"k": 1, "label": "qa-r2", "mode": "rerun"}]
    resumed = responses | {
        "qa-r2@a1": [RED],
        "qa-fix-r2@a1": [FIX],
        "qa-r3@a1": [RED],
        "qa-fix-r3@a1": [FIX],
    }
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][:6] == first["calls"][:6]
    assert _labels(second)[6:] == ["qa-r2@a1", "qa-fix-r2@a1", "qa-r3@a1"]
    assert second["calls"][6]["prompt"].startswith(first["calls"][6]["prompt"])
    ret = second["return"]
    assert (ret["stage"], ret["kind"]) == ("qa-r3@a1", "rounds_exhausted")

    green = resumed | {
        "qa-r2@a1": [GREEN | {"head_sha": "h-qa9"}],
        "deploy@a1": [DEPLOYED],
        "merge@a1": [MERGED],
    }
    third = _run(_args(limits=limits, answers=answers), green)
    assert _labels(third)[6:] == ["qa-r2@a1", "deploy@a1", "merge@a1"]
    assert "'h-qa9' 'squash'" in _call(third, "merge@a1")["prompt"]


def test_rerun_ask_raised_by_reviewer() -> None:
    roles = _args()["roles"] | {"reviewers": ["rev-a", "rev-b"]}
    responses = _responses(**{"review-r1-rev-b": [APPROVE | ASKS]})
    first = _run(_args(roles=roles), responses)
    assert (first["return"]["stage"], first["return"]["kind"]) == ("review-r1-rev-b", "question")

    answers = [{"k": 1, "label": "review-r1-rev-b", "mode": "rerun"}]
    resumed = _with_epoch(_responses(**{"review-r1-rev-b": [APPROVE]}), 1, ("plan", "setup"))
    resumed["implement"] = [IMPL]
    second = _run(_args(roles=roles, answers=answers), resumed)
    assert second["calls"][:3] == first["calls"][:3]
    assert _labels(second)[3:5] == ["review-r1-rev-a@a1", "review-r1-rev-b@a1"]
    assert second["calls"][3]["prompt"] == first["calls"][3]["prompt"]
    assert second["calls"][4]["prompt"].startswith(first["calls"][4]["prompt"])
    assert f"{RUN_DIR}/inputs/answers/1.md" in second["calls"][4]["prompt"]
    assert second["return"]["status"] == "completed"


def test_rerun_ask_raised_by_deploy() -> None:
    first = _run(_args(), _responses(deploy=[DEPLOYED | ASKS]))
    assert (first["return"]["stage"], first["return"]["kind"]) == ("deploy", "question")

    answers = [{"k": 1, "label": "deploy", "mode": "rerun"}]
    resumed = _responses() | {"deploy@a1": [DEPLOYED], "merge@a1": [MERGED]}
    second = _run(_args(answers=answers), resumed)
    assert second["calls"][:5] == first["calls"][:5]
    assert _labels(second)[5:] == ["deploy@a1", "merge@a1"]
    assert second["calls"][5]["prompt"].startswith(first["calls"][5]["prompt"])
    assert second["return"]["status"] == "completed"


def test_qa_rounds_exhausted_continue_replays_prefix() -> None:
    limits = {"review_rounds": 3, "qa_rounds": 1}
    responses = _responses(**{"qa-r1": [RED], "qa-fix-r1": [FIX]})
    first = _run(_args(limits=limits), responses)
    assert "qa-fix-r1" not in _labels(first)
    ret = first["return"]
    assert (ret["stage"], ret["kind"]) == ("qa-r1", "rounds_exhausted")
    assert "test_bar fails" in ret["question"]

    answers = [{"k": 1, "label": "qa-r1", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    resumed = responses | {
        "interpret-qa-r1-1": [go],
        "qa-r1@a1": [GREEN],
        "deploy@a1": [DEPLOYED],
        "merge@a1": [MERGED],
    }
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][: len(first["calls"])] == first["calls"]
    assert _labels(second)[len(first["calls"]) :] == [
        "interpret-qa-r1-1",
        "qa-r1@a1",
        "deploy@a1",
        "merge@a1",
    ]
    assert second["return"]["status"] == "completed"


def test_inlined_results_key_order_does_not_change_prompts() -> None:
    reordered = {key: PLAN[key] for key in reversed(list(PLAN))}
    assert list(reordered) != list(PLAN)
    a = _run(_args(), _responses())
    b = _run(_args(), _responses(plan=[reordered]))
    assert [c["prompt"] for c in a["calls"]] == [c["prompt"] for c in b["calls"]]


def test_unconsumed_answer_at_completion_stage_failed() -> None:
    answers = [{"k": 1, "label": "nowhere", "mode": "after"}]
    out = _run(_args(answers=answers), _responses())
    assert _labels(out)[-1] == "merge"
    ret = out["return"]
    assert (ret["status"], ret["stage"]) == ("stage_failed", "merge")
    assert "answer 1" in ret["error"] and "not consumed" in ret["error"]


def test_rounds_limit_one_asks_with_current_items_without_fix() -> None:
    limits = {"review_rounds": 1, "qa_rounds": 3}
    fixed = {"verdict": "changes", "findings": ["stale after fix"]}
    responses = _responses(**{"review-r1-rev-a": [CHANGES], "fix-r1": [FIX]})
    responses["review-r2-rev-a"] = [fixed]
    first = _run(_args(limits=limits), responses)
    assert _labels(first) == ["plan", "setup", "implement", "review-r1-rev-a"]
    ret = first["return"]
    assert (ret["status"], ret["stage"], ret["kind"]) == (
        "needs_human",
        "review-r1",
        "rounds_exhausted",
    )
    assert "rename foo" in ret["question"] and "after 1 round(s)" in ret["question"]

    answers = [{"k": 1, "label": "review-r1", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    resumed = responses | {
        "interpret-review-r1-1": [go],
        "review-r1-rev-a@a1": [CHANGES],
    }
    second = _run(_args(limits=limits, answers=answers), resumed)
    assert second["calls"][:4] == first["calls"]
    assert _labels(second)[4:] == ["interpret-review-r1-1", "review-r1-rev-a@a1"]
    assert (second["return"]["stage"], second["return"]["kind"]) == (
        "review-r1@a1",
        "rounds_exhausted",
    )


FULL_SHA = r"^[0-9a-f]{40,64}$"


def test_head_sha_schemas_require_full_sha() -> None:
    out = _run(_args(), _responses(**{"qa-r1": [RED], "qa-fix-r1": [FIX], "qa-r2": [GREEN]}))
    for label in ("implement", "qa-fix-r1", "qa-r1"):
        call = _call(out, label)
        assert call["schema"]["properties"]["head_sha"]["pattern"] == FULL_SHA, label
        assert f"full sha printed by `git -C '{WORKTREE}' rev-parse HEAD`" in call["prompt"], label


def test_interpret_prompt_amend_guidance_sentence() -> None:
    answers = [{"k": 1, "label": "plan", "mode": "after"}]
    go = {"action": "continue", "extra_rounds": 0, "guidance": ""}
    responses = _with_epoch(_responses(), 1, skip=("plan",))
    responses["interpret-plan-1"] = [go]
    out = _run(_args(plan_gate=True, answers=answers), responses)
    interp = _call(out, "interpret-plan-1")["prompt"]
    assert "For amend, guidance carries the changes the human asked for." in interp


# Working policy belongs to the user's agent definitions and settings, never to dioxide.
POLICY_DENYLIST = (
    "with tests",
    "review lens",
    "review focus",
    "focus on",
    "evidence format",
    "format your evidence",
    "conventional commit",
    "co-authored",
    "signed-off-by",
    "--permission-mode",
    "--allowedtools",
    "--dangerously",
)


def test_workflows_carry_no_working_policy() -> None:
    root = SCRIPT.parent
    files = sorted(p for p in root.rglob("*") if p.is_file())
    assert SCRIPT in files and root / "stage" / "merge.sh" in files
    for path in files:
        text = path.read_text(encoding="utf-8").lower()
        for phrase in POLICY_DENYLIST:
            assert phrase not in text, f"{path.name}: {phrase!r}"


def test_qa_prompt_names_evidence_dir() -> None:
    responses = _responses(
        **{"qa-r1": [RED], "qa-fix-r1": [FIX], "qa-r2": [GREEN | {"head_sha": "h-qa2"}]}
    )
    out = _run(_args(), responses)
    for label in ("qa-r1", "qa-r2"):
        prompt = _call(out, label)["prompt"]
        assert "Do not modify files or commits, with one exception: create the directory " in prompt
        assert f"{RUN_DIR}/out/evidence/{label}/ and save every evidence file" in prompt
        assert "(screenshots, logs) there; it is the only place you may write." in prompt
        assert "your evidence with the paths of those files" in prompt
