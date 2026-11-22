from pathlib import Path
from typing import Any

import pytest

from src.classify import Outcome, ScriptFailure, auth_message, classify, valid_return
from src.stream import (
    AuthFailed,
    Ev,
    Init,
    ParentToolUse,
    RateLimit,
    Result,
    StreamWatcher,
    WorkflowAck,
    WorkflowCall,
    WorkflowDone,
)

T = "toolu_1"
RESET = 1795114800.0
CALL = WorkflowCall(tool_use_id=T, input={"scriptPath": "/s.js", "args": {}})
ACK = WorkflowAck(
    tool_use_id=T, run_id="wf_1", task_id="w1", transcript_dir="/t", is_error=False, error_text=None
)
DONE = WorkflowDone(tool_use_id=T, status="completed")
INIT = Init(session_id="s", tools=[], mcp_servers=[])


def res(**kw: Any) -> Result:
    base: dict[str, Any] = {
        "is_error": False,
        "subtype": "success",
        "api_error_status": None,
        "api_error": None,
        "text": "",
        "cost_usd": 0.0,
        "tokens": 0,
        "session_id": "s",
        "errors": [],
        "permission_denials": [],
        "result_index": 0,
    }
    base.update(kw)
    return Result(**base)


def err_ack(text: str, tid: str = T) -> WorkflowAck:
    return WorkflowAck(
        tool_use_id=tid,
        run_id=None,
        task_id=None,
        transcript_dir=None,
        is_error=True,
        error_text=text,
    )


OK_RUN: list[Ev] = [INIT, CALL, ACK, DONE, res(), res(result_index=1)]
STAGE_429 = {
    "isApiErrorMessage": True,
    "apiErrorStatus": 429,
    "apiErrorParams": {"rate_limit_info": {"status": "rejected", "resetsAt": RESET}},
}
REJECTED = RateLimit("rejected", RESET, "five_hour")
NO_SESSION = "No conversation found with session ID: 17205b12-0d74-4fb1-83e8-5aaa30ba9192"

Case = tuple[str, dict[str, Any], Outcome, float | None]
CASES: list[Case] = [
    # 1. rate limits, structured only
    (
        "rl_event_rejected",
        {
            "events": [INIT, RateLimit("rejected", RESET, "five_hour"), res(is_error=True)],
            "exit_code": 1,
        },
        Outcome.rate_limited,
        RESET,
    ),
    (
        "rl_result_429",
        {"events": [INIT, res(is_error=True, api_error_status=429)], "exit_code": 1},
        Outcome.rate_limited,
        None,
    ),
    (
        "rl_result_usage_limit",
        {"events": [INIT, res(is_error=True, api_error="usage_limit_reached")], "exit_code": 1},
        Outcome.rate_limited,
        None,
    ),
    (
        "rl_stage_record",
        {
            "events": OK_RUN,
            "workflow_return": {"status": "stage_failed"},
            "stage_error_records": [STAGE_429],
        },
        Outcome.rate_limited,
        RESET,
    ),
    (
        "rl_allowed_is_not_limit",
        {
            "events": [RateLimit("allowed_warning", RESET, "five_hour"), *OK_RUN],
            "workflow_return": {"status": "completed"},
        },
        Outcome.success,
        None,
    ),
    # 2. resume refused by the CLI
    (
        "unknown_session",
        {
            "events": [res(is_error=True, subtype="error_during_execution", errors=[NO_SESSION])],
            "exit_code": 1,
        },
        Outcome.resume_refused,
        None,
    ),
    (
        "unknown_session_exit_none",
        {
            "events": [res(is_error=True, subtype="error_during_execution", errors=[NO_SESSION])],
            "exit_code": None,
        },
        Outcome.resume_refused,
        None,
    ),
    # 3. call problems (lifecycle)
    (
        "run_not_on_disk",
        {
            "events": [
                INIT,
                CALL,
                err_ack("The journal for workflow run wf_x is not on disk, and"),
                res(),
            ]
        },
        Outcome.resume_refused,
        None,
    ),
    (
        "tool_error_ack",
        {"events": [INIT, CALL, err_ack("scriptPath must be a script path"), res()]},
        Outcome.call_mismatch,
        None,
    ),
    ("no_call_clean_exit", {"events": [INIT, res()]}, Outcome.call_mismatch, None),
    (
        "second_call",
        {
            "events": [INIT, CALL, WorkflowCall("toolu_2", {}), ACK, DONE, res()],
            "workflow_return": {"status": "completed"},
        },
        Outcome.call_mismatch,
        None,
    ),
    (
        "parent_tool",
        {
            "events": [INIT, ParentToolUse("toolu_b", "Bash"), CALL, ACK, DONE, res()],
            "workflow_return": {"status": "completed"},
        },
        Outcome.parent_violation,
        None,
    ),
    # 4. not finished
    ("ack_no_done", {"events": [INIT, CALL, ACK, res()]}, Outcome.transient, None),
    (
        "done_other_call",
        {
            "events": [INIT, CALL, ACK, WorkflowDone("toolu_x", "completed"), res()],
            "workflow_return": {"status": "completed"},
        },
        Outcome.transient,
        None,
    ),
    (
        "done_failed",
        {
            "events": [INIT, CALL, ACK, WorkflowDone(T, "failed"), res()],
            "workflow_return": {"status": "completed"},
        },
        Outcome.transient,
        None,
    ),
    ("no_return", {"events": OK_RUN, "workflow_return": None}, Outcome.transient, None),
    (
        "killed_mid_run",
        {"events": [INIT, CALL, ACK], "exit_code": None},
        Outcome.transient,
        None,
    ),
    # 5. workflow return
    ("ret_completed", {"workflow_return": {"status": "completed"}}, Outcome.success, None),
    ("ret_needs_human", {"workflow_return": {"status": "needs_human"}}, Outcome.needs_human, None),
    (
        "ret_cancelled",
        {"workflow_return": {"status": "cancelled"}},
        Outcome.cancelled_by_workflow,
        None,
    ),
    (
        "ret_stage_failed",
        {"workflow_return": {"status": "stage_failed"}},
        Outcome.stage_failed,
        None,
    ),
    ("ret_unknown", {"workflow_return": {"status": "weird"}}, Outcome.fatal, None),
    ("ret_not_dict", {"workflow_return": "done"}, Outcome.fatal, None),
    (
        "ret_completed_exit_none",
        {"workflow_return": {"status": "completed"}, "exit_code": None},
        Outcome.success,
        None,
    ),
    # completed work beats rate-limit signals
    (
        "completed_with_stage_429",
        {"workflow_return": {"status": "completed"}, "stage_error_records": [STAGE_429]},
        Outcome.success,
        None,
    ),
    (
        "completed_with_rejected_event",
        {"events": [REJECTED, *OK_RUN], "workflow_return": {"status": "completed"}},
        Outcome.success,
        None,
    ),
    (
        "needs_human_with_rejected_event",
        {"events": [REJECTED, *OK_RUN], "workflow_return": {"status": "needs_human"}},
        Outcome.needs_human,
        None,
    ),
    (
        "cancelled_with_parent_429",
        {
            "events": [*OK_RUN, res(is_error=True, api_error_status=429)],
            "workflow_return": {"status": "cancelled"},
        },
        Outcome.cancelled_by_workflow,
        None,
    ),
    (
        "no_return_with_rejected_event",
        {"events": [REJECTED, *OK_RUN], "workflow_return": None},
        Outcome.rate_limited,
        RESET,
    ),
    (
        "call_mismatch_with_stage_429_not_limited",
        {
            "events": [INIT, CALL, err_ack("bad scriptPath"), res()],
            "stage_error_records": [STAGE_429],
        },
        Outcome.call_mismatch,
        None,
    ),
    # 6/7. single tasks
    (
        "single_ok_after_rejected_event",
        {"kind": "single", "events": [INIT, REJECTED, res()]},
        Outcome.success,
        None,
    ),
    (
        "single_ok_exit_none",
        {"kind": "single", "events": [INIT, res()], "exit_code": None},
        Outcome.success,
        None,
    ),
    ("single_ok", {"kind": "single", "events": [INIT, res()]}, Outcome.success, None),
    (
        "single_error",
        {"kind": "single", "events": [INIT, res(is_error=True)], "exit_code": 1},
        Outcome.transient,
        None,
    ),
    (
        "single_error_429",
        {"kind": "single", "events": [INIT, res(is_error=True, api_error_status=429)]},
        Outcome.rate_limited,
        None,
    ),
    (
        "single_nonzero_no_result",
        {"kind": "single", "events": [INIT], "exit_code": 2},
        Outcome.transient,
        None,
    ),
    (
        "single_ok_nonzero_exit",
        {"kind": "single", "events": [INIT, res()], "exit_code": 1},
        Outcome.transient,
        None,
    ),
    ("lifecycle_exit1_no_result", {"events": [INIT], "exit_code": 1}, Outcome.transient, None),
]


def run(**kw: Any) -> tuple[Outcome, float | None]:
    args: dict[str, Any] = {
        "exit_code": 0,
        "events": OK_RUN,
        "workflow_return": None,
        "stderr_tail": "",
        "stage_error_records": [],
        "kind": "lifecycle",
    }
    args.update(kw)
    return classify(**args)


@pytest.mark.parametrize(("name", "kw", "outcome", "reset"), CASES, ids=[c[0] for c in CASES])
def test_classify_matrix(
    name: str, kw: dict[str, Any], outcome: Outcome, reset: float | None
) -> None:
    assert run(**kw) == (outcome, reset)


def test_ratelimit_free_text_not_rate_limited() -> None:
    text = "You've hit your limit · resets 3pm (Europe/Paris)"
    outcome, reset = run(
        events=[INIT, res(is_error=True, text=text)],
        exit_code=1,
        stderr_tail=text,
        stage_error_records=[{"type": "assistant", "message": {"content": [{"text": text}]}}],
    )
    assert outcome is not Outcome.rate_limited
    assert reset is None
    outcome, _ = run(kind="single", events=[INIT, res(is_error=True, text=text)], exit_code=1)
    assert outcome is not Outcome.rate_limited


def test_exit_none_no_result_transient() -> None:
    assert run(exit_code=None, events=[]) == (Outcome.transient, None)
    assert run(exit_code=None, events=[INIT], kind="single") == (Outcome.transient, None)


def test_exit0_success_without_done_not_success() -> None:
    outcome, _ = run(
        exit_code=0,
        events=[INIT, CALL, err_ack("scriptPath must be a script path"), res()],
        workflow_return={"status": "completed"},
    )
    assert outcome is not Outcome.success


def test_script_exception_is_fatal_not_transient() -> None:
    failed = ScriptFailure(status="failed", error="TypeError: x is not a function")
    events = [INIT, CALL, ACK, WorkflowDone(tool_use_id=T, status="failed"), res()]
    assert run(events=events, workflow_return=failed) == (Outcome.fatal, None)
    assert run(workflow_return=failed) == (Outcome.fatal, None)  # whatever the notification says
    # the call checks still come first
    mismatch = [INIT, CALL, WorkflowCall("toolu_2", {}), ACK, DONE, res()]
    assert run(events=mismatch, workflow_return=failed) == (Outcome.call_mismatch, None)


def test_valid_return() -> None:
    for status in ("completed", "needs_human", "cancelled", "stage_failed"):
        assert valid_return({"status": status})
    for value in (None, "done", {"status": "weird"}, {}, ScriptFailure("failed", "x")):
        assert not valid_return(value)


BASH = ParentToolUse("toolu_b", "Bash")


def test_violation_after_completion_with_valid_return_does_not_fail() -> None:
    late = [INIT, CALL, ACK, DONE, BASH, res(), res(result_index=1)]
    assert run(events=late, workflow_return={"status": "completed"}) == (Outcome.success, None)
    asked = {"status": "needs_human", "stage": "plan", "kind": "question"}
    assert run(events=late, workflow_return=asked) == (Outcome.needs_human, None)
    # without a valid return the violation still counts
    assert run(events=late, workflow_return=None) == (Outcome.parent_violation, None)
    assert run(events=late, workflow_return={"status": "weird"}) == (Outcome.parent_violation, None)


def test_violation_before_completion_still_fails() -> None:
    ret = {"status": "completed"}
    for events in (
        [INIT, BASH, CALL, ACK, DONE, res()],
        [INIT, CALL, BASH, ACK, DONE, res()],
        [INIT, CALL, ACK, BASH, DONE, res()],
        [INIT, CALL, ACK, BASH, DONE, BASH, res()],
        [INIT, CALL, ACK, WorkflowDone("toolu_other", "completed"), BASH, DONE, res()],
    ):
        assert run(events=events, workflow_return=ret) == (Outcome.parent_violation, None)


def test_second_call_after_completion_with_valid_return_does_not_count() -> None:
    second = WorkflowCall("toolu_2", {"scriptPath": "/s.js", "args": {}})
    late = [INIT, CALL, ACK, DONE, second, err_ack("is not on disk", "toolu_2"), res()]
    assert run(events=late, workflow_return={"status": "completed"}) == (Outcome.success, None)
    # without a valid return, or before the completion, it is a call mismatch
    assert run(events=late, workflow_return=None) == (Outcome.resume_refused, None)
    early = [INIT, CALL, ACK, second, DONE, res()]
    assert run(events=early, workflow_return={"status": "completed"}) == (
        Outcome.call_mismatch, None,
    )  # fmt: skip


# -- authentication failures -------------------------------------------------------------------
AUTH_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "auth_failed.stream.jsonl"
AUTH_TEXT = "Failed to authenticate: OAuth session expired and could not be refreshed"
STAGE_AUTH = {
    "type": "assistant",
    "isSidechain": True,
    "error": "authentication_failed",
    "isApiErrorMessage": True,
    "apiErrorStatus": None,
    "message": {"content": [{"type": "text", "text": AUTH_TEXT}]},
}


def auth_events() -> list[Ev]:
    return StreamWatcher(AUTH_FIXTURE).poll()


@pytest.mark.parametrize("kind", ["single", "lifecycle"])
def test_auth_failure_from_the_install_evidence_is_auth_failed(kind: str) -> None:
    events = auth_events()
    assert AuthFailed(AUTH_TEXT) in events
    assert run(kind=kind, events=events, exit_code=1) == (Outcome.auth_failed, None)
    assert auth_message(events, []) == AUTH_TEXT


def test_auth_failure_result_text_is_the_fallback() -> None:
    events: list[Ev] = [INIT, res(is_error=True, text=AUTH_TEXT)]
    assert run(kind="single", events=events, exit_code=1) == (Outcome.auth_failed, None)
    assert auth_message(events, []) == AUTH_TEXT
    ok: list[Ev] = [INIT, res(is_error=False, text=AUTH_TEXT)]
    assert run(kind="single", events=ok, exit_code=0) == (Outcome.success, None)
    quoted: list[Ev] = [INIT, res(is_error=True, text=f"The log said: {AUTH_TEXT}")]
    assert run(kind="single", events=quoted, exit_code=1) == (Outcome.transient, None)


def test_auth_failure_wins_over_a_rate_limit_and_a_refused_resume() -> None:
    events: list[Ev] = [INIT, REJECTED, AuthFailed(AUTH_TEXT), res(is_error=True, text=AUTH_TEXT)]
    assert run(kind="single", events=events, exit_code=1) == (Outcome.auth_failed, None)
    refused = run(
        kind="lifecycle", events=[AuthFailed(AUTH_TEXT), res(is_error=True)], exit_code=1,
        stderr_tail=NO_SESSION,
    )  # fmt: skip
    assert refused == (Outcome.auth_failed, None)


def test_stage_auth_record_fails_the_parent_attempt() -> None:
    ret = {"status": "stage_failed", "stage": "implement", "error": "agent returned null"}
    outcome = run(events=OK_RUN, workflow_return=ret, stage_error_records=[STAGE_AUTH])
    assert outcome == (Outcome.auth_failed, None)
    assert auth_message(OK_RUN, [STAGE_AUTH]) == AUTH_TEXT
    unmarked = {k: v for k, v in STAGE_AUTH.items() if k != "isApiErrorMessage"}
    assert run(events=OK_RUN, workflow_return=ret, stage_error_records=[unmarked])[0] is (
        Outcome.stage_failed
    )


def test_auth_signal_after_completed_work_does_not_count() -> None:
    done = {"status": "completed"}
    assert run(events=[*OK_RUN, AuthFailed(AUTH_TEXT)], workflow_return=done) == (
        Outcome.success, None,
    )  # fmt: skip
    assert run(events=OK_RUN, workflow_return=done, stage_error_records=[STAGE_AUTH]) == (
        Outcome.success, None,
    )  # fmt: skip
