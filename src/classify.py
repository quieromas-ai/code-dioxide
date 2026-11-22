"""Decide what one finished attempt means, from structured signals only."""

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from src.stream import (
    AUTH_ERROR,
    AuthFailed,
    Ev,
    ParentToolUse,
    RateLimit,
    Result,
    WorkflowAck,
    WorkflowCall,
    WorkflowDone,
)
from src.tasks import KIND_LIFECYCLE

NO_SESSION = "No conversation found with session ID"
RUN_NOT_ON_DISK = "is not on disk"
USAGE_LIMIT = "usage_limit_reached"
# The text of an error result when the session could not authenticate (Claude Code 2.1.328):
# only a fallback for a stream without the structured ``authentication_failed`` line.
AUTH_FAILED_TEXT = re.compile(r"Failed to authenticate\b")


class Outcome(StrEnum):
    success = "success"
    needs_human = "needs_human"
    cancelled_by_workflow = "cancelled_by_workflow"
    rate_limited = "rate_limited"
    transient = "transient"
    stage_failed = "stage_failed"
    resume_refused = "resume_refused"
    call_mismatch = "call_mismatch"
    parent_violation = "parent_violation"
    auth_failed = "auth_failed"
    fatal = "fatal"


_RETURN_STATUS = {
    "completed": Outcome.success,
    "needs_human": Outcome.needs_human,
    "cancelled": Outcome.cancelled_by_workflow,
    "stage_failed": Outcome.stage_failed,
}


@dataclass(frozen=True)
class ScriptFailure:
    """This attempt's run file says the workflow script itself failed (an exception in the
    script, not a stage): its run status and error text, raw (redacted where served)."""

    status: str
    error: str


def valid_return(value: Any) -> bool:
    """A workflow return the engine can act on: an object with a known ``status``."""
    return isinstance(value, dict) and value.get("status") in _RETURN_STATUS


def _epoch(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _stage_record_reset(record: dict[str, Any]) -> float | None:
    params = record.get("apiErrorParams")
    info = params.get("rate_limit_info") if isinstance(params, dict) else None
    return _epoch(info.get("resetsAt")) if isinstance(info, dict) else None


_COMPLETED_WORK = {Outcome.success, Outcome.needs_human, Outcome.cancelled_by_workflow}
# Deterministic by nature: a relaunch after a wait would end the same way (a script exception or
# an unusable return replays; a refused resume needs a fresh session), so no rate-limit signal
# turns them into a wait.
_DETERMINISTIC = {Outcome.fatal, Outcome.resume_refused}
_STAGE_LIMITABLE = {Outcome.stage_failed, Outcome.transient}


def _rate_limit(
    events: list[Ev],
    last: Result | None,
    stage_error_records: list[dict[str, Any]],
    base: Outcome,
) -> tuple[bool, float | None]:
    """Structured rate-limit signals in the documented detection order; never free text.

    Completed work and deterministic outcomes (``fatal``, ``resume_refused``) always win. Parent
    signals (a rejected ``rate_limit_event``, a 429 / usage-limit result) count for any other
    outcome; stage transcript records count only for an attempt that would otherwise be
    ``stage_failed`` or ``transient`` (a stage that hit a limit and recovered leaves its record
    behind in a finished run).
    """
    if base in _COMPLETED_WORK or base in _DETERMINISTIC:
        return False, None
    rejected = [e for e in events if isinstance(e, RateLimit) and e.status == "rejected"]
    stage_hits = [
        r
        for r in stage_error_records
        if r.get("isApiErrorMessage") is True and r.get("apiErrorStatus") == 429
    ]
    if base not in _STAGE_LIMITABLE:
        stage_hits = []
    parent_hit = (
        last is not None
        and last.is_error
        and (last.api_error_status == 429 or last.api_error == USAGE_LIMIT)
    )
    if not (rejected or parent_hit or stage_hits):
        return False, None
    for event in reversed(rejected):
        if event.resets_at is not None:
            return True, event.resets_at
    for record in reversed(stage_hits):
        reset = _stage_record_reset(record)
        if reset is not None:
            return True, reset
    return True, None


def _stage_auth(stage_error_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        r
        for r in stage_error_records
        if r.get("isApiErrorMessage") is True and r.get("error") == AUTH_ERROR
    ]


def _auth_text_result(last: Result | None) -> bool:
    return last is not None and last.is_error and bool(AUTH_FAILED_TEXT.match(last.text or ""))


def _auth_failed(
    events: list[Ev], last: Result | None, stage_error_records: list[dict[str, Any]]
) -> bool:
    """The session (or a stage of its run) could not authenticate: the structured
    ``authentication_failed`` marker first, the error result's text only as a fallback."""
    if any(isinstance(e, AuthFailed) for e in events) or _stage_auth(stage_error_records):
        return True
    return _auth_text_result(last)


def _record_text(record: dict[str, Any]) -> str | None:
    message = record.get("message")
    content = message.get("content") if isinstance(message, dict) else message
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return None
    texts = [c.get("text") for c in content if isinstance(c, dict)]
    joined = "\n".join(t for t in texts if isinstance(t, str))
    return joined or None


def auth_message(events: list[Ev], stage_error_records: list[dict[str, Any]]) -> str:
    """Claude's own words for an ``auth_failed`` attempt (raw: redact where served)."""
    for event in events:
        if isinstance(event, AuthFailed) and event.message:
            return event.message
    for record in _stage_auth(stage_error_records):
        if text := _record_text(record):
            return text
    results = [e for e in events if isinstance(e, Result)]
    last = results[-1] if results else None
    if _auth_text_result(last) and last is not None and last.text:
        return last.text
    return "authentication failed"


def _resume_refused(exit_code: int | None, results: list[Result], stderr_tail: str) -> bool:
    if exit_code not in (1, None):
        return False
    for res in results:
        if res.subtype == "error_during_execution" and any(NO_SESSION in e for e in res.errors):
            return True
    return exit_code == 1 and NO_SESSION in stderr_tail


def _done_at(events: list[Ev]) -> int | None:
    """Index of the completion notification of the run the first ok launch ack names."""
    ok_ack = next((e for e in events if isinstance(e, WorkflowAck) and not e.is_error), None)
    if ok_ack is None:
        return None
    return next(
        (
            i
            for i, e in enumerate(events)
            if isinstance(e, WorkflowDone)
            and e.tool_use_id == ok_ack.tool_use_id
            and e.status == "completed"
        ),
        None,
    )


def _lifecycle(exit_code: int | None, events: list[Ev], workflow_return: Any) -> Outcome:
    done_at = _done_at(events)
    # What the parent did after this run reported completion with a valid return (a parent tool
    # call, a second Workflow call and its ack) does not count: the return wins.
    finished = done_at is not None and valid_return(workflow_return)
    before = events[:done_at] if finished else events
    calls = [e for e in before if isinstance(e, WorkflowCall)]
    error_acks = [e for e in before if isinstance(e, WorkflowAck) and e.is_error]
    if any(RUN_NOT_ON_DISK in (a.error_text or "") for a in error_acks):
        return Outcome.resume_refused
    if error_acks or len(calls) > 1 or (not calls and exit_code == 0):
        return Outcome.call_mismatch

    violations = [e for e in before if isinstance(e, ParentToolUse)]
    if violations:
        return Outcome.parent_violation

    if isinstance(workflow_return, ScriptFailure):
        return Outcome.fatal  # the script threw: a relaunch replays the same script
    if done_at is None or workflow_return is None:
        return Outcome.transient

    status = workflow_return.get("status") if isinstance(workflow_return, dict) else None
    return _RETURN_STATUS.get(status, Outcome.fatal) if isinstance(status, str) else Outcome.fatal


def classify(
    *,
    exit_code: int | None,
    events: list[Ev],
    workflow_return: Any,
    stderr_tail: str,
    stage_error_records: list[dict[str, Any]],
    kind: str | None,
) -> tuple[Outcome, float | None]:
    """Outcome of an attempt plus the structured rate-limit reset time (epoch s), if any.

    ``workflow_return`` is the run file's ``result`` (``src.journal.read_return``), a
    ``ScriptFailure`` (the run file says the script failed) or None.
    """
    results = [e for e in events if isinstance(e, Result)]
    last = results[-1] if results else None

    if _resume_refused(exit_code, results, stderr_tail):
        base = Outcome.resume_refused
    elif kind == KIND_LIFECYCLE:
        base = _lifecycle(exit_code, events, workflow_return)
    elif last is not None and not last.is_error and exit_code in (0, None):
        base = Outcome.success  # exit None: the pid died while detached; the stream decides
    else:
        base = Outcome.transient

    # Deterministic and before any wait: a relaunch (resume, fresh start, after a rate-limit
    # wait) would fail the same way until someone gives the service credentials.
    if base not in _COMPLETED_WORK and _auth_failed(events, last, stage_error_records):
        return Outcome.auth_failed, None
    limited, reset_at = _rate_limit(events, last, stage_error_records, base)
    if limited:
        return Outcome.rate_limited, reset_at
    return base, None
