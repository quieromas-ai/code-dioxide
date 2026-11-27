"""Stages of a lifecycle run: the stage a step label names, and the numbers its progress
entry reports (``workflowProgress`` items of type ``workflow_agent`` in the run file)."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_ATTEMPT = re.compile(r"@a\d+$")
_ROUND = re.compile(r"-r\d+(?=-|$)")


def normalize_stage(label: str) -> str:
    """``review-r1-dx-reviewer-correctness@a1`` → ``review-dx-reviewer-correctness``: the
    ``@aN`` attempt suffix and the ``-rN`` round segments stripped."""
    return _ROUND.sub("", _ATTEMPT.sub("", label))


@dataclass(frozen=True)
class StageNumbers:
    agent_type: str | None
    model: str | None
    stage_tokens: int | None
    tool_calls: int | None
    last_tool: str | None
    duration_s: float | None
    last_progress_at: float | None


def _count(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _ms(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        return None
    return float(value) / 1000


def _name(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def stage_numbers(entry: Mapping[str, Any] | None) -> StageNumbers:
    """The numbers of one progress entry; none for a missing or cached (replayed) entry, whose
    numbers belong to the run that measured them."""
    if entry is None or entry.get("cached") is True:
        return StageNumbers(None, None, None, None, None, None, None)
    return StageNumbers(
        agent_type=_name(entry.get("agentType")),
        model=_name(entry.get("model")),
        stage_tokens=_count(entry.get("tokens")),
        tool_calls=_count(entry.get("toolCalls")),
        last_tool=_name(entry.get("lastToolName")),
        duration_s=_ms(entry.get("durationMs")),
        last_progress_at=_ms(entry.get("lastProgressAt")),
    )
