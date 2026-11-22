"""Incremental parser for a ``claude -p --output-format stream-json`` stdout file.

Shapes verified against Claude Code 2.1.328 (Phase 0 findings B, C, D, A). The workflow's return
value is not in the stream: it is read from the run file after the process exits.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WORKFLOW_TOOL = "Workflow"
AUTH_ERROR = "authentication_failed"  # an assistant line's ``error`` when the session has no auth


@dataclass(frozen=True)
class Init:
    session_id: str | None
    tools: list[str]
    mcp_servers: list[Any]


@dataclass(frozen=True)
class WorkflowCall:
    tool_use_id: str
    input: dict[str, Any]


@dataclass(frozen=True)
class ParentToolUse:
    tool_use_id: str
    name: str


@dataclass(frozen=True)
class WorkflowAck:
    tool_use_id: str
    run_id: str | None
    task_id: str | None
    transcript_dir: str | None
    is_error: bool
    error_text: str | None


@dataclass(frozen=True)
class WorkflowDone:
    tool_use_id: str
    status: str | None


@dataclass(frozen=True)
class RateLimit:
    status: str | None
    resets_at: float | None
    limit_type: str | None


@dataclass(frozen=True)
class PermissionDenied:
    tool_name: str | None
    message: str | None


@dataclass(frozen=True)
class AuthFailed:
    """The parent session's synthetic assistant line for a request that could not be
    authenticated (``error: "authentication_failed"``); ``message``: its text."""

    message: str | None


@dataclass(frozen=True)
class Result:
    is_error: bool
    subtype: str | None
    api_error_status: int | None
    api_error: str | None
    text: str | None
    cost_usd: float | None
    tokens: int
    session_id: str | None
    errors: list[str]
    permission_denials: list[Any]
    result_index: int | None


Ev = (
    Init
    | WorkflowCall
    | ParentToolUse
    | WorkflowAck
    | WorkflowDone
    | RateLimit
    | PermissionDenied
    | AuthFailed
    | Result
)

_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _content_items(obj: dict[str, Any]) -> list[dict[str, Any]]:
    message = obj.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, list):
        return []
    return [item for item in content if isinstance(item, dict)]


def _texts(items: list[dict[str, Any]]) -> str | None:
    texts = [t for i in items if i.get("type") == "text" and isinstance(t := i.get("text"), str)]
    return "\n".join(texts) if texts else None


def _tool_result_text(content: Any) -> str | None:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [p.get("text") for p in content if isinstance(p, dict)]
        texts = [p for p in parts if isinstance(p, str)]
        return "\n".join(texts) if texts else None
    return None


def _workflow_input(raw: Any) -> dict[str, Any]:
    data = dict(raw) if isinstance(raw, dict) else {}
    args = data.get("args")
    if isinstance(args, str):
        try:
            data["args"] = json.loads(args)
        except ValueError:
            pass  # left as the raw string: it cannot match a canonical call
    return data


def _result(obj: dict[str, Any]) -> Result:
    usage = obj.get("usage")
    tokens = 0
    if isinstance(usage, dict):
        tokens = sum(_int(usage.get(name)) or 0 for name in _TOKEN_FIELDS)
    errors = obj.get("errors")
    denials = obj.get("permission_denials")
    return Result(
        is_error=obj.get("is_error") is True,
        subtype=_str(obj.get("subtype")),
        api_error_status=_int(obj.get("api_error_status")),
        api_error=_str(obj.get("api_error")),
        text=_str(obj.get("result")),
        cost_usd=_num(obj.get("total_cost_usd")),
        tokens=tokens,
        session_id=_str(obj.get("session_id")),
        errors=[e for e in errors if isinstance(e, str)] if isinstance(errors, list) else [],
        permission_denials=list(denials) if isinstance(denials, list) else [],
        result_index=_int(obj.get("result_index")),
    )


_TAIL_CHUNK = 64 * 1024
# The scan never reads further back than this: before the first turn ends there is no result
# record, and an unbounded scan would read the whole file on every status poll.
_MAX_SCAN = 4 * 1024 * 1024


def last_interim_result(path: Path) -> Result | None:
    """The last ``result`` record of a (possibly still growing) stream file, or None. The parent
    emits one per turn, so while an attempt runs this is its running total so far. Read from the
    end in growing chunks, at most ``_MAX_SCAN`` bytes back: the record is usually near the tail.
    A result record further back than that is not seen (None)."""
    try:
        with path.open("rb") as fh:
            size = fh.seek(0, 2)
            floor = max(0, size - _MAX_SCAN)
            chunk = _TAIL_CHUNK
            while True:
                start = max(floor, size - chunk)
                fh.seek(start)
                lines = fh.read(size - start).split(b"\n")
                if start > 0:
                    lines = lines[1:]  # the first line may start before the chunk
                for raw in reversed(lines):
                    if b'"result"' not in raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except ValueError:
                        continue  # a half-written tail line
                    if isinstance(obj, dict) and obj.get("type") == "result":
                        return _result(obj)
                if start == floor:
                    return None
                chunk *= 4
    except FileNotFoundError:
        return None


class StreamWatcher:
    """Reads complete lines from a growing stream file, from a remembered byte offset."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._offset = 0
        self._workflow_ids: set[str] = set()

    def reset(self) -> None:
        self._offset = 0
        self._workflow_ids = set()

    def poll(self) -> list[Ev]:
        try:
            with self.path.open("rb") as fh:
                fh.seek(self._offset)
                chunk = fh.read()
        except FileNotFoundError:
            return []
        end = chunk.rfind(b"\n")
        if end < 0:
            return []
        self._offset += end + 1
        events: list[Ev] = []
        for raw in chunk[: end + 1].splitlines():
            try:
                obj = json.loads(raw)
            except ValueError:
                continue
            if isinstance(obj, dict):
                events.extend(self._parse(obj))
        return events

    def _parse(self, obj: dict[str, Any]) -> list[Ev]:
        kind = obj.get("type")
        if kind == "assistant":
            return self._assistant(obj)
        if kind == "user":
            return self._user(obj)
        if kind == "result":
            return [_result(obj)]
        if kind == "rate_limit_event":
            info = obj.get("rate_limit_info")
            info = info if isinstance(info, dict) else {}
            return [
                RateLimit(
                    status=_str(info.get("status")),
                    resets_at=_num(info.get("resetsAt")),
                    limit_type=_str(info.get("rateLimitType")),
                )
            ]
        if kind != "system":
            return []
        subtype = obj.get("subtype")
        if subtype == "init":
            tools = obj.get("tools")
            servers = obj.get("mcp_servers")
            return [
                Init(
                    session_id=_str(obj.get("session_id")),
                    tools=[t for t in tools if isinstance(t, str)]
                    if isinstance(tools, list)
                    else [],
                    mcp_servers=list(servers) if isinstance(servers, list) else [],
                )
            ]
        if subtype == "task_notification":
            tool_use_id = _str(obj.get("tool_use_id"))
            if tool_use_id is None:
                return []
            return [WorkflowDone(tool_use_id=tool_use_id, status=_str(obj.get("status")))]
        if subtype == "permission_denied":
            return [
                PermissionDenied(
                    tool_name=_str(obj.get("tool_name")), message=_str(obj.get("message"))
                )
            ]
        return []

    def _assistant(self, obj: dict[str, Any]) -> list[Ev]:
        if obj.get("parent_tool_use_id") is not None:
            return []  # a subagent's call, not the parent session's
        events: list[Ev] = []
        if obj.get("error") == AUTH_ERROR:
            events.append(AuthFailed(_texts(_content_items(obj))))
        for item in _content_items(obj):
            if item.get("type") != "tool_use":
                continue
            tool_use_id = _str(item.get("id")) or ""
            name = _str(item.get("name")) or ""
            if name == WORKFLOW_TOOL:
                self._workflow_ids.add(tool_use_id)
                events.append(WorkflowCall(tool_use_id, _workflow_input(item.get("input"))))
            else:
                events.append(ParentToolUse(tool_use_id=tool_use_id, name=name))
        return events

    def _user(self, obj: dict[str, Any]) -> list[Ev]:
        events: list[Ev] = []
        meta = obj.get("tool_use_result")
        meta = meta if isinstance(meta, dict) else {}
        for item in _content_items(obj):
            tool_use_id = _str(item.get("tool_use_id"))
            if tool_use_id is None or tool_use_id not in self._workflow_ids:
                continue
            if item.get("type") != "tool_result":
                continue
            if item.get("is_error") is True:
                events.append(
                    WorkflowAck(
                        tool_use_id=tool_use_id,
                        run_id=None,
                        task_id=None,
                        transcript_dir=None,
                        is_error=True,
                        error_text=_tool_result_text(item.get("content")),
                    )
                )
                continue
            launched = meta.get("status") == "async_launched"
            events.append(
                WorkflowAck(
                    tool_use_id=tool_use_id,
                    run_id=_str(meta.get("runId")) if launched else None,
                    task_id=_str(meta.get("taskId")) if launched else None,
                    transcript_dir=_str(meta.get("transcriptDir")) if launched else None,
                    is_error=False,
                    error_text=None,
                )
            )
        return events
