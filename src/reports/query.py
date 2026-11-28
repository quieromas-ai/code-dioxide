"""Query parameters shared by the reporting routes: filters, time window, grouping, paging."""

import base64
import binascii
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from src.tasks import KIND_LIFECYCLE, KIND_SINGLE, TERMINAL

DEFAULT_WINDOW_S = 30 * 86400
LIST_LIMIT = 100
MAX_LIST_LIMIT = 1000
MAX_METRIC_ROWS = 5000
STATUSES = frozenset({"queued", "running", "needs_human"}) | TERMINAL
KINDS = frozenset({KIND_SINGLE, KIND_LIFECYCLE})
Bucket = Literal["none", "hour", "day"]
_BUCKET_S = {"hour": 3600, "day": 86400}


class QueryError(ValueError):
    """A bad query parameter; its message is the 400 response's error."""


def check_params(params: Mapping[str, str], allowed: frozenset[str]) -> None:
    """A misspelled parameter would silently return unfiltered data: refuse unknown ones."""
    for name in params:
        if name not in allowed:
            raise QueryError(
                f"unknown query parameter {name!r}: one of {', '.join(sorted(allowed))}"
            )


@dataclass(frozen=True)
class Filters:
    agent: str | None = None
    task: str | None = None
    project: str | None = None
    status: str | None = None
    kind: str | None = None
    since: float | None = None
    until: float | None = None
    stage_agent: str | None = None


def parse_time(value: str) -> float:
    """Epoch seconds, or ISO 8601 (a time without an offset is UTC)."""
    try:
        seconds = float(value)
    except ValueError:
        pass
    else:
        if not math.isfinite(seconds):
            raise QueryError(f"invalid time {value!r}")
        return seconds
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise QueryError(f"invalid time {value!r}: use ISO 8601 or epoch seconds") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _choice(params: Mapping[str, str], name: str, allowed: frozenset[str]) -> str | None:
    value = params.get(name)
    if value is not None and value not in allowed:
        raise QueryError(f"invalid {name} {value!r}: one of {', '.join(sorted(allowed))}")
    return value


def parse_filters(
    params: Mapping[str, str], *, now: float, windowed: bool, stage_agent: bool = False
) -> Filters:
    """The shared filters; ``windowed`` routes default to the last 30 days up to ``now``.
    ``stage_agent`` only where the route has stages (``where`` does not apply it)."""
    if not stage_agent and "stage_agent" in params:
        raise QueryError("stage_agent is not supported on this route")
    since = parse_time(params["since"]) if "since" in params else None
    until = parse_time(params["until"]) if "until" in params else None
    if windowed:
        until = now if until is None else until
        since = until - DEFAULT_WINDOW_S if since is None else since
    if since is not None and until is not None and since > until:
        raise QueryError("since must not be after until")
    return Filters(
        agent=params.get("agent"),
        task=params.get("task"),
        project=params.get("project"),
        status=_choice(params, "status", STATUSES),
        kind=_choice(params, "kind", KINDS),
        since=since,
        until=until,
        stage_agent=params.get("stage_agent") if stage_agent else None,
    )


def parse_group_by(value: str | None, allowed: tuple[str, ...]) -> tuple[str, ...]:
    keys = tuple(k.strip() for k in (value or "").split(",") if k.strip())
    for key in keys:
        if key not in allowed:
            raise QueryError(f"cannot group by {key!r} here: one of {', '.join(allowed)}")
    return keys


def parse_bucket(value: str | None) -> Bucket:
    if value is None or value == "none":
        return "none"
    if value == "hour":
        return "hour"
    if value == "day":
        return "day"
    raise QueryError(f"invalid bucket {value!r}: one of none, hour, day")


def bucket_start(at: float, bucket: str) -> float | None:
    """The UTC start of the bucket holding ``at``; None when not bucketed."""
    size = _BUCKET_S.get(bucket)
    return None if size is None else float(int(at // size) * size)


def parse_limit(value: str | None) -> int:
    if value is None:
        return LIST_LIMIT
    try:
        limit = int(value)
    except ValueError as exc:
        raise QueryError(f"invalid limit {value!r}") from exc
    return max(1, min(limit, MAX_LIST_LIMIT))


def encode_cursor(key: list[Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def decode_cursor(cursor: str) -> list[Any]:
    try:
        key = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except (ValueError, TypeError, binascii.Error) as exc:
        raise QueryError("invalid cursor") from exc
    if not isinstance(key, list):
        raise QueryError("invalid cursor")
    return key


def where(filters: Filters, *, time_column: str | None) -> tuple[str, list[Any]]:
    """An SQL condition over the ``tasks`` alias ``t`` (and ``time_column`` for the window)."""
    clauses: list[str] = []
    params: list[Any] = []
    for column, value in (
        ("t.agent", filters.agent),
        ("t.id", filters.task),
        ("t.cwd", filters.project),
        ("t.status", filters.status),
        ("t.kind", filters.kind),
    ):
        if value is not None:
            clauses.append(f"{column} = ?")
            params.append(value)
    if time_column is not None:
        if filters.since is not None:
            clauses.append(f"{time_column} >= ?")
            params.append(filters.since)
        if filters.until is not None:
            clauses.append(f"{time_column} < ?")
            params.append(filters.until)
    return (" AND ".join(clauses) or "1"), params
