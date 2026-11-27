"""Server-sent events framing."""

import json
from typing import Any

KEEPALIVE = ": keep-alive\n\n"


def sse(entry: dict[str, Any], event: str | None = None, event_id: str | None = None) -> str:
    """One event: optional ``event:`` and ``id:`` lines, then the JSON ``data:`` line."""
    head = f"event: {event}\n" if event else ""
    if event_id:
        head += f"id: {event_id}\n"
    return f"{head}data: {json.dumps(entry, ensure_ascii=False)}\n\n"
