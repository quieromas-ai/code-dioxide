import hashlib
import json
import re
import unicodedata
from typing import Any

import aiosqlite

# Top-level input_context keys that change on every call and must not defeat dedupe.
VOLATILE_KEYS = frozenset({"timestamp", "nonce", "wake_id"})

_WHITESPACE_RUN = re.compile(r"\s+")


def _normalise_description(description: str) -> str:
    return _WHITESPACE_RUN.sub(" ", unicodedata.normalize("NFC", description)).strip()


def _strip_strings(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return {k: _strip_strings(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_strings(v) for v in value]
    return value


def _canonical_context(input_context: dict[str, Any] | None) -> str:
    kept = {k: v for k, v in (input_context or {}).items() if k not in VOLATILE_KEYS}
    return json.dumps(
        _strip_strings(kept), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def fingerprint(
    agent: str, description: str, input_context: dict[str, Any] | None, cwd: str
) -> str:
    """Stable sha256 hex of the work identity; agent and cwd are the already-resolved values."""
    canonical = json.dumps(
        [agent, _normalise_description(description), _canonical_context(input_context), cwd],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def find_duplicate(
    db: aiosqlite.Connection, fp: str, window_s: float, now: float
) -> str | None:
    """Id of the newest task with this fingerprint that is active or recently completed.

    Read-only; `db` must come from the caller's open `write_tx()` so the check and the
    following insert are one atomic step. A zero window disables the completed-task match.
    """
    sql = "SELECT id FROM tasks WHERE fingerprint = ? AND (status IN ('queued', 'running')"
    params: list[Any] = [fp]
    if window_s > 0:
        sql += " OR (status = 'completed' AND completed_at >= ?)"
        params.append(now - window_s)
    sql += ") ORDER BY created_at DESC, id DESC LIMIT 1"
    async with db.execute(sql, params) as cur:
        row = await cur.fetchone()
    return None if row is None else str(row["id"])
