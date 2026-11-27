"""The reporting connection: a second, read-only SQLite connection.

The engine's shared connection runs one statement at a time on one thread, so a dashboard
query on it would delay engine writes. Reports read through this connection instead (WAL lets
it read while the engine writes); it can never write (`mode=ro`, `query_only`).
"""

from pathlib import Path

import aiosqlite

_read_db: aiosqlite.Connection | None = None


async def open_read_db(path: Path) -> None:
    """Open the read-only connection to an existing database (after `init_db` created it)."""
    global _read_db
    await close_read_db()
    # as_uri percent-encodes the path: a raw `#`, `?` or `%` would end or alter the URI path.
    conn = await aiosqlite.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.execute("PRAGMA query_only = ON")
    except BaseException:
        await conn.close()
        raise
    _read_db = conn


def get_read_db() -> aiosqlite.Connection:
    if _read_db is None:
        raise RuntimeError("reporting database not open; call open_read_db() first")
    return _read_db


async def close_read_db() -> None:
    global _read_db
    if _read_db is not None:
        conn, _read_db = _read_db, None
        await conn.close()
