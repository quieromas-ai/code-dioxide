import asyncio
import logging
import os
import re
import sqlite3
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import aiosqlite

from src.serverlog import LOGGER_NAME

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    agent TEXT,
    kind TEXT,
    description TEXT,
    input_context TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    fingerprint TEXT,
    cwd TEXT,
    base_branch TEXT,
    branch TEXT,
    worktree TEXT,
    session_id TEXT,
    run_id TEXT,
    args_json TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    resumes INTEGER NOT NULL DEFAULT 0,
    call_retries INTEGER NOT NULL DEFAULT 0,
    stage_retries INTEGER NOT NULL DEFAULT 0,
    rate_limit_waited_s REAL NOT NULL DEFAULT 0,
    corrective INTEGER NOT NULL DEFAULT 0,
    resume_mode TEXT CHECK (resume_mode IN ('first', 'resume', 'fresh')),
    pid INTEGER,
    pid_start REAL,
    kill_reason TEXT,
    output_json TEXT,
    error TEXT,
    question_json TEXT,
    next_attempt_at REAL,
    created_at REAL NOT NULL,
    started_at REAL,
    completed_at REAL,
    updated_at REAL NOT NULL,
    model_fallback_applied INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attempts (
    task_id TEXT NOT NULL,
    n INTEGER NOT NULL,
    session_id TEXT,
    argv_json TEXT,
    stream_path TEXT,
    stderr_path TEXT,
    started_at REAL,
    ended_at REAL,
    exit_code INTEGER,
    outcome TEXT,
    reason TEXT,  -- short machine value: a kill_reason or an outcome qualifier
    detail TEXT,  -- free text: error message, JSON pid list
    cost_usd REAL,
    tokens INTEGER,
    running_s REAL,
    PRIMARY KEY (task_id, n)
);

CREATE TABLE IF NOT EXISTS steps (
    task_id TEXT NOT NULL,
    label TEXT NOT NULL,
    phase TEXT,
    status TEXT NOT NULL,
    started_count INTEGER NOT NULL DEFAULT 0,
    cached INTEGER NOT NULL DEFAULT 0,
    agent_id TEXT,
    git_branch TEXT,
    git_head TEXT,
    started_at REAL,
    finished_at REAL,
    attempt INTEGER,  -- the attempt that last updated the row
    prev_started_count INTEGER NOT NULL DEFAULT 0,  -- started_count left by earlier attempts
    PRIMARY KEY (task_id, label)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT,
    type TEXT NOT NULL,
    data_json TEXT,
    at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS answers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    k INTEGER NOT NULL,
    label TEXT,
    mode TEXT CHECK (mode IN ('after', 'rerun')),
    question TEXT,
    answer TEXT,
    source TEXT,
    at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_fingerprint_status ON tasks (fingerprint, status);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at_id ON tasks (created_at, id);
CREATE INDEX IF NOT EXISTS idx_events_task_id ON events (task_id);
"""

# Bump on every schema change and migrate in `_check_version`.
SCHEMA_VERSION = 6
# Removed in v5: never-used notification fields (dropped from older DBs on open).
_V5_DROPPED_TASK_COLUMNS = ("slack_channel", "slack_ts")
_V5_DROPPED_TABLE = "slack_seen"
# Before a migration that drops data (to v5), the old file is copied once to `<name>.v<N>.bak`.
_BACKUP_BEFORE = 5
BACKUP_MODE = 0o600
# `ALTER TABLE ... DROP COLUMN` exists from SQLite 3.35.0; older libraries rebuild the table.
_DROP_COLUMN_MIN_SQLITE = (3, 35, 0)

_db: aiosqlite.Connection | None = None
_write_lock = asyncio.Lock()
_after_commit: list[Callable[[], None]] = []
_log = logging.getLogger(LOGGER_NAME)


async def init_db(path: Path) -> None:
    """Open the shared connection (WAL, FKs on) and create the schema if missing.

    Startup/test-only: swaps the module lock and connection, so never call it while writers run.
    """
    global _db, _write_lock
    if _db is not None:
        await close_db()
    path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: no implicit transactions; write_tx() issues BEGIN/COMMIT itself.
    conn = await aiosqlite.connect(path, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA foreign_keys = ON")
        await _backup_before_migration(conn, path)  # before SCHEMA touches the old file
        await conn.executescript(SCHEMA)
        await _check_version(conn)
    except BaseException:
        await conn.close()
        raise
    _db = conn
    _write_lock = asyncio.Lock()  # a Lock binds to one event loop; start fresh per init


async def _user_version(conn: aiosqlite.Connection) -> int:
    async with conn.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def _copy_db(conn: aiosqlite.Connection, target: Path) -> None:
    """A consistent copy of the open database (WAL content included) at ``target``."""
    await conn.execute("VACUUM INTO ?", (str(target),))


async def _backup_before_migration(conn: aiosqlite.Connection, path: Path) -> None:
    """Copy a pre-v5 database to ``<name>.v<N>.bak`` once (an existing backup is kept). A failed
    copy is a RuntimeError: the migration does not run without it."""
    version = await _user_version(conn)
    if not 1 <= version < _BACKUP_BEFORE:
        return
    backup = path.with_name(f"{path.name}.v{version}.bak")
    if backup.exists():
        return
    partial = backup.with_name(f".{backup.name}.tmp")
    try:
        partial.unlink(missing_ok=True)
        await _copy_db(conn, partial)
        partial.chmod(BACKUP_MODE)
        os.replace(partial, backup)
    except (OSError, sqlite3.Error) as exc:
        with suppress(OSError):
            partial.unlink(missing_ok=True)
        raise RuntimeError(
            f"cannot back up the v{version} database to {backup} before migrating it: {exc}"
        ) from exc
    _log.info("database backed up before migration",
              extra={"backup": str(backup), "from_version": version})  # fmt: skip


async def _check_version(conn: aiosqlite.Connection) -> None:
    """Migrate an older DB, stamp `SCHEMA_VERSION`; refuse a DB written by newer code.

    A fresh DB already has the current schema from `SCHEMA` and is stamped directly.
    """
    async with conn.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    version = int(row[0]) if row else 0
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"database schema version {version} is newer than this code ({SCHEMA_VERSION}); "
            "upgrade code-dioxide"
        )
    if 1 <= version < 2:  # v2: attempts.detail
        await _add_column(conn, "attempts", "detail", "TEXT")
    if 1 <= version < 3:  # v3: steps.attempt, steps.prev_started_count (cached after a restart)
        await _add_column(conn, "steps", "attempt", "INTEGER")
        await _add_column(conn, "steps", "prev_started_count", "INTEGER NOT NULL DEFAULT 0")
    if 1 <= version < 4:  # v4: tasks.stage_retries, tasks.corrective (recovery decider)
        await _add_column(conn, "tasks", "stage_retries", "INTEGER NOT NULL DEFAULT 0")
        await _add_column(conn, "tasks", "corrective", "INTEGER NOT NULL DEFAULT 0")
    if 1 <= version < 5:  # v5: unused notification columns and table removed
        await _drop_columns(conn, "tasks", _V5_DROPPED_TASK_COLUMNS)
        await conn.execute(f"DROP TABLE IF EXISTS {_V5_DROPPED_TABLE}")
    if 1 <= version < 6:  # v6: tasks.model_fallback_applied (rate-limit model fallback)
        await _add_column(conn, "tasks", "model_fallback_applied", "INTEGER NOT NULL DEFAULT 0")
    if version < SCHEMA_VERSION:
        await conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


async def _add_column(conn: aiosqlite.Connection, table: str, column: str, decl: str) -> None:
    if column not in await _columns(conn, table):
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def _supports_drop_column() -> bool:
    return sqlite3.sqlite_version_info >= _DROP_COLUMN_MIN_SQLITE


async def _columns(conn: aiosqlite.Connection, table: str) -> list[str]:
    async with conn.execute(f"PRAGMA table_info({table})") as cur:
        return [row[1] for row in await cur.fetchall()]


async def _drop_columns(conn: aiosqlite.Connection, table: str, columns: tuple[str, ...]) -> None:
    """Drop the listed columns that exist; rebuild ``table`` from ``SCHEMA`` on old SQLite."""
    existing = await _columns(conn, table)
    doomed = [column for column in columns if column in existing]
    if not doomed:
        return
    if _supports_drop_column():
        for column in doomed:
            await conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
        return
    await _rebuild_table(conn, table, [c for c in existing if c not in doomed])


async def _rebuild_table(conn: aiosqlite.Connection, table: str, keep: list[str]) -> None:
    """Recreate ``table`` with its ``SCHEMA`` definition, copying the ``keep`` columns."""
    match = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", SCHEMA, re.DOTALL)
    if match is None:
        raise RuntimeError(f"no {table} definition in SCHEMA")
    old = f"{table}_v4_old"
    cols = ", ".join(keep)
    await conn.execute("BEGIN IMMEDIATE")
    try:
        await conn.execute(f"ALTER TABLE {table} RENAME TO {old}")
        await conn.execute(match.group(0))
        await conn.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {old}")
        await conn.execute(f"DROP TABLE {old}")  # its indexes go with it: recreate them
        for index in re.findall(rf"CREATE INDEX IF NOT EXISTS \w+ ON {table} \([^)]*\);", SCHEMA):
            await conn.execute(index)
        await conn.execute("COMMIT")
    except BaseException:
        with suppress(sqlite3.OperationalError):
            await conn.execute("ROLLBACK")
        raise


def get_db() -> aiosqlite.Connection:
    """Return the single shared connection."""
    if _db is None:
        raise RuntimeError("database not initialised; call init_db() first")
    return _db


async def close_db() -> None:
    global _db
    if _db is not None:
        conn, _db = _db, None
        await conn.close()


async def _rollback(db: aiosqlite.Connection) -> None:
    # No-op when no transaction is open (e.g. cancelled before BEGIN ran).
    with suppress(sqlite3.OperationalError):
        await db.execute("ROLLBACK")


def after_commit(callback: Callable[[], None]) -> None:
    """Run ``callback`` once the open ``write_tx()`` commits, after the write lock is released.

    Dropped on rollback. A raising callback is logged and never affects the transaction. Call it
    only from inside a ``write_tx()`` block (the only time the lock is held by the caller).
    """
    if not _write_lock.locked():
        raise RuntimeError("after_commit() called outside write_tx()")
    _after_commit.append(callback)


@asynccontextmanager
async def write_tx() -> AsyncIterator[aiosqlite.Connection]:
    """Serialised write transaction: lock, BEGIN IMMEDIATE, commit or roll back and re-raise.

    aiosqlite still runs a queued statement when the awaiting task is cancelled, so BEGIN and
    COMMIT sit inside the guarded block and the rollback is shielded from cancellation.
    ``after_commit`` callbacks run after a successful commit, outside the lock.
    """
    db = get_db()
    committed: list[Callable[[], None]] = []
    async with _write_lock:
        try:
            await db.execute("BEGIN IMMEDIATE")
            yield db
            await db.execute("COMMIT")
            committed = list(_after_commit)
        except BaseException:
            await asyncio.shield(_rollback(db))
            raise
        finally:
            _after_commit.clear()
    for callback in committed:
        try:
            callback()
        except Exception:
            _log.exception("after-commit callback failed")
