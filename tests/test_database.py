import asyncio
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from src.database import SCHEMA_VERSION, close_db, get_db, init_db, write_tx
from src.tasks import Task

EXPECTED_TABLES = {
    "tasks", "attempts", "attempt_models", "steps", "events", "answers", "summaries",
    "summary_runs",
}  # fmt: skip


@pytest.fixture
async def db_path(tmp_path: Path) -> AsyncIterator[Path]:
    path = tmp_path / "dioxide.db"
    await init_db(path)
    yield path
    await close_db()


async def test_init_creates_tables(db_path: Path) -> None:
    db = get_db()
    async with db.execute("SELECT name FROM sqlite_master WHERE type = 'table'") as cur:
        tables = {row["name"] for row in await cur.fetchall()}
    assert EXPECTED_TABLES <= tables

    async with db.execute("SELECT name FROM sqlite_master WHERE type = 'index'") as cur:
        indexes = {row["name"] for row in await cur.fetchall()}
    assert {
        "idx_tasks_fingerprint_status",
        "idx_tasks_status",
        "idx_tasks_created_at_id",
        "idx_events_task_id",
        "idx_events_task_type_at",
        "idx_attempts_ended_at",
        "idx_tasks_completed_at",
    } <= indexes

    async with db.execute("PRAGMA journal_mode") as cur:
        row = await cur.fetchone()
    assert row is not None and row[0] == "wal"

    await init_db(db_path)  # idempotent


async def test_write_tx_serializes_concurrent_coroutines(db_path: Path) -> None:
    async with write_tx() as db:
        await db.execute("INSERT INTO events (type, data_json, at) VALUES ('counter', '0', 0)")

    async def increment() -> None:
        async with write_tx() as db:
            async with db.execute("SELECT data_json FROM events WHERE type = 'counter'") as cur:
                row = await cur.fetchone()
            assert row is not None
            await asyncio.sleep(0)  # yield so unserialized writers would interleave
            await db.execute(
                "UPDATE events SET data_json = ? WHERE type = 'counter'",
                (str(int(row["data_json"]) + 1),),
            )

    await asyncio.gather(*(increment() for _ in range(50)))

    async with get_db().execute("SELECT data_json FROM events WHERE type = 'counter'") as cur:
        row = await cur.fetchone()
    assert row is not None and row["data_json"] == "50"


async def test_write_tx_rollback_on_error(db_path: Path) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        async with write_tx() as db:
            await db.execute("INSERT INTO events (type, at) VALUES ('a', 1)")
            raise RuntimeError("boom")

    async with get_db().execute("SELECT COUNT(*) AS n FROM events") as cur:
        row = await cur.fetchone()
    assert row is not None and row["n"] == 0

    async with write_tx() as db:  # lock released, connection usable after rollback
        await db.execute("INSERT INTO events (type, at) VALUES ('a', 1)")


@pytest.mark.parametrize("ticks", [0, 1, 2, 3, 5])
async def test_write_tx_cancellation_leaves_connection_usable(db_path: Path, ticks: int) -> None:
    started = asyncio.Event()

    async def victim() -> None:
        async with write_tx() as db:
            started.set()
            await db.execute("INSERT INTO events (type, at) VALUES ('c', 1)")
            await asyncio.sleep(10)

    task = asyncio.create_task(victim())
    for _ in range(ticks):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with write_tx() as db:  # would raise "cannot start a transaction within a transaction"
        await db.execute("INSERT INTO events (type, at) VALUES ('d', 2)")
    assert not get_db().in_transaction
    async with get_db().execute("SELECT type FROM events") as cur:
        assert [row["type"] for row in await cur.fetchall()] == ["d"]


async def test_user_version_set_on_new_db(db_path: Path) -> None:
    async with get_db().execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    assert row is not None and row[0] == SCHEMA_VERSION == 9
    await init_db(db_path)  # an existing current-version DB opens fine


async def test_newer_user_version_rejected(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    with sqlite3.connect(path) as conn:
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    with pytest.raises(RuntimeError, match=r"schema version 10 is newer than this code \(9\)"):
        await init_db(path)


async def test_after_commit_runs_after_lock_release_and_not_on_rollback(db_path: Path) -> None:
    from src import database

    ran: list[bool] = []
    async with write_tx():
        database.after_commit(lambda: ran.append(database._write_lock.locked()))
        assert ran == []
    assert ran == [False]
    with pytest.raises(RuntimeError, match="boom"):
        async with write_tx():
            database.after_commit(lambda: ran.append(True))
            raise RuntimeError("boom")
    async with write_tx():
        database.after_commit(lambda: (_ for _ in ()).throw(OSError("mirror failed")))
    assert ran == [False]
    with pytest.raises(RuntimeError, match="outside write_tx"):
        database.after_commit(lambda: None)


async def test_version_1_database_migrated(tmp_path: Path) -> None:
    path = tmp_path / "v1.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE attempts (task_id TEXT NOT NULL, n INTEGER NOT NULL, session_id TEXT, "
            "argv_json TEXT, stream_path TEXT, stderr_path TEXT, started_at REAL, ended_at REAL, "
            "exit_code INTEGER, outcome TEXT, reason TEXT, cost_usd REAL, tokens INTEGER, "
            "running_s REAL, PRIMARY KEY (task_id, n))"
        )
        conn.execute("INSERT INTO attempts (task_id, n, outcome) VALUES ('t1', 1, 'ok')")
        conn.execute("PRAGMA user_version = 1")
    await init_db(path)
    try:
        db = get_db()
        async with db.execute("PRAGMA table_info(attempts)") as cur:
            columns = {row["name"] for row in await cur.fetchall()}
        assert "detail" in columns
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
        async with db.execute("SELECT task_id, n, outcome, detail FROM attempts") as cur:
            assert [tuple(r) for r in await cur.fetchall()] == [("t1", 1, "ok", None)]
        await init_db(path)  # reopening the migrated DB is a no-op
    finally:
        await close_db()


async def test_fresh_database_created_at_version_9(tmp_path: Path) -> None:
    await init_db(tmp_path / "fresh.db")
    try:
        async with get_db().execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
    finally:
        await close_db()


async def test_version_10_database_rejected(tmp_path: Path) -> None:
    path = tmp_path / "v10.db"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA user_version = 10")
    with pytest.raises(RuntimeError, match="schema version 10 is newer"):
        await init_db(path)


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0), reason="the fixture drops a column (SQLite 3.35+)"
)
async def test_version_5_database_migrated_to_6(tmp_path: Path) -> None:
    """v6 adds tasks.model_fallback_applied (0 for existing rows); no backup is needed."""
    path = tmp_path / "v5.db"
    await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE tasks DROP COLUMN model_fallback_applied")
        conn.execute(
            "INSERT INTO tasks (id, status, created_at, updated_at) VALUES ('t1', 'queued', 1, 1)"
        )
        conn.execute("PRAGMA user_version = 5")
    await init_db(path)
    try:
        async with get_db().execute("SELECT * FROM tasks") as cur:
            task = Task.from_row(await cur.fetchone())  # type: ignore[arg-type]
        assert (task.id, task.model_fallback_applied) == ("t1", 0)
        await init_db(path)  # reopening the migrated DB is a no-op
    finally:
        await close_db()
    fresh = tmp_path / "fresh.db"
    await init_db(fresh)
    await close_db()
    assert await _schema(path) == await _schema(fresh)
    assert not list(tmp_path.glob("*.bak"))


STAGE_COLUMNS = (
    "agent_type", "model", "stage_tokens", "tool_calls", "last_tool", "duration_s",
    "last_progress_at",
)  # fmt: skip


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0), reason="the fixture drops a column (SQLite 3.35+)"
)
async def test_v6_database_migrates_to_v7(tmp_path: Path) -> None:
    """v7 adds the stage numbers to steps (NULL for existing rows)."""
    path = tmp_path / "v6.db"
    await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        for column in STAGE_COLUMNS:
            conn.execute(f"ALTER TABLE steps DROP COLUMN {column}")
        conn.execute("INSERT INTO steps (task_id, label, status) VALUES ('t1', 'plan', 'running')")
        conn.execute("PRAGMA user_version = 6")
    await init_db(path)
    try:
        async with get_db().execute("PRAGMA table_info(steps)") as cur:
            columns = {row["name"] for row in await cur.fetchall()}
        assert set(STAGE_COLUMNS) <= columns
        async with get_db().execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
        async with get_db().execute("SELECT stage_tokens FROM steps") as cur:
            assert [tuple(r) for r in await cur.fetchall()] == [(None,)]
    finally:
        await close_db()
    fresh = tmp_path / "fresh.db"
    await init_db(fresh)
    await close_db()
    assert await _schema(path) == await _schema(fresh)


@pytest.mark.parametrize(
    ("version", "tables"),
    [(7, ("attempt_models", "summaries", "summary_runs")), (8, ("summaries", "summary_runs"))],
)
async def test_v7_and_v8_databases_migrate(
    tmp_path: Path, version: int, tables: tuple[str, ...]
) -> None:
    """v8 adds attempt_models, v9 summaries and summary_runs."""
    path = tmp_path / f"v{version}.db"
    await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        for table in tables:
            conn.execute(f"DROP TABLE {table}")
        conn.execute(f"PRAGMA user_version = {version}")
    await init_db(path)
    try:
        async with get_db().execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
    finally:
        await close_db()
    fresh = tmp_path / "fresh.db"
    await init_db(fresh)
    await close_db()
    assert await _schema(path) == await _schema(fresh)


async def test_version_2_database_migrated_to_3(tmp_path: Path) -> None:
    path = tmp_path / "v2.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE steps (task_id TEXT NOT NULL, label TEXT NOT NULL, phase TEXT, "
            "status TEXT NOT NULL, started_count INTEGER NOT NULL DEFAULT 0, "
            "cached INTEGER NOT NULL DEFAULT 0, agent_id TEXT, git_branch TEXT, git_head TEXT, "
            "started_at REAL, finished_at REAL, PRIMARY KEY (task_id, label))"
        )
        conn.execute(
            "INSERT INTO steps (task_id, label, status, started_count) "
            "VALUES ('t1', 'plan', 'completed', 2)"
        )
        conn.execute("PRAGMA user_version = 2")
    await init_db(path)
    try:
        db = get_db()
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
        async with db.execute(
            "SELECT label, started_count, attempt, prev_started_count FROM steps"
        ) as cur:
            assert [tuple(r) for r in await cur.fetchall()] == [("plan", 2, None, 0)]
        await init_db(path)  # reopening the migrated DB is a no-op
    finally:
        await close_db()


async def test_version_3_database_migrated_to_4(tmp_path: Path) -> None:
    """v4 adds the decider's counters to an existing tasks table; old rows default to 0."""
    path = tmp_path / "v3.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, agent TEXT, kind TEXT, description TEXT, "
            "input_context TEXT, priority INTEGER NOT NULL DEFAULT 0, "
            "max_retries INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, fingerprint TEXT, "
            "cwd TEXT, base_branch TEXT, branch TEXT, worktree TEXT, session_id TEXT, "
            "run_id TEXT, args_json TEXT, attempt INTEGER NOT NULL DEFAULT 0, "
            "resumes INTEGER NOT NULL DEFAULT 0, call_retries INTEGER NOT NULL DEFAULT 0, "
            "rate_limit_waited_s REAL NOT NULL DEFAULT 0, resume_mode TEXT, pid INTEGER, "
            "pid_start REAL, kill_reason TEXT, output_json TEXT, error TEXT, "
            "question_json TEXT, slack_channel TEXT, slack_ts TEXT, next_attempt_at REAL, "
            "created_at REAL NOT NULL, started_at REAL, completed_at REAL, "
            "updated_at REAL NOT NULL)"
        )
        conn.execute(
            "INSERT INTO tasks (id, status, resumes, created_at, updated_at) "
            "VALUES ('t1', 'queued', 2, 1, 1)"
        )
        conn.execute("PRAGMA user_version = 3")
    await init_db(path)
    try:
        db = get_db()
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
        async with db.execute("SELECT * FROM tasks") as cur:
            task = Task.from_row(await cur.fetchone())  # type: ignore[arg-type]
        assert (task.resumes, task.stage_retries, task.corrective) == (2, 0, 0)
        await init_db(path)  # reopening the migrated DB is a no-op
    finally:
        await close_db()


# The v4 schema's unused notification columns and table, dropped in v5.
V4_TASK_COLUMNS = ("slack_channel", "slack_ts")
V4_TABLE = "slack_seen"


async def _schema(path: Path) -> dict[str, list[tuple[object, ...]]]:
    """Each table's columns (name, type, notnull, default, pk), order-independent, plus indexes."""
    with sqlite3.connect(path) as conn:
        names = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        schema = {
            name: sorted(tuple(r[1:]) for r in conn.execute(f"PRAGMA table_info({name})"))
            for name in names
        }
        schema["(indexes)"] = sorted(
            (r[0], r[1])
            for r in conn.execute(
                "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )
    return schema


async def test_version_4_database_migrated_to_5(tmp_path: Path) -> None:
    """v5 drops the unused task columns and table; rows survive and the result equals a fresh DB."""
    path = tmp_path / "v4.db"
    await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        for column in V4_TASK_COLUMNS:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} TEXT")
        conn.execute(f"CREATE TABLE {V4_TABLE} (channel TEXT NOT NULL, ts TEXT NOT NULL, "
                     "PRIMARY KEY (channel, ts))")  # fmt: skip
        conn.execute(
            "INSERT INTO tasks (id, status, resumes, created_at, updated_at, "
            f"{V4_TASK_COLUMNS[0]}) VALUES ('t1', 'queued', 2, 1, 1, 'c')"
        )
        conn.execute("PRAGMA user_version = 4")
    await init_db(path)
    try:
        db = get_db()
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == 9
        async with db.execute("SELECT * FROM tasks") as cur:
            task = Task.from_row(await cur.fetchone())  # type: ignore[arg-type]
        assert (task.id, task.resumes) == ("t1", 2)
        await init_db(path)  # reopening the migrated DB is a no-op
    finally:
        await close_db()
    fresh = tmp_path / "fresh.db"
    await init_db(fresh)
    await close_db()
    migrated_schema = await _schema(path)
    assert V4_TABLE not in migrated_schema
    assert not set(V4_TASK_COLUMNS) & {c[0] for c in migrated_schema["tasks"]}
    assert migrated_schema == await _schema(fresh)


async def test_drop_column_falls_back_to_rebuild_on_old_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Below SQLite 3.35 (no DROP COLUMN) the table is rebuilt; rows and indexes survive."""
    from src import database

    monkeypatch.setattr(database, "_supports_drop_column", lambda: False)
    await test_version_4_database_migrated_to_5(tmp_path)


async def make_v4(path: Path) -> None:
    await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        for column in V4_TASK_COLUMNS:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} TEXT")
        conn.execute(
            "INSERT INTO tasks (id, status, created_at, updated_at) VALUES ('t1', 'queued', 1, 1)"
        )
        conn.execute("PRAGMA user_version = 4")


async def test_v4_backup_before_migration(tmp_path: Path) -> None:
    path = tmp_path / "dioxide.db"
    await make_v4(path)
    await init_db(path)
    await close_db()
    backup = tmp_path / "dioxide.db.v4.bak"
    with sqlite3.connect(backup) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        columns = {r[1] for r in conn.execute("PRAGMA table_info(tasks)")}
        assert set(V4_TASK_COLUMNS) <= columns  # the copy predates the migration
        assert conn.execute("SELECT id FROM tasks").fetchall() == [("t1",)]
    assert backup.stat().st_mode & 0o777 == 0o600
    before = backup.read_bytes()
    with sqlite3.connect(path) as conn:  # an older v4 again: the existing backup is kept
        conn.execute("PRAGMA user_version = 4")
    await init_db(path)
    await close_db()
    assert backup.read_bytes() == before
    fresh = tmp_path / "fresh.db"
    await init_db(fresh)
    await close_db()
    assert not list(tmp_path.glob("fresh.db*.bak"))  # a new database needs no backup


async def test_failed_backup_aborts_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src import database

    path = tmp_path / "dioxide.db"
    await make_v4(path)

    async def broken(conn: object, target: Path) -> None:
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(database, "_copy_db", broken)
    with pytest.raises(RuntimeError, match="backup"):
        await init_db(path)
    await close_db()
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4  # not migrated
    assert not list(tmp_path.glob("*.bak*"))


async def test_events_type_at_index_serves_window_queries(tmp_path: Path) -> None:
    await init_db(tmp_path / "d.db")
    try:
        async with get_db().execute(
            "EXPLAIN QUERY PLAN SELECT * FROM events e WHERE e.type IN ('a', 'b') "
            "AND e.at >= 1 AND e.at < 2"
        ) as cur:
            plan = " ".join(str(r["detail"]) for r in await cur.fetchall())
    finally:
        await close_db()
    assert "idx_events_type_at" in plan, plan
