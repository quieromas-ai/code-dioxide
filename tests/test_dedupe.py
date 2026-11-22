import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from src.database import close_db, get_db, init_db, write_tx
from src.dedupe import VOLATILE_KEYS, find_duplicate, fingerprint

NOW = 1_000_000.0
WINDOW = 900


@pytest.fixture
async def db_ready(tmp_path: Path) -> AsyncIterator[None]:
    await init_db(tmp_path / "dioxide.db")
    yield
    await close_db()


async def _insert(
    task_id: str,
    fp: str,
    status: str,
    *,
    kind: str = "single",
    created_at: float = NOW - 5000,
    completed_at: float | None = None,
) -> None:
    async with write_tx() as db:
        await db.execute(
            "INSERT INTO tasks"
            " (id, kind, status, fingerprint, created_at, completed_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (task_id, kind, status, fp, created_at, completed_at, created_at),
        )


async def _find(fp: str, window_s: int = WINDOW, now: float = NOW) -> str | None:
    async with write_tx() as db:
        return await find_duplicate(db, fp, window_s, now)


def test_fingerprint_canonical() -> None:
    base = fingerprint("dev", "do the thing", {"a": 1, "b": "x"}, "/repo")
    assert len(base) == 64 and int(base, 16) >= 0

    # reordered keys, whitespace in description and string values, Unicode form
    assert base == fingerprint("dev", "  do   the\n\tthing ", {"b": " x ", "a": 1}, "/repo")
    assert fingerprint("dev", "café", None, "/r") == fingerprint("dev", "café", {}, "/r")
    nested_a = fingerprint("dev", "d", {"k": {"z": [" v ", 2], "y": 1}}, "/r")
    nested_b = fingerprint("dev", "d", {"k": {"y": 1, "z": ["v", 2]}}, "/r")
    assert nested_a == nested_b

    # None context equals empty context
    assert fingerprint("dev", "d", None, "/r") == fingerprint("dev", "d", {}, "/r")

    # case is preserved, other fields differentiate
    assert base != fingerprint("dev", "Do the thing", {"a": 1, "b": "x"}, "/repo")
    assert base != fingerprint("dev", "do the thing", {"a": 1, "b": "x"}, "/other")
    assert base != fingerprint("qa", "do the thing", {"a": 1, "b": "x"}, "/repo")
    assert base != fingerprint("dev", "do the thing", {"a": 2, "b": "x"}, "/repo")

    # volatile keys ignored at the top level only
    assert VOLATILE_KEYS == frozenset({"timestamp", "nonce", "wake_id"})
    volatile = {"a": 1, "b": "x", "timestamp": "t1", "nonce": "n1", "wake_id": "w1"}
    assert base == fingerprint("dev", "do the thing", volatile, "/repo")
    assert fingerprint("dev", "d", {"n": {"timestamp": 1}}, "/r") != fingerprint(
        "dev", "d", {"n": {"timestamp": 2}}, "/r"
    )


async def test_dup_queued_running(db_ready: None) -> None:
    await _insert("t-q", "fp", "queued", created_at=NOW - 100)
    await _insert("t-r", "fp", "running", created_at=NOW - 50)
    await _insert("t-other", "other", "running", created_at=NOW - 10)

    assert await _find("fp") == "t-r"  # newest match wins
    assert await _find("fp", window_s=0) == "t-r"  # active tasks match regardless of window
    assert await _find("missing") is None

    async with write_tx() as db:
        await db.execute("UPDATE tasks SET status = 'cancelled' WHERE id = 't-r'")
    assert await _find("fp") == "t-q"


@pytest.mark.parametrize("kind", ["single", "lifecycle"])
async def test_dup_completed_within_window_single_and_lifecycle(db_ready: None, kind: str) -> None:
    await _insert("t-done", "fp", "completed", kind=kind, completed_at=NOW - 60)
    assert await _find("fp") == "t-done"
    # boundary: completed_at == now - window_s still matches
    await _insert("t-edge", "fp-edge", "completed", kind=kind, completed_at=NOW - WINDOW)
    assert await _find("fp-edge") == "t-edge"


@pytest.mark.parametrize("status", ["needs_human", "failed", "cancelled"])
async def test_no_dup_needs_human_failed_cancelled(db_ready: None, status: str) -> None:
    await _insert("t-1", "fp", status, completed_at=NOW - 10)
    assert await _find("fp") is None


async def test_no_dup_outside_window(db_ready: None) -> None:
    await _insert("t-old", "fp", "completed", completed_at=NOW - WINDOW - 1)
    assert await _find("fp") is None
    await _insert("t-null", "fp2", "completed", completed_at=None)
    assert await _find("fp2") is None


async def test_window_zero(db_ready: None) -> None:
    await _insert("t-done", "fp", "completed", completed_at=NOW)
    assert await _find("fp", window_s=0) is None
    assert await _find("fp", window_s=WINDOW) == "t-done"


async def test_check_and_insert_in_one_tx_is_atomic(db_ready: None) -> None:
    fp = fingerprint("dev", "same work", None, "/r")
    created: list[str] = []

    async def submit(task_id: str) -> str:
        async with write_tx() as db:
            existing = await find_duplicate(db, fp, WINDOW, NOW)
            if existing is not None:
                return existing
            await db.execute(
                "INSERT INTO tasks (id, status, fingerprint, created_at, updated_at)"
                " VALUES (?, 'queued', ?, ?, ?)",
                (task_id, fp, NOW, NOW),
            )
            created.append(task_id)
            return task_id

    results = await asyncio.gather(submit("a"), submit("b"))

    assert len(created) == 1
    assert results == [created[0], created[0]]
    async with get_db().execute("SELECT COUNT(*) FROM tasks WHERE fingerprint = ?", (fp,)) as cur:
        row = await cur.fetchone()
    assert row is not None and row[0] == 1
