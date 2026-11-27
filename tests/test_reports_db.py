import sqlite3
from pathlib import Path

import pytest

import src.main
import src.reports.db as read_db
from src.config import Settings
from src.reports.db import close_read_db, get_read_db, open_read_db
from tests.helpers import Env, submit

TOKEN = "test-token-value"


async def test_read_db_refuses_writes(env: Env) -> None:
    await open_read_db(env.settings.db_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            await get_read_db().execute("DELETE FROM tasks")
    finally:
        await close_read_db()


async def test_read_db_sees_committed_rows(env: Env) -> None:
    await submit(env)
    await open_read_db(env.settings.db_path)
    try:
        async with get_read_db().execute("SELECT count(*) AS n FROM tasks") as cur:
            row = await cur.fetchone()
        assert row is not None and row["n"] == 1
    finally:
        await close_read_db()


async def test_read_db_not_open_raises() -> None:
    with pytest.raises(RuntimeError):
        get_read_db()
    await close_read_db()  # idempotent


async def test_read_db_open_and_closed_by_lifespan(tmp_path: Path) -> None:
    app = src.main.create_app(Settings(home=tmp_path, auth_token=TOKEN))
    async with app.router.lifespan_context(app):
        assert read_db._read_db is not None
    assert read_db._read_db is None


async def test_read_db_closed_on_startup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(_: Path) -> bytes:
        raise RuntimeError("cursor key unusable")

    monkeypatch.setattr(src.main, "load_key", broken)
    app = src.main.create_app(Settings(home=tmp_path, auth_token=TOKEN))
    with pytest.raises(RuntimeError):
        async with app.router.lifespan_context(app):
            pass
    assert read_db._read_db is None


async def test_read_db_path_with_uri_characters(tmp_path: Path) -> None:
    home = tmp_path / "odd #1 ?x %20"
    app = src.main.create_app(Settings(home=home, auth_token=TOKEN))
    async with app.router.lifespan_context(app):
        async with get_read_db().execute("SELECT count(*) AS n FROM tasks") as cur:
            row = await cur.fetchone()
        assert row is not None and row["n"] == 0
    assert [p.name for p in tmp_path.iterdir()] == [home.name]  # no stray truncated-path file
