"""`dioxide repair-usage`: recompute recorded usage from the stored streams."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from click.testing import CliRunner

from cli.main import cli
from cli.usage_repair import apply_plan, plan_repair
from src.config import acquire_home_lock
from src.database import SCHEMA, SCHEMA_VERSION
from src.usage import Usage

S1 = "11111111-0000-4000-8000-000000000000"
S2 = "22222222-0000-4000-8000-000000000000"


def result_line(cost: float, session_tokens: int, session: str) -> str:
    return json.dumps({
        "type": "result", "subtype": "success", "is_error": False, "result": "Done.",
        "session_id": session, "total_cost_usd": cost,
        "usage": {"input_tokens": 10, "output_tokens": 5},
        "modelUsage": {"claude-opus-5-5": {"inputTokens": session_tokens, "outputTokens": 0}},
    })  # fmt: skip


def make_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    return conn


def add_task(conn: sqlite3.Connection, task_id: str, output: dict[str, object] | None) -> None:
    conn.execute(
        "INSERT INTO tasks (id, status, output_json, created_at, updated_at) "
        "VALUES (?, 'completed', ?, 0, 0)",
        (task_id, json.dumps(output) if output is not None else None),
    )


def add_attempt(
    conn: sqlite3.Connection, runs: Path, task_id: str, n: int, session: str,
    lines: list[str] | None, recorded: Usage, *, ended: bool = True,
) -> Path:  # fmt: skip
    stream = runs / task_id / f"attempt-{n}.stream.jsonl"
    if lines is not None:
        stream.parent.mkdir(parents=True, exist_ok=True)
        stream.write_text("".join(line + "\n" for line in lines))
    conn.execute(
        "INSERT INTO attempts (task_id, n, session_id, stream_path, ended_at, cost_usd, tokens) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (task_id, n, session, str(stream), 1.0 if ended else None, recorded.cost_usd,
         recorded.tokens),
    )  # fmt: skip
    return stream


def rows(conn: sqlite3.Connection, task_id: str) -> list[tuple[float | None, int | None]]:
    cur = conn.execute(
        "SELECT cost_usd, tokens FROM attempts WHERE task_id = ? ORDER BY n", (task_id,)
    )
    return [(r["cost_usd"], r["tokens"]) for r in cur]


def output(conn: sqlite3.Connection, task_id: str) -> dict[str, object]:
    row = conn.execute("SELECT output_json FROM tasks WHERE id = ?", (task_id,)).fetchone()
    return json.loads(row["output_json"])


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    return make_db(tmp_path / "data" / "dioxide.db")


def seed_resumed(conn: sqlite3.Connection, runs: Path) -> None:
    """Task t1: attempts 1-2 one session (2 holds its cumulative total), 3 a fresh session.
    Recorded the old way: cumulative cost, top-level tokens."""
    add_task(conn, "t1", {"text": "x", "tokens_used": 45, "cost_usd": 1.8, "return": {}})
    add_attempt(conn, runs, "t1", 1, S1, [result_line(0.5, 1_000, S1)], Usage(0.5, 15))
    add_attempt(conn, runs, "t1", 2, S1, [result_line(0.8, 1_600, S1)], Usage(0.8, 15))
    add_attempt(conn, runs, "t1", 3, S2, [result_line(0.5, 900, S2)], Usage(0.5, 15))
    conn.commit()


def test_plan_recomputes_each_attempts_share(db: sqlite3.Connection, tmp_path: Path) -> None:
    seed_resumed(db, tmp_path / "runs")
    plan = plan_repair(db)
    assert plan.skipped == []
    apply_plan(db, plan)
    assert rows(db, "t1") == [(0.5, 1_000), (pytest.approx(0.3), 600), (0.5, 900)]
    out = output(db, "t1")
    assert out["tokens_used"] == 2_500 and out["cost_usd"] == pytest.approx(1.3)
    assert out["text"] == "x" and out["return"] == {}


def test_second_run_changes_nothing(db: sqlite3.Connection, tmp_path: Path) -> None:
    seed_resumed(db, tmp_path / "runs")
    apply_plan(db, plan_repair(db))
    again = plan_repair(db)
    assert again.fixes == [] and again.skipped == []


def test_attempt_without_result_keeps_unknown_usage(db: sqlite3.Connection, tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    add_task(db, "t2", None)
    add_attempt(db, runs, "t2", 1, S1, ['{"type": "system", "subtype": "init"}'], Usage(None, None))
    db.commit()
    assert plan_repair(db).fixes == []


def test_pruned_stream_skips_the_whole_task(db: sqlite3.Connection, tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    add_task(db, "t3", {"text": "x", "tokens_used": 30, "cost_usd": 1.3})
    add_attempt(db, runs, "t3", 1, S1, None, Usage(0.5, 15))  # stream pruned
    add_attempt(db, runs, "t3", 2, S1, [result_line(0.8, 1_600, S1)], Usage(0.8, 15))
    db.commit()
    plan = plan_repair(db)
    assert plan.fixes == [] and [s.task_id for s in plan.skipped] == ["t3"]
    assert "pruned" in plan.skipped[0].reason
    apply_plan(db, plan)
    assert rows(db, "t3") == [(0.5, 15), (0.8, 15)]


def test_open_attempt_skips_the_task(db: sqlite3.Connection, tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    add_task(db, "t4", None)
    add_attempt(db, runs, "t4", 1, S1, [result_line(0.5, 1_000, S1)], Usage(0.5, 15))
    add_attempt(
        db, runs, "t4", 2, S1, [result_line(0.8, 1_600, S1)], Usage(None, None), ended=False
    )
    db.commit()
    plan = plan_repair(db)
    assert plan.fixes == [] and [s.task_id for s in plan.skipped] == ["t4"]


# -- CLI ---------------------------------------------------------------------------------------
@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "home"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    with closing(make_db(home / "data" / "dioxide.db")) as conn:
        seed_resumed(conn, home / "data" / "runs")
    return home


def db_rows(home: Path) -> list[tuple[float | None, int | None]]:
    with closing(sqlite3.connect(home / "data" / "dioxide.db")) as conn:
        conn.row_factory = sqlite3.Row
        return rows(conn, "t1")


def test_cli_dry_run_reports_and_writes_nothing(home: Path) -> None:
    result = CliRunner().invoke(cli, ["repair-usage"])
    assert result.exit_code == 0, result.output
    assert "t1 attempt 2" in result.output and "--apply" in result.output
    assert db_rows(home) == [(0.5, 15), (0.8, 15), (0.5, 15)]
    assert not list((home / "data").glob("*.bak"))


def test_cli_apply_backs_up_then_writes(home: Path) -> None:
    result = CliRunner().invoke(cli, ["repair-usage", "--apply"])
    assert result.exit_code == 0, result.output
    assert db_rows(home) == [(0.5, 1_000), (pytest.approx(0.3), 600), (0.5, 900)]
    (backup,) = (home / "data").glob("dioxide.db.repair-*.bak")
    assert backup.stat().st_mode & 0o777 == 0o600
    with closing(sqlite3.connect(backup)) as conn:
        assert conn.execute("SELECT cost_usd FROM attempts WHERE n = 2").fetchone()[0] == 0.8


def test_cli_apply_refused_while_dioxide_runs(home: Path) -> None:
    with acquire_home_lock(home / "data" / ".lock"):
        result = CliRunner().invoke(cli, ["repair-usage", "--apply"])
    assert result.exit_code != 0 and "running" in result.output
    assert db_rows(home) == [(0.5, 15), (0.8, 15), (0.5, 15)]


def test_rows_differing_only_by_float_noise_are_left(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    # The engine sums priors in SQL, the repair in Python: the last bit may differ (0.3 vs
    # 0.8 - 0.5 = 0.30000000000000004). That is not a fix.
    runs = tmp_path / "runs"
    add_task(db, "t5", {"text": "x", "tokens_used": 1_600, "cost_usd": 0.8})
    add_attempt(db, runs, "t5", 1, S1, [result_line(0.5, 1_000, S1)], Usage(0.5, 1_000))
    add_attempt(db, runs, "t5", 2, S1, [result_line(0.8, 1_600, S1)], Usage(0.3, 600))
    db.commit()
    assert plan_repair(db).fixes == []
