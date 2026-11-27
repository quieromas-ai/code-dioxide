import ast
import time
from pathlib import Path

import pytest

from src.auth import Access
from src.engine_watch import _VIOLATION_EVENT
from src.recovery import KILL_REASONS
from src.reports.events import EVENT_FIELDS, public_data
from tests.helpers import Env
from tests.reports_fixture import client_as, insert_event, insert_task

pytestmark = pytest.mark.usefixtures("reports")

NOW = time.time()
SECRET = "s3cr3t-text"
SRC = Path(__file__).resolve().parent.parent / "src"


def _written_types() -> set[str]:
    """Event types written in ``src``: the literal type argument of ``_event``/``_note``, and
    the types returned by the functions that name an event (``describe``, ``failure_event``);
    kill reasons are event types too."""
    found: set[str] = set()
    for path in SRC.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                index = {"_event": 2, "_note": 1}.get(node.func.attr)
                if index is not None and len(node.args) > index:
                    arg = node.args[index]
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        found.add(arg.value)
            if isinstance(node, ast.FunctionDef) and node.name in ("describe", "failure_event"):
                for ret in ast.walk(node):
                    if isinstance(ret, ast.Return) and isinstance(ret.value, ast.Tuple | ast.IfExp):
                        for const in ast.walk(ret.value):
                            if isinstance(const, ast.Constant) and isinstance(const.value, str):
                                found.add(const.value)
                                break
    found |= {_VIOLATION_EVENT.get(reason, reason) for reason in KILL_REASONS}
    return found


def test_every_written_event_type_has_an_entry() -> None:
    written = _written_types()
    assert {"attempt_started", "retry_scheduled", "workflow_script_failed", "stall"} <= written
    assert written - EVENT_FIELDS.keys() == set()


def test_allowlist_never_lists_text_fields() -> None:
    text = {"message", "error", "path_env", "script_dir", "moved_to", "note", "exempted",
            "branch", "worktree", "child_env"}  # fmt: skip
    assert all(not (fields & text) for fields in EVENT_FIELDS.values())


def test_public_data_of_unlisted_type_is_empty() -> None:
    assert public_data("no_such_type", {"x": SECRET}) == {}


async def plant() -> None:
    await insert_task("t1", created_at=NOW - 100)
    await insert_task("t2", created_at=NOW - 100)
    await insert_event("t1", "permission_denied", {"tool": "Bash", "message": SECRET}, NOW - 50)
    await insert_event("t1", "spawned", {"n": 1, "pid": 5, "path_env": SECRET}, NOW - 40)
    await insert_event("t1", "no_such_type", {"x": SECRET}, NOW - 30)
    await insert_event("t1", "attempt_started", {"n": 1, "mode": "fresh"}, NOW - 20)
    await insert_event("t2", "cancelled", {"from": "queued"}, NOW - 10)
    await insert_event("t2", "attempt_started", {"n": 1, "mode": "fresh"}, NOW - 40 * 86400)


async def test_event_data_allowlist_drops_text(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        response = await client.get("/api/v1/events?task=t1")
    assert SECRET not in response.text
    rows = response.json()["rows"]
    assert [(r["type"], r["data"]) for r in rows] == [
        ("permission_denied", {"tool": "Bash"}), ("spawned", {"n": 1}), ("no_such_type", {}),
        ("attempt_started", {"n": 1, "mode": "fresh"}),
    ]  # fmt: skip
    assert set(rows[0]) == {"id", "at", "task", "type", "data"} and rows[0]["task"] == "t1"
    assert rows[0]["at"].endswith("+00:00")


async def test_events_filtered_by_type_and_window(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        body = (await client.get("/api/v1/events?type=attempt_started,cancelled")).json()
        bad = await client.get("/api/v1/events?bogus=1")
    assert [(r["task"], r["type"]) for r in body["rows"]] == [
        ("t1", "attempt_started"),
        ("t2", "cancelled"),
    ]  # fmt: skip  (t2's attempt_started is outside the default 30-day window)
    assert bad.status_code == 400


async def test_events_paged_by_cursor(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.reports) as client:
        first = (await client.get("/api/v1/events?limit=2")).json()
        rest = (await client.get(f"/api/v1/events?cursor={first['next_cursor']}")).json()
        bad = await client.get("/api/v1/events?cursor=x")
    ids = [r["id"] for r in first["rows"] + rest["rows"]]
    assert len(ids) == 5 and ids == sorted(ids) and rest["next_cursor"] is None
    assert bad.status_code == 400


async def test_admin_sees_full_redacted_data(reports: Env) -> None:
    await plant()
    async with client_as(reports, Access.admin) as client:
        rows = (await client.get("/api/v1/events?task=t1&type=spawned")).json()["rows"]
    assert rows[0]["data"]["pid"] == 5
