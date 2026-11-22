import asyncio
import base64
import hashlib
import hmac
import io
import json
import logging
import os
import shutil
import tarfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import ClientDisconnect

from src import debug, engine_lifecycle, journal, logpager, rest_api, runlocator, serverlog
from src.config import Settings
from src.database import write_tx
from src.logpager import Cursor, Source, decode_cursor, load_key, read_page
from src.main import create_app
from src.mcp_server import ToolProvider
from src.redact import REDACTED, Redactor
from src.rest_api import build_router
from src.runlocator import RunLocator, load_view
from tests.helpers import (
    Env,
    rows,
    running_with_pid,
    submit,
    wait_for,
    wait_status,
)
from tests.stub_claude import make_launcher
from tests.test_engine_single import write_edit

pytestmark = pytest.mark.usefixtures("spawned")

PHASE0_STREAM = Path(__file__).resolve().parent / "fixtures" / "phase0_workflow.stream.jsonl"
TOKEN = "debug-test-token-" + "0" * 16
# Fake secrets, built by concatenation so no tracked file holds a token-shaped literal.
SK = "sk-" + "ant-api03-" + "x" * 32
GHP = "gh" + "p_" + "B" * 36
ENV_SECRET = 'q"uo\\te-' + "v" * 12  # JSON-escaping changes it: tests the escaped form too
PEM_BODY = ("MIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy", "KUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwE")
PEM = "\n".join(
    ["-----BEGIN RSA PRIV" + "ATE KEY-----", *PEM_BODY, "-----END RSA PRIV" + "ATE KEY-----"]
)
FORBIDDEN = (SK, "x" * 20, GHP, "B" * 20, ENV_SECRET, json.dumps(ENV_SECRET)[1:-1], "v" * 12)
FORBIDDEN += PEM_BODY


def assert_clean(text: str) -> None:
    for secret in FORBIDDEN:
        assert secret not in text


@pytest.fixture
def server_log(env: Env) -> Iterator[logging.Logger]:
    logger = serverlog.setup(env.settings.server_log_path)
    yield logger
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def router_app(env: Env, keepalive_s: float = 0.05) -> FastAPI:
    provider = ToolProvider()
    provider.attach(env.engine)
    app = FastAPI()
    app.include_router(build_router(provider, keepalive_s=keepalive_s, poll_s=0.02))
    return app


def client(app: FastAPI, headers: dict[str, str] | None = None) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 50000))
    return httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8500", headers=headers)


async def all_entries(
    env: Env, task_id: str, source: str | None, **kwargs: Any
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    cursor = None
    while True:
        page = await debug.task_logs(env.engine, task_id, source, cursor, **kwargs)
        assert "error" not in page, page
        entries += page["entries"]
        cursor = page["next_cursor"]
        if not page["entries"]:
            return entries


def cursor_of(env: Env, raw: str, task_id: str | None) -> Cursor:
    key = load_key(env.settings.cursor_key_path)
    return decode_cursor(raw, key, "server" if task_id is None else f"task:{task_id}")


def signed(env: Env, doc: Any, key: bytes | None = None) -> str:
    """A cursor signed with ``key`` (default: the install's real key) over any payload."""
    return sign_raw(env, json.dumps(doc).encode(), key)


def sign_raw(env: Env, payload: bytes, key: bytes | None = None) -> str:
    mac = hmac.new(key or load_key(env.settings.cursor_key_path), payload, hashlib.sha256)
    b64 = base64.urlsafe_b64encode
    return f"{b64(payload).decode()}.{b64(mac.digest()).decode()}"


async def drive(
    app: FastAPI,
    path: str,
    query: bytes,
    spec: str,
    stop: Callable[[bytes], bool],
    *,
    send_error: type[Exception] | None = None,
) -> list[dict[str, Any]]:
    """Raw ASGI GET. Once ``stop(body so far)`` holds, the client goes away: ``receive`` reports
    a disconnect, and with ``send_error`` every later ``send`` raises it (ASGI 2.4 servers)."""
    requested, gone = False, asyncio.Event()
    sent: list[dict[str, Any]] = []
    body = bytearray()

    async def receive() -> dict[str, Any]:
        nonlocal requested
        if not requested:
            requested = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await gone.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if gone.is_set() and send_error is not None:
            raise send_error("client gone")
        sent.append(message)
        body.extend(message.get("body", b""))
        if stop(bytes(body)):
            gone.set()

    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": spec}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": query, "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 50000), "server": ("test", 80), "root_path": "",
    }  # fmt: skip
    try:
        await asyncio.wait_for(app(scope, receive, send), 5.0)
    except TimeoutError:
        raise
    except Exception as exc:  # the app may surface the client error; nothing else
        if send_error is None or not isinstance(exc, send_error | ClientDisconnect):
            raise
    assert gone.is_set()
    return sent


def sse_events(body: str) -> list[tuple[str | None, Any]]:
    out: list[tuple[str | None, Any]] = []
    for block in body.split("\n\n"):
        event, data = None, None
        for line in block.split("\n"):
            if line.startswith("event: "):
                event = line[len("event: ") :]
            elif line.startswith("data: "):
                data = json.loads(line[len("data: ") :])
        if data is not None:
            out.append((event, data))
    return out


async def finished_single(env: Env) -> str:
    sub = await submit(env)
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed", "failed")
    return str(sub["task_id"])


def live_agent_file(env: Env, prompt: str) -> Path | None:
    for path in (env.tmp / "claude-home").glob("projects/*/*/subagents/workflows/*/agent-*.jsonl"):
        if prompt in path.read_text():
            return path
    return None


async def run_planted(
    env: Env, monkeypatch: pytest.MonkeyPatch, logger: logging.Logger
) -> tuple[str, Path]:
    """A lifecycle task with secrets in its stream, stderr, brief, a step transcript, the server
    log and a redact env file; cancelled mid-step so the run store holds the copies."""
    secrets_file = env.tmp / "secrets.env"
    secrets_file.write_text(f"MY_VALUE='{ENV_SECRET}'\n")
    env.settings.redact_env_files = [secrets_file]
    env.engine.redactor = Redactor.from_settings(env.settings)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    monkeypatch.setenv("STUB_CLAUDE_EXTRA", f"key {SK} and {ENV_SECRET} and {GHP}\n{PEM}")
    sub = await submit(env, f"Build it with {SK} and {ENV_SECRET}", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()

    async def implement() -> Path | None:
        return live_agent_file(env, "Implement the plan.")

    live: Path = await wait_for(implement)
    with live.open("a") as fh:
        fh.write(json.dumps({"type": "assistant", "message": f"found {GHP} {ENV_SECRET}"}) + "\n")
    logger.error(f"token leaked into an error: {SK}", extra={"task_id": task_id})
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")
    copy = env.engine.runstore.step_copy_path(task_id, "implement")

    async def copied() -> bool:
        return copy.is_file() and GHP in copy.read_text()

    await wait_for(copied)
    return task_id, live


# -- the brief's tests ------------------------------------------------------------------------
async def test_logs_cursor_line_boundaries_while_appending(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    sub = await submit(env)
    task_id = sub["task_id"]
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    with stderr.open("ab") as fh:  # the running writer has not finished its last line
        fh.write(b"one\ntwo\nthr")
    page = await debug.task_logs(env.engine, task_id, "stderr-1")
    assert [e["line"] for e in page["entries"]] == ["one", "two"] and page["done"] is False
    assert cursor_of(env, page["next_cursor"], task_id).offsets["stderr-1"] == 8
    with stderr.open("ab") as fh:
        fh.write("ee\nfour ü\n".encode())
    page = await debug.task_logs(env.engine, task_id, "stderr-1", page["next_cursor"])
    assert [e["line"] for e in page["entries"]] == ["three", "four ü"]
    end = cursor_of(env, page["next_cursor"], task_id).offsets["stderr-1"]
    assert end == stderr.stat().st_size and page["done"] is False
    assert {"engine", "stderr-1", "attempt-1"} <= set(page["available_sources"])
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")


async def test_terminal_task_serves_final_partial_line(env: Env) -> None:
    task_id = await finished_single(env)
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    stderr.write_bytes(b"one\nkilled mid-li")  # the child died before its last newline
    page = await debug.task_logs(env.engine, task_id, "stderr-1")
    assert [e["line"] for e in page["entries"]] == ["one", "killed mid-li"] and page["done"]
    again = await debug.task_logs(env.engine, task_id, "stderr-1", page["next_cursor"])
    assert again["entries"] == [] and again["done"] is True  # end of file: not a restart


async def test_logs_full_cap_and_next_cursor(env: Env) -> None:
    task_id = await finished_single(env)
    env.settings.log_page_chars = 1000
    env.settings.log_full_page_bytes = 5000
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    lines = [f"{i:03d} " + "é" * 96 for i in range(100)]  # 100 chars, 196 UTF-8 bytes each
    stderr.write_text("\n".join(lines) + "\n" + "w" * 8000 + "\n")
    page = await debug.task_logs(env.engine, task_id, "stderr-1")
    assert sum(len(e["line"]) for e in page["entries"]) <= 1000
    assert len(page["entries"]) == 10 and page["done"] is False
    served = await all_entries(env, task_id, "stderr-1", full=True)
    assert [e["line"] for e in served] == [*lines, "w" * 8000]  # each line once, in order
    page = await debug.task_logs(env.engine, task_id, "stderr-1", full=True)
    size = sum(len(e["line"].encode()) for e in page["entries"])
    assert size <= 5000 and len(page["entries"]) == 25
    page = await debug.task_logs(env.engine, task_id, "stderr-1", full=True, limit=3)
    assert len(page["entries"]) == 3 and page["next_cursor"]


async def test_logs_redacted_incl_escaped(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    secrets_file = env.tmp / "secrets.env"
    secrets_file.write_text(f"MY_VALUE='{ENV_SECRET}'\n")
    env.settings.redact_env_files = [secrets_file]
    env.engine.redactor = Redactor.from_settings(env.settings)
    monkeypatch.setenv("STUB_CLAUDE_EXTRA", f"use {ENV_SECRET} and {SK}")
    task_id = await finished_single(env)
    stream, _ = env.engine.runstore.attempt_paths(task_id, 1)
    assert json.dumps(ENV_SECRET)[1:-1] in stream.read_text()  # escaped in the raw file
    for source in ("attempt-1", "stderr-1"):
        entries = await all_entries(env, task_id, source)
        text = "\n".join(e["line"] for e in entries)
        assert_clean(text)
        assert REDACTED in text


async def test_running_step_logs_live(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()

    async def implement() -> Path | None:
        return live_agent_file(env, "Implement the plan.")

    live: Path = await wait_for(implement)
    page = await debug.task_logs(env.engine, task_id, "step:implement")
    assert "Implement the plan." in page["entries"][0]["line"] and page["done"] is False
    assert {"journal", "step:plan", "step:implement"} <= set(page["available_sources"])
    assert not env.engine.runstore.step_copy_path(task_id, "implement").exists()
    with live.open("a") as fh:
        fh.write(json.dumps({"type": "assistant", "message": "still working"}) + "\n")
    page = await debug.task_logs(env.engine, task_id, "step:implement", page["next_cursor"])
    assert [json.loads(e["line"])["message"] for e in page["entries"]] == ["still working"]
    journal_page = await debug.task_logs(env.engine, task_id, "journal")
    assert any('"started"' in e["line"] for e in journal_page["entries"])
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")


async def test_running_step_timing_agrees_on_status_and_debug(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(engine_lifecycle, "MIRROR_INTERVAL_S", 0.1)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()

    async def mirrored() -> dict[str, Any] | None:
        st = await env.engine.status(task_id)
        return next((s for s in st["steps"] if s["label"] == "implement"), None)

    status_step = await wait_for(mirrored)
    assert status_step["status"] == "running"
    # Only the live transcript exists for the running step.
    assert not env.engine.runstore.step_copy_path(task_id, "implement").exists()
    view = await debug.task_debug(env.engine, task_id)
    debug_step = next(s for s in view["steps"] if s["label"] == "implement")
    assert status_step["timing"] == debug_step["timing"] == "measured"
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")


async def test_unfinished_step_of_an_ended_task_is_interrupted_on_status_and_debug(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One step-state rule: a step still running in the journal and the mirror while the task
    is not running is ``interrupted`` on both the status and the debug view."""
    monkeypatch.setattr(engine_lifecycle, "MIRROR_INTERVAL_S", 0.1)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()

    async def mirrored() -> dict[str, Any] | None:
        st = await env.engine.status(task_id)
        return next((s for s in st["steps"] if s["label"] == "implement"), None)

    assert (await wait_for(mirrored))["status"] == "running"
    view = await debug.task_debug(env.engine, task_id)
    assert next(s for s in view["steps"] if s["label"] == "implement")["status"] == "running"
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")
    st = await env.engine.status(task_id)
    view = await debug.task_debug(env.engine, task_id)
    served = {s["label"]: s["status"] for s in st["steps"]}
    debugged = {s["label"]: s["status"] for s in view["steps"]}
    assert served["implement"] == debugged["implement"] == "interrupted"
    assert served == debugged


async def test_debug_shape_call_launch_agent_id(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed", "failed")
    view = await debug.task_debug(env.engine, task_id)
    assert set(view) == {
        "task_id", "status", "kind", "agent", "session_id", "run_id", "attempts", "steps",
        "call", "launch", "timeline", "bundle_url", "history",
    }  # fmt: skip
    assert view["history"] == []
    assert view["kind"] == "lifecycle" and view["agent"] == "lead"
    assert view["run_id"].startswith("wf_") and view["session_id"]
    (attempt,) = view["attempts"]
    assert set(attempt) == {
        "n", "argv", "started", "ended", "exit_code", "outcome", "reason", "detail", "cost_usd",
    }  # fmt: skip
    assert attempt["n"] == 1 and attempt["exit_code"] == 0 and attempt["argv"][1] == "-p"
    assert [s["label"] for s in view["steps"]] == ["plan", "implement"]
    for step in view["steps"]:
        assert step["agent_id"].startswith("a") and step["status"] == "completed"
        assert step["cached"] is False and step["started_count"] == 1 and step["git"] is None
    call = view["call"]
    assert call["passed"] == call["expected"] and call["mismatch"] is False
    assert call["expected"]["args"]["task_id"] == task_id
    launch = view["launch"]
    assert launch["path_env"] and launch["claude_version"] == "2.1.328 (Claude Code stub)"
    assert set(launch["agents_json"]) >= {"lead", "implementer"}
    assert launch["mcp_config"] == {"mcpServers": {}} and launch["settings"] is None
    types = [e["type"] for e in view["timeline"]]
    milestones = ["submitted", "attempt_started", "spawned", "attempt_finished"]
    assert [t for t in types if t in milestones] == milestones
    assert view["bundle_url"] == f"/api/v1/tasks/{task_id}/bundle.tar.gz"
    assert (await debug.task_debug(env.engine, "nope"))["status"] == "not_found"


async def test_sse_until_done(env: Env) -> None:
    sub = await submit(env)
    task_id = sub["task_id"]
    await env.engine.start()
    async with client(router_app(env)) as http:
        response = await http.get(f"/api/v1/tasks/{task_id}/stream")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = sse_events(response.text)
    assert events[-1][0] == "done" and all(name is None for name, _ in events[:-1])
    sources = {data["source"] for _, data in events[:-1]}
    assert sources == {"engine", "attempt-1"}
    lines = [data["line"] for _, data in events[:-1]]
    assert any("attempt_finished" in line for line in lines)
    assert (await env.engine.status(task_id))["status"] == "completed"


async def test_debug_lists_history_runs_names_only(env: Env) -> None:
    """A run a fresh start replaced is discoverable from the debug view (names, no content)."""
    task_id = await finished_single(env)
    old = env.engine.runstore.task_dir(task_id) / "history" / "wf_old-1"
    (old / "steps").mkdir(parents=True)
    (old / "journal.jsonl").write_text('{"secret": "' + SK + '"}\n')
    (old / "steps" / "plan.jsonl").write_text("{}\n")
    (old / "steps.json").write_text("[]")
    (old.parent / "wf_old-2").mkdir()
    view = await debug.task_debug(env.engine, task_id)
    assert view["history"] == [
        {"run": "wf_old-1", "entries": ["journal.jsonl", "steps.json", "steps/plan.jsonl"]},
        {"run": "wf_old-2", "entries": []},
    ]
    assert_clean(json.dumps(view))


async def test_log_and_debug_reads_keep_the_loop_free(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stream reads for the run lookup (logs, debug, SSE) never block the event loop."""
    task_id = await finished_single(env)
    release = threading.Event()
    blocked = threading.Event()
    real = runlocator.StreamWatcher

    class SlowWatcher(real):  # type: ignore[misc, valid-type]
        def poll(self) -> list[Any]:
            blocked.set()
            assert release.wait(10)
            return list(super().poll())

    monkeypatch.setattr(runlocator, "StreamWatcher", SlowWatcher)
    calls = {
        "logs": lambda: debug.task_logs(env.engine, task_id),
        "debug": lambda: debug.task_debug(env.engine, task_id),
        "sse": lambda: debug.LogFollower(env.engine, task_id).page(None, None, 10),
    }
    for name, call in calls.items():
        blocked.clear()
        release.clear()
        pending = asyncio.ensure_future(call())
        ticks = 0
        while not blocked.is_set():
            await asyncio.sleep(0.01)
        for _ in range(5):  # the loop keeps running while the read is stuck
            await asyncio.sleep(0.01)
            ticks += 1
        assert ticks == 5 and not pending.done(), name
        release.set()
        result = await pending
        assert "error" not in result, (name, result)


def sse_ids(body: str) -> list[tuple[str | None, str | None]]:
    """(id, data line) per event block: a page's last entry carries the page's next cursor."""
    out: list[tuple[str | None, str | None]] = []
    for block in body.split("\n\n"):
        lines = block.split("\n")
        ids = [line[len("id: ") :] for line in lines if line.startswith("id: ")]
        data = [line[len("data: ") :] for line in lines if line.startswith("data: ")]
        if data:
            out.append((ids[0] if ids else None, data[0]))
    return out


async def test_sse_ids_resume_after_drop(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    """A client that lost the stream reconnects with the last ``id:``: nothing repeats, nothing
    is lost."""
    monkeypatch.setattr(rest_api, "STREAM_LIMIT", 2)  # several pages, several ids
    sub = await submit(env)
    task_id = sub["task_id"]
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed")
    url = f"/api/v1/tasks/{task_id}/stream"
    async with client(router_app(env)) as http:
        full = sse_ids((await http.get(url, params={"source": "engine"})).text)
        marked = [n for n, (i, _) in enumerate(full) if i is not None]
        assert len(marked) >= 2 and marked[0] == 1  # the last entry of each 2-entry page
        assert full[-1][0] is None  # the done event
        for i, _ in full:
            if i is not None:
                cursor_of(env, i, task_id)  # a signed task cursor
        cut = marked[0]
        resumed = await http.get(url, params={"source": "engine", "cursor": full[cut][0]})
    assert [d for _, d in sse_ids(resumed.text)] == [d for _, d in full[cut + 1 :]]


async def test_bundle_redacted_all_sources(
    env: Env, monkeypatch: pytest.MonkeyPatch, server_log: logging.Logger
) -> None:
    task_id, _ = await run_planted(env, monkeypatch, server_log)
    async with client(router_app(env)) as http:
        response = await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")
    assert response.status_code == 200
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tar:
        members = {m.name: tar.extractfile(m).read().decode() for m in tar if m.isfile()}  # type: ignore[union-attr]
    prefix = f"{task_id}/"
    run = {name[len(prefix) + 4 :] for name in members if name.startswith(prefix + "run/")}
    assert {"attempt-1.stream.jsonl", "attempt-1.stderr", "inputs/brief.md", "journal.jsonl",
            "events.jsonl", "steps/implement.jsonl", "steps/plan.jsonl"} <= run  # fmt: skip
    for name, text in members.items():
        assert_clean(text)
    assert REDACTED in members[prefix + "run/steps/implement.jsonl"]
    assert REDACTED in members[prefix + "run/inputs/brief.md"]
    assert "token leaked" in members[prefix + "server.log"]


async def test_server_logs_source_shows_spawn_failure(
    env: Env, tmp_path: Path, server_log: logging.Logger
) -> None:
    env.settings.claude_bin = str(tmp_path / "no-such-claude")
    sub = await submit(env)
    await env.engine.start()
    st = await wait_status(env.engine, sub["task_id"], "failed", "completed")
    assert st["status"] == "failed"
    entries = await all_entries(env, sub["task_id"], "server")
    failures = [json.loads(e["line"]) for e in entries if "spawn failed" in e["line"]]
    assert failures and failures[0]["task_id"] == sub["task_id"]
    merged = await all_entries(env, sub["task_id"], None)
    assert any(e["source"] == "server" and "spawn failed" in e["line"] for e in merged)
    async with client(router_app(env)) as http:
        response = await http.get("/api/v1/server/logs")
    assert response.status_code == 200
    assert any("spawn failed" in e["line"] for e in response.json()["entries"])


@pytest.mark.parametrize("value", ["*SECRETALIASxyz", "!custom_s3cr3t x"])
async def test_reload_failure_never_serves_yaml_alias_or_tag_names(
    env: Env, server_log: logging.Logger, value: str
) -> None:
    """An unquoted credential that YAML reads as an alias or a tag reaches neither the event, the
    server log nor the served server logs: the reload error keeps the error class and position."""
    await env.engine.start()
    write_edit(
        env.settings.agents_dir / "implementer.md",
        f"---\ndescription: implementer\ntoken: {value}\n---\nYou.\n",
    )
    sub = await submit(env, "one")
    st = await wait_status(env.engine, sub["task_id"], "completed", "failed")
    assert st["status"] == "completed"
    (event,) = await rows("SELECT data_json FROM events WHERE type = 'agents_reload_failed'")
    error = json.loads(event["data_json"])["error"]
    assert "implementer.md" in error and "(line 2, column 8)" in error
    served = await debug.server_logs(env.engine)
    failures = [e["line"] for e in served["entries"] if "agents reload failed" in e["line"]]
    assert failures
    texts = [event["data_json"], env.settings.server_log_path.read_text(), json.dumps(served)]
    for text in texts:
        assert "SECRETALIASxyz" not in text and "s3cr3t" not in text


async def test_logs_survive_claude_transcript_deletion(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed", "failed")

    async def copied() -> bool:
        return (env.engine.runstore.task_dir(task_id) / "journal.jsonl").is_file()

    await wait_for(copied)
    shutil.rmtree(env.tmp / "claude-home")
    journal_lines = [e["line"] for e in await all_entries(env, task_id, "journal")]
    assert any('"label":"implement"' in line for line in journal_lines)
    assert await all_entries(env, task_id, "attempt-1")
    assert "Implement the plan." in (await all_entries(env, task_id, "step:implement"))[0]["line"]
    view = await debug.task_debug(env.engine, task_id)
    assert [s["label"] for s in view["steps"]] == ["plan", "implement"]


# -- additional tests -----------------------------------------------------------------------
async def test_page_boundary_inside_pem_block(env: Env) -> None:
    task_id = await finished_single(env)
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    stderr.write_text(f"before\n{PEM}\nafter\n")
    served: list[str] = []
    cursor = None
    while True:  # 2 lines per page: the boundary falls inside the block
        page = await debug.task_logs(env.engine, task_id, "stderr-1", cursor, limit=2)
        served += [e["line"] for e in page["entries"]]
        cursor = page["next_cursor"]
        if page["done"]:
            break
    assert served == ["before", REDACTED, REDACTED, REDACTED, REDACTED, "after"]
    assert cursor_of(env, cursor, task_id).pem.get("stderr-1", False) is False  # compact: omitted


async def test_bad_cursor_is_clean_error(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    task_id = await finished_single(env)
    other = await submit(env, "Another one")
    good = (await debug.task_logs(env.engine, task_id, "stderr-1"))["next_cursor"]
    body, sig = good.split(".")
    tampered = base64.urlsafe_b64encode(
        base64.urlsafe_b64decode(body).replace(b'"o":{', b'"o":{"x":1,')
    ).decode()

    def b64(doc: Any) -> str:
        return base64.urlsafe_b64encode(json.dumps(doc).encode()).decode()

    deep = "[" * 20_000 + "]" * 20_000  # past json's recursion limit
    scope = f"task:{task_id}"
    bad = [
        "%%%not-base64",
        "a.b.c",
        b64({"v": 1, "t": scope, "o": {}}),  # the old unsigned format
        f"{tampered}.{sig}",  # payload changed, signature kept
        signed(env, {"v": 1, "t": scope, "o": {}}, key=b"k" * 32),  # someone else's key
        signed(env, {"v": True, "t": scope, "o": {}}),
        signed(env, {"v": 1.0, "t": scope, "o": {}}),
        signed(env, {"v": 2, "t": scope, "o": {}}),
        signed(env, {"v": 1, "t": scope, "o": {"engine": -1}}),
        signed(env, {"v": 1, "t": scope, "o": {"engine": "3"}}),
        signed(env, {"v": 1, "t": scope, "o": {}, "p": {"engine": "yes"}}),
        signed(env, {"v": 1, "t": scope, "o": {}, "i": {"engine": True}}),
        signed(env, {"v": 1, "t": f"task:{other['task_id']}", "o": {}}),  # another task's
        "x" * 20_000,  # over the length cap
    ]
    for cursor in bad:
        assert await debug.task_logs(env.engine, task_id, None, cursor) == {"error": "bad cursor"}
        assert await debug.server_logs(env.engine, cursor) == {"error": "bad cursor"}
    server_cursor = signed(env, {"v": 1, "t": "server", "o": {}})  # valid, but not this task's
    assert await debug.task_logs(env.engine, task_id, None, server_cursor) == {
        "error": "bad cursor"
    }
    assert "entries" in await debug.server_logs(env.engine, server_cursor)
    assert await debug.server_logs(env.engine, good) == {"error": "bad cursor"}
    # deeply nested JSON under a valid signature (as if the key leaked): RecursionError caught
    payload = '{"v":1,"t":"' + scope + '","o":{},"p":' + deep + "}"
    nested = sign_raw(env, payload.encode())
    assert await debug.task_logs(env.engine, task_id, None, nested) == {"error": "bad cursor"}
    monkeypatch.setattr(logpager, "MAX_CURSOR_CHARS", 10**6)  # reach json.loads itself
    assert await debug.task_logs(env.engine, task_id, None, nested) == {"error": "bad cursor"}
    monkeypatch.undo()
    async with client(router_app(env)) as http:
        response = await http.get(f"/api/v1/tasks/{task_id}/logs", params={"cursor": bad[0]})
        assert response.status_code == 400 and response.json() == {"error": "bad cursor"}
        response = await http.get(f"/api/v1/tasks/{task_id}/stream", params={"cursor": good})
    assert response.status_code == 200  # its own cursor is accepted by the stream


async def test_forged_cursors_never_leak(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    secrets_file = env.tmp / "secrets.env"
    secrets_file.write_text(f"MY_VALUE='{ENV_SECRET}'\n")
    env.settings.redact_env_files = [secrets_file]
    env.engine.redactor = Redactor.from_settings(env.settings)
    task_id = await finished_single(env)
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    text = f"key {SK} here\nval {ENV_SECRET} end\nbefore\n{PEM}\nafter\n"
    stderr.write_text(text)
    raw = text.encode()
    mid_token = raw.index(SK.encode()) + 1
    mid_value = raw.index(ENV_SECRET.encode()) + 1
    pem_body = raw.index(PEM_BODY[0].encode())  # a line boundary inside the block
    scope = f"task:{task_id}"
    forgeries = [{"stderr-1": mid_token}, {"stderr-1": mid_value}, {"stderr-1": pem_body}]
    for offsets in forgeries:  # without the key: rejected outright
        doc = {"v": 1, "t": scope, "o": offsets, "p": {"stderr-1": False}}
        forged = signed(env, doc, key=b"\x00" * 32)
        assert await debug.task_logs(env.engine, task_id, "stderr-1", forged) == {
            "error": "bad cursor"
        }
        unsigned = base64.urlsafe_b64encode(json.dumps(doc).encode()).decode()
        assert (await debug.task_logs(env.engine, task_id, "stderr-1", unsigned))["error"]
    for offsets in forgeries[:2]:  # defence in depth: a mid-line offset restarts at 0
        doc = {"v": 1, "t": scope, "o": offsets, "p": {"stderr-1": False}}
        page = await debug.task_logs(env.engine, task_id, "stderr-1", signed(env, doc))
        served = "\n".join(e["line"] for e in page["entries"])
        assert page["entries"][0]["line"].startswith("key ") and REDACTED in served
        assert_clean(served)
        assert "k-ant" not in served and "uo\\te" not in served


async def test_merged_timeline_orders_and_tags_sources(
    env: Env, server_log: logging.Logger
) -> None:
    task_id = await finished_single(env)
    server_log.info("note for the task", extra={"task_id": task_id})
    server_log.info("note for another task", extra={"task_id": "other"})
    page = await debug.task_logs(env.engine, task_id)
    entries = page["entries"]
    assert {e["source"] for e in entries} == {"engine", "attempt-1", "server"}
    stamps = [e["at"] for e in entries]
    assert stamps == sorted(stamps) and all(stamps)
    assert not any("another task" in e["line"] for e in entries)
    stream_lines = [json.loads(e["line"]) for e in entries if e["source"] == "attempt-1"]
    assert stream_lines and all("timestamp" in line for line in stream_lines)  # untimed: skipped
    assert "stderr-1" in page["available_sources"] and page["done"] is True


async def test_long_line_truncated_marker_and_cursor_advances(env: Env) -> None:
    task_id = await finished_single(env)
    env.settings.log_page_chars = 1000
    _, stderr = env.engine.runstore.attempt_paths(task_id, 1)
    stderr.write_text("y" * 5000 + "\nnext\n")
    page = await debug.task_logs(env.engine, task_id, "stderr-1")
    (entry,) = page["entries"]
    assert len(entry["line"]) == 1000 and entry["line"].endswith("…[truncated 4023 chars]")
    page = await debug.task_logs(env.engine, task_id, "stderr-1", page["next_cursor"])
    assert [e["line"] for e in page["entries"]] == ["next"]


async def test_partial_last_line_not_served(tmp_path: Path) -> None:
    path = tmp_path / "log"
    path.write_bytes(b"whole\npart")
    source = Source("s", lambda: path.open("rb"))
    page = read_page([source], Cursor(), Redactor(), cap=1000, limit=10, full=False)
    assert [e["line"] for e in page.entries] == ["whole"] and page.exhausted
    assert page.cursor.offsets["s"] == len(b"whole\n")
    again = read_page([source], page.cursor, Redactor(), cap=1000, limit=10, full=False)
    assert again.entries == [] and again.cursor.offsets["s"] == len(b"whole\n")
    final = Source("s", lambda: path.open("rb"), final=True)  # the writer is gone
    last = read_page([final], page.cursor, Redactor(), cap=1000, limit=10, full=False)
    assert [e["line"] for e in last.entries] == ["part"] and last.cursor.offsets["s"] == 10


async def test_debug_view_redacted(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    secrets_file = env.tmp / "secrets.env"
    secrets_file.write_text(f"MY_VALUE='{ENV_SECRET}'\n")
    mcp = env.tmp / "mcp.json"
    mcp.write_text(json.dumps({"mcpServers": {"x": {"command": "y", "env": {"KEY": GHP}}}}))
    env.settings.redact_env_files = [secrets_file]
    env.settings.mcp_config_path = mcp
    env.settings.extra_settings = {"env": {"SOME_VAR": SK}}
    env.settings.extra_path = [f"/opt/{ENV_SECRET}/bin"]
    env.settings.parent_model = f"model-{SK}"  # lands in argv
    env.engine.redactor = Redactor.from_settings(env.settings)
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, f"Build with {SK}", agent="team-leader")
    await env.engine.start()
    await wait_status(env.engine, sub["task_id"], "completed", "failed")
    view = await debug.task_debug(env.engine, sub["task_id"])
    text = json.dumps(view, ensure_ascii=False)
    assert_clean(text)
    assert view["launch"]["mcp_config"]["mcpServers"]["x"]["env"]["KEY"] == REDACTED
    assert "--model" in view["attempts"][0]["argv"] and view["launch"]["path_env"]


async def test_rest_routes_require_token(tmp_path: Path) -> None:
    app = create_app(Settings(home=tmp_path, auth_token=TOKEN))
    routes = [
        ("GET", "/api/v1/tasks"),
        ("GET", "/api/v1/tasks/nope"),
        ("POST", "/api/v1/tasks/nope/cancel"),
        ("POST", "/api/v1/tasks/nope/answer"),
        ("GET", "/api/v1/tasks/nope/logs"),
        ("GET", "/api/v1/tasks/nope/debug"),
        ("GET", "/api/v1/tasks/nope/stream"),
        ("GET", "/api/v1/tasks/nope/bundle.tar.gz"),
        ("GET", "/api/v1/server/logs"),
    ]
    async with app.router.lifespan_context(app):
        async with client(app) as anonymous:
            for method, path in routes:
                denied = await anonymous.request(method, path, json={"answer": "x"})
                assert denied.status_code == 401, path
        async with client(app, {"Authorization": f"Bearer {TOKEN}"}) as http:
            listed = await http.get("/api/v1/tasks")
            assert listed.status_code == 200 and listed.json()["tasks"] == []
            logs = await http.get("/api/v1/server/logs")
            assert logs.status_code == 200 and "next_cursor" in logs.json()
            for method, path in routes[1:-1]:
                response = await http.request(method, path, json={"answer": "x"})
                assert response.status_code == 404, path
                assert response.json() == {"error": "task nope not found"}, path


async def test_sse_stops_on_client_disconnect(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "hang")
    sub = await submit(env)
    task_id = sub["task_id"]
    await env.engine.start()
    await running_with_pid(env.engine, task_id)
    app = router_app(env, keepalive_s=0.05)
    path = f"/api/v1/tasks/{task_id}/stream"
    for spec in ("2.3", "2.4"):  # Starlette's disconnect listener, and the 2.4 path without it
        sent = await drive(app, path, b"source=engine", spec, lambda body: b": keep-alive" in body)
        body = b"".join(m.get("body", b"") for m in sent)
        assert sent[0]["status"] == 200 and b"data: " in body and b": keep-alive\n\n" in body
        assert (await env.engine.status(task_id))["status"] == "running"
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")


async def test_bundle_contains_no_secret_and_expected_members(
    env: Env, monkeypatch: pytest.MonkeyPatch, server_log: logging.Logger
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_EXTRA", f"oops {SK}\n{PEM}")
    task_id = await finished_single(env)
    server_log.warning(f"leak {GHP}", extra={"task_id": task_id})
    server_log.warning("unrelated", extra={"task_id": "other"})
    async with client(router_app(env)) as http:
        response = await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/gzip"
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tar:
        members = {m.name: tar.extractfile(m).read().decode() for m in tar if m.isfile()}  # type: ignore[union-attr]
    p = f"{task_id}/"
    assert set(members) == {
        p + "task.json", p + "server.log", p + "run/attempt-1.stream.jsonl",
        p + "run/attempt-1.stderr", p + "run/events.jsonl", p + "run/inputs/brief.md",
        p + "run/procs.json", p + "run/kept.json",
    }  # fmt: skip
    for text in members.values():
        assert_clean(text)
    rows = json.loads(members[p + "task.json"])
    assert rows["task"]["id"] == task_id and rows["attempts"][0]["n"] == 1 and rows["events"]
    assert "leak" in members[p + "server.log"] and "unrelated" not in members[p + "server.log"]
    assert not list(env.settings.data_dir.glob("dioxide-bundle-*"))  # temp files removed


async def test_config_start_log_has_no_tokens(tmp_path: Path) -> None:
    settings = Settings(
        home=tmp_path, auth_token=TOKEN, redact_env_files=[tmp_path / "missing.env"]
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        pass
    log = settings.server_log_path.read_text()
    assert TOKEN not in log
    records = [json.loads(line) for line in log.splitlines()]
    (config,) = [r["config"] for r in records if r["msg"] == "config at start"]
    assert config["port"] == 8500 and "auth_token" not in config
    assert any("missing.env" in r["msg"] and r["level"] == "WARNING" for r in records)


# -- every served surface, one planted run --------------------------------------------------
SURFACES = [
    "logs:engine", "logs:server", "logs:attempt-1", "logs:stderr-1", "logs:journal",
    "logs:step:plan", "logs:step:implement", "logs:inputs", "logs:merged", "debug",
    "sse:merged", "sse:stderr-1", "bundle", "server-logs",
]  # fmt: skip


@pytest.mark.parametrize("surface", SURFACES)
async def test_no_secret_on_any_served_surface(
    env: Env, monkeypatch: pytest.MonkeyPatch, server_log: logging.Logger, surface: str
) -> None:
    task_id, _ = await run_planted(env, monkeypatch, server_log)
    kind, _, source = surface.partition(":")
    async with client(router_app(env)) as http:
        if kind == "logs":
            entries = await all_entries(env, task_id, None if source == "merged" else source)
            text = "\n".join(e["line"] for e in entries)
            assert entries
        elif kind == "debug":
            text = (await http.get(f"/api/v1/tasks/{task_id}/debug")).text
        elif kind == "sse":
            params = {} if source == "merged" else {"source": source}
            text = (await http.get(f"/api/v1/tasks/{task_id}/stream", params=params)).text
        elif kind == "bundle":
            raw = (await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")).content
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
                text = "\n".join(tar.extractfile(m).read().decode() for m in tar if m.isfile())  # type: ignore[union-attr]
        else:
            text = (await http.get("/api/v1/server/logs", params={"full": "true"})).text
    assert text
    assert_clean(text)


# -- fix round 1 ----------------------------------------------------------------------------
async def test_merged_timeline_includes_steps(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed", "failed")
    entries = await all_entries(env, task_id, None)
    sources = {e["source"] for e in entries}
    assert {"engine", "attempt-1", "step:plan", "step:implement"} <= sources
    stamps = [e["at"] for e in entries]
    assert stamps == sorted(stamps)


async def test_unknown_step_or_attempt_is_error(env: Env) -> None:
    task_id = await finished_single(env)
    for source in ("attempt-2", "stderr-9", "step:nope", "attempt-0", "bogus"):
        page = await debug.task_logs(env.engine, task_id, source)
        assert page["error"] == f"unknown source {source!r}", source
        assert "attempt-1" in page["available_sources"]


async def test_phase0_stream_fixture_redacted_on_every_surface(
    env: Env, server_log: logging.Logger
) -> None:
    """A real (sanitised) Phase-0 stream with secrets planted into its own line shapes."""
    secrets_file = env.tmp / "secrets.env"
    secrets_file.write_text(f"MY_VALUE='{ENV_SECRET}'\n")
    env.settings.redact_env_files = [secrets_file]
    env.engine.redactor = Redactor.from_settings(env.settings)
    task_id = await finished_single(env)
    planted = []
    shapes: dict[str, int] = dict.fromkeys(
        ("tool_result", "text", "tool_use", "wire", "summary"), 0
    )
    for raw in PHASE0_STREAM.read_text().splitlines():
        line = json.loads(raw)
        for item in (line.get("message") or {}).get("content") or []:
            if item.get("type") == "tool_result":
                item["content"] += f"\nerror: auth failed for {SK}"
                shapes["tool_result"] += 1
            if item.get("type") == "text":
                item["text"] += f" (env {ENV_SECRET})"
                shapes["text"] += 1
            if item.get("type") == "tool_use":
                item["input"]["args"]["note"] = f"pem {PEM}"
                shapes["tool_use"] += 1
        for wire in (line.get("wire_tool_inputs") or {}).values():
            args = json.loads(wire["args"])  # JSON in a string, as Claude Code sends it
            wire["args"] = json.dumps({**args, "note": f"use {GHP}", "nested": {"k": SK}})
            shapes["wire"] += 1
        if line.get("subtype") == "task_notification":
            line["summary"] += f" {GHP}"
            shapes["summary"] += 1
        planted.append(json.dumps(line))
    assert all(shapes.values()), shapes  # every shape planted: the test cannot pass vacuously
    stream, _ = env.engine.runstore.attempt_paths(task_id, 1)
    stream.write_text("\n".join(planted) + "\n")
    raw_text = stream.read_text()
    assert SK in raw_text and GHP in raw_text and json.dumps(ENV_SECRET)[1:-1] in raw_text
    texts = [
        "\n".join(e["line"] for e in await all_entries(env, task_id, "attempt-1")),
        "\n".join(e["line"] for e in await all_entries(env, task_id, None)),
    ]
    async with client(router_app(env)) as http:
        params = {"source": "attempt-1"}
        texts.append((await http.get(f"/api/v1/tasks/{task_id}/stream", params=params)).text)
        texts.append((await http.get(f"/api/v1/tasks/{task_id}/debug")).text)
        bundle_raw = (await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")).content
    with tarfile.open(fileobj=io.BytesIO(bundle_raw), mode="r:gz") as tar:
        texts.append("\n".join(tar.extractfile(m).read().decode() for m in tar if m.isfile()))  # type: ignore[union-attr]
    for text in texts:
        assert_clean(text)
    served = await all_entries(env, task_id, "attempt-1")
    assert len(served) == len(planted) and REDACTED in texts[0]
    assert json.loads(served[1]["line"])["type"] == "assistant"  # still structurally JSON


@pytest.mark.parametrize("spec", ["2.3", "2.4", "crash"])
async def test_bundle_temp_removed_on_disconnect_and_crash(env: Env, spec: str) -> None:
    task_id = await finished_single(env)
    noise = env.engine.runstore.task_dir(task_id) / "noise.log"
    noise.write_text("".join(os.urandom(40).hex() + "\n" for _ in range(20_000)))  # >64 KiB gz
    app = router_app(env)
    path = f"/api/v1/tasks/{task_id}/bundle.tar.gz"
    error: type[Exception] = RuntimeError if spec == "crash" else OSError
    sent = await drive(app, path, b"", "2.4" if spec == "crash" else spec,
                       lambda body: len(body) > 0, send_error=error)  # fmt: skip
    assert sent[0]["status"] == 200
    assert not any(m.get("more_body") is False for m in sent)  # stopped mid-stream
    assert not list(env.settings.data_dir.glob("dioxide-bundle-*"))


async def test_bundle_member_names_redacted_and_server_backups(
    env: Env, server_log: logging.Logger
) -> None:
    task_id = await finished_single(env)
    (env.engine.runstore.task_dir(task_id) / f"note-{GHP}.txt").write_text("hello\n")
    log = env.settings.server_log_path
    backup = log.with_name(log.name + ".1")
    backup.write_text(json.dumps({"msg": "rotated away", "task_id": task_id}) + "\n")
    server_log.info("current", extra={"task_id": task_id})
    async with client(router_app(env)) as http:
        raw = (await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")).content
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        names = tar.getnames()
        server = tar.extractfile(f"{task_id}/server.log").read().decode()  # type: ignore[union-attr]
    assert_clean("\n".join(names))
    assert f"{task_id}/run/note-{REDACTED}.txt" in names
    assert server.index("rotated away") < server.index("current")


async def test_boot_sweeps_stale_bundle_dirs(tmp_path: Path) -> None:
    settings = Settings(home=tmp_path, auth_token=TOKEN)
    stale = settings.data_dir / "dioxide-bundle-old"
    stale.mkdir(parents=True)
    (stale / "x.tar.gz").write_bytes(b"x")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert not stale.exists()
        key = settings.cursor_key_path
        assert len(key.read_bytes()) == 32 and (key.stat().st_mode & 0o777) == 0o600


async def test_claude_version_failure_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "late-claude"
    first = await debug.claude_version(str(missing))
    assert first.startswith("error: ")
    make_launcher(tmp_path / "bin").rename(missing)
    assert await debug.claude_version(str(missing)) == first  # within the retry window
    monkeypatch.setattr(debug, "VERSION_RETRY_S", 0.0)
    assert await debug.claude_version(str(missing)) == "2.1.328 (Claude Code stub)"
    monkeypatch.setattr(debug, "VERSION_RETRY_S", 3600.0)
    missing.unlink()
    assert await debug.claude_version(str(missing)) == "2.1.328 (Claude Code stub)"  # cached


def test_naive_iso_timestamp_is_utc() -> None:
    stamp = debug._stamp_key("ts")
    naive = stamp(b'{"ts": "2026-11-19T10:00:00"}')
    aware = stamp(b'{"ts": "2026-11-19T10:00:00+00:00"}')
    assert naive == aware == stamp(b'{"ts": "2026-11-19T10:00:00Z"}')


async def test_rotated_server_log_restarts_source(env: Env, server_log: logging.Logger) -> None:
    server_log.info("first line before rotation " + "z" * 200)
    page = await debug.server_logs(env.engine)
    assert page["entries"]
    log = env.settings.server_log_path
    log.rename(log.with_name(log.name + ".1"))  # a new file (new inode) takes its place
    log.write_text(json.dumps({"ts": "2026-11-19T10:00:00Z", "msg": "after rotation"}) + "\n")
    page = await debug.server_logs(env.engine, page["next_cursor"])
    assert [json.loads(e["line"])["msg"] for e in page["entries"]] == ["after rotation"]


# -- fix round 2 ----------------------------------------------------------------------------
def _key_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


@pytest.mark.parametrize("content", [b"", b"abc"])
def test_cursor_key_wrong_length_replaced(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "data" / "cursor.key"
    path.parent.mkdir()
    path.write_bytes(content)
    key = load_key(path)
    assert len(key) == 32 and path.read_bytes() == key and _key_mode(path) == 0o600


def test_cursor_key_kept_and_mode_tightened(tmp_path: Path) -> None:
    path = tmp_path / "cursor.key"
    path.write_bytes(b"k" * 32)
    path.chmod(0o644)
    assert load_key(path) == b"k" * 32
    assert _key_mode(path) == 0o600


@pytest.mark.parametrize("rival", [b"r" * 32, b""])
def test_cursor_key_lost_create_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rival: bytes
) -> None:
    path = tmp_path / "cursor.key"
    real_link = os.link

    def racing_link(src: Any, dst: Any) -> None:  # another process creates the key first
        Path(dst).write_bytes(rival)
        real_link(src, dst)

    monkeypatch.setattr(logpager.os, "link", racing_link)
    key = load_key(path)
    assert len(key) == 32 and path.read_bytes() == key
    if rival:
        assert key == rival  # the winner's valid key is used
    assert _key_mode(path) == 0o600


def test_cursor_key_unreadable_is_clear_error(tmp_path: Path) -> None:
    path = tmp_path / "cursor.key"
    path.write_bytes(b"k" * 32)
    path.chmod(0o000)
    try:
        with pytest.raises(RuntimeError, match="cursor key") as info:
            load_key(path)
        assert str(path) in str(info.value)
    finally:
        path.chmod(0o600)
    assert path not in logpager._keys  # nothing cached: no silent fallback


@pytest.mark.parametrize("merged", [False, True], ids=["single", "merged"])
async def test_live_to_copy_switch_no_replay(
    env: Env, monkeypatch: pytest.MonkeyPatch, merged: bool
) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "kill_mid_stage")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()

    async def implement() -> Path | None:
        return live_agent_file(env, "Implement the plan.")

    live: Path = await wait_for(implement)
    with live.open("a") as fh:
        for i in range(5):
            line = {
                "type": "assistant",
                "message": f"live {i}",
                "timestamp": f"2099-01-01T00:00:0{i}Z",
            }
            fh.write(json.dumps(line) + "\n")
    source = None if merged else "step:implement"
    name = "step:implement"
    served: list[dict[str, Any]] = []
    cursor = None
    while sum(e["source"] == name for e in served) < 2:  # part of the step read live
        page = await debug.task_logs(env.engine, task_id, source, cursor, limit=1)
        assert "error" not in page and page["entries"], page
        served += page["entries"]
        cursor = page["next_cursor"]
    await env.engine.cancel(task_id)
    await wait_status(env.engine, task_id, "cancelled")
    copy = env.engine.runstore.step_copy_path(task_id, "implement")

    async def copied() -> bool:
        return copy.is_file() and copy.read_bytes() == live.read_bytes()

    await wait_for(copied)
    shutil.rmtree(env.tmp / "claude-home")  # only the run-store copy (a new inode) is left
    while True:
        page = await debug.task_logs(env.engine, task_id, source, cursor, limit=3)
        assert "error" not in page, page
        served += page["entries"]
        cursor = page["next_cursor"]
        if not page["entries"]:
            break
    step_lines = [e["line"] for e in served if e["source"] == name]
    expected = copy.read_text().splitlines()
    if merged:
        expected = [line for line in expected if "timestamp" in json.loads(line)]
    assert [json.loads(line) for line in step_lines] == [json.loads(line) for line in expected]
    keys = [(e["source"], e["line"]) for e in served]
    assert len(keys) == len(set(keys))  # no source replayed


def test_merged_cursor_many_long_sources_pages_to_end(tmp_path: Path) -> None:
    key = b"k" * 32
    files: dict[str, Path] = {}
    for i in range(400):
        files[f"step:{i:03d}-" + "l" * 76] = tmp_path / f"step-{i}.jsonl"
    for n in range(1, 5):
        files[f"attempt-{n}"] = tmp_path / f"attempt-{n}.jsonl"
    expected: dict[str, list[dict[str, int]]] = {}
    for j, (name, path) in enumerate(files.items()):
        lines = [{"timestamp": 1_000_000 + j * 3 + k, "n": k} for k in range(3)]
        path.write_text("".join(json.dumps(line) + "\n" for line in lines))
        expected[name] = lines
    stamp = debug._stamp_key("timestamp")
    sources = [
        Source(name, (lambda p=path: p.open("rb")), stamp, final=True)
        for name, path in files.items()
    ]
    served: dict[str, list[dict[str, int]]] = {name: [] for name in files}
    raw: str | None = None
    longest = 0
    for _ in range(10_000):
        cursor = decode_cursor(raw, key, "task:t")
        page = read_page(sources, cursor, Redactor(), cap=40_000, limit=50, full=False, merged=True)
        for entry in page.entries:
            served[entry["source"]].append(json.loads(entry["line"]))
        raw = logpager.encode_cursor(page.cursor, key, "task:t")
        longest = max(longest, len(raw))
        if not page.entries:
            break
    assert served == expected  # every line once, none missing
    assert 16384 < longest <= logpager.MAX_CURSOR_CHARS


def test_cursor_encoding_is_compact() -> None:
    key = b"k" * 32
    cursor = Cursor({"a": 0, "b": 7}, {"a": False, "b": False}, {"a": 11, "b": 12})
    payload = json.loads(
        base64.urlsafe_b64decode(logpager.encode_cursor(cursor, key, "s").split(".")[0])
    )
    assert payload["o"] == {"b": 7} and "p" not in payload and payload.get("i", {}) == {"b": 12}
    decoded = decode_cursor(logpager.encode_cursor(cursor, key, "s"), key, "s")
    assert decoded.offsets == {"b": 7} and decoded.pem == {}
    huge = Cursor({f"s{i}" + "x" * 200: i + 1 for i in range(2000)})
    with pytest.raises(ValueError, match="cursor"):
        logpager.encode_cursor(huge, key, "s")


def test_only_rotating_source_restarts_on_new_inode(env: Env, tmp_path: Path) -> None:
    assert debug.server_source(env.engine).rotating
    path = tmp_path / "log"
    path.write_bytes(b"a\nb\n")
    served: dict[bool, list[str]] = {}
    for rotating in (True, False):
        source = Source("s", lambda: path.open("rb"), rotating=rotating)
        page = read_page([source], Cursor(), Redactor(), cap=1000, limit=10, full=False)
        fresh = tmp_path / "fresh"
        fresh.write_bytes(b"c\nd\ne\n")  # a new inode; the old offset is a line boundary in it
        fresh.replace(path)
        again = read_page([source], page.cursor, Redactor(), cap=1000, limit=10, full=False)
        served[rotating] = [e["line"] for e in again.entries]
        path.write_bytes(b"a\nb\n")
    assert served == {True: ["c", "d", "e"], False: ["e"]}


async def test_journal_parsed_once_per_page(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", "success")
    sub = await submit(env, "Build", agent="team-leader")
    task_id = str(sub["task_id"])
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed", "failed")
    real, calls = journal.steps_in, []  # every journal parse (live or copy) goes through it

    def counting(*args: Any) -> Any:
        calls.append(args)
        return real(*args)

    monkeypatch.setattr(journal, "steps_in", counting)
    page = await debug.task_logs(env.engine, task_id, None)
    assert {"step:plan", "step:implement"} <= {e["source"] for e in page["entries"]}
    assert len(calls) == 1


async def test_debug_masks_mcp_env_and_headers(env: Env) -> None:
    planted = ("hdr-value-1111", "Bearer hdr-value-2222", "env-value-3333", "env-value-4444")
    mcp = env.tmp / "mcp.json"
    servers = {
        "remote": {
            "type": "http",
            "url": "https://mcp.example.test/v1",
            "headers": {"X-Custom": planted[0], "Authorization": planted[1]},
        },
        "local": {"command": "run-it", "args": ["--flag"], "env": {"ODD_NAME": planted[2]}},
        "nested": {"command": "x", "env": {"LIST": [planted[3]], "PORT": 8080}},
    }
    mcp.write_text(json.dumps({"mcpServers": servers}))
    env.settings.mcp_config_path = mcp
    task_id = await finished_single(env)
    view = await debug.task_debug(env.engine, task_id)
    served = view["launch"]["mcp_config"]["mcpServers"]
    assert served["remote"]["headers"] == {"X-Custom": REDACTED, "Authorization": REDACTED}
    assert served["local"]["env"] == {"ODD_NAME": REDACTED}
    assert served["nested"]["env"] == {"LIST": [REDACTED], "PORT": 8080}
    assert served["remote"]["url"] == "https://mcp.example.test/v1"  # other fields kept
    assert served["local"]["command"] == "run-it" and served["local"]["args"] == ["--flag"]
    async with client(router_app(env)) as http:
        raw = (await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")).content
        rest = (await http.get(f"/api/v1/tasks/{task_id}/debug")).text
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        bundle = "\n".join(tar.extractfile(m).read().decode() for m in tar if m.isfile())  # type: ignore[union-attr]
    for text in (json.dumps(view), rest, bundle):
        for value in planted:
            assert value.split()[-1] not in text


async def test_cancelled_source_final_only_after_attempts_end(env: Env) -> None:
    sub = await submit(env)
    task_id = str(sub["task_id"])
    async with write_tx() as db:
        await db.execute(
            "UPDATE tasks SET status = 'cancelled', attempt = 1 WHERE id = ?", (task_id,)
        )
        await db.execute(
            "INSERT INTO attempts (task_id, n, started_at) VALUES (?, 1, 1.0)", (task_id,)
        )

    async def finals() -> tuple[bool, bool, bool]:
        view = await load_view(task_id)
        assert view is not None
        run = RunLocator(env.engine, view)
        sources = [debug._source(env.engine, view, run, name) for name in ("attempt-1", "stderr-1")]
        assert all(s is not None for s in sources)
        return view.terminal, view.final, all(s.final for s in sources if s is not None)

    assert await finals() == (True, False, False)  # the process may still write (kill grace)
    async with write_tx() as db:
        await db.execute("UPDATE attempts SET ended_at = 2.0 WHERE task_id = ?", (task_id,))
    assert await finals() == (True, True, True)


async def test_debug_masks_credential_blocks_in_settings_and_agents(env: Env) -> None:
    planted = ("settings-env-5555", "agent-env-6666", "agent-hdr-7777")
    task_id = await finished_single(env)
    data = env.settings.data_dir
    (data / "settings.json").write_text(json.dumps({"env": {"HARMLESS_NAME": planted[0]}}))
    agent = {
        "description": "d",
        "prompt": "p",
        "mcpServers": [
            {"local": {"command": "run-it", "env": {"ODD_NAME": planted[1]}}},
            {"remote": {"type": "http", "headers": {"X-Custom": planted[2]}}},
        ],
    }
    attempt_file = env.engine.runstore.attempt_agents_path(task_id, 1)
    attempt_file.write_text(json.dumps({"worker": agent}))
    view = await debug.task_debug(env.engine, task_id)
    launch = view["launch"]
    assert launch["settings"] == {"env": {"HARMLESS_NAME": REDACTED}}
    servers = launch["agents_json"]["worker"]["mcpServers"]
    assert servers[0]["local"] == {"command": "run-it", "env": {"ODD_NAME": REDACTED}}
    assert servers[1]["remote"]["headers"] == {"X-Custom": REDACTED}
    text = json.dumps(view)
    for value in planted:
        assert value not in text


@pytest.mark.parametrize(
    ("scenario", "where"),
    [
        ("needs_human_rounds", "question"),
        ("needs_human_merge_blocked", "question"),
        ("stage_failed", "error"),
    ],
)
async def test_planted_secret_in_question_reason_and_stage_error_never_served(
    env: Env, monkeypatch: pytest.MonkeyPatch, scenario: str, where: str
) -> None:
    """A secret in a question, a merge_blocked reason or a stage_failed error (all written by
    stages) is served redacted by check_task_status, the debug view and the bundle."""
    monkeypatch.setenv("STUB_CLAUDE_SCENARIO", scenario)
    monkeypatch.setenv("STUB_CLAUDE_PLANT", f" token {SK} and {GHP}")
    sub = await submit(env, "Build it", agent="team-leader")
    task_id = sub["task_id"]
    await env.engine.start()
    st = await wait_status(env.engine, task_id, "needs_human", "failed", "completed")
    assert st["status"] == ("needs_human" if where == "question" else "failed"), st
    provider = ToolProvider()
    provider.attach(env.engine)
    status = await provider.check_task_status(task_id)
    field = status["question"]["question"] if where == "question" else status["error"]
    assert "token " + REDACTED in field  # the planted text reached the field, masked
    served = json.dumps(status, ensure_ascii=False)
    view = json.dumps(await debug.task_debug(env.engine, task_id), ensure_ascii=False)
    async with client(router_app(env)) as http:
        rest_status = (await http.get(f"/api/v1/tasks/{task_id}")).text
        rest_debug = (await http.get(f"/api/v1/tasks/{task_id}/debug")).text
        bundle_raw = (await http.get(f"/api/v1/tasks/{task_id}/bundle.tar.gz")).content
    with tarfile.open(fileobj=io.BytesIO(bundle_raw), mode="r:gz") as tar:
        members = {m.name: tar.extractfile(m).read().decode() for m in tar if m.isfile()}  # type: ignore[union-attr]
    assert any(name.endswith("run/run.json") for name in members)
    for text in (served, view, rest_status, rest_debug, *members.values()):
        assert_clean(text)
    assert "token" in served and REDACTED in served


async def test_debug_shows_attempt_agents(env: Env) -> None:
    task_id = await finished_single(env)
    attempt_file = env.engine.runstore.attempt_agents_path(task_id, 1)
    ran_with = json.loads(attempt_file.read_text())
    (env.settings.data_dir / "agents.json").write_text(json.dumps({"other": {"prompt": "p"}}))
    view = await debug.task_debug(env.engine, task_id)
    assert view["launch"]["agents_json"] == ran_with
    assert "other" not in view["launch"]["agents_json"]
