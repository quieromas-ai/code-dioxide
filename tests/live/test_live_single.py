"""First live test: one real single task run by the real ``claude`` through the real engine."""

import asyncio
import json
import time
from typing import Any

import pytest

from src import procs
from src.engine import TERMINAL
from tests.live.conftest import LiveEnv

pytestmark = pytest.mark.live

TIMEOUT_S = 300.0
POLL_S = 2.0
DESCRIPTION = "create file x.txt containing ok"
TAIL = 20


async def _wait_terminal(env: LiveEnv, task_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_S
    while True:
        status = await env.provider.check_task_status(task_id)
        if status["status"] in TERMINAL or time.monotonic() > deadline:
            return status
        await asyncio.sleep(POLL_S)


async def _tail(env: LiveEnv, task_id: str, source: str) -> list[str]:
    page = await env.provider.get_task_logs(task_id, source, None, 200, False)
    return [str(entry["line"]) for entry in page.get("entries", [])][-TAIL:]


async def _diagnose(env: LiveEnv, task_id: str, status: dict[str, Any]) -> None:
    print("\n=== LIVE TEST DIAGNOSIS ===")
    print("status:", json.dumps(status, indent=2, default=str))
    for source in ("attempt-1", "stderr-1"):
        print(f"--- last {TAIL} lines of {source} (redacted) ---")
        for line in await _tail(env, task_id, source):
            print(line[:600])
    debug = await env.provider.get_task_debug(task_id)
    print("--- debug attempts ---")
    print(json.dumps(debug.get("attempts"), indent=2, default=str))


async def _check(env: LiveEnv, task_id: str, status: dict[str, Any]) -> None:
    assert status["status"] == "completed", status
    x_txt = env.repo / "x.txt"
    assert x_txt.is_file(), "x.txt was not created"
    assert x_txt.read_text(encoding="utf-8").strip() == "ok"
    assert status["cost_usd"] > 0, status

    logs = await env.provider.get_task_logs(task_id, "attempt-1", None, 200, False)
    assert logs["entries"], logs
    debug = await env.provider.get_task_debug(task_id)
    (attempt,) = debug["attempts"]
    assert attempt["outcome"] == "success", attempt
    argv = attempt["argv"]
    assert "--allowedTools" in argv and "Workflow" in argv and "--strict-mcp-config" in argv
    assert "--permission-mode" not in argv
    assert debug["launch"]["claude_version"], debug["launch"]
    assert not debug["launch"]["claude_version"].startswith("error"), debug["launch"]

    recorded = env.engine.runstore.read_procs(task_id)
    assert procs.marker_pids(task_id) == set(), "a process still carries the task marker"
    assert not [p for p, s in recorded.items() if procs.alive_and_same(p, s)]

    merged = await env.provider.get_task_logs(task_id, None, None, 200, False)
    assert merged["entries"], merged
    served = json.dumps([status, logs, merged, debug], default=str)
    assert env.token not in served


async def test_live_single_task(live_engine: LiveEnv) -> None:
    env = live_engine
    sub = await env.provider.delegate_task(
        "implementer", DESCRIPTION, {"project_dir": str(env.repo)}, 3, 0
    )
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]
    started = time.monotonic()
    status = await _wait_terminal(env, task_id)
    print(f"\nlive run: status={status['status']} wall={time.monotonic() - started:.1f}s "
          f"cost_usd={status.get('cost_usd')}")  # fmt: skip
    try:
        await _check(env, task_id, status)
    except AssertionError:
        await _diagnose(env, task_id, status)
        raise
    argv = (await env.provider.get_task_debug(task_id))["attempts"][0]["argv"]
    print("argv:", json.dumps(argv))
