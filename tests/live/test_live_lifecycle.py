"""Live lifecycle smoke: plan gate -> answer -> completed on the real ``claude``; the parent
session on opus (low effort), every stage on haiku."""

import asyncio
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from src import procs
from src.config import Settings
from src.engine import TERMINAL
from src.tasks import CLEAN_EXIT
from tests.live.conftest import EXAMPLES, LiveEnv

pytestmark = pytest.mark.live

GATE_TIMEOUT_S = 600.0
ANSWER_TIMEOUT_S = 900.0
POLL_S = 3.0
TAIL = 20
DESCRIPTION = "make test_x pass"
CALC = "def add(a, b):\n    return a - b\n"
TEST_X = """import unittest

from calc import add


class TestX(unittest.TestCase):
    def test_x(self):
        self.assertEqual(add(2, 3), 5)


if __name__ == "__main__":
    unittest.main()
"""
README = """\
Run the tests with: python3 -m unittest
There is no Makefile. "test_x" is the unittest test method test_x in test_x.py.
"""
REPO_FILES = {"README": README, "calc.py": CALC, "test_x.py": TEST_X}
# User config, passed verbatim: keep the stages off containers and remotes. Best effort only:
# these are command-prefix patterns, so a command that reaches the same tool another way (a
# wrapper script, `env`, a different spelling, a shell -c string) is not matched.
DENY = [
    "Bash(podman:*)",
    "Bash(docker:*)",
    "Bash(kubectl:*)",
    "Bash(git push:*)",
    "Bash(gh:*)",
]
PYCACHE = "__pycache__"  # left by a stage that ran the tests in the main checkout


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )
    return done.stdout.strip()


@pytest.fixture
def live_repo(live_repo: Path) -> Path:
    """The base repo plus one tiny module with one failing unittest, committed on main."""
    for name, text in REPO_FILES.items():
        (live_repo / name).write_text(text, encoding="utf-8")
    _git(live_repo, "add", *REPO_FILES)
    _git(live_repo, "commit", "-q", "-m", "add calc with a failing test")
    return live_repo


@pytest.fixture
def live_home(live_home: Settings) -> Settings:
    """The base home without a deployer role (no deploy stage); the parent session as in the
    base fixture (opus, low effort)."""
    roles = yaml.safe_load((EXAMPLES / "roles.yaml").read_text(encoding="utf-8"))
    roles.pop("deployer", None)
    live_home.roles_path.write_text(yaml.safe_dump(roles, sort_keys=False), encoding="utf-8")
    extra = json.loads(json.dumps(live_home.extra_settings))
    extra["permissions"]["deny"] = DENY
    return live_home.model_copy(update={"extra_settings": extra})


async def _wait(env: LiveEnv, task_id: str, wanted: set[str], timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        status = await env.provider.check_task_status(task_id)
        if status["status"] in wanted | TERMINAL or time.monotonic() > deadline:
            return status
        await asyncio.sleep(POLL_S)


async def _tail(env: LiveEnv, task_id: str, source: str) -> list[str]:
    page = await env.provider.get_task_logs(task_id, source, None, 200, False)
    return [str(entry["line"]) for entry in page.get("entries", [])][-TAIL:]


async def _diagnose(env: LiveEnv, task_id: str, status: dict[str, Any]) -> None:
    debug = await env.provider.get_task_debug(task_id)
    print("\n=== LIVE TEST DIAGNOSIS ===")
    print("status:", status.get("status"), "stage:", status.get("stage"))
    print("error:", status.get("error"))
    print("question:", json.dumps(status.get("question"), indent=2, default=str))
    print("steps:", json.dumps(debug.get("steps"), indent=2, default=str))
    print("attempts:", json.dumps(debug.get("attempts"), indent=2, default=str))
    print("call:", json.dumps(debug.get("call"), indent=2, default=str)[:4000])
    for attempt in debug.get("attempts") or []:
        for source in (f"attempt-{attempt['n']}", f"stderr-{attempt['n']}"):
            print(f"--- last {TAIL} lines of {source} (redacted) ---")
            for line in await _tail(env, task_id, source):
                print(line[:600])


def _session_flag(argv: list[str], flag: str) -> str | None:
    return argv[argv.index(flag) + 1] if flag in argv else None


def _check_gate(status: dict[str, Any]) -> None:
    assert status["status"] == "needs_human", status
    question = status["question"]
    assert question["kind"] == "plan_approval", question
    assert question["stage"] == "plan", question


async def _check_done(env: LiveEnv, task_id: str, status: dict[str, Any], base: str) -> None:
    assert status["status"] == "completed", status
    (plan,) = [s for s in status["steps"] if s["label"] == "plan"]
    assert plan["started_count"] == 1, plan
    assert plan["cached"] is True, plan

    new = _git(env.repo, "rev-list", f"{base}..main").splitlines()
    assert len(new) == 1, new
    assert _git(env.repo, "notes", "--ref=dioxide", "show", new[0]) == task_id
    untracked = [ln for ln in _git(env.repo, "status", "--porcelain").splitlines()
                 if ln != f"?? {PYCACHE}/"]  # fmt: skip
    assert not untracked, untracked
    entries = {p.name for p in env.repo.iterdir()} - {PYCACHE}
    assert entries == {".git", *REPO_FILES}, entries
    worktrees = _git(env.repo, "worktree", "list", "--porcelain").splitlines()
    paths = [Path(line.split(" ", 1)[1]) for line in worktrees if line.startswith("worktree ")]
    assert not [p for p in paths[1:] if p.is_relative_to(env.repo)], paths
    tests = subprocess.run(
        ["python3", "-m", "unittest", "-q"], cwd=env.repo, capture_output=True, text=True
    )
    assert tests.returncode == 0, tests.stderr

    debug = await env.provider.get_task_debug(task_id)
    first, *rest = debug["attempts"]
    session = _session_flag(first["argv"], "--session-id")
    assert session and session == status["session_id"], first["argv"]
    assert rest, "no resumed attempt"
    for attempt in rest:
        assert _session_flag(attempt["argv"], "--resume") == session, attempt["argv"]

    clean = {a["n"] for a in debug["attempts"] if a["reason"] == CLEAN_EXIT}
    kept = {pid: start for pid, (start, n) in env.engine.runstore.read_kept(task_id).items()
            if n in clean}  # fmt: skip
    recorded = env.engine.runstore.read_procs(task_id)
    alive = procs.marker_pids(task_id) | {
        p for p, s in recorded.items() if procs.alive_and_same(p, s)
    }
    assert not [p for p in alive if not procs.alive_and_same(p, kept.get(p))], alive

    assert status["cost_usd"] > 0, status


async def test_live_plan_gate_answer_completes(live_engine: LiveEnv) -> None:
    env = live_engine
    base = _git(env.repo, "rev-parse", "main")
    sub = await env.provider.delegate_task(
        "coordinator", DESCRIPTION, {"project_dir": str(env.repo), "plan_gate": True}, 3, 0
    )
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]

    started = time.monotonic()
    status = await _wait(env, task_id, {"needs_human"}, GATE_TIMEOUT_S)
    print(f"\nlive gate: status={status['status']} wall={time.monotonic() - started:.1f}s "
          f"cost_usd={status.get('cost_usd')}")  # fmt: skip
    try:
        _check_gate(status)
    except Exception:  # any error, not only a failed assertion
        await _diagnose(env, task_id, status)
        raise

    answered = await env.provider.answer_task(task_id, "approved")
    assert answered["status"] == "queued", answered
    resumed = time.monotonic()
    status = await _wait(env, task_id, {"needs_human"}, ANSWER_TIMEOUT_S)
    print(f"live answer: status={status['status']} wall={time.monotonic() - resumed:.1f}s "
          f"cost_usd={status.get('cost_usd')}")  # fmt: skip
    try:
        await _check_done(env, task_id, status, base)
    except Exception:  # any error, not only a failed assertion
        await _diagnose(env, task_id, status)
        raise
    debug = await env.provider.get_task_debug(task_id)
    print("steps:", json.dumps(debug["steps"], default=str))
    print("attempts:", json.dumps([{k: a[k] for k in ("n", "started", "ended", "outcome",
          "reason", "cost_usd")} for a in debug["attempts"]], default=str))  # fmt: skip
