"""Live recovery on the real ``claude``: the parent killed in the middle of a stage and right
after the merge commit landed, a server restart under a running task, and a duplicate
delegation. The parent session runs on opus (low effort), every stage on haiku.

Each test writes what it saw (process snapshots, file samples, debug payloads) under its own
``tmp_path / "evidence"``; nothing outside pytest's tmp dirs and the throwaway home is written.
"""

import asyncio
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import httpx2
import pytest
import yaml
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from src import journal, procs, watchdog
from src.config import ENV_PREFIX, Settings
from src.engine import TERMINAL
from src.runstore import RunStore
from tests.live.conftest import EXAMPLES, SCRIPT_MODEL, LiveEnv, kill_leftovers
from tests.live.test_live_lifecycle import DENY, REPO_FILES, _diagnose, _git, _wait

pytestmark = pytest.mark.live

DESCRIPTION = "make test_x pass"
RUN_TIMEOUT_S = 1500.0
STAGE_TIMEOUT_S = 900.0
POLL_S = 0.25
SAMPLE_S = 0.25
PROCS_EVERY = 4  # process table every 4th sample (about once a second)
RETRY_BACKOFF_S = 5.0  # the first transient resume starts 5 s after the kill
SHELL_EXIT_GRACE_S = 3.0  # a shell whose command was killed by the sweep exits by itself
SERVER_START_S = 60.0
SERVER_STOP_S = 60.0
# The implement stage's unittest run sleeps this long: a single long Bash command in a stage.
SLOW_TEST_S = 90
# How long the long command is watched before the parent is killed in it.
LONG_BASH_WATCH_S = 75.0
# Without a test run seen inside implement, the parent is killed this long into the stage.
IMPLEMENT_KILL_FALLBACK_S = 12.0
SLOW_TEST_X = f"""import time
import unittest

from calc import add


class TestX(unittest.TestCase):
    def test_x(self):
        time.sleep({SLOW_TEST_S})  # the suite is slow on purpose
        self.assertEqual(add(2, 3), 5)


if __name__ == "__main__":
    unittest.main()
"""
SLOW_README = f"""\
Run the tests with: python3 -m unittest
There is no Makefile. "test_x" is the unittest test method test_x in test_x.py.
The test suite takes about {SLOW_TEST_S} seconds. Run it in the foreground and let it finish;
do not interrupt it, background it or put a timeout on it.
"""
SINGLE_DESCRIPTION = "create file x.txt containing ok"


# -- fixtures ------------------------------------------------------------------------------------
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
    """The base home without a deployer role, the stage deny list, and a short backoff before a
    transient resume; the parent session as in the base fixture (opus, low effort)."""
    roles = yaml.safe_load((EXAMPLES / "roles.yaml").read_text(encoding="utf-8"))
    roles.pop("deployer", None)
    live_home.roles_path.write_text(yaml.safe_dump(roles, sort_keys=False), encoding="utf-8")
    extra = json.loads(json.dumps(live_home.extra_settings))
    extra["permissions"]["deny"] = DENY
    update = {"extra_settings": extra, "retry_backoff_s": RETRY_BACKOFF_S}
    return live_home.model_copy(update=update)


@pytest.fixture
def evidence(tmp_path: Path) -> Path:
    out = tmp_path / "evidence"
    out.mkdir()
    return out


# -- read-only views of the engine's state -------------------------------------------------------
def _rows(db: Path, sql: str, *params: Any) -> list[sqlite3.Row]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _task(db: Path, task_id: str) -> sqlite3.Row:
    (row,) = _rows(db, "SELECT * FROM tasks WHERE id = ?", task_id)
    return row


def _events(db: Path, task_id: str, type_: str | None = None) -> list[dict[str, Any]]:
    found = _rows(
        db, "SELECT type, data_json, at FROM events WHERE task_id = ? ORDER BY id", task_id
    )
    return [
        {"type": r["type"], "at": r["at"], **json.loads(r["data_json"] or "{}")}
        for r in found
        if type_ is None or r["type"] == type_
    ]


def _attempts(db: Path, task_id: str) -> list[sqlite3.Row]:
    return _rows(db, "SELECT * FROM attempts WHERE task_id = ? ORDER BY n", task_id)


def _session_dir(settings: Settings, row: sqlite3.Row) -> Path | None:
    if not row["session_id"] or not row["cwd"]:
        return None
    return journal.session_dir(row["cwd"], row["session_id"], settings.claude_home)


def _steps(settings: Settings, row: sqlite3.Row) -> list[journal.JournalStep]:
    if not row["session_id"]:
        return []
    run_id = row["run_id"] or journal.find_run_id(
        row["cwd"], row["session_id"], settings.claude_home
    )
    if run_id is None:
        return []
    return journal.read_steps(row["cwd"], row["session_id"], run_id, settings.claude_home)


def _running_step(settings: Settings, row: sqlite3.Row, prefix: str) -> journal.JournalStep | None:
    for step in _steps(settings, row):
        if step.label.startswith(prefix) and step.status == "running":
            return step
    return None


def _commands() -> dict[int, tuple[int, str]]:
    """pid -> (ppid, full command line) of this user's processes."""
    listing = subprocess.run(
        ["ps", "-U", str(os.getuid()), "-o", "pid=,ppid=,command="],
        capture_output=True, text=True, check=False,
    ).stdout  # fmt: skip
    table: dict[int, tuple[int, str]] = {}
    for line in listing.splitlines():
        parts = line.split(None, 2)
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            table[int(parts[0])] = (int(parts[1]), parts[2] if len(parts) == 3 else "")
    return table


def _tree(pid: int, table: dict[int, tuple[int, str]]) -> dict[int, str]:
    """Descendants of ``pid`` (by ppid) -> command line, shortened."""
    found: dict[int, str] = {}
    frontier = [pid]
    while frontier:
        parent = frontier.pop()
        for child, (ppid, command) in table.items():
            if ppid == parent and child not in found:
                found[child] = command[:160]
                frontier.append(child)
    return found


# -- a sampler thread: files and processes over time --------------------------------------------
@dataclass
class Sampler:
    """Samples, about every 0.25 s, the task's attempt streams (completion notification and
    result lines), the session's run files and the newest file under the session dir; about
    every second, the task's processes (the parent's descendants and the marker processes)."""

    settings: Settings
    task_id: str
    out: Path
    records: list[dict[str, Any]] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    _offsets: dict[int, int] = field(default_factory=dict)
    _seen: dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
        with self.out.open("w", encoding="utf-8") as fh:
            for record in self.records:
                fh.write(json.dumps(record, default=str) + "\n")

    def of(self, event: str) -> list[dict[str, Any]]:
        return [r for r in list(self.records) if r["event"] == event]

    def _add(self, event: str, **data: Any) -> None:
        self.records.append({"t": time.time(), "event": event, **data})

    def _loop(self) -> None:
        tick = 0
        while not self._stop.is_set():
            try:
                self._sample(tick % PROCS_EVERY == 0)
            except (OSError, sqlite3.Error, ValueError) as exc:
                self._add("sample_error", error=repr(exc))
            tick += 1
            self._stop.wait(SAMPLE_S)

    def _sample(self, with_procs: bool) -> None:
        row = _task(self.settings.db_path, self.task_id)
        runstore = RunStore(self.settings.runs_dir)
        for n in range(1, int(row["attempt"] or 0) + 1):
            self._stream(runstore.attempt_paths(self.task_id, n)[0], n)
        sdir = _session_dir(self.settings, row)
        if sdir is not None:
            self._session(sdir)
        if with_procs and row["pid"]:
            self._procs(int(row["pid"]), int(row["attempt"]))

    def _stream(self, path: Path, n: int) -> None:
        try:
            with path.open("rb") as fh:
                fh.seek(self._offsets.get(n, 0))
                data = fh.read()
                mtime = os.fstat(fh.fileno()).st_mtime
        except OSError:
            return
        end = data.rfind(b"\n")
        if end < 0:
            return
        self._offsets[n] = self._offsets.get(n, 0) + end + 1
        lines = data[: end + 1].splitlines()
        for i, raw in enumerate(lines):
            try:
                line = json.loads(raw)
            except ValueError:
                continue
            if line.get("subtype") == "task_notification":
                # When it is the last line read, the stream's mtime is when it was written.
                last = i == len(lines) - 1 and end == len(data) - 1
                self._add("notification", n=n, status=line.get("status"), last_line=last,
                          stream_mtime=mtime)  # fmt: skip
            elif line.get("type") == "result":
                self._add("result", n=n, subtype=line.get("subtype"),
                          cost=line.get("total_cost_usd"))  # fmt: skip

    def _session(self, sdir: Path) -> None:
        newest: tuple[float, str] | None = None
        for root, _dirs, files in os.walk(sdir):
            for name in files:
                path = Path(root, name)
                try:
                    st = path.stat()
                except OSError:
                    continue
                rel = str(path.relative_to(sdir))
                if newest is None or st.st_mtime > newest[0]:
                    newest = (st.st_mtime, rel)
                if path.parent.name == "workflows" and name.endswith(".json"):
                    key = f"run:{rel}"
                    if self._seen.get(key) != (st.st_mtime, st.st_size):
                        self._seen[key] = (st.st_mtime, st.st_size)
                        try:
                            status = json.loads(path.read_text(encoding="utf-8")).get("status")
                        except (OSError, ValueError):
                            status = None
                        self._add("run_file", path=rel, mtime=st.st_mtime, size=st.st_size,
                                  status=status)  # fmt: skip
        if newest is not None and self._seen.get("newest") != newest:
            self._seen["newest"] = newest
            self._add("activity", mtime=newest[0], path=newest[1])

    def _procs(self, pid: int, n: int) -> None:
        table = _commands()
        tree = _tree(pid, table)
        marked = sorted(procs.marker_pids(self.task_id))
        snapshot = {"claude": pid, "alive": pid in table, "tree": tree, "marked": marked}
        if self._seen.get("procs") != snapshot:
            self._seen["procs"] = snapshot
            self._add("procs", n=n, **snapshot)


@contextmanager
def sampling(settings: Settings, task_id: str, out: Path) -> Iterator[Sampler]:
    sampler = Sampler(settings, task_id, out / "samples.jsonl")
    sampler.start()
    try:
        yield sampler
    finally:
        sampler.stop()


# -- killing the parent --------------------------------------------------------------------------
def _kill_parent(settings: Settings, task_id: str) -> dict[str, Any]:
    """SIGKILL the task's ``claude`` (checked: still the process started); returns what ran
    under it just before and what was still alive right after (synchronous: the engine on this
    event loop cannot react in between)."""
    row = _task(settings.db_path, task_id)
    pid, start = int(row["pid"]), row["pid_start"]
    assert procs.alive_and_same(pid, start), (pid, start)
    table = _commands()
    tree = _tree(pid, table)
    marked = sorted(procs.marker_pids(task_id))
    os.kill(pid, signal.SIGKILL)
    at = time.time()
    after = _commands()
    return {
        "at": at,
        "n": int(row["attempt"]),
        "pid": pid,
        "claude_marked": pid in marked,
        "tree": tree,
        "marked": marked,
        "alive_after_kill": {p: c for p, c in tree.items() if p in after},
    }


async def _wait_for(check: Any, timeout: float, what: str) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        found = check()
        if found:
            return found
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(POLL_S)


async def _swept_after(settings: Settings, task_id: str, n: int, timeout: float) -> dict[str, Any]:
    """The ``attempt_end`` sweep event that followed attempt ``n``'s end."""

    def found() -> dict[str, Any] | None:
        ended = [a for a in _attempts(settings.db_path, task_id) if a["n"] == n and a["ended_at"]]
        if not ended:
            return None
        since = ended[0]["ended_at"] - 1
        sweeps = [e for e in _events(settings.db_path, task_id, "sweep")
                  if e["trigger"] == "attempt_end" and e["at"] >= since]  # fmt: skip
        return sweeps[0] if sweeps else None

    result: dict[str, Any] = await _wait_for(found, timeout, f"the sweep after attempt {n}")
    return result


# -- shared checks -------------------------------------------------------------------------------
def _session_flag(argv: list[str], flag: str) -> str | None:
    return argv[argv.index(flag) + 1] if flag in argv else None


def _one_merge_commit(repo: Path, base: str, task_id: str) -> str:
    new = _git(repo, "rev-list", f"{base}..main").splitlines()
    assert len(new) == 1, new
    notes = _git(repo, "notes", "--ref=dioxide", "list").splitlines()
    assert len(notes) == 1, notes
    assert _git(repo, "notes", "--ref=dioxide", "show", new[0]) == task_id
    return new[0]


def _task_branches(repo: Path) -> list[str]:
    return _git(repo, "branch", "--list", "dioxide/*", "--format=%(refname:short)").splitlines()


def _same_session(attempts: list[dict[str, Any]], session_id: str) -> None:
    first, *rest = attempts
    assert _session_flag(first["argv"], "--session-id") == session_id, first["argv"]
    assert rest, "no resumed attempt"
    for attempt in rest:
        assert _session_flag(attempt["argv"], "--resume") == session_id, attempt["argv"]


def _run_progress(settings: Settings, row: sqlite3.Row) -> list[dict[str, Any]]:
    if not row["session_id"] or not row["run_id"]:
        return []
    progress = journal.read_progress(
        row["cwd"], row["session_id"], row["run_id"], settings.claude_home
    )
    keys = ("label", "model", "agentType", "cached", "state", "durationMs")
    return [{k: p.get(k) for k in keys} for p in progress]


def _save(out: Path, name: str, value: Any) -> None:
    (out / name).write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def _copy_run_files(settings: Settings, row: sqlite3.Row, out: Path) -> None:
    """The real run file and journal, and the engine's attempt streams, for the evidence."""
    sdir = _session_dir(settings, row)
    if sdir is not None and row["run_id"]:
        run_file = sdir / "workflows" / f"{row['run_id']}.json"
        run_dir = sdir / "subagents" / "workflows" / row["run_id"]
        for source, name in ((run_file, "workflow-run-file.json"),
                             (run_dir / "journal.jsonl", "journal.jsonl")):  # fmt: skip
            if source.is_file():
                shutil.copy(source, out / name)
    task_dir = RunStore(settings.runs_dir).task_dir(row["id"])
    for source in task_dir.glob("attempt-*"):
        shutil.copy(source, out / source.name)


async def _report(env: LiveEnv, task_id: str, out: Path, started: float) -> dict[str, Any]:
    status = await env.provider.check_task_status(task_id)
    debug = await env.provider.get_task_debug(task_id)
    row = _task(env.settings.db_path, task_id)
    _save(out, "status.json", status)
    _save(out, "debug.json", debug)
    _save(out, "progress.json", _run_progress(env.settings, row))
    _copy_run_files(env.settings, row, out)
    attempts = [{k: a[k] for k in ("n", "outcome", "reason", "cost_usd")}
                for a in debug["attempts"]]  # fmt: skip
    print(f"\nlive: status={status['status']} wall={time.monotonic() - started:.1f}s "
          f"cost_usd={status.get('cost_usd')} attempts={json.dumps(attempts)}")  # fmt: skip
    print("progress:", json.dumps(_run_progress(env.settings, row)))
    print("evidence:", out)
    return status


@asynccontextmanager
async def _diagnosis(env: LiveEnv, task_id: str) -> AsyncIterator[None]:
    """Any error inside (a failed assertion, a wait that timed out) prints the diagnosis."""
    try:
        yield
    except Exception:
        await _diagnose(env, task_id, await env.provider.check_task_status(task_id))
        raise


@asynccontextmanager
async def _server_diagnosis(server: "Server", task_id: str) -> AsyncIterator[None]:
    try:
        yield
    except Exception:
        print("\n=== LIVE TEST DIAGNOSIS (server) ===")
        if server.proc is not None and server.proc.poll() is None:
            status = await server.get(f"/api/v1/tasks/{task_id}")
            debug = await server.get(f"/api/v1/tasks/{task_id}/debug")
            print("status:", json.dumps(status, indent=2, default=str)[:4000])
            for key in ("steps", "attempts"):
                print(f"{key}:", json.dumps(debug.get(key), indent=2, default=str)[:4000])
        else:
            print("server not running; see the server-*.out files in the evidence dir")
        raise


def _stage_models(settings: Settings, task_id: str, label: str) -> set[str]:
    """Models of a stage's assistant turns, from its transcript copy in the run store."""
    path = RunStore(settings.runs_dir).step_copy_path(task_id, label)
    models: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        try:
            line = json.loads(raw)
        except ValueError:
            continue
        message = line.get("message") if isinstance(line, dict) else None
        if line.get("type") == "assistant" and isinstance(message, dict) and message.get("model"):
            models.add(str(message["model"]))
    return models


# -- 1. SIGKILL in the middle of implement -------------------------------------------------------
def _make_suite_slow(repo: Path) -> None:
    (repo / "test_x.py").write_text(SLOW_TEST_X, encoding="utf-8")
    (repo / "README").write_text(SLOW_README, encoding="utf-8")
    _git(repo, "commit", "-q", "-am", "the test suite is slow")


def _unittest_under(pid: int) -> dict[int, str]:
    return {p: c for p, c in _tree(pid, _commands()).items() if "unittest" in c}


async def _kill_inside_implement(env: LiveEnv, task_id: str, sampler: Sampler) -> dict[str, Any]:
    """Wait for the implement stage, then for its long test run (or the fallback delay), watch
    the long command for a while and SIGKILL the parent inside it."""
    settings = env.settings

    def in_implement() -> journal.JournalStep | None:
        return _running_step(settings, _task(settings.db_path, task_id), "implement")

    await _wait_for(in_implement, STAGE_TIMEOUT_S, "the implement stage")
    entered = time.monotonic()
    pid = int(_task(settings.db_path, task_id)["pid"])
    long_bash: dict[int, str] = {}
    while time.monotonic() - entered < IMPLEMENT_KILL_FALLBACK_S and not long_bash:
        long_bash = _unittest_under(pid)
        await asyncio.sleep(POLL_S)
    watched_from = time.time()
    if long_bash:
        while time.time() - watched_from < LONG_BASH_WATCH_S and _unittest_under(pid):
            await asyncio.sleep(1.0)
    assert in_implement(), "implement finished before the kill"
    row = _task(settings.db_path, task_id)
    branch_commits = _git(env.repo, "rev-list", f"main..{row['branch']}").splitlines()
    killed = _kill_parent(settings, task_id)
    killed["long_bash"] = long_bash
    killed["long_bash_watched_s"] = round(killed["at"] - watched_from, 1) if long_bash else 0
    killed["branch_commits_at_kill"] = branch_commits
    killed["implement_running_after_kill"] = bool(in_implement())
    activity = [r for r in sampler.of("activity") if r["t"] >= watched_from]
    killed["activity_during_long_bash"] = activity
    return killed


async def test_live_kill_claude_during_implement(live_engine: LiveEnv, evidence: Path) -> None:
    env = live_engine
    _make_suite_slow(env.repo)
    base = _git(env.repo, "rev-parse", "main")
    sub = await env.provider.delegate_task(
        "planner", DESCRIPTION, {"project_dir": str(env.repo)}, 3, 0
    )
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]
    started = time.monotonic()
    async with _diagnosis(env, task_id):
        with sampling(env.settings, task_id, evidence) as sampler:
            killed = await _kill_inside_implement(env, task_id, sampler)
            # The orphan check below is not vacuous: the stage had a process under the parent.
            assert killed["tree"], "no process ran under the parent at the kill"
            run_before = _task(env.settings.db_path, task_id)["run_id"]
            swept = await _swept_after(env.settings, task_id, killed["n"], STAGE_TIMEOUT_S)
            await asyncio.sleep(SHELL_EXIT_GRACE_S)
            alive = _commands()
            killed["sweep"] = swept
            killed["alive_after_sweep"] = {p: c for p, c in killed["tree"].items() if p in alive}
            _save(evidence, "kill.json", killed)
            status = await _wait(env, task_id, {"needs_human"}, RUN_TIMEOUT_S)
        status = await _report(env, task_id, evidence, started)
        assert status["status"] == "completed", status
        assert not killed["alive_after_sweep"], killed["alive_after_sweep"]
        debug = await env.provider.get_task_debug(task_id)
        _same_session(debug["attempts"], status["session_id"])
        assert status["run_id"] == run_before, (status["run_id"], run_before)
        steps = {s["label"]: s for s in status["steps"]}
        for label in ("plan", "setup"):
            assert steps[label]["started_count"] == 1, steps[label]
            assert steps[label]["cached"] is True, steps[label]
        assert len(_task_branches(env.repo)) == 1, _task_branches(env.repo)
        _one_merge_commit(env.repo, base, task_id)


# -- 2. SIGKILL right after the merge commit landed on base --------------------------------------
async def test_live_kill_claude_after_merge_commit(live_engine: LiveEnv, evidence: Path) -> None:
    """The parent is killed right after the merge commit landed, inside the merge stage: the
    resume in the same session re-runs only merge (``started_count == 2``), which finds the
    branch already merged. Also: setup and merge ran on ``script_model``."""
    env = live_engine
    base = _git(env.repo, "rev-parse", "main")
    sub = await env.provider.delegate_task(
        "planner", DESCRIPTION, {"project_dir": str(env.repo)}, 3, 0
    )
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]
    started = time.monotonic()
    async with _diagnosis(env, task_id):
        with sampling(env.settings, task_id, evidence):

            def base_moved() -> bool:
                return _git(env.repo, "rev-parse", "main") != base

            deadline = time.monotonic() + RUN_TIMEOUT_S
            while not base_moved():  # a tight poll: the kill must follow the advance at once
                assert time.monotonic() < deadline, "base never advanced"
                assert _task(env.settings.db_path, task_id)["status"] in ("queued", "running")
                await asyncio.sleep(0.05)
            row = _task(env.settings.db_path, task_id)
            merge_at_kill = _running_step(env.settings, row, "merge")
            killed = _kill_parent(env.settings, task_id)
            killed["merge_running_at_kill"] = merge_at_kill is not None
            killed["merge_commit"] = _git(env.repo, "rev-parse", "main")
            killed["note"] = _git(
                env.repo, "notes", "--ref=dioxide", "show", killed["merge_commit"]
            )
            after = _running_step(env.settings, _task(env.settings.db_path, task_id), "merge")
            killed["merge_running_after_kill"] = after is not None
            _save(evidence, "kill.json", killed)
            # The kill must have interrupted the merge stage, or this test proves nothing.
            assert killed["merge_running_at_kill"], "the kill missed the merge stage"
            status = await _wait(env, task_id, {"needs_human"}, RUN_TIMEOUT_S)
        status = await _report(env, task_id, evidence, started)
        assert killed["note"] == task_id, killed
        assert status["status"] == "completed", status
        debug = await env.provider.get_task_debug(task_id)
        assert len(debug["attempts"]) >= 2, debug["attempts"]
        _same_session(debug["attempts"], status["session_id"])
        steps = {s["label"]: s for s in status["steps"]}
        assert steps["merge"]["started_count"] == 2, steps["merge"]
        _one_merge_commit(env.repo, base, task_id)
        models = {
            label: _stage_models(env.settings, task_id, label) for label in ("setup", "merge")
        }
        _save(evidence, "script_stage_models.json", {k: sorted(v) for k, v in models.items()})
        for label, found in models.items():
            assert found and all(SCRIPT_MODEL in m for m in found), (label, found)


# -- 3. SIGTERM the server while the task's claude runs; restart; re-attach ----------------------
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _server_env(settings: Settings, port: int) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(ENV_PREFIX)}
    values: dict[str, Any] = {
        "HOME": settings.home,
        "PORT": port,
        "AUTH_TOKEN": settings.auth_token,
        "CLAUDE_BIN": settings.claude_bin,
        "PARENT_MODEL": settings.parent_model,
        "PARENT_EFFORT": settings.parent_effort,
        "SCRIPT_MODEL": settings.script_model,
        "CLAUDE_HOME": settings.claude_home,
        "DEFAULT_PROJECT_DIR": settings.default_project_dir,
        "MAX_CONCURRENT_TASKS": settings.max_concurrent_tasks,
        "RETRY_BACKOFF_S": settings.retry_backoff_s,
        "EXTRA_SETTINGS": json.dumps(settings.extra_settings),
    }
    for name, value in values.items():
        if value is not None:
            env[f"{ENV_PREFIX}{name}"] = str(value)
    return env


@dataclass
class Server:
    """``dioxide up`` as a child process on the throwaway home."""

    settings: Settings
    port: int
    logs: Path
    runs: int = 0
    proc: subprocess.Popen[bytes] | None = None

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.settings.auth_token}"}

    async def start(self) -> None:
        self.runs += 1
        exe = Path(sys.executable).with_name("dioxide")
        out = (self.logs / f"server-{self.runs}.out").open("ab")
        self.proc = subprocess.Popen(
            [str(exe), "up"], cwd=self.logs, env=_server_env(self.settings, self.port),
            stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
        )  # fmt: skip
        out.close()
        deadline = time.monotonic() + SERVER_START_S
        async with httpx.AsyncClient(base_url=self.base, headers=self.headers) as client:
            while True:
                assert self.proc.poll() is None, f"server exited: {self.proc.returncode}"
                try:
                    if (await client.get("/health")).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                assert time.monotonic() < deadline, "server did not start"
                await asyncio.sleep(POLL_S)

    async def terminate(self) -> float:
        """SIGTERM; returns how long the server took to exit."""
        assert self.proc is not None
        sent = time.monotonic()
        self.proc.send_signal(signal.SIGTERM)
        await asyncio.to_thread(self.proc.wait, SERVER_STOP_S)
        return time.monotonic() - sent

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    async def tool(self, name: str, **arguments: Any) -> dict[str, Any]:
        """One MCP tool call over HTTP, in its own short session."""
        http = httpx2.AsyncClient(headers=self.headers, timeout=60)
        async with http:
            transport = streamable_http_client(f"{self.base}/mcp", http_client=http)
            async with Client(transport, mode="legacy", cache=None) as client:
                result = await client.call_tool(name, arguments)
        assert not result.is_error, result
        payload = result.structured_content
        if isinstance(payload, dict) and set(payload) == {"result"}:
            payload = payload["result"]
        if payload is None:
            payload = json.loads(result.content[0].text)  # type: ignore[union-attr]
        assert isinstance(payload, dict), payload
        return payload

    async def get(self, path: str) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=self.base, headers=self.headers) as client:
            response = await client.get(path)
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return body


@pytest.fixture
async def live_server(live_home: Settings, evidence: Path) -> AsyncIterator[Server]:
    server = Server(live_home, _free_port(), evidence)
    try:
        yield server
    finally:
        if server.proc is not None and server.proc.poll() is None:
            try:
                await server.terminate()
            except subprocess.TimeoutExpired:
                server.kill()
        ids = []
        if live_home.db_path.is_file():
            ids = [r["id"] for r in _rows(live_home.db_path, "SELECT id FROM tasks")]
        leaked = kill_leftovers(RunStore(live_home.runs_dir), ids)
    assert not leaked, f"claude processes left running (killed): {leaked}"


def _group_of(pid: int) -> str:
    return subprocess.run(
        ["ps", "-o", "pid=,ppid=,pgid=,sess=,stat=,lstart=", "-p", str(pid)],
        capture_output=True, text=True, check=False,
    ).stdout.strip()  # fmt: skip


async def test_live_restart_server_reattach(
    live_server: Server, live_repo: Path, evidence: Path
) -> None:
    server, settings = live_server, live_server.settings
    db = settings.db_path
    base = _git(live_repo, "rev-parse", "main")
    await server.start()
    request = {"agent": "planner", "description": DESCRIPTION,
               "input_context": {"project_dir": str(live_repo)}}  # fmt: skip
    sub = await server.tool("delegate_task", **request)
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]
    started = time.monotonic()
    facts: dict[str, Any] = {}
    async with _server_diagnosis(server, task_id):
        with sampling(settings, task_id, evidence):

            def in_stage() -> journal.JournalStep | None:
                return _running_step(settings, _task(db, task_id), "implement")

            await _wait_for(in_stage, STAGE_TIMEOUT_S, "the implement stage")
            row = _task(db, task_id)
            pid, stored = int(row["pid"]), row["pid_start"]
            facts["child_before"] = _group_of(pid)
            facts["server_pid"] = server.proc.pid if server.proc else None
            facts["stop_s"] = round(await server.terminate(), 2)
            facts["server_exit"] = server.proc.returncode if server.proc else None
            facts["child_after_stop"] = _group_of(pid)
            facts["stored_start"] = stored
            facts["read_start_after_stop"] = procs.proc_start_time(pid)
            survived = procs.alive_and_same(pid, stored)
            facts["survived"] = survived
            _save(evidence, "restart.json", facts)
            assert survived, facts  # right after the stop: the server's shutdown left it running
            facts["stream_size_at_stop"] = (
                RunStore(settings.runs_dir).attempt_paths(task_id, 1)[0].stat().st_size
            )
            await server.start()
            again = await server.tool("delegate_task", **request)  # repeated after the restart
            facts["delegate_after_restart"] = again
            _save(evidence, "restart.json", facts)
            deadline = time.monotonic() + RUN_TIMEOUT_S
            while (status := await server.get(f"/api/v1/tasks/{task_id}"))[
                "status"
            ] not in TERMINAL:
                assert time.monotonic() < deadline, status
                if status["status"] == "needs_human":
                    break
                await asyncio.sleep(3.0)
        debug = await server.get(f"/api/v1/tasks/{task_id}/debug")
        _save(evidence, "status.json", status)
        _save(evidence, "debug.json", debug)
        row = _task(db, task_id)
        _save(evidence, "progress.json", _run_progress(settings, row))
        _copy_run_files(settings, row, evidence)
        attempts = _attempts(db, task_id)
        wall, cost, shown = (
            time.monotonic() - started,
            status.get("cost_usd"),
            json.dumps(facts, default=str),
        )
        print(f"\nlive: status={status['status']} wall={wall:.1f}s cost_usd={cost} facts={shown}")
        print("progress:", json.dumps(_run_progress(settings, row)))
        print("evidence:", evidence)
        assert again["task_id"] == task_id and again.get("deduplicated") is True, again
        recovered = _events(db, task_id, "boot_recovered")
        assert [(e["n"], e["action"]) for e in recovered] == [(1, "reattach")], recovered
        assert len(attempts) == 1, [dict(a) for a in attempts]
        assert len(_events(db, task_id, "spawned")) == 1
        assert status["status"] == "completed", status
        assert attempts[0]["outcome"] == "success", dict(attempts[0])
        _one_merge_commit(live_repo, base, task_id)


# -- 4. the same delegation twice ----------------------------------------------------------------
async def test_live_duplicate_delegate(live_engine: LiveEnv, evidence: Path) -> None:
    env = live_engine
    db = env.settings.db_path
    context = {"project_dir": str(env.repo)}
    first = await env.provider.delegate_task("implementer", SINGLE_DESCRIPTION, context, 3, 0)
    assert first["status"] == "queued", first
    task_id = first["task_id"]
    started = time.monotonic()

    def running() -> bool:
        row = _task(db, task_id)
        return row["status"] == "running" and row["pid"] is not None

    async with _diagnosis(env, task_id):
        await _wait_for(running, STAGE_TIMEOUT_S, "the task to run")
        while_running = await env.provider.delegate_task(
            "implementer", SINGLE_DESCRIPTION, context, 3, 0
        )
        assert _task(db, task_id)["status"] == "running"  # the second call came while it ran
        status = await _wait(env, task_id, set(), RUN_TIMEOUT_S)
        after = await env.provider.delegate_task("implementer", SINGLE_DESCRIPTION, context, 3, 0)
        _save(evidence, "delegations.json", [first, while_running, after])
        status = await _report(env, task_id, evidence, started)
        assert status["status"] == "completed", status
        for again in (while_running, after):
            assert again["task_id"] == task_id, again
            assert again.get("deduplicated") is True, again
        assert len(_rows(db, "SELECT id FROM tasks")) == 1
        assert len(_attempts(db, task_id)) == 1
        assert len(_events(db, task_id, "spawned")) == 1


# -- 5. one long CPU-bound command inside a task: activity, not a stall -------------------------
LONG_COMMAND_S = 90
# Below the ~30 s cadence of the parent stream's tool_progress lines during a long Bash command
# (fix-wave live run 3): without the CPU rule the task would be stall-killed.
STALL_TIMEOUT_S = 20
LONG_DESCRIPTION = (
    "Run exactly this command once with the Bash tool, in the foreground, and wait for it to "
    f"finish; it takes about {LONG_COMMAND_S} seconds. Do not run it in the background, do not "
    "add a timeout and do not interrupt it:\n"
    f"python3 -c 'import time\nend = time.time() + {LONG_COMMAND_S}\nwhile time.time() < end: "
    'pass\nprint("burned")\'\n'
    "Then reply with the single word: done."
)


def _burning_under(pid: int) -> bool:
    """The long command runs under the task's claude (full command lines: ``_tree`` shortens
    them, and the interpreter path alone can fill its 160 characters)."""
    table = _commands()
    return any("while time.time()" in table[p][1] for p in _tree(pid, table) if p in table)


async def test_live_long_command_not_stalled(
    live_engine: LiveEnv, evidence: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = live_engine
    passes: list[dict[str, Any]] = []  # every watchdog CPU check: busy or not, stream age
    inner = watchdog.busy_processes

    def recording(engine: Any, key: tuple[str, int]) -> bool:
        busy = inner(engine, key)
        stream = RunStore(env.settings.runs_dir).attempt_paths(key[0], key[1])[0]
        age = engine._clock() - stream.stat().st_mtime if stream.exists() else None
        passes.append({"t": time.time(), "busy": busy, "stream_age_s": age})
        return busy

    monkeypatch.setattr(watchdog, "busy_processes", recording)
    # A short stall limit and watchdog interval (the engine reads its settings object live):
    # without the CPU activity rule the command, which writes no file, would be killed.
    env.settings.stall_timeout_s = STALL_TIMEOUT_S
    env.settings.watchdog_interval_s = 5.0
    # The running watchdog loop already sleeps its first (default) interval: restart it on the
    # short one, so its first pass (no CPU figures to compare yet) comes before the command.
    assert env.engine._watchdog is not None
    env.engine._watchdog.cancel()
    with suppress(asyncio.CancelledError):
        await env.engine._watchdog
    env.engine._watchdog = asyncio.create_task(watchdog.run(env.engine))
    sub = await env.provider.delegate_task(
        "implementer", LONG_DESCRIPTION, {"project_dir": str(env.repo)}, 3, 0
    )
    assert sub["status"] == "queued", sub
    task_id = sub["task_id"]
    started = time.monotonic()
    seen: list[float] = []  # when the burning command was seen under the task's claude
    async with _diagnosis(env, task_id):
        deadline = time.monotonic() + RUN_TIMEOUT_S
        while True:
            row = _task(env.settings.db_path, task_id)
            if row["status"] in TERMINAL or row["status"] == "needs_human":
                break
            assert time.monotonic() < deadline, "the task did not end"
            if row["pid"] and _burning_under(int(row["pid"])):
                seen.append(time.monotonic())
            await asyncio.sleep(1.0)
        status = await _report(env, task_id, evidence, started)
        burned = seen[-1] - seen[0] if seen else 0.0
        record = {"seen_s": burned, "samples": len(seen), "watchdog_passes": passes}
        _save(evidence, "long_command.json", record)
        assert burned > STALL_TIMEOUT_S + env.settings.watchdog_interval_s, burned
        assert any(p["busy"] for p in passes), passes  # the CPU rule counted it as activity
        # Decisive: on some pass nothing else was fresh enough (the stream older than the stall
        # limit), so only the CPU rule kept the task alive.
        stale = [p for p in passes if p["stream_age_s"] and p["stream_age_s"] > STALL_TIMEOUT_S]
        assert stale and all(p["busy"] for p in stale), passes
        assert status["status"] == "completed", status
        assert status["kill_reason"] is None, status
        assert len(_attempts(env.settings.db_path, task_id)) == 1
        assert _events(env.settings.db_path, task_id, "stall") == []
