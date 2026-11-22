import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import src.main
from src import procs


def _env(task_id: str) -> dict[str, str]:
    env = dict(os.environ)
    env["DIOXIDE_TASK_ID"] = task_id
    return env


def _gone(pid: int) -> bool:
    return not procs.is_running(pid)


def _wait_gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        procs.reap(pid)
        if _gone(pid):
            return True
        time.sleep(0.05)
    return False


Started = list[subprocess.Popen[bytes] | tuple[int, float | None]]


@pytest.fixture
def cleanup() -> Iterator[Started]:
    started: Started = []
    yield started
    for item in started:
        if isinstance(item, subprocess.Popen):
            item.kill()
            item.wait()
            if item.stdout is not None:
                item.stdout.close()
            continue
        pid, start = item
        if procs.alive_and_same(pid, start):  # never signal a reused pid
            os.kill(pid, signal.SIGKILL)
        _wait_gone(pid)
        procs.reap(pid)  # drop the engine-side Popen (no ResourceWarning)


# A non-platform binary: macOS hides the environment of Apple platform binaries (/bin/sleep,
# /bin/sh, ...) from `ps -E`, so a marker on those is not observable there.
SLEEPER = [sys.executable, "-c", "import time; time.sleep(30)"]


def _detached_sleep(task_id: str, started: Started) -> int:
    child = subprocess.Popen(
        SLEEPER,
        env=_env(task_id),
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    started.append(child)
    return child.pid


def _spawn_sleep(tmp_path: Path, task_id: str, started: Started) -> tuple[int, float | None]:
    pid, start = procs.spawn(
        ["sleep", "30"], tmp_path, _env(task_id), tmp_path / "out", tmp_path / "err"
    )
    started.append((pid, start))
    return pid, start


def _wait_marker(task_id: str, pid: int) -> None:
    deadline = time.monotonic() + 5
    while pid not in procs.marker_pids(task_id):
        assert time.monotonic() < deadline, "marker never visible"
        time.sleep(0.05)


def test_spawn_files_and_start_time(tmp_path: Path, cleanup: Started) -> None:
    cmd = [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"]
    pid, start = procs.spawn(cmd, tmp_path, dict(os.environ), tmp_path / "o", tmp_path / "e")
    cleanup.append((pid, start))
    assert _wait_gone(pid)
    assert (tmp_path / "o").read_text() == "out\n"
    assert (tmp_path / "e").read_text() == "err\n"
    assert (tmp_path / "o").stat().st_mode & 0o777 == 0o600
    assert procs.reap(pid) is None  # already reaped above

    pid, start = _spawn_sleep(tmp_path, "t", cleanup)
    assert procs.proc_start_time(pid) == start
    assert procs.alive_and_same(pid, start)
    assert not procs.alive_and_same(pid, start - 100)
    assert procs.reap(pid) is None
    os.kill(pid, signal.SIGKILL)
    assert _wait_gone(pid)
    assert not procs.alive_and_same(pid, start)


def test_kill_tree_noop_when_pid_reused(tmp_path: Path, cleanup: Started) -> None:
    task_id = str(uuid.uuid4())
    pid, start = _spawn_sleep(tmp_path, task_id, cleanup)
    procs.kill_tree(pid, start - 3600, task_id, grace_s=0.5)
    time.sleep(0.2)
    assert procs.alive_and_same(pid, start)


def test_kill_tree_reaches_marker_procs(tmp_path: Path, cleanup: Started) -> None:
    task_id = str(uuid.uuid4())
    parent, start = procs.spawn(
        ["sh", "-c", "sleep 30 & wait"], tmp_path, _env(task_id), tmp_path / "o", tmp_path / "e"
    )
    cleanup.append((parent, start))
    other = _detached_sleep(task_id, cleanup)
    _wait_marker(task_id, other)
    procs.kill_tree(parent, start, task_id, grace_s=2.0)
    assert _wait_gone(parent)
    assert _wait_gone(other)
    assert procs.marker_pids(task_id) == set()


def test_sweep_marker_kills_separate_pgroup(tmp_path: Path, cleanup: Started) -> None:
    task_id = str(uuid.uuid4())
    other = _detached_sleep(task_id, cleanup)
    assert os.getpgid(other) != os.getpgid(0)
    _wait_marker(task_id, other)
    assert procs.sweep(task_id, {}, grace_s=2.0).killed == [other]
    assert _wait_gone(other)
    assert procs.sweep(task_id, {}, grace_s=0.5).killed == []


def _command(pid: int) -> str:
    out = subprocess.run(["ps", "-o", "comm=", "-p", str(pid)], capture_output=True, text=True)
    return out.stdout.strip()


PERL = shutil.which("perl")
# /bin/sh starts a platform /bin/sleep in its own session (perl setsid + exec): the shape of an
# orphaned Bash-tool shell, whose env marker macOS does not show.
SETSID_SLEEP = "use POSIX; POSIX::setsid() or die; exec q(/bin/sleep), q(300)"


@pytest.mark.skipif(PERL is None, reason="perl not available")
def test_sweep_recorded_kills_orphaned_platform_binary(tmp_path: Path, cleanup: Started) -> None:
    task_id = str(uuid.uuid4())
    script = f"'{PERL}' -e '{SETSID_SLEEP}' & wait"
    parent, start = procs.spawn(
        ["/bin/sh", "-c", script], tmp_path, _env(task_id), tmp_path / "o", tmp_path / "e"
    )
    cleanup.append((parent, start))
    deadline = time.monotonic() + 5
    recorded: dict[int, float] = {}
    sleeper = None
    while sleeper is None:
        assert time.monotonic() < deadline, "sleep child never appeared"
        recorded = procs.descendants(parent)
        sleeper = next((p for p in recorded if _command(p).endswith("sleep")), None)
        time.sleep(0.05)
    cleanup.append((sleeper, recorded[sleeper]))
    assert os.getsid(sleeper) != os.getsid(parent)

    os.kill(parent, signal.SIGKILL)
    assert _wait_gone(parent)
    time.sleep(0.2)
    assert procs.alive_and_same(sleeper, recorded[sleeper])  # survived the parent's death
    procs.kill_tree(parent, start, task_id, grace_s=0.5, recorded=recorded)  # parent gone: no-op
    assert procs.alive_and_same(sleeper, recorded[sleeper])

    assert sleeper in procs.sweep(task_id, recorded, grace_s=2.0).killed
    assert _wait_gone(sleeper)


def test_recorded_pid_with_other_start_not_signalled(cleanup: Started) -> None:
    other = subprocess.Popen(["/bin/sleep", "300"], start_new_session=True)
    cleanup.append(other)
    start = procs.proc_start_time(other.pid)
    assert start is not None
    stale = {other.pid: start + 5}
    assert procs.kill_recorded(stale, grace_s=0.5).killed == []
    assert procs.sweep(str(uuid.uuid4()), stale, grace_s=0.5).killed == []
    assert procs.alive_and_same(other.pid, start)

    assert procs.kill_recorded({other.pid: start}, grace_s=2.0).killed == [other.pid]
    assert other.wait(timeout=5) is not None


def test_descendants_records_start_times(tmp_path: Path, cleanup: Started) -> None:
    parent, start = procs.spawn(
        ["/bin/sh", "-c", "/bin/sleep 300 & wait"],
        tmp_path,
        dict(os.environ),
        tmp_path / "o",
        tmp_path / "e",
    )
    cleanup.append((parent, start))
    deadline = time.monotonic() + 5
    found: dict[int, float] = {}
    while not found:
        assert time.monotonic() < deadline
        found = procs.descendants(parent)
        time.sleep(0.05)
    for pid, pstart in found.items():
        cleanup.append((pid, pstart))
        assert procs.proc_start_time(pid) == pstart
    assert parent not in found and os.getpid() not in found
    procs.kill_tree(parent, start, "unused-" + str(uuid.uuid4()), grace_s=2.0)
    assert _wait_gone(parent)
    assert all(_wait_gone(pid) for pid in found)


def test_marker_exact_match(cleanup: Started) -> None:
    long_id = str(uuid.uuid4())
    short_id = long_id[:-1]
    pid = _detached_sleep(long_id, cleanup)
    _wait_marker(long_id, pid)
    assert pid not in procs.marker_pids(short_id)
    assert pid not in procs.marker_pids(long_id + "0")
    assert os.getpid() not in procs.marker_pids(long_id)


IGNORE_TERM = (
    "import signal, sys, time\n"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "print('ready', flush=True)\n"
    "time.sleep(30)\n"
)


def _term_ignoring_child(cleanup: Started) -> tuple[subprocess.Popen[bytes], float]:
    child = subprocess.Popen(
        [sys.executable, "-c", IGNORE_TERM], stdout=subprocess.PIPE, start_new_session=True
    )
    cleanup.append(child)
    assert child.stdout is not None and child.stdout.readline() == b"ready\n"
    start = procs.proc_start_time(child.pid)
    assert start is not None
    return child, start


def test_sigkill_after_grace_for_term_ignoring_child(cleanup: Started) -> None:
    child, start = _term_ignoring_child(cleanup)
    assert procs.kill_recorded({child.pid: start}, grace_s=0.3).killed == [child.pid]
    assert child.wait(timeout=5) == -signal.SIGKILL


def test_no_sigkill_when_start_changed_before_escalation(
    cleanup: Started, monkeypatch: pytest.MonkeyPatch
) -> None:
    child, start = _term_ignoring_child(cleanup)
    sent: list[tuple[int, signal.Signals]] = []
    real_signal = procs._signal

    def spy(pids: list[int] | dict[int, float], sig: signal.Signals) -> None:
        sent.extend((pid, sig) for pid in pids)
        real_signal(pids, sig)

    real_snapshot = procs._snapshot

    def start_after_term() -> dict[int, Any]:
        table = real_snapshot()
        proc = table.get(child.pid)
        reused = any(s is signal.SIGTERM and p == child.pid for p, s in sent)
        if reused and proc is not None and proc.start is not None:
            table[child.pid] = replace(proc, start=proc.start + 1000)
        return table

    monkeypatch.setattr(procs, "_signal", spy)
    monkeypatch.setattr(procs, "_snapshot", start_after_term)
    assert procs.kill_recorded({child.pid: start}, grace_s=0.3).killed == [child.pid]
    assert sent == [(child.pid, signal.SIGTERM)]
    assert child.poll() is None  # the "reused" pid was not SIGKILLed


def test_pid_one_and_zero_never_signalled(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[int] = []
    monkeypatch.setattr(procs, "_signal", lambda pids, sig: sent.extend(pids))
    monkeypatch.setattr(procs, "_same", lambda targets: dict(targets))
    assert procs.kill_recorded({0: 1.0, 1: 1.0, -1: 1.0}, grace_s=0.1).killed == []
    assert sent == []


def test_start_time_independent_of_caller_tz(
    cleanup: Started, monkeypatch: pytest.MonkeyPatch
) -> None:
    child, _start = _term_ignoring_child(cleanup)
    values = []
    try:
        for tz in ("UTC", "Asia/Tokyo", "America/New_York"):
            monkeypatch.setenv("TZ", tz)
            time.tzset()
            values.append(procs.proc_start_time(child.pid))
    finally:
        monkeypatch.undo()
        time.tzset()
    assert values[0] is not None and len(set(values)) == 1
    assert abs(values[0] - time.time()) < 3600  # a UTC epoch, not shifted by a zone offset


def test_run_timeout_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(procs, "PS_TIMEOUT_S", 0.2)
    began = time.monotonic()
    assert procs._run([sys.executable, "-c", "import time; time.sleep(10)"]) == ""
    assert procs._run([sys.executable, "-c", "import time; time.sleep(10)"]) == ""
    assert time.monotonic() - began < 5


def test_same_uses_one_snapshot(cleanup: Started, monkeypatch: pytest.MonkeyPatch) -> None:
    child, start = _term_ignoring_child(cleanup)
    other, other_start = _term_ignoring_child(cleanup)
    calls: list[str] = []
    real_snapshot = procs._snapshot

    def counting() -> dict[int, Any]:
        calls.append("snapshot")
        return real_snapshot()

    monkeypatch.setattr(procs, "_snapshot", counting)
    monkeypatch.setattr(procs, "is_running", lambda pid: calls.append("is_running"))
    monkeypatch.setattr(procs, "proc_start_time", lambda pid: calls.append("start"))
    targets = {child.pid: start, other.pid: other_start + 5, 999_999: 1.0}
    assert procs._same(targets) == {child.pid: start}
    assert calls == ["snapshot"]


def test_marker_in_argv_only_is_never_the_task(cleanup: Started) -> None:
    """A process whose command line merely holds ``DIOXIDE_TASK_ID=<id>`` (a grep, an editor)
    is not a task process: never found by the marker, never signalled, never a survivor."""
    task_id = str(uuid.uuid4())
    env = {k: v for k, v in os.environ.items() if not k.startswith("DIOXIDE_")}
    bystander = subprocess.Popen(
        [*SLEEPER, f"DIOXIDE_TASK_ID={task_id}"],
        env=env,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    cleanup.append(bystander)
    marked = _detached_sleep(task_id, cleanup)  # a real task process, for contrast
    _wait_marker(task_id, marked)
    assert bystander.pid not in procs.marker_pids(task_id)
    assert procs.survivors(task_id, {}) == [marked]
    swept = procs.sweep(task_id, {}, grace_s=2.0)
    assert swept.killed == [marked]
    assert bystander.poll() is None  # not signalled
    assert procs.survivors(task_id, {}) == []


# Rows as `ps -axo pid=,ppid=,stat=,lstart=,time=,comm=` prints them (macOS `time`: [[D-]H:]M:SS.ss)
# and `ps -o time=` values of procps on Linux ([D-]HH:MM:SS).
MACOS_ROWS = [
    "    1     0 Ss   Thu Nov  5 17:03:04 2026     320:55.49 /sbin/launchd",
    "  412     1 Ss   Fri Nov 20 08:01:02 2026     0:00.03 /usr/sbin/cfprefsd",
    " 1066   980 R    Fri Nov 20 09:15:44 2026    12:34.56 /opt/homebrew/bin/python3.12",
    " 7001     1 S    Thu Nov 19 21:00:00 2026  1:02:03.45 /Applications/My App.app/x",
    " 7002     1 S    Mon Nov  2 21:00:00 2026 2-03:04:05.06 /usr/bin/make",
]
LINUX_TIMES = {"00:00:00": 0.0, "00:01:05": 65.0, "01:02:03": 3723.0, "2-03:04:05": 183845.0}


def test_cpu_time_parsed_from_ps_output(monkeypatch: pytest.MonkeyPatch) -> None:
    for text, seconds in LINUX_TIMES.items():
        assert procs.parse_cpu_time(text) == seconds, text
    for bad in ("", "abc", "1:2:3:4", "-1:00"):
        assert procs.parse_cpu_time(bad) is None, bad
    monkeypatch.setattr(procs, "_IS_DARWIN", True)
    monkeypatch.setattr(procs, "_run", lambda cmd: "\n".join(MACOS_ROWS) + "\n")
    table = procs._read_table()
    assert {pid: p.cpu for pid, p in table.items()} == {
        1: 19255.49, 412: 0.03, 1066: 754.56, 7001: 3723.45, 7002: 183845.06,
    }  # fmt: skip
    assert table[7001].command == "x" and table[1066].ppid == 980
    assert table[412].start == procs._parse_lstart("Fri Nov 20 08:01:02 2026")


def test_descendants_with_cpu_from_one_snapshot(cleanup: Started) -> None:
    """The real table: a busy child's CPU time grows; the parent itself is never listed."""
    parent = subprocess.Popen(
        [sys.executable, "-c", "import subprocess, sys, time\n"
         "subprocess.Popen([sys.executable, '-c', 'while True:\\n    pass'])\ntime.sleep(30)"],
        start_new_session=True,
    )  # fmt: skip
    cleanup.append(parent)
    start = procs.proc_start_time(parent.pid)
    deadline = time.monotonic() + 10
    while True:
        kids, cpu = procs.descendants_with_cpu(parent.pid, start)
        if cpu and max(c for _s, c in cpu.values()) >= 0.5:
            break
        assert time.monotonic() < deadline, cpu
        time.sleep(0.1)
    assert parent.pid not in kids and set(cpu) <= set(kids)
    assert procs.descendants_with_cpu(parent.pid, (start or 0) - 100) == ({}, {})
    procs.kill_tree(parent.pid, start, "unused-" + str(uuid.uuid4()), grace_s=2.0)
    assert all(_wait_gone(pid) for pid in kids)


def test_child_runs_with_the_umask_the_server_started_with(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The server's own files are private (umask 077); a spawned claude gets the user's umask
    back, so files it creates in the user's checkout have the user's usual modes."""
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", "umask-test-token")
    monkeypatch.setattr(src.main.uvicorn, "run", lambda *a, **k: None)
    original = os.umask(0o027)
    try:
        src.main.run()
        assert os.umask(0o077) == 0o077  # the server itself stays private
        out, err = tmp_path / "out", tmp_path / "err"
        script = "umask; : > made"
        pid, _ = procs.spawn(["/bin/sh", "-c", script], tmp_path, dict(os.environ), out, err)
        deadline = time.monotonic() + 10
        while procs.reap(pid) is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert out.read_text().strip() == "0027"
        assert (tmp_path / "made").stat().st_mode & 0o777 == 0o640
        assert out.stat().st_mode & 0o777 == 0o600  # the server's own log file
    finally:
        os.umask(original)
        procs.set_child_umask(None)
