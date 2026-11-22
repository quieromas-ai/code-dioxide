"""Process control: spawn detached sessions, identify them safely, kill whole task trees.

A pid is only signalled after its start time matches the recorded one (pid reuse). A task's
processes are found three ways:

* live descendants of the stored ``claude`` pid (only while it is alive);
* the ``DIOXIDE_TASK_ID=<id>`` environment marker;
* the recorded descendant set (``descendants`` snapshots taken by the engine on every poll and
  persisted by ``RunStore.write_procs``), which still names Bash-tool shells after they have been
  re-parented to PID 1 (they run in their own process group / session).

Two kinds of process are spared by every kill function (``Spare``): a process whose command name
is listed in ``sweep_exempt`` (never the task's own ``claude``), and a process a clean attempt
left running (``keep``, matched by pid and start time). The command name is the basename of
``argv[0]`` as ``ps -o comm`` reports it (Linux: ``/proc/<pid>/cmdline``), taken from the same
process-table snapshot; a process that rewrites its ``argv[0]`` is seen under the new name.

Limits: on macOS ``ps -E`` does not show the environment of Apple platform binaries
(``/bin/sh``, ``/bin/zsh``, ``/bin/bash``, ``/bin/sleep``, ...), so the marker is invisible for
exactly the shells that get orphaned; orphan cleanup there relies on the recorded set, which
misses a process spawned and orphaned between two polls. Double-forked daemons that scrub their
environment are not covered on any platform.
"""

import calendar
import logging
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from src.launcher import TASK_MARKER
from src.serverlog import LOGGER_NAME

_IS_DARWIN = sys.platform == "darwin"
# Fixed locale and time zone: lstart is parsed as UTC, whatever the caller's TZ is.
_PS_ENV = {"LC_ALL": "C", "TZ": "UTC", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}
_POLL_S = 0.05
PS_TIMEOUT_S = 10.0
SPAWN_START_TRIES = 3
_log = logging.getLogger(LOGGER_NAME)
_timeout_logged = False

_children: dict[int, subprocess.Popen[bytes]] = {}
# The umask every spawned child gets (-1: inherit this process's). The server sets 077 for its
# own files; its children (claude, the stages, merge.sh) get back the umask it started with, so
# what they create in the user's checkout has the user's usual modes. Set once at startup.
_child_umask = -1
Liveness = Literal["same", "dead", "other", "unknown"]


class ListingError(Exception):
    """The process table could not be read (``ps`` failed, timed out or listed nothing): no
    conclusion about any process may be drawn from it, least of all "nothing is alive"."""


def _run(cmd: list[str]) -> str:
    """Output of a ``ps`` call; "" when it fails or hangs past ``PS_TIMEOUT_S`` (logged once)."""
    global _timeout_logged
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, env=_PS_ENV, check=False, timeout=PS_TIMEOUT_S
        )
    except subprocess.TimeoutExpired:
        if not _timeout_logged:
            _timeout_logged = True
            _log.warning(
                "process listing timed out", extra={"cmd": cmd[0], "timeout_s": PS_TIMEOUT_S}
            )
        return ""
    except OSError:
        return ""
    return out.stdout


def set_child_umask(mask: int | None) -> None:
    """The umask for every child spawned from now on; None: inherit this process's."""
    global _child_umask
    _child_umask = -1 if mask is None else mask


def _open_log(path: Path) -> int:
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)


def spawn(
    cmd: Sequence[str],
    cwd: Path,
    env: Mapping[str, str],
    stdout_path: Path,
    stderr_path: Path,
) -> tuple[int, float | None]:
    """Start ``cmd`` in its own session with stdout/stderr appended to files."""
    out_fd = _open_log(stdout_path)
    try:
        err_fd = _open_log(stderr_path)
        try:
            proc = subprocess.Popen(
                list(cmd),
                cwd=cwd,
                env=dict(env),
                stdin=subprocess.DEVNULL,
                stdout=out_fd,
                stderr=err_fd,
                start_new_session=True,
                umask=_child_umask,  # set in the child after fork: no global flip here
            )
        finally:
            os.close(err_fd)
    finally:
        os.close(out_fd)
    _children[proc.pid] = proc
    start = proc_start_time(proc.pid)
    for _ in range(SPAWN_START_TRIES - 1):  # a failed ps must not leave the start unknown
        if start is not None or proc.poll() is not None:
            break
        time.sleep(_POLL_S)
        start = proc_start_time(proc.pid)
    return proc.pid, start


def _collect(pid: int) -> None:
    """Reap a spawned child that has exited, keeping its exit code for ``reap``."""
    proc = _children.get(pid)
    if proc is not None:
        proc.poll()


def tracked(pid: int) -> bool:
    """True while this engine holds the ``Popen`` of ``pid`` (``reap`` then knows its exit)."""
    return pid in _children


def reap(pid: int) -> int | None:
    """Exit code of a process this engine spawned, once it has exited; None otherwise."""
    proc = _children.get(pid)
    if proc is None:
        return None
    code = proc.poll()
    if code is not None:
        del _children[pid]
    return code


@dataclass(frozen=True)
class _Proc:
    ppid: int
    start: float | None
    zombie: bool
    command: str = ""  # basename of argv[0]
    cpu: float | None = None  # cumulative CPU time (user + system), seconds; None: unreadable


@dataclass(frozen=True)
class Spare:
    """Processes a kill or sweep leaves alone.

    ``exempt``: command names (exact, case-sensitive) never signalled, except the task's own
    ``claude`` (``own``, pid -> start). ``keep``: processes a clean attempt left running (pid ->
    start); a pid whose start time differs is another process and is not spared.
    """

    exempt: Collection[str] = ()
    keep: Mapping[int, float] = field(default_factory=dict)
    own: Mapping[int, float] = field(default_factory=dict)


@dataclass(frozen=True)
class Swept:
    """One kill pass: the pids signalled and the ``(pid, command)`` spared as exempt."""

    killed: list[int]
    exempted: list[tuple[int, str]]


def _parse_lstart(text: str) -> float | None:
    try:
        return float(calendar.timegm(time.strptime(" ".join(text.split()), "%a %b %d %H:%M:%S %Y")))
    except ValueError:
        return None


_CPU_TIME = re.compile(r"(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)")


def parse_cpu_time(text: str) -> float | None:
    """Seconds of a ``ps`` cumulative CPU time: ``[D-][HH:]MM:SS[.ss]`` (macOS ``time``:
    ``M:SS.ss``, ``H:MM:SS.ss``; procps ``time``: ``HH:MM:SS``, ``D-HH:MM:SS``); None when
    it is not one."""
    found = _CPU_TIME.fullmatch(text.strip())
    if found is None:
        return None
    days, hours, minutes, seconds = found.groups()
    return int(days or 0) * 86400 + int(hours or 0) * 3600 + int(minutes) * 60 + float(seconds)


def _clock_ticks() -> float:
    try:
        return float(os.sysconf("SC_CLK_TCK"))
    except (ValueError, OSError):
        return 100.0


def _linux_stat(pid: int | str) -> list[str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # Fields after the parenthesised command name (which may itself hold spaces).
    return raw[raw.rfind(")") + 2 :].split()


def _linux_command(pid: str) -> str:
    try:
        argv0 = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0", 1)[0]
    except OSError:
        return ""
    return os.path.basename(argv0.decode("utf-8", "replace"))


def _snapshot() -> dict[int, _Proc]:
    """One process-table snapshot: pid -> (ppid, start time, zombie, command name).

    Raises ``ListingError`` when the listing failed: a real table always holds this process."""
    table = _read_table()
    if os.getpid() not in table:
        raise ListingError("process listing failed or incomplete")
    return table


def _read_table() -> dict[int, _Proc]:
    table: dict[int, _Proc] = {}
    if _IS_DARWIN:
        # lstart is always five fields, time one; comm (argv[0], may hold spaces) is the rest.
        for row in _run(["ps", "-axo", "pid=,ppid=,stat=,lstart=,time=,comm="]).splitlines():
            parts = row.split(None, 9)
            if len(parts) >= 9 and parts[0].isdigit() and parts[1].isdigit():
                zombie = parts[2].startswith("Z")
                start = _parse_lstart(" ".join(parts[3:8]))
                cpu = parse_cpu_time(parts[8])
                command = os.path.basename(parts[9].strip()) if len(parts) == 10 else ""
                table[int(parts[0])] = _Proc(int(parts[1]), start, zombie, command, cpu)
        return table
    ticks = _clock_ticks()
    for entry in Path("/proc").iterdir():
        fields = _linux_stat(entry.name) if entry.name.isdigit() else None
        if fields is not None and len(fields) >= 20:
            zombie = fields[0] == "Z"
            command = _linux_command(entry.name)
            cpu = (int(fields[11]) + int(fields[12])) / ticks  # utime + stime
            table[int(entry.name)] = _Proc(int(fields[1]), float(fields[19]), zombie, command, cpu)
    return table


def proc_start_time(pid: int) -> float | None:
    """Stable process start time (darwin: lstart as UTC epoch, 1 s; linux: ticks since boot)."""
    if _IS_DARWIN:
        raw = _run(["ps", "-o", "lstart=", "-p", str(pid)]).strip()
        return _parse_lstart(raw) if raw else None
    fields = _linux_stat(pid)
    if fields is None or len(fields) < 20:
        return None
    return float(fields[19])


def _exists(pid: int) -> bool:
    """``kill(pid, 0)``: the pid is in use (by some process, maybe a zombie or another one)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def _state_and_start(pid: int) -> tuple[str, float | None] | None:
    """(state letters, start time) of ``pid`` from one ``ps`` call; None when nothing could be
    read (``ps`` failed or timed out, or the pid has just gone)."""
    if _IS_DARWIN:
        row = _run(["ps", "-o", "stat=,lstart=", "-p", str(pid)]).split(None, 1)
        if len(row) != 2:
            return None
        start = _parse_lstart(row[1])
        return (row[0], start) if start is not None else None
    fields = _linux_stat(pid)
    if fields is None or len(fields) < 20:
        return None
    return fields[0], float(fields[19])


def liveness(pid: int, start: float | None) -> Liveness:
    """Is ``pid`` still the process started at ``start``? ``same``; ``dead`` (no such process,
    or a zombie); ``other`` (a different process holds the pid); ``unknown`` (the process
    table could not be read: never taken as dead nor as reused). Without ``start`` a live pid
    is ``unknown`` (its identity cannot be checked)."""
    if not pid or not _exists(pid):
        return "dead"
    read = _state_and_start(pid)
    if read is None:
        return "unknown" if _exists(pid) else "dead"
    state, actual = read
    if state.startswith("Z"):
        return "dead"
    if start is None:
        return "unknown"
    return "same" if actual == start else "other"


def is_running(pid: int) -> bool:
    """Alive and not a zombie. An unreadable process table counts as running (unknown is
    never taken as gone)."""
    return liveness(pid, None) != "dead"


def alive_and_same(pid: int, start: float | None) -> bool:
    """Positively the process started at ``start`` (``unknown`` is False: never signal on it)."""
    return start is not None and liveness(pid, start) == "same"


def _commands(cmd: list[str]) -> dict[int, str]:
    """pid -> the rest of the row of a ``pid=,command=`` listing; ``ListingError`` when it
    listed nothing (this process is always listed)."""
    listing = _run(cmd)
    if not listing.strip():
        raise ListingError("process listing failed")
    rows: dict[int, str] = {}
    for row in listing.splitlines():
        pid_text, _, rest = row.strip().partition(" ")
        if pid_text.isdigit():
            rows[int(pid_text)] = rest
    return rows


def _marker_scan(task_id: str) -> set[int]:
    entry = f"{TASK_MARKER}={task_id}"
    found: set[int] = set()
    if _IS_DARWIN:
        # ``ps -E`` appends the environment to the command line: the marker must be found in
        # that appended part only. The plain command lines (listed right after) are subtracted
        # by count, so a process whose argv merely holds the text is never taken for the task's.
        pattern = re.compile(rf"(?:^|\s){re.escape(entry)}(?=\s|$)")
        with_env = _commands(["ps", "-Eww", "-U", str(os.getuid()), "-o", "pid=,command="])
        plain = _commands(["ps", "-ww", "-U", str(os.getuid()), "-o", "pid=,command="])
        for pid, line in with_env.items():
            argv = plain.get(pid)
            if argv is None:
                continue  # gone between the two listings
            if len(pattern.findall(line)) > len(pattern.findall(argv)):
                found.add(pid)
        return found
    target = entry.encode()
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            environ = (proc_dir / "environ").read_bytes()
        except OSError:
            continue
        if target in environ.split(b"\0"):
            found.add(int(proc_dir.name))
    return found


def _live(pids: Iterable[int], table: Mapping[int, _Proc]) -> dict[int, float]:
    out: dict[int, float] = {}
    for pid in pids:
        proc = table.get(pid)
        if proc is not None and proc.start is not None and not proc.zombie:
            out[pid] = proc.start
    return out


def marker_pids(task_id: str) -> set[int]:
    """Every live process of this user whose environment holds ``DIOXIDE_TASK_ID=<task_id>``."""
    return set(_live(_marker_scan(task_id), _snapshot()))


def _descendants(pid: int, table: Mapping[int, _Proc]) -> dict[int, float]:
    children: dict[int, list[int]] = {}
    for child, proc in table.items():
        children.setdefault(proc.ppid, []).append(child)
    seen: set[int] = set()
    stack = [pid]
    while stack:
        for child in children.get(stack.pop(), []):
            if child not in seen and child != pid:
                seen.add(child)
                stack.append(child)
    return _live(seen, table)


def descendants(pid: int) -> dict[int, float]:
    """Every live descendant of ``pid`` mapped to its start time (one ``ps`` snapshot)."""
    return _descendants(pid, _snapshot())


def descendants_if_same(pid: int, start: float) -> dict[int, float]:
    """Like ``descendants``, but empty unless ``pid`` is still the process started at ``start``
    (judged in the same snapshot): a reused pid's children are never taken for the task's."""
    table = _snapshot()
    proc = table.get(pid)
    if proc is None or proc.zombie or proc.start != start:
        return {}
    return _descendants(pid, table)


def descendants_with_cpu(
    pid: int, start: float | None
) -> tuple[dict[int, float], dict[int, tuple[float, float]]]:
    """From one snapshot: the live descendants of ``pid`` ``{pid: start}`` (as
    ``descendants_if_same``; without ``start``, as ``descendants``) and their cumulative CPU time
    ``{pid: (start, cpu seconds)}`` (``pid`` itself, the task's claude, is not among them)."""
    table = _snapshot()
    if start is not None:
        proc = table.get(pid)
        if proc is None or proc.zombie or proc.start != start:
            return {}, {}
    kids = _descendants(pid, table)
    cpu = {k: (s, table[k].cpu or 0.0) for k, s in kids.items() if table[k].cpu is not None}
    return kids, cpu


def _protected(table: Mapping[int, _Proc]) -> set[int]:
    """This process and its ancestors: never signalled."""
    protected = {os.getpid()}
    pid = os.getppid()
    while pid > 1 and pid not in protected:
        protected.add(pid)
        proc = table.get(pid)
        pid = proc.ppid if proc is not None else 0
    return protected


def _same(targets: Mapping[int, float]) -> dict[int, float]:
    """The targets that are still the very process recorded (start time checked per pid, all
    against one process-table snapshot)."""
    for pid in targets:
        _collect(pid)
    try:
        live = _live(targets, _snapshot())
    except ListingError:
        return {}  # identity unverified: nothing is signalled
    return {pid: start for pid, start in targets.items() if live.get(pid) == start}


def _signal(pids: Iterable[int], sig: signal.Signals) -> None:
    for pid in pids:
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


def _wait(targets: dict[int, float], timeout: float) -> dict[int, float]:
    deadline = time.monotonic() + timeout
    alive = targets
    while alive and time.monotonic() < deadline:
        time.sleep(_POLL_S)
        for pid in alive:
            _collect(pid)
        try:
            table = _snapshot()
        except ListingError:
            continue  # not known to be gone: still waited for
        alive = {p: s for p, s in alive.items() if p in _live([p], table)}
    return alive


def _split(
    targets: Mapping[int, float], table: Mapping[int, _Proc], spare: Spare
) -> tuple[dict[int, float], list[tuple[int, str]]]:
    """``targets`` without the spared processes, and the exempt ones as ``(pid, command)``."""
    remaining: dict[int, float] = {}
    exempted: list[tuple[int, str]] = []
    for pid, start in targets.items():
        if spare.keep.get(pid) == start:
            continue
        proc = table.get(pid)
        same = proc is not None and proc.start == start and not proc.zombie
        if same and proc is not None and proc.command in spare.exempt:
            if spare.own.get(pid) != start:  # the task's own claude is never exempt
                exempted.append((pid, proc.command))
                continue
        remaining[pid] = start
    return remaining, sorted(exempted)


def _terminate(targets: Mapping[int, float], grace_s: float, spare: Spare | None) -> Swept:
    """SIGTERM, wait, SIGKILL; the start time is re-checked before each signal."""
    table = _snapshot()
    protected = _protected(table)
    candidates = {p: s for p, s in targets.items() if p > 1 and p not in protected}
    candidates, exempted = _split(candidates, table, spare or Spare())
    term = _same(candidates)
    _signal(term, signal.SIGTERM)
    survivors = _wait(term, grace_s)
    kill = _same(survivors)  # a pid that exited and was reused meanwhile is left alone
    _signal(kill, signal.SIGKILL)
    _wait(kill, 2.0)
    return Swept(sorted(term), exempted)


def kill_recorded(
    recorded: Mapping[int, float], grace_s: float = 10.0, spare: Spare | None = None
) -> Swept:
    """Kill each recorded (pid, start) that is still that process and is not spared."""
    return _terminate(recorded, grace_s, spare)


def kill_tree(
    pid: int,
    start: float | None,
    task_id: str,
    grace_s: float = 10.0,
    recorded: Mapping[int, float] | None = None,
    spare: Spare | None = None,
) -> Swept:
    """SIGTERM the task's tree, marker processes and recorded survivors; SIGKILL after grace.

    No-op unless ``pid`` is still the process started at ``start``; use ``sweep`` when the
    parent is already gone. ``pid`` is the task's ``claude``: it is never exempt.
    """
    if start is None:
        return Swept([], [])
    table = _snapshot()
    if pid not in _live([pid], table) or table[pid].start != start:
        return Swept([], [])
    targets = {**dict(recorded or {}), **_live(_marker_scan(task_id), table)}
    targets.update(_descendants(pid, table))
    targets[pid] = start
    spare = spare or Spare()
    own = Spare(spare.exempt, spare.keep, {**spare.own, pid: start})
    return _terminate(targets, grace_s, own)


def sweep(
    task_id: str, recorded: Mapping[int, float], grace_s: float = 10.0, spare: Spare | None = None
) -> Swept:
    """Kill marker processes and recorded survivors, parent alive or not (spared ones excepted).

    The engine calls it after an unclean attempt: at its terminal status and before the relaunch.
    """
    targets = {**dict(recorded), **_live(_marker_scan(task_id), _snapshot())}
    return _terminate(targets, grace_s, spare)


def _found(
    task_id: str, recorded: Mapping[int, float], table: Mapping[int, _Proc]
) -> dict[int, float]:
    found = _live(_marker_scan(task_id), table)
    live = _live(recorded, table)
    found.update({pid: start for pid, start in recorded.items() if live.get(pid) == start})
    return found


def task_procs(task_id: str, recorded: Mapping[int, float]) -> dict[int, float]:
    """The task's live processes: marker hits plus recorded (pid, start) still that process."""
    return _found(task_id, recorded, _snapshot())


def survivors(task_id: str, recorded: Mapping[int, float], spare: Spare | None = None) -> list[int]:
    """The task's live processes a sweep would still target (spared ones never count)."""
    table = _snapshot()
    remaining, _exempted = _split(_found(task_id, recorded, table), table, spare or Spare())
    return sorted(remaining)
