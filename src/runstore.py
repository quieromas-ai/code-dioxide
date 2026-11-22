"""Per-task run directory: ``<runs_dir>/<task_id>/``.

Layout::

    inputs/brief.md, inputs/plan.json, inputs/answers/<k>.md   read-only (0444, dirs 0555)
    superseded/plan-<n>.json  a pinned plan a fresh start retired (re-planning run)
    script/...                write-once snapshot (0444, executables 0555, dirs 0555)
    out/merge-subject.txt     written by the merge stage; out/ (0700) is the only run dir the
                              stages may write (passed with --add-dir; inputs/, script/ sealed)
    attempt-<n>.stream.jsonl, attempt-<n>.stderr               claude stdout / stderr
    journal.jsonl, run.json, steps/<label>.jsonl               copies of Claude's run state
    events.jsonl                                               engine event log
    history/<run id>/  a run a fresh start replaced: its journal.jsonl, run.json, steps/, the
                       answers/ it was given, its out/ files and steps.json (its step rows)
    procs.json        recorded descendants {pid: start} since the last clean exit (then reset)
    kept.json         processes clean exits left running {pid: {start, n}} (n = that attempt)
"""

import fnmatch
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import journal

DIR_MODE = 0o700
FILE_MODE = 0o600
READ_ONLY = 0o444
READ_EXEC = 0o555
SEALED_DIR = 0o555  # entries can be neither added, removed nor renamed
_UNSAFE_LABEL = re.compile(r"[^A-Za-z0-9._-]")


@dataclass(frozen=True)
class RunPaths:
    run_dir: Path
    inputs_dir: Path
    script_dir: Path
    brief_path: Path
    out_dir: Path


def render_brief(description: str, input_context: dict[str, Any]) -> str:
    """The task brief: the description followed by the input context as a fenced JSON block."""
    context = json.dumps(input_context, indent=2, sort_keys=True, ensure_ascii=False)
    return f"{description}\n\n```json\n{context}\n```\n"


def _label_filename(label: str) -> str:
    """Readable file stem for a step label; a short hash keeps sanitised labels distinct."""
    name = _UNSAFE_LABEL.sub("_", label).lstrip(".")
    if name == label:
        return name
    digest = hashlib.sha256(label.encode()).hexdigest()[:8]
    return f"{name or '_'}-{digest}"


def _mkdir(path: Path) -> None:
    path.mkdir(mode=DIR_MODE, parents=True, exist_ok=True)
    os.chmod(path, DIR_MODE)


def write_atomic(path: Path, data: bytes, mode: int) -> None:
    """Write via a temp file + rename, so a read-only target can be replaced."""
    _mkdir(path.parent)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _seal(root: Path) -> None:
    """Make every directory under ``root`` (inclusive) read-only; deepest first."""
    for base, _dirs, _files in sorted(os.walk(root), key=lambda w: -len(Path(w[0]).parts)):
        os.chmod(base, SEALED_DIR)


def _unseal(root: Path) -> None:
    """Writable again (owner only) so the engine itself can add a file; top first."""
    if not root.is_dir():
        return
    os.chmod(root, DIR_MODE)
    for base, dirs, _files in os.walk(root):
        for name in dirs:
            os.chmod(Path(base, name), DIR_MODE)


def _rmtree(path: Path, ignore_errors: bool = False) -> None:
    """``shutil.rmtree`` that also removes sealed (read-only) directories."""

    def retry(func: Any, failed: str, exc: BaseException) -> None:
        parent = Path(failed).parent
        try:
            os.chmod(parent, DIR_MODE)
            if Path(failed).is_dir() and not Path(failed).is_symlink():
                os.chmod(failed, DIR_MODE)
                shutil.rmtree(failed, onexc=retry)
            else:
                func(failed)
        except OSError:
            if not ignore_errors:
                raise exc from None

    shutil.rmtree(path, onexc=retry)


def _copy_if_changed(src: Path, dst: Path) -> bool:
    try:
        st = src.stat()
    except OSError:
        return False
    try:
        dt = dst.stat()
        if dt.st_size == st.st_size and dt.st_mtime >= st.st_mtime:
            return False
    except OSError:
        pass
    write_atomic(dst, src.read_bytes(), FILE_MODE)
    os.utime(dst, (st.st_atime, st.st_mtime))
    return True


def _newest_mtime(root: Path) -> float:
    newest = root.stat().st_mtime
    for base, dirs, files in os.walk(root):
        for name in dirs + files:
            try:
                newest = max(newest, Path(base, name).lstat().st_mtime)
            except OSError:
                continue
    return newest


class RunStore:
    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = runs_dir

    def task_dir(self, task_id: str) -> Path:
        if not task_id or task_id in (".", "..") or "/" in task_id or os.sep in task_id:
            raise ValueError(f"invalid task id for a run directory: {task_id!r}")
        return self.runs_dir / task_id

    def paths(self, task_id: str) -> RunPaths:
        run_dir = self.task_dir(task_id)
        inputs = run_dir / "inputs"
        return RunPaths(
            run_dir=run_dir,
            inputs_dir=inputs,
            script_dir=run_dir / "script",
            brief_path=inputs / "brief.md",
            out_dir=run_dir / "out",
        )

    def _ensure(self, task_id: str) -> Path:
        _mkdir(self.runs_dir)
        run_dir = self.task_dir(task_id)
        _mkdir(run_dir)
        return run_dir

    def write_inputs(self, task_id: str, brief: str | None, pinned_plan: Any = None) -> None:
        """``brief.md`` (None: the existing one is kept) and, when given, the pinned plan as
        ``plan.json``; the inputs directory is sealed afterwards (only the engine adds to it)."""
        self._ensure(task_id)
        paths = self.paths(task_id)
        _unseal(paths.inputs_dir)
        try:
            if brief is not None:
                write_atomic(paths.brief_path, brief.encode(), READ_ONLY)
            if pinned_plan is not None:
                plan = json.dumps(pinned_plan, indent=2, sort_keys=True, ensure_ascii=False)
                write_atomic(paths.inputs_dir / "plan.json", plan.encode(), READ_ONLY)
        finally:
            _seal(paths.inputs_dir)

    def retire_plan(self, task_id: str, n: int) -> Path | None:
        """Move a pinned ``inputs/plan.json`` that is no longer current out of ``inputs/`` (no
        stage can read it as the approved plan) to ``superseded/plan-<n>.json`` (history).
        Returns the new path, or None when there was no pinned plan."""
        paths = self.paths(task_id)
        plan = paths.inputs_dir / "plan.json"
        if not plan.exists():
            return None
        target = paths.run_dir / "superseded" / f"plan-{int(n)}.json"
        k = 1
        while target.exists():  # the same attempt retired a plan before: keep both
            k += 1
            target = target.with_name(f"plan-{int(n)}-{k}.json")
        _mkdir(target.parent)
        _unseal(paths.inputs_dir)
        try:
            os.replace(plan, target)
        finally:
            _seal(paths.inputs_dir)
        return target

    def archive_run(
        self, task_id: str, run_id: str, step_rows: list[dict[str, Any]]
    ) -> Path | None:
        """Before a fresh start: move the previous run's copies (``journal.jsonl``, ``run.json``,
        ``steps/``), its answers (``inputs/answers/``) and its ``out/`` files to
        ``history/<run_id>/`` with ``steps.json`` (its step rows), so the new run neither
        overwrites them nor reads them. Returns the history directory, or None when there was
        nothing to move."""
        paths = self.paths(task_id)
        run_dir = paths.run_dir
        answers = paths.inputs_dir / "answers"
        moves = [(run_dir / name, name) for name in ("journal.jsonl", "run.json", "steps")]
        moves = [(src, name) for src, name in moves if src.exists()]
        outs = sorted(paths.out_dir.iterdir()) if paths.out_dir.is_dir() else []
        if not (moves or outs or step_rows or answers.exists()):
            return None
        target = run_dir / "history" / _label_filename(run_id or "unknown")
        k = 1
        while target.exists():  # the same run archived before (a refused fresh start): keep both
            k += 1
            target = target.with_name(f"{_label_filename(run_id or 'unknown')}-{k}")
        _mkdir(target)
        for src, name in moves:
            os.replace(src, target / name)
        if outs:
            _mkdir(target / "out")
            for src in outs:
                os.replace(src, target / "out" / src.name)
        if answers.exists():
            _unseal(paths.inputs_dir)
            try:
                os.replace(answers, target / "answers")
            finally:
                _seal(paths.inputs_dir)
        rows = json.dumps(step_rows, indent=2, sort_keys=True, ensure_ascii=False, default=str)
        write_atomic(target / "steps.json", rows.encode(), FILE_MODE)
        return target

    def copied_run_id(self, task_id: str) -> str | None:
        """The run id of the run-store copy (``run.json``), or None when missing/unreadable."""
        try:
            data = json.loads((self.task_dir(task_id) / "run.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        run_id = data.get("runId") if isinstance(data, dict) else None
        return run_id if isinstance(run_id, str) else None

    def ensure_out(self, task_id: str) -> Path:
        """``out/``, writable (0700) by the owner: where stages write (the merge subject).
        Idempotent; what is already in it is kept."""
        self._ensure(task_id)
        out = self.paths(task_id).out_dir
        _mkdir(out)
        return out

    def write_answer(self, task_id: str, k: int, text: str) -> Path:
        self._ensure(task_id)
        inputs = self.paths(task_id).inputs_dir
        path = inputs / "answers" / f"{int(k)}.md"
        _unseal(inputs)
        try:
            write_atomic(path, text.encode(), READ_ONLY)
        finally:
            _seal(inputs)
        return path

    def snapshot_script(
        self, task_id: str, src_dir: Path, executables: tuple[str, ...] = ()
    ) -> Path:
        """Copy ``src_dir`` to ``script/`` once; later calls leave the snapshot untouched.

        Files become read-only, directories sealed. Files whose path relative to ``src_dir``
        matches one of ``executables`` (glob) are made executable (0555) whatever their mode
        in ``src_dir`` (an installed package may not keep it)."""
        run_dir = self._ensure(task_id)
        target = self.paths(task_id).script_dir
        if target.exists():
            return target
        staging = Path(tempfile.mkdtemp(dir=run_dir, prefix=".script."))
        try:
            for base, _dirs, files in os.walk(src_dir):
                rel = Path(base).relative_to(src_dir)
                _mkdir(staging / rel)
                for name in files:
                    src = Path(base, name)
                    dst = staging / rel / name
                    shutil.copyfile(src, dst)
                    relative = (rel / name).as_posix()
                    executable = any(fnmatch.fnmatchcase(relative, g) for g in executables)
                    os.chmod(dst, READ_EXEC if executable else READ_ONLY)
            os.chmod(staging, DIR_MODE)
            os.rename(staging, target)
            _seal(target)
        except BaseException:
            _rmtree(staging, ignore_errors=True)
            raise
        return target

    def attempt_paths(self, task_id: str, n: int) -> tuple[Path, Path]:
        run_dir = self.task_dir(task_id)
        return run_dir / f"attempt-{n}.stream.jsonl", run_dir / f"attempt-{n}.stderr"

    def attempt_agents_path(self, task_id: str, n: int) -> Path:
        """The ``--agents`` file attempt ``n`` ran with; in the run dir, so retention removes it."""
        return self.task_dir(task_id) / f"attempt-{n}.agents.json"

    def step_copy_path(self, task_id: str, label: str) -> Path:
        """Where a step's transcript copy lives: ``steps/<sanitised label>.jsonl``."""
        return self.task_dir(task_id) / "steps" / f"{_label_filename(label)}.jsonl"

    def copy_transcripts(
        self,
        task_id: str,
        cwd: str,
        session_id: str,
        run_id: str,
        claude_home: Path,
        transcript_dir: Path | None = None,
        include_running: bool = True,
    ) -> list[Path]:
        """Copy the journal, run file and step transcripts; returns the files (re)copied.

        ``include_running=False`` leaves out the transcripts of steps still running (they are
        read live)."""
        run_dir = journal.workflow_run_dir(cwd, session_id, run_id, claude_home, transcript_dir)
        if not run_dir.is_dir():
            return []
        dest = self._ensure(task_id)
        run_file = journal.run_file_path(cwd, session_id, run_id, claude_home, transcript_dir)
        pairs = [
            (run_dir / "journal.jsonl", dest / "journal.jsonl"),
            (run_file, dest / "run.json"),
        ]
        for step in journal.read_steps(cwd, session_id, run_id, claude_home, transcript_dir):
            if step.agent_id is None or (not include_running and step.status != "completed"):
                continue
            src = journal.step_transcript_path(
                cwd, session_id, run_id, claude_home, step.agent_id, transcript_dir
            )
            pairs.append((src, self.step_copy_path(task_id, step.label)))
        return [dst for src, dst in pairs if _copy_if_changed(src, dst)]

    def read_procs(self, task_id: str) -> dict[int, float]:
        """The recorded descendant set ``{pid: start}``; empty when missing or unreadable."""
        return self._read_pids(task_id, "procs.json")

    def read_kept(self, task_id: str) -> dict[int, tuple[float, int]]:
        """Processes clean exits left running ``{pid: (start, attempt n)}``; empty when missing
        or unreadable (an invalid entry is dropped)."""
        try:
            raw = json.loads((self.task_dir(task_id) / "kept.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(raw, dict):
            return {}
        out: dict[int, tuple[float, int]] = {}
        for pid, entry in raw.items():
            if not (isinstance(entry, dict) and pid.isdigit() and int(pid) > 1):
                continue
            start, n = entry.get("start"), entry.get("n")
            if isinstance(start, bool) or isinstance(n, bool):
                continue
            if isinstance(start, int | float) and isinstance(n, int):
                out[int(pid)] = (float(start), n)
        return out

    def keep_procs(self, task_id: str, kept: Mapping[int, tuple[float, int]]) -> None:
        """After a committed clean exit: ``kept.json`` becomes ``kept``, the recorded set restarts
        empty."""
        data = {str(pid): {"start": s, "n": n} for pid, (s, n) in sorted(kept.items()) if pid > 1}
        write_atomic(self._ensure(task_id) / "kept.json", json.dumps(data).encode(), FILE_MODE)
        self._write_pids(task_id, "procs.json", {})

    def _read_pids(self, task_id: str, name: str) -> dict[int, float]:
        try:
            raw = json.loads((self.task_dir(task_id) / name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(raw, dict):
            return {}
        out: dict[int, float] = {}
        for pid, start in raw.items():
            valid_start = isinstance(start, int | float) and not isinstance(start, bool)
            if pid.isdigit() and int(pid) > 1 and valid_start:
                out[int(pid)] = float(start)
        return out

    def write_procs(self, task_id: str, recorded: Mapping[int, float]) -> None:
        """Union ``recorded`` into ``procs.json`` (0600), keeping entries from earlier writes."""
        merged = self.read_procs(task_id)
        merged.update(recorded)
        self._write_pids(task_id, "procs.json", merged)

    def _write_pids(self, task_id: str, name: str, pids: Mapping[int, float]) -> None:
        valid = {int(pid): float(start) for pid, start in pids.items() if pid > 1}
        data = json.dumps({str(pid): valid[pid] for pid in sorted(valid)})
        write_atomic(self._ensure(task_id) / name, data.encode(), FILE_MODE)

    def remove(self, task_id: str) -> None:
        """Delete the task's run directory (a submit that never inserted its row)."""
        _rmtree(self.task_dir(task_id), ignore_errors=True)

    def append_event(self, task_id: str, type: str, data: dict[str, Any]) -> None:
        path = self._ensure(task_id) / "events.jsonl"
        line = json.dumps({"at": time.time(), "type": type, "data": data}, ensure_ascii=False)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, FILE_MODE)
        with os.fdopen(fd, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def prune(self, days: float, keep: Collection[str], now: float | None = None) -> list[str]:
        """Delete task dirs whose newest mtime is older than ``days``; returns their ids.

        ``keep``: the ids of every task that is not terminal (a ``needs_human`` task may wait
        longer than the retention and still needs its ``script/`` and ``inputs/``); their dirs
        are never deleted, whatever their age."""
        cutoff = (time.time() if now is None else now) - days * 86400
        removed: list[str] = []
        try:
            entries = sorted(self.runs_dir.iterdir())
        except OSError:
            return removed
        for entry in entries:
            if not entry.is_dir() or entry.is_symlink() or entry.name in keep:
                continue
            try:
                stale = _newest_mtime(entry) < cutoff
            except OSError:
                continue
            if stale:
                _rmtree(entry)
                if not entry.exists():
                    removed.append(entry.name)
        return removed
