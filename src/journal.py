"""Read-only access to the run state Claude Code writes for a workflow.

Layout under ``claude_home`` (verified against Claude Code 2.1.328)::

    projects/<slug(cwd)>/<session_id>.jsonl                       session transcript
    projects/<slug(cwd)>/<session_id>/workflows/<run_id>.json     run file
    projects/<slug(cwd)>/<session_id>/subagents/workflows/<run_id>/journal.jsonl
    projects/<slug(cwd)>/<session_id>/subagents/workflows/<run_id>/agent-<agent_id>.jsonl

Everything here is a synchronous, pure reader; nothing is ever written under ``claude_home``.
"""

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

_SLUG_MAX = 200
_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]")
_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
_RUN_PREFIX = "wf_"
_JOURNAL = "journal.jsonl"
_MISSING = object()
_TAIL_CHUNK = 65536


@dataclass(frozen=True)
class JournalStep:
    """One workflow step (journal label), collapsed over its restarts."""

    key: str
    label: str
    phase: str | None
    status: str  # "completed" | "running"
    result: Any
    started_count: int
    agent_id: str | None


def _utf16_units(text: str) -> list[int]:
    raw = text.encode("utf-16-le")
    return [int.from_bytes(raw[i : i + 2], "little") for i in range(0, len(raw), 2)]


def _base36(n: int) -> str:
    out = ""
    while True:
        n, rem = divmod(n, 36)
        out = _BASE36[rem] + out
        if n == 0:
            return out


def _java_hash(units: list[int]) -> int:
    """Java-style int32 string hash: ``h = (h * 31 + unit) | 0``."""
    h = 0
    for unit in units:
        h = (h * 31 + unit) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def slug(cwd: str) -> str:
    """Project directory name Claude Code derives from a working directory."""
    path = unicodedata.normalize("NFC", os.path.realpath(cwd))
    units = _utf16_units(path)
    # Every UTF-16 code unit outside [a-zA-Z0-9] becomes "-": a non-BMP char gives "--".
    sanitized = "".join(chr(u) if chr(u).isascii() and chr(u).isalnum() else "-" for u in units)
    if len(sanitized) <= _SLUG_MAX:
        return sanitized
    return f"{sanitized[:_SLUG_MAX]}-{_base36(abs(_java_hash(units)))}"


def trusted_transcript_dir(
    transcript_dir: Path | None, session_id: str, run_id: str | None
) -> Path | None:
    """The run directory Claude reported in the Workflow tool result, if it can be trusted: an
    existing ``<session_id>/subagents/workflows/<run_id>`` (path resolved first). None
    otherwise, and the slug rule applies."""
    if transcript_dir is None or not run_id or not session_id:
        return None
    path = Path(os.path.realpath(transcript_dir))
    workflows = path.parent
    shaped = (
        path.name == run_id
        and workflows.name == "workflows"
        and workflows.parent.name == "subagents"
        and workflows.parent.parent.name == session_id
    )
    return path if shaped and path.is_dir() else None


def session_dir(
    cwd: str,
    session_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
    run_id: str | None = None,
) -> Path:
    """Directory holding a session's run state.

    ``transcript_dir`` (with the ``run_id`` it must name) is preferred over the slug rule when
    ``trusted_transcript_dir`` accepts it.
    """
    trusted = trusted_transcript_dir(transcript_dir, session_id, run_id)
    if trusted is not None:
        return trusted.parent.parent.parent
    return claude_home / "projects" / slug(cwd) / session_id


def workflow_run_dir(
    cwd: str, session_id: str, run_id: str, claude_home: Path, transcript_dir: Path | None
) -> Path:
    trusted = trusted_transcript_dir(transcript_dir, session_id, run_id)
    if trusted is not None:
        return trusted
    return session_dir(cwd, session_id, claude_home) / "subagents" / "workflows" / run_id


def find_run_id(cwd: str, session_id: str, claude_home: Path) -> str | None:
    """Newest ``wf_*`` run of a session. Fallback only: the stream normally names the run."""
    base = session_dir(cwd, session_id, claude_home) / "subagents" / "workflows"
    newest: tuple[float, str] | None = None
    try:
        entries = list(base.iterdir())
    except OSError:
        return None
    for entry in entries:
        if not entry.name.startswith(_RUN_PREFIX) or not entry.is_dir():
            continue
        try:
            mtime = max(entry.stat().st_mtime, (entry / _JOURNAL).stat().st_mtime)
        except OSError:
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
        if newest is None or (mtime, entry.name) > newest:
            newest = (mtime, entry.name)
    return newest[1] if newest else None


def _journal_records(path: Path) -> list[dict[str, Any]]:
    """Parsed journal records; skips blank, truncated and malformed lines."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def read_steps(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
) -> list[JournalStep]:
    """One step per label in first-start order; unknown record types are ignored."""
    return steps_in(workflow_run_dir(cwd, session_id, run_id, claude_home, transcript_dir))


def steps_in(run_dir: Path) -> list[JournalStep]:
    """``read_steps`` for the journal in ``run_dir`` (e.g. a run-store copy)."""
    records = _journal_records(run_dir / _JOURNAL)

    results: dict[tuple[str, str], Any] = {}
    for rec in records:
        if rec.get("type") == "result":
            results[(str(rec.get("key")), str(rec.get("agentId")))] = rec.get("result")

    order: list[str] = []
    latest: dict[str, dict[str, Any]] = {}
    counts: dict[str, int] = {}
    for rec in records:
        if rec.get("type") != "started":
            continue
        label = rec.get("label")
        if not isinstance(label, str):
            continue
        if label not in latest:
            order.append(label)
        latest[label] = rec
        counts[label] = counts.get(label, 0) + 1

    steps: list[JournalStep] = []
    for label in order:
        rec = latest[label]
        key = str(rec.get("key"))
        agent_id = rec.get("agentId")
        value = results.get((key, str(agent_id)), _MISSING)
        phase = rec.get("phase")
        steps.append(
            JournalStep(
                key=key,
                label=label,
                phase=phase if isinstance(phase, str) else None,
                status="running" if value is _MISSING else "completed",
                result=None if value is _MISSING else value,
                started_count=counts[label],
                agent_id=agent_id if isinstance(agent_id, str) else None,
            )
        )
    return steps


def read_run(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
) -> dict[str, Any] | None:
    """The parsed ``workflows/<run_id>.json`` run file; None if missing, unparseable or the
    file of another run (its ``runId`` differs)."""
    path = run_file_path(cwd, session_id, run_id, claude_home, transcript_dir)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("runId") != run_id:
        return None
    return data


def run_file_path(
    cwd: str, session_id: str, run_id: str, claude_home: Path, transcript_dir: Path | None = None
) -> Path:
    sdir = session_dir(cwd, session_id, claude_home, transcript_dir, run_id)
    return sdir / "workflows" / f"{run_id}.json"


def _written_since(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None,
    since: float | None,
) -> bool:
    """False when the run file is missing or last written before ``since`` (None: no bound)."""
    if since is None:
        return True
    try:
        mtime = run_file_path(cwd, session_id, run_id, claude_home, transcript_dir).stat()
    except OSError:
        return False
    return mtime.st_mtime >= since


def read_return(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
    written_since: float | None = None,
) -> Any | None:
    """The workflow's return value: the run file's ``result`` once its status is completed.

    ``written_since`` (epoch s): a run file last written before it is stale (e.g. the previous
    attempt's return while a resume did not finish the workflow again) and gives None.
    """
    if not _written_since(cwd, session_id, run_id, claude_home, transcript_dir, written_since):
        return None
    run = read_run(cwd, session_id, run_id, claude_home, transcript_dir)
    if run is None or run.get("status") != "completed":
        return None
    return run.get("result")


def read_failure(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
    written_since: float | None = None,
) -> tuple[str, str] | None:
    """``(status, error text)`` when the run file (written since ``written_since``) says the
    workflow script failed (status ``failed``); else None. The error text is the run file's
    ``error`` (a string, or an object's ``message``), raw."""
    if not _written_since(cwd, session_id, run_id, claude_home, transcript_dir, written_since):
        return None
    run = read_run(cwd, session_id, run_id, claude_home, transcript_dir)
    if run is None or run.get("status") != "failed":
        return None
    error = run.get("error")
    if isinstance(error, dict):
        error = error.get("message")
    return "failed", error if isinstance(error, str) else ""


def read_progress(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    transcript_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Per-agent ``workflowProgress`` entries (label, agentId, state, cached); phases dropped."""
    run = read_run(cwd, session_id, run_id, claude_home, transcript_dir)
    progress = run.get("workflowProgress") if run else None
    if not isinstance(progress, list):
        return []
    return [p for p in progress if isinstance(p, dict) and isinstance(p.get("label"), str)]


def cached_label(
    prev_started_count: int, step: JournalStep, progress: list[dict[str, Any]] | None = None
) -> bool:
    """Whether a resumed run served this step from cache.

    The run file's progress entry is authoritative (``cached: true``). Without an entry, a
    completed step whose ``started_count`` did not grow past the stored value was not re-run.
    """
    for entry in progress or []:
        if entry.get("label") == step.label:
            return entry.get("cached") is True
    return (
        step.status == "completed"
        and prev_started_count > 0
        and step.started_count == prev_started_count
    )


def step_transcript_path(
    cwd: str,
    session_id: str,
    run_id: str,
    claude_home: Path,
    agent_id: str,
    transcript_dir: Path | None = None,
) -> Path:
    run_dir = workflow_run_dir(cwd, session_id, run_id, claude_home, transcript_dir)
    return run_dir / f"agent-{agent_id}.jsonl"


def _record_time(line: bytes) -> float | None:
    """Epoch seconds of a transcript line's ISO ``timestamp``; None if absent or unparseable."""
    try:
        record = json.loads(line)
        stamp = record["timestamp"]
        return datetime.fromisoformat(stamp).timestamp() if isinstance(stamp, str) else None
    except (ValueError, KeyError, TypeError):
        return None


def _last_line(handle: BinaryIO) -> bytes:
    """The last non-blank line of a binary file, read backwards in chunks."""
    end = handle.seek(0, os.SEEK_END)
    data = b""
    while end > 0:
        start = max(0, end - _TAIL_CHUNK)
        handle.seek(start)
        data = handle.read(end - start) + data
        end = start
        body = data.rstrip()
        if b"\n" in body or end == 0:
            return body.rsplit(b"\n", 1)[-1]
    return b""


def step_times(transcript: Path) -> tuple[float | None, float | None]:
    """Real start and end of a step: the ``timestamp`` of its transcript's first and last record
    (epoch seconds). A part that is missing, truncated or unparseable is None."""
    try:
        with transcript.open("rb") as handle:
            first = handle.readline()
            last = _last_line(handle)
    except OSError:
        return None, None
    return _record_time(first), _record_time(last)


@dataclass(frozen=True)
class StageModelUsage:
    """Billed tokens one stage agent spent on one model, from its transcript."""

    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_5m_tokens: int
    cache_write_1h_tokens: int
    turns: int


def _count(usage: dict[str, Any], key: str) -> int:
    value = usage.get(key)
    return value if isinstance(value, int) and value >= 0 else 0


def _turn_usage(usage: dict[str, Any]) -> tuple[int, int, int, int, int]:
    """(input, output, cache read, cache write 5m, cache write 1h) of one API turn; without the
    ``cache_creation`` split every cache write counts as 5-minute."""
    written = _count(usage, "cache_creation_input_tokens")
    split = usage.get("cache_creation")
    if isinstance(split, dict):
        five, hour = (
            _count(split, "ephemeral_5m_input_tokens"),
            _count(split, "ephemeral_1h_input_tokens"),
        )
    else:
        five, hour = written, 0
    return (
        _count(usage, "input_tokens"), _count(usage, "output_tokens"),
        _count(usage, "cache_read_input_tokens"), five, hour,
    )  # fmt: skip


def step_usage(transcript: Path, agent_id: str | None = None) -> dict[str, StageModelUsage] | None:
    """Billed usage per ``message.model`` of a stage transcript: each assistant turn once (the
    transcript repeats a turn per content block, with the same ``message.id``; the last record
    wins), turns = distinct ids. None when the transcript is unreadable, or belongs to another
    agent than ``agent_id`` (a label-keyed run-store copy can still hold a restarted step's
    previous agent); unparseable lines are skipped."""
    turns: dict[str, tuple[str, tuple[int, int, int, int, int]]] = {}
    try:
        with transcript.open("rb") as handle:
            for line in handle:
                if b'"assistant"' not in line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                message = record.get("message") if isinstance(record, dict) else None
                if record.get("type") != "assistant" or not isinstance(message, dict):
                    continue
                owner = record.get("agentId")
                if agent_id is not None and isinstance(owner, str) and owner != agent_id:
                    return None
                msg_id, model, usage = message.get("id"), message.get("model"), message.get("usage")
                if isinstance(msg_id, str) and isinstance(model, str) and isinstance(usage, dict):
                    turns[msg_id] = (model, _turn_usage(usage))
    except OSError:
        return None
    sums: dict[str, list[int]] = {}
    for model, counts in turns.values():
        total = sums.setdefault(model, [0, 0, 0, 0, 0, 0])
        for i, value in enumerate(counts):
            total[i] += value
        total[5] += 1
    return {model: StageModelUsage(*values) for model, values in sorted(sums.items())}


def step_source(
    cwd: str,
    session_id: str | None,
    run_id: str | None,
    claude_home: Path,
    agent_id: str | None,
    copy: Path,
    transcript_dir: Path | None = None,
) -> Path:
    """The transcript a step's times and ``timing`` come from: the live one when it exists, else
    the run-store ``copy`` (also when the run, session or agent is not known). The copy is keyed
    by label, so after a step restart it can still hold the previous agent; hence live first."""
    if session_id and run_id and agent_id:
        live = step_transcript_path(cwd, session_id, run_id, claude_home, agent_id, transcript_dir)
        if live.is_file():
            return live
    return copy


def step_timing(source: Path) -> str:
    """``measured`` when the step's transcript exists and its start timestamp parses (its times
    are real), else ``observed`` (the mirror's observation time)."""
    return "measured" if step_times(source)[0] is not None else "observed"


def last_activity(cwd: str, session_id: str, claude_home: Path) -> float | None:
    """Newest mtime of any file under the session dir, or of its sibling transcript."""
    sdir = session_dir(cwd, session_id, claude_home)
    newest: float | None = None
    paths = [sdir.with_name(f"{sdir.name}.jsonl")]
    for root, _dirs, files in os.walk(sdir):
        paths.extend(Path(root, name) for name in files)
    for path in paths:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if newest is None or mtime > newest:
            newest = mtime
    return newest
