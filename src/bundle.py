"""Redacted debug bundle of one task: a ``.tar.gz`` built in a private temp dir.

Members (under ``<task_id>/``): ``run/<path>`` for every run-store file (not the per-attempt
agents files), ``task.json`` (the task's DB rows) and ``server.log`` (this task's server-log
lines). Each file is redacted line by line with its own ``Redactor`` (the rows structurally)
into a temp file before ``tarfile.add``; the caller streams the archive from disk and removes the
temp dir with ``cleanup``.
"""

import asyncio
import fnmatch
import json
import os
import shutil
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from src.database import get_db
from src.redact import Redactor
from src.serverlog import BACKUP_COUNT as BACKUPS
from src.tasks import TERMINAL

PREFIX = "dioxide-bundle-"
# Left out: agents' inline MCP credentials are masked structurally (get_task_debug), not by value.
NOT_BUNDLED = "attempt-*.agents.json"


def _redact_lines(lines: Iterable[bytes], redactor: Redactor, dst: Path) -> None:
    fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        for raw in lines:
            out.write(redactor.redact_line(raw.decode("utf-8", "replace")) + "\n")


def _file_lines(path: Path, task_id: str | None = None) -> Iterable[bytes]:
    """Lines of ``path`` (a trailing partial line included); with ``task_id``, only JSON lines
    whose ``task_id`` matches."""
    with path.open("rb") as fh:
        for raw in fh:
            line = raw.rstrip(b"\n")
            if task_id is not None and not _for_task(line, task_id):
                continue
            yield line


def _for_task(raw: bytes, task_id: str) -> bool:
    try:
        obj = json.loads(raw)
    except ValueError:
        return False
    return isinstance(obj, dict) and obj.get("task_id") == task_id


def build(
    *,
    task_id: str,
    run_dir: Path,
    server_log: Path,
    rows: dict[str, Any],
    redactor: Redactor,
    tmp_root: Path,
) -> tuple[Path, Path]:
    """Write the archive; returns ``(temp dir, archive path)``. The caller removes the dir."""
    tmp = Path(tempfile.mkdtemp(prefix=PREFIX, dir=tmp_root))
    try:
        staged = tmp / "staged"
        staged.mkdir(mode=0o700)
        members: list[tuple[Path, str]] = []
        if run_dir.is_dir():
            for base, _dirs, files in os.walk(run_dir):
                for name in sorted(files):
                    if fnmatch.fnmatchcase(name, NOT_BUNDLED):
                        continue
                    src = Path(base, name)
                    rel = src.relative_to(run_dir)
                    dst = staged / f"{len(members)}.txt"
                    _redact_lines(_file_lines(src), redactor.clone(), dst)
                    members.append((dst, f"run/{redactor.clone().redact_line(str(rel))}"))
        task_json = staged / "task.json"
        task_json.write_text(
            json.dumps(redactor.redact_json(rows), indent=2, ensure_ascii=False, default=str)
        )
        members.append((task_json, "task.json"))
        log = staged / "server.log"
        _redact_lines(_server_lines(server_log, task_id), redactor.clone(), log)
        members.append((log, "server.log"))
        archive = tmp / f"{task_id}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for path, arcname in members:
                tar.add(path, arcname=f"{task_id}/{arcname}", recursive=False)
        return tmp, archive
    except BaseException:
        cleanup(tmp)
        raise


def _server_lines(server_log: Path, task_id: str) -> Iterable[bytes]:
    """The task's lines from the rotated backups (oldest first, ``.5`` .. ``.1``) and the log."""
    backups = [server_log.with_name(f"{server_log.name}.{n}") for n in range(BACKUPS, 0, -1)]
    for path in [*backups, server_log]:
        if path.is_file():
            yield from _file_lines(path, task_id)


def cleanup(tmp: Path) -> None:
    shutil.rmtree(tmp, ignore_errors=True)


def sweep_stale(tmp_root: Path) -> list[str]:
    """Boot: remove bundle temp dirs a previous process left behind (crash, kill)."""
    removed = []
    for entry in sorted(tmp_root.glob(f"{PREFIX}*")):
        if entry.is_dir() and not entry.is_symlink():
            cleanup(entry)
            removed.append(entry.name)
    return removed


_TABLES = ("attempts", "steps", "events", "answers")


async def task_rows(task_id: str) -> dict[str, Any] | None:
    """The task row and its attempts, steps, events and answers; None for an unknown task."""
    db = get_db()
    async with db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cur:
        row = await cur.fetchone()
    if row is None:
        return None
    rows: dict[str, Any] = {"task": dict(row)}
    for table in _TABLES:
        async with db.execute(f"SELECT * FROM {table} WHERE task_id = ?", (task_id,)) as cur:
            rows[table] = [dict(r) for r in await cur.fetchall()]
    return rows


async def task_bundle(
    task_id: str, run_dir: Path, server_log: Path, redactor: Redactor, tmp_root: Path
) -> tuple[Path, Path] | None:
    """Fetch the rows, then build the archive off the event loop; None for an unknown task."""
    rows = await task_rows(task_id)
    if rows is None:
        return None
    if rows["task"]["status"] in TERMINAL and not run_dir.is_dir():
        rows["run_store"] = "pruned"  # retention deleted it: the archive holds rows and log only
    return await asyncio.to_thread(
        lambda: build(
            task_id=task_id, run_dir=run_dir, server_log=server_log, rows=rows,
            redactor=redactor, tmp_root=tmp_root,
        )
    )  # fmt: skip
