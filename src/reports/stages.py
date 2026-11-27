"""A lifecycle task's stages (``/tasks/{id}/stages``) and its workflow graph
(``/tasks/{id}/graph``, in the field names of Grafana's node graph panel)."""

import re
from collections.abc import Mapping
from typing import Any

import aiosqlite

from src.engine_lifecycle import served_step_status
from src.engine_read import iso_or_none
from src.reports.entities import NotFound, page
from src.stages import normalize_stage

_ATTEMPT = re.compile(r"^(?P<base>.*)@a(?P<n>\d+)$")


async def _steps(db: aiosqlite.Connection, task_id: str) -> tuple[str, list[aiosqlite.Row]]:
    async with db.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)) as cur:
        task = await cur.fetchone()
    if task is None:
        raise NotFound(f"task {task_id} not found")
    async with db.execute(
        "SELECT label, phase, status, started_count, cached, started_at, finished_at, "
        "agent_type, model, stage_tokens, tool_calls, last_tool, duration_s "
        "FROM steps WHERE task_id = ? ORDER BY started_at, rowid",
        (task_id,),
    ) as cur:
        return task["status"], list(await cur.fetchall())


async def stages(
    db: aiosqlite.Connection, task_id: str, params: Mapping[str, str]
) -> dict[str, Any]:
    task_status, found = await _steps(db, task_id)
    rows = [
        {
            "i": i, "label": r["label"], "stage": normalize_stage(r["label"]), "phase": r["phase"],
            "status": served_step_status(r["status"], task_status),
            "stage_agent": r["agent_type"], "model": r["model"],
            "started_count": r["started_count"], "cached": bool(r["cached"]),
            "started_at": iso_or_none(r["started_at"]),
            "finished_at": iso_or_none(r["finished_at"]), "duration_s": r["duration_s"],
            "stage_tokens": r["stage_tokens"], "tool_calls": r["tool_calls"],
            "last_tool": r["last_tool"],
        }
        for i, r in enumerate(found)
    ]  # fmt: skip
    body = page(rows, "i", int, params)
    for row in body["rows"]:
        del row["i"]
    return body


def _edge(source: str, target: str, kind: str) -> dict[str, str]:
    return {"id": f"{source}->{target}", "source": source, "target": target, "mainStat": kind}


async def graph(db: aiosqlite.Connection, task_id: str) -> dict[str, Any]:
    """Nodes are steps; ``next`` edges join every stage of a phase run (consecutive stages of
    one phase, by start) to every stage of the next run; ``retry`` edges join ``X@aN`` to
    ``X@aN+1`` (a label without the suffix is the first run: ``X`` → ``X@a1``)."""
    task_status, found = await _steps(db, task_id)
    nodes = [
        {
            "id": r["label"], "title": normalize_stage(r["label"]),
            "subTitle": r["agent_type"] or "",
            "mainStat": served_step_status(r["status"], task_status),
            "secondaryStat": f"{r['duration_s']:g}s" if r["duration_s"] is not None else "",
            "phase": r["phase"],
        }
        for r in found
    ]  # fmt: skip
    # Runs of consecutive stages of one phase, in start order: a phase that comes back (a
    # second review round after a fix) is a new run.
    groups: list[list[str]] = []
    previous: object = object()
    for r in found:
        if r["phase"] != previous:
            groups.append([])
            previous = r["phase"]
        groups[-1].append(r["label"])
    edges = [
        _edge(source, target, "next")
        for earlier, later in zip(groups, groups[1:], strict=False)
        for source in earlier
        for target in later
    ]
    labels = {r["label"] for r in found}
    for label in [r["label"] for r in found]:
        m = _ATTEMPT.match(label)
        base, n = (m["base"], int(m["n"])) if m is not None else (label, 0)
        if (following := f"{base}@a{n + 1}") in labels:
            edges.append(_edge(label, following, "retry"))
    return {"nodes": nodes, "edges": edges}
