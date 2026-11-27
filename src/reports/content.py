"""The Content family: text behind the content scope (and admin), always redacted by the route.

Sources: the task row (description, input context, args); the attempt streams (the workflow
script the parent launched, ``system/task_started.prompt``); the attempt agent snapshots
(``attempt-N.agents.json``); the workflow run through ``RunLocator`` (live while running, the
run-store copy afterwards: stage results from the journal, previews from the run file's
progress, stage transcripts). A file retention removed is ``PRUNED_FIELD`` for that field.
"""

import asyncio
import contextlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import aiosqlite

from src import debug
from src.engine import TaskEngine
from src.reports import events
from src.reports.entities import NotFound, resolve_agent
from src.reports.query import Filters, QueryError
from src.runlocator import RunLocator, load_view

PRUNED_FIELD: dict[str, Any] = {"available": False, "reason": "pruned"}


def _gone(pruned: bool) -> Any:
    """What a missing source reads as: the pruned marker when retention removed the run
    store, else null (not written yet, or never)."""
    return PRUNED_FIELD if pruned else None


async def _view(db: aiosqlite.Connection, task_id: str) -> Any:
    view = await load_view(task_id, db)
    if view is None:
        raise NotFound(f"task {task_id} not found")
    return view


async def task_events(
    db: aiosqlite.Connection, task_id: str, types: list[str] | None, params: Mapping[str, str]
) -> dict[str, Any]:
    """Every event of the task with its full data, paged by id (no window)."""
    async with db.execute("SELECT 1 FROM tasks WHERE id = ?", (task_id,)) as cur:
        if await cur.fetchone() is None:
            raise NotFound(f"task {task_id} not found")
    return await events.events(db, Filters(task=task_id), types, params, full=True)


async def agent_prompt(engine: TaskEngine, name: str) -> dict[str, Any]:
    await engine.refresh_agents()
    slug = resolve_agent(engine, name)
    agent = engine.agents.get(slug)
    if agent is None:
        raise NotFound(f"agent {name} not found")
    return {
        "id": slug, "model": agent.model, "effort": agent.effort, "tools": agent.tools,
        "disallowed_tools": agent.disallowed_tools, "system_prompt": agent.prompt,
    }  # fmt: skip


def _json_file(path: Path, pruned: bool) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return _gone(pruned)


def _workflow_script(paths: list[Path], pruned: bool) -> Any:
    """The workflow script of the newest attempt that launched one (its stream's
    ``system/task_started`` record), read line by line."""
    for path in paths:
        try:
            with path.open(encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    if '"task_started"' not in raw:
                        continue
                    with contextlib.suppress(ValueError):
                        record = json.loads(raw)
                        if record.get("type") == "system" and record.get("subtype") == (
                            "task_started"
                        ):
                            prompt = record.get("prompt")
                            if isinstance(prompt, str):
                                return prompt
        except OSError:
            continue
    return _gone(pruned)


def _first_prompt(path: Path, pruned: bool) -> Any:
    """The first ``user`` record's text of a stage transcript; ``_gone`` when missing."""
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                try:
                    record = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(record, dict) or record.get("type") != "user":
                    continue
                content = (record.get("message") or {}).get("content")
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    return "\n".join(
                        item.get("text", "") for item in content
                        if isinstance(item, dict) and item.get("type") == "text"
                    )  # fmt: skip
                return None
    except OSError:
        return _gone(pruned)
    return None


def _loads(raw: str | None) -> Any:
    try:
        return json.loads(raw) if raw else None
    except ValueError:
        return None


async def task_prompts(
    engine: TaskEngine, db: aiosqlite.Connection, task_id: str
) -> dict[str, Any]:
    view = await _view(db, task_id)
    task = view.task
    numbers = [a["n"] for a in view.attempts]

    def files() -> dict[str, Any]:
        store = engine.runstore
        pruned = debug.pruned(engine, view)
        script = None
        if task.is_lifecycle:
            streams = [store.attempt_paths(task.id, n)[0] for n in reversed(numbers)]
            script = _workflow_script(streams, pruned)
        agents = [
            {"n": n, "definitions": _json_file(store.attempt_agents_path(task.id, n), pruned)}
            for n in numbers
        ]
        stages = []
        if task.is_lifecycle:
            run = RunLocator(engine, view)
            previews = {p["label"]: p.get("promptPreview") for p in run.progress()}
            # The stages the database knows, then any only the live run lists yet.
            labels = [s["label"] for s in sorted(view.steps, key=_started)]
            labels += [label for label in previews if label not in labels]
            for label in labels:
                stages.append({
                    "label": label,
                    "prompt_preview": previews[label] if label in previews else _gone(pruned),
                    "prompt": _first_prompt(run.step_path(label), pruned),
                })  # fmt: skip
        return {"workflow_script": script, "agents": agents, "stages": stages}

    found = await asyncio.to_thread(files)
    return {
        "task": task.id, "description": task.description,
        "input_context": _loads(task.input_context), "args": _loads(task.args_json),
        **found,
    }  # fmt: skip


def _started(step: Mapping[str, Any]) -> tuple[float, str]:
    return (step["started_at"] or 0.0, step["label"])


def _known_label(view: Any, run: RunLocator, label: str) -> bool:
    return any(s["label"] == label for s in view.steps) or any(
        s.label == label for s in run.steps()
    )


async def stage_result(
    engine: TaskEngine, db: aiosqlite.Connection, task_id: str, label: str
) -> dict[str, Any]:
    view = await _view(db, task_id)

    def read() -> dict[str, Any] | None:
        run = RunLocator(engine, view)
        if not _known_label(view, run, label):
            return None
        pruned = debug.pruned(engine, view)
        step = next((s for s in run.steps() if s.label == label), None)
        entry = next((p for p in run.progress() if p["label"] == label), None)
        preview = entry.get("resultPreview") if entry is not None else None
        result = step.result if step is not None else None
        return {
            "label": label,
            "result_preview": PRUNED_FIELD if pruned and entry is None else preview,
            "result": PRUNED_FIELD if pruned and step is None else result,
        }

    found = await asyncio.to_thread(read)
    if found is None:
        raise NotFound(f"stage {label} of task {task_id} not found")
    return found


async def stage_transcript(
    engine: TaskEngine,
    db: aiosqlite.Connection,
    task_id: str,
    label: str,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    """The stage transcript as the logs pager serves it (source ``step:<label>``). A stage the
    journal does not hold (pruned, or not yet started there) has no entries: ``transcript`` is
    the pruned marker or null."""
    view = await _view(db, task_id)

    def locate() -> tuple[bool, bool]:  # streams are parsed here: off the event loop
        run = RunLocator(engine, view)
        return _known_label(view, run, label), any(s.label == label for s in run.steps())

    known, journaled = await asyncio.to_thread(locate)
    if not known:
        raise NotFound(f"stage {label} of task {task_id} not found")
    if not journaled:
        pruned = debug.pruned(engine, view)
        return {"entries": [], "next_cursor": None, "done": view.terminal,
                "transcript": _gone(pruned)}  # fmt: skip
    page: dict[str, Any] = await debug.task_logs(
        engine, task_id, f"step:{label}", cursor, limit, db=db
    )
    if "error" in page and page.get("status") != "not_found":
        raise QueryError(str(page["error"]))
    return page
