"""Where a task's run state lives: its DB view, the workflow run, live files or run-store copies.

While a task runs, the journal and step transcripts are read live under ``claude_home``; once
copied (attempt end), from the run store. Both are byte copies, so log offsets stay valid.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src import journal
from src.database import get_db
from src.engine import TaskEngine
from src.stream import StreamWatcher, WorkflowAck
from src.tasks import WAIT_ENDS, Task


@dataclass(frozen=True)
class TaskView:
    task: Task
    attempts: list[dict[str, Any]]
    events: list[dict[str, Any]]
    steps: list[dict[str, Any]] = field(default_factory=list)  # the steps table (mirror)

    @property
    def terminal(self) -> bool:
        """Nothing more is written: a terminal task, or one waiting in needs_human (until it
        is answered and resumed)."""
        return self.task.status in WAIT_ENDS

    @property
    def final(self) -> bool:
        """The task's own files are complete: terminal AND every attempt has ended (a cancelled
        task's process may still write during the kill grace)."""
        return self.terminal and all(a["ended_at"] is not None for a in self.attempts)


async def task_exists(task_id: str) -> bool:
    async with get_db().execute("SELECT 1 FROM tasks WHERE id = ?", (task_id,)) as cur:
        return await cur.fetchone() is not None


async def load_view(task_id: str) -> TaskView | None:
    db = get_db()
    async with db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cur:
        row = await cur.fetchone()
    if row is None:
        return None
    async with db.execute("SELECT * FROM attempts WHERE task_id = ? ORDER BY n", (task_id,)) as cur:
        attempts = [dict(r) for r in await cur.fetchall()]
    async with db.execute(
        "SELECT id, type, data_json, at FROM events WHERE task_id = ? ORDER BY id", (task_id,)
    ) as cur:
        events = [dict(r) for r in await cur.fetchall()]
    async with db.execute("SELECT * FROM steps WHERE task_id = ?", (task_id,)) as cur:
        steps = [dict(r) for r in await cur.fetchall()]
    return TaskView(Task.from_row(row), attempts, events, steps)


def event_json(event: dict[str, Any]) -> dict[str, Any]:
    data = json.loads(event["data_json"]) if event["data_json"] else {}
    return {"at": event["at"], "type": event["type"], "data": data}


@dataclass(frozen=True)
class RunRef:
    run_id: str
    transcript_dir: Path | None


def stream_events(path: Path) -> list[Any]:
    return StreamWatcher(path).poll()


def run_ref(engine: TaskEngine, view: TaskView) -> RunRef | None:
    """The workflow run: the task row's ``run_id``, else the newest stream's launch ack."""
    task = view.task
    for attempt in reversed(view.attempts):
        stream, _ = engine.runstore.attempt_paths(task.id, attempt["n"])
        acks = [e for e in stream_events(stream) if isinstance(e, WorkflowAck) and e.run_id]
        if acks and (task.run_id is None or acks[-1].run_id == task.run_id):
            tdir = acks[-1].transcript_dir
            return RunRef(acks[-1].run_id or "", Path(tdir) if tdir else None)
    return RunRef(task.run_id, None) if task.run_id else None


class RunLocator:
    """Where a task's journal and step transcripts are: live under ``claude_home``, or copied."""

    def __init__(self, engine: TaskEngine, view: TaskView, ref: RunRef | None = None) -> None:
        """``ref``: an already resolved run (e.g. cached by a follower); otherwise resolved here
        from the attempt streams."""
        self.engine, self.view = engine, view
        self.task = view.task
        self.copy_dir = engine.runstore.task_dir(self.task.id)
        self.ref = ref if ref is not None else run_ref(engine, view)
        self._steps: list[journal.JournalStep] | None = None  # parsed once per locator (page)

    def live_dir(self) -> Path | None:
        if self.ref is None or not self.task.session_id:
            return None
        return journal.workflow_run_dir(
            self.task.cwd or ".", self.task.session_id, self.ref.run_id,
            self.engine.settings.claude_home, self.ref.transcript_dir,
        )  # fmt: skip

    def use_live(self) -> bool:
        """Live while the task runs (or before anything was copied); the copy afterwards."""
        live = self.live_dir()
        copied = (self.copy_dir / "journal.jsonl").is_file()
        if live is None:
            return False
        return not copied or (not self.view.terminal and (live / "journal.jsonl").is_file())

    def journal_path(self) -> Path | None:
        live = self.live_dir()
        if self.use_live() and live is not None:
            return live / "journal.jsonl"
        copy = self.copy_dir / "journal.jsonl"
        return copy if copy.is_file() else None

    def steps(self) -> list[journal.JournalStep]:
        if self._steps is None:
            self._steps = self._read_steps()
        return self._steps

    def _read_steps(self) -> list[journal.JournalStep]:
        if self.use_live() and self.ref is not None and self.task.session_id:
            return journal.read_steps(
                self.task.cwd or ".", self.task.session_id, self.ref.run_id,
                self.engine.settings.claude_home, self.ref.transcript_dir,
            )  # fmt: skip
        if (self.copy_dir / "journal.jsonl").is_file():
            return journal.steps_in(self.copy_dir)
        return []

    def progress(self) -> list[dict[str, Any]]:
        if self.use_live() and self.ref is not None and self.task.session_id:
            return journal.read_progress(
                self.task.cwd or ".", self.task.session_id, self.ref.run_id,
                self.engine.settings.claude_home, self.ref.transcript_dir,
            )  # fmt: skip
        try:
            run = json.loads((self.copy_dir / "run.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []
        items = run.get("workflowProgress") if isinstance(run, dict) else None
        if not isinstance(items, list):
            return []
        return [p for p in items if isinstance(p, dict) and isinstance(p.get("label"), str)]

    def step_path(self, label: str) -> Path:
        copy = self.engine.runstore.step_copy_path(self.task.id, label)
        step = next((s for s in self.steps() if s.label == label), None)
        live = self.live_dir()
        if step is None or step.agent_id is None or live is None:
            return copy
        live_path = live / f"agent-{step.agent_id}.jsonl"
        if not copy.is_file() or (step.status == "running" and live_path.is_file()):
            return live_path
        return copy  # also for a killed step whose live transcript is gone
