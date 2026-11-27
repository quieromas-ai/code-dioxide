"""What each open attempt is doing, as its watcher last read it from the stream: the last
interim ``result`` record (the running cost) and the last tool the parent called. In memory
only: a re-attached watcher replays the stream from offset 0 and so rebuilds its entry."""

from collections.abc import Sequence
from dataclasses import dataclass

from src.stream import WORKFLOW_TOOL, Ev, ParentToolUse, Result, WorkflowCall


@dataclass
class AttemptActivity:
    last_result: Result | None = None
    last_tool: str | None = None
    last_event_at: float | None = None


class ActivityBook:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, int], AttemptActivity] = {}

    def update(self, task_id: str, n: int, events: Sequence[Ev], at: float) -> None:
        """Record a poll's ``events`` (read at ``at``). An entry exists from the first poll on,
        events or not: the watcher has then seen the whole stream so far."""
        entry = self._entries.setdefault((task_id, n), AttemptActivity())
        for event in events:
            if isinstance(event, Result):
                entry.last_result = event
            elif isinstance(event, ParentToolUse):
                entry.last_tool = event.name
            elif isinstance(event, WorkflowCall):
                entry.last_tool = WORKFLOW_TOOL
        if events:
            entry.last_event_at = at

    def get(self, task_id: str, n: int) -> AttemptActivity | None:
        return self._entries.get((task_id, n))

    def drop(self, task_id: str, n: int) -> None:
        self._entries.pop((task_id, n), None)
