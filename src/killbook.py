"""The engine's in-memory kill and watchdog bookkeeping, per (task, attempt) unless noted.

Nothing here survives a restart (boot rebuilds what matters from the rows); a task's entries go
when its live job ends (``forget``).
"""

import asyncio
from dataclasses import dataclass, field

Key = tuple[str, int]  # (task id, attempt number)


@dataclass
class KillBook:
    # Attempts with a kill in flight: one kill per attempt, whoever asks (cancel, watch,
    # watchdog, boot).
    killing: set[Key] = field(default_factory=set)
    # Attempt -> clock time its run reported completion (the watchdog's exit-after-completion).
    run_done: dict[Key, float] = field(default_factory=dict)
    # Attempt -> (kills given up so far, clock time the next re-request is due).
    kill_retry: dict[Key, tuple[int, float]] = field(default_factory=dict)
    # Attempt -> the watchdog's outstanding activity read (at most one).
    activity_reads: dict[Key, "asyncio.Future[float | None]"] = field(default_factory=dict)
    # Attempt -> its descendants' CPU times {pid: (start, cpu s)} from the watch's latest process
    # snapshot, and the figures the watchdog's previous pass saw.
    cpu: dict[Key, dict[int, tuple[float, float]]] = field(default_factory=dict)
    cpu_seen: dict[Key, dict[int, tuple[float, float]]] = field(default_factory=dict)
    # Attempt -> clock time of the watchdog's latest pass that found its processes busy: idle
    # time is measured from it as from a file write.
    last_busy: dict[Key, float] = field(default_factory=dict)
    # Task -> pre-launch sweeps unconfirmed in a row. Each round is its own job, so a job that
    # ended on such a round keeps the streak (``streak_round``); any other job end drops it.
    unconfirmed: dict[str, int] = field(default_factory=dict)
    streak_round: set[str] = field(default_factory=set)
    # Task -> a terminal task's unconfirmed leftover sweep, retried by the watchdog until a
    # sweep is confirmed: (own claude pid -> start, trigger, unconfirmed so far, clock time
    # due). Kept when the task's live job ends (no pre-launch sweep follows a terminal status).
    sweep_retry: dict[str, tuple[dict[int, float], str, int, float]] = field(default_factory=dict)

    def forget(self, task_id: str) -> None:
        """The task's live job ended: drop its entries."""
        self.killing = {k for k in self.killing if k[0] != task_id}
        for key in [k for k in self.run_done if k[0] == task_id]:
            del self.run_done[key]
        for key in [k for k in self.kill_retry if k[0] == task_id]:
            del self.kill_retry[key]
        for book in (self.activity_reads, self.cpu, self.cpu_seen, self.last_busy):
            for key in [k for k in book if k[0] == task_id]:
                del book[key]
        if task_id in self.streak_round:
            self.streak_round.discard(task_id)
        else:
            self.unconfirmed.pop(task_id, None)
