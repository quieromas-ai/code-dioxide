"""Task engine: queue, compare-and-set launch, cancel, and the single decider.

One decider: what follows an attempt is decided only by ``recovery.next_action`` via
``_decide``, called by the attempt's waiter (``engine_watch.finish_attempt``) or, when the
engine's own bookkeeping broke, by the engine-error path (as a ``transient`` end). ``cancel``
writes ``cancelled`` directly (the waiter then only closes the attempt row). Kills only ever hit
a process that is still the one that was started (pid + start time); without a start time a
kill is skipped and logged.

Submit lives in ``engine_submit``, the stream watch and waiter in ``engine_watch``, the sweep
policy (what is killed after an attempt) in ``engine_sweep``, the read side in ``engine_read``,
lifecycle launch preparation, step mirror and return mapping in ``engine_lifecycle``, answers in
``answers``; ``TaskEngine`` keeps every method and delegates to them.

At most one job (launch + watch + the attempt's end) runs per task: the scheduler starts no job
for a task whose previous job is still finishing (e.g. its terminal sweep after an answer
already re-queued it).
"""

import asyncio
import dataclasses
import json
import logging
import time
import uuid
from collections.abc import Callable, Coroutine
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol

import aiosqlite

from src import (
    answers,
    childenv,
    engine_boot,
    engine_lifecycle,
    engine_read,
    engine_recovery,
    engine_submit,
    engine_sweep,
    engine_watch,
    procs,
    recovery,
    retention,
    watchdog,
)
from src.classify import Outcome
from src.config import Settings
from src.database import after_commit, get_db, write_tx
from src.engine_lifecycle import LaunchProblem
from src.engine_watch import Attempt
from src.killbook import KillBook
from src.launcher import (
    Mode,
    build_command,
    expected_call,
    launch_env,
    render_mcp_config,
    render_settings,
)
from src.redact import Redactor
from src.registry import AgentDef, render_agents_json
from src.roles import Roles, load_registry, registry_signature
from src.runstore import FILE_MODE, RunStore, write_atomic
from src.serverlog import LOGGER_NAME
from src.stream import Ev
from src.tasks import TERMINAL, Task

__all__ = ["TERMINAL", "AfterSpawn", "Attempt", "TaskEngine"]

SCHEDULE_TICK_S = 1.0
_log = logging.getLogger(LOGGER_NAME)


class AfterSpawn(Protocol):
    """Test seam awaited between spawn and the pid-store transaction."""

    async def __call__(self, task_id: str, pid: int, start: float | None) -> None: ...


def _next_mode(task: Task) -> Mode:
    """The mode the decider (or an answer) left in the row; ``first`` before any launch.

    A re-queued attempt that never spawned (sweep survivors) repeats its own mode."""
    if task.resume_mode == "resume":
        return "resume"
    return "fresh" if task.resume_mode == "fresh" else "first"


def _write_json(path: Path, value: Any) -> None:
    write_atomic(path, json.dumps(value, indent=2, ensure_ascii=False).encode(), FILE_MODE)


class TaskEngine:
    def __init__(
        self,
        settings: Settings,
        roles: Roles | None,
        agents: dict[str, AgentDef],
        runstore: RunStore,
        redactor: Redactor,
        notifier: Any = None,  # optional phase notifier (`phase(task_id, phase)`)
    ) -> None:
        self.settings = settings
        self.roles = roles
        self.agents = agents
        self._registry_sig = registry_signature(settings)  # of the files the registry came from
        self.runstore = runstore
        self.redactor = redactor
        self.notifier = notifier
        self._wake = asyncio.Event()
        self._scheduler: asyncio.Task[None] | None = None
        self._jobs: set[asyncio.Task[None]] = set()
        self._live: dict[str, asyncio.Task[None]] = {}  # task id -> its one running job
        self._launching: set[str] = set()
        self._attempts: dict[str, Attempt] = {}  # claimed attempts of in-flight runs
        self._kills: set[asyncio.Task[None]] = set()  # cancel kills; awaited, not cancelled
        self._after_spawn: AfterSpawn | None = None
        self._watchdog: asyncio.Task[None] | None = None
        self._clock: Callable[[], float] = time.time  # the watchdog's clock (a test seam)
        self._watchdog_ticks = 0  # completed watchdog passes (observable by tests)
        self._last_prune_at: float | None = None  # last run store retention pass (engine clock)
        self.book = KillBook()  # kill and watchdog bookkeeping

    # -- lifecycle of the engine itself ---------------------------------------------------------
    async def start(self) -> None:
        data = self.settings.data_dir
        data.mkdir(parents=True, exist_ok=True)
        _write_json(data / "agents.json", render_agents_json(self.agents))
        _write_json(data / "mcp.json", render_mcp_config(self.settings))
        extra = render_settings(self.settings)
        if extra is not None:
            _write_json(data / "settings.json", extra)
        # Before anything launches: attempts left by a previous engine are re-attached or
        # ended (``engine_boot``), and registered as live jobs the scheduler waits for.
        await engine_boot.boot(self)
        await retention.maybe_prune(self)  # after re-attach: live tasks are known non-terminal
        self._scheduler = asyncio.create_task(self._schedule_loop())
        self._watchdog = asyncio.create_task(watchdog.run(self))

    async def stop(self) -> None:
        """Cancel the engine's own asyncio tasks; child processes are left running (detach).

        Kills already requested by ``cancel`` (or the watchdog) are finished first. ``stop()``
        never signals a live task's process: the next engine re-attaches to it at start. A
        terminal-sweep thread already running for a finished task cannot be cancelled, so it may
        still signal that task's own leftover processes after ``stop()`` returns.
        """
        await asyncio.gather(*self._kills, return_exceptions=True)
        tasks = [t for t in (self._scheduler, self._watchdog, *self._jobs) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # A job may have requested a kill while it was being cancelled: finish those too.
        while pending := [job for job in self._kills if not job.done()]:
            await asyncio.gather(*pending, return_exceptions=True)
        self._scheduler = self._watchdog = None
        self._jobs.clear()

    # -- hot reload ---------------------------------------------------------------------------
    async def refresh_agents(self) -> None:
        """Reload agents and roles when their files changed (stat only when they did not).

        Called before each launch and wherever a caller's agent name is resolved or listed, so
        an edit applies without a restart. Both are swapped together, only after both loaded; on
        any error the previous registry stays and the same broken files are not reported again
        until they change. The events are global (no task): the error text carries the problem
        and position, never the source line."""
        signature = registry_signature(self.settings)
        if signature == self._registry_sig:
            return
        self._registry_sig = signature
        try:
            agents, roles = load_registry(self.settings, _log)
        except (OSError, ValueError) as exc:
            await self._reload_failed(str(exc))
            return
        except Exception as exc:  # an unforeseen loader bug must not fail the caller
            await self._reload_failed(type(exc).__name__)
            return
        changed = sorted(
            slug
            for slug in self.agents.keys() | agents.keys()
            if self.agents.get(slug) != agents.get(slug)
        )
        if roles != self.roles:
            changed.append("roles.yaml")
        self.agents, self.roles = agents, roles
        try:
            _write_json(self.settings.data_dir / "agents.json", render_agents_json(agents))
        except OSError:
            _log.exception("could not refresh agents.json")  # the humans' copy only
        # A global event (no task) no read path serves: the log line is what surfaces it.
        _log.info("agents reloaded", extra={"changed": changed})
        await self._note(None, "agents_reloaded", {"changed": changed})

    async def _reload_failed(self, error: str) -> None:
        _log.error("agents reload failed; keeping the previous registry", extra={"error": error})
        await self._note(None, "agents_reload_failed", {"error": error})

    def _write_attempt_agents(self, task: Task, n: int) -> Path:
        """The agents file attempt ``n`` of ``task`` is launched with (``--agents``); once the
        task fell back (``model_fallback_applied``), its models mapped by ``model_fallback``."""
        path = self.runstore.attempt_agents_path(task.id, n)
        rendered = render_agents_json(self.agents)
        if task.model_fallback_applied:
            swap = self.settings.model_fallback
            for entry in rendered.values():
                if entry.get("model") in swap:
                    entry["model"] = swap[entry["model"]]
        _write_json(path, rendered)
        return path

    # -- submit -------------------------------------------------------------------------------
    def resolve(self, name: str) -> str:
        if self.roles is not None:
            return self.roles.resolve(name)
        if name in self.agents:
            return name
        raise KeyError(name)

    # -- delegated to engine_submit / engine_watch / engine_read -------------------------------
    async def submit(
        self,
        agent: str,
        description: str,
        input_context: Any,
        priority: int,
        max_retries: int,
        *,
        kind_override: str | None = None,
        dedupe: bool = True,
    ) -> dict[str, Any]:
        await self.refresh_agents()
        return await engine_submit.submit(
            self, agent, description, input_context, priority, max_retries,
            kind_override=kind_override, dedupe=dedupe,
        )  # fmt: skip

    async def _new_row(
        self, agent: str, description: str, ctx: Any, kind_override: str | None, dedupe: bool
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await engine_submit.new_row(self, agent, description, ctx, kind_override, dedupe)

    async def _watch(self, attempt: Attempt) -> None:
        await engine_watch.watch(self, attempt)

    async def _wait_exit(self, attempt: Attempt, interval: float) -> tuple[bool, int | None]:
        return await engine_watch.wait_exit(attempt, interval)

    async def _inspect(self, attempt: Attempt, new: list[Ev], state: dict[str, Any]) -> None:
        await engine_watch.inspect(self, attempt, new, state)

    async def _denied(self, task_id: str, tool: Any, message: str | None, seen: set[str]) -> None:
        await engine_watch.denied(self, task_id, tool, message, seen)

    async def _kill_for(
        self, attempt: Attempt, reason: str, data: dict[str, Any] | None = None
    ) -> None:
        await engine_watch.kill_for(self, attempt, reason, data)

    async def _finish_attempt(
        self, attempt: Attempt, events: list[Ev], exit_code: int | None, workflow_return: Any = None
    ) -> None:
        await engine_watch.finish_attempt(self, attempt, events, exit_code, workflow_return)

    async def status(self, task_id: str) -> dict[str, Any]:
        return await engine_read.status(self, task_id)

    async def busy_agents(self) -> set[str]:
        return await engine_read.busy_agents()

    async def task_counts(self) -> dict[str, int]:
        return await engine_read.task_counts(self)

    async def answer(self, task_id: str, text: str, source: str) -> dict[str, Any]:
        return await answers.answer(self, task_id, text, source)

    # -- scheduler ----------------------------------------------------------------------------
    async def _schedule_loop(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self._schedule_once()
            except Exception:
                _log.exception("scheduler pass failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), SCHEDULE_TICK_S)

    async def _schedule_once(self) -> None:
        db = get_db()
        # A slot is held while the task's latest attempt is open (a cancelled task whose
        # process is still dying keeps its slot until the waiter closes the attempt row).
        async with db.execute(
            "SELECT COUNT(*) AS n FROM tasks t JOIN attempts a "
            "ON a.task_id = t.id AND a.n = t.attempt WHERE a.ended_at IS NULL"
        ) as cur:
            row = await cur.fetchone()
        free = self.settings.max_concurrent_tasks - len(self._launching) - (row["n"] if row else 0)
        if free <= 0:
            return
        async with db.execute(
            "SELECT * FROM tasks WHERE status = 'queued' AND "
            "(next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY priority DESC, created_at ASC, id ASC",
            (self._clock(),),
        ) as cur:
            due = [Task.from_row(r) for r in await cur.fetchall()]
        # A task whose previous job is still finishing waits for it (one job per task).
        startable = [t for t in due if t.id not in self._launching and t.id not in self._live]
        for task in startable[:free]:
            self._launching.add(task.id)
            self._start_job(task.id, self._run(task, _next_mode(task)))

    def _start_job(self, task_id: str, work: Coroutine[Any, Any, None]) -> None:
        """The task's one live job (the scheduler starts none for it while this one runs)."""
        job = asyncio.create_task(work)
        self._jobs.add(job)
        self._live[task_id] = job
        job.add_done_callback(self._job_done(task_id))

    def _job_done(self, task_id: str) -> Any:
        def done(job: asyncio.Task[None]) -> None:
            self._jobs.discard(job)
            if self._live.get(task_id) is job:
                del self._live[task_id]
                self.book.forget(task_id)
                self._wake.set()  # a re-queued task may be waiting for this job

        return done

    async def _run(self, task: Task, mode: Mode) -> None:
        owned: list[Attempt] = []  # every Attempt object this job registered, newest last
        try:
            attempt = await self._launch(task, mode, owned)
            if attempt is not None:
                await self._watch(attempt)
        except Exception as exc:
            _log.exception("engine error", extra={"task_id": task.id})
            await self._engine_error(task, exc)
        finally:
            # Only this job's own entry (identity): an old job finishing late never drops a
            # newer job's.
            if owned and self._attempts.get(task.id) is owned[-1]:
                del self._attempts[task.id]
            self._wake.set()

    def _adopt(self, attempt: Attempt) -> None:
        """An attempt rebuilt at engine start becomes the task's in-flight attempt and live job
        at once (``cancel`` and the scheduler see it before the job first runs)."""
        self._attempts[attempt.task.id] = attempt
        self._start_job(attempt.task.id, self._resume_watch(attempt))

    async def _resume_watch(self, attempt: Attempt) -> None:
        """Watched to its end like a launched attempt (re-attached to a live process, or ended
        at once from its stream and run file)."""
        try:
            await self._watch(attempt)
        except Exception as exc:
            _log.exception("engine error", extra={"task_id": attempt.task.id})
            await self._engine_error(attempt.task, exc)
        finally:
            self._unregister(attempt)
            self._wake.set()

    def _register(self, attempt: Attempt, owned: list[Attempt]) -> None:
        self._attempts[attempt.task.id] = attempt
        owned.append(attempt)

    def _unregister(self, attempt: Attempt) -> None:
        if self._attempts.get(attempt.task.id) is attempt:
            del self._attempts[attempt.task.id]

    async def _engine_error(self, task: Task, exc: Exception) -> None:
        await engine_recovery.engine_error(self, task, exc)

    # -- launch -------------------------------------------------------------------------------
    async def _prepare_launch(self, task: Task, mode: Mode) -> Task | None:
        """Lifecycle: script snapshot + frozen args before the first claim (raises
        ``LaunchProblem``). Returns the task as the claim must see it, None if the row moved."""
        return await engine_lifecycle.prepare(self, task)

    def _require_agent(self, task: Task) -> None:
        """A single task whose agent was removed from the registry since it was queued."""
        if task.agent and not task.is_lifecycle and task.agent not in self.agents:
            raise LaunchProblem(f"unknown agent '{task.agent}'")

    async def _launch(
        self, task: Task, mode: Mode, owned: list[Attempt] | None = None
    ) -> Attempt | None:
        owned = [] if owned is None else owned
        await self.refresh_agents()
        try:
            try:
                prepared = await self._prepare_launch(task, mode)
                # Checked after the awaits of the preparation, right before the claim writes the
                # attempt's agents file: no reload can slip in between (no await in between).
                if prepared is not None:
                    self._require_agent(prepared)
            except LaunchProblem as exc:
                await self._fail_unlaunched(task, str(exc))
                return None
            if prepared is None:  # the row moved meanwhile: nothing to launch
                return None
            attempt = await self._claim(prepared, mode)
        finally:
            self._launching.discard(task.id)
        if attempt is None:
            return None
        self._register(attempt, owned)
        # Swept only after winning the CAS: a losing launch never touches the winner's process.
        own = engine_sweep.own_claude(task.pid, task.pid_start)  # the previous attempt's claude
        if not await self._sweep_clear(attempt, own) or not await self._still_running(attempt):
            self._unregister(attempt)
            return None
        return await self._spawn(attempt, owned)

    async def _still_running(self, attempt: Attempt) -> bool:
        """Re-check after the sweep: a task cancelled meanwhile is not spawned, and the attempt
        row the claim opened is closed in the same transaction (its slot is freed)."""
        async with write_tx() as db:
            row = await self._fetch(db, attempt.task.id)
            if row is not None and row["status"] == "running" and row["attempt"] == attempt.n:
                return True
            await db.execute(
                "UPDATE attempts SET ended_at = ?, outcome = 'cancelled', reason = ?, "
                "running_s = 0 WHERE task_id = ? AND n = ? AND ended_at IS NULL",
                (time.time(), row["kill_reason"] if row else None, attempt.task.id, attempt.n),
            )
        return False

    async def _sweep_clear(self, attempt: Attempt, own: dict[int, float]) -> bool:
        return await engine_sweep.sweep_clear(self, attempt, own)

    async def _fail_unlaunched(self, task: Task, error: str) -> None:
        await engine_recovery.fail_unlaunched(self, task, error)

    async def _claim(self, task: Task, mode: Mode) -> Attempt | None:
        """Compare-and-set ``queued -> running`` on (status, attempt) plus the attempts row."""
        now = time.time()
        n = task.attempt + 1
        session_id = task.session_id or str(uuid.uuid4())
        claimed = dataclasses.replace(
            task, status="running", attempt=n, session_id=session_id, pid=None, pid_start=None,
            kill_reason=None, resume_mode=mode, started_at=task.started_at or now,
            next_attempt_at=None,
        )  # fmt: skip
        paths = self.runstore.paths(task.id)
        argv = build_command(
            settings=self.settings, task=claimed, mode=mode, session_id=session_id, paths=paths,
            agents_path=self._write_attempt_agents(task, n), corrective=bool(task.corrective),
        )  # fmt: skip
        stream_path, stderr_path = self.runstore.attempt_paths(task.id, n)
        async with write_tx() as db:
            cur = await db.execute(
                "UPDATE tasks SET status = 'running', attempt = attempt + 1, pid = NULL, "
                "pid_start = NULL, kill_reason = NULL, session_id = ?, resume_mode = ?, "
                "next_attempt_at = NULL, started_at = COALESCE(started_at, ?), updated_at = ? "
                "WHERE id = ? AND status = ? AND attempt = ?",
                (session_id, mode, now, now, task.id, task.status, task.attempt),
            )
            if cur.rowcount == 0:
                return None  # someone else moved the task first
            await db.execute(
                "INSERT INTO attempts (task_id, n, session_id, argv_json, stream_path, "
                "stderr_path, started_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task.id, n, session_id, json.dumps(argv), str(stream_path), str(stderr_path), now),
            )
            started = {"n": n, "mode": mode, "corrective": bool(task.corrective)}
            await self._event(db, task.id, "attempt_started", started)
        call = expected_call(claimed, mode) if claimed.is_lifecycle else None
        return Attempt(claimed, mode, n, argv, stream_path, stderr_path, now, call=call)

    async def _spawn(self, attempt: Attempt, owned: list[Attempt]) -> Attempt | None:
        task = attempt.task
        try:
            # Read at every launch (a rotated token needs no restart); an unusable file fails
            # the task here, before anything is started. Values are registered as secrets.
            path = self.settings.child_env_file
            child = childenv.read(path) if path is not None else {}
            self.redactor.add_values(child.values())
            env = launch_env(self.settings, task, child_env=child)
            pid, start = await asyncio.to_thread(
                procs.spawn,
                attempt.argv,
                Path(task.cwd or "."),
                env,
                attempt.stream_path,
                attempt.stderr_path,
            )
        except Exception as exc:
            await self._spawn_failed(attempt, exc)
            self._unregister(attempt)
            return None
        attempt = dataclasses.replace(attempt, pid=pid, start=start)
        self._register(attempt, owned)  # from here an engine error kills this process
        if self._after_spawn is not None:
            await self._after_spawn(task.id, pid, start)
        async with write_tx() as db:
            await db.execute(
                "UPDATE tasks SET pid = ?, pid_start = ?, updated_at = ? "
                "WHERE id = ? AND attempt = ?",
                (pid, start, time.time(), task.id, attempt.n),
            )
            status = await self._column(db, task.id, "status")
            # For get_task_debug: the PATH the child was given (not recorded when the child env
            # file sets it: its values are never written) and the names of the file's variables.
            path_env = None if "PATH" in child else env.get("PATH")
            spawned = {"n": attempt.n, "pid": pid, "path_env": path_env, "child_env": sorted(child)}
            await self._event(db, task.id, "spawned", spawned)
        if start is not None:
            await asyncio.to_thread(self._record_procs, task.id, {pid: start})
        if status == "cancelled":  # cancelled inside the spawn window: the new process dies now
            # The one kill-per-attempt path: a kill that is given up is retried by the watchdog.
            engine_watch.request_kill(self, (task.id, attempt.n), pid, start, "cancel")
        return attempt

    def _record_procs(self, task_id: str, recorded: dict[int, float]) -> None:
        """Bookkeeping only: a failure is logged and never stops the attempt."""
        try:
            self.runstore.write_procs(task_id, recorded)
        except Exception:
            _log.exception("could not record task processes", extra={"task_id": task_id})

    async def _spawn_failed(self, attempt: Attempt, exc: Exception) -> None:
        await engine_recovery.spawn_failed(self, attempt, exc)

    async def _kill(self, task_id: str, pid: int, start: float | None, trigger: str) -> bool:
        """True when the kill was carried out or found nothing to kill; False when it was given
        up (the process table stayed unreadable)."""
        return await engine_sweep.kill(self, task_id, pid, start, trigger)

    async def _sweep_leftovers(self, task_id: str, trigger: str, own: dict[int, float]) -> None:
        await engine_sweep.sweep_leftovers(self, task_id, trigger, own)

    async def _note(self, task_id: str | None, type_: str, data: dict[str, Any]) -> None:
        """A non-critical event: a failure is logged, never raised."""
        try:
            async with write_tx() as db:
                await self._event(db, task_id, type_, data)
        except Exception:
            _log.exception("could not record event", extra={"task_id": task_id, "type": type_})

    def _background(self, work: Coroutine[Any, Any, None]) -> None:
        """A kill or sweep requested by ``cancel``: awaited by ``stop()``, never cancelled."""
        job = asyncio.create_task(work)
        self._kills.add(job)
        job.add_done_callback(self._kill_done)

    def _kill_done(self, job: asyncio.Task[None]) -> None:
        self._kills.discard(job)
        if not job.cancelled() and job.exception() is not None:
            _log.error("background kill failed", exc_info=job.exception())

    def _decide(
        self,
        task: Task,
        outcome: Outcome,
        reset_at: float | None,
        pending_answer: int | None = None,
    ) -> tuple[recovery.Action, dict[str, Any]]:
        return engine_recovery.decide(self, task, outcome, reset_at, pending_answer)

    # -- cancel / read side -------------------------------------------------------------------
    async def cancel(self, task_id: str) -> dict[str, Any]:
        return await engine_submit.cancel(self, task_id)

    # -- helpers (take the caller's connection; never open a nested write_tx) -------------------
    async def _fetch(self, db: aiosqlite.Connection, task_id: str) -> Any:
        async with db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cur:
            return await cur.fetchone()

    async def _column(self, db: aiosqlite.Connection, task_id: str, column: str) -> Any:
        row = await self._fetch(db, task_id)
        return None if row is None else row[column]

    async def _count(self, db: aiosqlite.Connection, status: str) -> int:
        async with db.execute("SELECT COUNT(*) AS n FROM tasks WHERE status = ?", (status,)) as cur:
            row = await cur.fetchone()
        return int(row["n"]) if row else 0

    async def _similar(self, db: aiosqlite.Connection, fp: str, task_id: str) -> str | None:
        async with db.execute(
            "SELECT id FROM tasks WHERE fingerprint = ? AND status = 'needs_human' AND id != ? "
            "ORDER BY created_at DESC LIMIT 1",
            (fp, task_id),
        ) as cur:
            row = await cur.fetchone()
        return None if row is None else str(row["id"])

    async def _event(
        self, db: aiosqlite.Connection, task_id: str | None, type_: str, data: dict[str, Any]
    ) -> None:
        await db.execute(
            "INSERT INTO events (task_id, type, data_json, at) VALUES (?, ?, ?, ?)",
            (task_id, type_, json.dumps(data, ensure_ascii=False), time.time()),
        )
        # events.jsonl mirror: written after commit, outside the write lock; failures are logged.
        if task_id is not None:
            after_commit(lambda: self.runstore.append_event(task_id, type_, data))

    # Defined last: inside the class body the name `list` shadows the builtin.
    async def list(
        self, status: str | None, agent: str | None, limit: int, cursor: str | None
    ) -> dict[str, Any]:
        await self.refresh_agents()
        return await engine_read.list_tasks(self, status, agent, limit, cursor)
