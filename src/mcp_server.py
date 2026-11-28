"""The MCP tool surface: 10 thin tools delegating to a `ToolProvider` backed by the engine."""

import asyncio
import time
from pathlib import Path
from typing import Any

from mcp.server.mcpserver import MCPServer

from src import bundle, debug, runlocator
from src.engine import TaskEngine
from src.reports.query import Filters
from src.roles import Roles
from src.tasks import KIND_SINGLE, WAIT_ENDS

SERVER_NAME = "dioxide"
PROMPT_WAIT_S = 300.0
PROMPT_POLL_S = 0.25
PROMPT_PRIORITY = 4
_ENGINE_NOT_STARTED = {"error": "engine not started", "status": "unavailable"}
_AGENT_KEYS = ("agents.id", "agents.name", "agents.description", "agents.role", "agents.status")

# Return keys per kept orcai tool (dotted names for nested keys): orcai's keys plus
# dioxide's additions. Parity-tested as a superset of the orcai snapshot.
RETURN_KEYS: dict[str, frozenset[str]] = {
    "delegate_task": frozenset({"task_id", "status", "position", "error", "deduplicated"}),
    "check_task_status": frozenset(
        {
            "task_id",
            "status",
            "output",
            "output.text",
            "output.tokens_used",
            "output.cost_usd",
            "output.return",
            "error",
            "started_at",
            "completed_at",
            "stage",
            "steps",
            "running_steps",
            "resumes",
            "session_id",
            "run_id",
            "question",
            "cost_usd",
            "cost_partial",
            "tokens_used",
            "uncounted_attempts",
            "cost_complete",
            "waiting",
            "waiting.until",
            "waiting.why",
            "kill_reason",
            "recovery",
            "recovery.resumes",
            "recovery.call_retries",
            "recovery.stage_retries",
            "recovery.rate_limit_waited_s",
            "attempts_by_mode",
            "attempts_by_mode.first",
            "attempts_by_mode.resume",
            "attempts_by_mode.fresh",
        }
    ),
    "prompt_agent": frozenset(
        {"agent_id", "response", "tokens_used", "status", "error", "task_id"}
    ),
    "get_agents": frozenset({"agents", "total", *_AGENT_KEYS}),
    "get_active_agents": frozenset({"agents", "active_count", "queue_depth", *_AGENT_KEYS}),
}


def engine_unavailable() -> dict[str, Any]:
    return dict(_ENGINE_NOT_STARTED)


def roles_by_agent(roles: Roles | None) -> dict[str, list[str]]:
    if roles is None:
        return {}
    assignments: list[tuple[str, str | None]] = [
        ("planner", roles.planner),
        ("implementer", roles.implementer),
        *(("reviewer", slug) for slug in roles.reviewers),
        ("qa", roles.qa),
        ("deployer", roles.deployer),
        ("lifecycle_agent", roles.lifecycle_agent),
    ]
    by_agent: dict[str, list[str]] = {}
    for role, slug in assignments:
        if slug is not None and role not in by_agent.setdefault(slug, []):
            by_agent[slug].append(role)
    return by_agent


class ToolProvider:
    """Backs the tools with the attached `TaskEngine`; payloads pass the engine's Redactor."""

    def __init__(self, prompt_wait_s: float = PROMPT_WAIT_S) -> None:
        self.engine: TaskEngine | None = None
        self.prompt_wait_s = prompt_wait_s

    def attach(self, engine: TaskEngine) -> None:
        self.engine = engine

    def detach(self) -> None:
        self.engine = None

    def _out(self, engine: TaskEngine, payload: dict[str, Any]) -> dict[str, Any]:
        redacted: dict[str, Any] = engine.redactor.redact_json(payload)
        return redacted

    async def _list_agents(self, engine: TaskEngine) -> list[tuple[dict[str, Any], list[str]]]:
        """(orcai-shaped agent record, the roles.yaml roles it fills) per boot-registry agent."""
        await engine.refresh_agents()
        roles = roles_by_agent(engine.roles)
        busy = await engine.busy_agents()
        listed = []
        for slug, agent in engine.agents.items():
            agent_roles = roles.get(slug, [])
            record = {
                "id": slug,
                "name": slug,
                "description": agent.description,
                "role": ",".join(agent_roles),
                "status": "busy" if slug in busy else "idle",
            }
            listed.append((record, agent_roles))
        return listed

    async def task_counts(self) -> dict[str, int]:
        if self.engine is None:
            return {"queued": 0, "running": 0, "needs_human": 0}
        return await self.engine.task_counts()

    async def delegate_task(
        self,
        agent: str,
        description: str,
        input_context: dict[str, Any] | None,
        priority: int,
        max_retries: int,
    ) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        result = await engine.submit(agent, description, input_context, priority, max_retries)
        return self._out(engine, result)

    async def check_task_status(self, task_id: str) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        return self._out(engine, await engine.status(task_id))

    async def prompt_agent(
        self, agent: str, message: str, context: dict[str, Any] | None, wait: bool
    ) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        submitted = await engine.submit(
            agent, message, context, PROMPT_PRIORITY, 0, kind_override=KIND_SINGLE, dedupe=False
        )
        if "task_id" not in submitted:
            return self._out(engine, {"agent_id": agent, **submitted})
        task_id = submitted["task_id"]
        if not wait:
            return self._out(engine, {"agent_id": agent, "task_id": task_id, "status": "queued"})
        deadline = time.monotonic() + self.prompt_wait_s
        status = await engine.status(task_id)
        # needs_human ends the wait too: the caller gets the question as the response.
        while status["status"] not in WAIT_ENDS and time.monotonic() < deadline:
            await asyncio.sleep(PROMPT_POLL_S)
            status = await engine.status(task_id)
        output = status.get("output") or {}
        error = status.get("error")
        if status["status"] not in WAIT_ENDS:
            state = status["status"]
            error = f"still {state} after {self.prompt_wait_s:g} s; poll check_task_status"
        payload = {
            "agent_id": agent,
            "task_id": task_id,
            "response": output.get("text") or "",
            "tokens_used": output.get("tokens_used") or 0,
            "status": status["status"],
            "error": error,
        }
        return self._out(engine, payload)

    async def get_agents(self, role: str | None, status: str | None) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        agents = [
            record
            for record, agent_roles in await self._list_agents(engine)
            if (role is None or role in agent_roles)
            and (status is None or record["status"] == status)
        ]
        return self._out(engine, {"agents": agents, "total": len(agents)})

    async def get_active_agents(self) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        busy = [r for r, _ in await self._list_agents(engine) if r["status"] == "busy"]
        depth = (await engine.task_counts())["queued"]
        return self._out(engine, {"agents": busy, "active_count": len(busy), "queue_depth": depth})

    async def answer_task(self, task_id: str, answer: str, source: str = "mcp") -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        return self._out(engine, await engine.answer(task_id, answer, source))

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        return self._out(engine, await engine.cancel(task_id))

    async def list_tasks(
        self,
        status: str | None,
        agent: str | None,
        limit: int,
        cursor: str | None,
        filters: Filters | None = None,
    ) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        f = filters or Filters()
        listed = await engine.list(
            status, agent, limit, cursor,
            project=f.project, kind=f.kind, since=f.since, until=f.until, task=f.task,
        )  # fmt: skip
        return self._out(engine, listed)

    async def get_task_logs(
        self, task_id: str, source: str | None, cursor: str | None, limit: int, full: bool
    ) -> dict[str, Any]:
        """Entries are redacted line by line by the pager (not re-walked here)."""
        if (engine := self.engine) is None:
            return engine_unavailable()
        return await debug.task_logs(engine, task_id, source, cursor, limit, full)

    async def get_task_debug(self, task_id: str) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        return await debug.task_debug(engine, task_id)

    async def server_logs(self, cursor: str | None, limit: int, full: bool) -> dict[str, Any]:
        if (engine := self.engine) is None:
            return engine_unavailable()
        return await debug.server_logs(engine, cursor, limit, full)

    async def task_exists(self, task_id: str) -> bool:
        return self.engine is not None and await runlocator.task_exists(task_id)

    def log_follower(self, task_id: str) -> debug.LogFollower | None:
        """A pager for repeated reads of one task's logs (SSE)."""
        return None if self.engine is None else debug.LogFollower(self.engine, task_id)

    async def task_bundle(self, task_id: str) -> tuple[Path, Path] | None:
        """``(temp dir, archive)`` of the redacted bundle; the caller removes the temp dir."""
        if (engine := self.engine) is None:
            return None
        settings = engine.settings
        return await bundle.task_bundle(
            task_id, engine.runstore.task_dir(task_id), settings.server_log_path,
            engine.redactor, settings.data_dir,
        )  # fmt: skip


def build_mcp(provider: ToolProvider) -> MCPServer:
    """A fresh MCP server whose 10 tools close over `provider` (one per app)."""

    async def delegate_task(
        agent: str,
        description: str,
        input_context: dict[str, Any] | None = None,
        priority: int = 3,
        max_retries: int = 0,
    ) -> dict[str, Any]:
        """Queue a task for an agent (or a role alias) and return immediately.

        Poll with check_task_status. priority: 1 (lowest) to 5 (highest).
        """
        return await provider.delegate_task(
            agent, description, input_context, priority, max_retries
        )

    async def check_task_status(task_id: str) -> dict[str, Any]:
        """Status, output, stage and steps of a delegated task."""
        return await provider.check_task_status(task_id)

    async def prompt_agent(
        agent: str,
        message: str,
        context: dict[str, Any] | None = None,
        wait: bool = True,
    ) -> dict[str, Any]:
        """Send an ad-hoc prompt to an agent; wait=True blocks until it finishes (up to 300 s)."""
        return await provider.prompt_agent(agent, message, context, wait)

    async def get_agents(role: str | None = None, status: str | None = None) -> dict[str, Any]:
        """List the configured agents, optionally filtered by role or status (idle|busy)."""
        return await provider.get_agents(role, status)

    async def get_active_agents() -> dict[str, Any]:
        """List only the agents currently running a task."""
        return await provider.get_active_agents()

    async def answer_task(task_id: str, answer: str) -> dict[str, Any]:
        """Answer a task waiting in needs_human; it resumes in the same session."""
        return await provider.answer_task(task_id, answer)

    async def cancel_task(task_id: str) -> dict[str, Any]:
        """Cancel a queued, running or needs_human task; a cancelled task is never resumed."""
        return await provider.cancel_task(task_id)

    async def list_tasks(
        status: str | None = None,
        agent: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """List tasks newest first, optionally filtered; page with the returned cursor."""
        return await provider.list_tasks(status, agent, limit, cursor)

    async def get_task_logs(
        task_id: str,
        source: str | None = None,
        cursor: str | None = None,
        limit: int = 200,
        full: bool = False,
    ) -> dict[str, Any]:
        """Page through a task's logs (redacted); page with the returned next_cursor.

        source: engine | server | attempt-<n> | stderr-<n> | journal | step:<label> | script |
        inputs; omitted = merged timestamped timeline. done = nothing more and task finished.
        """
        return await provider.get_task_logs(task_id, source, cursor, limit, full)

    async def get_task_debug(task_id: str) -> dict[str, Any]:
        """Debug view of a task: attempts, steps, workflow call, launch, timeline, bundle_url."""
        return await provider.get_task_debug(task_id)

    server: MCPServer = MCPServer(SERVER_NAME, instructions="Delegate work to Claude Code agents")
    for tool in (
        delegate_task,
        check_task_status,
        prompt_agent,
        get_agents,
        get_active_agents,
        answer_task,
        cancel_task,
        list_tasks,
        get_task_logs,
        get_task_debug,
    ):
        server.add_tool(tool)
    return server
