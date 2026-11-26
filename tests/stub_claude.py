#!/usr/bin/env python3
"""Stand-in for the ``claude`` CLI in engine tests.

Plays the scenario named in ``$STUB_CLAUDE_SCENARIO`` (``a,b,c`` = one per attempt, the last one
repeats) and writes, in the shapes Claude Code 2.1.328 produces:

* stream-json lines on stdout (init, Workflow tool_use, background ack, task_* lines,
  task_notification, second init, two result lines);
* the run state under ``$STUB_CLAUDE_HOME/projects/<slug>/<session>/``: ``workflows/<runId>.json``
  and ``subagents/workflows/<runId>/{journal.jsonl, agent-<id>.jsonl, agent-<id>.meta.json}``.

``$STUB_CLAUDE_BG_CHILD`` (optional) starts a detached sleeper that outlives the stub before
any scenario plays: ``1`` runs it with this interpreter, any other value is the interpreter path
to run it with (e.g. a link under a neutral name); its pid is logged as ``bg_child_pid``.

``$STUB_CLAUDE_REPORT_ENV`` (optional, comma-separated names) logs, per name, a short sha256 of
that variable's value (null when unset) as ``reported_env``: never the value itself.

``$STUB_CLAUDE_EXTRA`` (optional) is emitted as an assistant text line and written to stderr
before the scenario plays; ``--version`` alone prints a version line.

Every launch appends its argv and selected environment to ``$STUB_CLAUDE_LOG`` (one JSON line);
the attempt counter lives next to it (``<log>.count``). Lifecycle scenarios echo the Workflow
input found in the prompt; a resumed run serves steps already in the journal from cache.

``$STUB_CLAUDE_SESSION_USAGE`` (optional) adds ``modelUsage`` (the run's tokens plus subagent
tokens, ``RUN_MODEL_TOKENS`` per run) to the result lines and makes them report session totals:
a resumed session restores the totals its earlier runs saved, as Claude Code 2.1.277+ does.

``$STUB_CLAUDE_NO_PROGRESS`` (optional) leaves ``workflowProgress`` out of the run file.
``call_mutated``, ``drop_resume_id`` and ``second_call`` start their first stage only after
``$STUB_CLAUDE_STAGE_DELAY_S`` (default 15 s); ``answer_mismatch`` and ``workflow_abort`` return
``stage_failed`` / ``cancelled`` as feature-lifecycle.js does; ``stale_run_file`` completes the
workflow without rewriting the run file (a resume then still finds the previous return).

``needs_human_rounds`` and ``needs_human_merge_blocked`` ask a ``rounds_exhausted`` (stage
``review-r1``) / ``merge_blocked`` (stage ``merge``) gate as feature-lifecycle.js does; the answer
is consumed by an ``interpret-<stage>-<k>`` step.
``needs_human_gate_hang`` plays ``needs_human_gate`` but hangs inside ``implement`` (after an
answer was consumed) until killed; ``needs_human_gate_interpret_hang`` hangs inside the
interpret step itself (the answer is read, its decision never recorded).
``script_exception`` ends the run with status ``failed`` and an ``error`` (a script
exception); ``unknown_status`` returns an unknown status;
``stage_failed`` returns ``stage_failed`` with an error. ``rate_limit_past`` hits a parent usage
limit whose reset already passed; ``rate_limit_stage_unjournaled`` has a stage that hit a usage
limit and, as in Claude Code, journaled no result for it; ``stage_limit_recovered`` has a
stage that hit a usage limit, recovered, then failed (``stage_failed``). ``auth_failed`` is a
session that is not authenticated (exit 1 after an ``authentication_failed`` line);
``auth_failed_stage`` has a stage whose transcript ends with that error (``stage_failed``).
``$STUB_CLAUDE_PLAN`` (optional, JSON) is the plan step's result. A workflow scenario name with
the suffix ``_late_bash`` (e.g. ``success_late_bash``) plays that scenario, then makes a parent
Bash call after the run's completion notification and hangs until killed; with
``_late_workflow`` the late call is a second Workflow call. ``$STUB_CLAUDE_PLANT`` (optional) is
put into the gate questions, the ``merge_blocked`` reason and the ``stage_failed`` / script error
texts.

``finish_while_detached`` finishes the run, then waits (``$STUB_CLAUDE_DELAY_S``, default 2 s, or
until the file ``$STUB_CLAUDE_RELEASE`` exists when that is set) before it reports completion and
exits, so an engine can stop and restart meanwhile; ``$STUB_CLAUDE_DETACHED_RETURN=needs_human``
makes its run ask a plan gate instead of completing. ``kill_mid_stage`` hangs inside
``implement`` until killed (an idle stage). ``long_stage`` keeps writing its ``implement``
transcript every 0.1 s (no stream lines meanwhile) until the file ``$STUB_CLAUDE_QUIET`` exists,
then hangs idle until killed. ``success_then_hang`` completes the run (completion notification
sent) and then hangs before its result lines, as a parent that is slow to exit (with
``$STUB_CLAUDE_DETACHED_RETURN=needs_human`` its run asks the plan gate).

``burn_in_stage`` / ``sleep_in_stage`` start, inside ``implement``, one child that burns CPU /
sleeps (logged as ``stage_child_pid``; it writes nothing) and hang until killed.

``background_return`` is an alias of ``success``: real Workflow calls always acknowledge in the
background and complete through ``task_notification``.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.journal import slug

CALL_MARKER = "this input and nothing else: "
LATE_BASH = "_late_bash"
LATE_WORKFLOW = "_late_workflow"
SLEEPER = [sys.executable, "-c", "import time; time.sleep(300)"]
LIMIT_TEXT = "You've hit your limit · resets 3pm (Europe/Paris)"
AUTH_TEXT = "Failed to authenticate: OAuth session expired and could not be refreshed"
# Scenarios whose Workflow call the engine must kill: the first stage starts only after
# $STUB_CLAUDE_STAGE_DELAY_S (default 15 s), so a kill "before the first stage" is observable.
# Tokens one run spends over every model, subagents included ($STUB_CLAUDE_SESSION_USAGE).
RUN_MODEL_TOKENS = 121_104
SESSION_USAGE_FILE = "stub-session-usage.json"
STAGE_DELAYED = frozenset({"call_mutated", "drop_resume_id", "second_call"})


def make_launcher(bin_dir: Path) -> Path:
    """Write an executable ``claude`` wrapper that runs this stub with the current interpreter."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    launcher = bin_dir / "claude"
    launcher.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{Path(__file__).resolve()}" "$@"\n')
    launcher.chmod(0o755)
    return launcher


def _hex(n: int) -> str:
    return uuid.uuid4().hex[:n]


def _hang() -> None:
    while True:
        time.sleep(3600)


class Ctx:
    def __init__(self, argv: list[str]) -> None:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("-p", dest="prompt", required=True)
        for flag in ("--session-id", "--resume", "--agents", "--mcp-config", "--settings"):
            parser.add_argument(flag)
        for flag in ("--output-format", "--model", "--agent"):
            parser.add_argument(flag)
        parser.add_argument("--allowedTools", action="append", default=[])
        parser.add_argument("--add-dir", nargs="+", action="extend", default=[])
        parser.add_argument("--strict-mcp-config", action="store_true")
        parser.add_argument("--verbose", action="store_true")
        self.args, _unknown = parser.parse_known_args(argv)
        self.argv = argv
        self.session: str = self.args.resume or self.args.session_id or str(uuid.uuid4())
        self.cwd = os.path.realpath(os.getcwd())
        home = os.environ.get("STUB_CLAUDE_HOME") or os.path.join(self.cwd, ".stub-claude-home")
        self.sdir = Path(home) / "projects" / slug(self.cwd) / self.session
        self.log_path = os.environ.get("STUB_CLAUDE_LOG")
        self.attempt = self._next_attempt()
        self._restored: dict[str, Any] | None = None  # $STUB_CLAUDE_SESSION_USAGE base
        # Default: a lifecycle prompt plays `success`, any other prompt `single_success`.
        default = "success" if CALL_MARKER in self.args.prompt else "single_success"
        names = (os.environ.get("STUB_CLAUDE_SCENARIO") or default).split(",")
        scenario = names[min(self.attempt, len(names)) - 1].strip()
        self.late_bash = scenario.endswith(LATE_BASH)
        self.late_workflow = scenario.endswith(LATE_WORKFLOW)
        self.scenario = scenario.removesuffix(LATE_BASH).removesuffix(LATE_WORKFLOW)

    def _next_attempt(self) -> int:
        if not self.log_path:
            return 1
        counter = Path(f"{self.log_path}.count")
        n = int(counter.read_text()) + 1 if counter.exists() else 1
        counter.write_text(str(n))
        return n

    def log(self, entry: dict[str, Any]) -> None:
        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry) + "\n")

    def emit(self, obj: dict[str, Any]) -> None:
        obj.setdefault("session_id", self.session)
        if obj.get("type") == "assistant":  # as Claude Code does: assistant lines carry a time
            obj.setdefault("timestamp", datetime.now(UTC).isoformat().replace("+00:00", "Z"))
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()

    # -- stream line builders ------------------------------------------------------------------
    def init(self) -> None:
        tools = ["Bash", "Edit", "Read", "Workflow", "Write"]
        self.emit(
            {
                "type": "system",
                "subtype": "init",
                "cwd": self.cwd,
                "session_id": self.session,
                "tools": tools,
                "mcp_servers": [],
                "model": self.args.model or "stub-model",
                "permissionMode": "default",
                "agent": None,
            }
        )

    def text(self, text: str) -> None:
        self.emit(
            {
                "type": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
                "parent_tool_use_id": None,
            }
        )

    def tool_use(self, tool_use_id: str, name: str, inp: dict[str, Any]) -> None:
        wire = dict(inp)
        if "args" in wire:
            wire["args"] = json.dumps(wire["args"])
        self.emit(
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": tool_use_id,
                            "name": name,
                            "input": inp,
                            "caller": {"type": "direct"},
                        }
                    ],
                },
                "parent_tool_use_id": None,
                "wire_tool_inputs": {tool_use_id: wire},
            }
        )

    def tool_result(self, tool_use_id: str, content: str, is_error: bool, meta: Any = None) -> None:
        line: dict[str, Any] = {
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {
                        "tool_use_id": tool_use_id,
                        "type": "tool_result",
                        "content": content,
                        "is_error": is_error,
                    }
                ],
            },
            "parent_tool_use_id": None,
        }
        if meta is not None:
            line["tool_use_result"] = meta
        self.emit(line)

    def rate_limit(self, status: str, resets_at: int) -> None:
        info = {"status": status, "resetsAt": resets_at, "rateLimitType": "five_hour"}
        self.emit({"type": "rate_limit_event", "rate_limit_info": info})

    def result(self, index: int, text: str, **extra: Any) -> None:
        line: dict[str, Any] = {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "api_error_status": None,
            "num_turns": 1,
            "result": text,
            "stop_reason": "end_turn",
            "terminal_reason": "completed",
            "total_cost_usd": 0.0125,
            "usage": {
                "input_tokens": 4,
                "output_tokens": 100,
                "cache_creation_input_tokens": 1000,
                "cache_read_input_tokens": 20000,
            },
            "permission_denials": [],
            "result_index": index,
        }
        line.update(extra)
        if os.environ.get("STUB_CLAUDE_SESSION_USAGE"):
            self._session_totals(line)
        self.emit(line)

    def _session_totals(self, line: dict[str, Any]) -> None:
        """Report session totals: this run's spend plus what a resumed session restored."""
        saved = self.sdir / SESSION_USAGE_FILE
        if self._restored is None:  # read once per run: every result line reports the same base
            self._restored = {"cost": 0.0, "tokens": 0}
            if self.args.resume and saved.exists():
                self._restored = json.loads(saved.read_text())
        cost = self._restored["cost"] + float(line["total_cost_usd"])
        tokens = self._restored["tokens"] + RUN_MODEL_TOKENS
        line["total_cost_usd"] = cost
        line["modelUsage"] = {
            "claude-opus-5-5": {
                "inputTokens": 4, "outputTokens": 100, "cacheReadInputTokens": tokens - 1104,
                "cacheCreationInputTokens": 1000, "costUSD": cost,
            }
        }  # fmt: skip
        self.sdir.mkdir(parents=True, exist_ok=True)
        saved.write_text(json.dumps({"cost": cost, "tokens": tokens}))  # saved at exit


class ScriptError:
    """A workflow script that threw: the run file ends with status ``failed``."""

    def __init__(self, message: str) -> None:
        self.message = message


class Run:
    """One workflow run: journal, step transcripts, run file, progress (with cache on resume)."""

    def __init__(self, ctx: Ctx, call: dict[str, Any], tool_use_id: str, run_id: str) -> None:
        self.ctx = ctx
        self.call = call
        self.tool_use_id = tool_use_id
        self.run_id = run_id
        self.task_id = "w" + _hex(8)
        self.args: dict[str, Any] = call.get("args") if isinstance(call.get("args"), dict) else {}
        self.tdir = ctx.sdir / "subagents" / "workflows" / run_id
        self.run_file = ctx.sdir / "workflows" / f"{run_id}.json"
        self.journal = self.tdir / "journal.jsonl"
        self.progress: list[dict[str, Any]] = []
        self.key = ""
        self.missed = False
        self.status = "completed"
        self.cache: dict[str, tuple[str, Any]] = {}
        for raw in self.journal.read_text().splitlines() if self.journal.exists() else []:
            rec = json.loads(raw)
            if rec.get("type") == "result":
                self.cache[rec["key"]] = (rec["agentId"], rec["result"])

    def launch(self) -> None:
        if not self.journal.exists():
            self._journal({"type": "launched"})

    def _journal(self, rec: dict[str, Any]) -> None:
        self.tdir.mkdir(parents=True, exist_ok=True)
        with self.journal.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")

    def _progress(self, label: str, agent_id: str, state: str, cached: bool | None) -> None:
        entry = {
            "type": "workflow_agent",
            "index": len(self.progress) + 1,
            "label": label,
            "agentId": agent_id,
            "agentType": "stage",
            "state": state,
            "attempt": None if cached else 1,
            "cached": cached,
            "tokens": None if cached else 1000,
        }
        self.progress = [p for p in self.progress if p["label"] != label] + [entry]
        self.ctx.emit(
            {
                "type": "system",
                "subtype": "task_progress",
                "task_id": self.task_id,
                "run_id": "0" + _hex(8),
                "tool_use_id": self.tool_use_id,
                "description": label,
                "last_tool_name": label,
                "usage": {"total_tokens": 1000, "tool_uses": 1, "duration_ms": 10},
                "workflow_progress": list(self.progress),
            }
        )

    def step(
        self,
        label: str,
        prompt: str,
        result: Any,
        *,
        extra_records: list[dict[str, Any]] | None = None,
        on_start: Callable[[], None] | None = None,
        journaled: bool = True,
    ) -> Any:
        digest = hashlib.sha256(f'{self.key}\0{prompt}\0{{"agentType":"stage"}}'.encode())
        self.key = "v2:" + digest.hexdigest()
        if not self.missed and self.key in self.cache:
            agent_id, value = self.cache[self.key]
            self._progress(label, agent_id, "done", True)
            return value
        self.missed = True
        agent_id = "a" + _hex(16)
        self._journal({"type": "started", "key": self.key, "agentId": agent_id, "label": label})
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        transcript = [
            {"type": "user", "isSidechain": True, "agentId": agent_id, "message": prompt},
            {"type": "assistant", "isSidechain": True, "agentId": agent_id, "message": "ok"},
            *(extra_records or []),
        ]
        for record in transcript:  # as in Phase-0 transcripts: every record carries a time
            record.setdefault("timestamp", now)
        (self.tdir / f"agent-{agent_id}.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in transcript)
        )
        meta = {"agentType": "stage", "description": label, "inWorkflowRun": True}
        (self.tdir / f"agent-{agent_id}.meta.json").write_text(json.dumps(meta))
        self._progress(label, agent_id, "start", None)
        if on_start is not None:
            on_start()
        if not journaled:  # as a stage that hit a usage limit: started, never a result record
            return None
        self._journal(
            {
                "type": "result",
                "key": self.key,
                "agentId": agent_id,
                "result": result,
                "toolsUsed": ["StructuredOutput"],
                "sessionId": self.ctx.session,
            }
        )
        self._progress(label, agent_id, "done", None)
        return result

    def finish(self, value: Any) -> None:
        self.run_file.parent.mkdir(parents=True, exist_ok=True)
        failed = isinstance(value, ScriptError)
        run = {
            "runId": self.run_id,
            "taskId": self.task_id,
            "scriptPath": self.call.get("scriptPath"),
            "args": self.args,
            "result": None if failed else value,
            "status": "failed" if failed else "completed",
            "logs": [],
            "workflowProgress": self.progress,
        }
        if isinstance(value, ScriptError):
            run["error"] = value.message
        self.status = str(run["status"])
        if os.environ.get("STUB_CLAUDE_NO_PROGRESS"):
            del run["workflowProgress"]
        self.run_file.write_text(json.dumps(run, indent=2))


def _answer(args: dict[str, Any], label: str) -> dict[str, Any] | None:
    answers = args.get("answers") or []
    return next((a for a in answers if isinstance(a, dict) and a.get("label") == label), None)


# -- workflow scripts (what feature-lifecycle.js would do) -------------------------------------
def _plan_result() -> Any:
    """The plan step's result: ``$STUB_CLAUDE_PLAN`` (JSON) or a placeholder."""
    raw = os.environ.get("STUB_CLAUDE_PLAN")
    return json.loads(raw) if raw else {"plan": "ok"}


def script_two_steps(run: Run, on_implement: Callable[[], None] | None = None) -> Any:
    plan = run.step("plan", "Plan the task.", _plan_result())
    impl = run.step("implement", "Implement the plan.", "done", on_start=on_implement)
    return {"status": "completed", "plan": plan, "implement": impl}


def script_gate(
    run: Run,
    on_implement: Callable[[], None] | None = None,
    on_interpret: Callable[[], None] | None = None,
) -> Any:
    run.step("plan", "Plan the task.", _plan_result())
    answer = _answer(run.args, "plan")
    if answer is None:
        question = "Which colour?"
        return {"status": "needs_human", "stage": "plan", "kind": "plan_approval",
                "question": question, "result": {"branch": run.args.get("branch")}}  # fmt: skip
    k = answer["k"]
    colour = run.step(f"interpret-plan-{k}", f"Read answer {k}.", "blue", on_start=on_interpret)
    run.step("implement", "Implement the plan.", "done", on_start=on_implement)
    return {"status": "completed", "stage": "merge", "result": {"colour": colour}}


def _plant() -> str:
    return os.environ.get("STUB_CLAUDE_PLANT", "")


def _gate_script(stage: str, kind: str, question: str, steps: list[str]) -> Callable[[Run], Any]:
    """A gate ask after ``steps``; the answer is read by ``interpret-<stage>-<k>``."""

    def play(run: Run) -> Any:
        for label in steps:
            run.step(label, f"Run {label}.", {"ok": True})
        answer = _answer(run.args, stage)
        if answer is None:
            return {"status": "needs_human", "stage": stage, "kind": kind,
                    "question": question + _plant(), "result": {}}  # fmt: skip
        k = answer["k"]
        run.step(f"interpret-{stage}-{k}", f"Read answer {k}.", {"action": "continue"})
        run.step(f"merge@a{k}", "Merge.", {"merge_sha": "m1", "exit_code": 0})
        return {"status": "completed", "stage": f"merge@a{k}", "result": {"merge_sha": "m1"}}

    return play


script_rounds = _gate_script(
    "review-r1",
    "rounds_exhausted",
    "review did not pass after 1 round(s). Open items:\n- rename foo\n",
    ["plan", "implement", "review-r1-reviewer"],
)
script_merge_blocked = _gate_script(
    "merge",
    "merge_blocked",
    "Merging is blocked: branch tip moved",
    ["plan", "implement", "review-r1-reviewer", "qa-r1", "merge"],
)


def script_exception(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    return ScriptError("TypeError: plan.steps is not iterable" + _plant())


def script_unknown_status(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    return {"status": "finished-ish", "stage": "plan"}


def script_stage_failed(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    return {"status": "stage_failed", "stage": "merge", "error": "merge.sh: failed" + _plant()}


def script_rerun(run: Run) -> Any:
    """A stage-raised ask: the stage re-runs as ``plan@a{k}`` with the answer path appended."""
    answer = _answer(run.args, "plan")
    prompt = "Plan the task."
    label = "plan"
    if answer is not None:
        prompt += f" Answer file: {run.args.get('run_dir')}/inputs/answers/{answer['k']}.md"
        label = f"plan@a{answer['k']}"
    plan = run.step(label, prompt, "NEEDS_HUMAN: which colour?" if answer is None else "blue")
    if answer is None:
        return {"status": "needs_human", "stage": "plan", "kind": "question", "question": plan}
    run.step(f"implement@a{answer['k']}", "Implement the plan.", "done")
    return {"status": "completed", "stage": "merge", "result": {"colour": plan}}


def script_answer_mismatch(run: Run) -> Any:
    """What feature-lifecycle.js returns when an answer does not fit the next ask point."""
    run.step("plan", "Plan the task.", {"plan": "ok"})
    pending = (run.args.get("answers") or [{}])[0]
    error = f"answer {pending.get('k')} ({pending.get('label')}, {pending.get('mode')}) was not "
    return {"status": "stage_failed", "stage": "implement", "error": error + "consumed"}


def script_no_stage(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    return {"status": "needs_human", "stage": "", "kind": "question", "question": "Hm?"}


def script_abort(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    return {"status": "cancelled", "stage": "interpret-plan-1", "result": {"branch": None}}


def _limit_record() -> dict[str, Any]:
    return {
        "type": "assistant",
        "isSidechain": True,
        "error": "rate_limit",
        "isApiErrorMessage": True,
        "apiErrorStatus": 429,
        "apiError": "usage_limit_reached",
        "apiErrorParams": {
            "rate_limit_info": {
                "status": "rejected",
                "resetsAt": int(time.time()) + 3600,
                "rateLimitType": "five_hour",
            }
        },
        "message": {"content": [{"type": "text", "text": LIMIT_TEXT}]},
    }


def script_stage_rate_limit(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    run.step("implement", "Implement the plan.", None, extra_records=[_limit_record()])
    return {"status": "stage_failed", "stage": "implement", "error": "agent returned null"}


def script_stage_rate_limit_unjournaled(run: Run) -> Any:
    """As Claude Code journals a stage that hit a usage limit: ``started`` and no ``result``,
    so a resumed run starts that stage again (``success`` then continues the same chain)."""
    run.step("plan", "Plan the task.", _plan_result())
    run.step("implement", "Implement the plan.", None, extra_records=[_limit_record()],
             journaled=False)  # fmt: skip
    return {"status": "stage_failed", "stage": "implement", "error": "agent returned null"}


def script_stage_limit_recovered(run: Run) -> Any:
    """A stage hit a usage limit early, recovered (a later successful turn), then failed."""
    later = {"type": "assistant", "isSidechain": True, "message": "worked on after the limit"}
    run.step("plan", "Plan the task.", {"plan": "ok"})
    run.step("implement", "Implement the plan.", None, extra_records=[_limit_record(), later])
    return {"status": "stage_failed", "stage": "implement", "error": "tests failed"}


def _auth_record() -> dict[str, Any]:
    """A stage transcript's synthetic record for a session that is not authenticated."""
    return {
        "type": "assistant",
        "isSidechain": True,
        "error": "authentication_failed",
        "isApiErrorMessage": True,
        "apiErrorStatus": None,
        "message": {"content": [{"type": "text", "text": AUTH_TEXT}]},
    }


def script_stage_auth_failed(run: Run) -> Any:
    run.step("plan", "Plan the task.", {"plan": "ok"})
    run.step("implement", "Implement the plan.", None, extra_records=[_auth_record()])
    return {"status": "stage_failed", "stage": "implement", "error": "agent returned null"}


def _spawn_marked_child(ctx: Ctx, interpreter: str | None = None) -> None:
    child = subprocess.Popen(
        [interpreter, *SLEEPER[1:]] if interpreter else SLEEPER,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    ctx.log({"bg_child_pid": child.pid})


def _long_stage(run: Run) -> None:
    """A stage that keeps working (its transcript grows) until told to go quiet, then idles."""
    quiet = os.environ.get("STUB_CLAUDE_QUIET")
    transcript = max(run.tdir.glob("agent-*.jsonl"), key=lambda p: p.stat().st_mtime)
    while not (quiet and os.path.exists(quiet)):
        with transcript.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"type": "assistant", "isSidechain": True, "message": "..."}))
            fh.write("\n")
        time.sleep(0.1)
    _hang()


def _wait_release() -> None:
    release = os.environ.get("STUB_CLAUDE_RELEASE")
    if not release:
        time.sleep(float(os.environ.get("STUB_CLAUDE_DELAY_S", "2")))
        return
    while not os.path.exists(release):
        time.sleep(0.05)


BURNER = [sys.executable, "-c", "while True:\n    pass"]


def _child_then_hang(ctx: Ctx, cmd: list[str]) -> None:
    """A stage running one long command: a child of this process (no new session) that writes
    nothing; this process hangs (silent) until killed."""
    child = subprocess.Popen(
        cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    ctx.log({"stage_child_pid": child.pid})
    _hang()


def _spawn_then_hang(ctx: Ctx) -> None:
    _spawn_marked_child(ctx)
    _hang()


# -- stream scenarios --------------------------------------------------------------------------
def _expected_input(ctx: Ctx) -> dict[str, Any]:
    prompt: str = ctx.args.prompt
    start = prompt.find(CALL_MARKER)
    if start < 0:
        return {}
    value, _end = json.JSONDecoder().raw_decode(prompt[start + len(CALL_MARKER) :])
    return value if isinstance(value, dict) else {}


def _call_and_ack(ctx: Ctx, call: dict[str, Any], fresh_run: bool = False) -> Run | None:
    tool_use_id = "toolu_" + _hex(24)
    ctx.tool_use(tool_use_id, "Workflow", call)
    resume_id = None if fresh_run else call.get("resumeFromRunId")
    run = Run(ctx, call, tool_use_id, resume_id or f"wf_{_hex(8)}-{_hex(3)}")
    if resume_id and not run.journal.exists():
        text = (
            f"<tool_use_error>The journal for workflow run {resume_id} is not on disk, and this "
            "session cannot fetch a remote copy, so there is nothing to resume. Fallback: Read "
            f"the agent-<id>.jsonl files in {run.tdir} and hand-author a continuation "
            "script.</tool_use_error>"
        )
        ctx.tool_result(tool_use_id, text, True, "Error: " + text)
        return None
    run.launch()
    task = {"task_id": run.task_id, "run_id": "0" + _hex(8), "task_type": "local_workflow"}
    ctx.emit({"type": "system", "subtype": "background_tasks_changed", "tasks": [task]})
    ctx.emit({"type": "system", "subtype": "task_started", "tool_use_id": tool_use_id, **task})
    meta = {
        "status": "async_launched",
        "taskId": run.task_id,
        "taskType": "local_workflow",
        "runId": run.run_id,
        "transcriptDir": str(run.tdir),
        "scriptPath": call.get("scriptPath"),
    }
    ack = f"Workflow launched in background. Task ID: {run.task_id}\nRun ID: {run.run_id}"
    ctx.tool_result(tool_use_id, ack, False, meta)
    return run


def _complete(ctx: Ctx, runs: list[Run], after_done: Callable[[], None] | None = None) -> None:
    for run in runs:
        ctx.emit(
            {
                "type": "system",
                "subtype": "task_updated",
                "task_id": run.task_id,
                "patch": {"status": "completed", "end_time": int(time.time() * 1000)},
            }
        )
        ctx.emit(
            {
                "type": "system",
                "subtype": "task_notification",
                "task_id": run.task_id,
                "run_id": "0" + _hex(8),
                "tool_use_id": run.tool_use_id,
                "status": run.status,
                "workflow_progress": run.progress,
            }
        )
    ctx.emit({"type": "system", "subtype": "background_tasks_changed", "tasks": []})
    if after_done is not None:
        after_done()
    ctx.init()
    ctx.text("The workflow completed.")
    ctx.result(0, "Workflow launched.")
    origin = {"kind": "task-notification", "producer": "session-task"}
    ctx.result(1, "The workflow completed.", origin=origin)


def play_workflow(ctx: Ctx) -> int:
    name = ctx.scenario
    call = _expected_input(ctx)
    if name == "call_mutated":
        call = dict(call, args=dict(call.get("args") or {}, mutated=True))
    if name == "drop_resume_id":
        call = {k: v for k, v in call.items() if k != "resumeFromRunId"}
    ctx.init()
    if name in ("parent_bash", "parent_bash_hang"):
        ctx.tool_use("toolu_bash_" + _hex(12), "Bash", {"command": "git status"})
    run = _call_and_ack(ctx, call)
    if run is None:
        ctx.text("The workflow could not be resumed.")
        ctx.result(0, "The workflow could not be resumed.")
        return 0
    runs = [run]
    if name == "second_call":
        second = _call_and_ack(ctx, call, fresh_run=True)
        runs += [second] if second is not None else []
    if name in STAGE_DELAYED:  # a real run needs a moment before its first stage starts
        time.sleep(float(os.environ.get("STUB_CLAUDE_STAGE_DELAY_S", "15")))

    hooks: dict[str, Callable[[], None]] = {
        "kill_mid_stage": _hang,
        "parent_bash_hang": _hang,
        "spawn_bg_child": lambda: _spawn_marked_child(ctx),
        "orphan_bash_group": lambda: _spawn_then_hang(ctx),
        "burn_in_stage": lambda: _child_then_hang(ctx, BURNER),
        "sleep_in_stage": lambda: _child_then_hang(ctx, SLEEPER),
        "parent_bash_mid_run": lambda: ctx.tool_use("toolu_bash_" + _hex(12), "Bash", {}),
    }
    scripts: dict[str, Callable[[Run], Any]] = {
        "needs_human_gate": script_gate,
        "needs_human_gate_hang": lambda r: script_gate(r, _hang),
        "needs_human_gate_interpret_hang": lambda r: script_gate(r, None, _hang),
        "needs_human_rerun": script_rerun,
        "rate_limit_stage": script_stage_rate_limit,
        "rate_limit_stage_unjournaled": script_stage_rate_limit_unjournaled,
        "stage_limit_recovered": script_stage_limit_recovered,
        "auth_failed_stage": script_stage_auth_failed,
        "answer_mismatch": script_answer_mismatch,
        "workflow_abort": script_abort,
        "needs_human_no_stage": script_no_stage,
        "needs_human_rounds": script_rounds,
        "needs_human_merge_blocked": script_merge_blocked,
        "script_exception": script_exception,
        "unknown_status": script_unknown_status,
        "stage_failed": script_stage_failed,
    }
    ctx.rate_limit("allowed", int(time.time()) + 3600)
    ctx.text("Workflow launched in the background.")
    for r in runs:
        if name == "stale_run_file":  # the run ends without rewriting <runId>.json
            continue
        script = scripts.get(name)
        detached = name in ("finish_while_detached", "success_then_hang")
        if detached and os.environ.get("STUB_CLAUDE_DETACHED_RETURN"):
            script = script_gate  # needs_human: the run asks the plan gate
        if name == "long_stage":
            value = script_two_steps(r, lambda: _long_stage(r))
        else:
            value = script(r) if script else script_two_steps(r, hooks.get(name))
        r.finish(value)
    if name == "finish_while_detached":
        _wait_release()
    after_done: Callable[[], None] | None = _hang if name == "success_then_hang" else None
    if ctx.late_workflow:

        def after_done() -> None:  # a parent that calls the Workflow tool again after the run
            ctx.tool_use("toolu_" + _hex(24), "Workflow", call)
            _hang()

    if ctx.late_bash:

        def after_done() -> None:  # a parent that keeps working after the run finished
            ctx.tool_use("toolu_bash_" + _hex(12), "Bash", {"command": "ls"})
            _hang()

    _complete(ctx, runs, after_done)
    return 0


def play_tool_error_exit0(ctx: Ctx) -> int:
    ctx.init()
    tool_use_id = "toolu_" + _hex(24)
    call = _expected_input(ctx)
    ctx.tool_use(tool_use_id, "Workflow", call)
    text = (
        "<tool_use_error>scriptPath must be a script path this tool returned, or a file you can "
        f"already read: {call.get('scriptPath')}</tool_use_error>"
    )
    ctx.tool_result(tool_use_id, text, True, "Error: " + text)
    ctx.text("The Workflow tool refused the script path.")
    ctx.result(0, "The Workflow tool refused the script path.")
    return 0


def play_exit1(ctx: Ctx) -> int:
    ctx.init()
    sys.stderr.write("Error: stub failure\n")
    return 1


def play_hang(ctx: Ctx) -> int:
    ctx.init()
    _hang()
    return 0


def play_rate_limit_parent(ctx: Ctx) -> int:
    ctx.init()
    ctx.rate_limit("rejected", int(time.time()) + 3600)
    ctx.text(LIMIT_TEXT)
    ctx.result(
        0,
        LIMIT_TEXT,
        is_error=True,
        api_error_status=429,
        api_error="usage_limit_reached",
        total_cost_usd=0,
    )
    return 1


def play_rate_limit_past(ctx: Ctx) -> int:
    """A parent usage limit whose reset time already passed."""
    ctx.init()
    ctx.rate_limit("rejected", int(time.time()) - 60)
    ctx.text(LIMIT_TEXT)
    ctx.result(
        0, LIMIT_TEXT, is_error=True, api_error_status=429, api_error="usage_limit_reached",
        total_cost_usd=0,
    )  # fmt: skip
    return 1


def play_auth_failed(ctx: Ctx) -> int:
    """What a session that is not authenticated prints (cut from a real install, 2.1.328):
    a synthetic assistant line marked ``authentication_failed``, an error result, exit 1."""
    ctx.init()
    ctx.emit(
        {
            "type": "assistant",
            "message": {
                "model": "<synthetic>", "role": "assistant", "stop_reason": "stop_sequence",
                "content": [{"type": "text", "text": AUTH_TEXT}],
            },
            "parent_tool_use_id": None,
            "error": "authentication_failed",
        }
    )  # fmt: skip
    ctx.result(
        0, AUTH_TEXT, is_error=True, stop_reason="stop_sequence", terminal_reason="api_error",
        total_cost_usd=0,
    )  # fmt: skip
    return 1


def play_ratelimit_text(ctx: Ctx) -> int:
    ctx.init()
    ctx.text(LIMIT_TEXT)
    ctx.result(0, LIMIT_TEXT, is_error=True)
    sys.stderr.write(LIMIT_TEXT + "\n")
    return 1


def play_refuse_resume(ctx: Ctx) -> int:
    if not ctx.args.resume:
        return play_workflow(ctx)
    message = f"No conversation found with session ID: {ctx.session}"
    sys.stderr.write(message + "\n")
    ctx.result(
        0,
        "",
        subtype="error_during_execution",
        is_error=True,
        num_turns=0,
        total_cost_usd=0,
        errors=[message],
    )
    return 1


def play_single_success(ctx: Ctx) -> int:
    ctx.init()
    ctx.text("Done.")
    ctx.result(0, "Done.", total_cost_usd=0.0042)
    return 0


SCENARIOS: dict[str, Callable[[Ctx], int]] = {
    "success": play_workflow,
    "background_return": play_workflow,
    "needs_human_gate": play_workflow,
    "needs_human_gate_hang": play_workflow,
    "needs_human_gate_interpret_hang": play_workflow,
    "needs_human_rerun": play_workflow,
    "rate_limit_stage": play_workflow,
    "rate_limit_stage_unjournaled": play_workflow,
    "stage_limit_recovered": play_workflow,
    "auth_failed_stage": play_workflow,
    "answer_mismatch": play_workflow,
    "stale_run_file": play_workflow,
    "needs_human_no_stage": play_workflow,
    "workflow_abort": play_workflow,
    "needs_human_rounds": play_workflow,
    "needs_human_merge_blocked": play_workflow,
    "script_exception": play_workflow,
    "unknown_status": play_workflow,
    "stage_failed": play_workflow,
    "call_mutated": play_workflow,
    "drop_resume_id": play_workflow,
    "second_call": play_workflow,
    "parent_bash": play_workflow,
    "parent_bash_hang": play_workflow,
    "kill_mid_stage": play_workflow,
    "finish_while_detached": play_workflow,
    "long_stage": play_workflow,
    "success_then_hang": play_workflow,
    "spawn_bg_child": play_workflow,
    "orphan_bash_group": play_workflow,
    "burn_in_stage": play_workflow,
    "sleep_in_stage": play_workflow,
    "parent_bash_mid_run": play_workflow,
    "tool_error_exit0": play_tool_error_exit0,
    "exit1": play_exit1,
    "hang": play_hang,
    "rate_limit_parent": play_rate_limit_parent,
    "auth_failed": play_auth_failed,
    "rate_limit_past": play_rate_limit_past,
    "ratelimit_text_in_output": play_ratelimit_text,
    "refuse_resume": play_refuse_resume,
    "single_success": play_single_success,
}


def _digest(value: str | None) -> str | None:
    return None if value is None else hashlib.sha256(value.encode()).hexdigest()[:16]


def main(argv: list[str]) -> int:
    if argv == ["--version"]:
        sys.stdout.write("2.1.328 (Claude Code stub)\n")
        return 0
    ctx = Ctx(argv)
    dioxide = sorted(k for k in os.environ if k.startswith("DIOXIDE_"))
    env_keys = ("DIOXIDE_TASK_ID", "CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS")
    entry: dict[str, Any] = {
        "attempt": ctx.attempt,
        "scenario": ctx.scenario,
        "argv": argv,
        "cwd": ctx.cwd,
        "env": {k: os.environ.get(k) for k in env_keys},
        "dioxide_vars": dioxide,
    }
    if names := os.environ.get("STUB_CLAUDE_REPORT_ENV"):  # credentials: a hash, never a value
        entry["reported_env"] = {name: _digest(os.environ.get(name)) for name in names.split(",")}
    ctx.log(entry)
    ctx.sdir.mkdir(parents=True, exist_ok=True)
    with ctx.sdir.with_name(f"{ctx.session}.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"type": "user", "sessionId": ctx.session}) + "\n")
    if bg_child := os.environ.get("STUB_CLAUDE_BG_CHILD"):
        _spawn_marked_child(ctx, None if bg_child == "1" else bg_child)
    if extra := os.environ.get("STUB_CLAUDE_EXTRA"):  # planted text: stream and stderr
        ctx.text(extra)
        sys.stderr.write(extra + "\n")
    play = SCENARIOS.get(ctx.scenario)
    if play is None:
        sys.stderr.write(f"stub_claude: unknown scenario {ctx.scenario!r}\n")
        return 2
    return play(ctx)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
