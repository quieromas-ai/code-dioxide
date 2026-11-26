import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import stub_claude

from src import journal, procs
from src.classify import Outcome, classify
from src.launcher import CALL_SUFFIX, call_matches
from src.stream import (
    Ev,
    Init,
    ParentToolUse,
    PermissionDenied,
    RateLimit,
    Result,
    StreamWatcher,
    WorkflowAck,
    WorkflowCall,
    WorkflowDone,
    last_interim_result,
)

SESSION = "11111111-2222-3333-4444-555555555555"
CALL_ID = "toolu_01TD1qovLCUg95S9R1enpT9F"


def line(obj: dict[str, Any]) -> str:
    return json.dumps(obj) + "\n"


def tool_use(tid: str, name: str, inp: dict[str, Any], parent: str | None = None) -> str:
    return line(
        {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": tid, "name": name, "input": inp}],
            },
            "parent_tool_use_id": parent,
            "session_id": SESSION,
        }
    )


def ack_ok(tid: str, run_id: str) -> str:
    return line(
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {
                        "tool_use_id": tid,
                        "type": "tool_result",
                        "content": f"Workflow launched in background. Run ID: {run_id}",
                        "is_error": False,
                    }
                ],
            },
            "parent_tool_use_id": None,
            "tool_use_result": {
                "status": "async_launched",
                "taskId": "wynyffy2h",
                "runId": run_id,
                "transcriptDir": f"/h/projects/x/{SESSION}/subagents/workflows/{run_id}",
            },
        }
    )


def notification(tid: str, status: str = "completed") -> str:
    return line(
        {
            "type": "system",
            "subtype": "task_notification",
            "task_id": "wynyffy2h",
            "run_id": "0mwnvkajn-d8fa3229",
            "tool_use_id": tid,
            "status": status,
        }
    )


def result(index: int, **extra: Any) -> str:
    base: dict[str, Any] = {
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "api_error_status": None,
        "result": "done",
        "session_id": SESSION,
        "total_cost_usd": 0.006,
        "usage": {
            "input_tokens": 4,
            "output_tokens": 100,
            "cache_creation_input_tokens": 1000,
            "cache_read_input_tokens": 20000,
        },
        "permission_denials": [],
        "result_index": index,
    }
    base.update(extra)
    return line(base)


def test_stream_line_boundaries(tmp_path: Path) -> None:
    path = tmp_path / "attempt-1.stream.jsonl"
    watcher = StreamWatcher(path)
    assert watcher.poll() == []  # missing file

    init = line({"type": "system", "subtype": "init", "session_id": SESSION, "tools": ["Workflow"]})
    call = tool_use(CALL_ID, "Workflow", {"scriptPath": "/s.js", "args": {"n": 1}})
    path.write_text(init + "not json at all\n" + call[:40])
    events = watcher.poll()
    assert events == [Init(session_id=SESSION, tools=["Workflow"], mcp_servers=[])]

    with path.open("a") as fh:
        fh.write(call[40:])
    assert watcher.poll() == [
        WorkflowCall(tool_use_id=CALL_ID, input={"scriptPath": "/s.js", "args": {"n": 1}})
    ]
    assert watcher.poll() == []

    watcher.reset()
    assert [type(e) for e in watcher.poll()] == [Init, WorkflowCall]


def test_stream_non_ascii_split_mid_character(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl"
    raw = (
        json.dumps(
            {"type": "system", "subtype": "init", "session_id": "żółw-🐢", "tools": []},
            ensure_ascii=False,
        ).encode()
        + b"\n"
    )
    cut = raw.index("🐢".encode()) + 2  # inside the 4-byte UTF-8 sequence
    path.write_bytes(raw[:cut])
    watcher = StreamWatcher(path)
    assert watcher.poll() == []
    with path.open("ab") as fh:
        fh.write(raw[cut:])
    assert watcher.poll() == [Init(session_id="żółw-🐢", tools=[], mcp_servers=[])]


def test_parent_tool_use_detected(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl"
    path.write_text(
        tool_use(CALL_ID, "Workflow", {"scriptPath": "/s.js", "args": '{"n": 1}'})
        + tool_use("toolu_bash", "Bash", {"command": "ls"})
        + tool_use("toolu_stage", "Bash", {"command": "ls"}, parent="toolu_agent")
    )
    events = StreamWatcher(path).poll()
    assert events == [
        WorkflowCall(tool_use_id=CALL_ID, input={"scriptPath": "/s.js", "args": {"n": 1}}),
        ParentToolUse(tool_use_id="toolu_bash", name="Bash"),
    ]


def test_workflow_ack_error_shape(tmp_path: Path) -> None:
    text = (
        "<tool_use_error>The journal for workflow run wf_deadbeef-000 is not on disk, "
        "and this session cannot fetch a remote copy</tool_use_error>"
    )
    err = line(
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {"type": "tool_result", "content": text, "is_error": True, "tool_use_id": "t2"}
                ],
            },
            "parent_tool_use_id": None,
            "tool_use_result": "Error: The journal for workflow run wf_deadbeef-000 ...",
        }
    )
    list_content = line(
        {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "t3",
                        "is_error": True,
                        "content": [{"type": "text", "text": "bad scriptPath"}],
                    }
                ]
            },
        }
    )
    other = line(
        {
            "type": "user",
            "message": {
                "content": [{"type": "tool_result", "tool_use_id": "tb", "is_error": True}]
            },
        }
    )
    path = tmp_path / "s.jsonl"
    path.write_text(
        tool_use("t2", "Workflow", {"scriptPath": "/s.js", "resumeFromRunId": "wf_deadbeef-000"})
        + err
        + tool_use("t3", "Workflow", {"scriptPath": "/x.js"})
        + list_content
        + other
    )
    acks = [e for e in StreamWatcher(path).poll() if isinstance(e, WorkflowAck)]
    assert acks == [
        WorkflowAck(
            tool_use_id="t2",
            run_id=None,
            task_id=None,
            transcript_dir=None,
            is_error=True,
            error_text=text,
        ),
        WorkflowAck(
            tool_use_id="t3",
            run_id=None,
            task_id=None,
            transcript_dir=None,
            is_error=True,
            error_text="bad scriptPath",
        ),
    ]


def test_workflow_return_background_shape(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl"
    path.write_text(
        tool_use(CALL_ID, "Workflow", {"scriptPath": "/s.js", "args": {}})
        + ack_ok(CALL_ID, "wf_0b1533f1-219")
        + notification(CALL_ID)
    )
    events = StreamWatcher(path).poll()
    assert events[1:] == [
        WorkflowAck(
            tool_use_id=CALL_ID,
            run_id="wf_0b1533f1-219",
            task_id="wynyffy2h",
            transcript_dir=f"/h/projects/x/{SESSION}/subagents/workflows/wf_0b1533f1-219",
            is_error=False,
            error_text=None,
        ),
        WorkflowDone(tool_use_id=CALL_ID, status="completed"),
    ]


def test_stream_two_results_keeps_both(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl"
    denial = {"tool_name": "Bash", "tool_use_id": "t", "tool_input": {}}
    path.write_text(
        line(
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "allowed",
                    "resetsAt": 1795114800,
                    "rateLimitType": "five_hour",
                },
            }
        )
        + line(
            {
                "type": "system",
                "subtype": "permission_denied",
                "tool_name": "Bash",
                "message": "This command requires approval",
            }
        )
        + result(0)
        + result(1, permission_denials=[denial], total_cost_usd=0.01)
    )
    events = StreamWatcher(path).poll()
    assert events[0] == RateLimit(status="allowed", resets_at=1795114800.0, limit_type="five_hour")
    assert events[1] == PermissionDenied(tool_name="Bash", message="This command requires approval")
    results = [e for e in events if isinstance(e, Result)]
    assert [r.result_index for r in results] == [0, 1]
    last = results[-1]
    assert last.cost_usd == 0.01
    assert last.tokens == 21104
    assert last.permission_denials == [denial]
    assert last.session_id == SESSION and last.text == "done" and last.subtype == "success"
    assert last.is_error is False and last.api_error_status is None and last.api_error is None


def _run_stub(tmp_path: Path, scenario: str, prompt: str) -> tuple[int, Path, Path]:
    claude_bin = stub_claude.make_launcher(tmp_path / "bin")
    cwd = tmp_path / "repo"
    cwd.mkdir(exist_ok=True)
    home = tmp_path / "claude"
    log = tmp_path / "stub.log"
    env = dict(os.environ)
    env.update(
        STUB_CLAUDE_HOME=str(home),
        STUB_CLAUDE_LOG=str(log),
        STUB_CLAUDE_SCENARIO=scenario,
        DIOXIDE_TASK_ID="task-1",
    )
    stream = tmp_path / "attempt-1.stream.jsonl"
    stderr = tmp_path / "attempt-1.stderr"
    cmd = [str(claude_bin), "-p", prompt, "--session-id", SESSION, "--output-format", "stream-json"]
    pid, _start = procs.spawn(cmd, cwd, env, stream, stderr)
    deadline = time.monotonic() + 30
    code = procs.reap(pid)
    while code is None and time.monotonic() < deadline:
        time.sleep(0.05)
        code = procs.reap(pid)
    assert code is not None, "stub did not exit"
    return code, stream, home


def test_stub_success_shapes(tmp_path: Path) -> None:
    call = {"scriptPath": "/run/script/feature-lifecycle.js", "args": {"task_id": "task-1"}}
    prompt = (
        "Call the Workflow tool exactly once with this input and nothing else: "
        + json.dumps(call, sort_keys=True, separators=(",", ":"))
        + CALL_SUFFIX
    )
    code, stream, home = _run_stub(tmp_path, "success", prompt)
    assert code == 0
    events = StreamWatcher(stream).poll()
    calls = [e for e in events if isinstance(e, WorkflowCall)]
    acks = [e for e in events if isinstance(e, WorkflowAck)]
    dones = [e for e in events if isinstance(e, WorkflowDone)]
    results = [e for e in events if isinstance(e, Result)]
    assert isinstance(events[0], Init) and events[0].session_id == SESSION
    assert [c.input for c in calls] == [call]
    assert len(acks) == 1 and not acks[0].is_error and acks[0].run_id is not None
    assert acks[0].run_id.startswith("wf_")
    assert dones == [WorkflowDone(tool_use_id=calls[0].tool_use_id, status="completed")]
    assert [r.result_index for r in results] == [0, 1]
    assert not any(isinstance(e, ParentToolUse) for e in events)

    assert acks[0].transcript_dir is not None
    transcript_dir = Path(acks[0].transcript_dir)
    assert (transcript_dir / "journal.jsonl").is_file()
    run_file = transcript_dir.parent.parent.parent / "workflows" / f"{acks[0].run_id}.json"
    run = json.loads(run_file.read_text())
    assert run["status"] == "completed"
    assert run["result"]["status"] == "completed"
    assert transcript_dir.is_relative_to(home)

    entries = [json.loads(x) for x in (tmp_path / "stub.log").read_text().splitlines()]
    assert entries[0]["env"]["DIOXIDE_TASK_ID"] == "task-1"


CALL = {"scriptPath": "/run/script/feature-lifecycle.js", "args": {"task_id": "t", "answers": []}}
RESUME_CALL = dict(CALL, resumeFromRunId="wf_00000000-000")


def _lifecycle_prompt(call: dict[str, Any]) -> str:
    body = json.dumps(call, sort_keys=True, separators=(",", ":"))
    return (
        f"Call the Workflow tool exactly once with this input and nothing else: {body}{CALL_SUFFIX}"
    )


SINGLE_PROMPT = "Read the task brief at /run/inputs/brief.md and do it."

# scenario -> (kind, prompt call or None for single, --resume?, expected outcome)
TERMINATING: dict[str, tuple[str, dict[str, Any] | None, bool, Outcome]] = {
    "success": ("lifecycle", CALL, False, Outcome.success),
    "background_return": ("lifecycle", CALL, False, Outcome.success),
    "finish_while_detached": ("lifecycle", CALL, False, Outcome.success),
    "spawn_bg_child": ("lifecycle", CALL, False, Outcome.success),
    "needs_human_gate": ("lifecycle", CALL, False, Outcome.needs_human),
    "needs_human_rerun": ("lifecycle", CALL, False, Outcome.needs_human),
    "exit1": ("lifecycle", CALL, False, Outcome.transient),
    "rate_limit_parent": ("lifecycle", CALL, False, Outcome.rate_limited),
    "rate_limit_stage": ("lifecycle", CALL, False, Outcome.rate_limited),
    "ratelimit_text_in_output": ("lifecycle", CALL, False, Outcome.transient),
    "auth_failed": ("single", None, False, Outcome.auth_failed),
    "auth_failed_stage": ("lifecycle", CALL, False, Outcome.auth_failed),
    "refuse_resume": ("lifecycle", RESUME_CALL, True, Outcome.resume_refused),
    "call_mutated": ("lifecycle", CALL, False, Outcome.success),
    "drop_resume_id": ("lifecycle", RESUME_CALL, True, Outcome.success),
    "second_call": ("lifecycle", CALL, False, Outcome.call_mismatch),
    "parent_bash": ("lifecycle", CALL, False, Outcome.parent_violation),
    "tool_error_exit0": ("lifecycle", CALL, False, Outcome.call_mismatch),
    "single_success": ("single", None, False, Outcome.success),
}
# Calls the engine rejects live (classify never sees the expected input).
LIVE_MISMATCH = {"call_mutated", "drop_resume_id"}


def _launch(
    tmp_path: Path, scenario: str, prompt: str, resume: bool
) -> tuple[int, float | None, Path, str]:
    claude_bin = stub_claude.make_launcher(tmp_path / "bin")
    cwd = tmp_path / "repo"
    cwd.mkdir(exist_ok=True)
    task_id = str(uuid.uuid4())
    env = dict(os.environ)
    env.update(
        STUB_CLAUDE_HOME=str(tmp_path / "claude"),
        STUB_CLAUDE_LOG=str(tmp_path / "stub.log"),
        STUB_CLAUDE_SCENARIO=scenario,
        STUB_CLAUDE_DELAY_S="0.2",
        DIOXIDE_TASK_ID=task_id,
    )
    flag = "--resume" if resume else "--session-id"
    cmd = [str(claude_bin), "-p", prompt, flag, SESSION, "--output-format", "stream-json"]
    stream = tmp_path / "attempt-1.stream.jsonl"
    pid, start = procs.spawn(cmd, cwd, env, stream, tmp_path / "attempt-1.stderr")
    return pid, start, stream, task_id


def _wait_exit(pid: int, timeout: float = 30.0) -> int | None:
    deadline = time.monotonic() + timeout
    code = procs.reap(pid)
    while code is None and time.monotonic() < deadline:
        time.sleep(0.05)
        code = procs.reap(pid)
    return code


def _classify(
    tmp_path: Path, events: list[Ev], code: int | None, kind: str
) -> tuple[Outcome, float | None]:
    ack = next((e for e in events if isinstance(e, WorkflowAck) and not e.is_error), None)
    ret = None
    records: list[dict[str, Any]] = []
    if ack is not None and ack.run_id and ack.transcript_dir:
        cwd, tdir = str(tmp_path / "repo"), Path(ack.transcript_dir)
        home = tmp_path / "claude"
        ret = journal.read_return(cwd, SESSION, ack.run_id, home, tdir)
        for step in journal.read_steps(cwd, SESSION, ack.run_id, home, tdir):
            assert step.agent_id is not None
            path = journal.step_transcript_path(cwd, SESSION, ack.run_id, home, step.agent_id, tdir)
            records += [json.loads(x) for x in path.read_text().splitlines()]
    stderr = (tmp_path / "attempt-1.stderr").read_text()
    return classify(
        exit_code=code,
        events=events,
        workflow_return=ret,
        stderr_tail=stderr,
        stage_error_records=records,
        kind=kind,
    )


@pytest.mark.parametrize("scenario", sorted(TERMINATING))
def test_stub_scenarios_classify(tmp_path: Path, scenario: str) -> None:
    kind, call, resume, expected = TERMINATING[scenario]
    prompt = _lifecycle_prompt(call) if call is not None else SINGLE_PROMPT
    pid, _start, stream, task_id = _launch(tmp_path, scenario, prompt, resume)
    try:
        code = _wait_exit(pid)
        assert code is not None, "stub did not exit"
        events = StreamWatcher(stream).poll()
        outcome, reset = _classify(tmp_path, events, code, kind)
        assert outcome is expected
        assert (reset is not None) == (expected is Outcome.rate_limited)
        if scenario in LIVE_MISMATCH:
            first = next(e for e in events if isinstance(e, WorkflowCall))
            assert call is not None and not call_matches(call, first.input)
    finally:
        swept = procs.sweep(task_id, {}, grace_s=2.0).killed
    assert bool(swept) == (scenario == "spawn_bg_child")


def test_stub_default_single_prompt_plays_single_success(tmp_path: Path) -> None:
    pid, _start, stream, _task_id = _launch(tmp_path, "", SINGLE_PROMPT, False)
    assert _wait_exit(pid) == 0
    events = StreamWatcher(stream).poll()
    assert not any(isinstance(e, WorkflowCall) for e in events)
    assert _classify(tmp_path, events, 0, "single") == (Outcome.success, None)


HANGING: dict[str, type] = {
    "hang": Init,
    "kill_mid_stage": WorkflowAck,
    "orphan_bash_group": WorkflowAck,
}


@pytest.mark.parametrize("scenario", sorted(HANGING))
def test_stub_hanging_scenarios(tmp_path: Path, scenario: str) -> None:
    pid, start, stream, task_id = _launch(tmp_path, scenario, _lifecycle_prompt(CALL), False)
    watcher = StreamWatcher(stream)
    events: list[Ev] = []
    recorded: dict[int, float] = {}
    try:
        deadline = time.monotonic() + 15
        while not any(isinstance(e, HANGING[scenario]) for e in events):
            assert time.monotonic() < deadline, "expected partial events never arrived"
            time.sleep(0.05)
            events += watcher.poll()
            recorded.update(procs.descendants(pid))
        if scenario == "orphan_bash_group":
            log = tmp_path / "stub.log"
            while "bg_child_pid" not in log.read_text():
                assert time.monotonic() < deadline, "bg child never started"
                time.sleep(0.05)
        assert _wait_exit(pid, timeout=0.3) is None  # still running
        procs.kill_tree(pid, start, task_id, grace_s=2.0, recorded=recorded)
        code = _wait_exit(pid, timeout=5)
    finally:
        procs.sweep(task_id, recorded, grace_s=2.0)
    assert code is not None and code < 0
    assert procs.marker_pids(task_id) == set()
    events += watcher.poll()
    assert not any(isinstance(e, Result) for e in events)
    assert _classify(tmp_path, events, None, "lifecycle") == (Outcome.transient, None)


def test_last_interim_result_scans_back_past_large_tail(tmp_path: Path) -> None:
    path = tmp_path / "attempt-1.stream.jsonl"
    assert last_interim_result(path) is None  # no file yet
    result = {"type": "result", "total_cost_usd": 0.3, "usage": {"input_tokens": 7}}
    filler = json.dumps({"type": "assistant", "text": "x" * 200_000})  # beyond one tail chunk
    path.write_text(json.dumps(result) + "\n" + filler + "\n" + filler + "\n")
    found = last_interim_result(path)
    assert found is not None and (found.cost_usd, found.tokens) == (0.3, 7)
    path.write_text(filler + "\n")
    assert last_interim_result(path) is None


def test_last_interim_result_scan_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.stream._TAIL_CHUNK", 1024)
    monkeypatch.setattr("src.stream._MAX_SCAN", 8 * 1024)
    path = tmp_path / "attempt-1.stream.jsonl"
    line = json.dumps({"type": "assistant", "text": "x" * 500}) + "\n"
    path.write_text(line * 2000)  # ~1 MB, no result record: the first turn is still running
    read = {"bytes": 0}
    real_open = Path.open

    class Counting:
        def __init__(self, fh: Any) -> None:
            self._fh = fh

        def __enter__(self) -> "Counting":
            return self

        def __exit__(self, *exc: object) -> None:
            self._fh.close()

        def seek(self, *args: int) -> int:
            return int(self._fh.seek(*args))

        def read(self, n: int) -> bytes:
            data: bytes = self._fh.read(n)
            read["bytes"] += len(data)
            return data

    def counting_open(self: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        fh = real_open(self, mode, *args, **kwargs)
        return Counting(fh) if mode == "rb" else fh

    monkeypatch.setattr(Path, "open", counting_open)
    assert last_interim_result(path) is None
    assert 0 < read["bytes"] <= 2 * 8 * 1024  # growing chunks re-read: bounded by ~1.33x the cap
    # A result record beyond the window is not seen; one inside it is.
    result = json.dumps({"type": "result", "total_cost_usd": 1.0}) + "\n"
    path.write_text(result + line * 2000)
    assert last_interim_result(path) is None
    path.write_text(line * 2000 + result + line * 5)
    found = last_interim_result(path)
    assert found is not None and found.cost_usd == 1.0


def _result_with(tmp_path: Path, **fields: Any) -> Result:
    path = tmp_path / "attempt-1.stream.jsonl"
    record = {"type": "result", "total_cost_usd": 0.5, "usage": {"input_tokens": 7}, **fields}
    path.write_text(json.dumps(record) + "\n")
    found = last_interim_result(path)
    assert found is not None
    return found


def test_result_session_tokens_sum_every_model(tmp_path: Path) -> None:
    # modelUsage covers subagents and every model; usage is the top-level loop only.
    model_usage = {
        "claude-opus-5-5": {
            "inputTokens": 26, "outputTokens": 8185,
            "cacheReadInputTokens": 333465, "cacheCreationInputTokens": 62587,
            "webSearchRequests": 3, "costUSD": 0.6, "contextWindow": 1000000,
        },
        "claude-haiku-5-5": {
            "inputTokens": 1, "outputTokens": 2,
            "cacheReadInputTokens": 3, "cacheCreationInputTokens": 4,
        },
    }  # fmt: skip
    found = _result_with(tmp_path, modelUsage=model_usage)
    assert found.session_tokens == 404263 + 10
    assert found.tokens == 7  # unchanged: still the top-level usage


@pytest.mark.parametrize(
    "fields",
    [{}, {"modelUsage": {}}, {"modelUsage": None}, {"modelUsage": "x"}, {"modelUsage": [1]},
     {"modelUsage": {"m": "not a dict"}}],
)  # fmt: skip
def test_result_session_tokens_none_without_model_usage(
    tmp_path: Path, fields: dict[str, Any]
) -> None:
    assert _result_with(tmp_path, **fields).session_tokens is None


def test_result_session_tokens_ignores_non_integer_fields(tmp_path: Path) -> None:
    model_usage = {"m": {"inputTokens": "5", "outputTokens": True, "cacheReadInputTokens": 9}}
    assert _result_with(tmp_path, modelUsage=model_usage).session_tokens == 9
