import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from src.journal import slug
from src.runstore import RunStore, render_brief, write_atomic

TASK_ID = "4c1f6d2e-0000-4000-8000-000000000001"
SESSION = "11111111-2222-3333-4444-555555555555"
RUN_ID = "wf_0b1533f1-219"
REVIEW = "review_r1_x-" + hashlib.sha256(b"review/r1 x").hexdigest()[:8]


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_inputs_readonly(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    brief = render_brief("Add a health endpoint", {"project_dir": "/repo", "n": 1})
    assert brief.startswith("Add a health endpoint\n")
    assert '```json\n{\n  "n": 1,\n  "project_dir": "/repo"\n}\n```' in brief

    store.write_inputs(TASK_ID, brief)
    paths = store.paths(TASK_ID)
    assert paths.brief_path.read_text() == brief
    assert mode(paths.brief_path) == 0o444
    assert not (paths.inputs_dir / "plan.json").exists()
    assert mode(paths.run_dir) == 0o700
    assert mode(paths.inputs_dir) == 0o555  # sealed: a stage can neither add nor replace

    store.write_inputs(TASK_ID, brief, pinned_plan={"steps": ["a"]})
    plan = paths.inputs_dir / "plan.json"
    assert json.loads(plan.read_text()) == {"steps": ["a"]}
    assert mode(plan) == 0o444

    answer = store.write_answer(TASK_ID, 1, "blue")
    assert answer == paths.inputs_dir / "answers" / "1.md"
    assert answer.read_text() == "blue"
    assert mode(answer) == 0o444
    with pytest.raises(PermissionError):
        answer.open("w")
    assert mode(answer.parent) == 0o555
    assert store.write_answer(TASK_ID, 2, "green").read_text() == "green"
    store.remove(TASK_ID)  # sealed directories are removed too
    assert not paths.run_dir.exists()


def test_snapshot_write_once(tmp_path: Path) -> None:
    src = tmp_path / "src"
    (src / "stages").mkdir(parents=True)
    (src / "feature-lifecycle.js").write_text("v1")
    (src / "stages" / "setup.sh").write_text("echo setup")
    (src / "stages" / "merge.sh").write_text("echo merge")
    (src / "stages" / "merge.sh").chmod(0o644)  # e.g. an install that lost the mode
    store = RunStore(tmp_path / "runs")

    script_dir = store.snapshot_script(TASK_ID, src, ("stages/merge.sh",))
    assert (script_dir / "feature-lifecycle.js").read_text() == "v1"
    assert (script_dir / "stages" / "setup.sh").read_text() == "echo setup"
    assert mode(script_dir / "feature-lifecycle.js") == 0o444
    assert mode(script_dir / "stages" / "setup.sh") == 0o444
    assert mode(script_dir / "stages" / "merge.sh") == 0o555  # made executable by the snapshot
    assert mode(script_dir) == mode(script_dir / "stages") == 0o555

    (src / "feature-lifecycle.js").write_text("v2")
    assert store.snapshot_script(TASK_ID, src) == script_dir
    assert (script_dir / "feature-lifecycle.js").read_text() == "v1"


def test_task_id_must_be_a_plain_name(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    for bad in ("", "..", "a/b", "."):
        with pytest.raises(ValueError):
            store.task_dir(bad)


def test_attempt_paths_and_events(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    stream, stderr = store.attempt_paths(TASK_ID, 3)
    assert stream == store.task_dir(TASK_ID) / "attempt-3.stream.jsonl"
    assert stderr == store.task_dir(TASK_ID) / "attempt-3.stderr"
    agents = store.attempt_agents_path(TASK_ID, 3)
    assert agents == store.task_dir(TASK_ID) / "attempt-3.agents.json"
    write_atomic(agents, b"{}", 0o600)
    store.remove(TASK_ID)  # retention removes the whole run dir, the agents file with it
    assert not agents.exists()

    store.append_event(TASK_ID, "call_mismatch", {"n": 1})
    store.append_event(TASK_ID, "killed", {})
    events_path = store.task_dir(TASK_ID) / "events.jsonl"
    lines = [json.loads(x) for x in events_path.read_text().splitlines()]
    assert [e["type"] for e in lines] == ["call_mismatch", "killed"]
    assert lines[0]["data"] == {"n": 1}
    assert isinstance(lines[0]["at"], float)
    assert mode(events_path) == 0o600


def _fake_session(claude_home: Path, cwd: Path) -> Path:
    sdir = claude_home / "projects" / slug(str(cwd)) / SESSION
    run_dir = sdir / "subagents" / "workflows" / RUN_ID
    run_dir.mkdir(parents=True)
    (sdir / "workflows").mkdir()
    (sdir / "workflows" / f"{RUN_ID}.json").write_text(json.dumps({"status": "completed"}))
    journal = [
        {"type": "launched"},
        {"type": "started", "key": "v2:a", "agentId": "a1", "label": "plan", "phase": "P"},
        {"type": "result", "key": "v2:a", "agentId": "a1", "result": "ok"},
        {"type": "started", "key": "v2:b", "agentId": "a2", "label": "review/r1 x", "phase": "R"},
    ]
    (run_dir / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in journal))
    (run_dir / "agent-a1.jsonl").write_text('{"type":"user"}\n')
    (run_dir / "agent-a2.jsonl").write_text('{"type":"user"}\n')
    return run_dir


def test_copy_transcripts_idempotent(tmp_path: Path) -> None:
    cwd = tmp_path / "repo"
    cwd.mkdir()
    claude_home = tmp_path / "claude"
    run_dir = _fake_session(claude_home, cwd)
    store = RunStore(tmp_path / "runs")
    tdir = store.task_dir(TASK_ID)

    copied = store.copy_transcripts(TASK_ID, str(cwd), SESSION, RUN_ID, claude_home)
    assert {p.relative_to(tdir).as_posix() for p in copied} == {
        "journal.jsonl",
        "run.json",
        "steps/plan.jsonl",
        f"steps/{REVIEW}.jsonl",
    }
    assert (tdir / "run.json").read_text() == json.dumps({"status": "completed"})
    assert mode(tdir / "steps" / "plan.jsonl") == 0o600

    assert store.copy_transcripts(TASK_ID, str(cwd), SESSION, RUN_ID, claude_home) == []

    with (run_dir / "agent-a2.jsonl").open("a") as fh:
        fh.write('{"type":"assistant"}\n')
    again = store.copy_transcripts(
        TASK_ID, str(cwd), SESSION, RUN_ID, claude_home, transcript_dir=run_dir
    )
    assert again == [tdir / "steps" / f"{REVIEW}.jsonl"]
    assert (tdir / "steps" / f"{REVIEW}.jsonl").read_text().count("\n") == 2


def test_copy_transcripts_missing_session_is_noop(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    assert store.copy_transcripts(TASK_ID, str(tmp_path), SESSION, RUN_ID, tmp_path) == []


def test_prune_by_newest_mtime(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.write_inputs("old", "brief")
    store.write_inputs("fresh", "brief")
    store.append_event("mixed", "x", {})
    now = 1_000_000_000.0
    day = 86400.0
    for task_id, age in (("old", 10), ("fresh", 1), ("mixed", 10)):
        for root, dirs, files in os.walk(store.task_dir(task_id)):
            for name in dirs + files:
                os.utime(Path(root, name), (now - age * day, now - age * day))
        os.utime(store.task_dir(task_id), (now - age * day, now - age * day))
    os.utime(store.task_dir("mixed") / "events.jsonl", (now, now))

    assert store.prune(5, keep=(), now=now) == ["old"]
    assert not store.task_dir("old").exists()
    assert store.task_dir("fresh").exists() and store.task_dir("mixed").exists()


def test_procs_recorded_set_merges(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    assert store.read_procs(TASK_ID) == {}
    store.write_procs(TASK_ID, {101: 1000.0, 102: 1001.0})
    store.write_procs(TASK_ID, {103: 1002.0, 101: 1000.0})
    assert store.read_procs(TASK_ID) == {101: 1000.0, 102: 1001.0, 103: 1002.0}
    path = store.task_dir(TASK_ID) / "procs.json"
    assert mode(path) == 0o600

    restarted = RunStore(tmp_path / "runs")
    restarted.write_procs(TASK_ID, {})
    assert restarted.read_procs(TASK_ID) == {101: 1000.0, 102: 1001.0, 103: 1002.0}

    path.write_text("{not json")
    assert store.read_procs(TASK_ID) == {}


def test_procs_drop_pid_one_and_below(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.write_procs(TASK_ID, {0: 1.0, 1: 1.0, 500: 2.0})
    assert store.read_procs(TASK_ID) == {500: 2.0}
    path = store.task_dir(TASK_ID) / "procs.json"
    path.write_text(json.dumps({"1": 1.0, "0": 1.0, "-3": 1.0, "77": 3.0}))
    assert store.read_procs(TASK_ID) == {77: 3.0}


def test_step_label_filenames_do_not_collide(tmp_path: Path) -> None:
    cwd = tmp_path / "repo"
    cwd.mkdir()
    claude_home = tmp_path / "claude"
    run_dir = _fake_session(claude_home, cwd)
    extra = [
        {"type": "started", "key": "v2:c", "agentId": "a3", "label": "a/b"},
        {"type": "result", "key": "v2:c", "agentId": "a3", "result": 1},
        {"type": "started", "key": "v2:d", "agentId": "a4", "label": "a_b"},
        {"type": "result", "key": "v2:d", "agentId": "a4", "result": 2},
    ]
    with (run_dir / "journal.jsonl").open("a") as fh:
        fh.write("".join(json.dumps(r) + "\n" for r in extra))
    (run_dir / "agent-a3.jsonl").write_text("slash\n")
    (run_dir / "agent-a4.jsonl").write_text("underscore\n")
    store = RunStore(tmp_path / "runs")
    store.copy_transcripts(TASK_ID, str(cwd), SESSION, RUN_ID, claude_home)
    steps = store.task_dir(TASK_ID) / "steps"
    assert (steps / "a_b.jsonl").read_text() == "underscore\n"
    slashed = [p for p in steps.iterdir() if p.name.startswith("a_b-")]
    assert len(slashed) == 1 and slashed[0].read_text() == "slash\n"


def test_prune_keeps_non_terminal_task_dirs(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    for task_id in ("waiting", "done"):
        store.write_inputs(task_id, "brief")
        store.snapshot_script(task_id, Path(__file__).resolve().parent.parent / "workflows")
    now = 1_000_000_000.0
    old = now - 100 * 86400.0
    for task_id in ("waiting", "done"):
        for root, dirs, files in os.walk(store.task_dir(task_id)):
            for name in dirs + files:
                os.utime(Path(root, name), (old, old), follow_symlinks=False)
        os.utime(store.task_dir(task_id), (old, old))

    # a needs_human task waiting longer than the retention keeps script/ and inputs/
    assert store.prune(90, keep={"waiting"}, now=now) == ["done"]
    paths = store.paths("waiting")
    assert paths.brief_path.read_text() == "brief"
    assert (paths.script_dir / "feature-lifecycle.js").is_file()
    assert not store.task_dir("done").exists()


def test_out_dir_writable_inputs_and_script_sealed(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.write_inputs(TASK_ID, "brief")
    store.snapshot_script(TASK_ID, Path(__file__).resolve().parent.parent / "workflows")
    out = store.ensure_out(TASK_ID)
    paths = store.paths(TASK_ID)
    assert out == paths.out_dir == paths.run_dir / "out"
    assert mode(out) == 0o700
    (out / "merge-subject.txt").write_text("feat: x\n")
    assert store.ensure_out(TASK_ID) == out  # idempotent, keeps what is there
    assert (out / "merge-subject.txt").read_text() == "feat: x\n"
    assert mode(paths.inputs_dir) == mode(paths.script_dir) == 0o555


def test_retire_plan_twice_for_one_attempt_keeps_both(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    store.write_inputs(TASK_ID, "brief", {"steps": ["first"]})
    first = store.retire_plan(TASK_ID, 3)
    store.write_inputs(TASK_ID, None, {"steps": ["second"]})
    second = store.retire_plan(TASK_ID, 3)
    assert first is not None and second is not None and first != second
    assert (first.name, second.name) == ("plan-3.json", "plan-3-2.json")
    assert json.loads(first.read_text()) == {"steps": ["first"]}
    assert json.loads(second.read_text()) == {"steps": ["second"]}
    assert store.retire_plan(TASK_ID, 3) is None  # nothing pinned any more


def test_archive_run_moves_copies_answers_and_out(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "runs")
    run_dir = store.task_dir("t1")
    store.write_inputs("t1", "brief", {"steps": ["a"]})
    store.write_answer("t1", 1, "abort")
    out = store.ensure_out("t1")
    (out / "merge-subject.txt").write_text("subject")
    (run_dir / "journal.jsonl").write_text("{}\n")
    (run_dir / "run.json").write_text(json.dumps({"runId": "wf_1"}))
    (run_dir / "steps").mkdir()
    (run_dir / "steps" / "plan.jsonl").write_text("{}\n")
    rows = [{"label": "plan", "status": "completed"}]

    moved = store.archive_run("t1", "wf_1", rows)
    assert moved == run_dir / "history" / "wf_1"
    assert json.loads((moved / "run.json").read_text()) == {"runId": "wf_1"}
    assert (moved / "journal.jsonl").is_file() and (moved / "steps" / "plan.jsonl").is_file()
    assert (moved / "answers" / "1.md").read_text() == "abort"
    assert (moved / "out" / "merge-subject.txt").read_text() == "subject"
    assert json.loads((moved / "steps.json").read_text()) == rows
    for gone in ("journal.jsonl", "run.json", "steps", "inputs/answers", "out/merge-subject.txt"):
        assert not (run_dir / gone).exists(), gone
    # The rest of the run dir stays: brief, pinned plan, out/ itself; inputs/ sealed again.
    assert (run_dir / "inputs" / "plan.json").is_file() and out.is_dir()
    assert mode(run_dir / "inputs") == 0o555
    store.write_answer("t1", 2, "next")  # the new run's answers start in a fresh directory
    assert sorted(p.name for p in (run_dir / "inputs" / "answers").iterdir()) == ["2.md"]

    assert store.archive_run("t1", "wf_1", rows) == run_dir / "history" / "wf_1-2"
    assert store.archive_run("t2", "wf_9", []) is None  # nothing to move
