"""Journal / run-file parsing.

The fixtures ``tests/fixtures/journal_*.jsonl`` and ``run_*.json`` are sanitised captures from
real Claude Code 2.1.328 workflow runs (Phase 0 evidence):

* ``journal_completed.jsonl`` + ``run_completed.json``: a run that completed (phases, structured
  result);
* ``journal_gate_resumed.jsonl`` + ``run_gate_resumed.json``: a gate-ask run resumed twice (four
  entries served from cache, two live);
* ``journal_killed.jsonl``: an interrupted run (a step started, never finished);
* ``journal_restarted.jsonl`` + ``run_restarted.json``: the same run after resume (that step
  started twice, with a new agent id).
"""

import json
import os
import re
import shutil
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.classify import Outcome, classify
from src.journal import (
    JournalStep,
    cached_label,
    find_run_id,
    last_activity,
    read_progress,
    read_return,
    read_run,
    read_steps,
    session_dir,
    slug,
    step_times,
    step_transcript_path,
    steps_in,
    workflow_run_dir,
)
from src.stream import WorkflowAck, WorkflowCall, WorkflowDone

FIXTURES = Path(__file__).parent / "fixtures"
# Fixture agent ids are placeholders (the shape Claude Code uses: "a" + 16 hex digits).
PLACEHOLDER_AGENT_ID = re.compile(r"a0{12}[0-9a-f]{4}")
SESSION = "00000000-0000-4000-8000-0000000000aa"
RUN = "wf_0b1533f1-219"
GATE_RUN = "wf_709b881b-781"  # the runId inside run_gate_resumed.json


def install(
    claude_home: Path,
    cwd: Path,
    journal: str | None,
    run_file: str | None = None,
    run_id: str = RUN,
    session: str = SESSION,
) -> Path:
    """Copy fixtures into the on-disk layout Claude Code uses; returns the session dir."""
    sdir = claude_home / "projects" / slug(os.path.realpath(cwd)) / session
    if journal is not None:
        wf = sdir / "subagents" / "workflows" / run_id
        wf.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / journal, wf / "journal.jsonl")
    if run_file is not None:
        (sdir / "workflows").mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / run_file, sdir / "workflows" / f"{run_id}.json")
    return sdir


@pytest.fixture
def cwd(tmp_path: Path) -> Path:
    d = tmp_path / "repo"
    d.mkdir()
    return d


@pytest.fixture
def home(tmp_path: Path) -> Path:
    h = tmp_path / "claude-home"
    h.mkdir()
    return h


# --- slug / session dir ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        # Observed pairs (Claude Code 2.1.328), parent directories replaced by a generic one.
        ("/zz/dir.with.dots_and_under", "-zz-dir-with-dots-and-under"),
        ("/zz/weird-chars@+~=,#%", "-zz-weird-chars-------"),
        ("/zz/MixedCase-ü-ñ", "-zz-MixedCase----"),
        ("/zz/aaa--bb..cc__dd", "-zz-aaa--bb--cc--dd"),
        ("/zz/emoji-😀-x", "-zz-emoji----x"),
        ("/zz/nfd-" + unicodedata.normalize("NFD", "ü"), "-zz-nfd--"),
    ],
)
def test_slug_observed_rules(path: str, expected: str) -> None:
    assert slug(path) == expected


def test_slug_long_path_hash_suffix() -> None:
    path = "/zz/" + "L" * 230 + "/deep.dir"
    result = slug(path)
    assert result == "-zz-" + "L" * 196 + "-4i5yai"
    assert len(result) == 200 + 1 + len("4i5yai")


def test_slug_long_path_counts_utf16_units() -> None:
    # Reference value from the spike's independent implementation (JS-style int32 hash).
    result = slug("/zz/" + "é😀" * 110)
    assert result == "-zz" + "-" * 197 + "-hja78g"


def test_slug_exactly_200_has_no_suffix() -> None:
    path = "/" + "a" * 199
    assert slug(path) == "-" + "a" * 199


def test_journal_dir_realpath_and_slug(tmp_path: Path, home: Path) -> None:
    real = tmp_path / "dir.with.dots_and_under"
    real.mkdir()
    link = tmp_path / "link.to_it"
    link.symlink_to(real)

    expected_slug = re.sub(r"[^a-zA-Z0-9]", "-", os.path.realpath(real))
    via_link = session_dir(str(link), SESSION, home)
    via_real = session_dir(str(real), SESSION, home)

    assert via_link == via_real == home / "projects" / expected_slug / SESSION
    assert via_link.parent.name.endswith("dir-with-dots-and-under")
    assert "link-to-it" not in via_link.parent.name


def test_session_dir_prefers_transcript_dir(cwd: Path, home: Path, tmp_path: Path) -> None:
    sdir = tmp_path / "elsewhere" / SESSION
    run_dir = sdir / "subagents" / "workflows" / RUN
    run_dir.mkdir(parents=True)

    assert session_dir(str(cwd), SESSION, home, transcript_dir=run_dir, run_id=RUN) == sdir
    # A transcript dir that does not exist falls back to the slug rule.
    missing = tmp_path / "nope" / SESSION / "subagents" / "workflows" / RUN
    assert session_dir(str(cwd), SESSION, home, transcript_dir=missing, run_id=RUN) == session_dir(
        str(cwd), SESSION, home
    )


# --- steps ----------------------------------------------------------------------------------


def test_read_steps_completed(cwd: Path, home: Path) -> None:
    install(home, cwd, "journal_completed.jsonl")

    steps = read_steps(str(cwd), SESSION, RUN, home)

    assert [s.label for s in steps] == ["stage-one", "stage-two"]
    one, two = steps
    assert one.status == "completed"
    assert one.result == "ok"
    assert one.phase == "One"
    assert one.key.startswith("v2:")
    assert two.status == "completed"
    assert two.result == {"ok": True, "n": 7}
    assert two.phase == "Two"


def test_read_steps_missing_file_is_empty(cwd: Path, home: Path) -> None:
    assert read_steps(str(cwd), SESSION, RUN, home) == []


def test_read_steps_tolerates_truncated_last_line_and_unknown_records(
    cwd: Path, home: Path
) -> None:
    sdir = install(home, cwd, "journal_completed.jsonl")
    path = sdir / "subagents" / "workflows" / RUN / "journal.jsonl"
    with path.open("a", encoding="utf-8") as fh:
        fh.write('{"type":"mystery","x":1}\n')
        fh.write("\n")
        fh.write("not json at all\n")
        fh.write('{"type":"started","key":"v2:zz","agentId":"a1","label":"stage-three","ph')

    steps = read_steps(str(cwd), SESSION, RUN, home)

    assert [s.label for s in steps] == ["stage-one", "stage-two"]


def test_read_steps_started_without_phase(cwd: Path, home: Path) -> None:
    install(home, cwd, "journal_gate_resumed.jsonl")

    steps = read_steps(str(cwd), SESSION, RUN, home)

    assert [s.label for s in steps] == ["s1", "s2", "interpret-s2-1", "s3", "interpret-s3-2", "s4"]
    assert all(s.phase is None for s in steps)
    assert all(s.status == "completed" and s.started_count == 1 for s in steps)


def test_interrupted_marks_running(cwd: Path, home: Path) -> None:
    # Real journal captured right after the process was killed mid-parallel().
    install(home, cwd, "journal_killed.jsonl")

    by_label = {s.label: s for s in read_steps(str(cwd), SESSION, RUN, home)}

    assert list(by_label) == ["s1", "r-a-1", "r-b-1"]
    assert by_label["s1"].status == "completed"
    assert by_label["r-a-1"].status == "completed"
    assert by_label["r-b-1"].status == "running"
    assert by_label["r-b-1"].result is None
    assert by_label["r-b-1"].agent_id == "a000000000000000d"


def test_started_count_and_agent_id(cwd: Path, home: Path) -> None:
    # r-b-1 was started, killed, then re-run on resume with a new agent id.
    install(home, cwd, "journal_restarted.jsonl")

    steps = read_steps(str(cwd), SESSION, RUN, home)
    by_label = {s.label: s for s in steps}

    assert [s.label for s in steps] == ["s1", "r-a-1", "r-b-1", "s2"]
    rb = by_label["r-b-1"]
    assert rb.started_count == 2
    assert rb.agent_id == "a0000000000000004"
    assert rb.status == "completed"
    assert isinstance(rb.result, str)
    assert rb.result.startswith("ok")
    assert by_label["s1"].started_count == 1
    assert by_label["s1"].agent_id == "a000000000000000b"


def test_result_matches_latest_started_only(cwd: Path, home: Path) -> None:
    sdir = install(home, cwd, None)
    wf = sdir / "subagents" / "workflows" / RUN
    wf.mkdir(parents=True)
    lines = [
        {"type": "launched"},
        {"type": "started", "key": "k1", "agentId": "a1", "label": "x"},
        {"type": "result", "key": "k1", "agentId": "a1", "result": "old"},
        {"type": "started", "key": "k2", "agentId": "a2", "label": "x"},
    ]
    (wf / "journal.jsonl").write_text("\n".join(json.dumps(r) for r in lines) + "\n")

    (step,) = read_steps(str(cwd), SESSION, RUN, home)

    assert step.status == "running"
    assert step.result is None
    assert step.started_count == 2
    assert step.agent_id == "a2"


# --- cached labels ----------------------------------------------------------------------------


def _step(status: str = "completed", started_count: int = 1, label: str = "s1") -> JournalStep:
    return JournalStep(
        key="k",
        label=label,
        phase=None,
        status=status,
        result="ok",
        started_count=started_count,
        agent_id="a1",
    )


def test_cached_label_from_stored_count() -> None:
    # Fallback path (no run-file progress): unchanged count on a completed step means cached.
    assert cached_label(1, _step(started_count=1)) is True
    assert cached_label(1, _step(started_count=2)) is False
    assert cached_label(0, _step(started_count=1)) is False
    assert cached_label(1, _step(status="running", started_count=1)) is False
    # Progress path: the run file's `cached` flag wins over the stored count.
    progress = [{"label": "s1", "agentId": "a1", "state": "done", "cached": True}]
    assert cached_label(0, _step(started_count=1), progress) is True
    live = [{"label": "s1", "agentId": "a1", "state": "done", "cached": None}]
    assert cached_label(1, _step(started_count=1), live) is False
    # A progress list without this label falls back to the count rule.
    other = [{"label": "other", "agentId": "a9", "state": "done", "cached": True}]
    assert cached_label(1, _step(started_count=1), other) is True
    assert cached_label(0, _step(started_count=1), other) is False


# --- run file: return value + progress --------------------------------------------------------


def test_read_return_final_record(cwd: Path, home: Path) -> None:
    install(home, cwd, "journal_completed.jsonl", "run_completed.json")

    assert read_return(str(cwd), SESSION, RUN, home) == {
        "first": "ok",
        "second": {"ok": True, "n": 7},
        "tag": "spike-c",
    }


def test_read_return_none_when_missing_or_not_completed(cwd: Path, home: Path) -> None:
    assert read_return(str(cwd), SESSION, RUN, home) is None

    sdir = install(home, cwd, None, "run_completed.json")
    run_path = sdir / "workflows" / f"{RUN}.json"
    data = json.loads(run_path.read_text())
    data["status"] = "running"
    run_path.write_text(json.dumps(data))

    assert read_return(str(cwd), SESSION, RUN, home) is None


def test_read_run_missing_or_unparseable(cwd: Path, home: Path) -> None:
    assert read_run(str(cwd), SESSION, RUN, home) is None

    sdir = install(home, cwd, None, "run_completed.json")
    run = read_run(str(cwd), SESSION, RUN, home)
    assert run is not None
    assert run["status"] == "completed"
    assert run["runId"] == RUN

    (sdir / "workflows" / f"{RUN}.json").write_text('{"status": "comp')
    assert read_run(str(cwd), SESSION, RUN, home) is None
    assert read_return(str(cwd), SESSION, RUN, home) is None
    assert read_progress(str(cwd), SESSION, RUN, home) == []


def test_read_progress_marks_cached(cwd: Path, home: Path) -> None:
    install(home, cwd, "journal_gate_resumed.jsonl", "run_gate_resumed.json", run_id=GATE_RUN)

    progress = read_progress(str(cwd), SESSION, GATE_RUN, home)
    by_label = {p["label"]: p for p in progress}

    assert list(by_label) == ["s1", "s2", "interpret-s2-1", "s3", "interpret-s3-2", "s4"]
    assert [bool(by_label[k].get("cached")) for k in by_label] == [
        True,
        True,
        True,
        True,
        False,
        False,
    ]
    assert by_label["s1"]["agentId"] == "a0000000000000006"
    assert by_label["s1"]["state"] == "done"


def test_read_progress_skips_phase_entries(cwd: Path, home: Path) -> None:
    install(home, cwd, None, "run_completed.json")

    progress = read_progress(str(cwd), SESSION, RUN, home)

    assert [p["label"] for p in progress] == ["stage-one", "stage-two"]


def test_cached_label_end_to_end_on_resume(cwd: Path, home: Path) -> None:
    install(home, cwd, "journal_gate_resumed.jsonl", "run_gate_resumed.json", run_id=GATE_RUN)
    steps = read_steps(str(cwd), SESSION, GATE_RUN, home)
    progress = read_progress(str(cwd), SESSION, GATE_RUN, home)

    cached = [s.label for s in steps if cached_label(1, s, progress)]

    assert cached == ["s1", "s2", "interpret-s2-1", "s3"]


# --- run id / transcripts ---------------------------------------------------------------------


def test_find_run_id_newest(cwd: Path, home: Path) -> None:
    assert find_run_id(str(cwd), SESSION, home) is None

    sdir = install(home, cwd, "journal_completed.jsonl", run_id="wf_old-111")
    install(home, cwd, "journal_completed.jsonl", run_id="wf_new-222")
    base = sdir / "subagents" / "workflows"
    os.utime(base / "wf_old-111" / "journal.jsonl", (1_000, 1_000))
    os.utime(base / "wf_new-222" / "journal.jsonl", (2_000, 2_000))
    (base / "not-a-run").mkdir()
    os.utime(base / "not-a-run", (3_000, 3_000))

    assert find_run_id(str(cwd), SESSION, home) == "wf_new-222"


def test_step_transcript_path(cwd: Path, home: Path) -> None:
    sdir = install(home, cwd, "journal_completed.jsonl")

    path = step_transcript_path(str(cwd), SESSION, RUN, home, "a0000000000000008")

    assert path == sdir / "subagents" / "workflows" / RUN / "agent-a0000000000000008.jsonl"


def test_transcript_dir_override_for_readers(cwd: Path, home: Path, tmp_path: Path) -> None:
    # The run lives under a different slug than the cwd would give; transcript_dir wins.
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()
    sdir = install(home, other_cwd, "journal_completed.jsonl", "run_completed.json")
    transcript_dir = sdir / "subagents" / "workflows" / RUN

    assert read_steps(str(cwd), SESSION, RUN, home) == []
    steps = read_steps(str(cwd), SESSION, RUN, home, transcript_dir=transcript_dir)
    assert len(steps) == 2
    assert read_return(str(cwd), SESSION, RUN, home, transcript_dir=transcript_dir) is not None
    assert len(read_progress(str(cwd), SESSION, RUN, home, transcript_dir=transcript_dir)) == 2
    assert read_run(str(cwd), SESSION, RUN, home, transcript_dir=transcript_dir) is not None
    assert (
        step_transcript_path(str(cwd), SESSION, RUN, home, "a1", transcript_dir=transcript_dir)
        == transcript_dir / "agent-a1.jsonl"
    )


# --- activity ---------------------------------------------------------------------------------


def test_last_activity_recursive(cwd: Path, home: Path) -> None:
    assert last_activity(str(cwd), SESSION, home) is None

    sdir = install(home, cwd, "journal_completed.jsonl", "run_completed.json")
    wf = sdir / "subagents" / "workflows" / RUN
    agent = wf / "agent-a1.jsonl"
    agent.write_text("{}\n")
    transcript = sdir.parent / f"{SESSION}.jsonl"
    transcript.write_text("{}\n")

    for path in (*wf.iterdir(), sdir / "workflows" / f"{RUN}.json", transcript):
        os.utime(path, (1_000, 1_000))
    assert last_activity(str(cwd), SESSION, home) == 1_000

    os.utime(agent, (5_000, 5_000))
    assert last_activity(str(cwd), SESSION, home) == 5_000

    os.utime(transcript, (9_000, 9_000))
    assert last_activity(str(cwd), SESSION, home) == 9_000


def test_last_activity_none_for_empty_dir(cwd: Path, home: Path) -> None:
    session_dir(str(cwd), SESSION, home).mkdir(parents=True)

    assert last_activity(str(cwd), SESSION, home) is None


# --- Task 3.4 fix round 1: run-file and transcript-dir guards ----------------------------------


def test_read_return_none_for_failed_run_and_attempt_transient(cwd: Path, home: Path) -> None:
    sdir = install(home, cwd, None, "run_completed.json")
    run_path = sdir / "workflows" / f"{RUN}.json"
    data = json.loads(run_path.read_text())
    data["status"] = "failed"
    run_path.write_text(json.dumps(data))
    assert read_run(str(cwd), SESSION, RUN, home) is not None
    assert read_return(str(cwd), SESSION, RUN, home) is None
    tid = "toolu_1"
    events = [
        WorkflowCall(tool_use_id=tid, input={"scriptPath": "/s.js", "args": {}}),
        WorkflowAck(tid, RUN, "w1", None, False, None),
        WorkflowDone(tool_use_id=tid, status="completed"),
    ]
    outcome, _ = classify(
        exit_code=0, events=events, workflow_return=read_return(str(cwd), SESSION, RUN, home),
        stderr_tail="", stage_error_records=[], kind="lifecycle",
    )  # fmt: skip
    assert outcome is Outcome.transient  # never completed


def test_read_run_rejects_another_runs_file(cwd: Path, home: Path) -> None:
    sdir = install(home, cwd, None, "run_completed.json")
    run_path = sdir / "workflows" / f"{RUN}.json"
    data = json.loads(run_path.read_text())
    data["runId"] = "wf_other-000"
    run_path.write_text(json.dumps(data))
    assert read_run(str(cwd), SESSION, RUN, home) is None
    assert read_return(str(cwd), SESSION, RUN, home) is None


def test_transcript_dir_must_name_the_run_under_this_session(
    cwd: Path, home: Path, tmp_path: Path
) -> None:
    fallback = session_dir(str(cwd), SESSION, home) / "subagents" / "workflows" / RUN
    good = tmp_path / "x" / SESSION / "subagents" / "workflows" / RUN
    wrong_run = tmp_path / "x" / SESSION / "subagents" / "workflows" / "wf_other-000"
    wrong_session = tmp_path / "x" / "other-session" / "subagents" / "workflows" / RUN
    wrong_shape = tmp_path / "x" / SESSION / "elsewhere" / RUN
    for path in (good, wrong_run, wrong_session, wrong_shape):
        path.mkdir(parents=True)
    assert workflow_run_dir(str(cwd), SESSION, RUN, home, good) == good.resolve()
    for bad in (wrong_run, wrong_session, wrong_shape):
        assert workflow_run_dir(str(cwd), SESSION, RUN, home, bad) == fallback
        assert session_dir(str(cwd), SESSION, home, bad, RUN) == session_dir(
            str(cwd), SESSION, home
        )
    dotted = good.parent / "wf_other-000" / ".." / RUN  # resolved before the name check
    assert workflow_run_dir(str(cwd), SESSION, RUN, home, dotted) == good.resolve()


def test_steps_in_reads_a_copied_journal(cwd: Path, home: Path, tmp_path: Path) -> None:
    install(home, cwd, "journal_completed.jsonl")
    copy = tmp_path / "copy"
    copy.mkdir()
    shutil.copy(FIXTURES / "journal_completed.jsonl", copy / "journal.jsonl")
    assert steps_in(copy) == read_steps(str(cwd), SESSION, RUN, home) != []


def test_fixture_agent_ids_are_placeholders() -> None:
    ids: set[str] = set()
    for path in [*FIXTURES.glob("journal_*.jsonl"), *FIXTURES.glob("run_*.json")]:
        ids |= set(re.findall(r'"agentId": ?"([^"]*)"', path.read_text()))
    assert ids and all(PLACEHOLDER_AGENT_ID.fullmatch(i) for i in ids), sorted(ids)


def _write_records(path: Path, records: list[dict[str, object]]) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    return path


def test_step_times_first_last_timestamp(tmp_path: Path) -> None:
    transcript = _write_records(
        tmp_path / "agent-a1.jsonl",
        [
            {"type": "user", "timestamp": "2026-01-02T19:40:01Z"},
            {"type": "assistant", "timestamp": "2026-01-02T19:40:05.500Z"},
            {"type": "attachment", "timestamp": "2026-01-02T19:40:09Z"},
        ],
    )
    first, last = step_times(transcript)
    assert first == datetime(2026, 1, 2, 19, 40, 1, tzinfo=UTC).timestamp()
    assert last == datetime(2026, 1, 2, 19, 40, 9, tzinfo=UTC).timestamp()


def test_step_times_missing_or_unparseable(tmp_path: Path) -> None:
    assert step_times(tmp_path / "absent.jsonl") == (None, None)
    junk = tmp_path / "junk.jsonl"
    junk.write_text("not json\n{}\n", encoding="utf-8")
    assert step_times(junk) == (None, None)
    assert step_times(_write_records(tmp_path / "empty.jsonl", [])) == (None, None)


def test_step_times_truncated_last_line_is_none(tmp_path: Path) -> None:
    transcript = tmp_path / "agent-a1.jsonl"
    transcript.write_text(
        json.dumps({"timestamp": "2026-01-02T19:40:01Z"})
        + "\n"
        + '\n{"timestamp": "2026-01-02T19:4',
        encoding="utf-8",
    )
    first, last = step_times(transcript)
    assert first == datetime(2026, 1, 2, 19, 40, 1, tzinfo=UTC).timestamp()
    assert last is None
