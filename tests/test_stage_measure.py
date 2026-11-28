"""Server-side stage measurement: transcript usage, files changed, the mirror's writes."""

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from src import engine_lifecycle
from src.database import write_tx
from src.journal import JournalStep, StageModelUsage, step_transcript_path, step_usage
from src.stage_measure import StepRun, attempt_at, files_changed, write_usage
from tests.helpers import Env, rows
from tests.reports_fixture import insert_attempt
from tests.test_engine_lifecycle import lifecycle


def _turn(msg_id: str, model: str, **usage: Any) -> dict[str, Any]:
    return {"type": "assistant", "message": {"id": msg_id, "model": model, "usage": usage}}


def _write(path: Path, records: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    return path


SPLIT = {"ephemeral_5m_input_tokens": 30, "ephemeral_1h_input_tokens": 70}


def test_step_usage_dedupes_turns_and_splits_cache_writes(tmp_path: Path) -> None:
    first = _turn("m1", "opus", input_tokens=2, output_tokens=10, cache_read_input_tokens=5,
                  cache_creation_input_tokens=100, cache_creation=SPLIT)  # fmt: skip
    path = _write(tmp_path / "t.jsonl", [
        {"type": "user", "message": {"content": "go"}},
        first, first,  # one turn written once per content block
        _turn("m2", "opus", input_tokens=1, output_tokens=4, cache_creation_input_tokens=8),
        _turn("m3", "haiku", output_tokens=7),
        {"type": "assistant", "message": {"id": "m4"}},  # no model/usage: skipped
    ])  # fmt: skip
    path.write_text(path.read_text() + "{not json\n", encoding="utf-8")
    assert step_usage(path) == {
        "haiku": StageModelUsage(0, 7, 0, 0, 0, 1),
        "opus": StageModelUsage(3, 14, 5, 38, 70, 2),  # m2's 8 writes, no split: all 5-minute
    }
    assert step_usage(tmp_path / "missing.jsonl") is None


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )
    return done.stdout.strip()


def test_files_changed_between_heads(tmp_path: Path) -> None:
    repo = tmp_path / "r"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "t")
    (repo / "keep.txt").write_text("a")
    (repo / "gone.txt").write_text("b")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "one")
    before = _git(repo, "rev-parse", "HEAD")
    (repo / "keep.txt").write_text("changed")
    (repo / "gone.txt").unlink()
    (repo / "new.txt").write_text("c")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "two")
    after = _git(repo, "rev-parse", "HEAD")
    assert files_changed(str(repo), before, after) == [
        {"status": "D", "path": "gone.txt"}, {"status": "M", "path": "keep.txt"},
        {"status": "A", "path": "new.txt"},
    ]  # fmt: skip
    assert files_changed(str(repo), after, after) == []  # a review: head unchanged
    assert files_changed(str(repo), None, after) is None  # no previous head
    assert files_changed(str(tmp_path / "nope"), before, after) is None  # no worktree
    assert files_changed(str(repo), "0" * 40, after) is None  # the diff fails


USAGE = {"opus": StageModelUsage(1, 2, 3, 4, 5, 1)}


async def _step_models(task_id: str) -> list[tuple[Any, ...]]:
    found = await rows(
        "SELECT label, agent_id, run_id, attempt_n, model, output_tokens FROM step_models "
        "WHERE task_id = ? ORDER BY label, agent_id", task_id,
    )  # fmt: skip
    return [tuple(r) for r in found]


async def _sums(task_id: str, label: str) -> tuple[Any, ...]:
    (row,) = await rows(
        "SELECT billed_input_tokens, billed_output_tokens, billed_cache_read_tokens, "
        "billed_cache_write_tokens, turns FROM steps WHERE task_id = ? AND label = ?",
        task_id, label,
    )  # fmt: skip
    return tuple(row)


async def test_write_usage_keeps_attempt_and_sums_current_run(env: Env) -> None:
    task_id = await lifecycle(env)
    step = JournalStep("k", "implement", "Implement", "completed", {}, 1, "a1")
    async with write_tx() as db:
        await engine_lifecycle._upsert(db, task_id, 1, step, None, [], (None, None))
        await write_usage(db, StepRun(task_id, "implement", "wf_1", 1), "a1", USAGE)
    assert await _sums(task_id, "implement") == (1, 2, 3, 9, 1)
    # A re-parse (final mirror, a later attempt) updates the numbers, never the attempt.
    bigger = {"opus": StageModelUsage(1, 20, 3, 4, 5, 2)}
    async with write_tx() as db:
        await write_usage(db, StepRun(task_id, "implement", "wf_1", 2), "a1", bigger)
    assert await _step_models(task_id) == [("implement", "a1", "wf_1", 1, "opus", 20)]
    # A restart (new agent, same run) adds its usage to the step's sums.
    async with write_tx() as db:
        await write_usage(db, StepRun(task_id, "implement", "wf_1", 2), "a2", USAGE)
    assert await _sums(task_id, "implement") == (2, 22, 6, 18, 3)
    # A fresh start's new run does not inherit the old run's usage.
    async with write_tx() as db:
        await write_usage(db, StepRun(task_id, "implement", "wf_2", 3), "b1", USAGE)
    assert await _sums(task_id, "implement") == (1, 2, 3, 9, 1)
    assert len(await _step_models(task_id)) == 3  # the attempts keep their stage usage


@pytest.mark.parametrize("final_mirror", [False, True])
async def test_measured_reads_usage_once_unless_final(env: Env, final_mirror: bool) -> None:
    task_id = await lifecycle(env)
    home, cwd, session, run_id = env.settings.claude_home, str(env.repo), "sess", "wf_run"
    live = step_transcript_path(cwd, session, run_id, home, "a1")
    _write(live, [_turn("m1", "opus", output_tokens=5)])
    step = JournalStep("k", "implement", "Implement", "completed", {}, 1, "a1")
    where = engine_lifecycle._RunPlace(cwd, session, run_id, home, None)
    store = env.engine.runstore
    first = engine_lifecycle._measured(store, task_id, [step], {}, where, False)
    assert first["implement"].usage == {"opus": StageModelUsage(0, 5, 0, 0, 0, 1)}
    # Usage is there on the first tick after completion, before any run-store copy exists.
    assert not store.step_copy_path(task_id, "implement").exists()
    async with write_tx() as db:
        await engine_lifecycle._upsert(db, task_id, 1, step, None, [], (None, None))
    found = {
        r["label"]: dict(r) for r in await rows("SELECT * FROM steps WHERE task_id = ?", task_id)
    }
    # The transcript grew after the first read (its last turn was flushed late).
    _write(live, [_turn("m1", "opus", output_tokens=5), _turn("m2", "opus", output_tokens=7)])
    later = engine_lifecycle._measured(store, task_id, [step], found, where, final_mirror)
    if final_mirror:
        assert later["implement"].usage == {"opus": StageModelUsage(0, 12, 0, 0, 0, 2)}
        assert later["implement"].times == (None, None)  # times stay final
    else:
        assert later == {}  # final in the DB: not reopened


def test_measured_prefers_live_over_stale_copy(env: Env) -> None:
    store = env.engine.runstore
    home, cwd, session, run_id = env.settings.claude_home, str(env.repo), "sess", "wf_run"
    task_id = "t-copy"
    copy = store.step_copy_path(task_id, "implement")
    _write(copy, [_turn("old", "opus", output_tokens=99)])  # the previous agent's transcript
    live = step_transcript_path(cwd, session, run_id, home, "a2")
    _write(live, [_turn("new", "opus", output_tokens=1)])
    step = JournalStep("k", "implement", "Implement", "completed", {}, 2, "a2")
    where = engine_lifecycle._RunPlace(cwd, session, run_id, home, None)
    measured = engine_lifecycle._measured(store, task_id, [step], {}, where, False)
    assert measured["implement"].usage == {"opus": StageModelUsage(0, 1, 0, 0, 0, 1)}


def test_step_usage_refuses_another_agents_copy(tmp_path: Path) -> None:
    turn = {**_turn("m1", "opus", output_tokens=5), "agentId": "old-agent"}
    path = _write(tmp_path / "copy.jsonl", [turn])
    assert step_usage(path, "new-agent") is None
    assert step_usage(path, "old-agent") == {"opus": StageModelUsage(0, 5, 0, 0, 0, 1)}


async def test_attempt_at_places_a_late_observed_step(env: Env) -> None:
    task_id = await lifecycle(env)
    for n, started in ((1, 100.0), (2, 500.0)):
        await insert_attempt(task_id, n, started_at=started, ended_at=started + 50)
    async with write_tx() as db:
        assert await attempt_at(db, task_id, 120.0, 2) == 1  # ran in 1, first seen by 2
        assert await attempt_at(db, task_id, 600.0, 2) == 2
        assert await attempt_at(db, task_id, None, 2) == 2
        assert await attempt_at(db, task_id, 10.0, 2) == 2  # before any attempt: the observer


def test_files_changed_of_newly_done_steps(tmp_path: Path) -> None:
    rows_ = {
        "plan": {"status": "completed", "git_head": "h1", "finished_at": 1.0},
        "implement": {"status": "completed", "git_head": "h2", "finished_at": 2.0},
        "review": {"status": "running", "git_head": None, "finished_at": None},
    }
    seen: list[tuple[Any, ...]] = []

    def fake(worktree: Any, before: Any, after: Any) -> list[Any]:
        seen.append((worktree, before, after))
        return []

    step = JournalStep("k", "review", "Review", "completed", {}, 1, "a")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("src.stage_measure.files_changed", fake)
        assert engine_lifecycle._files_changed("/wt", [step], rows_, "h3") == {"review": []}
        assert engine_lifecycle._files_changed("/wt", [], rows_, "h3") == {}
    assert seen == [("/wt", "h2", "h3")]  # from the latest completed step's head
    assert engine_lifecycle._files_changed("/wt", [step], {}, "h3") == {"review": None}
