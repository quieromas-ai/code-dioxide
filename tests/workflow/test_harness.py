import json
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.workflow import run
from tests.workflow.run import HARNESS, WorkflowHarnessError, run_workflow

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required for the workflow harness tests"
)


def _script(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "wf.js"
    path.write_text(
        "export const meta = { name: 't', description: 'test' }\n" + body, encoding="utf-8"
    )
    return path


def _json(tmp_path: Path, name: str, value: object) -> str:
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def test_harness_trivial(tmp_path: Path) -> None:
    script = _script(
        tmp_path,
        """
phase('One')
const a = await agent('first ' + args.who, {label: 'a', phase: 'One', agentType: 'x'})
const b = await agent('second', {label: 'a', schema: {type: 'object', properties: {}}})
const c = await agent('third', {label: 'missing'})
return {a, b, c}
""",
    )
    out = run_workflow(script, {"who": "me"}, {"a": ["r1", {"k": 2}]})
    assert out["return"] == {"a": "r1", "b": {"k": 2}, "c": None}
    assert [c["prompt"] for c in out["calls"]] == ["first me", "second", "third"]
    assert out["calls"][0] == {
        "label": "a",
        "agentType": "x",
        "model": None,
        "phase": "One",
        "prompt": "first me",
        "schema": None,
        "optionKeys": ["agentType", "label", "phase"],
    }
    assert out["calls"][1]["optionKeys"] == ["label", "schema"]
    assert out["calls"][1]["schema"] == {"type": "object", "properties": {}}


def test_harness_forbids_date_now(tmp_path: Path) -> None:
    script = _script(tmp_path, "return Date.now()")
    with pytest.raises(WorkflowHarnessError, match=r"Date\.now\(\)"):
        run_workflow(script, None, {})
    with pytest.raises(WorkflowHarnessError, match=r"Math\.random\(\)"):
        run_workflow(_script(tmp_path, "return Math.random()"), None, {})
    with pytest.raises(WorkflowHarnessError, match=r"new Date\(\)"):
        run_workflow(_script(tmp_path, "return new Date()"), None, {})


def test_harness_script_error_surfaces_message(tmp_path: Path) -> None:
    with pytest.raises(WorkflowHarnessError, match="boom"):
        run_workflow(_script(tmp_path, "throw new Error('boom')"), None, {})


def test_harness_parallel_and_pipeline_null_on_throw(tmp_path: Path) -> None:
    script = _script(
        tmp_path,
        """
const par = await parallel([
  () => agent('p1', {label: 'p'}),
  () => { throw new Error('x') },
  () => agent('p2', {label: 'p'}),
])
const pipe = await pipeline(
  [1, 2, 3],
  (item) => agent('s1-' + item, {label: 's1'}),
  (prev, item, i) => { if (item === 2) throw new Error('y'); return [prev, item, i] },
)
return {par, pipe}
""",
    )
    out = run_workflow(script, None, {"p": ["A", "B"], "s1": ["x", "y", "z"]})
    assert out["return"] == {"par": ["A", None, "B"], "pipe": [["x", 1, 0], None, ["z", 3, 2]]}
    assert [c["prompt"] for c in out["calls"]] == ["p1", "p2", "s1-1", "s1-2", "s1-3"]


def test_harness_phase_tracks_current_and_opts_override(tmp_path: Path) -> None:
    script = _script(
        tmp_path,
        """
await agent('before')
phase('X')
await agent('in x')
await agent('override', {phase: 'Y'})
await agent('still x')
""",
    )
    out = run_workflow(script, None, {})
    assert [c["phase"] for c in out["calls"]] == [None, "X", "Y", "X"]


@pytest.mark.parametrize("bad", ["text", {"k": 1}, None])
def test_harness_rejects_non_array_responses(tmp_path: Path, bad: object) -> None:
    with pytest.raises(WorkflowHarnessError, match='responses for label "a" must be an array'):
        run_workflow(_script(tmp_path, "return 1"), None, {"a": bad})


def test_harness_script_console_output_does_not_corrupt_result(tmp_path: Path) -> None:
    script = _script(tmp_path, "console.log('noise'); console.error('more'); return {ok: true}")
    assert run_workflow(script, None, {})["return"] == {"ok": True}


def test_harness_script_error_has_stack_on_stderr(tmp_path: Path) -> None:
    script = _script(tmp_path, "throw new Error('boom')")
    proc = subprocess.run(
        ["node", str(HARNESS), str(script), _json(tmp_path, "a", None), _json(tmp_path, "r", {})],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert proc.returncode == 1
    assert proc.stderr.startswith("workflow error: boom\n")
    assert "at " in proc.stderr


def test_harness_cli_prints_result_json_on_stdout(tmp_path: Path) -> None:
    script = _script(tmp_path, "return await agent('hi', {label: 'l'})")
    proc = subprocess.run(
        [
            "node",
            str(HARNESS),
            str(script),
            _json(tmp_path, "a", None),
            _json(tmp_path, "r", {"l": ["out"]}),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert json.loads(proc.stdout)["return"] == "out"


def test_harness_date_with_arguments_still_works(tmp_path: Path) -> None:
    script = _script(
        tmp_path,
        "return [new Date(0).toISOString(), Date.parse('1970-01-02T00:00:00Z'),"
        " Date.UTC(1970, 0, 3)]",
    )
    out = run_workflow(script, None, {})
    assert out["return"] == ["1970-01-01T00:00:00.000Z", 86_400_000, 172_800_000]


def test_harness_timeout_is_wrapped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run, "TIMEOUT_S", 0.5)
    script = _script(tmp_path, "await new Promise(() => setTimeout(() => {}, 5000))")
    with pytest.raises(WorkflowHarnessError, match="timed out"):
        run_workflow(script, None, {})
