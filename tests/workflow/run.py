"""Run a Workflow script under the Node test harness and return its recorded output."""

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

HARNESS = Path(__file__).with_name("harness.mjs")
ERROR_PREFIX = "workflow error: "
TIMEOUT_S = 60


class WorkflowHarnessError(RuntimeError):
    """The harness or the script under test failed; the message is the script's error."""


def run_workflow(script: Path | str, args: Any, responses: dict[str, list[Any]]) -> dict[str, Any]:
    """Execute ``script`` with ``args`` and per-label ``responses`` queues.

    Returns ``{"calls": [...], "return": ...}``; ``calls`` is in call order.
    """
    with tempfile.TemporaryDirectory() as tmp:
        args_file = Path(tmp) / "args.json"
        responses_file = Path(tmp) / "responses.json"
        args_file.write_text(json.dumps(args), encoding="utf-8")
        responses_file.write_text(json.dumps(responses), encoding="utf-8")
        try:
            proc = subprocess.run(
                ["node", str(HARNESS), str(script), str(args_file), str(responses_file)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                stdin=subprocess.DEVNULL,
                timeout=TIMEOUT_S,
            )
        except subprocess.TimeoutExpired as exc:
            raise WorkflowHarnessError(f"workflow timed out after {TIMEOUT_S}s") from exc
    if proc.returncode != 0:
        raise WorkflowHarnessError(_error_message(proc.stderr, proc.returncode))
    output: dict[str, Any] = json.loads(proc.stdout)
    return output


def _error_message(stderr: str, returncode: int) -> str:
    """The script's error message (the harness's marker line), else all of stderr."""
    for line in stderr.splitlines():
        if line.startswith(ERROR_PREFIX):
            return line[len(ERROR_PREFIX) :]
    return stderr.strip() or f"node exited {returncode}"
