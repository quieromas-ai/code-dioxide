"""Answering a lifecycle task that waits in ``needs_human``.

The answer text goes to ``inputs/answers/<k>.md`` (read-only); the args only grow by one
appended ``{k, label, mode}``; ``k`` is unique per task (never reused after a fresh start). The
status compare-and-set ``needs_human -> queued`` and the append happen in one write transaction,
so concurrent answers and scheduler passes relaunch the task exactly once; the relaunch resumes
the same session and the same workflow run.
"""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

from src.database import write_tx
from src.engine_lifecycle import answer_mode
from src.tasks import CAUSE_ANSWER, Task

if TYPE_CHECKING:
    from src.engine import TaskEngine

NOT_YET = "it will ask when it needs you"
MAX_ANSWER_BYTES = 65536  # UTF-8


async def answer(engine: "TaskEngine", task_id: str, text: Any, source: str) -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        return {"task_id": task_id, "error": "answer must be a non-empty string"}
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:  # a lone surrogate
        return {"task_id": task_id, "error": "answer is not valid UTF-8 text"}
    if size > MAX_ANSWER_BYTES:
        error = f"answer is {size} bytes; the limit is {MAX_ANSWER_BYTES} bytes (UTF-8)"
        return {"task_id": task_id, "error": error}
    async with write_tx() as db:
        row = await engine._fetch(db, task_id)
        if row is None:
            return {"task_id": task_id, "error": f"task {task_id} not found"}
        task = Task.from_row(row)
        if task.status in ("queued", "running"):
            return {"task_id": task_id, "status": task.status,
                    "error": f"task is {task.status} — {NOT_YET}"}  # fmt: skip
        if task.status != "needs_human":
            return {"task_id": task_id, "status": task.status, "error": f"task is {task.status}"}
        question = json.loads(task.question_json) if task.question_json else None
        if (
            not task.is_lifecycle
            or not isinstance(question, dict)
            or not isinstance(question.get("stage"), str)
        ):
            error = "task has no pending workflow question"
            return {"task_id": task_id, "status": task.status, "error": error}
        args = task.args_dict
        answers = list(args.get("answers") or [])
        # k names the answer file (answers/<k>.md), the interpret step (interpret-<ask>-<k>) and
        # the epoch suffix (@a<k>). It is unique per task, also across fresh starts (a fresh run
        # starts with no answers; earlier files and rows stay as history, never overwritten).
        async with db.execute(
            "SELECT MAX(k) AS k FROM answers WHERE task_id = ?", (task.id,)
        ) as cur:
            last = await cur.fetchone()
        k = max([int(last["k"] or 0) if last else 0, *(int(a["k"]) for a in answers)]) + 1
        entry = {"k": k, "label": question["stage"], "mode": answer_mode(question.get("kind"))}
        args_json = json.dumps({**args, "answers": [*answers, entry]}, ensure_ascii=False)
        # Written before the commit: a relaunch never sees args that name a missing file. A
        # rollback leaves the file behind; the next answer gets the same k and replaces it.
        try:
            await asyncio.to_thread(engine.runstore.write_answer, task.id, k, text)
        except OSError as exc:  # nothing else is written: the task keeps waiting
            error = f"could not store the answer: {type(exc).__name__}: {exc}"
            return {"task_id": task_id, "status": task.status, "error": error}
        now = time.time()
        await db.execute(
            "UPDATE tasks SET status = 'queued', resume_mode = 'resume', resume_cause = ?, "
            "args_json = ?, "
            "question_json = NULL, output_json = NULL, error = NULL, completed_at = NULL, "
            "next_attempt_at = NULL, updated_at = ? "
            "WHERE id = ? AND status = 'needs_human' AND attempt = ?",
            (CAUSE_ANSWER, args_json, now, task.id, task.attempt),
        )
        await db.execute(
            "INSERT INTO answers (task_id, k, label, mode, question, answer, source, at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (task.id, k, entry["label"], entry["mode"], question.get("question"), text, source,
             now),
        )  # fmt: skip
        await engine._event(db, task.id, "answered", {**entry, "source": source})
    engine._wake.set()
    return {"task_id": task_id, "status": "queued", "k": k}
