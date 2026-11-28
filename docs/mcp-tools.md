# MCP tools and task lifecycle

## MCP tools

The first five tools keep the parameters of the tools of the same name in [orcai-mcp](https://github.com/quieromas-ai/orcai-mcp) (another
MCP server for delegating work to Claude Code agents), so a caller written for it works
unchanged.

| Tool | Purpose |
|---|---|
| `delegate_task(agent, description, input_context?, priority=3, max_retries=0)` | Queue a task for an agent or a role alias; returns at once. |
| `check_task_status(task_id)` | Status, output, current stage, steps, resumes, question, cost, recovery state, attempts per launch mode. |
| `prompt_agent(agent, message, context?, wait=true)` | Ad-hoc single task, never deduplicated; `wait` blocks up to 300 s. |
| `get_agents(role?, status?)` | Configured agents, filtered by role or `idle`/`busy`. |
| `get_active_agents()` | Agents running a task, and the queue depth. |
| `answer_task(task_id, answer)` | Answer a lifecycle task in `needs_human`; it resumes in the same session. |
| `cancel_task(task_id)` | Cancel a queued, running or `needs_human` task. It is never resumed. |
| `list_tasks(status?, agent?, limit=20, cursor?)` | Tasks, newest first, paged by cursor. |
| `get_task_logs(task_id, source?, cursor?, limit=200, full=false)` | Page through a task's logs. |
| `get_task_debug(task_id)` | Attempts, steps, the workflow call, launch details, timeline, bundle URL. |

`priority` runs from 1 (lowest) to 5 (highest). `max_retries` adds resumes to the task's budget;
a retry is always a resume of the same task, never a new task.

`input_context` keys that dioxide reads:

| Key | Meaning |
|---|---|
| `project_dir` | The git repo to work in. Defaults to `default_project_dir`. Fixed at submit. |
| `base_branch` | The branch to merge into. Defaults to the repo's current branch at submit. |
| `plan_gate` | `true` asks for plan approval before setup. Off by default. |
| `approved_plan` | A plan to pin: the `plan` stage is skipped. |

The whole description and `input_context` are written to the task's read-only brief, which the
stages read.

## Task lifecycle and asks

Statuses: `queued` → `running` → `completed`, `failed` or `cancelled`, plus `needs_human`.

A lifecycle task enters `needs_human` in two ways:

- **Gate asks**: plan approval (with `plan_gate`), review or QA rounds exhausted, or a merge
  that `merge.sh` refused (for example a conflict or a base that kept moving).
- **Stage asks**: any agent stage can return a question.

The question is in `check_task_status` (`question`, and `output.text` starts with
`NEEDS_HUMAN:`). Answer with `answer_task`; an answer is at most 64 KiB. The task resumes the
same Claude session and workflow run. Finished stages replay from Claude's workflow cache. For
a gate ask, an `interpret` step reads your answer and decides to continue, amend (re-plan with
your changes) or abort. For a stage ask, that stage runs again with your answer.

Only lifecycle tasks ask. A single task never enters `needs_human`: it ends `completed`,
`failed` or `cancelled`. `answer_task` on a task that is not waiting returns an error and
changes nothing.
