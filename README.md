# code-dioxide

code-dioxide is a local MCP server that runs delegated work as headless Claude Code
sessions. A caller (a Claude Code session or a person) sends one `delegate_task`. dioxide then
runs the whole feature loop as a Claude Code workflow script: plan, set up a git worktree,
implement, review, QA, optional deploy, and merge. If the run needs a human decision, it waits,
and the answer continues the same session and the same workflow run.

Task state is stored in SQLite. Every attempt (one launch of `claude` for a task) has its
output kept on disk, so a crash, a rate limit or a server restart resumes the work in a new
attempt instead of starting over. Everything that happened
can be read through the API without shell access to the host.

Status: alpha (0.1.0). The background service is macOS only.

## How it works

- **Lifecycle tasks.** A task whose agent resolves to `lifecycle_agent` (see `roles.yaml`) runs
  `workflows/feature-lifecycle.js` in one `claude -p` session. The parent session only calls
  the Workflow tool. Each stage is an agent with a structured output schema, and agents never
  delegate to each other.
- **Single tasks.** Any other agent runs as `claude -p --agent <slug>` with the task brief.
- **Your config, not dioxide's.** Permissions, models, effort, tools, skills, hooks, review
  focus and commit conventions come from your Claude Code settings, your agent files and the
  target repo's CLAUDE.md. The only permission dioxide adds is `--allowedTools Workflow`.

Stages of the lifecycle workflow:

| Stage | Role | What it does |
|---|---|---|
| `plan` | planner | Steps, acceptance criteria, whether a deploy is needed. Skipped when a plan is pinned. |
| `setup` | none | Runs `setup.sh`: creates or reuses the task branch and worktree. |
| `implement` | implementer | Commits the change and its tests on the task branch. |
| `review-r{n}-{reviewer}` | each of `reviewers`, in parallel | Approves or returns findings; `fix-r{n}` (implementer) addresses them. |
| `qa-r{n}` | qa | Gate green or red; `qa-fix-r{n}` (implementer) on red. |
| `deploy` | deployer | Only when the plan says a deploy is needed and a deployer role exists. |
| `merge` | none | Runs `merge.sh`: merges the QA-approved head into the base branch. |

Worktrees live outside the target repo, under `<home>/worktrees/`.

## Requirements

- Python 3.12 or later.
- Claude Code with the Workflow tool, logged in. Development and testing used Claude Code
  2.1.328.
- git. `gh` only for `merge_strategy=pr`.
- macOS for `dioxide service install` (launchd). Linux is untested; use `dioxide up` there.

## Quick start

```sh
git clone <repo-url> code-dioxide
cd code-dioxide
python3.12 -m venv .venv
.venv/bin/pip install .

.venv/bin/dioxide init               # home dir, example agents and roles, auth token
.venv/bin/dioxide service install    # launchd user agent; or run `dioxide up` in the foreground
.venv/bin/dioxide status             # is the server answering?
.venv/bin/dioxide register           # prints the `claude mcp add` command
```

`dioxide init` creates the home (`$DIOXIDE_HOME`, default `~/.code-dioxide`) with `agents/`,
`data/` and `logs/`, copies the example agents and `roles.yaml` if they are missing, and writes
a random token to `<home>/.env` (mode 0600). It prints the token the server will use, masked (a
`DIOXIDE_AUTH_TOKEN` in the environment overrides the file). For a home other than the default,
the commands that follow need `DIOXIDE_HOME=<home>` set; `init` prints it in its hint.

`dioxide register` prints a command of this form. Run it to add the server to Claude Code. It
uses the user scope because the default (local) scope would make the server available in one
project directory only:

```sh
claude mcp add --scope user --transport http dioxide http://127.0.0.1:8500/mcp \
  --header "Authorization: Bearer <token>"
```

The printed command contains the real token. Do not paste it anywhere public.

Then, from a Claude Code session, delegate work:

```
delegate_task(agent="planner",
              description="Add a --dry-run flag to the export command",
              input_context={"project_dir": "<path to a git repo>"})
```

In the example `roles.yaml`, `lifecycle_agent` is `planner`, so this runs the full lifecycle.
Poll with `check_task_status(task_id)`.

## CLI

| Command | What it does |
|---|---|
| `dioxide init [--home DIR]` | Create the home layout, copy the examples if absent, generate the token. |
| `dioxide up` | Run the server in the foreground. |
| `dioxide service install [--label L] [--extra-path P]...` | Write and load the launchd plist (macOS), wait for `/health`. |
| `dioxide service uninstall [--label L]` | Unload the service, remove the plist, count tasks left running. |
| `dioxide status` | Whether the server answers, its task counts, the home, the masked token, and `child env: <names>`, `child env: none` or `child env: unusable: <reason>`. |
| `dioxide register` | Print the `claude mcp add` command. |
| `dioxide logs TASK_ID [--source S] [--follow] [--url U] [--token T]` | Print a task's logs, locally or remotely (see Remote debugging). |
| `dioxide sync-agents --source DIR [--prefix TEXT]` | Show drift between agent definitions in DIR and the home's agents. |
| `dioxide repair-usage [--apply]` | Recompute recorded cost and tokens from the stored streams and fill missing per-model splits and stage numbers; a dry run without `--apply`. |

`service install` captures `PATH` from your login shell at install time, with `extra_path` and
`--extra-path` prepended (the order a task's `PATH` gets them in), and an absolute path to
`claude`. It lists the entries of your current shell's `PATH` that the service's `PATH` lacks;
add the ones the service needs with `--extra-path`. The default label is `dev.code-dioxide`,
and launchd output goes to `<home>/logs/`. The plist's `PATH` and `DIOXIDE_CLAUDE_BIN` are
fixed at install and override later changes in `<home>/.env`: run `service install` again after
changing either, or `PATH`. The token is not stored in the plist; the server reads it from
`<home>/.env`, so a token set only in the shell environment is an install error.

After loading the plist, `service install` polls `/health` for up to 30 s and prints `running`,
or exits 1 with the end of `<home>/logs/launchd.err.log`, where a startup failure (such as an
invalid setting) is reported. launchd restarts a server that exits at most once every 30 s
(`ThrottleInterval`), so a server that cannot start retries every 30 s until the cause is fixed
or the service is uninstalled. A stop waits at most 5 s for open requests (SSE and MCP streams)
before it cancels them; launchd kills the process 20 s after its SIGTERM (`ExitTimeOut`).

`service uninstall` reports a failed `launchctl bootout` (exit 1; the plist is kept) and how many
tasks are still running without supervision: their processes are their own sessions and keep
running; `dioxide up` or a new `service install` re-attaches them.

`sync-agents` never writes. It prints a unified diff per pair of files, listing the frontmatter
and the body separately, plus the files found on one side only. It exits 1 on drift and 0
otherwise. Use `--prefix` when the home holds prefixed copies: with `--prefix dx-`, the home
file `dx-reviewer.md` pairs with the source `reviewer.md`, and a `name:` value that differs
only by the prefix is not reported.

`repair-usage` fixes usage recorded before per-attempt shares (see Known limits): each
attempt's cost and tokens are computed again from its stream's last result line, and a task
output's `tokens_used` and `cost_usd` become the task's totals. A task with an open attempt or
a pruned stream is listed as skipped and left as it is. Without `--apply` it only prints the
changes. `--apply` refuses while dioxide runs for the home (stop the service first), copies the
database to `<home>/data/dioxide.db.repair-<unix time>.bak`, then writes in one transaction. A
second run changes nothing. It also fills or replaces each attempt's per-model split
(`attempt_models`) where it differs from the recomputed one, and the stage numbers of steps mirrored before they were
recorded, from `runs/<task>/run.json` where that still has the stage's progress entry.

## MCP tools

The first five tools keep the parameters of the tools of the same name in orcai-mcp (another
MCP server for delegating work to Claude Code agents), so a caller written for it works
unchanged.

| Tool | Purpose |
|---|---|
| `delegate_task(agent, description, input_context?, priority=3, max_retries=0)` | Queue a task for an agent or a role alias; returns at once. |
| `check_task_status(task_id)` | Status, output, current stage, steps, resumes, question, cost, recovery state. |
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

## roles.yaml

Workflows refer to roles, not agent names. `<home>/roles.yaml` maps roles to agent slugs (the
file names in `<home>/agents/`):

```yaml
planner: planner
implementer: implementer
reviewers: [reviewer]          # one or more; run in parallel each review round
qa: qa
deployer: deployer             # optional; without it the deploy stage never runs
lifecycle_agent: planner       # delegating to this agent runs the lifecycle workflow
aliases: {}                    # e.g. {feature: planner}; an alias must not shadow an agent slug
branch_template: "dioxide/{slug}-{id8}"   # must contain {id8}
```

Every named agent must exist. Without `roles.yaml`, every task is a single task.

Agent edits (`<home>/agents/*.md`, `roles.yaml`) apply to the next attempt; no restart. Files are
re-read before a launch when one changed; an invalid edit keeps the last good registry (event
`agents_reload_failed`, a global event). A reloaded `roles.yaml` applies to lifecycle tasks not yet
prepared; a prepared lifecycle keeps its roles, since its args are fixed at the first launch.
Each attempt runs with its own `attempt-<n>.agents.json` in its run dir.

Delegating to `lifecycle_agent` (directly or through an alias) always runs the full lifecycle;
this is by design. To run that agent alone as a single task, use `prompt_agent`.

Agent files are Claude Code agent definitions: Markdown with YAML frontmatter. dioxide accepts
the keys `name`, `description`, `model`, `effort`, `tools`, `disallowedTools`, `skills`,
`mcpServers` and `memory` (snake_case spellings are accepted as aliases) and rejects any other
key at startup, because Claude Code silently ignores unknown keys.

## Configuration

Settings are `DIOXIDE_*` environment variables, read from the process environment and from
`<home>/.env`; the process environment wins. `DIOXIDE_HOME` is read from the process
environment only, because it locates the `.env`; a `DIOXIDE_HOME` line in `<home>/.env` stops
the server at startup with an error naming the file. List and object settings take JSON;
`sweep_exempt` takes a comma-separated list. Settings are read at startup: restart the server
after a change.

| Setting | Default | Meaning |
|---|---|---|
| `DIOXIDE_HOME` | `~/.code-dioxide` | Home dir: `.env`, `agents/`, `roles.yaml`, optional `mcp.json`, `data/`, `worktrees/`. |
| `DIOXIDE_HOST` / `DIOXIDE_PORT` | `127.0.0.1` / `8500` | Bind address. |
| `DIOXIDE_AUTH_TOKEN` | none | Bearer token; required unless `auth_disabled`. |
| `DIOXIDE_AUTH_DISABLED` | `false` | Turn authentication off (local development only). |
| `DIOXIDE_REPORTS_TOKEN` | unset | Read-only token for numbers (see Reporting API). Must differ from the other tokens. |
| `DIOXIDE_CONTENT_TOKEN` | unset | Read-only token for numbers and redacted text (see Reporting API). Must differ from the other tokens. |
| `DIOXIDE_TRUST_LOOPBACK` | `false` | Accept loopback requests without a token (see Security). |
| `DIOXIDE_ALLOWED_HOSTS` | `[]` | Extra `Host` values the MCP endpoint accepts (needed behind a proxy or tunnel). |
| `DIOXIDE_CLAUDE_BIN` | `claude` | The Claude Code binary. |
| `DIOXIDE_PARENT_MODEL` | unset | `--model` for the lifecycle parent session. |
| `DIOXIDE_PARENT_EFFORT` | unset | `--effort` for the lifecycle parent: `low`, `medium`, `high`, `xhigh`, `max`. |
| `DIOXIDE_SCRIPT_MODEL` | unset (parent's model) | Model of the role-less `setup` and `merge` stages. |
| `DIOXIDE_DEFAULT_PROJECT_DIR` | unset | Repo used when `input_context` has no `project_dir`. |
| `DIOXIDE_CLAUDE_HOME` | `~/.claude` | Where Claude Code keeps sessions (read for live step logs). |
| `DIOXIDE_EXTRA_PATH` | `[]` | Prepended to every session's `PATH` and to the `service install` `PATH`. |
| `DIOXIDE_MAX_CONCURRENT_TASKS` | `3` | Tasks running at once (a running summary holds a slot too). |
| `DIOXIDE_SUMMARIES` | `false` | Serve `GET /api/v1/tasks/{id}/summary` (one `claude -p` run per summary). |
| `DIOXIDE_SUMMARY_MODEL` | `haiku` | Model of the summary runs. |
| `DIOXIDE_SUMMARY_MIN_INTERVAL_S` | `300` | A summary younger than this is served from the cache even if the task moved on. |
| `DIOXIDE_TASK_QUEUE_SIZE` | `20` | Queued tasks accepted before `delegate_task` rejects. |
| `DIOXIDE_DEDUPE_WINDOW_S` | `900` | How long a completed task still absorbs an identical delegation. |
| `DIOXIDE_STALL_TIMEOUT_S` | `2700` | No activity for this long: the attempt is killed and resumed. |
| `DIOXIDE_TASK_TIMEOUT_S` | `21600` | Total running time of a task; over it, the task fails. |
| `DIOXIDE_MAX_RESUMES` | `3` | Resumes per task, across its whole life. |
| `DIOXIDE_RATE_LIMIT_DEFAULT_WAIT_S` | `900` | Wait when a rate limit gives no reset time. |
| `DIOXIDE_RATE_LIMIT_MAX_WAIT_S` | `86400` | Total rate-limit wait per task. |
| `DIOXIDE_MODEL_FALLBACK` | `{}` (off) | Agent models to swap once when a task hits a rate limit, e.g. `{"fable":"opus"}`; see Model fallback. |
| `DIOXIDE_CALL_MISMATCH_RETRIES` | `2` | Corrective resumes when the parent miscalls the workflow. |
| `DIOXIDE_REVIEW_ROUNDS` / `DIOXIDE_QA_ROUNDS` | `3` / `3` | Fix rounds before asking a human. |
| `DIOXIDE_MERGE_STRATEGY` | `squash` | `squash` (local commit on base, no push) or `pr` (push and `gh pr create`). |
| `DIOXIDE_RUN_RETENTION_DAYS` | `90` | Run stores of tasks that ended longer ago are deleted; `0` keeps all. |
| `DIOXIDE_SWEEP_EXEMPT` | empty | Command names a sweep (killing a task's leftover processes) never kills; see Process policy. |
| `DIOXIDE_REDACT_ENV_FILES` | `[]` | Extra `.env` files whose values are masked in everything served. |
| `DIOXIDE_CHILD_ENV_FILE` | `<home>/child.env` if present at start | Variables added to every session's environment: credentials (see Credentials). |
| `DIOXIDE_EXTRA_SETTINGS` | `{}` | Claude Code settings passed verbatim to every session with `--settings`. |
| `DIOXIDE_MCP_CONFIG_PATH` | `<home>/mcp.json` if present | MCP servers available to sessions. |
| `DIOXIDE_MCP_EXCLUDE` | `["dioxide"]` | Server names removed from that config. |

Less common settings (watchdog interval, backoff, log page sizes) are listed with their
defaults in `src/config.py`.

Sessions run with `--strict-mcp-config`: they see only the servers in your `mcp.json`, minus
`mcp_exclude`, and none of your other MCP servers. Keep dioxide itself (and any other
orchestrator) out of that list so a stage cannot start new work.

### Orchestrator model

The lifecycle parent session drives the whole workflow and needs a capable model with a large
context window. Use Opus or Fable at low to medium effort (`DIOXIDE_PARENT_MODEL`,
`DIOXIDE_PARENT_EFFORT`). Stage models come from each agent's frontmatter, so stages can use
cheaper models. The `setup` and `merge` stages only run a fixed script; set
`DIOXIDE_SCRIPT_MODEL` to a cheap model (for example `haiku`) for them. `parent_model` is
never applied to single tasks, so a single task runs on its agent's model.

### Model fallback

A usage limit is per model: when one model is limited, a task whose agents use it would wait for
the reset even though another model is available. `DIOXIDE_MODEL_FALLBACK` is your policy for
that case, a JSON object from model to replacement, matched exactly against the `model` the agent
files give (for example `{"fable":"opus"}`). dioxide never picks a model itself; the default `{}`
turns the fallback off.

On the first rate limit of a task whose rendered agents use a mapped model, the task resumes the
same session and workflow run after `rate_limit_min_wait_s` instead of waiting for the reset; the
wait is not charged to the rate-limit wait budget. From then on every attempt of that task runs
with an agents file whose mapped models are replaced (event `model_fallback` with `from_to`, and
`retry_scheduled` with `why: rate_limited_fallback`). It happens once per task: a later rate limit
waits for the reset as usual. Steps that already completed replay from the run's cache: the agent
model is not part of a step's cache key, so only steps that had not finished run on the new
model; a stage that hit the limit has no journaled result, so it re-runs on the new model.
The parent session's model (`DIOXIDE_PARENT_MODEL`) and `DIOXIDE_SCRIPT_MODEL` are not
swapped: they are part of the launch and of the run's args, which a resume must keep identical.
dioxide does not tell which model hit the limit; the fallback applies whenever a mapped model is
in use.

### Commit attribution and MCP permissions

Stage sessions follow your Claude Code settings, including commit attribution: unless your
settings turn it off, commits made by stages get Claude Code's attribution trailer. To turn it
off for dioxide's sessions only, and to allow the tools of an MCP server from your `mcp.json`,
put this in `<home>/.env`:

```sh
DIOXIDE_EXTRA_SETTINGS='{"attribution":{"commit":"","pr":""},"permissions":{"allow":["mcp__<server>__*"]}}'
```

MCP tool calls need an allow rule like this even when the server is configured; a denied call
does not fail the stage, it shows up as a `permission_denied` event in `get_task_debug`.
`examples/settings.json` is a starter permission allowlist. dioxide does not read it: merge it
into your Claude Code settings or into `DIOXIDE_EXTRA_SETTINGS`. The `.env` that `dioxide init`
writes carries a commented `DIOXIDE_EXTRA_SETTINGS` example.

## Credentials

The `claude` sessions dioxide starts use whatever credentials Claude Code finds. From a terminal
that is your logged-in store. A service (launchd) or a headless host may not see that store,
and every session then ends at once with `Failed to authenticate`. Give the sessions credentials
explicitly in `<home>/child.env` (or the file `child_env_file` names), one of:

- `CLAUDE_CODE_OAUTH_TOKEN`: a long-lived token for your subscription, from `claude setup-token`.
- `ANTHROPIC_API_KEY`: an API key, billed to its Console account.

```sh
CLAUDE_CODE_OAUTH_TOKEN=<token from claude setup-token>
```

The file is python-dotenv syntax and must be a regular file owned by the service user with
mode 0600 (`chmod 600 <home>/child.env`): no group or other permission bits. Otherwise the
server does not start, with one line naming the file; a file that becomes unusable later fails
the task at its next launch instead of starting `claude`. `DIOXIDE_*` names are refused there.
`HOME` and `CLAUDE_CONFIG_DIR` are refused too: they would move the Claude Code state (sessions,
journals, transcripts) away from `claude_home`, where the engine reads it.
A `PATH` in the file replaces the service's `PATH` (then `extra_path` is prepended), so it
changes which binaries sessions resolve.
The file is read at every launch, so a rotated token needs no restart; the file itself is found
at startup (create it, then restart once). `dioxide init` does not create it.

Its values are added to every session's environment, after the `DIOXIDE_*` settings are
removed, and to the redaction set: they are never written to events, argv, the server log or
the bundle, and are masked wherever a session prints them, with these limits:

- Values shorter than 8 characters are not masked.
- A value is assumed to be one line: redaction works line by line, so a value spanning
  several lines is not reliably masked.
- The mask set holds the values read since the server started. After a rotation and a
  restart the old value is no longer in it (only the token-shape and key-name rules still
  apply), so rotate a leaked token rather than rely on masking.

`get_task_debug` shows only the variable names (`launch.child_env`); `dioxide status` prints
`child env: <names>`, `child env: none`, or `child env: unusable: <reason>` when the file is
refused. Nothing about credentials goes into the launchd plist.

A session that cannot authenticate is not retried: the attempt's outcome is `auth_failed`, the
task fails after that one attempt with `claude is not authenticated for this service: <claude's
message>. Set credentials in <home>/child.env (see README: Credentials) or log in as the service
user.`, and the timeline has an `auth_failed` event. The same holds when a stage of a lifecycle
run fails to authenticate.

## Guarantees

- **Recovery in the same session.** A process exit without a result, a crash, a stall or a
  server restart resumes the same Claude session and workflow run (`--resume` with
  `resumeFromRunId`). Completed stages replay from cache; an interrupted stage runs again from
  the start. Resumes back off `min(30 * 2^n, 600)` s. On restart, a still-running `claude`
  process is re-attached, and a dead one's output is checked for a result before anything is
  resumed. A rate limit waits until the reset time and does not use up resumes (or, once, falls
  back to user-mapped models; see Model fallback). If Claude
  refuses the resume, the task starts a fresh session on the same branch and worktree, unless
  that would drop an answer the run has not applied yet; then the task fails and keeps its
  branch.
- **Deduplication.** A `delegate_task` with the same agent, normalised description,
  `input_context` (ignoring `timestamp`, `nonce`, `wake_id`) and repo returns the existing task
  while it is queued, running, or completed within `dedupe_window_s`. The check and the insert
  are one transaction. `prompt_agent` is never deduplicated. Callers pass no idempotency keys.
- **Merge safety.** `merge.sh` merges only the QA-approved head: the branch tip must equal it
  exactly, for both strategies.
  - `squash` (default): the merge commit carries a git note (`refs/notes/dioxide` = task id)
    attached before the base branch moves, and every attempt first checks for that note, so a
    retried or concurrent merge of the same task never lands twice. The base advances by a
    fast-forward in the worktree that has it checked out, or by an atomic compare-and-set of
    the ref. A conflict or a base that keeps moving asks a human. Nothing is pushed, notes
    included.
  - `pr`: pushes the task branch to `origin` (or the first remote), then reuses the open pull
    request for that branch or creates one with `gh pr create`. No remote, no `gh`, or a failed
    push or create asks a human. There is no merge note: if the pull request is merged or closed
    between two retries, the retry creates a second pull request.
- **One engine per home**, enforced by a file lock on `<home>/data/.lock`.
- **The parent session is restricted.** Its first Workflow call must match the expected call
  exactly, and any other parent-level tool call or a second Workflow call kills the attempt.

## Process policy

A sweep is dioxide killing the processes a task left running. Stage commands run in their own
process groups and can outlive `claude`. dioxide finds a
task's processes through the `claude` process tree, a `DIOXIDE_TASK_ID=<id>` environment
marker set on every session, and a recorded set of descendants.

- **When processes are swept.** Only after an unclean end of an attempt: cancel of a running
  task, failure, a kill by dioxide (stall, timeout, parent violation), a crash, an engine error,
  or a restart that finds an attempt still open. The sweep runs at the attempt's end and again
  before the next launch; a relaunch never starts while a swept process survives.
- **A clean exit sweeps nothing.** When `claude` exits by itself and the attempt ends
  `completed` or `needs_human`, the processes still alive are recorded as kept and left
  running. Cancelling a task that waits after a clean exit kills nothing either.
- **`sweep_exempt`** lists command names (the basename of `argv[0]`, exact match) that are never
  killed, for example a shared development server. `claude`, shells and `git` are rejected. It
  is not a security boundary: a process can rename itself.
- Every sweep decision is a `sweep` event in the task's timeline.

Cleaning up after the work (stopping servers a stage started, removing worktrees or branches)
belongs in your agent definitions, not in dioxide.

## Stall detection

An attempt is stalled after `stall_timeout_s` (45 min) with no activity. Activity is the newest
of: growth of the session's output stream, changes to the workflow's files (journal, run file,
step transcripts) for a lifecycle task, and at least 1 s of CPU time used by the attempt's
descendant processes since the previous watchdog pass. A stalled attempt is killed and resumed.

Limits:

- Work outside the task's process tree is not seen, for example a build running inside a VM or
  container, or a long network wait.
- A descendant that is always busy (such as an MCP server that keeps using CPU) counts as
  activity, which disables stall detection for that attempt. `task_timeout_s` still applies.

Agent instructions should protect long-running commands: keep output flowing, or raise
`stall_timeout_s` for workloads that run silent for long.

## Security

The server can spawn `claude` with Bash, so anyone who can call it can run code on the host.

- **Token.** A bearer token is required on every request, including `/health`. `dioxide init`
  generates one. `auth_disabled=true` turns this off; use it only for local development.
- **`trust_loopback`.** With `trust_loopback=true`, requests from a loopback address are accepted
  without a token, but never when they carry `X-Forwarded-*`, `Forwarded` or `X-Real-IP`
  headers, because the peer is then a proxy. The peer address comes from the connection, never
  from headers. On a shared machine every local user can reach loopback, so leave it off there.
- **Reporting tokens.** `DIOXIDE_REPORTS_TOKEN` and `DIOXIDE_CONTENT_TOKEN` are read-only and
  reach only the routes listed under Reporting API (403 otherwise; `/mcp`, POSTs, logs, stream,
  debug and bundle stay admin-only). Loopback trust grants admin: a Grafana on the same host
  relying on it can read and change everything, so give it the reports token instead.
- **Tunnels.** The default bind is `127.0.0.1`. For remote access put the server behind
  Tailscale, a reverse proxy or a tunnel rather than binding a public interface, and keep the
  token on. Through a proxy, `trust_loopback` never applies. Add the external host name to
  `DIOXIDE_ALLOWED_HOSTS` (for example `["dioxide.example.net"]`); otherwise the MCP endpoint's
  DNS-rebinding check rejects the request's `Host`.
- File modes: the home and its `data/`, `logs/`, `agents/` and `worktrees/` directories are
  0700. The server runs with umask 077, so `.env`, the database (and its backups), the server
  log, the cursor key and the run store are private to you. The agents and `roles.yaml` that
  `init` copies keep the default mode, inside the 0700 home. Child processes (`claude`, the
  stages, `merge.sh`) run with the umask the server started with, your own, so files they
  create in your checkout get your usual modes.

## Remote debugging

Everything about a task is available through the API, so a person without a shell on the host
can investigate.

- **`check_task_status`**: status, stage, steps, `waiting` (until when and why), `kill_reason`,
  and the recovery budgets used.
- **`get_task_debug`**: every attempt (argv, start, end, exit code, outcome, reason, cost),
  every step (label, status, cached, start count, git branch and head), the expected and the
  actual workflow call, the launch environment (`PATH`, Claude Code version, agents, MCP config,
  settings, the names of the child env variables), the event timeline with a reason for each engine decision, and `bundle_url`.
- **`get_task_logs`**: sources `engine`, `server`, `attempt-<n>`, `stderr-<n>`, `journal`,
  `step:<label>`, `script`, `inputs`; without a source, a merged timeline. A running step is read
  live. Pages hold up to 40,000 characters, or up to 5 MiB with `full=true`; continue with
  `next_cursor` until `done`.
- **REST**, with the same bearer token:
  - `GET /api/v1/tasks/{id}/stream?source=` streams logs as Server-Sent Events; reconnect with
    `?cursor=<last id>` to miss nothing.
  - `GET /api/v1/tasks/{id}/bundle.tar.gz` downloads the redacted run store as one archive.
  - `GET /api/v1/server/logs?cursor=` pages the server log (startup config, spawn failures,
    boot recovery).
  - `GET /api/v1/tasks`, `GET /api/v1/tasks/{id}`, `/logs`, `/debug`, and
    `POST /api/v1/tasks/{id}/cancel` and `/answer` mirror the tools.
- **`dioxide logs TASK_ID --follow --url <server-url>`** follows the SSE stream from another
  machine and reconnects from the last cursor when the connection drops. With `--url` the
  token comes from `DIOXIDE_REMOTE_TOKEN` (preferred: a `--token` value is visible to other
  users in `ps`) or `--token`, never from this home's `.env`. Plain `http://` to a host that is
  not loopback sends the token unencrypted; the command warns, use `https://` or a tunnel.
- **On the host**, the workflow run can be inspected in Claude Code itself: run
  `claude --resume <session_id>` in the task's project directory (the folder must be trusted)
  and use `/workflows`, which lists the finished headless run.

The run store (`<home>/data/runs/<task>/`) keeps the script and input snapshots, every
attempt's stream and stderr, and copies of the journal, run file and step transcripts, so it
outlives Claude Code's own session cleanup. Run stores of tasks that ended more than
`run_retention_days` ago are deleted at startup and then once a day; tasks that are queued,
running or waiting, or that the engine still works on, are kept whatever their age. The task
rows stay: `check_task_status` keeps working, while the logs and the bundle's run files are
gone. `get_task_logs`, `get_task_debug` and the bundle's `task.json` then carry
`"run_store": "pruned"` (logs report `done: true`).

### Redaction and its limits

Everything served (tool results, logs, SSE, bundle, server log) is redacted line by line before
paging. Masked: Anthropic, OpenAI, Slack, GitHub and AWS key formats, JWTs, PEM private-key
blocks, `Authorization: Bearer` values, cookie headers, curl `-u user:pass`, URL userinfo, values
under secret-looking keys (`token`, `password`, `secret`, `api_key` and similar), the auth token,
values under secret-looking keys in `<home>/.env`, every value in `redact_env_files`, and every
value in the child env file (see Credentials).

Limits:

- A secret in an unknown format, under an innocuous name, is not masked.
- Environment values shorter than 8 characters are not registered.
- In plain text, an unquoted value that contains whitespace is masked only up to the whitespace.
- A purely numeric value under a sensitive key is not masked.
- JSON lines are re-encoded after redaction: served lines are equivalent to the files, not
  byte-identical.

### Process kill limits

A kill sends SIGTERM to the task's tree, marker processes and recorded descendants, then SIGKILL
after a grace period, and signals a pid only if its start time matches the recorded one.

- On macOS, `ps -E` does not show the environment of Apple platform binaries (`/bin/sh`,
  `/bin/zsh`, `/bin/bash`, `/bin/sleep`, ...), so the marker is invisible for those shells.
  Cleanup of such orphans relies on the recorded set, which misses a process spawned and orphaned
  between two polls.
- Double-forked daemons that scrub their environment are not covered on any platform.

## Reporting API

Read-only HTTP routes for dashboards (Grafana Infinity, a spreadsheet, a script). `GET /api/v1`
lists them by family; `GET /api/v1/openapi.json` is the OpenAPI document (no interactive docs).

| Family | Answers | Routes |
|---|---|---|
| Entities | what exists | `/api/v1/tasks`, `/api/v1/tasks/{id}`, `/api/v1/tasks/{id}/attempts`, `/api/v1/tasks/{id}/stages`, `/api/v1/tasks/{id}/graph`, `/api/v1/agents`, `/api/v1/agents/{id}`, `/api/v1/projects` |
| Metrics | numbers over time | `/api/v1/metrics/usage`, `/api/v1/metrics/time`, `/api/v1/metrics/reliability`, `/api/v1/metrics/stages`, `/api/v1/metrics/savings`, `/api/v1/metrics/overhead` |
| Activity | what is happening now | `/api/v1/activity`, `/api/v1/activity/stream`, `/api/v1/events` |
| Content | prompts, results, transcripts | `/api/v1/agents/{id}/prompt`, `/api/v1/tasks/{id}/prompts`, `/api/v1/tasks/{id}/events`, `/api/v1/tasks/{id}/stages/{label}/result`, `/api/v1/tasks/{id}/stages/{label}/transcript`, `/api/v1/tasks/{id}/summary` |

**Access.** Three bearer tokens:

| Token | Reaches |
|---|---|
| `DIOXIDE_AUTH_TOKEN` (admin) | everything |
| `DIOXIDE_REPORTS_TOKEN` | the routes above and `/health`; numbers, ids, names and timestamps only: on the task routes descriptions, errors, output text, return values, questions and branch names are removed |
| `DIOXIDE_CONTENT_TOKEN` | the reports routes with text kept (always redacted) and the Content family |

**Tokens.** Set `DIOXIDE_REPORTS_TOKEN` and/or `DIOXIDE_CONTENT_TOKEN` in `<home>/.env` (or
the environment) to any secret string distinct from the other tokens, e.g. the output of
`openssl rand -hex 32`, then restart the server; `dioxide init` creates only the admin token.
Send it as `Authorization: Bearer <token>`: there is no token query parameter and no CORS, so a
browser page on another origin, and the browser's `EventSource` (which cannot set headers),
need a same-origin proxy that adds the header.
Base URL: `http://<DIOXIDE_HOST>:<DIOXIDE_PORT>`, default `http://127.0.0.1:8500`.

**Filters.** `agent` (slug or alias), `task`, `project` (the task's working directory),
`status` (`queued`, `running`, `needs_human`, `completed`, `failed`, `cancelled`: the task's
current status), `kind` (`single`/`lifecycle`), `since`, `until`, and `stage_agent` (the
stage's agent type) only where there are stages. Each route takes:

| Route | Parameters |
|---|---|
| `/tasks` | `status`, `agent`, `project`, `kind`, `since`, `until` (bound `created_at`; no default window), `limit` (default 20), `cursor` |
| `/tasks/{id}`, `/tasks/{id}/graph` | none |
| `/tasks/{id}/attempts`, `/tasks/{id}/stages` | `limit`, `cursor` |
| `/agents`, `/projects` | filters except `stage_agent`, `limit`, `cursor` |
| `/agents/{id}` | filters except `stage_agent` |
| `/metrics/usage`, `/metrics/reliability` | filters except `stage_agent`, `group_by`, `bucket` |
| `/metrics/time` | the same plus `stat=sum\|p50\|p90\|max` |
| `/metrics/stages`, `/metrics/savings` | filters, `group_by`, `bucket` |
| `/metrics/overhead` | `task`, `since`, `until`, `bucket` (`group_by` takes no key) |
| `/activity` | filters (no window: `since`/`until` have no effect) |
| `/activity/stream` | filters, `last_event_id` |
| `/events` | filters except `stage_agent`, `type` (comma list), `limit`, `cursor` |
| `/tasks/{id}/events` | `type`, `limit`, `cursor` |
| `/tasks/{id}/stages/{label}/transcript` | `limit`, `cursor` |
| `/agents/{id}/prompt`, `/tasks/{id}/prompts`, `/tasks/{id}/stages/{label}/result`, `/tasks/{id}/summary` | none |

Every route under `/api/v1` except `/tasks` and `/tasks/{id}` refuses an unknown parameter
(400); those two ignore it.

**Times.** `since`/`until` are epoch seconds or ISO 8601 (no offset means UTC). A window is
`since` ≤ time < `until`; windowed routes default to the last 30 days up to now. Responses give
times as ISO 8601 in UTC (`+00:00`); `bucket` is the bucket's start. In Grafana pass
`since=${__from:date:seconds}&until=${__to:date:seconds}`: plain `${__from}` is epoch
milliseconds, read as seconds it is a window far in the future and returns nothing.

**Lists.** `{"rows": [...], "next_cursor": ...}`; pass `next_cursor` back as `cursor` until it
is null. `limit` defaults to 100; a value outside 1–1000 is clamped to that range, not refused.
`/tasks` returns `{"tasks", "count", "next_cursor"}` instead; `/activity` returns `{"rows"}`,
unpaged.

**Metrics.** `{"rows", "window": {"since", "until"}, "group_by", "bucket"}` (`/metrics/time`
adds `stat`). `group_by` is a comma list of the metric's keys (listed below); `bucket` is
`none` (default), `hour` or `day`, on UTC boundaries; more than 5000 rows is refused (400).
A row has `bucket` (when bucketed), its group keys, the metric's columns and `partial`; rows
are sorted by bucket, then keys. A bucket or group with nothing in it has no row: fill gaps
with 0 in the chart. Sums are null when no figure of the row has a value; counts are never
null. `tasks` (usage, time, stages) counts distinct tasks, so it does not add up across
buckets or groups. With `stat=p50|p90|max` each task's figures are summed first, then the
nearest-rank percentile (or max) over the row's tasks is taken: those rows do not add up
either.

**Errors.** `{"error": "<message>"}` with 400 (bad or unknown parameter, bad cursor, too many
rows), 401 (no or wrong token), 403 (the token's scope does not reach the route), 404 (unknown
task, agent or stage label; summaries off), 502 (summary failed), 503 (engine not started yet;
summary with no free slot: also `retry_after` and a `Retry-After` header).

**Event time.** Each figure counts when its own fact happened: cost, tokens, attempts and
running time at the attempt's end; `submitted` at creation; outcomes at `completed_at`; waits
when they ended. A running attempt's interim figures count now, in rows marked `partial: true`.
Closed buckets do not change afterwards, except through `repair-usage` and except when grouping
or filtering by `status`: that is the task's current status, so a task's earlier figures move
to its new status when it finishes.

**Cost.** A task's `cost_usd` is the sum of its attempts' shares of Claude Code's
`total_cost_usd` (list prices; main session, subagents and workflow stages, all models). An
attempt cut short (no result line, or killed) whose session is never resumed with a result is
`uncounted`: its spend is missing. Task and usage rows carry `uncounted_attempts` and
`cost_complete`. A metric sums only attempts that ended inside the window, so it can be lower
than a task's `cost_usd` when the task spans the window's edge.

**Per model.** Each attempt's share is also split per model (`attempt_models`, from the result
line's `modelUsage`): `input_tokens`, `output_tokens`, `cache_read_tokens`,
`cache_write_tokens` and cost per model, so the model shares add up to the attempt.
`/metrics/usage` and `/tasks/{id}/attempts` carry the four token columns (null where an
attempt has no split) and usage rows `unattributed_tokens` (the tokens no split covers).
`group_by=model` puts what has no split in a `model: null` row, so model rows still sum to the
total; an attempt counts in `attempts` of each model row it has a share in. An attempt has no
split when its result has no `modelUsage`, when an earlier attempt of its session has none
(pruned stream, or recorded before this version), or when a model's total fell below its
earlier share while the session grew. `repair-usage` recomputes the split from the stored
streams together with each attempt's share: it fills missing ones and replaces one that no
longer matches its share.

**Savings.** `/metrics/savings` counts `cached_stages` (stages a resumed run served from
cache), `deduplicated_submissions` (`delegate_task` calls answered with an existing task,
counted from this version on) and `est_stage_tokens_saved` (per cached stage, the median
`stage_tokens` of measured runs of the same stage and stage agent in the window; null with
fewer than 3). It has no cost estimate: cost per stage is not known. `submitted` (reliability)
plus `deduplicated_submissions` is the number of submit calls.

**Stages.** A lifecycle task's steps carry their own numbers from the workflow's progress:
agent type, model, `stage_tokens`, tool calls, last tool, duration. `stage` is the label without
its `@aN` attempt suffix and `-rN` round segments (`review-r1-dx-reviewer-correctness@a1` →
`review-dx-reviewer-correctness`); `label` stays as recorded. A stage served from cache on a
resume keeps the numbers of the run that measured it; that run counts in `runs`, the replay in
`cached_runs`.
`stage_tokens` is the stage's context size as the workflow reports it, not billed tokens:
never add it to `tokens` or price it. Steps mirrored before this version get their numbers from
`repair-usage` (from the run-store copy of the run file). `/tasks/{id}/graph` returns
`{"nodes", "edges"}` in the field names of Grafana's Node Graph panel (`id`, `title`,
`subTitle`, `mainStat`, `secondaryStat`; edges `id`, `source`, `target`, `mainStat` = `next` or
`retry`): every stage of a phase links to every stage of the next phase in start order (a
phase that comes back, such as a second review round, is linked again), and `X@aN` to `X@aN+1`
(`X` to `X@a1`).

**Activity.** `/activity` has one row per running task, per task waiting on a human
(`waiting_on: human`) and per task queued for a later relaunch (`rate_limit`, `backoff`,
`sweep`, `sweep_unconfirmed`), with the current stage and its agent, the last tool, the cost so
far and `since`. It has no window. `/events` is the event log (`type` takes a comma list), paged
by id. Outside admin scope an event's `data` keeps only ids, numbers, enums and tool names
(never messages, errors, paths or environment). `/activity/stream` is server-sent events:
first `event: ready` (its `id:` is the start position), one `progress` event per activity
row, then each new event as `event: event` with
`id: <event id>`, a `progress` for each changed row and `{"task": ..., "removed": true}` for a
row gone; a `: keep-alive` comment every 15 s of silence. A client reconnecting with
`Last-Event-ID` (EventSource does this) or `?last_event_id=` gets every later event once;
without one it starts after the newest event.

**Content.** Text, for the content token and admin only, always redacted:
`/agents/{id}/prompt` (the agent's current definition: model, tools, `system_prompt`);
`/tasks/{id}/prompts` (description, input context, args, the workflow script the run was
launched with, the agent definitions each attempt ran with, and per stage its prompt preview
and full prompt from the stage transcript); `/tasks/{id}/stages/{label}/result` (the stage's
result preview and its structured result from the journal); `/tasks/{id}/stages/{label}/transcript`
(the stage transcript, paged like `/logs`); `/tasks/{id}/events` (every event of the task with
its full data, paged by id). A field whose file retention removed is
`{"available": false, "reason": "pruned"}`; the rest of the response is still served. `label`
is matched exactly against the task's stages (404 otherwise).

**Summaries.** With `DIOXIDE_SUMMARIES=true`, `/tasks/{id}/summary` (content token and admin)
returns a short prose summary of the task so far: one `claude -p` run on
`DIOXIDE_SUMMARY_MODEL` with the child env credentials, over the task's stages (status, last
tool) and their results, redacted and capped at 20,000 characters. It runs only when a task slot
is free (otherwise 503 with `retry_after`) and holds that slot while it runs. Concurrent
requests for one task share one run. The summary is cached: served again while the task has
made no progress since, or while it is younger than `DIOXIDE_SUMMARY_MIN_INTERVAL_S`, so a
dashboard refreshing every few seconds triggers at most one run per task per interval. Off: 404.
A failed run: 502. Its spend is dioxide's own: `/metrics/overhead` (`summaries`, `cost_usd`,
`tokens` per run, by run time; filters `task`, `since`, `until`, `bucket`), never in a task's
`cost_usd`. Total dioxide spend = Σ `/metrics/usage` `cost_usd` + Σ `/metrics/overhead`
`cost_usd`.

#### Fields

Units: `*_usd` US dollars (list prices), `*_s` seconds, `*tokens*` tokens, times ISO 8601 UTC.

**`/metrics/usage`**: `group_by` `agent`, `model`, `project`, `kind`, `status`.

| Column | Meaning |
|---|---|
| `cost_usd`, `tokens` | the attempts' shares (see Cost) |
| `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens` | the per-model split (see Per model) |
| `unattributed_tokens` | tokens no split covers |
| `attempts` | attempts ended in the row, plus open ones (`partial`) |
| `uncounted_attempts`, `cost_complete` | attempts whose spend is missing; `cost_complete` is false when there are any |
| `tasks` | distinct tasks |

**`/metrics/time`**: `group_by` `agent`, `project`, `kind`, `status`; all columns seconds.

| Column | Meaning (counted when it ended) |
|---|---|
| `queued_s` | creation to the first attempt's start |
| `running_s` | attempts' running time (an open attempt: start to now, `partial`) |
| `human_wait_s` | an attempt ending `needs_human` to the answer, cancel or task end |
| `rate_limit_wait_s` | a rate-limit retry to the next start or cancel, at most until the retry was due |
| `backoff_wait_s` | the same for other retries and sweep requeues |
| `tasks` | distinct tasks |

**`/metrics/reliability`**: `group_by` `agent`, `project`, `kind`; all columns counts.

| Column | Meaning |
|---|---|
| `submitted` | tasks created |
| `completed`, `failed`, `cancelled` | tasks reaching that status, at `completed_at` |
| `attempts` | attempts ended (plus open ones) = first attempts + `resumes` + `fresh_restarts`; equals usage `attempts` |
| `resumes`, `fresh_restarts` | attempts started by resuming the session / by a fresh session |
| `call_retries` | corrective retries after the parent miscalled the workflow (`retry_scheduled`, `why: corrective`) |
| `stage_retries` | retries after a stage failed (`why: stage_failed`) |
| `tasks_with_fallback` | tasks at their first `model_fallback` |
| `killed` | attempts ended by a kill (cancel, stall, timeout, call violations) |
| `auth_failed`, `permission_denied` | those events |

**`/metrics/stages`**: `group_by` `stage`, `stage_agent`, `model`, `project`; columns
`stage_tokens` (context size, not billed), `tool_calls`, `duration_s`, `runs`, `cached_runs`,
`tasks` (see Stages).

**`/metrics/savings`**: `group_by` `agent`, `project`, `stage`; columns `cached_stages`,
`deduplicated_submissions`, `est_stage_tokens_saved` (see Savings).

**`/metrics/overhead`**: no `group_by`; columns `summaries`, `cost_usd`, `tokens`.

**`/tasks` rows**: `task_id`, `agent`, `kind`, `status`, `priority`, `created_at`,
`started_at`, `completed_at`, `stage` (lifecycle: the current stage), `cost_usd`,
`uncounted_attempts`, `cost_complete`; admin and content also `description` (first 200
characters), `error`, `branch`, `worktree`. **`/tasks/{id}`** for the reports token: `task_id`,
`status`, `started_at`, `completed_at`, `stage`, `steps` (`label`, `phase`, `status`,
`started_count`, `cached`, `agent_id`, `started_at`, `finished_at`, `timing`), `running_steps`,
`resumes`, `session_id`, `run_id`, `cost_usd`, `cost_partial`, `tokens_used`, `waiting`,
`kill_reason`, `recovery`, `uncounted_attempts`, `cost_complete`, `output` (`tokens_used`,
`cost_usd`).

**`/tasks/{id}/attempts` rows**: `n`, `session_id`, `mode` (`first`, `resume`, `fresh`),
`started_at`, `ended_at`, `outcome` (`success`, `needs_human`, `cancelled_by_workflow`,
`rate_limited`, `transient`, `stage_failed`, `resume_refused`, `call_mismatch`,
`parent_violation`, `auth_failed`, `fatal`), `reason` (the kill reason, if killed),
`cost_usd`, `tokens`, the four token columns, `running_s`, `uncounted`.

**`/tasks/{id}/stages` rows**: `label`, `stage`, `phase`, `status` (as the workflow reports it;
`interrupted` when still marked running while the task is not), `stage_agent`, `model`,
`started_count`, `cached`, `started_at`, `finished_at`, `duration_s`, `stage_tokens`,
`tool_calls`, `last_tool`.

**`/agents` rows**: `id`, `role`, `model`, `status` (`busy`/`idle` now), `running_tasks` (now),
`tasks_total` (created in the window), `last_active_at`; `/agents/{id}` adds
`tasks_by_status`. **`/projects` rows**: `project`, `tasks_total` (created in the window),
`running` (now), `last_task_at`.

**`/activity` rows** (also the `progress` events of the stream): `task`, `agent`, `project`,
`kind`, `status`, `stage`, `stage_agent`, `stage_model`, `last_tool`, `last_activity_at`,
`cost_usd`, `tokens`, `waiting_on` (null while running; `human`, `rate_limit`, `backoff`,
`sweep`, `sweep_unconfirmed`), `since` (when the attempt started or the wait began).

**`/events` rows** (also the `event` events of the stream): `id`, `at`, `task` (null for engine
events), `type`, `data`. The stream's `ready` event has data `{"after": <event id>}`. Types and
the `data` fields kept outside admin scope:

| Type | `data` outside admin |
|---|---|
| `submitted`, `deduplicated` | `agent` |
| `attempt_started` | `n`, `mode`, `corrective` |
| `spawned`, `auth_failed` | `n` |
| `attempt_finished` | `n`, `outcome`, `status` |
| `attempt_closed_at_boot` | `n`, `status`, `outcome`, `reason` |
| `retry_scheduled` | `n`, `why`, `at` (when due), `corrective`, `resumes`, `call_retries`, `stage_retries`, `rate_limit_waited_s` |
| `fresh_start` | `n`, `session_id`, `previous`, `previous_run_id`, `plan_pinned`, `dropped_answers`, `resumes`, `call_retries`, `stage_retries`, `rate_limit_waited_s` |
| `model_fallback` | `n`, `from_to` |
| `answered` | `k`, `label`, `mode`, `source` |
| `answer_not_applied` | `n`, `k` |
| `cancelled` | `from` |
| `permission_denied` | `tool` |
| `boot_recovered` | `n`, `process`, `action` |
| `cancel`, `stall`, `timeout`, `call_mismatch`, `second_workflow_call`, `parent_tool_violation`, `parent_tool_after_completion`, `exit_after_completion`, `workflow_call_after_completion` (kills) | `n`, `after_s`, `running_s`, `idle_s`, `limit_s`, `tool` |
| `kill_rerequested` | `n`, `reason`, `given_up` |
| `kill_skipped` | `reason` |
| `kill_unconfirmed` | `tries` |
| `run_started`, `run_archived`, `plan_unpinned` | `run_id` |
| `plan_pinned` | `label`, `run_id` |
| `sweep` | `policy`, `trigger`, `survivors_unconfirmed` |
| `sweep_requeued` | `next_attempt_at`, `streak` |
| `sweep_unconfirmed` | `trigger`, `streak` |
| `similar_active_task` | `similar` |
| `agents_reloaded` | `changed` |
| `workflow_script_failed` | `status` |
| `agents_reload_failed`, `engine_error`, `invalid_workflow_return`, `kill_skipped_no_start_time`, `launch_failed`, `lifecycle_prepared`, `spawn_failed`, `sweep_survivors` | none (`{}`) |

**Content**: `/agents/{id}/prompt` `{"id", "model", "effort", "tools", "disallowed_tools",
"system_prompt"}`; `/tasks/{id}/prompts` `{"task", "description", "input_context", "args",
"workflow_script", "agents": [{"n", "definitions"}], "stages": [{"label", "prompt_preview",
"prompt"}]}`; `/stages/{label}/result` `{"label", "result_preview", "result"}`;
`/stages/{label}/transcript` `{"entries", "next_cursor", "done"}`; `/tasks/{id}/summary`
`{"task", "summary", "generated_at", "basis_at", "cached", "cost_usd", "tokens"}`.

```sh
TOKEN=...   # DIOXIDE_REPORTS_TOKEN
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1/agents
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8500/api/v1/metrics/usage?bucket=day&group_by=agent"
```

Grafana (Infinity data source): type JSON, URL
`http://127.0.0.1:8500/api/v1/metrics/usage?bucket=day&group_by=agent`, header
`Authorization: Bearer <reports token>`, rows/root `rows`, columns `bucket` (time), `agent`
(string), `cost_usd` (number). Workflow graph: Node Graph panel, two Infinity queries on
`/api/v1/tasks/{id}/graph`, rows/root `nodes` and `edges`.

```sh
curl -N -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1/activity/stream
```

## Known limits

- Tokens (`tokens_used`, each attempt's `tokens`) count every model and subagent, cache reads
  and writes included (the result line's `modelUsage`). A result line without it falls back to
  `usage`, which covers the parent session only.
- A resumed session reports its whole total, earlier attempts included (Claude Code 2.1.277 and
  later). Each attempt keeps only its own share: that total minus what the session's earlier
  attempts already count. Before 2.1.277 a resumed session started its totals at zero. A total
  below the already-counted amount is taken as such a restart, but a pre-2.1.277 resume that
  spends more than the earlier attempts is undercounted by their amount.
- A killed attempt writes no result line, so its own row has no cost. When the next attempt
  resumes the same session, that session's total includes the killed attempt's spend, which is
  then counted in the resumed attempt's share. After a final kill or a fresh start it is not
  counted.
- The background service is macOS only; Linux is untested.
- A fresh-session restart may ask again: a plan that was not yet approved is planned (and, with
  `plan_gate`, asked) again.
- A failed stage gets one resume, and that resume may replay the stage's cached failure.
- Rate-limit detection is partly unverified: the stream's `rate_limit_event` and the transcript
  error record were observed, while the shape of a rate-limited result line is derived from the
  schema.
- One lifecycle task works in one repo.

## Upgrade and rollback

The database (`<home>/data/dioxide.db`, home from `DIOXIDE_HOME`) carries a schema version;
newer code migrates an older database on start and older code refuses a newer one ("upgrade
code-dioxide"). Schema v6 only adds the column `tasks.model_fallback_applied` (default 0), so
v5 to v6 drops nothing and takes no backup. To roll back to a v5 release, stop the service
(`dioxide service uninstall`, or stop `dioxide up`), copy the database, then run
`sqlite3 <home>/data/dioxide.db "PRAGMA user_version=5"` and start the old version. v5 code
reads tasks by column name and inserts with named columns, so it ignores the extra column; a
task that already fell back to the mapped model runs on the original models again.

## Comparing with a queue-based orchestrator (A/B)

dioxide was built to be compared with a queue-based setup in which a manager session hands work
between agents. Per task it records what such a comparison needs: wall time, cost and tokens,
stages run and cached, resumes, suppressed duplicates and human answers, all readable from
`check_task_status`, `get_task_debug` and the event timeline.

## Development

```sh
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy src cli
.venv/bin/pytest -q
```

Tests use a stub `claude`; `pytest -m live` runs the live tests against a real Claude Code
binary. `tests/test_publish_gate.py` scans tracked files for secrets and user paths; set
`DIOXIDE_DENYLIST` (comma-separated words) to also fail on private terms.

## Roadmap

Planned: Slack ask/continue, a cleanup hook with a cleaner agent, and custom workflows.

## License

MIT
