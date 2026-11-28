# Configuration

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
| `DIOXIDE_REPORTS_TOKEN` | unset | Read-only token for numbers (see [Reporting API](reporting-api.md)). Must differ from the other tokens. |
| `DIOXIDE_CONTENT_TOKEN` | unset | Read-only token for numbers and redacted text (see [Reporting API](reporting-api.md)). Must differ from the other tokens. |
| `DIOXIDE_TRUST_LOOPBACK` | `false` | Accept loopback requests without a token (see [Security model](security-model.md)). |
| `DIOXIDE_ALLOWED_HOSTS` | `[]` | Extra `Host` values the MCP endpoint accepts (needed behind a proxy or tunnel). |
| `DIOXIDE_TUNNEL_HOST` / `DIOXIDE_TUNNEL_PORT` | `127.0.0.1` / `0` (off) | Optional (off by default) second listener for Cloudflare Tunnel traffic, one proposed way to reach the server from outside (see [Security model](security-model.md#cloudflare-tunnel--access)). Needs the three `DIOXIDE_CF_ACCESS_*` settings and `DIOXIDE_ALLOWED_HOSTS`; must differ from `DIOXIDE_PORT`; refused with `auth_disabled`. |
| `DIOXIDE_CF_ACCESS_TEAM_DOMAIN` | unset | `https://<team>.cloudflareaccess.com`. |
| `DIOXIDE_CF_ACCESS_AUD` | unset | The Access application's AUD tag. |
| `DIOXIDE_CF_ACCESS_COMMON_NAME` | unset | The Access service token's client id. |
| `DIOXIDE_CLAUDE_BIN` | `claude` | The Claude Code binary. |
| `DIOXIDE_PARENT_MODEL` | unset | `--model` for the lifecycle parent session. |
| `DIOXIDE_PARENT_EFFORT` | unset | `--effort` for the lifecycle parent: `low`, `medium`, `high`, `xhigh`, `max`. |
| `DIOXIDE_SCRIPT_MODEL` | unset (parent's model) | Model of the role-less `setup`, `merge` and `interpret-*` stages. Unset: they run on the parent's model, and the server logs a hint at start. |
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
| `DIOXIDE_MODEL_FALLBACK` | `{}` (off) | Agent models to swap once when a task hits a rate limit, e.g. `{"fable":"opus"}`; see [Model fallback](#model-fallback). |
| `DIOXIDE_CALL_MISMATCH_RETRIES` | `2` | Corrective resumes when the parent miscalls the workflow. |
| `DIOXIDE_REVIEW_ROUNDS` / `DIOXIDE_QA_ROUNDS` | `3` / `3` | Fix rounds before asking a human. |
| `DIOXIDE_MERGE_STRATEGY` | `squash` | `squash` (local commit on base, no push) or `pr` (push and `gh pr create`). |
| `DIOXIDE_RUN_RETENTION_DAYS` | `90` | Run stores of tasks that ended longer ago are deleted; `0` keeps all. |
| `DIOXIDE_SWEEP_EXEMPT` | empty | Command names a sweep (killing a task's leftover processes) never kills; see [Process policy](reliability.md#process-policy). |
| `DIOXIDE_REDACT_ENV_FILES` | `[]` | Extra `.env` files whose values are masked in everything served. |
| `DIOXIDE_CHILD_ENV_FILE` | `<home>/child.env` if present at start | Variables added to every session's environment: credentials (see [Credentials](credentials.md)). |
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
cheaper models. The `setup` and `merge` stages only run a fixed script, and the
`interpret-<ask>-<k>` stages only classify a human answer at a gate; set
`DIOXIDE_SCRIPT_MODEL` to a cheap model (for example `haiku`) for them. Unset, they run on the
parent's model (often Opus): there is no automatic switch to a cheaper one. `parent_model` is
never applied to single tasks, so a single task runs on its agent's model.

Gate answers (plan approval, review/QA rounds exhausted, merge blocked) are classified by an
`interpret-<ask>-<k>` stage as `continue`, `amend`, `abort` or `unclear`. `unclear` asks the
human again at the same gate, with a note; only `abort` cancels the task; a stage that returns
no result fails (`stage_failed`), as any stage does. A task runs the workflow copied into its
run directory at its first launch, so a task paused across an upgrade keeps its earlier gate
behaviour and resumes from its cache unchanged.

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

## roles.yaml

Workflows refer to roles, not agent names. `<home>/roles.yaml` maps roles to agent slugs (the
file names in `<home>/agents/`):

```yaml
planner: planner
implementer: implementer
reviewers: [reviewer]          # one or more; run in parallel each review round
qa: qa
deployer: deployer             # optional; without it the deploy stage never runs
lifecycle_agent: coordinator   # delegating to this agent runs the lifecycle workflow
aliases: {}                    # e.g. {feature: coordinator}; an alias must not shadow an agent slug
branch_template: "dioxide/{slug}-{id8}"   # must contain {id8}
```

Every named agent must exist. Give `lifecycle_agent` an agent of its own (the example ships
`coordinator`): a lifecycle run does not use its prompt or model, so sharing it with a role only
makes that agent show two roles. Without `roles.yaml`, every task is a single task.

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
