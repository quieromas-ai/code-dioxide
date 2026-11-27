# code-dioxide

[![CI](https://github.com/quieromas-ai/code-dioxide/actions/workflows/ci.yml/badge.svg)](https://github.com/quieromas-ai/code-dioxide/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![MCP](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io)

code-dioxide (CO₂) is a local MCP server that runs delegated work as headless Claude Code
sessions. A caller (a Claude Code session or a person) sends one `delegate_task`. dioxide then
runs the whole feature loop as a Claude Code workflow script: plan, set up a git worktree,
implement, review, QA, optional deploy, and merge. If the run needs a human decision, it waits,
and the answer continues the same session and the same workflow run.

Task state is stored in SQLite. Every attempt (one launch of `claude` for a task) has its
output kept on disk, so a crash, a rate limit or a server restart resumes the work in a new
attempt instead of starting over. Everything that happened
can be read through the API without shell access to the host.

Status: alpha (0.1.0). The background service is macOS only.

code-dioxide is an independent project, not affiliated with or endorsed by Anthropic. It runs
Claude Code, which you install and log in to yourself.

## How it works

- **Lifecycle tasks.** A task whose agent resolves to `lifecycle_agent` (see [`roles.yaml`](docs/configuration.md#rolesyaml)) runs
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
git clone https://github.com/quieromas-ai/code-dioxide.git
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
delegate_task(agent="coordinator",
              description="Add a --dry-run flag to the export command",
              input_context={"project_dir": "<path to a git repo>"})
```

In the example `roles.yaml`, `lifecycle_agent` is `coordinator`, so this runs the full lifecycle;
delegating to any other agent runs it as a single task.
Poll with `check_task_status(task_id)`.

The [MCP tools](docs/mcp-tools.md) page lists every tool and how asks are answered.

## Documentation

| Topic | Page |
|---|---|
| Commands (`init`, `up`, `service`, `logs`, `repair-usage`, ...) | [CLI](docs/cli.md) |
| `delegate_task` and the other MCP tools; asks and answers | [MCP tools and task lifecycle](docs/mcp-tools.md) |
| `DIOXIDE_*` settings, orchestrator model, model fallback, `roles.yaml` | [Configuration](docs/configuration.md) |
| Credentials for the sessions dioxide starts | [Credentials](docs/credentials.md) |
| Guarantees, process policy, stall detection, known limits | [Reliability](docs/reliability.md) |
| Auth, loopback trust, redaction and kill limits | [Security model](docs/security-model.md) |
| Logs, debug bundles, reading a task remotely | [Remote debugging](docs/debugging.md) |
| Read-only HTTP API for dashboards | [Reporting API](docs/reporting-api.md), [reference](docs/reporting-api-reference.md) |
| Building dashboards (Grafana, custom pages) | [Dashboards](docs/dashboards.md) |
| Upgrading and rolling back | [Upgrade and rollback](docs/upgrade.md) |

## Roadmap

Planned: Slack ask/continue, a cleanup hook with a cleaner agent, and custom workflows.

## Contributing

Contributions are welcome. Please:

1. Open a [Discussion](https://github.com/quieromas-ai/code-dioxide/discussions) before starting significant work so we can agree on the approach.
2. Follow the existing code style: `ruff` and `mypy` must pass with no new errors.
3. Add or update tests for any changed behaviour.
4. Keep PRs focused: one concern per PR.

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities.

## License

MIT — see [LICENSE](LICENSE).
