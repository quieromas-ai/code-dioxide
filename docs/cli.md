# CLI

| Command | What it does |
|---|---|
| `dioxide init [--home DIR]` | Create the home layout, copy the examples if absent, generate the token. |
| `dioxide up` | Run the server in the foreground. |
| `dioxide service install [--label L] [--extra-path P]...` | Write and load the launchd plist (macOS), wait for `/health`. |
| `dioxide service uninstall [--label L]` | Unload the service, remove the plist, count tasks left running. |
| `dioxide status` | Whether the server answers, its task counts, the home, the masked token, and `child env: <names>`, `child env: none` or `child env: unusable: <reason>`. |
| `dioxide register` | Print the `claude mcp add` command. |
| `dioxide logs TASK_ID [--source S] [--follow] [--url U] [--token T]` | Print a task's logs, locally or remotely (see [Remote debugging](debugging.md)). |
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

`repair-usage` fixes usage recorded before per-attempt shares (see [Known limits](reliability.md#known-limits)): each
attempt's cost and tokens are computed again from its stream's last result line, and a task
output's `tokens_used` and `cost_usd` become the task's totals. A task with an open attempt or
a pruned stream is listed as skipped and left as it is. Without `--apply` it only prints the
changes. `--apply` refuses while dioxide runs for the home (stop the service first), copies the
database to `<home>/data/dioxide.db.repair-<unix time>.bak`, then writes in one transaction. A
second run changes nothing. It also fills or replaces each attempt's per-model split
(`attempt_models`) where it differs from the recomputed one, and the stage numbers of steps mirrored before they were
recorded, from `runs/<task>/run.json` where that still has the stage's progress entry.
