# Remote debugging

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
