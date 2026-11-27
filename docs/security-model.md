# Security model

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

## Redaction and its limits

Everything served (tool results, logs, SSE, bundle, server log) is redacted line by line before
paging. Masked: Anthropic, OpenAI, Slack, GitHub and AWS key formats, JWTs, PEM private-key
blocks, `Authorization: Bearer` values, cookie headers, curl `-u user:pass`, URL userinfo, values
under secret-looking keys (`token`, `password`, `secret`, `api_key` and similar), the auth token,
values under secret-looking keys in `<home>/.env`, every value in `redact_env_files`, and every
value in the child env file (see [Credentials](credentials.md)).

Limits:

- A secret in an unknown format, under an innocuous name, is not masked.
- Environment values shorter than 8 characters are not registered.
- In plain text, an unquoted value that contains whitespace is masked only up to the whitespace.
- A purely numeric value under a sensitive key is not masked.
- JSON lines are re-encoded after redaction: served lines are equivalent to the files, not
  byte-identical.

## Process kill limits

A kill sends SIGTERM to the task's tree, marker processes and recorded descendants, then SIGKILL
after a grace period, and signals a pid only if its start time matches the recorded one.

- On macOS, `ps -E` does not show the environment of Apple platform binaries (`/bin/sh`,
  `/bin/zsh`, `/bin/bash`, `/bin/sleep`, ...), so the marker is invisible for those shells.
  Cleanup of such orphans relies on the recorded set, which misses a process spawned and orphaned
  between two polls.
- Double-forked daemons that scrub their environment are not covered on any platform.
