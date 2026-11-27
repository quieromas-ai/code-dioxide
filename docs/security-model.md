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
- **Cloudflare headers.** `trust_loopback` never applies to a request carrying
  `Cf-Connecting-IP` or `Cf-Access-Jwt-Assertion`. When Cloudflare Access is configured (see
  below), such a request on any listener also needs a valid Access token (403 otherwise).
- File modes: the home and its `data/`, `logs/`, `agents/` and `worktrees/` directories are
  0700. The server runs with umask 077, so `.env`, the database (and its backups), the server
  log, the cursor key and the run store are private to you. The agents and `roles.yaml` that
  `init` copies keep the default mode, inside the 0700 home. Child processes (`claude`, the
  stages, `merge.sh`) run with the umask the server started with, your own, so files they
  create in your checkout get your usual modes.

## Cloudflare Tunnel + Access

> **Optional.** This is one proposed way to reach the server from outside your network, not a
> requirement. Tailscale, a reverse proxy with TLS, or any other tunnel work too (see
> **Tunnels** above). The tunnel listener is off unless `DIOXIDE_TUNNEL_PORT` is set.

Suited to clients that cannot join your private network, without opening inbound ports. The
`cloudflared` connector may run on this host or on another one that reaches the tunnel listener.

1. **Zero Trust → Access → Service Auth → Service Tokens:** create a token for the client. Note
   its Client ID and Secret.
2. **Access → Applications → Self-hosted:** create an application for the tunnel host name, with
   a policy whose action is **Service Auth** and which includes only that service token. Copy
   the application's AUD tag. Create it *before* the next step so the host name is never public.
3. **Configure the tunnel listener** in `<home>/.env`, then restart the server:
   ```bash
   DIOXIDE_TUNNEL_HOST=127.0.0.1        # an address the cloudflared host can reach
   DIOXIDE_TUNNEL_PORT=8501
   DIOXIDE_CF_ACCESS_TEAM_DOMAIN=https://<team>.cloudflareaccess.com
   DIOXIDE_CF_ACCESS_AUD=<application-aud-tag>
   DIOXIDE_CF_ACCESS_COMMON_NAME=<service-token-client-id>
   DIOXIDE_ALLOWED_HOSTS=["dioxide.example.net"]
   ```
   Startup fails if the settings are incomplete. The main listener keeps its own bind.
4. **Tunnel → Public Hostname:** point the host name at `http://<tunnel-host>:8501`, the tunnel
   port, not `DIOXIDE_PORT`.
5. **Client:**
   ```bash
   claude mcp add --transport http dioxide https://dioxide.example.net/mcp \
     --header "CF-Access-Client-Id: <client-id>" \
     --header "CF-Access-Client-Secret: <client-secret>" \
     --header "Authorization: Bearer <auth-token>"
   ```

Every request on the tunnel listener needs both a valid Access token and the bearer token;
`trust_loopback` never applies there. The server verifies the `Cf-Access-Jwt-Assertion` token
itself, so the listener stays closed even if the Access application is deleted or
misconfigured: RS256 signature against `<team-domain>/cdn-cgi/access/certs`, issuer, audience,
expiry, `type: app`, and `common_name` equal to `DIOXIDE_CF_ACCESS_COMMON_NAME`. If the signing
keys have never been fetched, requests are refused (403); while later fetches fail, the last
keys fetched stay in use. Bind the tunnel listener to an address only
the `cloudflared` host reaches (loopback when it runs here, a private-network address
otherwise), never a public interface.

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
