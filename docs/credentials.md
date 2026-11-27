# Credentials

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
message>. Set credentials in <home>/child.env (see docs/credentials.md) or log in as the service
user.`, and the timeline has an `auth_failed` event. The same holds when a stage of a lifecycle
run fails to authenticate.
