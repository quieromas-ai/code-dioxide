# Reliability

## Guarantees

- **Recovery in the same session.** A process exit without a result, a crash, a stall or a
  server restart resumes the same Claude session and workflow run (`--resume` with
  `resumeFromRunId`). Completed stages replay from cache; an interrupted stage runs again from
  the start. Resumes back off `min(30 * 2^n, 600)` s. On restart, a still-running `claude`
  process is re-attached, and a dead one's output is checked for a result before anything is
  resumed. A rate limit waits until the reset time and does not use up resumes (or, once, falls
  back to user-mapped models; see [Model fallback](configuration.md#model-fallback)). If Claude
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
