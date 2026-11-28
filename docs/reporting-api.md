# Reporting API

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
need a same-origin proxy that adds the header; see [Dashboards](dashboards.md#a-custom-web-page).
Base URL: `http://<DIOXIDE_HOST>:<DIOXIDE_PORT>`, default `http://127.0.0.1:8500`.

**Filters.** `agent` (slug or alias), `task`, `project` (the task's working directory),
`status` (`queued`, `running`, `needs_human`, `completed`, `failed`, `cancelled`: the task's
current status), `kind` (`single`/`lifecycle`), `since`, `until`, and `stage_agent` (the
stage's agent type) only where there are stages. Each route takes:

| Route | Parameters |
|---|---|
| `/tasks` | `status`, `agent`, `task` (one task id), `project`, `kind`, `since`, `until` (bound `created_at`; no default window), `limit` (default 20), `cursor` |
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

Every reporting route in the tables above except `/tasks/{id}` refuses an unknown parameter
(400), and a bad `status` is 400 too; `/tasks/{id}` ignores parameters. On `/tasks` an empty
value (`?status=`) means no filter.

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
never add it to `tokens` or price it. Billed usage per stage (`billed_*_tokens`, `turns`) is read
by the server from the stage's transcript when the step completes (each API turn once), and
again at the attempt's end.

**Stage cost.** `estimated_cost_usd` splits the attempt's known per-model cost (the CLI's
`modelUsage`, as `/metrics/usage`) across the stages that used that model in that attempt and
the parent session's own overhead; there is no price list. `cost_basis` is `exact` when one
stage was the model's only user in the attempt (the parent used none of it), else `allocated`:
each participant gets a share by token-class weights relative to base input (defaults: output
5, cache read 0.1, 5-minute cache write 1.25, 1-hour write 2, the parent's cache writes 2), and
the parent overhead takes the residual (and the share of stages of a workflow run replaced by a
fresh start), so stage estimates plus `parent_overhead_estimated_cost_usd` equal the attempt's
cost exactly. The per-attempt total is always exact; only the split within
one model is an estimate. The defaults are off where a model's ratios differ (Opus 5.5 reads
cost 0.05x input) and for long-context tiers: set `DIOXIDE_COST_RATIOS` (see
[Configuration](configuration.md)) to match your models. Null when the attempt has no
per-model breakdown or the stage was not measured (completed before this version). Steps mirrored before this version get their numbers from
`repair-usage` (from the run-store copy of the run file). `/tasks/{id}/graph` returns
`{"nodes", "edges"}` in the field names of Grafana's Node Graph panel (`id`, `title`,
`subTitle`, `mainStat`, `secondaryStat`; edges `id`, `source`, `target`, `mainStat` = `next`,
`retry` or `gate`). Steps are grouped into waves by time overlap (in start order, a step joins
the wave when it starts strictly before the wave's latest finish; a running step stays open,
an interrupted one ends at its last progress; steps never started form a last wave), so
parallel reviews share a wave; `next` links every step of a wave to every step of the next.
`retry` links `X@aN` to `X@aN+1` (`X` to `X@a1`; the suffix is the answer epoch, so a jump of
more than one answer has no `retry` edge), `gate` links the asking step to each
`interpret-<ask>-<k>` (for a review round, every reviewer step of that round);
either replaces a `next` edge between the same pair, so edge ids stay unique.

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

Every field, column and event type: [Reporting API reference](reporting-api-reference.md). Building panels: [Dashboards](dashboards.md).

## Try it

```sh
TOKEN=...   # DIOXIDE_REPORTS_TOKEN
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1/agents
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8500/api/v1/metrics/usage?bucket=day&group_by=agent"
curl -N -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8500/api/v1/activity/stream
```

## Measuring runs

Per task dioxide records wall time, cost and tokens, stages run and cached, resumes, suppressed
duplicates and human answers, all readable from `check_task_status`, `get_task_debug`, the
event timeline and the Reporting API, so a run can be compared with another way of doing the
same work (for example a queue-based setup in which a manager session hands work between
agents).
