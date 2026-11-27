# Dashboards

dioxide ships no dashboard: it serves the [Reporting API](reporting-api.md) and you build the
panels in the tool you already use. This page shows how, with Grafana and the
[Infinity data source](https://grafana.com/docs/plugins/yesoreyeram-infinity-datasource/latest/)
as the worked example; any tool that reads JSON over HTTP works the same way. Field names are
in the [reference](reporting-api-reference.md).

## Setup

1. Create a reports token (`DIOXIDE_REPORTS_TOKEN` in `<home>/.env`, then restart; see
   [Reporting API](reporting-api.md)). Use `DIOXIDE_CONTENT_TOKEN` only for panels that need
   text (prompts, results, summaries).
2. Add an Infinity data source: Authentication **Bearer token**, Token = the reports token;
   **Allowed hosts** = your dioxide base URL (required once authentication is set), e.g.
   `http://127.0.0.1:8500`.
3. Each panel is one Infinity query: Type **JSON**, Source **URL**, the route's URL,
   **Rows/Root** `rows`, and one column per field you plot (**Add Columns**: Selector, Title,
   Type). Use the **JSONata** or **JQ** (backend) parser if you want alerting or query caching.

Pass the dashboard's time range in seconds, not Grafana's default milliseconds:

```
http://127.0.0.1:8500/api/v1/metrics/usage?since=${__from:date:seconds}&until=${__to:date:seconds}&bucket=day&group_by=agent
```

`bucket` (ISO 8601, UTC) is the time column: type **Time**. Use `bucket=hour` for ranges up to
a few days and `bucket=day` beyond; more than 5000 rows is refused.

## Panel recipes

| Question | Route and parameters | Plot |
|---|---|---|
| Spend over time, per agent | `/metrics/usage?bucket=day&group_by=agent` | `bucket` × `cost_usd`, series `agent` |
| Spend per model | `/metrics/usage?group_by=model` | `model` × `cost_usd` (bar); `model: null` is spend without a per-model split |
| Token mix | `/metrics/usage?bucket=day` | `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, `unattributed_tokens` (stacked) |
| Is the cost figure complete? | `/metrics/usage` | stat on `uncounted_attempts` (0 = complete) |
| Total dioxide spend | `/metrics/usage` + `/metrics/overhead` (no bucket) | sum of both `cost_usd` |
| Throughput | `/metrics/reliability?bucket=day` | `submitted`, `completed`, `failed`, `cancelled` |
| Failure rate per agent | `/metrics/reliability?group_by=agent` | `failed / (completed + failed + cancelled)` (Grafana transformation **Add field from calculation**) |
| Recovery work | `/metrics/reliability?bucket=day` | `resumes`, `fresh_restarts`, `call_retries`, `stage_retries`, `killed` |
| Where time goes | `/metrics/time?group_by=agent` | `queued_s`, `running_s`, `human_wait_s`, `rate_limit_wait_s`, `backoff_wait_s` (stacked bar) |
| Typical task duration | `/metrics/time?stat=p50` and `stat=p90` | `running_s` per agent |
| Heaviest stages | `/metrics/stages?group_by=stage` | `stage_tokens`, `duration_s`, `tool_calls`, `runs` (table) |
| Work saved | `/metrics/savings?bucket=day` | `cached_stages`, `deduplicated_submissions`, `est_stage_tokens_saved` |
| What runs now | `/activity` | table: `task`, `agent`, `stage`, `last_tool`, `cost_usd`, `waiting_on`, `since` |
| Recent failures | `/events?type=attempt_finished,auth_failed,permission_denied,stall,timeout` | table: `at`, `task`, `type`, `data` |
| Tasks list | `/tasks?limit=100` (Rows/Root `tasks`) | table: `task_id`, `agent`, `status`, `stage`, `cost_usd`, `created_at` |
| One task's workflow | `/tasks/{id}/graph` | Node Graph panel: two queries, Rows/Root `nodes` and `edges` (the fields already use Node Graph's names) |

Use a dashboard variable for `agent`, `project` or `task` and add it as a query parameter
(`&agent=${agent}`); `/agents` and `/projects` list the values.

## Totals that add up

- **Empty buckets have no row.** A line chart draws a gap or joins the points
  (**Connect null values**); Grafana has no transformation that inserts zeros, so for a bar per
  bucket either accept missing bars or fill zeros in a script.
- **Sum cost, tokens and counts; never sum `tasks`** (distinct tasks per row) or
  `stat=p50|p90|max` rows across buckets or groups.
- **`group_by=model` attempts** count once in each model they used: sum cost and tokens across
  models, not `attempts`.
- **`group_by=status`** (and the `status` filter) use the task's current status, so past
  buckets move when a task finishes. Use `/metrics/reliability` outcomes for history.
- **`stage_tokens` is context size, not billing.** Never add it to usage `tokens` or price it.
- **`partial: true`** rows include a running attempt's interim figures and change until it
  ends.
- **Reliability identity:** per bucket `attempts` = first attempts + `resumes` +
  `fresh_restarts`, and equals usage `attempts`.

## A custom web page

The API has no CORS and takes the token only in the `Authorization` header, so a page in a
browser needs a same-origin proxy that serves the page and adds the header. With
[Caddy](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy):

```
:8600 {
	root * ./site
	file_server
	handle /api/* {
		reverse_proxy 127.0.0.1:8500 {
			header_up Authorization "Bearer {env.DIOXIDE_REPORTS_TOKEN}"
			flush_interval -1
		}
	}
}
```

Keep the proxy on a trusted network: anyone who reaches it reads with the token's scope.

Live activity from the page, through the proxy (`EventSource` reconnects by itself and sends
`Last-Event-ID`, so no event is missed or repeated):

```js
const rows = new Map();
const stream = new EventSource("/api/v1/activity/stream");
stream.addEventListener("progress", (e) => {
  const row = JSON.parse(e.data);
  if (row.removed) rows.delete(row.task); else rows.set(row.task, row);
  render([...rows.values()]);
});
stream.addEventListener("event", (e) => {
  const event = JSON.parse(e.data); // {id, at, task, type, data}
  if (event.type === "attempt_finished") console.log(event.task, event.data.outcome);
});
```
