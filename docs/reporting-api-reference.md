# Reporting API reference

The fields of each [Reporting API](reporting-api.md) response.

Units: `*_usd` US dollars (list prices), `*_s` seconds, `*tokens*` tokens, times ISO 8601 UTC.

**`/metrics/usage`**: `group_by` `agent`, `model`, `project`, `kind`, `status`.

| Column | Meaning |
|---|---|
| `cost_usd`, `tokens` | the attempts' shares (see **Cost** in [Reporting API](reporting-api.md)) |
| `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens` | the per-model split (see **Per model** in [Reporting API](reporting-api.md)) |
| `unattributed_tokens` | tokens no split covers |
| `attempts` | attempts ended in the row, plus open ones (`partial`) |
| `uncounted_attempts`, `cost_complete` | attempts whose spend is missing; `cost_complete` is false when there are any |
| `tasks` | distinct tasks |

**`/metrics/time`**: `group_by` `agent`, `project`, `kind`, `status`; all columns seconds.

| Column | Meaning (counted when it ended) |
|---|---|
| `queued_s` | creation to the first attempt's start |
| `running_s` | attempts' running time (an open attempt: start to now, `partial`) |
| `human_wait_s` | an attempt ending `needs_human` to the answer, cancel or task end |
| `rate_limit_wait_s` | a rate-limit retry to the next start or cancel, at most until the retry was due |
| `backoff_wait_s` | the same for other retries and sweep requeues |
| `tasks` | distinct tasks |

**`/metrics/reliability`**: `group_by` `agent`, `project`, `kind`; all columns counts.

| Column | Meaning |
|---|---|
| `submitted` | tasks created |
| `completed`, `failed`, `cancelled` | tasks reaching that status, at `completed_at` |
| `attempts` | attempts ended (plus open ones) = first attempts + `resumes` + `fresh_restarts`; equals usage `attempts` |
| `resumes`, `fresh_restarts` | attempts started by resuming the session / by a fresh session |
| `answer_resumes`, `transient_resumes`, `corrective_resumes`, `stage_failed_resumes`, `rate_limit_resumes`, `unattributed_resumes` | `resumes` by cause (`attempt_started.cause`); they sum to `resumes`. `rate_limit_resumes` covers `rate_limited` and `rate_limited_fallback`; `unattributed_resumes` counts old events (written before `cause`) with nothing to derive a cause from, normally 0. `resumes` is not the recovery budget (`recovery.resumes` on `/tasks/{id}`): answers use none, fresh restarts use one, sweep re-queues and retries cancelled before launch make them differ. `corrective_resumes` / `stage_failed_resumes` count claimed attempts at their end, `call_retries` / `stage_retries` the scheduling events |
| `call_retries` | corrective retries after the parent miscalled the workflow (`retry_scheduled`, `why: corrective`) |
| `stage_retries` | retries after a stage failed (`why: stage_failed`) |
| `tasks_with_fallback` | tasks at their first `model_fallback` |
| `killed` | attempts ended by a kill (cancel, stall, timeout, call violations) |
| `auth_failed`, `permission_denied` | those events |

**`/metrics/stages`**: `group_by` `stage`, `stage_agent`, `model`, `project`; columns
`stage_tokens` (context size, not billed), `tool_calls`, `duration_s`, `runs`, `cached_runs`,
`billed_input_tokens`, `billed_output_tokens`, `billed_cache_read_tokens`,
`billed_cache_write_tokens`, `turns` (billed, from the stage transcripts; a row replayed from
cache keeps the measured run's), `estimated_cost_usd`, `cost_basis` (`exact` only if every summed stage is, null if any
completed stage is unknown), `tasks` (see **Stages** and **Stage cost** in
[Reporting API](reporting-api.md)).

**`/metrics/savings`**: `group_by` `agent`, `project`, `stage`; columns `cached_stages`,
`deduplicated_submissions`, `est_stage_tokens_saved` (see **Savings** in [Reporting API](reporting-api.md)).

**`/metrics/overhead`**: no `group_by`; columns `summaries`, `cost_usd`, `tokens`.

**`/tasks` rows**: `task_id`, `agent`, `kind`, `status`, `priority`, `created_at`,
`started_at`, `completed_at`, `stage` (lifecycle: the current stage), `cost_usd`,
`uncounted_attempts`, `cost_complete`; admin and content also `description` (first 200
characters), `error`, `branch`, `worktree`. **`/tasks/{id}`** for the reports token: `task_id`,
`status`, `started_at`, `completed_at`, `stage`, `steps` (`label`, `phase`, `status`,
`started_count`, `cached`, `agent_id`, `started_at`, `finished_at`, `timing`), `running_steps`,
`resumes`, `session_id`, `run_id`, `cost_usd`, `cost_partial`, `tokens_used`, `waiting`,
`kill_reason`, `recovery` (`recovery.resumes` is the recovery budget used: transient resumes
and fresh restarts, never answers), `attempts_by_mode` (`first`, `resume`, `fresh`: claimed
attempts, answer resumes included), `uncounted_attempts`, `cost_complete`, `output`
(`tokens_used`, `cost_usd`).

**`/tasks/{id}/attempts` rows**: `n`, `session_id`, `mode` (`first`, `resume`, `fresh`),
`started_at`, `ended_at`, `outcome` (`success`, `needs_human`, `cancelled_by_workflow`,
`rate_limited`, `transient`, `stage_failed`, `resume_refused`, `call_mismatch`,
`parent_violation`, `auth_failed`, `fatal`), `reason` (the kill reason, if killed),
`cost_usd`, `tokens`, the four token columns, `running_s`, `uncounted`,
`parent_overhead_tokens` (the attempt's tokens minus its stages', per model; null without a
per-model breakdown), `parent_overhead_estimated_cost_usd` (the attempt's cost minus its stages'
estimates).

**`/tasks/{id}/stages` rows**: `label`, `stage`, `phase`, `status` (as the workflow reports it;
`interrupted` when still marked running while the task is not), `stage_agent`, `model`,
`started_count`, `cached`, `started_at`, `finished_at`, `duration_s`, `stage_tokens`,
`tool_calls`, `last_tool`, `billed_input_tokens`, `billed_output_tokens`,
`billed_cache_read_tokens`, `billed_cache_write_tokens`, `turns` (null: not measured),
`estimated_cost_usd`, `cost_basis` (`exact`, `allocated` or null).

**`/agents` rows**: `id`, `role`, `model`, `status` (`busy`/`idle` now: running a task or a
stage), `running_tasks` (now), `tasks_total` (tasks submitted to the agent, created in the
window), `stages_total` / `stage_tasks_total` (lifecycle stages the agent ran, and their distinct
tasks, of tasks created in the window), `running_stages` (now), `last_active_at` (the later of
its last task activity and its last stage start or finish); `agent=` matches the task's agent
for task figures and the stage's agent for stage figures; script stages (no agent) count for
no agent. MCP `get_agents` / `get_active_agents` count only submitted tasks as busy. `/agents/{id}` adds
`tasks_by_status`. **`/projects` rows**: `project`, `tasks_total` (created in the window),
`running` (now), `last_task_at`.

**`/activity` rows** (also the `progress` events of the stream): `task`, `agent`, `project`,
`kind`, `status`, `stage`, `stage_agent`, `stage_model`, `last_tool`, `last_activity_at`,
`cost_usd`, `tokens`, `waiting_on` (null while running; `human`, `rate_limit`, `backoff`,
`sweep`, `sweep_unconfirmed`), `since` (when the attempt started or the wait began).

**`/events` rows** (also the `event` events of the stream): `id`, `at`, `task` (null for engine
events), `type`, `data`. The stream's `ready` event has data `{"after": <event id>}`. Types and
the `data` fields kept outside admin scope:

| Type | `data` outside admin |
|---|---|
| `submitted`, `deduplicated` | `agent` |
| `attempt_started` | `n`, `mode`, `corrective`, `cause` (`answer`, `transient`, `corrective`, `stage_failed`, `rate_limited`, `rate_limited_fallback`, `fresh`; null on a first launch) |
| `spawned`, `auth_failed` | `n` |
| `attempt_finished` | `n`, `outcome`, `status`, `cost_usd`, `tokens` (the attempt's share) |
| `stage_started` | `label`, `agent_type`, `model`, `started_at` (epoch s): a new stage or a restart; never for a replay from cache. `agent_type` / `model` are null before the stage's first progress entry, `started_at` of a restart before its transcript is readable |
| `stage_finished` | `label`, `agent_type`, `model`, `status` (`completed`), `started_at`, `finished_at` (epoch s): a stage newly completed; never for a replay. A stage killed with its attempt has a `stage_started` and no `stage_finished` |
| `attempt_closed_at_boot` | `n`, `status`, `outcome`, `reason` |
| `retry_scheduled` | `n`, `why`, `at` (when due), `corrective`, `resumes`, `call_retries`, `stage_retries`, `rate_limit_waited_s` |
| `fresh_start` | `n`, `session_id`, `previous`, `previous_run_id`, `plan_pinned`, `dropped_answers`, `resumes`, `call_retries`, `stage_retries`, `rate_limit_waited_s` |
| `model_fallback` | `n`, `from_to` |
| `answered` | `k`, `label`, `mode`, `source` |
| `answer_not_applied` | `n`, `k` |
| `cancelled` | `from` |
| `permission_denied` | `tool` |
| `boot_recovered` | `n`, `process`, `action` |
| `cancel`, `stall`, `timeout`, `call_mismatch`, `second_workflow_call`, `parent_tool_violation`, `parent_tool_after_completion`, `exit_after_completion`, `workflow_call_after_completion` (kills) | `n`, `after_s`, `running_s`, `idle_s`, `limit_s`, `tool` |
| `kill_rerequested` | `n`, `reason`, `given_up` |
| `kill_skipped` | `reason` |
| `kill_unconfirmed` | `tries` |
| `run_started`, `run_archived`, `plan_unpinned` | `run_id` |
| `plan_pinned` | `label`, `run_id` |
| `sweep` | `policy`, `trigger`, `survivors_unconfirmed` |
| `sweep_requeued` | `next_attempt_at`, `streak` |
| `sweep_unconfirmed` | `trigger`, `streak` |
| `similar_active_task` | `similar` |
| `agents_reloaded` | `changed` |
| `workflow_script_failed` | `status` |
| `agents_reload_failed`, `engine_error`, `invalid_workflow_return`, `kill_skipped_no_start_time`, `launch_failed`, `lifecycle_prepared`, `spawn_failed`, `sweep_survivors` | none (`{}`) |

**Content**: `/agents/{id}/prompt` `{"id", "model", "effort", "tools", "disallowed_tools",
"system_prompt"}`; `/tasks/{id}/prompts` `{"task", "description", "input_context", "args",
"workflow_script", "agents": [{"n", "definitions"}], "stages": [{"label", "prompt_preview",
"prompt"}]}`; `/stages/{label}/result` `{"label", "result_preview", "result", "files_changed"}`
(`files_changed`: `[{"status", "path"}]` from the server's `git diff --name-status --no-renames`
between the worktree head recorded at the previous completed step and at this one: steps
completing together or in parallel share it, a merge or rebase lists everything the head moved
by; `[]` when the head did not move; null when not knowable, e.g. the first completed step);
`/stages/{label}/transcript` `{"entries", "next_cursor", "done"}`; `/tasks/{id}/summary`
`{"task", "summary", "generated_at", "basis_at", "cached", "cost_usd", "tokens"}`.
