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
| `call_retries` | corrective retries after the parent miscalled the workflow (`retry_scheduled`, `why: corrective`) |
| `stage_retries` | retries after a stage failed (`why: stage_failed`) |
| `tasks_with_fallback` | tasks at their first `model_fallback` |
| `killed` | attempts ended by a kill (cancel, stall, timeout, call violations) |
| `auth_failed`, `permission_denied` | those events |

**`/metrics/stages`**: `group_by` `stage`, `stage_agent`, `model`, `project`; columns
`stage_tokens` (context size, not billed), `tool_calls`, `duration_s`, `runs`, `cached_runs`,
`tasks` (see **Stages** in [Reporting API](reporting-api.md)).

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
`kill_reason`, `recovery`, `uncounted_attempts`, `cost_complete`, `output` (`tokens_used`,
`cost_usd`).

**`/tasks/{id}/attempts` rows**: `n`, `session_id`, `mode` (`first`, `resume`, `fresh`),
`started_at`, `ended_at`, `outcome` (`success`, `needs_human`, `cancelled_by_workflow`,
`rate_limited`, `transient`, `stage_failed`, `resume_refused`, `call_mismatch`,
`parent_violation`, `auth_failed`, `fatal`), `reason` (the kill reason, if killed),
`cost_usd`, `tokens`, the four token columns, `running_s`, `uncounted`.

**`/tasks/{id}/stages` rows**: `label`, `stage`, `phase`, `status` (as the workflow reports it;
`interrupted` when still marked running while the task is not), `stage_agent`, `model`,
`started_count`, `cached`, `started_at`, `finished_at`, `duration_s`, `stage_tokens`,
`tool_calls`, `last_tool`.

**`/agents` rows**: `id`, `role`, `model`, `status` (`busy`/`idle` now), `running_tasks` (now),
`tasks_total` (created in the window), `last_active_at`; `/agents/{id}` adds
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
| `attempt_started` | `n`, `mode`, `corrective` |
| `spawned`, `auth_failed` | `n` |
| `attempt_finished` | `n`, `outcome`, `status` |
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
"prompt"}]}`; `/stages/{label}/result` `{"label", "result_preview", "result"}`;
`/stages/{label}/transcript` `{"entries", "next_cursor", "done"}`; `/tasks/{id}/summary`
`{"task", "summary", "generated_at", "basis_at", "cached", "cost_usd", "tokens"}`.
