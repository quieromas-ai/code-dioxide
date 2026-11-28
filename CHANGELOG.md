# Changelog

All notable changes to code-dioxide. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) (0.x: a minor release may break).

## Unreleased

### Breaking

- `GET /api/v1/tasks` refuses an unknown query parameter with 400 instead of ignoring it, and
  validates `status` (400 on an unknown value). A misspelled filter used to return unfiltered rows.
  An empty value (`?status=`) means no filter.
- Lifecycle gates (tasks first launched on this release): `interpret-*` stages run without a
  role on `DIOXIDE_SCRIPT_MODEL` (else the parent's model) instead of the planner role, so
  `stage_agent` / `model` group-bys of those stages shift.

### Added

- `GET /api/v1/tasks?task=<id>`: filter the list to one task.
- Gate classification `unclear`: the human is asked again at the same gate. A task keeps the
  workflow copied at its first launch, so tasks paused across the upgrade are unaffected.
- A start-up log line when `DIOXIDE_SCRIPT_MODEL` is unset.
- Per-stage billed usage (`billed_*_tokens`, `turns`) parsed server-side from stage transcripts,
  and `estimated_cost_usd` / `cost_basis` (the attempt's per-model cost split by token-class
  weights, `DIOXIDE_COST_RATIOS`) on `/tasks/{id}/stages` and `/metrics/stages`;
  `parent_overhead_tokens` / `parent_overhead_estimated_cost_usd` on `/tasks/{id}/attempts`.
- Events `stage_started` / `stage_finished` (on real step transitions, never for a replay from
  cache; visible to the reports token), and `cost_usd` / `tokens` on `attempt_finished`.
- `files_changed` on `/tasks/{id}/stages/{label}/result`: the server's `git diff --name-status`
  since the previous completed step. Schema v11 (`step_models`, new `steps` columns).
- `/metrics/reliability`: `resumes` split by cause (`answer_resumes`, `transient_resumes`,
  `corrective_resumes`, `stage_failed_resumes`, `rate_limit_resumes`, `unattributed_resumes`;
  they sum to `resumes`). Event `attempt_started` gains `cause`; `/tasks/{id}` gains
  `attempts_by_mode`. Schema v10 (`tasks.resume_cause`); older history derives the cause from
  its events.

### Changed

- `/api/v1/tasks/{id}/graph`: `next` edges join waves of time-overlapping steps instead of
  phase runs (fewer cross-phase edges; sequential steps of one phase are now linked); new edge
  kind `gate` (ask → its interpret stages). Node and edge id formats are unchanged.

- `/api/v1/agents`: `last_active_at` includes the agent's lifecycle stage work (it can move
  later), and `status` is `busy` while one of its stages runs. New fields `stages_total`,
  `stage_tasks_total`, `running_stages`.

- Docs: reliability `resumes` (resumed launches, answers included) is not the recovery budget
  (`recovery.resumes` on a task).
