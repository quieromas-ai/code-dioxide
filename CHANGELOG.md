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
