# Changelog

All notable changes to code-dioxide. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) (0.x: a minor release may break).

## Unreleased

### Breaking

- `GET /api/v1/tasks` refuses an unknown query parameter with 400 instead of ignoring it, and
  validates `status` (400 on an unknown value). A misspelled filter used to return unfiltered rows.

### Added

- `GET /api/v1/tasks?task=<id>`: filter the list to one task.
