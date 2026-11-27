# Contributing to code-dioxide

## Prerequisites

- Python 3.12+
- Claude Code, logged in (only for the live tests and for running the server)
- git

## Local Dev Setup

```bash
git clone https://github.com/quieromas-ai/code-dioxide.git
cd code-dioxide

python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Run the server in the foreground (see the README's Quick start and [Configuration](docs/configuration.md)):

```bash
.venv/bin/dioxide init
.venv/bin/dioxide up
```

## Running Tests

All tests must pass before submitting a PR:

```bash
.venv/bin/pytest -q          # uses a stub `claude`; no account needed
```

`pytest -m live` runs the live tests against a real Claude Code binary; they spend on your
account. `tests/test_publish_gate.py` scans tracked files for secrets and user paths; set
`DIOXIDE_DENYLIST` (comma-separated words) to also fail on private terms.

## Static Analysis

All must be clean before committing:

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy src cli
```

Code style rules: line-length 100, Python 3.12 target (configured in `pyproject.toml`).

## Submitting a Pull Request

- One feature or fix per PR — keep changes focused
- Write a clear PR description explaining the problem and your solution
- New functionality requires tests
- All checks (tests + static analysis) must pass

## Reporting Bugs

Open a [GitHub Issue](https://github.com/quieromas-ai/code-dioxide/issues) with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Relevant logs or error output (`dioxide` redacts known secrets, but check before posting)

For **security vulnerabilities**, see [SECURITY.md](SECURITY.md) — do not open a public issue.

## Feature Requests

For substantial new features, open a [GitHub Discussion](https://github.com/quieromas-ai/code-dioxide/discussions) before building to align on scope and approach. Small improvements can go straight to a PR.
