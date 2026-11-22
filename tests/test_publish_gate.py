"""Publish gate: tracked files carry no private terms, secrets or user paths.

The private terms come from ``DIOXIDE_DENYLIST`` (comma-separated, matched as whole words,
case-insensitive); naming them here would itself be a hit, so without the variable that test
is skipped. The secret and path patterns are always checked; they are assembled from pieces
so this file does not match itself.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
# A token prefix followed by a token character: a real credential, not a mention of the
# prefix (the redactor's own patterns, a test that builds a fake key).
_TOKEN_TAIL = "[A-Za-z0-9]"
SECRET_PATTERNS = (
    "xox" + "[abpr]-" + _TOKEN_TAIL,
    "xa" + "pp-" + _TOKEN_TAIL,
    "sk-" + "ant-" + _TOKEN_TAIL,
    "gh" + "p_" + _TOKEN_TAIL,
    "/" + "Users/",
    "/" + "home/[a-z]",
)


def _denylist(raw: str) -> list[str]:
    return [term.strip() for term in raw.split(",") if term.strip()]


# Read at import: the autouse fixture in conftest.py removes every DIOXIDE_* variable before
# each test runs.
DENYLIST = _denylist(os.environ.get("DIOXIDE_DENYLIST", ""))


def _words_pattern(terms: list[str]) -> re.Pattern[str]:
    return re.compile("|".join(rf"\b{re.escape(term)}\b" for term in terms), re.IGNORECASE)


def _tracked_files() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True
    ).stdout.decode()
    return [REPO / name for name in listed.split("\0") if name]


def _hits(pattern: re.Pattern[str]) -> list[str]:
    hits = []
    for path in _tracked_files():
        if not path.is_file():  # deleted in the work tree, not yet staged
            continue
        text = path.read_bytes().decode("utf-8", errors="replace")
        for number, line in enumerate(text.splitlines(), start=1):
            if found := [match.group(0) for match in pattern.finditer(line)]:
                hits.append(f"{path.relative_to(REPO)}:{number}: {found}")
    return hits


def test_no_denylisted_terms() -> None:
    if not DENYLIST:
        pytest.skip("DIOXIDE_DENYLIST is unset (comma-separated private terms to fail on)")
    hits = _hits(_words_pattern(DENYLIST))
    assert not hits, "denylisted terms:\n" + "\n".join(hits)


def test_no_secrets_or_user_paths() -> None:
    hits = _hits(re.compile("|".join(SECRET_PATTERNS), re.IGNORECASE))
    assert not hits, "secrets or user paths:\n" + "\n".join(hits)
