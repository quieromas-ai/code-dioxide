"""Credentials for the ``claude`` sessions dioxide starts: the child env file.

A service (launchd) or a headless host may not see the credential store a terminal session
uses, so child sessions get their credentials explicitly from ``<home>/child.env`` (or
``child_env_file``): ``CLAUDE_CODE_OAUTH_TOKEN`` or ``ANTHROPIC_API_KEY``. The file is read at
every launch, so a rotated token needs no restart. It must be a regular file owned by the
service user with no group/other permission bits. Its values are secrets: they are added to the
redactor and never recorded; only the variable names are.
"""

import io
import os
import stat
from pathlib import Path

from dotenv import dotenv_values

from src.config import ENV_PREFIX

# Where Claude Code keeps sessions, journals and transcripts: the engine reads them through
# ``claude_home``, so a session must not be pointed elsewhere.
CLAUDE_STATE = frozenset({"HOME", "CLAUDE_CONFIG_DIR"})


class ChildEnvError(ValueError):
    """The child env file is unusable: missing, not private, or holds a refused name."""


def _open(path: Path) -> int:
    # O_NOFOLLOW: a symlink is refused (its target's owner and mode would be checked, not the
    # file named in the settings); O_NONBLOCK: opening a FIFO never blocks the engine.
    try:
        return os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        raise ChildEnvError(f"child env file {path} does not exist") from None
    except OSError as exc:
        reason = exc.strerror or type(exc).__name__
        raise ChildEnvError(
            f"child env file {path} cannot be opened ({reason}); it must be a regular file"
        ) from None


def _check(path: Path, st: os.stat_result) -> None:
    if not stat.S_ISREG(st.st_mode):
        raise ChildEnvError(f"child env file {path} must be a regular file")
    if st.st_uid != os.geteuid():
        raise ChildEnvError(
            f"child env file {path} must be owned by the service user (uid {os.geteuid()}), "
            f"not uid {st.st_uid}"
        )
    if st.st_mode & 0o077:
        mode = oct(stat.S_IMODE(st.st_mode))
        raise ChildEnvError(
            f"child env file {path} must have no group/other permission bits (mode {mode}); "
            f"run: chmod 600 {path}"
        )


def read(path: Path) -> dict[str, str]:
    """The file's variables (python-dotenv syntax). Raises ``ChildEnvError`` (naming the file
    and variable names, never a value) when the file is unusable."""
    fd = _open(path)
    try:
        _check(path, os.fstat(fd))
    except BaseException:
        os.close(fd)
        raise
    with os.fdopen(fd, encoding="utf-8", errors="replace") as fh:
        text = fh.read()
    parsed = dotenv_values(stream=io.StringIO(text))
    refused = sorted(
        name for name in parsed if name.upper().startswith(ENV_PREFIX) or name in CLAUDE_STATE
    )
    if refused:
        raise ChildEnvError(
            f"child env file {path}: {', '.join(refused)} not allowed ({ENV_PREFIX}* variables "
            "configure dioxide and are never passed to sessions; HOME and CLAUDE_CONFIG_DIR "
            "would move the Claude Code state the engine reads through claude_home)"
        )
    unset = sorted(name for name, value in parsed.items() if value is None)
    if unset:
        raise ChildEnvError(f"child env file {path}: {', '.join(unset)} has no value")
    return {name: value for name, value in parsed.items() if value is not None}


def secret_values(path: Path | None) -> list[str]:
    """The file's values for the redactor; nothing when there is no usable file (the engine
    then refuses to start or to launch, so no value of it reaches a session)."""
    if path is None:
        return []
    try:
        return list(read(path).values())
    except ChildEnvError:
        return []
