"""The launchd user agent (macOS): plist and launchctl argv as pure functions, plus the calls.

The service runs ``dioxide up`` with the environment captured at install: ``HOME``,
``extra_path`` then the login shell's ``PATH``, an absolute ``DIOXIDE_HOME`` and an absolute
``DIOXIDE_CLAUDE_BIN``. The bearer token is not in the plist: the server reads it from
``DIOXIDE_HOME/.env``.
"""

import os
import plistlib
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

DEFAULT_LABEL = "dev.code-dioxide"
PLATFORM = sys.platform
TEMPLATE_NAME = "launchd.plist.j2"
LOGIN_PATH_TIMEOUT_S = 30.0
# launchd sends SIGTERM at bootout and SIGKILL after ExitTimeOut. The server's stop waits up to
# src.main.GRACEFUL_SHUTDOWN_S for open requests, then stops the engine; a stop right after a
# cancel also waits out that task's kill grace and can meet the SIGKILL. That is harmless to the
# data (every write is a transaction; task processes are their own sessions) and the next boot
# re-attaches the tasks.
EXIT_TIMEOUT_S = 20
# KeepAlive restarts a server that exited at most once per ThrottleInterval (a startup failure
# is retried this often; its reason is in logs/launchd.err.log).
THROTTLE_INTERVAL_S = 30
# A bootout returns before launchd has finished removing the service; bootstrap waits for it,
# longer than launchd itself waits for the process to exit.
BOOTOUT_WAIT_S = 30.0
BOOTOUT_POLL_S = 0.2
# After bootstrap: how long `service install` polls the server's /health before it reports the
# tail of the service's stderr log.
HEALTH_WAIT_S = 30.0
HEALTH_POLL_S = 0.5
ERR_TAIL_LINES = 20
ERR_LOG_NAME = "launchd.err.log"
_PLACEHOLDER = re.compile(r"\{\{ (\w+) \}\}")


def asset_dir(name: str, package_dir: Path | None = None) -> Path:
    """A shipped data dir (``templates``, ``examples``): packaged under ``cli/`` in a wheel, at
    the repository root in a source checkout."""
    here = Path(__file__).resolve().parent if package_dir is None else package_dir
    for candidate in (here / name, here.parent / name):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(f"code-dioxide data dir {name!r} not found near {here}")


class ServiceError(RuntimeError):
    """The service cannot be installed or removed; the message says why."""


@dataclass(frozen=True)
class ServiceSpec:
    label: str
    program: list[str]
    home: Path
    logs_dir: Path
    env: dict[str, str]


def require_supported() -> None:
    if PLATFORM != "darwin":
        raise ServiceError(f"dioxide service: {PLATFORM} is not yet supported (macOS only)")


def login_shell_path(shell: str) -> str:
    """``$SHELL -lc 'echo $PATH'``: the last output line (login banners print before it)."""
    try:
        proc = subprocess.run(
            [shell, "-lc", "echo $PATH"],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=LOGIN_PATH_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ServiceError(f"cannot read the login shell PATH from {shell}: {exc}") from exc
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if proc.returncode != 0 or not lines:
        raise ServiceError(f"{shell} -lc 'echo $PATH' failed (exit {proc.returncode})")
    return lines[-1]


def service_path(login_path: str, extra_path: list[str]) -> str:
    """``extra_path`` prepended to the login PATH, as the launcher prepends it for a task
    (order kept; a repeated entry keeps its first place)."""
    out: list[str] = []
    for entry in [*extra_path, *login_path.split(os.pathsep)]:
        if entry and entry not in out:
            out.append(entry)
    return os.pathsep.join(out)


def build_spec(
    *, label: str, home: Path, user_home: Path, path: str, claude_bin: str, python: str
) -> ServiceSpec:
    """The service definition; ``claude_bin`` is resolved to an absolute path on ``path``."""
    found = shutil.which(claude_bin, path=path)
    if found is None:
        raise ServiceError(f"claude binary {claude_bin!r} not found on the service PATH {path}")
    env = {
        "HOME": str(user_home),
        "PATH": path,
        "DIOXIDE_HOME": str(home),
        "DIOXIDE_CLAUDE_BIN": str(Path(found).absolute()),
    }
    # -P: the working directory (the home) is not put on sys.path and cannot shadow the package.
    program = [python, "-P", "-m", "cli.main", "up"]
    return ServiceSpec(label, program, home, home / "logs", env)


def render_plist(spec: ServiceSpec) -> str:
    indent = " " * 8
    values = {
        "label": escape(spec.label),
        "home": escape(str(spec.home)),
        "exit_timeout": str(EXIT_TIMEOUT_S),
        "throttle_interval": str(THROTTLE_INTERVAL_S),
        "program_arguments": "\n".join(
            f"{indent}<string>{escape(arg)}</string>" for arg in spec.program
        ),
        "stdout_path": escape(str(spec.logs_dir / "launchd.out.log")),
        "stderr_path": escape(str(spec.logs_dir / ERR_LOG_NAME)),
        "environment": "\n".join(
            f"{indent}<key>{escape(k)}</key>\n{indent}<string>{escape(v)}</string>"
            for k, v in spec.env.items()
        ),
    }
    template = (asset_dir("templates") / TEMPLATE_NAME).read_text(encoding="utf-8")

    def fill(match: re.Match[str]) -> str:
        return values[match.group(1)]

    return _PLACEHOLDER.sub(fill, template)


def plist_path(user_home: Path, label: str) -> Path:
    return user_home / "Library" / "LaunchAgents" / f"{label}.plist"


def launchctl_argv(action: str, label: str, plist: Path, uid: int) -> list[str]:
    if action == "install":
        return ["launchctl", "bootstrap", f"gui/{uid}", str(plist)]
    if action == "uninstall":
        return ["launchctl", "bootout", f"gui/{uid}/{label}"]
    if action == "print":
        return ["launchctl", "print", f"gui/{uid}/{label}"]
    raise ValueError(f"unknown service action {action!r}")


def run_launchctl(argv: list[str]) -> tuple[int, str]:
    """(exit code, stderr) of one launchctl call."""
    proc = subprocess.run(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        text=True, check=False,
    )  # fmt: skip
    return proc.returncode, proc.stderr


def wait_until_gone(
    label: str,
    uid: int,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll ``launchctl print`` until the service is gone; ServiceError after
    ``BOOTOUT_WAIT_S``."""
    argv = launchctl_argv("print", label, Path(), uid)
    deadline = clock() + BOOTOUT_WAIT_S
    while run_launchctl(argv)[0] == 0:
        if clock() >= deadline:
            raise ServiceError(
                f"{label} is still loaded {BOOTOUT_WAIT_S:g}s after bootout; try again"
            )
        sleep(BOOTOUT_POLL_S)


def install(
    spec: ServiceSpec,
    user_home: Path,
    uid: int,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Path:
    """Write the plist and (re)load it: a loaded service is booted out first, and bootstrap
    waits until launchd reports it gone."""
    plist = plist_path(user_home, spec.label)
    plist.parent.mkdir(parents=True, exist_ok=True)
    spec.logs_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_launchctl(launchctl_argv("uninstall", spec.label, plist, uid))  # not loaded: ignored
    wait_until_gone(spec.label, uid, clock, sleep)
    plist.write_text(render_plist(spec), encoding="utf-8")
    plist.chmod(0o644)
    code, stderr = run_launchctl(launchctl_argv("install", spec.label, plist, uid))
    if code != 0:
        detail = " ".join(stderr.split()) or "no output"
        raise ServiceError(f"launchctl bootstrap failed (exit {code}) for {plist}: {detail}")
    return plist


def missing_entries(current_path: str, service_path_value: str) -> list[str]:
    """Entries of ``current_path`` (this shell's PATH) the service PATH lacks, in order."""
    have = set(service_path_value.split(os.pathsep))
    out: list[str] = []
    for entry in current_path.split(os.pathsep):
        if entry and entry not in have and entry not in out:
            out.append(entry)
    return out


def wait_healthy(
    probe: Callable[[], bool],
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Poll ``probe`` (the server's /health answered 200) until true or ``HEALTH_WAIT_S``."""
    deadline = clock() + HEALTH_WAIT_S
    while not probe():
        if clock() >= deadline:
            return False
        sleep(HEALTH_POLL_S)
    return True


def err_tail(logs_dir: Path, lines: int = ERR_TAIL_LINES) -> str:
    """The last ``lines`` lines of the service's stderr log; a note when there is none."""
    path = logs_dir / ERR_LOG_NAME
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return f"({path} not written yet)"
    return "\n".join(text.splitlines()[-lines:]) or f"({path} is empty)"


def plist_home(plist: Path) -> Path | None:
    """The ``DIOXIDE_HOME`` an installed plist names; None when unreadable."""
    try:
        doc = plistlib.loads(plist.read_bytes())
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    home = (
        doc.get("EnvironmentVariables", {}).get("DIOXIDE_HOME") if isinstance(doc, dict) else None
    )
    return Path(home) if isinstance(home, str) and home else None


def loaded(label: str, uid: int) -> bool:
    return run_launchctl(launchctl_argv("print", label, Path(), uid))[0] == 0


def uninstall(
    label: str,
    user_home: Path,
    uid: int,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[Path, bool]:
    """Unload the service if loaded, then remove its plist; returns (plist, was loaded). A
    failed bootout raises ServiceError and keeps the plist (the service is still loaded)."""
    plist = plist_path(user_home, label)
    was_loaded = loaded(label, uid)
    if was_loaded:
        code, stderr = run_launchctl(launchctl_argv("uninstall", label, plist, uid))
        if code != 0:
            detail = " ".join(stderr.split()) or "no output"
            raise ServiceError(
                f"launchctl bootout failed (exit {code}) for {label}: {detail}; {plist} kept"
            )
        wait_until_gone(label, uid, clock, sleep)
    plist.unlink(missing_ok=True)
    return plist, was_loaded
