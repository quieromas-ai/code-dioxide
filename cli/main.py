import difflib
import ipaddress
import json
import os
import re
import secrets
import shlex
import shutil
import sqlite3
import sys
from collections.abc import Iterable, Iterator, Mapping
from contextlib import closing
from pathlib import Path
from time import monotonic, sleep
from typing import Any, NoReturn

import click
import httpx
from dotenv import dotenv_values

from cli import service
from cli.usage_repair import Plan, apply_plan, backup, plan_repair
from src import childenv
from src.childenv import ChildEnvError
from src.config import DEFAULT_HOME, AlreadyRunning, Settings, acquire_home_lock, resolve_home
from src.database import SCHEMA_VERSION
from src.main import serve

PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
TOKEN_BYTES = 32
HOME_DIRS = ("agents", "data", "logs")
HOME_EXAMPLES = ("roles.yaml",)
# The SSE stream sends a keep-alive every 15 s; a read idle for longer means a dead peer.
READ_TIMEOUT_S = 60.0
RECONNECT_S = 1.0
RECONNECT_MAX_S = 30.0
# Consecutive failed reconnects (no line read in between) before `logs --follow` gives up.
MAX_RECONNECTS = 8
TOKEN_KEY = "DIOXIDE_AUTH_TOKEN"
# `logs --url` reads its token from `--token` or this variable, never from the home's .env.
REMOTE_TOKEN_KEY = "DIOXIDE_REMOTE_TOKEN"
# An uncommented token assignment, whatever its value (one line: never crosses a newline).
_TOKEN_LINE = re.compile(rf"^[ \t]*(?:export[ \t]+)?{TOKEN_KEY}[ \t]*=.*$", re.MULTILINE)
_WILDCARD_HOSTS = ("0.0.0.0", "::", "")

ENV_STARTER = """\
# code-dioxide settings for this home (DIOXIDE_* variables; the process environment wins).
# Written by `dioxide init`; keep it private (mode 0600).
DIOXIDE_AUTH_TOKEN={token}
# Credentials for the claude sessions dioxide starts go in child.env next to this file, not
# here (mode 0600; see README: Credentials): DIOXIDE_* settings never reach the sessions.
# DIOXIDE_HOST=127.0.0.1
# DIOXIDE_PORT=8500
# The orchestrator (lifecycle parent session) needs a capable model with a large context
# window; stages may use cheaper models (set in the agents). Effort: low|medium|high|xhigh|max.
# DIOXIDE_PARENT_MODEL=opus
# DIOXIDE_PARENT_EFFORT=low
# The role-less script stages (setup, merge) only run a fixed command: a cheap model is enough.
# Unset: they run on the parent's model.
# DIOXIDE_SCRIPT_MODEL=haiku
# Command names (comma-separated) a sweep never kills; claude, shells and git are never exempt.
# DIOXIDE_SWEEP_EXEMPT=
# Claude Code settings passed verbatim to every session (--settings). This one turns off
# commit/PR attribution; add "permissions":{{"allow":["mcp__<server>__*"]}} for MCP tools.
# DIOXIDE_EXTRA_SETTINGS='{{"attribution":{{"commit":"","pr":""}}}}'
"""


@click.group()
@click.version_option(package_name="code-dioxide")
def cli() -> None:
    """code-dioxide command line."""


def load(*, secure: bool = False, **values: Any) -> Settings:
    """Settings as the server reads them (the environment, then the home's ``.env``). A
    configuration error is one line (``ClickException``), never a traceback. ``secure``: also
    the token check the server makes before it starts."""
    try:
        settings = Settings(**values)
        if secure:
            settings.validate_security()
    except ValueError as exc:  # pydantic's ValidationError included
        raise click.ClickException("invalid configuration: " + " ".join(str(exc).split())) from None
    return settings


def mask(token: str) -> str:
    return f"{token[:4]}…{token[-4:]}" if len(token) >= 12 else "…"


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=PRIVATE_DIR_MODE)
    path.chmod(PRIVATE_DIR_MODE)


def _copy_if_absent(source: Path, target: Path) -> bool:
    if target.exists():
        return False
    shutil.copyfile(source, target)
    return True


def _env_token(env_file: Path) -> str:
    """The token as the server reads it (python-dotenv, as pydantic-settings parses the file:
    quotes, ``export`` and inline comments handled); empty when unset or blank."""
    return (dotenv_values(env_file, encoding="utf-8").get(TOKEN_KEY) or "").strip()


def _write_private(path: Path, text: str) -> None:
    """Replace ``path`` atomically with ``text`` at mode 0600."""
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, PRIVATE_FILE_MODE)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    tmp.chmod(PRIVATE_FILE_MODE)
    os.replace(tmp, path)


def ensure_token(env_file: Path) -> tuple[str, bool]:
    """The token set in ``env_file``, or a new one written there: the starter file when it is
    missing, in place of a blank token line, else appended. Returns (token, created)."""
    text = env_file.read_text(encoding="utf-8") if env_file.exists() else None
    if text is not None and (current := _env_token(env_file)):
        return current, False
    token = secrets.token_urlsafe(TOKEN_BYTES)
    line = f"{TOKEN_KEY}={token}"
    if text is None:
        updated = ENV_STARTER.format(token=token)
    elif blanks := list(_TOKEN_LINE.finditer(text)):
        last = blanks[-1]  # the assignment dotenv reads (the last one wins)
        updated = text[: last.start()] + line + text[last.end() :]
    else:
        separator = "" if text == "" or text.endswith("\n") else "\n"
        updated = f"{text}{separator}{line}\n"
    _write_private(env_file, updated)
    return token, True


@cli.command()
@click.option(
    "--home",
    type=click.Path(file_okay=False, path_type=Path),
    help="Home dir (default: $DIOXIDE_HOME, else ~/.code-dioxide).",
)
def init(home: Path | None) -> None:
    """Create the home layout, copy the example config if absent, generate the auth token."""
    root = resolve_home(home)
    _private_dir(root)
    for name in HOME_DIRS:
        _private_dir(root / name)
    examples = service.asset_dir("examples")
    copied = [
        f"agents/{agent.name}"
        for agent in sorted((examples / "agents").glob("*.md"))
        if _copy_if_absent(agent, root / "agents" / agent.name)
    ]
    copied += [name for name in HOME_EXAMPLES if _copy_if_absent(examples / name, root / name)]
    token, created = ensure_token(root / ".env")
    served = load(home=root).auth_token  # what the server will read: the environment wins
    click.echo(f"home: {root}")
    click.echo(f"copied: {', '.join(copied)}" if copied else "copied: nothing (all present)")
    if served == token:
        state = "generated" if created else "kept"
        click.echo(f"auth token ({state}, in {root / '.env'}): {mask(token)}")
    else:
        click.echo(
            f"auth token (from the environment's {TOKEN_KEY}, which overrides "
            f"{root / '.env'}): {mask(served)}"
        )
    absolute = root.absolute()
    prefix = (
        ""
        if absolute == DEFAULT_HOME.expanduser().absolute()
        else f"DIOXIDE_HOME={shlex.quote(str(absolute))} "
    )
    click.echo(f"next: `{prefix}dioxide register` prints the `claude mcp add` command")


@cli.command()
def up() -> None:
    """Run the dioxide server in the foreground."""
    serve(load(secure=True))


def base_url(settings: Settings) -> str:
    host = "127.0.0.1" if settings.host in _WILDCARD_HOSTS else settings.host
    if ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{settings.port}"


def http_client(url: str, token: str, *, local: bool) -> httpx.Client:
    """``local``: this home's server; proxy settings from the environment are ignored, so a
    proxy never receives the home's token. A remote ``--url`` keeps httpx's defaults."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    timeout = httpx.Timeout(30.0, read=READ_TIMEOUT_S)
    return httpx.Client(base_url=url, headers=headers, timeout=timeout, trust_env=not local)


@cli.command()
def register() -> None:
    """Print the `claude mcp add` command for this server (it contains the auth token)."""
    settings = load()
    if not settings.auth_token and not settings.auth_disabled:
        raise click.ClickException(f"no auth token in {settings.home / '.env'}: run `dioxide init`")
    # User scope: the default (local) scope would register it for one project directory only.
    command = f"claude mcp add --scope user --transport http dioxide {base_url(settings)}/mcp"
    if settings.auth_token:
        command += f' --header "Authorization: Bearer {settings.auth_token}"'
    click.echo(command)


@cli.command()
def status() -> None:
    """Show whether the server answers, its task counts, the home and the (masked) token."""
    settings = load()
    url = base_url(settings)
    try:
        with http_client(url, settings.auth_token, local=True) as client:
            response = client.get("/health")
    except httpx.HTTPError as exc:
        click.echo(f"dioxide: not reachable at {url} ({exc})")
        raise SystemExit(1) from None
    if response.status_code != 200:
        click.echo(f"dioxide: {url} answered HTTP {response.status_code}")
        raise SystemExit(1)
    body = response.json()
    click.echo(f"dioxide: {body.get('status')} at {url}")
    for key, value in body.items():
        if key != "status":
            click.echo(f"  {key}: {json.dumps(value)}")
    click.echo(f"home: {settings.home}")
    click.echo(f"auth token: {mask(settings.auth_token) if settings.auth_token else 'none'}")
    click.echo(f"child env: {child_env_names(settings)}")


def child_env_names(settings: Settings) -> str:
    """The child env file's variable names (never a value), ``none``, or why it is unusable."""
    if settings.child_env_file is None:
        return "none"
    try:
        names = sorted(childenv.read(settings.child_env_file))
    except ChildEnvError as exc:
        return f"unusable: {exc}"
    return ", ".join(names) if names else "none"


@cli.group("service")
def service_group() -> None:
    """Install or remove the launchd user agent (macOS)."""


@service_group.command("install")
@click.option("--label", default=service.DEFAULT_LABEL, show_default=True)
@click.option("--extra-path", multiple=True, help="Prepended to the login-shell PATH; repeatable.")
def service_install(label: str, extra_path: tuple[str, ...]) -> None:
    """Write the plist (PATH captured from the login shell now), load it, wait for /health."""
    try:
        service.require_supported()
        settings = load()
        home = settings.home.absolute()  # launchd has no working directory to resolve it in
        env_file = home / ".env"
        file_token = _env_token(env_file) if env_file.is_file() else ""
        if not file_token and not settings.auth_disabled:
            where = "is set only in the environment" if settings.auth_token else "is missing"
            raise service.ServiceError(
                f"{TOKEN_KEY} {where}: the service reads it from {env_file} (the plist does not "
                "carry it); run `dioxide init` first"
            )
        shell = os.environ.get("SHELL") or "/bin/zsh"
        extra = [*settings.extra_path, *extra_path]
        path = service.service_path(service.login_shell_path(shell), extra)
        spec = service.build_spec(
            label=label,
            home=home,
            user_home=Path.home(),
            path=path,
            claude_bin=settings.claude_bin,
            python=sys.executable,
        )
        gaps = service.missing_entries(os.environ.get("PATH", ""), path)
        in_file = dotenv_values(env_file, encoding="utf-8") if env_file.is_file() else {}
        env_only = unseen_settings(os.environ, in_file)
        # The address the service binds: its .env and the defaults, never this shell's values.
        defaults = Settings.model_fields
        url = base_url(
            load(
                home=home,
                host=in_file.get("DIOXIDE_HOST") or defaults["host"].default,
                port=in_file.get("DIOXIDE_PORT") or defaults["port"].default,
            )
        )
        plist = service.install(spec, Path.home(), os.getuid())
    except service.ServiceError as exc:
        raise click.ClickException(str(exc)) from None
    if gaps:
        click.echo(
            f"note: on this shell's PATH but not the service's: {', '.join(gaps)} (add the ones "
            "the service needs with --extra-path <dir> and reinstall)",
            err=True,
        )
    if env_only:
        click.echo(
            f"warning: set in this environment but not in {env_file}, so the service does not "
            f"see them: {', '.join(env_only)}",
            err=True,
        )
    click.echo(f"installed {label}: {plist} (logs in {spec.logs_dir})")

    def probe() -> bool:
        try:
            with http_client(url, file_token, local=True) as client:
                return client.get("/health").status_code == 200
        except httpx.HTTPError:
            return False

    if not service.wait_healthy(probe, monotonic, sleep):
        log = spec.logs_dir / service.ERR_LOG_NAME
        click.echo(
            f"Error: {label} did not answer {url}/health within {service.HEALTH_WAIT_S:g}s; "
            f"the end of {log}:",
            err=True,
        )
        click.echo(service.err_tail(spec.logs_dir), err=True)
        click.echo(
            f"The service stays loaded: launchd retries it every {service.THROTTLE_INTERVAL_S} s; "
            "`dioxide service uninstall` stops it.",
            err=True,
        )
        raise SystemExit(1)
    click.echo(f"running: {url}")


# Names the service gets without the .env: the plist carries them, or they are not settings.
_SERVICE_CARRIED = frozenset({"DIOXIDE_HOME", "DIOXIDE_CLAUDE_BIN", REMOTE_TOKEN_KEY})


def unseen_settings(environ: Mapping[str, str], in_file: Mapping[str, str | None]) -> list[str]:
    """``DIOXIDE_*`` names set in ``environ`` but absent from the home's .env (``in_file``): the
    service, which gets only the plist's environment, does not see them. Names only."""
    present = {name.upper() for name in in_file}
    return sorted(
        name
        for name in environ
        if name.upper().startswith("DIOXIDE_")
        and name.upper() not in present
        and name.upper() not in _SERVICE_CARRIED
    )


def running_tasks(db_path: Path) -> int | None:
    """Tasks in status ``running`` (read-only); 0 without a database, None when unreadable."""
    if not db_path.is_file():
        return 0
    try:
        with closing(sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True)) as conn:
            row = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = 'running'").fetchone()
    except sqlite3.Error:
        return None
    return int(row[0])


@service_group.command("uninstall")
@click.option("--label", default=service.DEFAULT_LABEL, show_default=True)
def service_uninstall(label: str) -> None:
    """Unload the service and remove its plist (running tasks are left alone)."""
    try:
        service.require_supported()
        user_home = Path.home()
        home = service.plist_home(service.plist_path(user_home, label)) or resolve_home()
        plist, was_loaded = service.uninstall(label, user_home, os.getuid(), monotonic, sleep)
    except service.ServiceError as exc:
        raise click.ClickException(str(exc)) from None
    state = "unloaded" if was_loaded else "not loaded"
    click.echo(f"uninstalled {label} ({state}): {plist} removed")
    db_path = Settings.model_construct(home=home).db_path  # the layout only: nothing is read
    count = running_tasks(db_path)
    if count is None:
        click.echo(f"note: cannot read the running tasks from {db_path}", err=True)
    elif count:
        noun = "task" if count == 1 else "tasks"
        click.echo(
            f"{count} {noun} still running without supervision; `dioxide up` or "
            "`dioxide service install` re-attaches them"
        )


def _split_agent(text: str) -> tuple[list[str], list[str]]:
    """(frontmatter lines, body lines) of an agent file; no frontmatter: all body."""
    lines = text.splitlines(keepends=True)
    if lines and lines[0].strip() == "---":
        for end in range(1, len(lines)):
            if lines[end].strip() == "---":
                return lines[1:end], lines[end + 1 :]
    return [], lines


def _unprefixed_name(lines: list[str], prefix: str) -> list[str]:
    """Frontmatter lines with ``prefix`` dropped from the ``name:`` value."""
    if not prefix:
        return lines
    pattern = re.compile(rf"^(name:[ \t]*[\"']?){re.escape(prefix)}")
    return [pattern.sub(r"\1", line, count=1) for line in lines]


def _part_diff(home: list[str], source: list[str], home_label: str, source_label: str) -> str:
    diff = difflib.unified_diff(home, source, home_label, source_label)
    return "".join(line if line.endswith("\n") else line + "\n" for line in diff)


@cli.command("sync-agents")
@click.option(
    "--source",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Dir of agent definitions (*.md) to compare with the home's agents dir.",
)
@click.option(
    "--prefix",
    default="",
    help="Home copies are named <prefix><name>.md (and `name: <prefix><name>`).",
)
def sync_agents(source: Path, prefix: str) -> None:
    """Show drift between SOURCE and the home's agents; never writes. Exit 1 on drift."""
    agents_dir = load().agents_dir
    theirs = {p.name: p for p in source.glob("*.md") if p.is_file()}
    ours: dict[str, Path] = {}
    home_only: list[str] = []
    for path in agents_dir.glob("*.md"):
        if not path.is_file():
            continue
        if path.name.startswith(prefix) and len(path.name) > len(prefix):
            ours[path.name.removeprefix(prefix)] = path
        else:
            home_only.append(path.name)
    drift = False
    for name in sorted(theirs.keys() | ours.keys()):
        if name not in ours:
            click.echo(f"only in source: {name}")
        elif name not in theirs:
            home_only.append(ours[name].name)
            continue
        else:
            home_meta, home_body = _split_agent(ours[name].read_text(encoding="utf-8"))
            source_meta, source_body = _split_agent(theirs[name].read_text(encoding="utf-8"))
            home_label, source_label = f"home/{ours[name].name}", f"source/{name}"
            text = _part_diff(
                _unprefixed_name(home_meta, prefix),
                source_meta,
                f"{home_label} (frontmatter)",
                f"{source_label} (frontmatter)",
            ) + _part_diff(home_body, source_body, home_label, source_label)
            if not text:
                continue
            click.echo(text)
        drift = True
    for home_name in sorted(home_only):
        click.echo(f"only in home: {home_name}")
        drift = True
    if not drift:
        click.echo("no drift")
    raise SystemExit(1 if drift else 0)


# -- logs -----------------------------------------------------------------------------------
def _fail(message: str) -> NoReturn:
    click.echo(f"Error: {message}", err=True)
    raise SystemExit(1)


def _entry_line(entry: dict[str, Any], source: str | None) -> str:
    line = str(entry.get("line", ""))
    return line if source else f"{entry.get('source')} | {line}"


def _error_of(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        body = None
    detail = body.get("error") if isinstance(body, dict) else None
    return f"HTTP {response.status_code}: {detail or response.text[:200]}"


class MalformedEvent(ValueError):
    """An SSE ``data:`` payload that is not JSON."""


def sse_events(lines: Iterable[str]) -> Iterator[tuple[str | None, Any, str | None]]:
    """(event, decoded data, id) per dispatched SSE event; comments are skipped."""
    event: str | None = None
    event_id: str | None = None
    data: list[str] = []
    for line in lines:
        if line == "":
            if data:
                try:
                    decoded = json.loads("\n".join(data))
                except ValueError as exc:
                    raise MalformedEvent(f"malformed event from server: {exc}") from None
                yield event, decoded, event_id
            event, event_id, data = None, None, []
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if name == "event":
            event = value
        elif name == "data":
            data.append(value)
        elif name == "id":
            event_id = value


def plain_http_remote(url: str) -> str | None:
    """The host of an ``http://`` URL that is not loopback; None otherwise."""
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL:
        return None
    host = parsed.host
    if parsed.scheme != "http" or host == "localhost":
        return None
    try:
        if ipaddress.ip_address(host).is_loopback:
            return None
    except ValueError:
        pass
    return host


def follow_logs(client: httpx.Client, task_id: str, source: str | None) -> None:
    """Print the task's SSE stream until ``done``; a dropped stream reconnects from the last
    ``id:`` cursor (entries after it may repeat, none are lost). Each reconnect is reported on
    stderr; after ``MAX_RECONNECTS`` consecutive failures (no line read) it gives up."""
    cursor: str | None = None
    failures = 0
    read = [False]  # a line arrived since the last reconnect: the failure count restarts

    def counted(lines: Iterable[str]) -> Iterator[str]:
        for line in lines:
            read[0] = True
            yield line

    while True:
        params = {k: v for k, v in (("source", source), ("cursor", cursor)) if v}
        read[0] = False
        try:
            with client.stream("GET", f"/api/v1/tasks/{task_id}/stream", params=params) as resp:
                if resp.status_code == 503:
                    reason = "server not ready (HTTP 503)"
                elif resp.status_code != 200:
                    resp.read()
                    _fail(_error_of(resp))
                else:
                    reason = "stream ended"
                    for event, data, event_id in sse_events(counted(resp.iter_lines())):
                        if event == "error":
                            _fail(str(data.get("error") if isinstance(data, dict) else data))
                        if event == "done":
                            return
                        click.echo(_entry_line(data, source))
                        if event_id:
                            cursor = event_id
        except MalformedEvent as exc:
            _fail(str(exc))
        except httpx.TransportError as exc:
            reason = f"connection failed ({exc or type(exc).__name__})"
        if read[0]:
            failures = 0
        if failures >= MAX_RECONNECTS:
            _fail(f"{reason}; giving up after {MAX_RECONNECTS} reconnects")
        delay = min(RECONNECT_S * 2**failures, RECONNECT_MAX_S)
        click.echo(
            f"dioxide logs: {reason}; reconnecting in {delay:g}s ({failures + 1}/{MAX_RECONNECTS})",
            err=True,
        )
        sleep(delay)
        failures += 1


def page_logs(client: httpx.Client, task_id: str, source: str | None) -> None:
    """Print every entry available now, page by page."""
    cursor: str | None = None
    while True:
        params = {k: v for k, v in (("source", source), ("cursor", cursor)) if v}
        response = client.get(f"/api/v1/tasks/{task_id}/logs", params=params)
        if response.status_code != 200:
            _fail(_error_of(response))
        page = response.json()
        for entry in page["entries"]:
            click.echo(_entry_line(entry, source))
        cursor = page.get("next_cursor")
        if not page["entries"] or page.get("done"):
            return


def _usage_text(cost: float | None, tokens: int | None) -> str:
    return f"${cost:.4f}/{tokens} tok" if cost is not None else f"$?/{tokens} tok"


def _print_plan(plan: Plan) -> None:
    for fix in plan.fixes:
        for a in fix.attempts:
            old, new = (
                _usage_text(a.old.cost_usd, a.old.tokens),
                _usage_text(a.new.cost_usd, a.new.tokens),
            )
            click.echo(f"{fix.task_id} attempt {a.n}: {old} -> {new}")
        for m in fix.models:
            names = ", ".join(sorted(m.shares)) or "none (split removed)"
            click.echo(f"{fix.task_id} attempt {m.n} models: {names}")
        if fix.output_json is not None:
            out = json.loads(fix.output_json)
            click.echo(f"{fix.task_id} output: {_usage_text(out['cost_usd'], out['tokens_used'])}")
    for skip in plan.skipped:
        click.echo(f"{skip.task_id} skipped: {skip.reason}")
    for stage in plan.stages:
        click.echo(f"{stage.task_id} stage {stage.label}: numbers from run.json")
    rows = sum(len(f.attempts) for f in plan.fixes)
    outputs = sum(f.output_json is not None for f in plan.fixes)
    splits = sum(len(f.models) for f in plan.fixes)
    click.echo(
        f"{rows} attempt rows, {outputs} outputs, {splits} model splits, "
        f"{len(plan.stages)} stages to change; "
        f"{len(plan.skipped)} tasks skipped"
    )


@cli.command("repair-usage")
@click.option(
    "--apply", "apply_", is_flag=True, help="Write the changes (default: only show them)."
)
def repair_usage(apply_: bool) -> None:
    """Recompute recorded cost and tokens from the stored streams (dry run by default)."""
    settings = load()
    db_path = settings.db_path
    if not db_path.is_file():
        _fail(f"no database at {db_path}")
    lock = None
    if apply_:
        try:
            lock = acquire_home_lock(settings.lock_path)
        except AlreadyRunning:
            _fail(f"dioxide is running for home {settings.home}: stop it first")
    try:
        uri = db_path.as_uri() + ("" if apply_ else "?mode=ro")
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version != SCHEMA_VERSION:
                _fail(
                    f"database schema v{version}, this code v{SCHEMA_VERSION}: start dioxide once"
                )
            plan = plan_repair(conn, settings.runs_dir)
            _print_plan(plan)
            if not apply_:
                if not plan.empty:
                    click.echo("dry run: run with --apply to write (stop dioxide first)")
                return
            if not plan.empty:
                click.echo(f"backup: {backup(conn, db_path)}")
                apply_plan(conn, plan)
                click.echo("applied")
    finally:
        if lock is not None:
            lock.release()


@cli.command()
@click.argument("task_id")
@click.option("--source", help="One log source (default: the merged view).")
@click.option("--follow", is_flag=True, help="Stream until the task is done (reconnects).")
@click.option("--url", help="Server base URL (default: this home's host and port).")
@click.option(
    "--token",
    help=f"Bearer token (default: this home's {TOKEN_KEY}; with --url: ${REMOTE_TOKEN_KEY}).",
)
def logs(
    task_id: str, source: str | None, follow: bool, url: str | None, token: str | None
) -> None:
    """Print a task's logs from a local or remote server."""
    local = url is None
    if url is None:
        settings = load()
        url = base_url(settings)
        token = settings.auth_token if token is None else token
    else:  # another server: this home's token is never sent there
        token = token if token is not None else os.environ.get(REMOTE_TOKEN_KEY, "")
        if not token:
            raise click.ClickException(
                f"--url needs a token: --token or {REMOTE_TOKEN_KEY} (this home's {TOKEN_KEY} "
                "is never sent to --url)"
            )
        if (host := plain_http_remote(url)) is not None:
            click.echo(
                f"warning: {url} is plain http:// to {host} (not loopback): the token travels "
                "unencrypted",
                err=True,
            )
    try:
        with http_client(url, token, local=local) as client:
            if follow:
                follow_logs(client, task_id, source)
            else:
                page_logs(client, task_id, source)
    except httpx.HTTPError as exc:
        _fail(f"{url}: {exc}")


if __name__ == "__main__":
    cli()
