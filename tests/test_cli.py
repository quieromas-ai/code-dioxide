import json
import os
import plistlib
import re
import shutil
import sqlite3
import stat
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from click.testing import CliRunner

from cli import main as cli_main
from cli import service
from cli.main import cli
from src.config import Settings
from src.database import SCHEMA

TOKEN_LINE = "DIOXIDE_AUTH_TOKEN="


def env_token(home: Path) -> list[str]:
    lines = (home / ".env").read_text().splitlines()
    return [line[len(TOKEN_LINE) :] for line in lines if line.startswith(TOKEN_LINE)]


def snapshot(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


# -- init -----------------------------------------------------------------------------------
def test_init_idempotent_generates_token_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    runner = CliRunner()
    first = runner.invoke(cli, ["init", "--home", str(home)])
    assert first.exit_code == 0, first.output
    (token,) = env_token(home)
    assert len(token) >= 32
    assert token not in first.output and f"{token[:4]}…{token[-4:]}" in first.output
    assert stat.S_IMODE((home / ".env").stat().st_mode) == 0o600
    assert stat.S_IMODE(home.stat().st_mode) == 0o700
    for sub in ("agents", "data", "logs"):
        assert (home / sub).is_dir()
    examples = Path(__file__).resolve().parent.parent / "examples"
    assert (home / "roles.yaml").read_bytes() == (examples / "roles.yaml").read_bytes()
    assert not (home / "settings.json").exists()  # nothing reads it: not copied
    shipped = sorted(p.name for p in (examples / "agents").glob("*.md"))
    assert sorted(p.name for p in (home / "agents").glob("*.md")) == shipped
    text = (home / ".env").read_text()
    for name in ("PARENT_MODEL", "PARENT_EFFORT", "SCRIPT_MODEL", "SWEEP_EXEMPT", "EXTRA_SETTINGS"):
        assert f"# DIOXIDE_{name}=" in text
    assert not (home / "child.env").exists()  # credentials are the user's to add
    assert "# Credentials for the claude sessions dioxide starts go in child.env" in text
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    assert Settings().extra_settings == {}  # the example stays commented out
    assert "slack" not in text.lower()

    # A user's edits survive a second init; the token is never regenerated.
    (home / "roles.yaml").write_text("planner: mine\n")
    (home / "agents" / "planner.md").unlink()
    before = snapshot(home)
    second = runner.invoke(cli, ["init", "--home", str(home)])
    assert second.exit_code == 0, second.output
    assert env_token(home) == [token] and token not in second.output
    assert (home / "roles.yaml").read_text() == "planner: mine\n"
    after = snapshot(home)
    assert {k: v for k, v in after.items() if k != "agents/planner.md"} == before
    assert "agents/planner.md" in after  # a missing example is copied again

    # An existing .env without a token gets one appended; its other lines stay.
    other = tmp_path / "other"
    other.mkdir()
    (other / ".env").write_text("DIOXIDE_PORT=8600\n# DIOXIDE_AUTH_TOKEN=commented\n")
    assert runner.invoke(cli, ["init", "--home", str(other)]).exit_code == 0
    (appended,) = env_token(other)
    assert (other / ".env").read_text().startswith("DIOXIDE_PORT=8600\n# DIOXIDE_AUTH_TOKEN=")
    assert appended != token and len(appended) >= 32


def test_init_blank_token_line_replaced_once(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("# keep me\nDIOXIDE_AUTH_TOKEN=\nDIOXIDE_HOST=127.0.0.1\n")
    runner = CliRunner()
    first = runner.invoke(cli, ["init", "--home", str(home)])
    assert first.exit_code == 0, first.output
    (token,) = env_token(home)
    assert len(token) >= 32 and "generated" in first.output
    assert "127." not in first.output  # nothing of the next line is shown as the token
    assert (home / ".env").read_text() == (
        f"# keep me\nDIOXIDE_AUTH_TOKEN={token}\nDIOXIDE_HOST=127.0.0.1\n"
    )
    assert stat.S_IMODE((home / ".env").stat().st_mode) == 0o600
    second = runner.invoke(cli, ["init", "--home", str(home)])
    assert second.exit_code == 0 and "kept" in second.output
    assert env_token(home) == [token] and f"{token[:4]}…{token[-4:]}" in second.output


@pytest.mark.parametrize(
    "line",
    [
        "DIOXIDE_AUTH_TOKEN=abcd1234efgh5678 # a comment",
        "DIOXIDE_AUTH_TOKEN='abcd1234efgh5678'",
        'export DIOXIDE_AUTH_TOKEN="abcd1234efgh5678"',
    ],
)
def test_init_masks_the_value_settings_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, line: str
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text(line + "\n")
    result = CliRunner().invoke(cli, ["init", "--home", str(home)])
    assert result.exit_code == 0 and "kept" in result.output
    assert "abcd…5678" in result.output
    assert (home / ".env").read_text() == line + "\n"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    assert Settings().auth_token == "abcd1234efgh5678"  # the same value the server reads


def test_init_home_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "from-env"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    assert CliRunner().invoke(cli, ["init"]).exit_code == 0
    assert len(env_token(home)) == 1


def test_asset_dirs_packaged_then_source_tree(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parent.parent
    assert service.asset_dir("examples") == repo / "examples"  # this checkout
    assert (service.asset_dir("templates") / service.TEMPLATE_NAME).is_file()
    package = tmp_path / "site" / "cli"
    (package / "templates").mkdir(parents=True)
    (tmp_path / "site" / "templates").mkdir()
    assert service.asset_dir("templates", package) == package / "templates"  # packaged first
    (package / "templates").rmdir()
    assert service.asset_dir("templates", package) == tmp_path / "site" / "templates"
    with pytest.raises(FileNotFoundError):
        service.asset_dir("examples", package)


# -- register / status ------------------------------------------------------------------------
def init_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    home = tmp_path / "home"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    assert CliRunner().invoke(cli, ["init"]).exit_code == 0
    (token,) = env_token(home)
    return home, token


def test_register_prints_full_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, token = init_home(tmp_path, monkeypatch)
    result = CliRunner().invoke(cli, ["register"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == (
        "claude mcp add --scope user --transport http dioxide http://127.0.0.1:8500/mcp "
        f'--header "Authorization: Bearer {token}"'
    )


def test_register_without_token_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    result = CliRunner().invoke(cli, ["register"])
    assert result.exit_code != 0 and "dioxide init" in result.output


def mock_client(handler: Any, monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def recording(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        response: httpx.Response = handler(request)
        return response

    def factory(url: str, token: str, *, local: bool) -> httpx.Client:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        headers["X-Test-Local"] = str(local)  # which client kind the command asked for
        return httpx.Client(base_url=url, headers=headers, transport=httpx.MockTransport(recording))

    monkeypatch.setattr(cli_main, "http_client", factory)
    return seen


def test_status_masks_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home, token = init_home(tmp_path, monkeypatch)
    seen = mock_client(
        lambda r: httpx.Response(200, json={"status": "ok", "running": 1, "queued": 2}),
        monkeypatch,
    )
    result = CliRunner().invoke(cli, ["status"])
    assert result.exit_code == 0, result.output
    assert token not in result.output and f"{token[:4]}…{token[-4:]}" in result.output
    assert "ok" in result.output and "running" in result.output and str(home) in result.output
    assert seen[0].url.path == "/health"
    assert seen[0].headers["Authorization"] == f"Bearer {token}"


def test_status_names_child_env_variables_never_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, _ = init_home(tmp_path, monkeypatch)
    mock_client(lambda r: httpx.Response(200, json={"status": "ok"}), monkeypatch)
    assert "child env: none" in CliRunner().invoke(cli, ["status"]).output
    child = home / "child.env"
    child.write_text("CLAUDE_CODE_OAUTH_TOKEN=placeholder-oauth-value-0001\nANTHROPIC_API_KEY=x\n")
    child.chmod(0o600)
    result = CliRunner().invoke(cli, ["status"])
    assert result.exit_code == 0, result.output
    assert "child env: ANTHROPIC_API_KEY, CLAUDE_CODE_OAUTH_TOKEN" in result.output
    assert "placeholder-oauth-value-0001" not in result.output
    child.chmod(0o644)
    result = CliRunner().invoke(cli, ["status"])
    assert "child env: unusable: " in result.output and "chmod 600" in result.output


def test_status_unreachable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_home(tmp_path, monkeypatch)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    mock_client(refuse, monkeypatch)
    result = CliRunner().invoke(cli, ["status"])
    assert result.exit_code == 1 and "not reachable" in result.output


# -- service ----------------------------------------------------------------------------------
@pytest.fixture
def fake_shell(tmp_path: Path) -> Path:
    shell = tmp_path / "bin" / "fake-shell"
    shell.parent.mkdir()
    shell.write_text(
        "#!/bin/sh\n"
        '[ "$1" = "-lc" ] && [ "$2" = \'echo $PATH\' ] || exit 3\n'
        "echo 'login banner noise'\n"
        "echo /login/bin:/usr/bin:/bin\n"
    )
    shell.chmod(0o755)
    claude = tmp_path / "bin" / "claude"
    claude.write_text("#!/bin/sh\n")
    claude.chmod(0o755)
    return shell


def test_plist_login_shell_path_abandon_pg(tmp_path: Path, fake_shell: Path) -> None:
    login = service.login_shell_path(str(fake_shell))
    assert login == "/login/bin:/usr/bin:/bin"  # the last line: banners are ignored
    bin_dir = str(fake_shell.parent)
    path = service.service_path(login, ["/extra/one", "/usr/bin", bin_dir])
    assert path == f"/extra/one:/usr/bin:{bin_dir}:/login/bin:/bin"  # as the launcher: prepended

    home = tmp_path / "dioxide-home"
    spec = service.build_spec(
        label=service.DEFAULT_LABEL,
        home=home,
        user_home=tmp_path / "user",
        path=path,
        claude_bin="claude",
        python="/venv/bin/python3",
    )
    doc = plistlib.loads(service.render_plist(spec).encode())
    assert doc["Label"] == "dev.code-dioxide"
    # -P: the working directory (the home) is not put on sys.path, so it cannot shadow cli/src
    assert doc["ProgramArguments"] == ["/venv/bin/python3", "-P", "-m", "cli.main", "up"]
    assert doc["ExitTimeOut"] == 20 and doc["ThrottleInterval"] == 30
    assert service.BOOTOUT_WAIT_S == 30 and service.BOOTOUT_WAIT_S > doc["ExitTimeOut"]
    assert doc["KeepAlive"] is True and doc["RunAtLoad"] is True
    assert doc["AbandonProcessGroup"] is True
    assert doc["StandardOutPath"] == str(home / "logs" / "launchd.out.log")
    assert doc["StandardErrorPath"] == str(home / "logs" / "launchd.err.log")
    assert doc["EnvironmentVariables"] == {
        "HOME": str(tmp_path / "user"),
        "PATH": path,
        "DIOXIDE_HOME": str(home),
        "DIOXIDE_CLAUDE_BIN": str(fake_shell.parent / "claude"),  # absolute, found on PATH
    }
    assert service.plist_path(tmp_path / "user", "dev.code-dioxide") == (
        tmp_path / "user" / "Library" / "LaunchAgents" / "dev.code-dioxide.plist"
    )
    plist = Path("/p/dev.code-dioxide.plist")
    assert service.launchctl_argv("install", "dev.code-dioxide", plist, 501) == [
        "launchctl", "bootstrap", "gui/501", "/p/dev.code-dioxide.plist",
    ]  # fmt: skip
    assert service.launchctl_argv("uninstall", "dev.code-dioxide", plist, 501) == [
        "launchctl", "bootout", "gui/501/dev.code-dioxide",
    ]  # fmt: skip


@pytest.mark.skipif(shutil.which("plutil") is None, reason="macOS plutil not available")
def test_plist_passes_plutil_lint(tmp_path: Path, fake_shell: Path) -> None:
    spec = service.build_spec(
        label=service.DEFAULT_LABEL, home=tmp_path / "h", user_home=tmp_path,
        path=str(fake_shell.parent), claude_bin="claude", python="/py",
    )  # fmt: skip
    plist = tmp_path / "x.plist"
    plist.write_text(service.render_plist(spec))
    lint = subprocess.run(["plutil", "-lint", str(plist)], capture_output=True, text=True)
    assert lint.returncode == 0, lint.stdout + lint.stderr


def test_plist_escapes_xml(tmp_path: Path, fake_shell: Path) -> None:
    spec = service.build_spec(
        label="a&b", home=tmp_path / "h<1>", user_home=tmp_path, path=str(fake_shell.parent),
        claude_bin="claude", python="/py",
    )  # fmt: skip
    doc = plistlib.loads(service.render_plist(spec).encode())
    assert doc["Label"] == "a&b" and doc["EnvironmentVariables"]["DIOXIDE_HOME"].endswith("h<1>")


def test_plist_requires_resolvable_claude(tmp_path: Path) -> None:
    with pytest.raises(service.ServiceError, match="not found"):
        service.build_spec(
            label="x", home=tmp_path, user_home=tmp_path, path=str(tmp_path / "empty"),
            claude_bin="claude", python="/py",
        )  # fmt: skip


@pytest.fixture
def launchctl(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[list[str]]]:
    """Records launchctl argv and tracks one service: ``bootstrap`` loads it, ``bootout``
    unloads it, ``print`` exits 0 while it is loaded and 113 once it is gone."""
    calls: list[list[str]] = []
    loaded = [False]

    def record(argv: list[str]) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "print":
            return (0, "") if loaded[0] else (113, "Could not find service")
        if argv[1] == "bootout":
            was, loaded[0] = loaded[0], False
            return (0, "") if was else (3, "Boot-out failed: 3: No such process")
        loaded[0] = True
        return 0, ""

    monkeypatch.setattr(service, "run_launchctl", record)
    yield calls


@pytest.fixture
def healthy(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    """The installed server answers /health at once; the health wait never really sleeps."""
    clock = FakeClock()
    monkeypatch.setattr(cli_main, "monotonic", clock)
    monkeypatch.setattr(cli_main, "sleep", clock.sleep)
    return mock_client(lambda r: httpx.Response(200, json={"status": "ok"}), monkeypatch)


def test_service_install_uninstall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    home, token = init_home(tmp_path, monkeypatch)
    user = tmp_path / "user"
    monkeypatch.setenv("HOME", str(user))
    monkeypatch.setenv("SHELL", str(fake_shell))
    monkeypatch.setattr(service, "PLATFORM", "darwin")
    runner = CliRunner()
    bin_dir = str(fake_shell.parent)  # holds the fake claude
    args = ["service", "install", "--extra-path", "/opt/x/bin", "--extra-path", bin_dir]
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output
    plist = user / "Library" / "LaunchAgents" / "dev.code-dioxide.plist"
    doc = plistlib.loads(plist.read_bytes())
    env = doc["EnvironmentVariables"]
    assert env["PATH"] == f"/opt/x/bin:{bin_dir}:/login/bin:/usr/bin:/bin"  # extras first
    assert env["DIOXIDE_HOME"] == str(home) and env["HOME"] == str(user)
    assert stat.S_IMODE(plist.stat().st_mode) == 0o644
    assert token not in plist.read_text()  # the server reads the token from DIOXIDE_HOME/.env
    uid = os.getuid()
    target = f"gui/{uid}/dev.code-dioxide"
    assert launchctl == [
        ["launchctl", "bootout", target],
        ["launchctl", "print", target],
        ["launchctl", "bootstrap", f"gui/{uid}", str(plist)],
    ]
    assert (home / "logs").is_dir()
    assert "running" in result.output and healthy[-1].url.path == "/health"

    result = runner.invoke(cli, ["service", "uninstall"])
    assert result.exit_code == 0, result.output
    assert launchctl[3:] == [
        ["launchctl", "print", target],
        ["launchctl", "bootout", target],
        ["launchctl", "print", target],
    ]
    assert not plist.exists()
    assert "without supervision" not in result.output  # no running task: nothing said


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def spec_for(tmp_path: Path, fake_shell: Path) -> service.ServiceSpec:
    return service.build_spec(
        label="dev.x", home=tmp_path / "h", user_home=tmp_path, path=str(fake_shell.parent),
        claude_bin="claude", python="/py",
    )  # fmt: skip


def test_install_waits_for_bootout_before_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path
) -> None:
    """A reinstall over a running service bootstraps only once ``print`` reports it gone."""
    calls: list[list[str]] = []
    still_there = [0, 0, 0]  # three polls see the old service, then it is gone

    def fake(argv: list[str]) -> tuple[int, str]:
        calls.append(argv)
        if argv[1] == "print":
            return (0, "") if still_there and still_there.pop() == 0 else (113, "")
        return 0, ""

    monkeypatch.setattr(service, "run_launchctl", fake)
    clock = FakeClock()
    service.install(spec_for(tmp_path, fake_shell), tmp_path, 501, clock=clock, sleep=clock.sleep)
    assert [c[1] for c in calls] == ["bootout", "print", "print", "print", "print", "bootstrap"]
    assert clock.sleeps == [service.BOOTOUT_POLL_S] * 3


def test_install_gives_up_when_bootout_never_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path
) -> None:
    calls: list[list[str]] = []

    def fake(argv: list[str]) -> tuple[int, str]:
        calls.append(argv)
        return 0, ""  # print always finds the service

    monkeypatch.setattr(service, "run_launchctl", fake)
    clock = FakeClock()
    with pytest.raises(service.ServiceError, match="still loaded"):
        service.install(
            spec_for(tmp_path, fake_shell), tmp_path, 501, clock=clock, sleep=clock.sleep
        )
    assert "bootstrap" not in [c[1] for c in calls]
    assert clock.now <= service.BOOTOUT_WAIT_S + service.BOOTOUT_POLL_S


def test_service_install_bootstrap_failure_shows_stderr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path
) -> None:
    init_home(tmp_path, monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path / "user"))
    monkeypatch.setenv("SHELL", str(fake_shell))
    monkeypatch.setattr(service, "PLATFORM", "darwin")

    def fake(argv: list[str]) -> tuple[int, str]:
        if argv[1] == "bootstrap":
            return 5, "Bootstrap failed: 5: Input/output error\n"
        return 113, ""

    monkeypatch.setattr(service, "run_launchctl", fake)
    args = ["service", "install", "--extra-path", str(fake_shell.parent)]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 1, result.output
    assert "Bootstrap failed: 5: Input/output error" in result.output
    assert "Traceback" not in result.output


def test_service_linux_not_supported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, launchctl: list[list[str]]
) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setattr(service, "PLATFORM", "linux")
    for action in ("install", "uninstall"):
        result = CliRunner().invoke(cli, ["service", action])
        assert result.exit_code != 0 and "not yet supported" in result.output
    assert launchctl == []


def test_up_rejects_home_in_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("DIOXIDE_AUTH_TOKEN=placeholder-value-1234\nDIOXIDE_HOME=/x\n")
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    monkeypatch.setattr(cli_main, "serve", lambda settings: pytest.fail("must not serve"))
    result = CliRunner().invoke(cli, ["up"])
    assert result.exit_code == 1, result.output
    lines = result.output.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("Error: invalid configuration: ")
    assert str(home / ".env") in lines[0] and "DIOXIDE_HOME" in lines[0]


# -- sync-agents ------------------------------------------------------------------------------
def test_sync_agents_never_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    (home / "agents").mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "same.md").write_text("same\n")
    (home / "agents" / "same.md").write_text("same\n")
    (source / "drift.md").write_text("line one\nsource line\n")
    (home / "agents" / "drift.md").write_text("line one\nhome line\n")
    (source / "new.md").write_text("new\n")
    (home / "agents" / "local.md").write_text("local\n")
    (source / "notes.txt").write_text("not an agent\n")
    before = snapshot(tmp_path)
    result = CliRunner().invoke(cli, ["sync-agents", "--source", str(source)])
    assert snapshot(tmp_path) == before
    assert result.exit_code == 1, result.output  # drift found
    out = result.output
    assert "-home line" in out and "+source line" in out
    assert "only in source: new.md" in out and "only in home: local.md" in out
    assert "same.md" not in out and "notes.txt" not in out

    for name in ("drift.md", "new.md"):
        (source / name).unlink()
    (source / "local.md").write_text("local\n")
    (home / "agents" / "drift.md").unlink()
    clean = CliRunner().invoke(cli, ["sync-agents", "--source", str(source)])
    assert clean.exit_code == 0 and "no drift" in clean.output


def test_sync_agents_pairs_prefixed_copies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    agents = home / "agents"
    agents.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "lead.md").write_text("---\nname: lead\nmodel: opus\n---\nLead the work.\n")
    (agents / "dx-lead.md").write_text("---\nname: dx-lead\nmodel: opus\n---\nLead the work.\n")
    (source / "dev.md").write_text("---\nname: dev\nmodel: opus\n---\nWrite code.\n")
    (agents / "dx-dev.md").write_text("---\nname: dx-dev\nmodel: sonnet\n---\nWrite tests.\n")
    (source / "new.md").write_text("---\nname: new\n---\nNew.\n")
    (agents / "local.md").write_text("---\nname: local\n---\nLocal.\n")
    before = snapshot(tmp_path)
    args = ["sync-agents", "--source", str(source), "--prefix", "dx-"]
    result = CliRunner().invoke(cli, args)
    assert snapshot(tmp_path) == before
    assert result.exit_code == 1, result.output
    out = result.output
    assert "-model: sonnet" in out and "+model: opus" in out  # frontmatter diff
    assert "-Write tests." in out and "+Write code." in out  # body diff
    assert "-name:" not in out and "+name:" not in out  # the prefix alone is no difference
    assert "lead.md" not in out  # identical apart from the prefix
    assert "only in source: new.md" in out and "only in home: local.md" in out

    for name in ("dev.md", "new.md"):
        (source / name).unlink()
    (agents / "dx-dev.md").unlink()
    (agents / "local.md").unlink()
    clean = CliRunner().invoke(cli, args)
    assert clean.exit_code == 0 and "no drift" in clean.output, clean.output


# -- logs -------------------------------------------------------------------------------------
def sse(entries: list[dict[str, Any]], cursor: str | None, done: bool = False) -> bytes:
    out = ""
    for i, entry in enumerate(entries):
        head = f"id: {cursor}\n" if cursor and i == len(entries) - 1 else ""
        out += f"{head}data: {json.dumps(entry)}\n\n"
    if done:
        out += f"event: done\ndata: {json.dumps({'next_cursor': cursor})}\n\n"
    return out.encode()


class Dropping(httpx.SyncByteStream):
    """Yields ``body`` then fails as a dropped connection does."""

    def __init__(self, body: bytes) -> None:
        self.body = body

    def __iter__(self) -> Iterator[bytes]:
        yield self.body
        raise httpx.RemoteProtocolError("peer closed connection")


def test_logs_cli_follows_sse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, token = init_home(tmp_path, monkeypatch)
    monkeypatch.setattr(cli_main, "sleep", lambda s: None)
    one = {"source": "engine", "line": "first"}
    two = {"source": "attempt-1", "line": "second"}
    three = {"source": "attempt-1", "line": "third"}
    streams = [
        lambda: httpx.Response(200, stream=Dropping(sse([one, two], "c1") + b": keep-alive\n\n")),
        lambda: httpx.Response(503, json={"status": "unavailable"}),  # server restarting
        lambda: httpx.Response(200, content=sse([three], "c2")),  # clean end, no done: reconnect
        lambda: httpx.Response(200, content=sse([], "c2", done=True)),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        response: httpx.Response = streams.pop(0)()
        return response

    seen = mock_client(handler, monkeypatch)
    args = ["logs", "t-1", "--follow", "--url", "http://remote:9", "--token", "tk-explicit"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "engine | first", "attempt-1 | second", "attempt-1 | third",
    ]  # fmt: skip
    assert streams == []
    assert [r.url.path for r in seen] == ["/api/v1/tasks/t-1/stream"] * 4
    assert [r.url.params.get("cursor") for r in seen] == [None, "c1", "c1", "c2"]
    assert {r.url.host for r in seen} == {"remote"}
    assert {r.headers["Authorization"] for r in seen} == {"Bearer tk-explicit"}
    assert token not in result.output


def test_logs_follow_error_event_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_home(tmp_path, monkeypatch)
    body = b'event: error\ndata: {"error": "unknown source \'x\'"}\n\n'
    mock_client(lambda r: httpx.Response(200, content=body), monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--follow", "--source", "x"])
    assert result.exit_code == 1 and "unknown source" in result.output


def test_logs_follow_not_found_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_home(tmp_path, monkeypatch)
    mock_client(lambda r: httpx.Response(404, json={"error": "task t-1 not found"}), monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--follow"])
    assert result.exit_code == 1 and "not found" in result.output


def test_logs_pages_without_follow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_home(tmp_path, monkeypatch)
    pages = [
        {"entries": [{"source": "engine", "line": "a"}], "next_cursor": "p1", "done": False},
        {"entries": [{"source": "engine", "line": "b"}], "next_cursor": "p2", "done": False},
        {"entries": [], "next_cursor": "p2", "done": False},
    ]
    seen = mock_client(lambda r: httpx.Response(200, json=pages.pop(0)), monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--source", "engine"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == ["a", "b"]  # one source: lines only
    assert [r.url.params.get("cursor") for r in seen] == [None, "p1", "p2"]
    assert {r.url.params.get("source") for r in seen} == {"engine"}


def test_logs_follow_malformed_data_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    init_home(tmp_path, monkeypatch)
    body = b'data: {"source": "engine", "line": "ok"}\n\ndata: {not json\n\n'
    mock_client(lambda r: httpx.Response(200, content=body), monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--follow"])
    assert result.exit_code == 1, result.output
    assert result.stdout.splitlines() == ["engine | ok"]
    (line,) = result.stderr.splitlines()
    assert "malformed" in line and "Traceback" not in result.output


def test_logs_follow_reports_reconnects_and_gives_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    init_home(tmp_path, monkeypatch)
    delays: list[float] = []
    monkeypatch.setattr(cli_main, "sleep", delays.append)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    seen = mock_client(refuse, monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--follow"])
    limit = cli_main.MAX_RECONNECTS
    assert result.exit_code == 1, result.output
    assert len(seen) == limit + 1  # the first try, then MAX_RECONNECTS reconnects
    lines = result.stderr.splitlines()
    assert len(lines) == limit + 1 and "giving up" in lines[-1]
    assert all("reconnecting in" in line for line in lines[:-1])
    assert delays == sorted(delays) and delays[-1] == cli_main.RECONNECT_MAX_S
    assert delays[0] == cli_main.RECONNECT_S and len(delays) == limit


def test_logs_follow_failure_count_resets_on_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failures separated by successful reads never add up to the give-up limit."""
    init_home(tmp_path, monkeypatch)
    delays: list[float] = []
    monkeypatch.setattr(cli_main, "sleep", delays.append)
    limit = cli_main.MAX_RECONNECTS
    entry = {"source": "engine", "line": "x"}
    plan: list[Any] = []
    for n in range(limit + 2):  # more failures in total than the limit, never consecutive
        plan += ["refuse", sse([entry], f"c{n}")]
    plan.append(sse([], "end", done=True))

    def handler(request: httpx.Request) -> httpx.Response:
        step = plan.pop(0)
        if step == "refuse":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, content=step)

    mock_client(handler, monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--follow"])
    assert result.exit_code == 0, result.output
    assert plan == [] and len(result.stdout.splitlines()) == limit + 2
    # a read resets the count: no wait ever grows past the second step
    assert max(delays) <= 2 * cli_main.RECONNECT_S


# -- phase 6 fix wave: settings loader, remote token, install checks ------------------------
@pytest.mark.parametrize(
    "args",
    [["register"], ["status"], ["service", "install"], ["sync-agents"], ["logs", "t-1"]],
    ids=["register", "status", "service-install", "sync-agents", "logs"],
)
def test_config_error_is_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, args: list[str]
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("DIOXIDE_AUTH_TOKEN=placeholder-value-1234\nDIOXIDE_PORT=many\n")
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    monkeypatch.setattr(service, "PLATFORM", "darwin")
    if args == ["sync-agents"]:
        args = [*args, "--source", str(tmp_path)]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 1, result.output
    lines = result.output.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("Error: invalid configuration: "), lines
    assert "port" in lines[0]


def test_logs_url_never_sends_home_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, token = init_home(tmp_path, monkeypatch)
    page = {"entries": [], "next_cursor": "p", "done": True}
    seen = mock_client(lambda r: httpx.Response(200, json=page), monkeypatch)
    url = ["logs", "t-1", "--url", "https://remote.example:9"]
    missing = CliRunner().invoke(cli, url)
    assert missing.exit_code != 0 and seen == []
    assert "--token" in missing.output and "DIOXIDE_REMOTE_TOKEN" in missing.output
    assert token not in missing.output

    monkeypatch.setenv("DIOXIDE_REMOTE_TOKEN", "remote-from-env")
    assert CliRunner().invoke(cli, url).exit_code == 0
    assert CliRunner().invoke(cli, [*url, "--token", "remote-flag"]).exit_code == 0
    assert [r.headers["Authorization"] for r in seen] == [
        "Bearer remote-from-env", "Bearer remote-flag",
    ]  # fmt: skip

    monkeypatch.delenv("DIOXIDE_REMOTE_TOKEN")
    assert CliRunner().invoke(cli, ["logs", "t-1"]).exit_code == 0  # local: the home's token
    assert seen[-1].headers["Authorization"] == f"Bearer {token}"


@pytest.mark.parametrize(
    ("url", "warned"),
    [
        ("http://remote.example:9", True),
        ("http://203.0.113.9:9", True),
        ("https://remote.example:9", False),
        ("http://127.0.0.1:9", False),
        ("http://localhost:9", False),
        ("http://[::1]:9", False),
    ],
)
def test_logs_plain_http_remote_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, url: str, warned: bool
) -> None:
    page = {"entries": [], "next_cursor": "p", "done": True}
    mock_client(lambda r: httpx.Response(200, json=page), monkeypatch)
    result = CliRunner().invoke(cli, ["logs", "t-1", "--url", url, "--token", "tk"])
    assert result.exit_code == 0, result.output
    assert ("warning" in result.stderr and "http://" in result.stderr) is warned, result.stderr
    assert result.stdout == ""


def test_init_and_status_mask_settings_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home, file_token = init_home(tmp_path, monkeypatch)
    override = "envtoken-0123456789-wxyz"
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", override)
    init = CliRunner().invoke(cli, ["init"])
    assert init.exit_code == 0, init.output
    assert "envt…wxyz" in init.output and mask_of(file_token) not in init.output
    mock_client(lambda r: httpx.Response(200, json={"status": "ok"}), monkeypatch)
    status = CliRunner().invoke(cli, ["status"])
    assert status.exit_code == 0, status.output
    assert "envt…wxyz" in status.output and mask_of(file_token) not in status.output


def mask_of(token: str) -> str:
    return f"{token[:4]}…{token[-4:]}"


def test_init_next_hint_names_custom_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    user = tmp_path / "user"
    monkeypatch.setenv("HOME", str(user))
    monkeypatch.delenv("DIOXIDE_HOME")
    monkeypatch.chdir(tmp_path)
    custom = CliRunner().invoke(cli, ["init", "--home", "custom"])
    assert custom.exit_code == 0, custom.output
    (hint,) = [line for line in custom.output.splitlines() if line.startswith("next:")]
    assert f"DIOXIDE_HOME={tmp_path / 'custom'} dioxide register" in hint
    default = CliRunner().invoke(cli, ["init", "--home", str(user / ".code-dioxide")])
    assert default.exit_code == 0, default.output
    (hint,) = [line for line in default.output.splitlines() if line.startswith("next:")]
    assert "DIOXIDE_HOME" not in hint


def service_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path) -> Path:
    """HOME, SHELL and the platform set for `service install`; returns the user home."""
    user = tmp_path / "user"
    monkeypatch.setenv("HOME", str(user))
    monkeypatch.setenv("SHELL", str(fake_shell))
    monkeypatch.setattr(service, "PLATFORM", "darwin")
    return user


def test_service_install_requires_token_in_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / ".env").write_text("DIOXIDE_PORT=8600\n")
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    monkeypatch.setenv("DIOXIDE_AUTH_TOKEN", "shell-only-token-1234")
    service_env(tmp_path, monkeypatch, fake_shell)
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 1, result.output
    assert str(home / ".env") in result.output and "environment" in result.output
    assert "shell-only-token-1234" not in result.output and launchctl == []


def test_service_install_absolute_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DIOXIDE_HOME", "rel-home")
    assert CliRunner().invoke(cli, ["init"]).exit_code == 0
    user = service_env(tmp_path, monkeypatch, fake_shell)
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 0, result.output
    doc = plistlib.loads((user / "Library/LaunchAgents/dev.code-dioxide.plist").read_bytes())
    home = tmp_path / "rel-home"
    assert doc["EnvironmentVariables"]["DIOXIDE_HOME"] == str(home)
    assert doc["WorkingDirectory"] == str(home)
    assert doc["StandardErrorPath"] == str(home / "logs" / "launchd.err.log")


def test_service_install_reports_path_gaps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    monkeypatch.setenv("PATH", f"/only/here:/usr/bin:/given/too:{os.environ['PATH']}")
    args = ["service", "install", "--extra-path", str(fake_shell.parent)]
    result = CliRunner().invoke(cli, [*args, "--extra-path", "/given/too"])
    assert result.exit_code == 0, result.output
    (gap,) = [line for line in result.stderr.splitlines() if "/only/here" in line]
    assert "--extra-path" in gap and "/given/too" not in gap and "/usr/bin" not in gap


def test_service_install_waits_for_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
) -> None:
    _, token = init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    clock = FakeClock()
    monkeypatch.setattr(cli_main, "monotonic", clock)
    monkeypatch.setattr(cli_main, "sleep", clock.sleep)
    answers = ["refuse", 503, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        step = answers.pop(0)
        if step == "refuse":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(int(step), json={"status": "ok"})

    seen = mock_client(handler, monkeypatch)
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 0, result.output
    assert answers == [] and "running" in result.output
    assert {r.headers["Authorization"] for r in seen} == {f"Bearer {token}"}
    assert clock.sleeps == [service.HEALTH_POLL_S] * 2


def test_service_install_unhealthy_shows_err_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
) -> None:
    home, _ = init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    clock = FakeClock()
    monkeypatch.setattr(cli_main, "monotonic", clock)
    monkeypatch.setattr(cli_main, "sleep", clock.sleep)
    (home / "logs").mkdir(exist_ok=True)
    lines = [f"old line {n}" for n in range(50)] + ["Error: invalid configuration: boom"]
    (home / "logs" / "launchd.err.log").write_text("\n".join(lines) + "\n")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    mock_client(refuse, monkeypatch)
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 1, result.output
    assert "Error: invalid configuration: boom" in result.output
    assert "old line 0\n" not in result.output  # the tail only
    assert "launchd.err.log" in result.output and "Traceback" not in result.output
    assert clock.now <= service.HEALTH_WAIT_S + service.HEALTH_POLL_S


def test_service_uninstall_reports_launchctl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path
) -> None:
    user = service_env(tmp_path, monkeypatch, fake_shell)
    plist = service.plist_path(user, service.DEFAULT_LABEL)
    plist.parent.mkdir(parents=True)
    plist.write_text("<plist/>")

    def fake(argv: list[str]) -> tuple[int, str]:
        return (5, "Boot-out failed: 5: Input/output error\n") if argv[1] == "bootout" else (0, "")

    monkeypatch.setattr(service, "run_launchctl", fake)
    result = CliRunner().invoke(cli, ["service", "uninstall"])
    assert result.exit_code == 1, result.output
    assert "Boot-out failed: 5: Input/output error" in result.output
    assert plist.exists()  # still loaded: its plist stays


def test_service_uninstall_not_loaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_shell: Path, launchctl: list[list[str]]
) -> None:
    service_env(tmp_path, monkeypatch, fake_shell)
    result = CliRunner().invoke(cli, ["service", "uninstall"])
    assert result.exit_code == 0, result.output
    assert "not loaded" in result.output
    assert [c[1] for c in launchctl] == ["print"]


def test_service_uninstall_counts_unsupervised_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    home, _ = init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    install = ["service", "install", "--extra-path", str(fake_shell.parent)]
    assert CliRunner().invoke(cli, install).exit_code == 0
    db = home / "data" / "dioxide.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(SCHEMA)
        for task_id, state in (("a", "running"), ("b", "running"), ("c", "completed")):
            conn.execute(
                "INSERT INTO tasks (id, status, created_at, updated_at) VALUES (?, ?, 0, 0)",
                (task_id, state),
            )
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path / "elsewhere"))  # the plist names the home
    result = CliRunner().invoke(cli, ["service", "uninstall"])
    assert result.exit_code == 0, result.output
    assert "2 tasks still running without supervision" in result.output


def test_env_names_exist_in_settings() -> None:
    repo = Path(__file__).resolve().parent.parent
    texts = [cli_main.ENV_STARTER, (repo / ".env.example").read_text()]
    names = {m for text in texts for m in re.findall(r"\bDIOXIDE_([A-Z_]+)\b", text)}
    assert names and {n.lower() for n in names} <= set(Settings.model_fields), names


def test_local_client_ignores_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example:3128")
    with cli_main.http_client("http://127.0.0.1:8500", "tk", local=True) as client:
        assert client.trust_env is False  # a proxy from the environment never sees the token
    with cli_main.http_client("https://remote.example", "tk", local=False) as client:
        assert client.trust_env is True


def test_commands_pick_local_or_remote_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    page = {"entries": [], "next_cursor": "p", "done": True}
    install = ["service", "install", "--extra-path", str(fake_shell.parent)]
    assert CliRunner().invoke(cli, install).exit_code == 0  # the probe: healthy[0]
    seen = mock_client(lambda r: httpx.Response(200, json={**page, "status": "ok"}), monkeypatch)
    for args in (
        ["status"],
        ["logs", "t-1"],
        ["logs", "t-1", "--url", "https://r", "--token", "x"],
    ):
        assert CliRunner().invoke(cli, args).exit_code == 0
    kinds = [r.headers["X-Test-Local"] for r in [healthy[0], *seen]]
    assert kinds == ["True", "True", "True", "False"]


def test_service_install_warns_env_only_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
    healthy: list[httpx.Request],
) -> None:
    home, _ = init_home(tmp_path, monkeypatch)
    with (home / ".env").open("a") as fh:
        fh.write("DIOXIDE_PORT=8611\nDIOXIDE_MAX_RESUMES=5\n")
    service_env(tmp_path, monkeypatch, fake_shell)
    monkeypatch.setenv("DIOXIDE_PORT", "8700")  # the service reads 8611 from the .env
    monkeypatch.setenv("DIOXIDE_HOST", "127.0.0.2")
    monkeypatch.setenv("DIOXIDE_PARENT_MODEL", "opus")
    monkeypatch.setenv("DIOXIDE_MAX_RESUMES", "1")
    monkeypatch.setenv("DIOXIDE_REMOTE_TOKEN", "not-a-service-setting")
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 0, result.output
    (line,) = [x for x in result.stderr.splitlines() if "not in" in x and ".env" in x]
    assert "DIOXIDE_HOST" in line and "DIOXIDE_PARENT_MODEL" in line
    for absent in ("DIOXIDE_PORT", "DIOXIDE_MAX_RESUMES", "DIOXIDE_HOME", "DIOXIDE_REMOTE_TOKEN"):
        assert absent not in line, absent
    assert "opus" not in line  # names only, never values
    assert {(r.url.host, r.url.port) for r in healthy} == {("127.0.0.1", 8611)}


def test_service_install_unhealthy_says_service_stays_loaded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_shell: Path,
    launchctl: list[list[str]],
) -> None:
    init_home(tmp_path, monkeypatch)
    service_env(tmp_path, monkeypatch, fake_shell)
    clock = FakeClock()
    monkeypatch.setattr(cli_main, "monotonic", clock)
    monkeypatch.setattr(cli_main, "sleep", clock.sleep)
    mock_client(lambda r: httpx.Response(503), monkeypatch)
    result = CliRunner().invoke(cli, ["service", "install", "--extra-path", str(fake_shell.parent)])
    assert result.exit_code == 1, result.output
    assert "stays loaded" in result.stderr and "every 30 s" in result.stderr
    assert "dioxide service uninstall" in result.stderr
