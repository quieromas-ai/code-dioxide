import json
import logging
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from src import serverlog
from src.config import AlreadyRunning, Settings, acquire_home_lock
from src.redact import Redactor


def test_defaults_match_spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    s = Settings()
    assert s.home == tmp_path
    assert (s.host, s.port) == ("127.0.0.1", 8500)
    assert s.auth_token == ""
    assert s.auth_disabled is False
    assert s.trust_loopback is False
    assert s.claude_bin == "claude"
    assert s.parent_model is None
    assert s.parent_effort is None
    assert (s.retry_backoff_s, s.retry_backoff_max_s) == (30.0, 600.0)
    assert s.rate_limit_min_wait_s == 30.0
    assert s.claude_home == Path.home() / ".claude"
    assert s.default_project_dir is None
    assert s.extra_path == []
    assert (s.max_concurrent_tasks, s.task_queue_size) == (3, 20)
    assert s.dedupe_window_s == 900
    assert s.stall_timeout_s == 2700
    assert s.task_timeout_s == 21600
    assert s.watchdog_interval_s == 30.0
    assert s.max_resumes == 3
    assert s.rate_limit_default_wait_s == 900
    assert s.rate_limit_max_wait_s == 86400
    assert s.run_retention_days == 90
    assert (s.review_rounds, s.qa_rounds) == (3, 3)
    assert s.merge_strategy == "squash"
    assert s.mcp_exclude == ["dioxide"]
    assert s.log_page_chars == 40000
    assert s.log_full_page_bytes == 5242880
    assert s.call_mismatch_retries == 2
    assert s.redact_env_files == []
    assert s.extra_settings == {}
    assert s.model_fallback == {}
    assert s.mcp_config_path is None
    for removed in ("slack_bot_token", "slack_app_token", "slack_channel", "slack_allowed_users"):
        assert removed not in Settings.model_fields
    assert s.agents_dir == tmp_path / "agents"
    assert s.data_dir == tmp_path / "data"
    assert s.db_path == tmp_path / "data" / "dioxide.db"
    assert s.roles_path == tmp_path / "roles.yaml"
    assert s.runs_dir == tmp_path / "data" / "runs"
    assert s.worktrees_dir == tmp_path / "worktrees"
    assert s.server_log_path == tmp_path / "data" / "server.log"
    assert s.lock_path == tmp_path / "data" / ".lock"


def test_default_home_is_under_user_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DIOXIDE_HOME")
    assert Settings().home == Path.home() / ".code-dioxide"


def test_home_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "custom"
    home.mkdir()
    (home / ".env").write_text("DIOXIDE_PORT=9123\nDIOXIDE_CLAUDE_BIN=from-dotenv\n")
    (home / "mcp.json").write_text("{}")
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    monkeypatch.setenv("DIOXIDE_CLAUDE_BIN", "from-env")
    s = Settings()
    assert s.home == home
    assert s.db_path == home / "data" / "dioxide.db"
    assert s.port == 9123  # .env is read from the resolved home
    assert s.claude_bin == "from-env"  # real environment wins over .env
    assert s.mcp_config_path == home / "mcp.json"


def test_token_required_unless_disabled() -> None:
    with pytest.raises(ValueError, match="auth_token"):
        Settings().validate_security()
    Settings(auth_token="secret").validate_security()
    Settings(auth_disabled=True).validate_security()


def test_home_lock_exclusive(tmp_path: Path) -> None:
    path = tmp_path / "data" / ".lock"
    first = acquire_home_lock(path)
    with pytest.raises(AlreadyRunning):
        acquire_home_lock(path)
    first.release()
    acquire_home_lock(path).release()


@pytest.fixture
def server_logger(tmp_path: Path) -> Iterator[logging.Logger]:
    logger = serverlog.setup(tmp_path / "data" / "server.log")
    yield logger
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def test_serverlog_writes_json_lines(tmp_path: Path, server_logger: logging.Logger) -> None:
    path = tmp_path / "data" / "server.log"
    server_logger.info("boot", extra={"port": 8500})
    for handler in server_logger.handlers:
        handler.flush()
    record = json.loads(path.read_text().splitlines()[-1])
    assert record["msg"] == "boot"
    assert record["level"] == "INFO"
    assert record["port"] == 8500
    assert "ts" in record


@pytest.mark.parametrize("line", ["DIOXIDE_HOME=/elsewhere", "export dioxide_home='/elsewhere'"])
def test_home_in_home_env_is_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, line: str
) -> None:
    (tmp_path / ".env").write_text(f"DIOXIDE_PORT=9123\n{line}\n")
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    with pytest.raises(ValueError, match="DIOXIDE_HOME") as caught:
        Settings()
    assert str(tmp_path / ".env") in str(caught.value)


def test_home_never_read_from_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default home's .env cannot move the home (data in one dir, .env from another)."""
    monkeypatch.delenv("DIOXIDE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    default_home = tmp_path / ".code-dioxide"
    default_home.mkdir()
    (default_home / ".env").write_text(f"DIOXIDE_HOME={tmp_path / 'other'}\n")
    with pytest.raises(ValueError, match="DIOXIDE_HOME"):
        Settings()
    (default_home / ".env").write_text("DIOXIDE_PORT=9123\n")
    s = Settings()
    assert (s.home, s.port) == (default_home, 9123)


def test_env_example_is_safe_to_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    example = Path(__file__).parent.parent / ".env.example"
    home = tmp_path / "home"
    home.mkdir()
    shutil.copy(example, home / ".env")
    monkeypatch.setenv("DIOXIDE_HOME", str(home))
    copied = Settings()
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path / "empty"))
    assert copied.model_dump(exclude={"home"}) == Settings().model_dump(exclude={"home"})
    assert copied.home == home
    # empty DIOXIDE_HOME (e.g. blank line in a .env) falls back to the default home
    monkeypatch.setenv("DIOXIDE_HOME", "")
    assert Settings().home == Path.home() / ".code-dioxide"


def test_tilde_expanded_in_path_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    s = Settings(
        redact_env_files=[Path("~/x.env"), Path("~/y.env")],
        default_project_dir=Path("~/proj"),
        mcp_config_path=Path("~/mcp.json"),
    )
    assert s.redact_env_files == [Path.home() / "x.env", Path.home() / "y.env"]
    assert s.default_project_dir == Path.home() / "proj"
    assert s.mcp_config_path == Path.home() / "mcp.json"
    monkeypatch.setenv("DIOXIDE_REDACT_ENV_FILES", '["~/z.env"]')
    assert Settings().redact_env_files == [Path.home() / "z.env"]


def test_tilde_redact_file_masks_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "x.env").write_text("DB=supersecretvalue123\n")
    redactor = Redactor.from_settings(
        Settings(home=tmp_path / "h", redact_env_files=[Path("~/x.env")])
    )
    assert redactor.redact_line("conn supersecretvalue123 end") == "conn **** end"


def test_unreadable_redact_file_warns_with_path_only(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    missing = tmp_path / "gone.env"
    with caplog.at_level(logging.WARNING):
        Redactor.from_settings(Settings(home=tmp_path / "h", redact_env_files=[missing]))
    warnings = [r for r in caplog.records if r.name == "dioxide" and r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert str(missing) in warnings[0].getMessage()
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        Redactor.from_settings(Settings(home=tmp_path / "h"))
    assert caplog.records == []


def test_secrets_not_in_repr() -> None:
    s = Settings(auth_token="tok-aaa111")
    text = repr(s) + str(s)
    assert "tok-aaa111" not in text
    assert s.auth_token == "tok-aaa111"


def test_merge_strategy_validated() -> None:
    for ok in ("squash", "pr"):
        assert Settings(merge_strategy=ok).merge_strategy == ok
    for bad in ("rebase", "merge"):
        with pytest.raises(ValidationError, match="merge_strategy must be one of squash, pr"):
            Settings(merge_strategy=bad)


@pytest.mark.parametrize(
    "name",
    [
        "dedupe_window_s",
        "stall_timeout_s",
        "task_timeout_s",
        "max_resumes",
        "rate_limit_default_wait_s",
        "rate_limit_max_wait_s",
        "run_retention_days",
        "review_rounds",
        "qa_rounds",
        "log_page_chars",
        "log_full_page_bytes",
        "call_mismatch_retries",
        "max_concurrent_tasks",
        "task_queue_size",
        "port",
    ],
)
def test_negative_limit_rejected(name: str) -> None:
    with pytest.raises(ValidationError):
        Settings(**{name: -1})


@pytest.mark.parametrize(
    "name",
    [
        "port",
        "max_concurrent_tasks",
        "log_page_chars",
        "log_full_page_bytes",
        "stall_timeout_s",
        "task_timeout_s",
        "watchdog_interval_s",
        "review_rounds",
        "qa_rounds",
    ],
)
def test_zero_rejected_where_meaningless(name: str) -> None:
    with pytest.raises(ValidationError):
        Settings(**{name: 0})


def test_zero_accepted_where_meaningful() -> None:
    assert Settings(dedupe_window_s=0).dedupe_window_s == 0
    assert Settings(max_resumes=0, task_queue_size=0).qa_rounds == 3


def test_blank_non_home_value_in_home_env_keeps_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DIOXIDE_CLAUDE_HOME=\nDIOXIDE_PORT=\n")
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    s = Settings()
    assert s.claude_home == Path.home() / ".claude"
    assert s.port == 8500


def test_sweep_exempt_parsed_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    assert Settings().sweep_exempt == []  # the repo ships no default names
    (tmp_path / ".env").write_text("DIOXIDE_SWEEP_EXEMPT=fake-daemon\n")
    assert Settings().sweep_exempt == ["fake-daemon"]
    monkeypatch.setenv("DIOXIDE_SWEEP_EXEMPT", " fake-daemon, other-helper,,Fake-Daemon ")
    assert Settings().sweep_exempt == ["fake-daemon", "other-helper", "Fake-Daemon"]


@pytest.mark.parametrize("name", ["claude", "sh", "bash", "zsh", "dash", "fish", "git"])
def test_sweep_exempt_rejects_names_that_defeat_the_sweep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv("DIOXIDE_HOME", str(tmp_path))
    monkeypatch.setenv("DIOXIDE_SWEEP_EXEMPT", f"fake-daemon,{name}")
    with pytest.raises(ValidationError, match=f"sweep_exempt must not name {name!r}"):
        Settings()
    with pytest.raises(ValidationError, match="never exempt"):
        Settings(home=tmp_path, sweep_exempt=[name])
    assert Settings(home=tmp_path, sweep_exempt=["fake-daemon"]).sweep_exempt == ["fake-daemon"]


@pytest.mark.parametrize("name", ["-zsh", "-bash", "-sh", "-claude"])
def test_sweep_exempt_rejects_login_shell_forms(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValidationError, match="never exempt"):
        Settings(home=tmp_path, sweep_exempt=[name])
    assert Settings(home=tmp_path, sweep_exempt=["-fake-daemon"]).sweep_exempt == ["-fake-daemon"]


@pytest.mark.parametrize("level", ["low", "medium", "high", "xhigh", "max"])
def test_parent_effort_accepts_cli_levels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, level: str
) -> None:
    monkeypatch.setenv("DIOXIDE_PARENT_EFFORT", level)
    assert Settings(home=tmp_path).parent_effort == level


@pytest.mark.parametrize("level", ["LOW", "minimal", "ultra", ""])
def test_parent_effort_rejects_unknown_levels(tmp_path: Path, level: str) -> None:
    with pytest.raises(ValidationError, match="parent_effort must be one of"):
        Settings(home=tmp_path, parent_effort=level)


def test_model_fallback_parsed_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_MODEL_FALLBACK", '{"fable": "opus", "opus": "sonnet"}')
    assert Settings(home=tmp_path).model_fallback == {"fable": "opus", "opus": "sonnet"}


@pytest.mark.parametrize("raw", ['{"fable": ', '["opus"]', '{"fable": 1}'])
def test_model_fallback_rejects_malformed_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("DIOXIDE_MODEL_FALLBACK", raw)
    with pytest.raises(ValueError, match="model_fallback"):
        Settings(home=tmp_path)


@pytest.mark.parametrize("mapping", [{"": "opus"}, {"fable": " "}])
def test_model_fallback_rejects_empty_names(tmp_path: Path, mapping: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="model_fallback must map non-empty model names"):
        Settings(home=tmp_path, model_fallback=mapping)


def test_report_tokens_must_differ(tmp_path: Path) -> None:
    for reports, content in (("same", "same"), ("adm", ""), ("", "adm")):
        settings = Settings(
            home=tmp_path, auth_token="adm", reports_token=reports, content_token=content
        )
        with pytest.raises(ValueError, match="distinct"):
            settings.validate_security()
    Settings(
        home=tmp_path, auth_token="adm", reports_token="rep", content_token="con"
    ).validate_security()
    Settings(home=tmp_path, auth_token="adm").validate_security()


def test_report_tokens_read_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DIOXIDE_REPORTS_TOKEN", "rep-env-1")
    monkeypatch.setenv("DIOXIDE_CONTENT_TOKEN", "con-env-1")
    settings = Settings(home=tmp_path, auth_token="adm")
    assert (settings.reports_token, settings.content_token) == ("rep-env-1", "con-env-1")
    assert "rep-env-1" not in repr(settings) and "con-env-1" not in repr(settings)


def test_cost_ratios_validated(tmp_path: Path) -> None:
    ok = Settings(home=tmp_path, cost_ratios={"claude-opus-5-5": {"cache_read": 0.05}})
    assert ok.cost_ratios == {"claude-opus-5-5": {"cache_read": 0.05}}
    for bad, match in (
        ({"claude-opus": {"cache_reads": 0.05}}, "unknown key"),
        ({"claude-opus": {"output": 0}}, "finite number > 0"),
        ({"claude-opus": {"output": float("inf")}}, "finite number > 0"),
        ({" ": {"output": 5}}, "non-empty"),
    ):
        with pytest.raises(ValueError, match=match):
            Settings(home=tmp_path, cost_ratios=bad)
