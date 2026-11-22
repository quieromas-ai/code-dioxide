import fcntl
import os
from pathlib import Path
from typing import Annotated, Any, Literal, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    NoDecode,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

ENV_PREFIX = "DIOXIDE_"
DEFAULT_HOME = Path("~/.code-dioxide")
CHILD_ENV_NAME = "child.env"  # credentials for the sessions dioxide starts (src.childenv)

MERGE_STRATEGIES = ("squash", "pr")
# The levels `claude --effort <level>` accepts (Claude Code 2.1.x `--help`).
EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")
# sweep_exempt names that would defeat the sweep: the task's claude, the shells the Bash tool
# runs commands in, and git (a stage's side effects).
SHELLS = ("sh", "bash", "zsh", "dash", "fish")
NEVER_EXEMPT = frozenset({"claude", *SHELLS, "git"})
NonNegInt = Annotated[int, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
NonNegFloat = Annotated[float, Field(ge=0)]
PositiveFloat = Annotated[float, Field(gt=0)]


class AlreadyRunning(RuntimeError):
    """Another engine already holds the lock for this home."""


class FileLock:
    """Non-blocking exclusive flock held on an open file descriptor."""

    def __init__(self, path: Path, fd: int) -> None:
        self.path = path
        self._fd: int | None = fd

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


def acquire_home_lock(path: Path) -> FileLock:
    """Take an exclusive non-blocking flock on `path`; raise AlreadyRunning if held."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise AlreadyRunning(f"another dioxide engine holds {path}") from None
    return FileLock(path, fd)


def resolve_home(home: str | Path | None = None) -> Path:
    """The home dir: ``home`` if given, else ``$DIOXIDE_HOME``, else ``~/.code-dioxide``."""
    return _resolve_home({"home": home})


def _resolve_home(init_kwargs: dict[str, Any]) -> Path:
    candidates = (init_kwargs.get("home"), os.environ.get(f"{ENV_PREFIX}HOME"))
    for raw in candidates:
        if raw is not None and str(raw).strip():
            return Path(raw).expanduser()
    return DEFAULT_HOME.expanduser()


class _HomeDotEnv(DotEnvSettingsSource):
    """The home's ``.env``. It never sets the home: the file is found through the home, so a
    ``DIOXIDE_HOME`` line in it would split the home (data in one dir, ``.env`` from another)
    and is an error naming the file."""

    def __init__(self, settings_cls: type[BaseSettings], env_file: Path) -> None:
        super().__init__(settings_cls, env_file=env_file)
        if f"{ENV_PREFIX}HOME".lower() in self.env_vars:
            raise ValueError(
                f"{env_file}: DIOXIDE_HOME must not be set in the home's .env (the .env is "
                "found through the home); set it in the process environment instead"
            )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX, extra="ignore", env_ignore_empty=True)

    home: Path = DEFAULT_HOME

    host: str = "127.0.0.1"
    port: PositiveInt = 8500
    auth_token: str = Field(default="", repr=False)
    auth_disabled: bool = False
    trust_loopback: bool = False
    allowed_hosts: list[str] = Field(default_factory=list)

    claude_bin: str = "claude"
    parent_model: str | None = None
    # `--effort` for every lifecycle parent (orchestrator) session; unset: no flag.
    parent_effort: str | None = None
    # Model of the role-less script stages (setup, merge) of a lifecycle run; unset: the parent
    # session's model. Frozen into the run's args at its first launch.
    script_model: str | None = None
    claude_home: Path = Path("~/.claude")
    default_project_dir: Path | None = None
    extra_path: list[str] = Field(default_factory=list)

    max_concurrent_tasks: PositiveInt = 3
    task_queue_size: NonNegInt = 20

    dedupe_window_s: NonNegInt = 900
    stall_timeout_s: PositiveInt = 2700
    task_timeout_s: PositiveInt = 21600
    # How often the watchdog checks running attempts for a stall or the task timeout.
    watchdog_interval_s: PositiveFloat = 30.0
    # A parent still alive this long after its run reported completion with a valid return is
    # killed by the watchdog (the return then decides the task).
    exit_after_completion_s: PositiveFloat = 120.0
    max_resumes: NonNegInt = 3
    rate_limit_default_wait_s: NonNegInt = 900
    rate_limit_max_wait_s: NonNegInt = 86400
    # Every rate-limit wait lasts at least this long (a past or missing reset never relaunches
    # at once); each wait is charged to the task's rate_limit_waited_s.
    rate_limit_min_wait_s: PositiveFloat = 30.0
    # User policy for a rate-limited task: agent models to swap, e.g. {"fable": "opus"} (JSON in
    # the env). Applied once per task to the agents file of every later attempt; {} is off.
    model_fallback: dict[str, str] = Field(default_factory=dict)
    run_retention_days: NonNegInt = 90
    review_rounds: PositiveInt = 3
    qa_rounds: PositiveInt = 3
    merge_strategy: Literal["squash", "pr"] = "squash"
    log_page_chars: PositiveInt = 40000
    log_full_page_bytes: PositiveInt = 5242880
    call_mismatch_retries: NonNegInt = 2
    # Backoff before a transient resume: min(retry_backoff_s * 2^resumes, retry_backoff_max_s).
    retry_backoff_s: NonNegFloat = 30.0
    retry_backoff_max_s: NonNegFloat = 600.0
    # Command names (argv[0] basename, exact, case-sensitive) never killed by a sweep or kill;
    # comma-separated in the env. Not a security boundary: a process can rename its argv[0].
    # An entry matches every process of that name, a task's in-flight workers included (only
    # the task's own claude is never exempt).
    sweep_exempt: Annotated[list[str], NoDecode] = Field(default_factory=list)

    redact_env_files: list[Path] = Field(default_factory=list)
    # Variables added to every session's environment (credentials: CLAUDE_CODE_OAUTH_TOKEN or
    # ANTHROPIC_API_KEY), read at each launch; default <home>/child.env when it exists at start.
    child_env_file: Path | None = None
    extra_settings: dict[str, Any] = Field(default_factory=dict)
    mcp_config_path: Path | None = None
    mcp_exclude: list[str] = Field(default_factory=lambda: ["dioxide"])

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        init_kwargs: dict[str, Any] = getattr(init_settings, "init_kwargs", {})
        env_file = _resolve_home(init_kwargs) / ".env"
        return (
            init_settings,
            env_settings,
            _HomeDotEnv(settings_cls, env_file),
            file_secret_settings,
        )

    @field_validator("sweep_exempt", mode="before")
    @classmethod
    def _split_commas(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [name.strip() for name in value.split(",") if name.strip()]
        return value

    @field_validator("sweep_exempt", mode="after")
    @classmethod
    def _no_defeating_exemption(cls, value: list[str]) -> list[str]:
        for name in value:
            # A login shell's argv[0] carries a leading "-" (e.g. "-zsh").
            if name.removeprefix("-") in NEVER_EXEMPT:
                raise ValueError(
                    f"sweep_exempt must not name {name!r}: claude, shells ({', '.join(SHELLS)}) "
                    "and git are never exempt (exempting them would let a killed attempt's work "
                    "survive the sweep)"
                )
        return value

    @field_validator("parent_effort", mode="after")
    @classmethod
    def _known_effort(cls, value: str | None) -> str | None:
        if value is not None and value not in EFFORT_LEVELS:
            raise ValueError(
                f"parent_effort must be one of {', '.join(EFFORT_LEVELS)} (got {value!r}); "
                "these are the levels `claude --effort` accepts"
            )
        return value

    @field_validator("script_model", mode="after")
    @classmethod
    def _non_empty_model(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("script_model must be a non-empty model name when set")
        return value

    @field_validator("model_fallback", mode="after")
    @classmethod
    def _non_empty_fallback(cls, value: dict[str, str]) -> dict[str, str]:
        for model, replacement in value.items():
            if not model.strip() or not replacement.strip():
                raise ValueError(
                    "model_fallback must map non-empty model names to non-empty model names "
                    f"(got {model!r}: {replacement!r})"
                )
        return value

    @field_validator("merge_strategy", mode="before")
    @classmethod
    def _known_merge_strategy(cls, value: Any) -> Any:
        if value not in MERGE_STRATEGIES:
            raise ValueError(
                f"merge_strategy must be one of {', '.join(MERGE_STRATEGIES)} (got {value!r}); "
                "the merge stage script supports no other strategy"
            )
        return value

    @field_validator("home", "claude_home", mode="after")
    @classmethod
    def _expand_user(cls, value: Path) -> Path:
        return value.expanduser()

    @field_validator("default_project_dir", "mcp_config_path", "child_env_file", mode="after")
    @classmethod
    def _expand_optional_user(cls, value: Path | None) -> Path | None:
        return value.expanduser() if value is not None else None

    @field_validator("redact_env_files", mode="after")
    @classmethod
    def _expand_user_list(cls, value: list[Path]) -> list[Path]:
        return [path.expanduser() for path in value]

    @model_validator(mode="after")
    def _default_mcp_config(self) -> Self:
        if self.mcp_config_path is None:
            candidate = self.home / "mcp.json"
            if candidate.is_file():
                self.mcp_config_path = candidate
        return self

    @model_validator(mode="after")
    def _default_child_env(self) -> Self:
        # lexists: anything at the path (a symlink, a directory) is taken and then refused by
        # the checks at startup, never silently skipped.
        if self.child_env_file is None:
            candidate = self.home / CHILD_ENV_NAME
            if os.path.lexists(candidate):
                self.child_env_file = candidate
        return self

    @property
    def agents_dir(self) -> Path:
        return self.home / "agents"

    @property
    def data_dir(self) -> Path:
        return self.home / "data"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "dioxide.db"

    @property
    def roles_path(self) -> Path:
        return self.home / "roles.yaml"

    @property
    def runs_dir(self) -> Path:
        return self.data_dir / "runs"

    @property
    def worktrees_dir(self) -> Path:
        return self.home / "worktrees"

    @property
    def server_log_path(self) -> Path:
        return self.data_dir / "server.log"

    @property
    def cursor_key_path(self) -> Path:
        return self.data_dir / "cursor.key"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / ".lock"

    def validate_security(self) -> None:
        if self.auth_token == "" and not self.auth_disabled:
            raise ValueError("auth_token is required unless auth_disabled is true")
