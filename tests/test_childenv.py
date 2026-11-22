"""child.env: credentials for the sessions dioxide starts (read at every launch, redacted)."""

import hashlib
import json
import logging
import os
import tarfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from src import bundle, childenv, debug, serverlog
from src.childenv import ChildEnvError
from src.config import Settings
from src.launcher import BG_WAIT_VAR, TASK_MARKER, launch_env
from tests.helpers import Env, rows, stub_log, submit, wait_status
from tests.test_debug import client, router_app
from tests.test_recovery import event_data, table_task

# Obvious placeholders: no real credential has this shape.
TOKEN = "placeholder-oauth-value-0001"
ROTATED = "placeholder-oauth-value-0002"
API_KEY = "placeholder-api-key-value-01"
OAUTH_VAR = "CLAUDE_CODE_OAUTH_TOKEN"
API_VAR = "ANTHROPIC_API_KEY"
# A value whose JSON-escaped form differs from the bare one (non-ASCII), printed without any
# key name next to it: only its registration in the redactor can mask it.
LEAKED = "placeholder-oauth-v\u00e4lue-0003"
LEAKED_FORMS = (LEAKED, json.dumps(LEAKED)[1:-1], json.dumps(LEAKED, ensure_ascii=False)[1:-1])


def write_child_env(path: Path, text: str, mode: int = 0o600) -> Path:
    path.write_text(text)
    path.chmod(mode)
    return path


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def reported(env: Env) -> list[dict[str, str | None]]:
    return [e["reported_env"] for e in stub_log(env.log) if "reported_env" in e]


@pytest.fixture
def child_env(env: Env, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = write_child_env(env.settings.home / "child.env", f"{OAUTH_VAR}={TOKEN}\n")
    env.settings.child_env_file = path
    monkeypatch.setenv("STUB_CLAUDE_REPORT_ENV", f"{OAUTH_VAR},{API_VAR}")
    return path


@pytest.fixture
def server_log(env: Env) -> Iterator[Path]:
    path = env.settings.server_log_path
    serverlog.setup(path)
    yield path
    logger = logging.getLogger(serverlog.LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


# -- the file ------------------------------------------------------------------------------------
def test_child_env_parsed_like_dotenv(tmp_path: Path) -> None:
    path = write_child_env(
        tmp_path / "child.env",
        f"# credentials\nexport {OAUTH_VAR}='{TOKEN}'\n{API_VAR}={API_KEY} # billing\n",
    )
    assert childenv.read(path) == {OAUTH_VAR: TOKEN, API_VAR: API_KEY}
    assert childenv.secret_values(path) == [TOKEN, API_KEY]
    assert childenv.secret_values(None) == []


@pytest.mark.parametrize(
    "name", ["DIOXIDE_AUTH_TOKEN", "dioxide_home", "DIOXIDE_TASK_ID", "HOME", "CLAUDE_CONFIG_DIR"]
)
def test_child_env_refused_names(tmp_path: Path, name: str) -> None:
    path = write_child_env(tmp_path / "child.env", f"{OAUTH_VAR}={TOKEN}\n{name}={API_KEY}\n")
    with pytest.raises(ChildEnvError, match=name) as raised:
        childenv.read(path)
    assert TOKEN not in str(raised.value) and API_KEY not in str(raised.value)


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o660, 0o644, 0o700 | 0o001])
def test_child_env_loose_mode_refused(tmp_path: Path, mode: int) -> None:
    path = write_child_env(tmp_path / "child.env", f"{OAUTH_VAR}={TOKEN}\n", mode)
    with pytest.raises(ChildEnvError, match="no group/other permission bits") as raised:
        childenv.read(path)
    assert f"chmod 600 {path}" in str(raised.value) and TOKEN not in str(raised.value)


def test_child_env_must_be_a_private_regular_file(tmp_path: Path) -> None:
    target = write_child_env(tmp_path / "real.env", f"{OAUTH_VAR}={TOKEN}\n")
    link = tmp_path / "child.env"
    link.symlink_to(target)
    with pytest.raises(ChildEnvError, match="regular file"):
        childenv.read(link)
    (tmp_path / "dir.env").mkdir(mode=0o700)
    with pytest.raises(ChildEnvError, match="regular file"):
        childenv.read(tmp_path / "dir.env")
    with pytest.raises(ChildEnvError, match="does not exist"):
        childenv.read(tmp_path / "missing.env")
    bare = write_child_env(tmp_path / "bare.env", f"{OAUTH_VAR}\n")
    with pytest.raises(ChildEnvError, match=f"{OAUTH_VAR} has no value"):
        childenv.read(bare)
    assert childenv.secret_values(link) == []


def test_child_env_file_setting_defaults_to_home_file_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    assert Settings(home=home).child_env_file is None
    write_child_env(home / "child.env", "")
    assert Settings(home=home).child_env_file == home / "child.env"
    other = write_child_env(tmp_path / "elsewhere.env", "")
    monkeypatch.setenv("DIOXIDE_CHILD_ENV_FILE", str(other))
    assert Settings(home=home).child_env_file == other


def test_child_env_added_after_the_dioxide_strip(tmp_path: Path) -> None:
    settings = Settings(home=tmp_path, extra_path=["/opt/x/bin"])
    base = {"PATH": "/usr/bin", "DIOXIDE_AUTH_TOKEN": "t" * 24, "HOME": "/h"}
    child = {OAUTH_VAR: TOKEN, BG_WAIT_VAR: "5", "PATH": "/child/bin"}
    env = launch_env(settings, table_task(id="t1"), base, child)
    assert env[OAUTH_VAR] == TOKEN and "DIOXIDE_AUTH_TOKEN" not in env and env["HOME"] == "/h"
    assert env[BG_WAIT_VAR] == "0" and env[TASK_MARKER] == "t1"  # dioxide's own always win
    assert env["PATH"] == os.pathsep.join(["/opt/x/bin", "/child/bin"])


# -- launches ------------------------------------------------------------------------------------
async def test_child_env_variables_reach_the_session(env: Env, child_env: Path) -> None:
    write_child_env(child_env, f"{OAUTH_VAR}={TOKEN}\n{API_VAR}={API_KEY}\n")
    task_id = (await submit(env))["task_id"]
    await env.engine.start()
    assert (await wait_status(env.engine, task_id, "completed", "failed"))["status"] == "completed"
    assert reported(env) == [{OAUTH_VAR: digest(TOKEN), API_VAR: digest(API_KEY)}]
    (spawned,) = await event_data(task_id, "spawned")
    assert spawned["child_env"] == [API_VAR, OAUTH_VAR]
    view = await debug.task_debug(env.engine, task_id)
    assert view["launch"]["child_env"] == [API_VAR, OAUTH_VAR]


async def test_child_env_rotation_picked_up_without_restart(
    env: Env, child_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    await env.engine.start()
    first = (await submit(env, "first"))["task_id"]
    await wait_status(env.engine, first, "completed")
    write_child_env(child_env, f"{OAUTH_VAR}={ROTATED}\n")
    monkeypatch.setenv("STUB_CLAUDE_EXTRA", f"the session printed {ROTATED}")
    second = (await submit(env, "second"))["task_id"]
    await wait_status(env.engine, second, "completed")
    assert reported(env) == [{OAUTH_VAR: digest(TOKEN), API_VAR: None},
                             {OAUTH_VAR: digest(ROTATED), API_VAR: None}]  # fmt: skip
    page = await debug.task_logs(env.engine, second)
    assert "the session printed" in json.dumps(page) and ROTATED not in json.dumps(page)


async def test_child_env_loose_mode_at_launch_fails_the_task(
    env: Env, child_env: Path, server_log: Path
) -> None:
    child_env.chmod(0o644)
    task_id = (await submit(env))["task_id"]
    await env.engine.start()
    status = await wait_status(env.engine, task_id, "failed", "completed")
    assert status["status"] == "failed"
    assert "no group/other permission bits" in status["error"]
    assert f"chmod 600 {child_env}" in status["error"]
    assert stub_log(env.log) == []  # never launched
    assert TOKEN not in server_log.read_text()


def absent(text: str, where: str) -> None:
    for form in LEAKED_FORMS:
        assert form not in text, f"{where} holds the child env value ({form!r})"


async def test_child_env_values_never_recorded_or_served(
    env: Env, child_env: Path, server_log: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_child_env(child_env, f"{OAUTH_VAR}='{LEAKED}'\n")
    escaped = json.dumps(LEAKED)[1:-1]
    # A session that prints the bare value and its JSON-escaped form (stream and stderr).
    monkeypatch.setenv("STUB_CLAUDE_EXTRA", f"leaked {LEAKED} and escaped {escaped} end")
    task_id = (await submit(env))["task_id"]
    await env.engine.start()
    await wait_status(env.engine, task_id, "completed")
    assert reported(env) == [{OAUTH_VAR: digest(LEAKED), API_VAR: None}]
    raw: list[Any] = []
    for table in ("events", "attempts", "tasks"):
        key = "id" if table == "tasks" else "task_id"
        raw += [dict(r) for r in await rows(f"SELECT * FROM {table} WHERE {key} = ?", task_id)]
    absent(json.dumps(raw, default=str, ensure_ascii=False), "events/attempts/task row")
    absent(json.dumps(raw, default=str), "events/attempts/task row")
    served = {
        "get_task_debug": await debug.task_debug(env.engine, task_id),
        "get_task_logs": await debug.task_logs(env.engine, task_id),
        "stderr-1": await debug.task_logs(env.engine, task_id, source="stderr-1"),
        "server logs": await debug.server_logs(env.engine),
    }
    async with client(router_app(env)) as http:
        for name, path in (
            ("REST page", f"/api/v1/tasks/{task_id}/logs"),
            ("REST stderr", f"/api/v1/tasks/{task_id}/logs?source=stderr-1"),
            ("REST debug", f"/api/v1/tasks/{task_id}/debug"),
            ("SSE", f"/api/v1/tasks/{task_id}/stream"),
            ("REST server log", "/api/v1/server/logs"),
        ):
            response = await http.get(path)
            assert response.status_code == 200, name
            served[name] = response.text
    for name, value in served.items():
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        absent(text, name)
        if isinstance(value, dict):
            absent(json.dumps(value), name)
    for name in ("get_task_logs", "stderr-1", "REST page", "REST stderr", "SSE"):
        assert "leaked " in str(served[name]) and " end" in str(served[name]), name  # served
    built = await bundle.task_bundle(
        task_id, env.engine.runstore.task_dir(task_id), server_log, env.engine.redactor,
        env.settings.data_dir,
    )  # fmt: skip
    assert built is not None
    tmp, archive = built
    try:
        with tarfile.open(archive) as tar:
            for member in tar.getmembers():
                handle = tar.extractfile(member)
                if handle is not None:
                    absent(handle.read().decode("utf-8", errors="replace"), member.name)
    finally:
        bundle.cleanup(tmp)
    log = server_log.read_text()
    assert log
    absent(log, "server log")
