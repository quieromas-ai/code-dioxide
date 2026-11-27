"""Cloudflare Tunnel listener: Access token verification and how the auth middleware uses it."""

import asyncio
import socket
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import jwt
import pytest
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, WebSocket
from jwt import PyJWK, PyJWKClient
from jwt.algorithms import RSAAlgorithm
from pydantic import ValidationError

import src.main
from src.auth import BearerAuthMiddleware
from src.cf_access import CfAccessError, CfAccessPolicy, CfAccessVerifier, ThrottledJWKClient
from src.config import Settings

TEAM = "https://example-team.cloudflareaccess.com"
AUD = "test-aud-tag"
COMMON_NAME = "client123.access"
KID = "kid-1"
MAIN_PORT = 8500
TUNNEL_PORT = 8501
TOKEN = "test-token-value"
LOOPBACK = ("127.0.0.1", 50000)
REMOTE = ("203.0.113.10", 50000)
TUNNEL_HOSTNAME = "dioxide.example.net"

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
POLICY = CfAccessPolicy(team_domain=TEAM, audience=AUD, common_name=COMMON_NAME)


class FakeJWKClient:
    """Stands in for PyJWKClient: serves one public key by kid, or fails like the real one."""

    def __init__(self, unreachable: bool = False) -> None:
        jwk_dict = RSAAlgorithm.to_jwk(_KEY.public_key(), as_dict=True)
        self._key = PyJWK({**jwk_dict, "kid": KID, "alg": "RS256", "use": "sig"})
        self._unreachable = unreachable

    def get_signing_key_from_jwt(self, token: str) -> PyJWK:
        if self._unreachable:
            raise jwt.PyJWKClientConnectionError("certs endpoint unreachable")
        kid = jwt.get_unverified_header(token).get("kid")
        if kid != KID:
            raise jwt.PyJWKClientError(f'Unable to find a signing key that matches: "{kid}"')
        return self._key


def make_token(key: Any = _KEY, kid: str = KID, alg: str = "RS256", **overrides: Any) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "aud": [AUD],
        "iss": TEAM,
        "iat": now,
        "exp": now + 3600,
        "type": "app",
        "common_name": COMMON_NAME,
        "sub": "",
    }
    claims.update(overrides)
    claims = {k: v for k, v in claims.items() if v is not None}
    return jwt.encode(claims, key, algorithm=alg, headers={"kid": kid})


def tunnel_settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "home": tmp_path,
        "auth_token": TOKEN,
        "tunnel_port": TUNNEL_PORT,
        "cf_access_team_domain": TEAM,
        "cf_access_aud": AUD,
        "cf_access_common_name": COMMON_NAME,
        "allowed_hosts": [TUNNEL_HOSTNAME],
    }
    values.update(overrides)
    return Settings(**values)


# --- CfAccessVerifier ---------------------------------------------------------------------


def test_verify_accepts_valid_token() -> None:
    claims = CfAccessVerifier(POLICY, FakeJWKClient()).verify(make_token())
    assert claims["common_name"] == COMMON_NAME


@pytest.mark.parametrize(
    "token_kwargs",
    [
        {"key": _OTHER_KEY},  # forged signature
        {"aud": ["other-aud"]},
        {"iss": "https://evil.cloudflareaccess.com"},
        {"exp": int(time.time()) - 3600},  # expired beyond leeway
        {"exp": None},
        {"iat": None},
        {"type": "org"},
        {"common_name": "other-client.access"},
        {"common_name": None},
        {"kid": "unknown-kid"},
    ],
)
def test_verify_rejects_invalid_tokens(token_kwargs: dict[str, Any]) -> None:
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, FakeJWKClient()).verify(make_token(**token_kwargs))


def test_verify_rejects_hs256_algorithm_confusion() -> None:
    claims = {"aud": AUD, "iss": TEAM, "iat": 0, "exp": 2**31, "type": "app"}
    public_pem = _KEY.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    header = jwt.utils.base64url_encode(
        b'{"alg":"HS256","typ":"JWT","kid":"' + KID.encode() + b'"}'
    )
    payload = jwt.utils.base64url_encode(
        jwt.api_jws.json.dumps({**claims, "common_name": COMMON_NAME}).encode()
    )
    signing_input = header + b"." + payload
    signature = jwt.utils.base64url_encode(
        jwt.algorithms.HMACAlgorithm(jwt.algorithms.HMACAlgorithm.SHA256).sign(
            signing_input, public_pem
        )
    )
    token = (signing_input + b"." + signature).decode()
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, FakeJWKClient()).verify(token)


@pytest.mark.parametrize("token", ["", "not-a-jwt", "aaa.bbb.ccc"])
def test_verify_rejects_missing_or_malformed(token: str) -> None:
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, FakeJWKClient()).verify(token)


def test_verify_fails_closed_when_keys_unreachable() -> None:
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, FakeJWKClient(unreachable=True)).verify(make_token())


def test_verify_accepts_token_expired_within_leeway() -> None:
    token = make_token(exp=int(time.time()) - 30)
    assert CfAccessVerifier(POLICY, FakeJWKClient()).verify(token)["type"] == "app"


def test_verify_rejects_token_not_yet_valid() -> None:
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, FakeJWKClient()).verify(make_token(nbf=int(time.time()) + 3600))


class BrokenKeySource:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def get_signing_key_from_jwt(self, token: str) -> PyJWK:
        raise self._error


@pytest.mark.parametrize("error", [ValueError("not json"), ConnectionResetError("reset")])
def test_verify_wraps_key_source_failures(error: Exception) -> None:
    with pytest.raises(CfAccessError):
        CfAccessVerifier(POLICY, BrokenKeySource(error)).verify(make_token())


def test_default_key_source_is_the_team_certs_endpoint() -> None:
    verifier = CfAccessVerifier(POLICY)
    assert isinstance(verifier.key_source, ThrottledJWKClient)
    assert verifier.key_source.uri == f"{TEAM}/cdn-cgi/access/certs"


def test_throttled_client_limits_refetch_on_unknown_kid() -> None:
    client = ThrottledJWKClient(f"{TEAM}/cdn-cgi/access/certs", min_refresh_seconds=60)
    jwk_dict = {**RSAAlgorithm.to_jwk(_KEY.public_key(), as_dict=True), "kid": KID, "use": "sig"}
    fetches: list[int] = []

    def fake_fetch() -> dict[str, Any]:
        fetches.append(1)
        data = {"keys": [jwk_dict]}
        assert client.jwk_set_cache is not None
        client.jwk_set_cache.put(data)
        return data

    with patch.object(client, "fetch_data", side_effect=fake_fetch):
        for _ in range(5):
            with pytest.raises(jwt.PyJWKClientError):
                client.get_signing_key("random-kid")
        assert client.get_signing_key(KID).key_id == KID
    assert len(fetches) == 1


def test_throttled_client_serves_last_good_keys_when_fetch_fails() -> None:
    client = ThrottledJWKClient(f"{TEAM}/cdn-cgi/access/certs", min_refresh_seconds=0)
    jwk_dict = {**RSAAlgorithm.to_jwk(_KEY.public_key(), as_dict=True), "kid": KID, "use": "sig"}
    outcomes: list[Any] = [{"keys": [jwk_dict]}, jwt.PyJWKClientConnectionError("down")]

    def upstream_fetch(self: PyJWKClient) -> Any:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    with patch.object(PyJWKClient, "fetch_data", upstream_fetch):
        assert client.get_signing_key(KID).key_id == KID
        assert client.jwk_set_cache is not None
        client.jwk_set_cache.put(None)  # the cache expired; the next lookup fetches again
        assert client.get_signing_key(KID).key_id == KID
    assert outcomes == []


def test_throttled_client_fails_fast_after_failed_fetch() -> None:
    client = ThrottledJWKClient(f"{TEAM}/cdn-cgi/access/certs", min_refresh_seconds=60)
    failure = jwt.PyJWKClientConnectionError("down")
    with patch.object(client, "fetch_data", side_effect=failure) as fetch:
        for _ in range(3):
            with pytest.raises(jwt.PyJWKClientConnectionError):
                client.get_signing_key(KID)
    assert fetch.call_count == 1


# --- settings ----------------------------------------------------------------------------


def test_tunnel_off_by_default(tmp_path: Path) -> None:
    settings = Settings(home=tmp_path, auth_token=TOKEN)
    assert (settings.tunnel_host, settings.tunnel_port) == ("127.0.0.1", 0)
    assert settings.cf_access_team_domain == settings.cf_access_aud == ""
    assert settings.cf_access_common_name == ""


def test_tunnel_settings_read_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIOXIDE_TUNNEL_HOST", "100.64.0.1")
    monkeypatch.setenv("DIOXIDE_TUNNEL_PORT", str(TUNNEL_PORT))
    monkeypatch.setenv("DIOXIDE_CF_ACCESS_TEAM_DOMAIN", TEAM + "/")
    monkeypatch.setenv("DIOXIDE_CF_ACCESS_AUD", AUD)
    monkeypatch.setenv("DIOXIDE_CF_ACCESS_COMMON_NAME", COMMON_NAME)
    monkeypatch.setenv("DIOXIDE_ALLOWED_HOSTS", f'["{TUNNEL_HOSTNAME}"]')
    settings = Settings(home=tmp_path, auth_token=TOKEN)
    assert (settings.tunnel_host, settings.tunnel_port) == ("100.64.0.1", TUNNEL_PORT)
    assert settings.cf_access_team_domain == TEAM  # trailing slash dropped
    assert settings.cf_access_policy() == POLICY


@pytest.mark.parametrize(
    "missing", ["cf_access_team_domain", "cf_access_aud", "cf_access_common_name"]
)
def test_tunnel_requires_every_cf_access_setting(tmp_path: Path, missing: str) -> None:
    with pytest.raises(ValidationError, match="all of cf_access_team_domain"):
        tunnel_settings(tmp_path, **{missing: ""})


@pytest.mark.parametrize(
    "missing", ["cf_access_team_domain", "cf_access_aud", "cf_access_common_name"]
)
def test_partial_cf_access_without_tunnel_rejected(tmp_path: Path, missing: str) -> None:
    with pytest.raises(ValidationError, match="all of cf_access_team_domain"):
        tunnel_settings(tmp_path, tunnel_port=0, **{missing: ""})


def test_tunnel_port_must_differ_from_port(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="must differ"):
        tunnel_settings(tmp_path, tunnel_port=MAIN_PORT)


def test_tunnel_requires_allowed_hosts(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="allowed_hosts"):
        tunnel_settings(tmp_path, allowed_hosts=[])


@pytest.mark.parametrize("domain", ["http://example-team.cloudflareaccess.com", "example.com"])
def test_team_domain_must_be_https(tmp_path: Path, domain: str) -> None:
    with pytest.raises(ValidationError, match="https://"):
        tunnel_settings(tmp_path, cf_access_team_domain=domain)


def test_cf_access_without_tunnel_port_is_allowed(tmp_path: Path) -> None:
    settings = tunnel_settings(tmp_path, tunnel_port=0)
    assert settings.cf_access_policy() == POLICY


def test_no_policy_without_team_domain(tmp_path: Path) -> None:
    assert Settings(home=tmp_path, auth_token=TOKEN).cf_access_policy() is None


def test_tunnel_port_must_be_a_tcp_port(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        tunnel_settings(tmp_path, tunnel_port=70000)


def test_tunnel_refuses_auth_disabled(tmp_path: Path) -> None:
    settings = tunnel_settings(tmp_path, auth_token="", auth_disabled=True)
    with pytest.raises(ValueError, match="auth_disabled"):
        settings.validate_security()


# --- middleware --------------------------------------------------------------------------


def protected_app(verifier: CfAccessVerifier | None, **options: Any) -> Any:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        await socket.accept()
        await socket.send_text("ok")
        await socket.close()

    return BearerAuthMiddleware(
        app,
        token=TOKEN,
        auth_disabled=False,
        trust_loopback=options.get("trust_loopback", False),
        tunnel_port=options.get("tunnel_port", TUNNEL_PORT),
        cf_verifier=verifier,
    )


def jwt_header(token: str | None = None) -> dict[str, str]:
    return {"Cf-Access-Jwt-Assertion": token if token is not None else make_token()}


def bearer() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


async def get_health(
    app: Any, port: int, client: tuple[str, int], headers: dict[str, str]
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app, client=client)
    async with httpx.AsyncClient(transport=transport, base_url=f"http://127.0.0.1:{port}") as c:
        return await c.get("/health", headers=headers)


VERIFIER = CfAccessVerifier(POLICY, FakeJWKClient())


@pytest.mark.parametrize(
    ("port", "client", "headers", "status"),
    [
        # Tunnel listener: a valid Access token and the bearer token, both required.
        (TUNNEL_PORT, REMOTE, {**jwt_header(), **bearer()}, 200),
        (TUNNEL_PORT, REMOTE, jwt_header(), 401),
        (TUNNEL_PORT, REMOTE, bearer(), 403),
        (TUNNEL_PORT, LOOPBACK, bearer(), 403),
        (TUNNEL_PORT, REMOTE, {**jwt_header(make_token(key=_OTHER_KEY)), **bearer()}, 403),
        (TUNNEL_PORT, REMOTE, {**jwt_header(make_token(aud=["x"])), **bearer()}, 403),
        # Main listener: bearer as before; Cloudflare headers make it stricter, never looser.
        (MAIN_PORT, REMOTE, bearer(), 200),
        (MAIN_PORT, REMOTE, {**jwt_header("forged"), **bearer()}, 403),
        (MAIN_PORT, REMOTE, {"Cf-Connecting-IP": "198.51.100.1", **bearer()}, 403),
        (MAIN_PORT, REMOTE, {**jwt_header(), **bearer()}, 200),
    ],
)
async def test_tunnel_access_matrix(
    port: int, client: tuple[str, int], headers: dict[str, str], status: int
) -> None:
    response = await get_health(protected_app(VERIFIER), port, client, headers)
    assert response.status_code == status


@pytest.mark.parametrize(
    "headers", [{"Cf-Connecting-IP": "198.51.100.1"}, {"Cf-Access-Jwt-Assertion": "x"}]
)
async def test_cloudflare_headers_never_trusted_as_loopback(headers: dict[str, str]) -> None:
    app = protected_app(VERIFIER, trust_loopback=True)
    assert (await get_health(app, MAIN_PORT, LOOPBACK, {})).status_code == 200
    assert (await get_health(app, MAIN_PORT, LOOPBACK, headers)).status_code == 403


@pytest.mark.parametrize(
    "headers", [{"Cf-Connecting-IP": "198.51.100.1"}, {"Cf-Access-Jwt-Assertion": "x"}]
)
async def test_cloudflare_headers_without_verifier_keep_bearer_auth(
    headers: dict[str, str],
) -> None:
    """Without Cloudflare Access configured (an existing setup with cloudflared in front of the
    main port), Cloudflare headers change nothing for bearer callers but are never local."""
    app = protected_app(None, tunnel_port=0, trust_loopback=True)
    assert (await get_health(app, MAIN_PORT, REMOTE, {**headers, **bearer()})).status_code == 200
    assert (await get_health(app, MAIN_PORT, LOOPBACK, headers)).status_code == 401


async def test_tunnel_listener_never_trusted_as_loopback() -> None:
    app = protected_app(VERIFIER, trust_loopback=True)
    response = await get_health(app, TUNNEL_PORT, LOOPBACK, jwt_header())
    assert response.status_code == 401


async def test_tunnel_listener_without_verifier_fails_closed() -> None:
    app = protected_app(None)
    response = await get_health(app, TUNNEL_PORT, REMOTE, {**jwt_header(), **bearer()})
    assert response.status_code == 403
    assert response.json() == {"error": "forbidden"}


async def test_unknown_listener_counts_as_tunnel_when_tunnel_on() -> None:
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/health",
        "raw_path": b"/health",
        "query_string": b"",
        "headers": [(b"authorization", f"Bearer {TOKEN}".encode())],
        "client": REMOTE,
        "server": None,
    }
    await protected_app(VERIFIER)(scope, receive, send)
    assert sent[0]["status"] == 403


async def test_websocket_on_tunnel_without_jwt_closed() -> None:
    app = protected_app(VERIFIER)
    sent: list[dict[str, Any]] = []
    messages = [{"type": "websocket.connect"}]

    async def receive() -> dict[str, Any]:
        return messages.pop(0) if messages else {"type": "websocket.disconnect", "code": 1000}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "websocket",
        "path": "/ws",
        "raw_path": b"/ws",
        "query_string": b"",
        "headers": [(b"authorization", f"Bearer {TOKEN}".encode())],
        "client": REMOTE,
        "server": ("127.0.0.1", TUNNEL_PORT),
        "subprotocols": [],
    }
    await app(scope, receive, send)
    assert sent == [{"type": "websocket.close", "code": 1008, "reason": ""}]


async def test_unreachable_keys_refuse_tunnel_requests() -> None:
    app = protected_app(CfAccessVerifier(POLICY, FakeJWKClient(unreachable=True)))
    response = await get_health(app, TUNNEL_PORT, REMOTE, {**jwt_header(), **bearer()})
    assert response.status_code == 403


async def test_create_app_verifies_tunnel_requests(tmp_path: Path) -> None:
    app = src.main.create_app(tunnel_settings(tmp_path))
    response = await get_health(app, TUNNEL_PORT, REMOTE, bearer())
    assert response.status_code == 403


# --- listeners ---------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


def test_serve_binds_main_and_tunnel_listeners(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    main_port, tunnel_port = free_port(), free_port()
    settings = tunnel_settings(tmp_path, port=main_port, tunnel_port=tunnel_port)
    served: list[tuple[Any, list[tuple[str, int]]]] = []

    def fake_run(server: Any, sockets: list[socket.socket]) -> None:
        served.append((server.config, [s.getsockname()[:2] for s in sockets]))
        server.started = True
        for sock in sockets:
            sock.close()

    monkeypatch.setattr(src.main.uvicorn.Server, "run", fake_run)
    monkeypatch.setattr(src.main.uvicorn, "run", lambda *a, **k: pytest.fail("one listener"))
    src.main.serve(settings)
    ((config, addresses),) = served
    assert addresses == [("127.0.0.1", main_port), ("127.0.0.1", tunnel_port)]
    assert config.proxy_headers is False
    assert config.http == "h11"
    assert config.h11_max_incomplete_event_size == src.main.MAX_REQUEST_HEAD_BYTES


async def test_real_listeners_tell_tunnel_from_main() -> None:
    """Real uvicorn on two pre-bound sockets: the listener port reaches the middleware."""
    main_sock = src.main._bind_listener("127.0.0.1", 0)
    tunnel_sock = src.main._bind_listener("127.0.0.1", 0)
    tunnel_port = tunnel_sock.getsockname()[1]
    app = protected_app(VERIFIER, tunnel_port=tunnel_port)
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="critical"))
    serving = asyncio.create_task(server.serve(sockets=[main_sock, tunnel_sock]))
    while not server.started:
        await asyncio.sleep(0.01)
    try:
        async with httpx.AsyncClient() as client:
            main = f"http://127.0.0.1:{main_sock.getsockname()[1]}/health"
            tunnel = f"http://127.0.0.1:{tunnel_port}/health"
            assert (await client.get(main, headers=bearer())).status_code == 200
            assert (await client.get(tunnel, headers=bearer())).status_code == 403
            both = {**jwt_header(), **bearer()}
            assert (await client.get(tunnel, headers=both)).status_code == 200
    finally:
        server.should_exit = True
        await serving
        main_sock.close()
        tunnel_sock.close()


def test_serve_exits_with_startup_failure_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = tunnel_settings(tmp_path, port=free_port(), tunnel_port=free_port())
    monkeypatch.setattr(src.main.uvicorn.Server, "run", lambda server, sockets: None)
    with pytest.raises(SystemExit) as exited:
        src.main.serve(settings)
    assert exited.value.code == 3  # uvicorn's STARTUP_FAILURE, as uvicorn.run exits
