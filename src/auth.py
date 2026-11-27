"""Bearer-token access control for every route, as pure ASGI (the MCP endpoint streams).

Cloudflare requests first: a request on the tunnel listener needs a valid Cloudflare Access
application token (src.cf_access), else 403 (always 403 without a configured verifier). With a
verifier configured, so does a request carrying Cloudflare headers (`Cf-Connecting-IP`,
`Cf-Access-Jwt-Assertion`) on any listener. Without one, those headers only make the caller
non-local. Then: `auth_disabled` -> admin; admin token -> admin; content token -> content;
reports token -> reports; `trust_loopback` and a loopback peer without forwarding or
Cloudflare headers, not on the tunnel listener -> admin; anything else -> 401. The peer address
and the listener port come from the ASGI connection (`scope["client"]`, `scope["server"]`),
never from headers. A reports or content caller reaches only the routes its allowlist names
(method and the whole decoded path); anything else is 403, a websocket is closed. The scope is
left in `scope["state"]["access"]` for handlers that shape their response by it.
"""

import hmac
import ipaddress
import re
from enum import StrEnum

from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketClose

from src.cf_access import (
    CF_ACCESS_JWT_HEADER,
    CF_CONNECTING_IP_HEADER,
    CfAccessError,
    CfAccessVerifier,
)

# Set by reverse proxies: the loopback peer is then a proxy, not a local client.
_FORWARDING_PREFIX = b"x-forwarded-"
_FORWARDING_HEADERS = frozenset({b"forwarded", b"x-real-ip"})
_CLOUDFLARE_HEADERS = frozenset({CF_ACCESS_JWT_HEADER, CF_CONNECTING_IP_HEADER})


class Access(StrEnum):
    admin = "admin"
    content = "content"
    reports = "reports"


_ID = "[^/]+"
# Numbers only: no prompt, result, command or free text is served on these routes.
REPORTS_ROUTES: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    ("GET", re.compile(path))
    for path in (
        "/health",
        "/api/v1",
        "/api/v1/openapi.json",
        "/api/v1/tasks",
        f"/api/v1/tasks/{_ID}",
        f"/api/v1/tasks/{_ID}/(attempts|stages|graph)",
        "/api/v1/agents",
        f"/api/v1/agents/{_ID}",
        "/api/v1/projects",
        "/api/v1/metrics/(usage|time|reliability|stages|savings|overhead)",
        "/api/v1/events",
        "/api/v1/activity",
        "/api/v1/activity/stream",
    )
)
# Text routes, redacted; the content scope also has every reports route.
CONTENT_ROUTES: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    ("GET", re.compile(path))
    for path in (
        f"/api/v1/agents/{_ID}/prompt",
        f"/api/v1/tasks/{_ID}/(prompts|events|summary)",
        f"/api/v1/tasks/{_ID}/stages/{_ID}/(result|transcript)",
    )
)
_ALLOWED = {
    Access.reports: REPORTS_ROUTES,
    Access.content: REPORTS_ROUTES + CONTENT_ROUTES,
}


def access_of(scope: Scope) -> Access:
    """The caller's scope as the middleware resolved it; the least privileged scope when no
    middleware ran (a route served outside it fails closed)."""
    access = scope.get("state", {}).get("access")
    return access if isinstance(access, Access) else Access.reports


def _route_allowed(access: Access, scope: Scope) -> bool:
    if access is Access.admin:
        return True
    if scope["type"] != "http":
        return False
    method, path = scope["method"], scope["path"]
    return any(m == method and p.fullmatch(path) for m, p in _ALLOWED[access])


def _is_loopback(scope: Scope) -> bool:
    client = scope.get("client")
    if not client:
        return False
    try:
        address = ipaddress.ip_address(client[0])
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_loopback


def _is_forwarded(scope: Scope) -> bool:
    return any(
        name.startswith(_FORWARDING_PREFIX) or name in _FORWARDING_HEADERS
        for name, _ in scope["headers"]
    )


def _header(scope: Scope, wanted: bytes) -> str:
    for name, value in scope["headers"]:
        if name == wanted:
            return str(value.decode("latin-1"))
    return ""


def _bearer_token(scope: Scope) -> str | None:
    for name, value in scope["headers"]:
        if name == b"authorization":
            scheme, _, token = value.decode("latin-1").partition(" ")
            return token.strip() if scheme.lower() == "bearer" else None
    return None


class BearerAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        token: str,
        auth_disabled: bool,
        trust_loopback: bool,
        reports_token: str = "",
        content_token: str = "",
        tunnel_port: int = 0,
        cf_verifier: CfAccessVerifier | None = None,
    ) -> None:
        self.app = app
        self._tokens = [
            (value.encode(), access)
            for value, access in (
                (token, Access.admin),
                (content_token, Access.content),
                (reports_token, Access.reports),
            )
            if value
        ]
        self._auth_disabled = auth_disabled
        self._trust_loopback = trust_loopback
        self._tunnel_port = tunnel_port
        self._cf_verifier = cf_verifier

    def _token_access(self, scope: Scope) -> Access | None:
        presented = _bearer_token(scope)
        if presented is None:
            return None
        found = None
        for value, access in self._tokens:  # compare against every token: no early exit
            if hmac.compare_digest(presented.encode(), value) and found is None:
                found = access
        return found

    def _on_tunnel(self, scope: Scope) -> bool:
        if not self._tunnel_port:
            return False
        server = scope.get("server")
        # An unknown listener counts as the tunnel one: the stricter checks apply.
        return server is None or server[1] == self._tunnel_port

    async def _cf_access_verified(self, scope: Scope) -> bool:
        if self._cf_verifier is None:
            return False  # Cloudflare traffic without verification configured: fail closed
        token = _header(scope, CF_ACCESS_JWT_HEADER)
        try:
            await run_in_threadpool(self._cf_verifier.verify, token)  # may fetch signing keys
        except CfAccessError:
            return False
        return True

    def _access(self, scope: Scope, from_cloudflare: bool) -> Access | None:
        if self._auth_disabled:
            return Access.admin
        if (access := self._token_access(scope)) is not None:
            return access
        if (
            self._trust_loopback
            and not from_cloudflare
            and _is_loopback(scope)
            and not _is_forwarded(scope)
        ):
            return Access.admin
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        on_tunnel = self._on_tunnel(scope)
        cloudflare_headers = any(name in _CLOUDFLARE_HEADERS for name, _ in scope["headers"])
        must_verify = on_tunnel or (cloudflare_headers and self._cf_verifier is not None)
        if must_verify and not await self._cf_access_verified(scope):
            await _refuse(scope, receive, send, unauthenticated=False)
            return
        from_cloudflare = on_tunnel or cloudflare_headers
        access = self._access(scope, from_cloudflare)
        if access is not None and _route_allowed(access, scope):
            scope.setdefault("state", {})["access"] = access
            await self.app(scope, receive, send)
            return
        await _refuse(scope, receive, send, unauthenticated=access is None)


async def _refuse(scope: Scope, receive: Receive, send: Send, *, unauthenticated: bool) -> None:
    if scope["type"] == "websocket":
        await WebSocketClose(code=1008)(scope, receive, send)
        return
    if unauthenticated:
        response = JSONResponse(
            {"error": "unauthorized"}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
        )
    else:
        response = JSONResponse({"error": "forbidden"}, status_code=403)
    await response(scope, receive, send)
