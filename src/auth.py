"""Bearer-token access control for every route, as pure ASGI (the MCP endpoint streams).

Order: `auth_disabled` -> admin; admin token -> admin; content token -> content; reports token
-> reports; `trust_loopback` and a loopback peer without forwarding headers -> admin; anything
else -> 401. The peer address comes from the ASGI connection (`scope["client"]`), never from
headers. A reports or content caller reaches only the routes its allowlist names (method and
the whole decoded path); anything else is 403, a websocket is closed. The scope is left in
`scope["state"]["access"]` for handlers that shape their response by it.
"""

import hmac
import ipaddress
import re
from enum import StrEnum

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketClose

# Set by reverse proxies: the loopback peer is then a proxy, not a local client.
_FORWARDING_PREFIX = b"x-forwarded-"
_FORWARDING_HEADERS = frozenset({b"forwarded", b"x-real-ip"})


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

    def _token_access(self, scope: Scope) -> Access | None:
        presented = _bearer_token(scope)
        if presented is None:
            return None
        found = None
        for value, access in self._tokens:  # compare against every token: no early exit
            if hmac.compare_digest(presented.encode(), value) and found is None:
                found = access
        return found

    def _access(self, scope: Scope) -> Access | None:
        if self._auth_disabled:
            return Access.admin
        if (access := self._token_access(scope)) is not None:
            return access
        if self._trust_loopback and _is_loopback(scope) and not _is_forwarded(scope):
            return Access.admin
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        access = self._access(scope)
        if access is not None and _route_allowed(access, scope):
            scope.setdefault("state", {})["access"] = access
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await WebSocketClose(code=1008)(scope, receive, send)
            return
        if access is None:
            response = JSONResponse(
                {"error": "unauthorized"}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
            )
        else:
            response = JSONResponse({"error": "forbidden"}, status_code=403)
        await response(scope, receive, send)
