"""Bearer-token access control for every route, as pure ASGI (the MCP endpoint streams).

Order: `auth_disabled` -> pass; valid token -> pass; `trust_loopback` and a loopback
peer without forwarding headers -> pass; anything else -> 401. The peer address comes
from the ASGI connection (`scope["client"]`), never from headers.
"""

import hmac
import ipaddress

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketClose

# Set by reverse proxies: the loopback peer is then a proxy, not a local client.
_FORWARDING_PREFIX = b"x-forwarded-"
_FORWARDING_HEADERS = frozenset({b"forwarded", b"x-real-ip"})


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
        self, app: ASGIApp, *, token: str, auth_disabled: bool, trust_loopback: bool
    ) -> None:
        self.app = app
        self._token = token.encode()
        self._auth_disabled = auth_disabled
        self._trust_loopback = trust_loopback

    def _has_valid_token(self, scope: Scope) -> bool:
        presented = _bearer_token(scope)
        if not self._token or presented is None:
            return False
        return hmac.compare_digest(presented.encode(), self._token)

    def _is_allowed(self, scope: Scope) -> bool:
        return (
            self._auth_disabled
            or self._has_valid_token(scope)
            or (self._trust_loopback and _is_loopback(scope) and not _is_forwarded(scope))
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket") or self._is_allowed(scope):
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await WebSocketClose(code=1008)(scope, receive, send)
            return
        response = JSONResponse(
            {"error": "unauthorized"}, status_code=401, headers={"WWW-Authenticate": "Bearer"}
        )
        await response(scope, receive, send)
