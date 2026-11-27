"""Origin-side verification of Cloudflare Access application tokens.

Cloudflare Access forwards a signed RS256 JWT in the ``Cf-Access-Jwt-Assertion`` header to the
origin. Verifying it here keeps tunnel traffic closed even if the Access application is deleted,
misconfigured or bypassed.
Reference: https://developers.cloudflare.com/cloudflare-one/identity/authorization-cookie/validating-json/
"""

import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol

import jwt
from jwt import PyJWK, PyJWKClient, PyJWKClientConnectionError, PyJWKSet

CF_ACCESS_JWT_HEADER = b"cf-access-jwt-assertion"
CF_CONNECTING_IP_HEADER = b"cf-connecting-ip"
_ALGORITHMS = ["RS256"]
_REQUIRED_CLAIMS = ["aud", "iss", "exp", "iat"]
_EXPECTED_TOKEN_TYPE = "app"  # a service-token (or user) application token, not an org token


class CfAccessError(Exception):
    """A Cloudflare Access token is missing, invalid or cannot be verified."""


@dataclass(frozen=True)
class CfAccessPolicy:
    team_domain: str  # https://<team>.cloudflareaccess.com, no trailing slash
    audience: str  # the Access application's AUD tag
    common_name: str  # the service token's client id
    leeway_seconds: int = 60


class SigningKeySource(Protocol):
    def get_signing_key_from_jwt(self, token: str) -> PyJWK: ...


class ThrottledJWKClient(PyJWKClient):
    """PyJWKClient that contacts the certs endpoint at most once per ``min_refresh_seconds``.

    - Unknown ``kid`` values cannot make the server hammer the endpoint: within the interval
      the cached set is reused and the token is rejected.
    - After a failed fetch, requests fail fast until the interval passes instead of each one
      waiting on the network (pyjwt caches nothing on fetch errors).
    - The last good key set is kept and used when a later fetch fails, for as long as fetches
      keep failing: Cloudflare's published keys stay trusted through an outage of the endpoint.
    Trade-off: a key rotated within the interval is rejected until the next fetch.
    """

    def __init__(self, uri: str, min_refresh_seconds: float = 60.0, timeout: float = 5.0) -> None:
        super().__init__(uri, cache_jwk_set=True, lifespan=300, timeout=timeout)
        self._min_refresh_seconds = min_refresh_seconds
        self._last_fetch_attempt: float | None = None
        self._last_good_jwk_set: Any = None
        self._lock = threading.Lock()

    def fetch_data(self) -> Any:
        try:
            jwk_set = super().fetch_data()
        except PyJWKClientConnectionError:
            if self._last_good_jwk_set is None:
                raise
            if self.jwk_set_cache is not None:
                self.jwk_set_cache.put(self._last_good_jwk_set)
            return self._last_good_jwk_set
        self._last_good_jwk_set = jwk_set
        return jwk_set

    def get_jwk_set(self, refresh: bool = False) -> PyJWKSet:
        with self._lock:
            cached = None if self.jwk_set_cache is None else self.jwk_set_cache.get()
            if refresh or cached is None:
                now = time.monotonic()
                throttled = (
                    self._last_fetch_attempt is not None
                    and now - self._last_fetch_attempt < self._min_refresh_seconds
                )
                if throttled and cached is None and self._last_good_jwk_set is None:
                    raise PyJWKClientConnectionError("certs fetch throttled after a recent failure")
                if throttled:
                    refresh = False
                    if cached is None and self.jwk_set_cache is not None:
                        self.jwk_set_cache.put(self._last_good_jwk_set)
                else:
                    self._last_fetch_attempt = now
            return super().get_jwk_set(refresh)


class CfAccessVerifier:
    def __init__(self, policy: CfAccessPolicy, key_source: SigningKeySource | None = None) -> None:
        self._policy = policy
        self.key_source: SigningKeySource = key_source or ThrottledJWKClient(
            f"{policy.team_domain}/cdn-cgi/access/certs"
        )

    def verify(self, token: str) -> dict[str, Any]:
        """The verified claims, or CfAccessError. Blocking: may fetch the signing keys."""
        if not token:
            raise CfAccessError("missing Cloudflare Access token")
        try:
            signing_key = self.key_source.get_signing_key_from_jwt(token)
            claims: dict[str, Any] = jwt.decode(
                token,
                signing_key.key,
                algorithms=_ALGORITHMS,
                audience=self._policy.audience,
                issuer=self._policy.team_domain,
                leeway=self._policy.leeway_seconds,
                options={"require": _REQUIRED_CLAIMS},
            )
        # OSError, ValueError: a certs response cut short or not JSON (pyjwt wraps only some).
        except (jwt.PyJWTError, OSError, ValueError) as exc:
            raise CfAccessError(f"invalid Cloudflare Access token: {type(exc).__name__}") from exc
        if claims.get("type") != _EXPECTED_TOKEN_TYPE:
            raise CfAccessError("unexpected Cloudflare Access token type")
        if claims.get("common_name") != self._policy.common_name:
            raise CfAccessError("Cloudflare Access token is not for the expected service token")
        return claims
