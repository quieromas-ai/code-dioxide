import functools
import json
import logging
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Self

from src import childenv
from src.config import Settings

REDACTED = "****"
MIN_ENV_VALUE_LEN = 8

_PEM_BEGIN = re.compile(r"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----")
_PEM_END = re.compile(r"-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----")

# Plain-text patterns run on unescaped text (JSON strings are decoded first, see Redactor), so
# they need no escape awareness. All are linear: bounded runs, possessive quantifiers, and
# no unanchored scan that can restart inside one long identifier run.
#
# Self-delimiting tokens: the whole match is replaced; no leading boundary, so a token directly
# after any character (including the `n` of a literal `\n` in a non-JSON line) is still found.
# The JWT and the generic `sk-` key are the patterns whose first segment can be re-entered inside
# a long run (at every `eyJ`, or inside a word such as `task-`), so they must start at a run
# boundary, or right after a literal escape sequence or a %XX URL escape.
_TOKEN_START = (
    r"(?:(?<![A-Za-z0-9_\-])|(?<=\\[nrtbf])|(?<=\\u[0-9a-fA-F]{4})"
    r"|(?<=\\x[0-9a-fA-F]{2})|(?<=%[0-9A-Fa-f]{2}))"
)
_TOKENS = re.compile(
    "|".join(
        (
            r"sk-ant-[A-Za-z0-9_\-]+",
            r"sk-proj-[A-Za-z0-9_\-]+",
            _TOKEN_START + r"sk-[A-Za-z0-9_\-]{20,}+",
            r"xox[abpr]-[A-Za-z0-9\-]+",
            r"xapp-[A-Za-z0-9\-]+",
            r"ghp_[A-Za-z0-9]+",
            r"github_pat_[A-Za-z0-9_]+",
            r"AKIA[0-9A-Z]{16}",
            _TOKEN_START + r"eyJ[A-Za-z0-9_\-]{10,2048}\.[A-Za-z0-9_\-]{10,65536}\.[A-Za-z0-9_\-]*",
        )
    )
)

# Header and command-line forms: the header name / flag is kept, the credential is masked.
# A cookie header is masked to the end of the line (values hold spaces and `;`).
_COOKIE_HEADER = re.compile(r"((?<![A-Za-z0-9_])(?:set-)?cookie[ \t]*+:[ \t]*+)[^\r\n]+", re.I)
# curl basic auth: `-u|--user user:pass`, `--user=user:pass`, optionally quoted. The argument must
# hold a colon, so `-u name`, `sort -u file` and `id -u` are left alone. The flag starts at a
# whitespace boundary.
_CURL_USER = re.compile(
    r"""((?<![^\s])(?:-u[ \t]++|--user(?:[ \t]++|=)))"""
    r"""(?:"[^"\n]*:[^"\n]*"|'[^'\n]*:[^'\n]*'|[^\s:'"]*+:\S*+)"""
)
# URL userinfo `scheme://user:pass@host`. Anchored on the literal `://`; the credential cannot
# contain `/`, `?`, `#` or whitespace, so ports and plain `user@host` forms do not match.
_URL_USERINFO = re.compile(r"(://)[^\s/?#@:]*+:[^\s/?#@]*+@")

# Key/value forms: keep key and separator, replace only the value. The value is a quoted run
# on the same line (closed by a delimiter) or otherwise everything up to the next whitespace.
# The key is a bounded identifier run starting at a run boundary (or right after a literal
# escape sequence / %XX); whether it is sensitive is decided by `is_sensitive_key`, not here.
_KEY_CHARS = r"A-Za-z0-9_.\-"
_KEY_START = (
    rf"(?:(?<![{_KEY_CHARS}\\%])|(?<=\\[nrtbf])|(?<=\\u[0-9a-fA-F]{{4}})"
    r"|(?<=\\x[0-9a-fA-F]{2})|(?<=%[0-9A-Fa-f]{2}))"
)
_KEY_SEP = re.compile(rf"""{_KEY_START}([{_KEY_CHARS}]{{1,128}}+)(?:\\?["'])?\s*+[=:]\s*+""")
_QUOTED = r"""(?:"[^"\n]*"|'[^'\n]*')(?=[\s,;)}\]]|$)"""
# Bare literals (usage counters such as `max_tokens: 4096`) are not secrets.
_BARE_LITERAL = r"""(?!(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)(?:[\s,}\]]|$))"""
# Whole values that are not credentials either: type words (`tokens: number | null`), env-var
# expansions (`$NAME`, `${NAME}`, `${NAME:-}`, `${NAME:-$NAME}`), a fixed list of placeholder
# words (`<token>`, `<api-key>`; any other `<...>`, such as `<hunter2>`, may hold a literal) and
# the mask itself. Command substitution `$(...)` is NOT exempt: its body can
# hold a literal. Case-sensitive forms use `(?-i:...)` (the pattern is case-insensitive). A form
# is exempt only as the entire value: bare (followed by whitespace/EOL, optionally one
# punctuation char first, so `$X,s3cr3t` is masked) or between matching quotes.
_ENV_NAME = r"[A-Z_][A-Z0-9_]*"
_PLACEHOLDER_WORDS = "token|key|api-key|secret|password|value|user|name|id|path|url|host|port"
_EXEMPT_FORM = (
    r"(?:number|string|boolean|undefined|none|n/a|\*{4}"
    rf"|(?-i:\$(?:{_ENV_NAME}|\{{{_ENV_NAME}(?::-(?:\${_ENV_NAME})?)?\}}))"
    rf"|(?-i:<(?:{_PLACEHOLDER_WORDS})>))"
)
_EXEMPT_END_BARE = r"[,;)}\]]?(?=\s|$)"
_EXEMPT_END_QUOTED = r"(?=[\s,;)}\]]|$)"
_EXEMPT_VALUE = (
    rf"""(?:{_EXEMPT_FORM}{_EXEMPT_END_BARE}"""
    rf"""|"{_EXEMPT_FORM}"{_EXEMPT_END_QUOTED}|'{_EXEMPT_FORM}'{_EXEMPT_END_QUOTED})"""
)
_EXEMPT = re.compile(_EXEMPT_VALUE, re.I)
_VALUE = re.compile(rf"{_BARE_LITERAL}(?!{_EXEMPT_VALUE})((?:bearer\s+)?)({_QUOTED}|\S+)", re.I)
_BEARER = re.compile(r"""(authorization(?:\\?["'])?\s*[=:]\s*(?:\\?["'])?bearer\s+)\S+""", re.I)

# Sensitive key names, decided on word parts (split on non-alphanumerics and camelCase).
# Unambiguous words count anywhere inside a part, so glued names (`PGPASSWORD`) are caught.
_SECRET_SUBSTRINGS = (
    "secret", "token", "password", "passwd", "credential", "apikey", "privatekey",
)  # fmt: skip
# Short or ambiguous words count only as a whole part.
_SECRET_PARTS = frozenset(
    {"pass", "pwd", "pat", "auth", "authorization", "cookie", "bearer"}
)  # fmt: skip
# `<prefix>pass` glued forms (`DBPASS`); `compass` and `bypass` are not credentials.
_PASS_PREFIXES = frozenset(
    {"db", "pg", "mysql", "redis", "smtp", "ftp", "ssh", "sudo", "user", "admin", "root", "my"}
)  # fmt: skip
_KEY_PARTS = frozenset({"key", "keys"})
# A `key` part next to one of these names a lookup key, not a credential.
_BENIGN_KEY_PARTS = frozenset(
    {
        "cache", "sort", "primary", "foreign", "partition", "public", "group", "index", "row",
        "map", "dict", "object", "json", "hot", "shortcut", "keyboard",
    }
)  # fmt: skip
_WORD = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")


def _secret_part(part: str) -> bool:
    if part in _SECRET_PARTS or any(word in part for word in _SECRET_SUBSTRINGS):
        return True
    return part.endswith("pass") and part[:-4] in _PASS_PREFIXES


@functools.lru_cache(maxsize=4096)
def is_sensitive_key(key: str) -> bool:
    """True for a key whose value is a credential, decided per word part:

    - a part containing an unambiguous word (``secret``, ``token``, ``password``, ``passwd``,
      ``credential``, ``apikey``, ``privatekey``): ``PGPASSWORD``, ``clientsecret``;
    - a part equal to a short word (``pass``, ``pwd``, ``pat``, ``auth``, ``authorization``,
      ``cookie``, ``bearer``) or ``<prefix>pass`` for a known prefix: ``DBPASS``;
    - a ``key``/``keys`` part with at least one other part, none of them benign:
      ``OPENAI_KEY``, ``X-API-Key``; ``key`` alone, ``cache_key`` and ``public_key`` are not.
    """
    parts = [w.lower() for w in _WORD.findall(key)]
    if any(_secret_part(part) for part in parts):
        return True
    others = [part for part in parts if part not in _KEY_PARTS]
    return len(others) < len(parts) and bool(others) and not _BENIGN_KEY_PARTS.intersection(others)


# Home `.env` mixes config and secrets: only values under secret-looking keys are registered.
_SECRET_ENV_KEY = re.compile(r"token|secret|password|passwd|key|credential|auth", re.I)
_INLINE_COMMENT = re.compile(r"\s+#.*$")


def _masked(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return f"{value[0]}{REDACTED}{value[0]}"
    return REDACTED


def _mask_key_values(text: str) -> str:
    """Mask the value after every sensitive ``key<sep>``; one forward scan (linear)."""
    parts: list[str] = []
    pos = done = 0
    while (key := _KEY_SEP.search(text, pos)) is not None:
        pos = key.end()
        if not is_sensitive_key(key.group(1)):
            pos = key.end(1)  # the separator may start the next key's boundary
            continue
        value = _VALUE.match(text, key.end())
        if value is None:
            if (exempt := _EXEMPT.match(text, key.end())) is not None:
                pos = exempt.end()  # keys inside an exempt value (`${A_TOKEN:-}`) are not keys
            continue
        parts += [text[done : value.start(2)], _masked(value.group(2))]
        pos = done = value.end()
    return "".join(parts) + text[done:] if parts else text


def _mask_pem(text: str) -> tuple[str, bool]:
    """Mask whole private-key blocks in one linear pass; True when a block is left open."""
    if "PRIVATE KEY" not in text:
        return text, False
    parts: list[str] = []
    pos = 0
    while (begin := _PEM_BEGIN.search(text, pos)) is not None:
        end = _PEM_END.search(text, begin.end())
        if end is None:
            parts.append(text[pos : begin.start()])
            parts.append(REDACTED)
            return "".join(parts), True
        parts.append(text[pos : begin.start()])
        parts.append(REDACTED)
        pos = end.end()
    parts.append(text[pos:])
    return "".join(parts), False


def _unquote(value: str) -> str:
    """Strip quotes (keeping any `#` inside); for unquoted values drop an inline comment."""
    if value[:1] in ("'", '"'):
        quote = value[0]
        if len(value) >= 2 and value[-1] == quote:
            return value[1:-1]
        closing = value.find(quote, 1)
        if closing != -1:
            return value[1:closing]
    return _INLINE_COMMENT.sub("", value).rstrip()


def _parse_env_file(path: Path, *, only_secret_keys: bool, warn: bool = False) -> list[str]:
    """Values of simple `KEY=VALUE` lines; unreadable or missing files yield nothing.

    `warn` logs the path (never content) when the file cannot be read: for files the user
    configured, silence would mean secrets quietly left unmasked.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        if warn:
            logging.getLogger("dioxide").warning(
                "redact_env_files entry %s cannot be read (%s); its values are NOT masked",
                path,
                type(exc).__name__,
            )
        return []
    values: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").lstrip()
        key, sep, value = line.partition("=")
        if not sep or (only_secret_keys and not _SECRET_ENV_KEY.search(key)):
            continue
        values.append(_unquote(value.strip()))
    return values


def _compile_values(values: Iterable[str]) -> re.Pattern[str] | None:
    variants: set[str] = set()
    for value in values:
        if len(value) < MIN_ENV_VALUE_LEN:
            continue
        variants.add(value)
        variants.add(json.dumps(value)[1:-1])
        variants.add(json.dumps(value, ensure_ascii=False)[1:-1])
    if not variants:
        return None
    # Longest first so a value that prefixes another never leaves a tail behind.
    ordered = sorted(variants, key=lambda v: (-len(v), v))
    return re.compile("|".join(re.escape(v) for v in ordered))


_NOT_JSON = object()
MAX_JSON_DEPTH = 8  # JSON documents nested inside JSON strings
MAX_LEVEL = 200  # container nesting; deeper subtrees are masked outright


def _loads(text: str) -> Any:
    try:
        return json.loads(text)
    except (ValueError, RecursionError):
        return _NOT_JSON


def _dumps(obj: Any) -> str:
    out = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    try:
        out.encode("utf-8")
    except UnicodeEncodeError:  # lone surrogates from `\\ud800` escapes: keep them escaped
        out = json.dumps(obj, ensure_ascii=True, separators=(",", ":"))
    return out


def _mask_leaves(obj: Any, level: int) -> Any:
    """Value under a sensitive key: every non-empty string leaf is masked, the rest is kept."""
    if isinstance(obj, str):
        return REDACTED if obj else obj
    if isinstance(obj, dict | list | tuple) and level >= MAX_LEVEL:
        return REDACTED
    if isinstance(obj, dict):
        return {key: _mask_leaves(value, level + 1) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_mask_leaves(item, level + 1) for item in obj]
    if isinstance(obj, tuple):
        return tuple(_mask_leaves(item, level + 1) for item in obj)
    return obj


def mask_leaves(obj: Any) -> Any:
    """Every non-empty string leaf masked; numbers, booleans and null kept."""
    return _mask_leaves(obj, 0)


class Redactor:
    """Masks secrets as `****`. Line based, so redaction precedes any paging or chunking.

    Contract:
    - A line that is a JSON object/array is redacted structurally and re-encoded compactly on one
      line, so the served line is equivalent to, but not byte-identical with, the file's line.
      Strings that themselves hold JSON are redacted recursively (to a bounded depth).
    - Any other line is redacted as plain text (patterns, env values, Bearer, key/value).
    - An instance keeps PEM state across `redact_line` calls (private-key blocks span lines), so
      it must serve one stream at a time; call `reset()` between streams. `pem_open` exposes that
      state and `clone(pem_open)` makes a per-stream instance (sharing the compiled patterns)
      that resumes a stream from a saved state, e.g. a log cursor.

    Documented limits: in plain text an unquoted value containing whitespace is masked only up to
    the whitespace; a purely numeric secret under a sensitive key is not masked.
    """

    def __init__(self, env_values: Iterable[str] = ()) -> None:
        self._values = frozenset(env_values)
        self._env = _compile_values(self._values)
        self._in_pem = False

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        values = _parse_env_file(settings.home / ".env", only_secret_keys=True)
        # Files the user listed explicitly are secret stores: every value counts.
        for path in settings.redact_env_files:
            values.extend(_parse_env_file(path, only_secret_keys=False, warn=True))
        values.extend(childenv.secret_values(settings.child_env_file))  # credentials, all of it
        values.append(settings.auth_token)
        return cls(values)

    def add_values(self, values: Iterable[str]) -> None:
        """Also mask ``values`` from now on (e.g. a rotated credential read at a launch); values
        added earlier stay masked. Clones made afterwards share the new set."""
        merged = self._values | frozenset(values)
        if merged != self._values:
            self._env = _compile_values(merged)
            self._values = merged

    def reset(self) -> None:
        self._in_pem = False

    @property
    def pem_open(self) -> bool:
        """True while the stream is inside a private-key block (every line is masked)."""
        return self._in_pem

    def clone(self, pem_open: bool = False) -> Self:
        """A fresh instance for another stream: same secrets, no file re-read, given PEM state."""
        twin = type(self).__new__(type(self))
        twin._values = self._values
        twin._env = self._env
        twin._in_pem = pem_open
        return twin

    def redact_line(self, line: str) -> str:
        if self._in_pem:
            if _PEM_END.search(line):
                self._in_pem = False
            return REDACTED
        stripped = line.strip()
        if stripped[:1] in ("{", "[", '"'):
            parsed = _loads(stripped)
            if parsed is not _NOT_JSON:
                try:
                    return _dumps(self._walk(parsed, 0, 0))
                except RecursionError:  # pathologically deep document: fall back to text
                    pass
        text, opened = _mask_pem(line)
        if opened:
            self._in_pem = True
            return REDACTED
        return self._redact_plain(text)

    def redact_text(self, text: str) -> str:
        self.reset()
        try:
            return "\n".join(self.redact_line(line) for line in text.split("\n"))
        finally:
            self.reset()

    def redact_json(self, obj: Any) -> Any:
        """Deep copy with every string redacted; values under sensitive keys are masked."""
        return self._walk(obj, 0, 0)

    def _walk(self, obj: Any, depth: int, level: int) -> Any:
        """`depth` counts JSON-in-string nesting, `level` counts containers on the way down."""
        if isinstance(obj, str):
            return self._redact_string(obj, depth, level)
        if isinstance(obj, dict | list | tuple) and level >= MAX_LEVEL:
            return REDACTED
        if isinstance(obj, dict):
            return {
                key: _mask_leaves(value, level + 1)
                if isinstance(key, str) and is_sensitive_key(key)
                else self._walk(value, depth, level + 1)
                for key, value in obj.items()
            }
        if isinstance(obj, list):
            return [self._walk(item, depth, level + 1) for item in obj]
        if isinstance(obj, tuple):
            return tuple(self._walk(item, depth, level + 1) for item in obj)
        return obj

    def _redact_string(self, value: str, depth: int, level: int) -> str:
        """One decoded string value: embedded JSON is walked, anything else is plain text."""
        stripped = value.strip()
        if depth < MAX_JSON_DEPTH and stripped[:1] in ("{", "["):
            parsed = _loads(stripped)
            if parsed is not _NOT_JSON:
                return _dumps(self._walk(parsed, depth + 1, level))
        text, opened = _mask_pem(value)
        return REDACTED if opened else self._redact_plain(text)

    def _redact_plain(self, text: str) -> str:
        if self._env is not None:
            text = self._env.sub(REDACTED, text)
        text = _TOKENS.sub(REDACTED, text)
        text = _BEARER.sub(rf"\1{REDACTED}", text)
        text = _COOKIE_HEADER.sub(rf"\1{REDACTED}", text)
        text = _CURL_USER.sub(rf"\1{REDACTED}", text)
        text = _URL_USERINFO.sub(rf"\1{REDACTED}@", text)
        return _mask_key_values(text)
