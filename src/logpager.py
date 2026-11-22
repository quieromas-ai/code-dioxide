"""Line pager over growing log streams: signed cursors, per-line redaction, page caps.

A source is anything that opens as a binary stream of newline-terminated lines (a file, or a
virtual byte stream such as the task's engine events). Only complete lines are served; a partial
last line is neither served nor advanced past, unless the source is ``final`` (its writer is
gone), when it is served and the cursor moves to the end of the file. Every line is redacted (by
a per-source ``Redactor`` restored from the cursor's PEM flag) before it counts toward the caps.

Cursor: ``base64url(payload) + "." + base64url(HMAC-SHA256(key, payload))`` with payload
``{"v": 1, "t": scope, "o": {source: byte_offset}, "p": {source: pem_open}, "i": {source: inode}}``
(compact: defaults left out; inodes only for rotating sources).
The key is per install (``data/cursor.key``), so a client can neither forge an offset or a PEM
flag nor reuse a cursor for another scope (task). As defence in depth, an offset that is not at a
line boundary, past the end, or (rotating sources) into a file with another inode restarts that
source at 0.
"""

import base64
import binascii
import hashlib
import heapq
import hmac
import io
import json
import os
import secrets
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from src.redact import Redactor

CURSOR_VERSION = 1
# Only bounds the base64/HMAC work: the signature is verified before the payload is parsed.
MAX_CURSOR_CHARS = 262144
KEY_BYTES = 32
CHUNK_BYTES = 1 << 16
TRUNCATED = "…[truncated {n} chars]"

Opener = Callable[[], IO[bytes] | None]  # None: the source has no data (yet)
Stamp = Callable[[bytes], float | None]
Keep = Callable[[bytes], bool]

_keys: dict[Path, bytes] = {}
_keys_lock = threading.Lock()


class BadCursor(ValueError):
    pass


@dataclass(frozen=True)
class Source:
    name: str
    open: Opener
    stamp: Stamp | None = None  # epoch seconds of a raw line, when the line carries one
    keep: Keep | None = None  # lines it rejects are consumed but not served
    final: bool = False  # the writer is gone: a trailing partial line is served too
    rotating: bool = False  # renamed away and recreated (server.log): the cursor keeps its inode


@dataclass
class Cursor:
    offsets: dict[str, int] = field(default_factory=dict)
    pem: dict[str, bool] = field(default_factory=dict)
    inodes: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    entries: list[dict[str, Any]]
    cursor: Cursor
    exhausted: bool  # no line left to serve in any source right now


def load_key(path: Path) -> bytes:
    """The install's cursor key: exactly ``KEY_BYTES`` bytes at 0600, else (re)created
    atomically; then cached. Any key that cannot be read or created is a hard error: the logs
    API never runs on an empty or in-memory key."""
    with _keys_lock:
        if path in _keys:
            return _keys[path]
        try:
            key = _read_key(path)
            if key is None or len(key) != KEY_BYTES:
                key = _create_key(path, replace=key is not None)
            _restrict(path)
        except OSError as exc:
            raise RuntimeError(f"cursor key {path}: {exc.strerror or exc}") from exc
        _keys[path] = key
        return key


def _read_key(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def _restrict(path: Path) -> None:
    if os.stat(path).st_mode & 0o077:
        os.chmod(path, 0o600)


def _create_key(path: Path, replace: bool) -> bytes:
    """A fresh key, durable (fsync) before it becomes visible. ``replace``: the file holds a
    key of the wrong length (e.g. empty after a power loss); old cursors then become invalid,
    which is safe."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        key = secrets.token_bytes(KEY_BYTES)
        with os.fdopen(fd, "wb") as fh:
            fh.write(key)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o600)
        if not replace:
            try:
                os.link(tmp, path)  # atomic create-if-absent
            except FileExistsError:
                pass  # another process created it: use it only when it is a valid key
            current = _read_key(path)
            if current is not None and len(current) == KEY_BYTES:
                return current
        os.replace(tmp, path)
        return key
    finally:
        with suppress(FileNotFoundError):
            os.unlink(tmp)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode()


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), altchars=b"-_", validate=True)


def _mac(key: bytes, payload: bytes) -> bytes:
    return hmac.new(key, payload, hashlib.sha256).digest()


class CursorTooLarge(ValueError):
    pass


def encode_cursor(cursor: Cursor, key: bytes, scope: str) -> str:
    """Compact: a source at offset 0 with no open PEM block is left out (the defaults), and
    so are false PEM flags and the inodes of sources left out. Never above ``MAX_CURSOR_CHARS``:
    ``CursorTooLarge`` instead of a cursor that ``decode_cursor`` would reject."""
    pem = {name: True for name, flag in cursor.pem.items() if flag}
    offsets = {name: off for name, off in cursor.offsets.items() if off or name in pem}
    inodes = {name: ino for name, ino in cursor.inodes.items() if name in offsets}
    doc: dict[str, Any] = {"v": CURSOR_VERSION, "t": scope, "o": offsets}
    if pem:
        doc["p"] = pem
    if inodes:
        doc["i"] = inodes
    payload = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()
    raw = f"{_b64(payload)}.{_b64(_mac(key, payload))}"
    if len(raw) > MAX_CURSOR_CHARS:
        raise CursorTooLarge(f"cursor of {len(raw)} chars exceeds {MAX_CURSOR_CHARS}")
    return raw


def decode_cursor(raw: str | None, key: bytes, scope: str) -> Cursor:
    """Verify (signature, then scope and shape) and decode; any failure raises ``BadCursor``."""
    if not raw:
        return Cursor()
    if len(raw) > MAX_CURSOR_CHARS or raw.count(".") != 1:
        raise BadCursor("bad cursor")
    body, sig = raw.split(".")
    try:
        payload, mac = _unb64(body), _unb64(sig)
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise BadCursor("bad cursor") from exc
    if not hmac.compare_digest(mac, _mac(key, payload)):
        raise BadCursor("bad cursor")
    try:
        doc = json.loads(payload)
    except (ValueError, RecursionError) as exc:
        raise BadCursor("bad cursor") from exc
    if not isinstance(doc, dict) or type(doc.get("v")) is not int or doc["v"] != CURSOR_VERSION:
        raise BadCursor("bad cursor")
    offsets, pem, inodes = doc.get("o"), doc.get("p", {}), doc.get("i", {})
    if doc.get("t") != scope:
        raise BadCursor("bad cursor")
    if not isinstance(offsets, dict) or not isinstance(pem, dict) or not isinstance(inodes, dict):
        raise BadCursor("bad cursor")
    for value in [*offsets.values(), *inodes.values()]:
        if type(value) is not int or value < 0:
            raise BadCursor("bad cursor")
    if not all(isinstance(value, bool) for value in pem.values()):
        raise BadCursor("bad cursor")
    return Cursor(dict(offsets), dict(pem), dict(inodes))


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat()


def _inode(fh: IO[bytes]) -> int | None:
    try:
        return os.fstat(fh.fileno()).st_ino
    except (OSError, AttributeError, io.UnsupportedOperation):
        return None  # a virtual (in-memory) source


def _valid_start(fh: IO[bytes], offset: int, size: int, final: bool) -> bool:
    """Offset 0, a line boundary, or (for a final source) the end of the file."""
    if offset == 0 or (final and offset == size):
        return True
    if offset > size:
        return False
    fh.seek(offset - 1)
    return fh.read(1) == b"\n"


def iter_lines(fh: IO[bytes], offset: int, final: bool = False) -> Iterator[tuple[bytes, int]]:
    """``(line without its newline, offset after it)`` for each complete line from ``offset``;
    with ``final`` also a trailing partial line (its end is the end of the file)."""
    fh.seek(offset)
    buf = bytearray()
    pos = offset
    while chunk := fh.read(CHUNK_BYTES):
        buf += chunk
        start = 0
        while (nl := buf.find(b"\n", start)) >= 0:
            pos += nl + 1 - start
            yield bytes(buf[start:nl]), pos
            start = nl + 1
        del buf[:start]
    if final and buf:
        yield bytes(buf), pos + len(buf)


class _Budget:
    """Page caps: redacted chars (or UTF-8 bytes when ``full``) and entry count."""

    def __init__(self, cap: int, limit: int, full: bool) -> None:
        self.cap, self.limit, self.full = cap, max(1, limit), full
        self.used = 0
        self.count = 0

    def take(self, text: str) -> str | None:
        """The text to serve (truncated when needed), or None when the page is full."""
        if self.count >= self.limit:
            return None
        if not self.full and len(text) > self.cap:
            # The marker length is bounded by the one for n = len(text): the result fits the cap.
            keep = max(0, self.cap - len(TRUNCATED.format(n=len(text))))
            text = text[:keep] + TRUNCATED.format(n=len(text) - keep)
        size = len(text.encode("utf-8", "replace")) if self.full else len(text)
        if self.count and self.used + size > self.cap:
            return None
        self.used += size
        self.count += 1
        return text


@dataclass
class _Head:
    at: float | None
    text: str
    end: int
    pem_after: bool


class _Feed:
    """One source: a committed position (offset + PEM state) and the next servable line."""

    def __init__(self, source: Source, fh: IO[bytes] | None, cursor: Cursor, base: Redactor):
        self.source = source
        self.offset = cursor.offsets.get(source.name, 0)
        self.pem = cursor.pem.get(source.name, False)
        self.inode: int | None = None
        self.head: _Head | None = None
        self._lines: Iterator[tuple[bytes, int]] = iter(())
        if fh is not None:
            size = fh.seek(0, os.SEEK_END)
            # Only a rotating source is tracked by inode. A run-store copy replaces the live
            # file with the same bytes (and a new inode): the boundary check suffices there.
            self.inode = _inode(fh) if source.rotating else None
            saved = cursor.inodes.get(source.name)
            moved = saved is not None and self.inode is not None and saved != self.inode
            if moved or not _valid_start(fh, self.offset, size, source.final):
                self.offset, self.pem = 0, False  # rotated, replaced or truncated: start over
            self._lines = iter_lines(fh, self.offset, source.final)
        self.redactor = base.clone(self.pem)

    def advance(self, timed_only: bool) -> None:
        self.head = None
        for raw, end in self._lines:
            text = self.redactor.redact_line(raw.decode("utf-8", "replace"))
            source = self.source
            at = source.stamp(raw) if source.stamp is not None else None
            if (source.keep is not None and not source.keep(raw)) or (timed_only and at is None):
                self.offset, self.pem = end, self.redactor.pem_open  # consumed, not served
                continue
            self.head = _Head(at, text, end, self.redactor.pem_open)
            return

    def commit(self) -> None:
        assert self.head is not None
        self.offset, self.pem = self.head.end, self.head.pem_after


def _key(feed: _Feed, merged: bool) -> float:
    at = feed.head.at if feed.head is not None else None
    return at if merged and at is not None else 0.0


def read_page(
    sources: list[Source],
    cursor: Cursor,
    base: Redactor,
    *,
    cap: int,
    limit: int,
    full: bool,
    merged: bool = False,
) -> Page:
    """One page. ``merged``: only timestamped lines, ordered by time across the sources."""
    budget = _Budget(cap, limit, full)
    entries: list[dict[str, Any]] = []
    with ExitStack() as stack:
        feeds: list[_Feed] = []
        for source in sources:
            fh = source.open()
            if fh is not None:
                stack.enter_context(fh)
            feeds.append(_Feed(source, fh, cursor, base))
        heap: list[tuple[float, int]] = []
        for i, feed in enumerate(feeds):
            feed.advance(merged)
            if feed.head is not None:
                heapq.heappush(heap, (_key(feed, merged), i))
        while heap:
            _, i = heap[0]
            feed = feeds[i]
            assert feed.head is not None
            text = budget.take(feed.head.text)
            if text is None:
                break
            entry: dict[str, Any] = {"source": feed.source.name, "line": text}
            if feed.head.at is not None:
                entry["at"] = iso(feed.head.at)
            entries.append(entry)
            feed.commit()
            feed.advance(merged)
            if feed.head is None:
                heapq.heappop(heap)
            else:
                heapq.heapreplace(heap, (_key(feed, merged), i))
        exhausted = not heap
    out = Cursor(dict(cursor.offsets), dict(cursor.pem), dict(cursor.inodes))
    for feed in feeds:
        out.offsets[feed.source.name] = feed.offset
        out.pem[feed.source.name] = feed.pem
        if feed.inode is not None:
            out.inodes[feed.source.name] = feed.inode
        else:
            out.inodes.pop(feed.source.name, None)
    return Page(entries, out, exhausted)
