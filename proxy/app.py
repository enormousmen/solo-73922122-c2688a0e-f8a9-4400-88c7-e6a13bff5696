"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
* Origin circuit breaker: two consecutive fills that fail because of a
  connection error, an origin 5xx, or a transfer interruption open a 5 s
  cooldown. While open, a miss that has no in-flight fill to join is refused
  immediately with 503; complete cache hits and already-running fills are
  unaffected. After the cooldown exactly one miss becomes a probe; other new
  fills are refused until it resolves. A complete fill or any non-5xx origin
  response (notably 404) closes the breaker; a failed probe re-opens it.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read
BREAKER_THRESHOLD = 2
BREAKER_COOLDOWN = 5.0  # seconds

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class OriginCoolingDown(Exception):
    """Raised when the origin breaker refuses a new fill."""

    def __init__(self, retry_after: float, probing: bool) -> None:
        self.retry_after = max(0.0, retry_after)
        self.probing = probing
        super().__init__("origin circuit breaker is open")


def _is_origin_transport_failure(exc: BaseException) -> bool:
    """Connection failure or transfer interruption (not a local disk error).

    aiohttp wraps connector/DNS/socket errors in ``ClientConnectionError`` and
    truncated or malformed payloads in ``ClientPayloadError``; our explicit
    short-read check raises the latter too. Read timeouts arrive as
    ``asyncio.TimeoutError``.
    """
    return isinstance(
        exc,
        (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError),
    )


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None
        self.epoch = 0  # breaker epoch this generation's fill was admitted in

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class OriginBreaker:
    """Origin-wide failure isolation shared by every cache path.

    closed --(2 consecutive qualifying failures)--> open
    open   --(cooldown elapsed, next miss)--> half_open (that miss is the probe)
    half_open --(complete / non-5xx origin response)--> closed
    half_open --(qualifying failure)--> open (fresh cooldown)

    Only a new fill consults the breaker: cache hits and readers attached to
    an already-running fill never touch it. Each admitted fill carries the
    breaker ``epoch`` it started in, so a failure that only surfaces after
    the breaker has already moved on (e.g. a fill that started before the
    cooldown) is not counted as a fresh consecutive failure.
    """

    def __init__(self, threshold: int = BREAKER_THRESHOLD,
                 cooldown: float = BREAKER_COOLDOWN) -> None:
        self.threshold = threshold
        self.cooldown = cooldown
        self.state = "closed"          # closed | open | half_open
        self.epoch = 0                 # bumped on every state transition
        self.consecutive_failures = 0
        self.opened_at = 0.0
        self.probe_key: str | None = None
        self.fills_in_flight = 0
        self.last_error: str | None = None

    def _retry_after(self, now: float) -> float:
        return max(0.0, self.opened_at + self.cooldown - now)

    def check_miss(self, key: str, now: float) -> int:
        """Admit one new fill for ``key`` or raise ``OriginCoolingDown``.

        Returns the epoch the fill is admitted in. Caller holds
        ``Cache.lock`` and already knows there is no in-flight fill for this
        key to join.
        """
        if self.state == "closed":
            return self.epoch
        if self.state == "open":
            if now >= self.opened_at + self.cooldown:
                self.state = "half_open"
                self.consecutive_failures = 0
                self.probe_key = key
                self.epoch += 1
                return self.epoch
            raise OriginCoolingDown(self._retry_after(now), probing=False)
        # half_open: the single probe slot is taken until it resolves.
        raise OriginCoolingDown(self.cooldown, probing=True)

    def on_fill_started(self) -> None:
        self.fills_in_flight += 1

    def on_fill_succeeded(self, key: str, epoch: int) -> None:
        # Any complete fill, probe or not, proves the origin is healthy.
        self.fills_in_flight = max(0, self.fills_in_flight - 1)
        self._close()

    def on_origin_answered(self, key: str, epoch: int) -> None:
        """Origin produced a non-5xx response (e.g. 404): it is reachable.

        404 is not cached, but it restores normal operation (and ends a probe).
        """
        self.fills_in_flight = max(0, self.fills_in_flight - 1)
        self._close()

    def on_fill_cancelled(self, key: str, epoch: int) -> None:
        """A cancelled fill says nothing about origin health, but a cancelled
        probe must not leave the breaker wedged in half_open."""
        self.fills_in_flight = max(0, self.fills_in_flight - 1)
        if key == self.probe_key:
            self._open(time.monotonic())

    def on_local_error(self, key: str, epoch: int) -> None:
        """Failure not attributable to the origin (e.g. local disk error)."""
        self.fills_in_flight = max(0, self.fills_in_flight - 1)
        if key == self.probe_key:
            self._open(time.monotonic())

    def on_fill_failed(self, key: str, epoch: int, *, error: str) -> None:
        """Connection failure, origin 5xx, or transfer interruption."""
        self.fills_in_flight = max(0, self.fills_in_flight - 1)
        if epoch != self.epoch:
            return  # stale: the breaker already moved on
        self.last_error = error
        if key == self.probe_key:
            # A failed probe re-enters cooldown with a fresh window.
            self.consecutive_failures += 1
            self._open(time.monotonic())
            return
        if self.state != "closed":
            return  # already isolated; do not extend the cooldown
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.threshold:
            self._open(time.monotonic())

    def _close(self) -> None:
        if self.state != "closed" or self.probe_key is not None:
            self.epoch += 1  # probes/fills admitted before this are stale
        self.consecutive_failures = 0
        self.probe_key = None
        self.last_error = None
        self.state = "closed"

    def _open(self, now: float) -> None:
        if self.state != "open":
            self.epoch += 1
        self.state = "open"
        self.opened_at = now
        self.probe_key = None

    def info(self) -> dict:
        return {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "threshold": self.threshold,
            "cooldown_s": self.cooldown,
            "retry_after_s": round(self._retry_after(time.monotonic()), 3)
            if self.state == "open" else 0.0,
            "probe_key": self.probe_key,
            "fills_in_flight": self.fills_in_flight,
            "last_error": self.last_error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None
        self.breaker = OriginBreaker()

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Entry:
        """Return the current generation for key, starting a fill if needed.

        Raises ``OriginCoolingDown`` when the origin breaker refuses a new
        fill. Concurrent misses join an existing fill without consulting the
        breaker, and complete cached entries are always served.
        """
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                # No fill to join: the breaker decides whether we may probe or
                # start a new fill right now.
                epoch = self.breaker.check_miss(key, time.monotonic())
                e = Entry(key)
                e.epoch = epoch
                self.entries[key] = e
                self.breaker.on_fill_started()
                e.fill_task = asyncio.create_task(self._fill(e))
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader."""
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over."""
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # never keep a half package
        elif e.purged:  # complete or closed
            _unlink_quiet(e.part_path)
            _unlink_quiet(e.final_path)
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    # A 5xx is an origin outage; any other status (notably
                    # 404) proves the origin is reachable and answer-capable.
                    if resp.status >= 500:
                        self.breaker.on_fill_failed(e.key, e.epoch, error=e.error)
                    else:
                        self.breaker.on_origin_answered(e.key, e.epoch)
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
                async with self.lock:
                    e.size = written if e.size == -1 else e.size
                    e.meta["size"] = written
                    if e.purged:
                        # Purged mid-fill: keep the part file for current
                        # readers only; never register this generation.
                        e.state = "closed"
                    else:
                        os.replace(e.part_path, e.final_path)  # atomic publish
                        try:
                            e.meta_path.write_text(json.dumps(e.meta))
                        except OSError:
                            pass
                        e.state = "complete"
                    self.breaker.on_fill_succeeded(e.key, e.epoch)
                    async with e.cond:
                        e.cond.notify_all()
                    self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            # Shutdown (or explicit cancel): not evidence about the origin.
            self.breaker.on_fill_cancelled(e.key, e.epoch)
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            if _is_origin_transport_failure(exc):
                self.breaker.on_fill_failed(e.key, e.epoch, error=e.error)
            else:
                self.breaker.on_local_error(e.key, e.epoch)
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await self.detach(e)


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    try:
        entry = await cache.attach(key)
    except OriginCoolingDown as exc:
        reason = "probe in progress" if exc.probing else "origin cooling down"
        return PlainTextResponse(
            f"origin temporarily unavailable: {reason}",
            status_code=503,
            headers={"Retry-After": str(max(1, math.ceil(exc.retry_after)))},
        )
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        await cache.detach(entry)
        raise

    if entry.state == "failed":
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}


@app.get("/origin/status")
async def origin_status():
    """Origin circuit-breaker state (cooldown, probe slot, counters)."""
    async with cache.lock:
        return cache.breaker.info()
