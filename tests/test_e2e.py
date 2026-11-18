"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def breaker_status(proxy_url: str) -> dict:
    with client() as c:
        r = c.get(f"{proxy_url}/origin/status")
        r.raise_for_status()
        return r.json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024, fail_status: int | None = None) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
                "fail_status": fail_status,
            },
        )
        r.raise_for_status()


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


@pytest.fixture()
def dead_proxy(tmp_path):
    """A proxy whose origin address has no listener (connection refused)."""
    runner = ProxyRunner(free_port(), tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


# --------------------------------------------------------------------- #
# origin circuit breaker                                                  #
# --------------------------------------------------------------------- #

def test_breaker_opens_after_two_5xx_and_short_circuits_misses(proxy, origin):
    set_behavior(origin.url, fail_status=503)
    origin_reset(origin.url)
    try:
        with client() as c:
            r1 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r1.status_code == 503 and r2.status_code == 503
            assert origin_stats(origin.url)["total_hits"] == 2
            st = breaker_status(proxy.url)
            assert st["state"] == "open"
            assert st["consecutive_failures"] == 2

            # During cooldown a new miss is refused without touching origin.
            r3 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r3.status_code == 503
            assert int(r3.headers["Retry-After"]) >= 1
            assert origin_stats(origin.url)["total_hits"] == 2
    finally:
        set_behavior(origin.url)


def test_breaker_half_open_single_probe_then_recovers(proxy, origin):
    set_behavior(origin.url, fail_status=500)
    origin_reset(origin.url)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] == "open"
    finally:
        set_behavior(origin.url)  # origin healthy again

    time.sleep(5.2)  # let the 5 s cooldown elapse

    # Origin healthy: the first miss after cooldown is the probe and succeeds.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    st = breaker_status(proxy.url)
    assert st["state"] == "closed" and st["consecutive_failures"] == 0
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 3  # 2 fails + probe
    assert download_bytes(proxy.url, PKG_SMALL) == body  # served from cache


def test_breaker_half_open_probe_blocks_other_fills(proxy, origin):
    # Two failures on one path open the breaker; reset counters afterwards so
    # the assertion below counts only the probe phase.
    set_behavior(origin.url, fail_status=502)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] == "open"
    finally:
        set_behavior(origin.url)
    origin_reset(origin.url)

    time.sleep(5.2)

    # Make the probe slow so it is still in flight when we race other paths.
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)  # ~0.8s fill
    try:
        result: dict = {}

        def probe():
            result["body"] = download_bytes(proxy.url, PKG_SMALL)

        t = threading.Thread(target=probe)
        t.start()
        time.sleep(0.3)  # probe has started, still filling
        with client() as c:
            # A different uncached path must be refused while probing.
            r_other = c.get(f"{proxy.url}/packages/{PKG_BIG}")
            # The probe's own path joins the running fill normally.
            r_same = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert r_other.status_code == 503
        assert "x-cache" not in r_other.headers  # refused, not served
        t.join(30)
        assert r_same.status_code == 200 and len(r_same.content) == MIB
        assert sha256(result["body"]) == sha256(r_same.content)
        assert breaker_status(proxy.url)["state"] == "closed"
        # other path never fetched; shared probe path fetched exactly once more
        stats = origin_stats(origin.url)
        assert PKG_BIG not in stats["paths"]
        assert stats["paths"][PKG_SMALL] == 1
    finally:
        set_behavior(origin.url)


def test_breaker_failed_probe_reopens_cooldown(proxy, origin):
    set_behavior(origin.url, fail_status=503)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        time.sleep(5.2)
        # Still failing: the probe itself fails and the breaker re-opens.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r.status_code == 503
            st = breaker_status(proxy.url)
            assert st["state"] == "open"
            assert st["retry_after_s"] > 4.0
            hits = origin_stats(origin.url)["total_hits"]
            # Immediately after the failed probe, misses are refused again.
            r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r2.status_code == 503
            assert origin_stats(origin.url)["total_hits"] == hits
    finally:
        set_behavior(origin.url)


def test_breaker_keeps_serving_complete_cache_and_running_fills(proxy, origin):
    # Warm the cache for the big package, then take the origin down.
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    set_behavior(origin.url, fail_status=503)
    origin_reset(origin.url)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] == "open"
            assert origin_stats(origin.url)["total_hits"] == 2
            # Cached complete object is still a normal HIT.
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
            assert r.status_code == 200 and r.headers["x-cache"] == "HIT"
            assert sha256(r.content) == sha256(body)
            assert origin_stats(origin.url)["total_hits"] == 2
    finally:
        set_behavior(origin.url)


def test_breaker_404_counts_as_origin_available(proxy, origin):
    set_behavior(origin.url, fail_status=503)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] == "open"
    finally:
        set_behavior(origin.url)

    time.sleep(5.2)
    # The probe hits a path the origin does not have: 404, origin is reachable.
    with client() as c:
        r = c.get(f"{proxy.url}/packages/nope-probe.bin")
        assert r.status_code == 404
        st = breaker_status(proxy.url)
        assert st["state"] == "closed"
        # Normal operation resumed: a real package fills again.
        assert len(c.get(f"{proxy.url}/packages/{PKG_SMALL}").content) == MIB
        # 404 was not cached as an object.
        assert PKG_SMALL in c.get(f"{proxy.url}/cache/status").json()
        assert "nope-probe.bin" not in c.get(f"{proxy.url}/cache/status").json()


def test_breaker_opens_on_connection_refused(dead_proxy):
    with client() as c:
        r1 = c.get(f"{dead_proxy.url}/packages/{PKG_SMALL}")
        r2 = c.get(f"{dead_proxy.url}/packages/{PKG_SMALL}")
        assert r1.status_code == 502 and r2.status_code == 502
        st = breaker_status(dead_proxy.url)
        assert st["state"] == "open"
        # Third request is refused locally with 503, no origin attempt possible.
        r3 = c.get(f"{dead_proxy.url}/packages/{PKG_SMALL}")
        assert r3.status_code == 503
        assert int(r3.headers["Retry-After"]) >= 1


def test_breaker_opens_on_transfer_interruption(proxy, origin):
    set_behavior(origin.url, delay_ms=5, chunk_size=64 * 1024, truncate=100_000)
    origin_reset(origin.url)
    try:
        with client() as c:
            with pytest.raises(httpx.HTTPError):
                c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            with pytest.raises(httpx.HTTPError):
                c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] == "open"
            assert origin_stats(origin.url)["total_hits"] == 2
            r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert r.status_code == 503
            assert origin_stats(origin.url)["total_hits"] == 2
    finally:
        set_behavior(origin.url)


def test_breaker_counts_only_consecutive_failures(proxy, origin):
    # One failure, then a success, then one failure: must not open.
    set_behavior(origin.url, fail_status=503)
    try:
        with client() as c:
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    finally:
        set_behavior(origin.url)
    assert len(download_bytes(proxy.url, PKG_SMALL)) == MIB  # success resets
    set_behavior(origin.url, fail_status=503)
    try:
        with client() as c:
            with client() as c2:
                c2.post(f"{proxy.url}/purge/{PKG_SMALL}")
            c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert breaker_status(proxy.url)["state"] != "open"
    finally:
        set_behavior(origin.url)


def test_breaker_open_does_not_disturb_inflight_fill_or_its_readers(proxy, origin):
    # Start a slow fill on the big package first (~2.6s at 20ms/chunk).
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    origin_reset(origin.url)
    result: dict = {}
    try:
        def slow_reader():
            with client(timeout_read=120).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(256 * 1024):
                    got += len(chunk)
                    if got >= 4 * 256 * 1024:
                        time.sleep(0.3)  # still reading when breaker opens
                result["got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.5)  # fill is in flight

        # Two 5xx on a different path open the breaker while the fill runs.
        set_behavior(origin.url, fail_status=503)
        with client() as c:
            assert c.get(f"{proxy.url}/packages/{PKG_SMALL}").status_code == 503
            assert c.get(f"{proxy.url}/packages/{PKG_SMALL}").status_code == 503
            assert breaker_status(proxy.url)["state"] == "open"
            # A third miss with no in-flight fill is refused locally.
            assert c.get(f"{proxy.url}/packages/{PKG_SMALL}").status_code == 503
            # A miss that can join the already-running fill still joins it.
            joined = c.get(f"{proxy.url}/packages/{PKG_BIG}", timeout=120)
            assert joined.status_code == 200 and len(joined.content) == 32 * MIB

        t.join(60)
        assert result.get("got") == 32 * MIB  # reader finished unaffected
        # The in-flight fill completed successfully and restored normal service.
        st = breaker_status(proxy.url)
        assert st["state"] == "closed"
        assert len(download_bytes(proxy.url, PKG_BIG)) == 32 * MIB
    finally:
        set_behavior(origin.url)
