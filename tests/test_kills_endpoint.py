import asyncio
import time

import app.routers.systems as systems
from app.binary_encoder import encode_kills_binary


def _columns(ids):
    n = len(ids)
    return {
        "killmail_ids": list(ids),
        "killmail_times": [1000 + i for i in range(n)],
        "x": [0] * n,
        "y": [0] * n,
        "z": [0] * n,
        "ship_types": [587] * n,
    }


class _FakeBinaryCache:
    """In-process stand-in for kills_binary_cache with the Task 4 shape."""

    def __init__(self):
        self.store: dict[str, tuple[int, bool, bytes]] = {}
        self.touched = False

    async def get(self, params):
        self.touched = True
        key = str(sorted(params.items()))
        return self.store.get(key)

    async def set(self, params, value, fresh_to):
        self.touched = True
        key = str(sorted(params.items()))
        res = (fresh_to, False, value)
        self.store[key] = res
        return res


def _patch(monkeypatch, *, latest, fake_cache, fetch_counter=None, latest_seq=None):
    seq = list(latest_seq or [])

    async def fake_latest(_sid):
        if seq:
            return seq.pop(0)
        return latest

    async def fake_fetch(solar_system_id, since=None):
        if fetch_counter is not None:
            fetch_counter.append(1)
        return _columns([1, 2, 3])

    monkeypatch.setattr(systems, "get_system_latest", fake_latest)
    monkeypatch.setattr(systems, "fetch_raw_kills", fake_fetch)
    monkeypatch.setattr(systems, "kills_binary_cache", fake_cache)


def test_full_path_builds_once_and_sets_headers(monkeypatch):
    cache = _FakeBinaryCache()
    _patch(monkeypatch, latest=1500, fake_cache=cache)
    resp = asyncio.run(systems.get_system_kills(30009001, since=None))
    assert resp.headers["X-Kills-Fresh-To"] == "1500"
    assert resp.headers["Cache-Control"] == "public, max-age=300"
    assert "ETag" not in resp.headers
    assert resp.status_code == 200


def test_full_path_fresh_to_is_build_time_across_hits(monkeypatch):
    cache = _FakeBinaryCache()
    # First call sees latest=1500 and stores it; a later call would see 9999 live,
    # but the cached fresh_to must remain 1500.
    _patch(monkeypatch, latest=9999, fake_cache=cache, latest_seq=[1500])
    r1 = asyncio.run(systems.get_system_kills(30009002, since=None))
    r2 = asyncio.run(systems.get_system_kills(30009002, since=None))
    assert r1.headers["X-Kills-Fresh-To"] == "1500"
    assert r2.headers["X-Kills-Fresh-To"] == "1500"


def test_full_path_single_flight_collapses_builds(monkeypatch):
    cache = _FakeBinaryCache()
    calls: list[int] = []

    async def slow_fetch(solar_system_id, since=None):
        calls.append(1)
        await asyncio.sleep(0.02)
        return _columns([1, 2, 3])

    async def fake_latest(_sid):
        return 1500

    monkeypatch.setattr(systems, "get_system_latest", fake_latest)
    monkeypatch.setattr(systems, "fetch_raw_kills", slow_fetch)
    monkeypatch.setattr(systems, "kills_binary_cache", cache)

    async def go():
        return await asyncio.gather(
            *[systems.get_system_kills(30009003, since=None) for _ in range(8)]
        )

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert {r.headers["X-Kills-Fresh-To"] for r in resps} == {"1500"}


def test_poll_path_is_no_store_and_never_touches_cache(monkeypatch):
    class _Boom:
        async def get(self, *a):
            raise AssertionError("poll path must not read the binary cache")

        async def set(self, *a):
            raise AssertionError("poll path must not write the binary cache")

    _patch(monkeypatch, latest=2000, fake_cache=_Boom())
    resp = asyncio.run(systems.get_system_kills(30009004, since=100))
    assert resp.headers["Cache-Control"] == "no-store"
    assert resp.headers["X-Kills-Fresh-To"] == "2000"


def test_short_circuit_returns_empty_no_store(monkeypatch):
    cache = _FakeBinaryCache()
    _patch(monkeypatch, latest=100, fake_cache=cache)
    resp = asyncio.run(systems.get_system_kills(30009005, since=100))  # since >= latest
    assert resp.headers["Cache-Control"] == "no-store"
    assert resp.headers["X-Kills-Fresh-To"] == "100"
    # empty payload = 4-byte row count of 0
    assert resp.body == b"\x00\x00\x00\x00"
    assert cache.touched is False


def test_full_path_empty_system_uses_wallclock(monkeypatch):
    cache = _FakeBinaryCache()
    _patch(monkeypatch, latest=None, fake_cache=cache)  # no kills → fresh_to = now
    before = int(time.time())
    resp = asyncio.run(systems.get_system_kills(30009006, since=None))
    after = int(time.time())
    assert before <= int(resp.headers["X-Kills-Fresh-To"]) <= after


def _expected(result):
    return encode_kills_binary(
        result["killmail_ids"],
        result["killmail_times"],
        [int(v) for v in result["x"]],
        [int(v) for v in result["y"]],
        [int(v) for v in result["z"]],
        result["ship_types"],
    )


def test_encode_maybe_offload_uses_thread_for_large(monkeypatch):
    calls = {"n": 0}

    async def rec(fn, *a):
        calls["n"] += 1
        return fn(*a)

    monkeypatch.setattr(systems.asyncio, "to_thread", rec)
    result = _columns(list(range(2000)))
    out = asyncio.run(systems._encode_maybe_offload(result, 2000))
    assert calls["n"] == 1  # offloaded to a thread
    assert out == _expected(result)  # ...and byte-identical


def test_encode_maybe_offload_inline_for_small(monkeypatch):
    calls = {"n": 0}

    async def rec(fn, *a):
        calls["n"] += 1
        return fn(*a)

    monkeypatch.setattr(systems.asyncio, "to_thread", rec)
    result = _columns([1, 2, 3])
    out = asyncio.run(systems._encode_maybe_offload(result, 2000))
    assert calls["n"] == 0  # below threshold → inline, no thread hop
    assert out == _expected(result)


def test_encode_maybe_offload_zeroes_out_of_range_positions(monkeypatch):
    # An abyssal-deadspace kill at ~1e32 overflows np.int64 in the encoder (the
    # reported HTTP 500). Its position must collapse to the (0,0,0) no-position
    # sentinel — the whole triple, since one bad axis makes the point unplottable —
    # while a normal neighbouring row is untouched.
    async def rec(fn, *a):
        return fn(*a)

    monkeypatch.setattr(systems.asyncio, "to_thread", rec)
    n = 100  # >= _NUMPY_MIN_ROWS -> exercises the numpy path that crashed
    result = _columns(list(range(n)))
    result["x"] = [10**32, 111] + [0] * (n - 2)
    result["y"] = [5, 222] + [0] * (n - 2)  # in range, but shares the bad triple
    result["z"] = [0, 333] + [0] * (n - 2)

    out = asyncio.run(systems._encode_maybe_offload(result, 2000))  # must not raise

    expected = encode_kills_binary(
        result["killmail_ids"],
        result["killmail_times"],
        [0, 111] + [0] * (n - 2),  # bad triple zeroed, normal row kept
        [0, 222] + [0] * (n - 2),
        [0, 333] + [0] * (n - 2),
        result["ship_types"],
    )
    assert out == expected
