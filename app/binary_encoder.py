import struct

import numpy as np

# Below this row count the scalar path is used: it's exact and avoids numpy's
# per-call array-setup overhead (delta polls encode only a handful of rows).
_NUMPY_MIN_ROWS = 64


def _encode_delta_varints(values: list[int]) -> bytearray:
    """Delta + zigzag + LEB128 varint encode a list of signed integers."""
    buf = bytearray()
    prev = 0
    for v in values:
        delta = v - prev
        prev = v
        uval = (delta << 1) ^ (delta >> 63)
        while uval > 0x7F:
            buf.append((uval & 0x7F) | 0x80)
            uval >>= 7
        buf.append(uval)
    return buf


def _encode_kills_binary_scalar(
    killmail_ids: list[int],
    killmail_times: list[int],
    x: list[int],
    y: list[int],
    z: list[int],
    ship_types: list[int],
) -> bytes:
    """Reference pure-Python encoder. Retained as the small-N fast path and as
    the differential-test oracle for the numpy implementation below."""
    buf = bytearray(struct.pack(">I", len(killmail_ids)))
    buf += _encode_delta_varints(killmail_ids)
    buf += _encode_delta_varints(killmail_times)
    buf += _encode_delta_varints(x)
    buf += _encode_delta_varints(y)
    buf += _encode_delta_varints(z)

    for s in ship_types:
        uval = (s << 1) ^ (s >> 63)
        while uval > 0x7F:
            buf.append((uval & 0x7F) | 0x80)
            uval >>= 7
        buf.append(uval)

    return bytes(buf)


def _zigzag_i64(arr: np.ndarray) -> np.ndarray:
    # (n << 1) ^ (n >> 63) in two's-complement int64, reinterpreted as uint64.
    return ((arr << np.int64(1)) ^ (arr >> np.int64(63))).astype(np.uint64)


def _varint_encode(u: np.ndarray) -> bytes:
    """LEB128 varint-encode a uint64 array (vectorized byte-plane scatter)."""
    n = u.shape[0]
    if n == 0:
        return b""
    # bytes per value: 1 + one extra per 7-bit group beyond the first.
    nbytes = np.ones(n, dtype=np.int64)
    for k in range(1, 10):
        nbytes += (u >> np.uint64(7 * k)) > np.uint64(0)
    ends = np.cumsum(nbytes)
    starts = ends - nbytes
    out = np.zeros(int(ends[-1]), dtype=np.uint8)
    for j in range(10):
        sel = nbytes > j
        if not sel.any():
            break
        byte = ((u >> np.uint64(7 * j)) & np.uint64(0x7F)).astype(np.uint8)
        cont = np.where(nbytes > (j + 1), np.uint8(0x80), np.uint8(0)).astype(np.uint8)
        byte = byte | cont
        out[(starts + j)[sel]] = byte[sel]
    return out.tobytes()


def _delta_varint_column(values: list[int]) -> bytes:
    if not values:
        return b""
    arr = np.asarray(values, dtype=np.int64)
    d = np.empty_like(arr)
    d[0] = arr[0]  # prev starts at 0, matching the scalar impl
    if arr.shape[0] > 1:
        d[1:] = np.diff(arr)
    return _varint_encode(_zigzag_i64(d))


def _zigzag_varint_column(values: list[int]) -> bytes:
    if not values:
        return b""
    return _varint_encode(_zigzag_i64(np.asarray(values, dtype=np.int64)))


def encode_kills_binary(
    killmail_ids: list[int],
    killmail_times: list[int],
    x: list[int],
    y: list[int],
    z: list[int],
    ship_types: list[int],
) -> bytes:
    """
    Columnar binary encoding:

    [4 bytes]  row count (uint32 BE)
    [N bytes]  killmail_ids    delta + zigzag varint
    [N bytes]  killmail_times  delta + zigzag varint
    [N bytes]  x               delta + zigzag varint
    [N bytes]  y               delta + zigzag varint
    [N bytes]  z               delta + zigzag varint
    [N bytes]  ship_types      zigzag varint (no delta)

    numpy fast path; the scalar impl is used for tiny inputs (numpy setup
    overhead) and is the differential oracle -- output is byte-for-byte
    identical to _encode_kills_binary_scalar for the (int64-domain) kill data.

    Row order is whatever the caller passes: full-system fetches are
    killmail_time DESC; delta (since) fetches are unordered and sorted
    client-side. Delta-varint coding is order-independent, so either is valid.
    """
    if len(killmail_ids) < _NUMPY_MIN_ROWS:
        return _encode_kills_binary_scalar(
            killmail_ids, killmail_times, x, y, z, ship_types
        )
    buf = bytearray(struct.pack(">I", len(killmail_ids)))
    buf += _delta_varint_column(killmail_ids)
    buf += _delta_varint_column(killmail_times)
    buf += _delta_varint_column(x)
    buf += _delta_varint_column(y)
    buf += _delta_varint_column(z)
    buf += _zigzag_varint_column(ship_types)
    return bytes(buf)
