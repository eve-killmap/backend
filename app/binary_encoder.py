import struct

import numpy as np

_NUMPY_MIN_ROWS = 64


def _encode_delta_varints(values: list[int]) -> bytearray:
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
    return ((arr << np.int64(1)) ^ (arr >> np.int64(63))).astype(np.uint64)


def _varint_encode(u: np.ndarray) -> bytes:
    n = u.shape[0]
    if n == 0:
        return b""
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
    d[0] = arr[0]
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
