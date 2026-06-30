import struct


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


def encode_kills_binary(
    killmail_ids: list[int],
    killmail_times: list[int],
    x: list[int],
    y: list[int],
    z: list[int],
    ship_types: list[int],
) -> bytes:
    """
    Columnar binary encoding (killmail_time DESC order):

    [4 bytes]  row count (uint32 BE)
    [N bytes]  killmail_ids    delta + zigzag varint
    [N bytes]  killmail_times  delta + zigzag varint
    [N bytes]  x               delta + zigzag varint
    [N bytes]  y               delta + zigzag varint
    [N bytes]  z               delta + zigzag varint
    [N bytes]  ship_types      zigzag varint (no delta)
    """
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
