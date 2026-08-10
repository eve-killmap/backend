import random
import struct

from app.binary_encoder import encode_kills_binary, _encode_kills_binary_scalar


def test_golden_known_frame():
    # Independently hand-derived expected bytes (not just == scalar):
    # count=1; ids[1]->zz2->0x02; times[100]->zz200->0xC8 0x01;
    # x/y/z[0]->0x00; ship_types[7]->zz14->0x0E
    out = encode_kills_binary([1], [100], [0], [0], [0], [7])
    assert out == (
        struct.pack(">I", 1)
        + b"\x02"  # ids
        + b"\xc8\x01"  # times
        + b"\x00\x00\x00"  # x, y, z
        + b"\x0e"  # ship_types
    )


def test_empty():
    out = encode_kills_binary([], [], [], [], [], [])
    assert out == struct.pack(">I", 0)
    assert out == _encode_kills_binary_scalar([], [], [], [], [], [])


def _case(rng, n):
    return dict(
        killmail_ids=[rng.randint(10_000_000_000, 200_000_000_000) for _ in range(n)],
        killmail_times=[rng.randint(1_400_000_000, 1_800_000_000) for _ in range(n)],
        x=[rng.randint(-(10**17), 10**17) for _ in range(n)],
        y=[rng.randint(-(10**17), 10**17) for _ in range(n)],
        z=[rng.randint(-(10**17), 10**17) for _ in range(n)],
        ship_types=[rng.randint(0, 50_000) for _ in range(n)],
    )


def test_differential_random_realistic():
    rng = random.Random(1234)
    for n in [1, 2, 3, 5, 63, 64, 65, 100, 1000, 5000]:
        c = _case(rng, n)
        assert encode_kills_binary(**c) == _encode_kills_binary_scalar(**c), f"n={n}"


def test_differential_edge_values():
    # n >= _NUMPY_MIN_ROWS so these exercise the NUMPY path, not the scalar fallback.
    n = 100
    cases = [
        # all zeros
        dict(
            killmail_ids=[0] * n,
            killmail_times=[0] * n,
            x=[0] * n,
            y=[0] * n,
            z=[0] * n,
            ship_types=[0] * n,
        ),
        # all identical (zero deltas after the first)
        dict(
            killmail_ids=[5] * n,
            killmail_times=[9] * n,
            x=[1] * n,
            y=[-1] * n,
            z=[0] * n,
            ship_types=[3] * n,
        ),
        # strictly descending large ids (negative deltas) + extreme +/- positions
        dict(
            killmail_ids=list(range(200_000_000_000, 200_000_000_000 - n, -1)),
            killmail_times=list(range(1_800_000_000, 1_800_000_000 - n, -1)),
            x=[10**17, -(10**17)] * (n // 2),
            y=[-(10**17), 10**17] * (n // 2),
            z=[0] * n,
            ship_types=[50_000, 0] * (n // 2),
        ),
    ]
    for c in cases:
        assert encode_kills_binary(**c) == _encode_kills_binary_scalar(**c)
