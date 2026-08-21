"""Kill-position sanitization shared by the REST binary path and the live WS feed.

Kill coordinates round-trip the client pipeline only within +/-(2**53 - 1),
JavaScript's Number.MAX_SAFE_INTEGER: the binary encoder packs them as int64
delta-varints and the browser decoder rebuilds them as float64. A handful of
abyssal-deadspace killmails report positions up to ~1e36 m. These overflow
np.int64 in the encoder (the reported HTTP 500) and, being far past 2**53, decode
as garbage on the client anyway -- and can't be meaningfully plotted regardless,
since the float64 ULP at that magnitude dwarfs an entire abyssal pocket.

Map any out-of-range triple to the client's (0, 0, 0) "no position" sentinel
(see the frontend's countMissingPositions): the kill stays listed and counted but
is simply never rendered, rather than crashing the endpoint or being mislocated.
"""

# The largest coordinate magnitude that survives the int64 -> float64 round trip
# intact on the client (Number.MAX_SAFE_INTEGER).
SAFE_COORD_MAX = 2**53 - 1


def sanitize_position(x, y, z) -> tuple[int, int, int]:
    """Coerce a position triple to ints, collapsing the whole triple to (0, 0, 0)
    -- the client's "no position" sentinel -- if any axis is out of range."""
    xi, yi, zi = int(x), int(y), int(z)
    if (
        abs(xi) > SAFE_COORD_MAX
        or abs(yi) > SAFE_COORD_MAX
        or abs(zi) > SAFE_COORD_MAX
    ):
        return 0, 0, 0
    return xi, yi, zi
