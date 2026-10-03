# Number.MAX_SAFE_INTEGER
SAFE_COORD_MAX = 2**53 - 1


def sanitize_position(x, y, z) -> tuple[int, int, int]:
    xi, yi, zi = int(x), int(y), int(z)
    if abs(xi) > SAFE_COORD_MAX or abs(yi) > SAFE_COORD_MAX or abs(zi) > SAFE_COORD_MAX:
        return 0, 0, 0
    return xi, yi, zi
