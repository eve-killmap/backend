from app.positions import sanitize_position, SAFE_COORD_MAX


def test_in_range_passes_through_as_ints():
    assert sanitize_position(-4.5e12, 1.0e11, 0.0) == (-4_500_000_000_000, 100_000_000_000, 0)


def test_boundary_max_safe_is_kept():
    assert sanitize_position(SAFE_COORD_MAX, -SAFE_COORD_MAX, 0) == (
        SAFE_COORD_MAX,
        -SAFE_COORD_MAX,
        0,
    )


def test_just_beyond_boundary_is_zeroed():
    assert sanitize_position(SAFE_COORD_MAX + 1, 0, 0) == (0, 0, 0)


def test_any_axis_out_of_range_zeroes_the_whole_triple():
    # A position is a 3-vector; if one axis can't round-trip, the point can't be
    # placed, so the entire triple collapses to the (0,0,0) sentinel.
    assert sanitize_position(1, 2, 10**32) == (0, 0, 0)
    assert sanitize_position(1, 10**32, 3) == (0, 0, 0)
    assert sanitize_position(10**32, 2, 3) == (0, 0, 0)


def test_negative_out_of_range_is_zeroed():
    assert sanitize_position(-(10**36), 0, 0) == (0, 0, 0)


def test_real_abyssal_samples_are_zeroed():
    # From the reported dataset. The smallest offending magnitude (~6.09e17) still
    # trips the guard: it exceeds 2**53 even though it fits inside int64.
    assert sanitize_position(
        6.088839837036236e17, 3.324823252621145e17, 6.08883984030867e17
    ) == (0, 0, 0)
    assert sanitize_position(
        1.4048816610602347e32, 6.919427609529127e31, 1.4049075104032597e32
    ) == (0, 0, 0)
