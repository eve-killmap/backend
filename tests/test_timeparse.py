from datetime import datetime, timezone

from app.timeparse import iso_to_epoch


def test_none_passes_through():
    assert iso_to_epoch(None) is None


def test_epoch_zero():
    assert iso_to_epoch("1970-01-01T00:00:00Z") == 0


def test_z_suffix_is_utc():
    expected = int(datetime(2026, 6, 28, 18, 51, 9, tzinfo=timezone.utc).timestamp())
    assert iso_to_epoch("2026-06-28T18:51:09Z") == expected


def test_offset_form():
    assert iso_to_epoch("1970-01-01T00:00:00+00:00") == 0
