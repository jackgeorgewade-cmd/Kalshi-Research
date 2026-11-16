"""UTC parsing, half-open ranges and exact decimal handling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from kalshi_data.decimals import dumps_exact, loads_exact, to_decimal
from kalshi_data.errors import InvalidTimestampError
from kalshi_data.timeutil import iso, parse_api_time, parse_user_time, to_unix, validate_range


def test_date_only_is_midnight_utc() -> None:
    assert parse_user_time("2025-01-01") == datetime(2025, 1, 1, tzinfo=UTC)


def test_offset_converted_to_utc() -> None:
    assert parse_user_time("2025-01-01T09:30:00-04:00") == datetime(2025, 1, 1, 13, 30, tzinfo=UTC)
    assert parse_user_time("2025-01-01T13:30:00Z").tzinfo == UTC


@pytest.mark.parametrize("bad", ["2025-01-01T00:00:00", "yesterday", "2025-13-01", ""])
def test_ambiguous_or_invalid_rejected(bad: str) -> None:
    with pytest.raises(InvalidTimestampError):
        parse_user_time(bad)


def test_naive_datetime_rejected_aware_accepted() -> None:
    with pytest.raises(InvalidTimestampError):
        parse_user_time(datetime(2025, 1, 1))
    eastern = timezone(timedelta(hours=-5))
    assert parse_user_time(datetime(2025, 1, 1, tzinfo=eastern)) == datetime(
        2025, 1, 1, 5, tzinfo=UTC
    )


def test_api_time_microseconds_and_iso_roundtrip() -> None:
    dt = parse_api_time("2023-12-29T20:59:57.15728Z")
    assert dt is not None and dt.microsecond == 157280
    assert iso(dt) == "2023-12-29T20:59:57.157280Z"
    assert parse_api_time("") is None and parse_api_time(None) is None


def test_range_validation_and_unix_floor() -> None:
    s = parse_user_time("2025-01-02")
    with pytest.raises(InvalidTimestampError):
        validate_range(s, s)
    assert to_unix(datetime(2025, 1, 1, 0, 0, 0, 999999, tzinfo=UTC)) == 1735689600


def test_fixed_point_strings_and_fractional_quantities() -> None:
    assert to_decimal("0.1200") == Decimal("0.1200")
    assert str(to_decimal("0.1200")) == "0.1200"  # trailing zeros preserved
    assert to_decimal("34.46") == Decimal("34.46")
    assert to_decimal("0.0001") == Decimal("0.0001")  # sub-cent price


def test_zero_is_not_null() -> None:
    assert to_decimal("0.0000") == Decimal(0)
    assert to_decimal(None) is None


@pytest.mark.parametrize("bad", ["", "abc", True, 0.1, "NaN", "Infinity"])
def test_invalid_decimal_inputs(bad: object) -> None:
    with pytest.raises(ValueError):
        to_decimal(bad)


def test_json_numbers_never_become_float() -> None:
    body = loads_exact(b'{"floor_strike": 7949.9999, "n": 3, "p": 0.1}')
    assert isinstance(body["floor_strike"], Decimal)
    assert body["floor_strike"] == Decimal("7949.9999")
    assert body["n"] == 3
    assert dumps_exact(body) == '{"floor_strike": "7949.9999", "n": 3, "p": "0.1"}'
