"""UTC timestamp parsing and conversion.

Conventions used by the whole package:

* Every datetime is timezone-aware UTC. Naive datetimes are rejected because
  their meaning depends on the machine they run on.
* User-facing ranges are half-open ``[start, end)``.
* A date-only input such as ``2025-01-01`` means midnight UTC at the *start*
  of that day. So ``--start 2025-01-01 --end 2026-01-01`` covers all of 2025
  and nothing from 2026-01-01.
* Kalshi query parameters (``min_ts``, ``start_ts`` ...) are integer Unix
  seconds; Kalshi rejects milliseconds with HTTP 400 (observed live).
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

from .errors import InvalidTimestampError

_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def utcnow() -> datetime:
    """Current time as an aware UTC datetime."""
    return datetime.now(UTC)


def parse_user_time(value: str | datetime | date) -> datetime:
    """Parse a user-supplied time into an aware UTC datetime.

    Accepted forms:
        * ``YYYY-MM-DD`` -> midnight UTC at the start of that day.
        * ISO 8601 with an explicit offset or ``Z`` (``2025-01-01T13:30:00Z``,
          ``2025-01-01T09:30:00-04:00``); converted to UTC.
        * ``now``.
        * An aware ``datetime`` or a ``date``.

    Raises:
        InvalidTimestampError: for naive datetimes/strings without an offset
            or for unparseable input.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise InvalidTimestampError(
                f"Naive datetime {value!r} is ambiguous; attach a timezone (e.g. UTC)."
            )
        return value.astimezone(UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    text = value.strip()
    if text.lower() == "now":
        return utcnow()
    if _DATE_ONLY.match(text):
        try:
            d = date.fromisoformat(text)
        except ValueError as exc:
            raise InvalidTimestampError(f"Invalid date {text!r}: {exc}") from exc
        return datetime(d.year, d.month, d.day, tzinfo=UTC)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError as exc:
        raise InvalidTimestampError(
            f"Cannot parse {text!r}. Use YYYY-MM-DD or ISO 8601 with an offset, "
            "e.g. 2025-01-01T00:00:00Z."
        ) from exc
    if dt.tzinfo is None:
        raise InvalidTimestampError(
            f"Timestamp {text!r} has no timezone offset; append 'Z' for UTC or an offset."
        )
    return dt.astimezone(UTC)


def parse_api_time(value: str | None) -> datetime | None:
    """Parse an RFC 3339 timestamp from a Kalshi response (microsecond precision).

    Returns None for None or the empty string. Raises ValueError for any other
    unparseable value so the caller can record a validation issue.
    """
    if value is None or value == "":
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"API timestamp without offset: {value!r}")
    return dt.astimezone(UTC)


def to_unix(dt: datetime) -> int:
    """Whole Unix seconds (floor) for an aware datetime."""
    if dt.tzinfo is None:
        raise InvalidTimestampError("to_unix requires an aware datetime")
    return int(dt.timestamp() // 1)


def from_unix(ts: int) -> datetime:
    """Aware UTC datetime from Unix seconds."""
    return datetime.fromtimestamp(ts, UTC)


def iso(dt: datetime | None) -> str | None:
    """Canonical ISO 8601 text used in SQLite and exports (UTC, microseconds, 'Z')."""
    if dt is None:
        return None
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def validate_range(start: datetime, end: datetime) -> None:
    """Ensure a half-open range is non-empty and aware."""
    if start.tzinfo is None or end.tzinfo is None:
        raise InvalidTimestampError("Range bounds must be timezone-aware")
    if end <= start:
        raise InvalidTimestampError(f"End {iso(end)} must be after start {iso(start)}")


def month_floor(dt: datetime) -> datetime:
    """First instant of the UTC calendar month containing ``dt``."""
    return datetime(dt.year, dt.month, 1, tzinfo=UTC)


def next_month(dt: datetime) -> datetime:
    """First instant of the UTC calendar month after ``dt``'s month."""
    if dt.month == 12:
        return datetime(dt.year + 1, 1, 1, tzinfo=UTC)
    return datetime(dt.year, dt.month + 1, 1, tzinfo=UTC)


ONE_SECOND = timedelta(seconds=1)
