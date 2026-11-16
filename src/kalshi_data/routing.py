"""Live vs historical routing and window planning.

Kalshi partitions data into a *live* and a *historical* tier, with one
cutoff per dataset published at ``GET /historical/cutoff``:

* **Markets and candlesticks** are routed by the *market's settlement time*:
  a market that settled before ``market_settled_ts`` (and its candles) is only
  served by ``/historical/markets...``. Candle timestamps are irrelevant to
  this decision, so a candle request is never split by the trade cutoff.
* **Trades** are routed by *trade time*: trades created before
  ``trades_created_ts`` are only served by ``/historical/trades``. A window
  that straddles the cutoff is split into a historical part ``[start, cutoff)``
  and a live part ``[cutoff, end)``.

Window planning is deterministic so that the same request always maps to the
same work-unit keys (needed for resume and for skipping final work):

* Trades: UTC calendar-month cells intersected with the requested range.
* Candles: cells of ``5000 * interval`` minutes aligned to the Unix epoch
  (the documented per-request period limit), intersected with the range.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .endpoints import Tier
from .timeutil import from_unix, month_floor, next_month, parse_api_time, to_unix

MAX_CANDLE_PERIODS = 5000  # documented; 5,001 rejected with HTTP 400 (observed)


@dataclass(frozen=True)
class Cutoff:
    """Parsed ``/historical/cutoff`` response."""

    market_settled: datetime
    trades_created: datetime
    raw: dict[str, Any]

    @classmethod
    def from_body(cls, body: dict[str, Any]) -> Cutoff:
        ms = parse_api_time(body.get("market_settled_ts"))
        tc = parse_api_time(body.get("trades_created_ts"))
        if ms is None or tc is None:
            raise ValueError("historical cutoff response lacks market_settled_ts/trades_created_ts")
        return cls(market_settled=ms, trades_created=tc, raw=dict(body))


def market_tier(settlement_ts: datetime | None, cutoff: Cutoff) -> Tier:
    """Tier serving a market's metadata and candlesticks.

    A market with no settlement time has not settled, so it is live.
    """
    if settlement_ts is not None and settlement_ts < cutoff.market_settled:
        return Tier.HISTORICAL
    return Tier.LIVE


def split_trade_window(
    start: datetime, end: datetime, cutoff: Cutoff
) -> list[tuple[Tier, datetime, datetime]]:
    """Split ``[start, end)`` at the trade cutoff into tier segments (historical first)."""
    c = cutoff.trades_created
    parts: list[tuple[Tier, datetime, datetime]] = []
    if start < c:
        parts.append((Tier.HISTORICAL, start, min(end, c)))
    if end > c:
        parts.append((Tier.LIVE, max(start, c), end))
    return parts


def trade_cells(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """UTC calendar-month cells covering ``[start, end)``, clipped to the range."""
    cells = []
    cur = month_floor(start)
    while cur < end:
        nxt = next_month(cur)
        cells.append((max(cur, start), min(nxt, end)))
        cur = nxt
    return cells


def candle_cells(
    start: datetime, end: datetime, interval_minutes: int
) -> list[tuple[datetime, datetime]]:
    """Epoch-aligned cells of at most 5,000 periods covering ``[start, end)``.

    A candle belongs to the cell whose half-open range contains its
    ``end_period_ts``; the request for cell ``[a, b)`` therefore uses
    ``start_ts=a`` and ``end_ts=b-1`` because Kalshi treats both bounds as
    inclusive on ``end_period_ts`` (observed live).
    """
    span = interval_minutes * 60 * MAX_CANDLE_PERIODS
    s, e = to_unix(start), to_unix(end)
    if end.timestamp() > e:
        e += 1  # keep sub-second end inside the range
    cells = []
    cur = (s // span) * span
    while cur < e:
        nxt = cur + span
        cells.append((from_unix(max(cur, s)), from_unix(min(nxt, e))))
        cur = nxt
    return cells


def clip_to_market_life(
    start: datetime, end: datetime, created: datetime | None, settled: datetime | None
) -> tuple[datetime, datetime] | None:
    """Clip a request to ``[created_time, settlement_ts + 1 day)``.

    No trade or candle can precede market creation, and none can follow
    settlement; the one-day buffer is a deliberate safety margin. Returns None
    if nothing remains.
    """
    s = max(start, created) if created else start
    e = min(end, settled + timedelta(days=1)) if settled else end
    return (s, e) if e > s else None
