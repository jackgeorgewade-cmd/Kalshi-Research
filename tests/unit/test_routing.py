"""Dataset-specific routing and window planning."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta

from kalshi_data.endpoints import Tier
from kalshi_data.routing import (
    MAX_CANDLE_PERIODS,
    Cutoff,
    candle_cells,
    clip_to_market_life,
    market_tier,
    split_trade_window,
    trade_cells,
)
from kalshi_data.timeutil import to_unix

C = datetime(2026, 9, 17, tzinfo=UTC)
CUT = Cutoff(market_settled=C, trades_created=C, raw={})


def d(*a: int) -> datetime:
    return datetime(*a, tzinfo=UTC)  # type: ignore[misc]


def test_market_tier_depends_on_settlement_only() -> None:
    assert market_tier(None, CUT) is Tier.LIVE  # unsettled
    assert market_tier(d(2026, 9, 16), CUT) is Tier.HISTORICAL
    assert market_tier(d(2026, 9, 17), CUT) is Tier.LIVE  # "before" the cutoff only


def test_trade_window_split_at_cutoff() -> None:
    parts = split_trade_window(d(2026, 9, 1), d(2026, 10, 1), CUT)
    assert parts == [(Tier.HISTORICAL, d(2026, 9, 1), C), (Tier.LIVE, C, d(2026, 10, 1))]
    assert split_trade_window(d(2026, 1, 1), d(2026, 2, 1), CUT)[0][0] is Tier.HISTORICAL
    assert len(split_trade_window(d(2026, 1, 1), d(2026, 2, 1), CUT)) == 1
    assert split_trade_window(C, d(2026, 10, 1), CUT) == [(Tier.LIVE, C, d(2026, 10, 1))]


def test_trade_cells_are_calendar_months_clipped() -> None:
    cells = trade_cells(d(2025, 1, 15), d(2025, 3, 2))
    assert cells == [
        (d(2025, 1, 15), d(2025, 2, 1)),
        (d(2025, 2, 1), d(2025, 3, 1)),
        (d(2025, 3, 1), d(2025, 3, 2)),
    ]
    assert trade_cells(d(2025, 12, 5), d(2026, 1, 5))[1][0] == d(2026, 1, 1)


def test_candle_cells_respect_period_limit_and_are_adjacent() -> None:
    for interval in (1, 60, 1440):
        cells = candle_cells(d(2024, 1, 1), d(2026, 1, 1), interval)
        for (_s, e), (s2, _) in itertools.pairwise(cells):
            assert e == s2  # adjacent, no overlap, no hole
        for s, e in cells:
            # request uses end_ts = e - 1 (inclusive): must be <= 5000 periods
            assert (to_unix(e) - 1 - to_unix(s)) / 60 / interval <= MAX_CANDLE_PERIODS
        assert cells[0][0] == d(2024, 1, 1) and cells[-1][1] == d(2026, 1, 1)


def test_candle_cells_deterministic_epoch_alignment() -> None:
    a = candle_cells(d(2025, 1, 1), d(2025, 6, 1), 1)
    b = candle_cells(d(2025, 1, 1), d(2025, 7, 1), 1)
    assert a[:-1] == b[: len(a) - 1]  # same interior cells -> same unit keys


def test_clip_to_market_life() -> None:
    created, settled = d(2025, 3, 1), d(2025, 3, 10)
    assert clip_to_market_life(d(2025, 1, 1), d(2026, 1, 1), created, settled) == (
        created,
        settled + timedelta(days=1),
    )
    assert clip_to_market_life(d(2024, 1, 1), d(2024, 2, 1), created, settled) is None
    assert clip_to_market_life(d(2025, 1, 1), d(2025, 2, 1), None, None) == (
        d(2025, 1, 1),
        d(2025, 2, 1),
    )
