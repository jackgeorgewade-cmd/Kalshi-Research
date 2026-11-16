"""Normalization of SYNTHETIC records: essentials, warnings, schema adapters."""

from __future__ import annotations

from decimal import Decimal

from fixtures.synthetic import hist_candle, live_candle, market, trade
from kalshi_data.decimals import loads_exact
from kalshi_data.normalize import (
    parse_candle,
    parse_market,
    parse_orderbook,
    parse_trade,
)


def test_trade_ok_with_fractional_count() -> None:
    rec, issues = parse_trade(trade("t1", "M", 1_700_000_000.5, "0.0010", "34.46"))
    assert rec is not None and not [i for i in issues if i.severity == "error"]
    assert rec["count"] == Decimal("34.46")
    assert rec["yes_price"] == Decimal("0.0010")
    assert rec["no_price"] == Decimal("0.9990")
    assert rec["created_time"].microsecond == 500000


def test_trade_unknown_optional_field_is_harmless() -> None:
    rec, issues = parse_trade(trade("t1", "M", 1_700_000_000, new_field="x"))
    assert rec is not None and issues == []


def test_trade_missing_essential_is_quarantined() -> None:
    raw = trade("t1", "M", 1_700_000_000)
    del raw["yes_price_dollars"]
    rec, issues = parse_trade(raw)
    assert rec is None
    assert issues[0].severity == "error" and issues[0].raw == raw


def test_trade_malformed_essential_and_timestamp() -> None:
    rec, issues = parse_trade(trade("t1", "M", 1_700_000_000, count_fp="lots"))
    assert rec is None and issues[0].code == "malformed_field"
    rec, issues = parse_trade(trade("t1", "M", 1_700_000_000, created_time="not-a-time"))
    assert rec is None


def test_trade_optional_malformed_is_warning_and_null() -> None:
    rec, issues = parse_trade(trade("t1", "M", 1_700_000_000, no_price_dollars="??"))
    assert rec is not None and rec["no_price"] is None
    assert [i.severity for i in issues] == ["warning"]


def test_trade_zero_price_kept_as_zero_with_mismatch_check() -> None:
    rec, _issues = parse_trade(trade("t1", "M", 1, price="0.0000"))
    assert rec is not None and rec["yes_price"] == Decimal("0")


def test_market_strikes_decimal_and_distinct_times() -> None:
    body = loads_exact(
        b'{"market": %s}'
        % __import__("json").dumps(market("M", "E", settled="2026-09-16T23:01:48.444806Z")).encode()
    )
    rec, _issues = parse_market(body["market"])
    assert rec is not None
    assert rec["cap_strike"] == Decimal("7899.9999")
    assert rec["floor_strike"] == Decimal("7875")
    assert rec["settlement_ts"] != rec["close_time"]
    assert rec["result"] == "no"


def test_market_missing_event_ticker_is_error() -> None:
    m = market("M", "E")
    del m["event_ticker"]
    rec, issues = parse_market(m)
    assert rec is None and issues[0].severity == "error"


def test_live_and_historical_candle_adapters_agree() -> None:
    a, ia = parse_candle(live_candle(3600, "0.4200", "2.50"), "M", 60, "live")
    b, ib = parse_candle(hist_candle(3600, "0.4200", "2.50"), "M", 60, "historical")
    assert a is not None and b is not None and not ia and not ib
    for k in (
        "trade_close",
        "yes_bid_open",
        "yes_ask_close",
        "volume",
        "open_interest",
        "trade_previous",
    ):
        assert a[k] == b[k], k
    assert a["trade_close"] == Decimal("0.4200")
    assert a["yes_bid_close"] == Decimal("0.4100") and a["yes_ask_close"] == Decimal("0.4300")


def test_candle_without_trades_has_null_trade_prices_not_zero() -> None:
    c, issues = parse_candle(live_candle(3600, close=None, volume="0.00"), "M", 60, "live")
    assert c is not None and not issues
    assert c["trade_close"] is None and c["trade_open"] is None
    assert c["volume"] == Decimal("0")
    assert c["yes_bid_close"] is not None  # quotes still present


def test_candle_schema_mismatch_detected() -> None:
    c, issues = parse_candle(hist_candle(3600), "M", 60, "live")
    assert c is None and issues[-1].code == "schema_mismatch"


def test_candle_bad_end_ts_quarantined() -> None:
    c, issues = parse_candle({"end_period_ts": "3600"}, "M", 60, "live")
    assert c is None and issues[0].severity == "error"


def test_orderbook_levels_ranked_best_first() -> None:
    body = {
        "orderbook_fp": {
            "yes_dollars": [["0.4100", "5.00"], ["0.4200", "1.50"]],
            "no_dollars": [["0.5600", "2.00"]],
        }
    }
    levels, issues = parse_orderbook(body, "M")
    assert levels is not None and not issues
    best_yes = next(lv for lv in levels if lv["side"] == "yes" and lv["rank"] == 1)
    assert best_yes["price"] == Decimal("0.4200") and best_yes["quantity"] == Decimal("1.50")


def test_orderbook_malformed_level() -> None:
    levels, issues = parse_orderbook(
        {"orderbook_fp": {"yes_dollars": [["0.4"]], "no_dollars": []}}, "M"
    )
    assert levels is None and issues[0].severity == "error"
