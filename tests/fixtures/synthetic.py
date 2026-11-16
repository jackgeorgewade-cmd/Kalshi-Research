"""SYNTHETIC test fixtures and an in-memory fake of the Kalshi REST API.

Nothing in this file is live evidence. Values are invented; the *shapes*
follow the documented OpenAPI schemas and the behaviours observed live on
2026-11-16 (see docs/api_findings.md):

* market/trade/candle field names (live candles use ``*_dollars``/``volume_fp``,
  historical candles use ``open``/``volume``);
* ``min_ts``/``max_ts`` compared against sub-second trade times;
* candle ``start_ts``/``end_ts`` inclusive on ``end_period_ts``; > 5,000 periods -> 400;
* ``/markets/{ticker}`` returns 404 for archived markets, which are served by
  ``/historical/markets/{ticker}``;
* cursor pagination with an empty cursor at the end.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

BASE = "https://kalshi.test.invalid/trade-api/v2"

CUTOFF = "2026-09-17T00:00:00Z"
CUTOFF_TS = int(datetime(2026, 9, 17, tzinfo=UTC).timestamp())


def ts(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())


def iso_us(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def market(ticker: str, event: str, *, status: str = "active", settled: str | None = None,
           created: str = "2026-01-01T00:00:00Z", close: str = "2026-12-31T21:00:00Z",
           **extra: Any) -> dict[str, Any]:
    """A SYNTHETIC market object following the documented schema."""
    m: dict[str, Any] = {
        "ticker": ticker, "event_ticker": event, "market_type": "binary",
        "title": f"Synthetic market {ticker}", "subtitle": "", "yes_sub_title": "range",
        "no_sub_title": "range", "created_time": created, "updated_time": created,
        "open_time": created, "close_time": close,
        "latest_expiration_time": close, "expected_expiration_time": close,
        "settlement_timer_seconds": 60, "status": status,
        "notional_value_dollars": "1.0000", "yes_bid_dollars": "0.4200",
        "yes_ask_dollars": "0.4400", "no_bid_dollars": "0.5600", "no_ask_dollars": "0.5800",
        "yes_bid_size_fp": "10.00", "yes_ask_size_fp": "5.50", "last_price_dollars": "0.4300",
        "previous_yes_bid_dollars": "0.4000", "previous_yes_ask_dollars": "0.4500",
        "previous_price_dollars": "0.4100", "volume_fp": "123.45", "volume_24h_fp": "0.00",
        "open_interest_fp": "99.99", "result": "", "can_close_early": True,
        "expiration_value": "", "rules_primary": "Synthetic rules.", "rules_secondary": "",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
        "settlement_bounds_type": "default", "strike_type": "between",
        "floor_strike": 7875, "cap_strike": 7899.9999,
    }
    if settled:
        m.update(status="finalized", result="no", settlement_ts=settled,
                 settlement_value_dollars="0.0000")
    m.update(extra)
    return m


def trade(tid: str, ticker: str, t: float, price: str = "0.4200", count: str = "1.00",
          **extra: Any) -> dict[str, Any]:
    """A SYNTHETIC trade object."""
    no_price = str(1 - __import__("decimal").Decimal(price))
    d = {"trade_id": tid, "ticker": ticker, "count_fp": count, "yes_price_dollars": price,
         "no_price_dollars": f"{__import__('decimal').Decimal(no_price):.4f}",
         "taker_side": "yes", "taker_outcome_side": "yes", "taker_book_side": "bid",
         "created_time": iso_us(t), "is_block_trade": False}
    d.update(extra)
    return d


def live_candle(end_ts: int, close: str | None = "0.4200", volume: str = "1.00",
                bid: str = "0.4100", ask: str = "0.4300") -> dict[str, Any]:
    """A SYNTHETIC candle in the documented *live* shape."""
    price: dict[str, Any] = {}
    if close is not None:
        price = {"open_dollars": close, "high_dollars": close, "low_dollars": close,
                 "close_dollars": close, "mean_dollars": close, "previous_dollars": "0.4000"}
    return {"end_period_ts": end_ts, "volume_fp": volume, "open_interest_fp": "50.00",
            "price": price,
            "yes_bid": {"open_dollars": bid, "high_dollars": bid, "low_dollars": bid,
                        "close_dollars": bid},
            "yes_ask": {"open_dollars": ask, "high_dollars": ask, "low_dollars": ask,
                        "close_dollars": ask}}


def hist_candle(end_ts: int, close: str | None = "0.4200", volume: str = "1.00",
                bid: str = "0.4100", ask: str = "0.4300") -> dict[str, Any]:
    """A SYNTHETIC candle in the documented *historical* shape."""
    return {"end_period_ts": end_ts, "volume": volume, "open_interest": "50.00",
            "price": {"open": close, "high": close, "low": close, "close": close,
                      "mean": close, "previous": "0.4000"},
            "yes_bid": {"open": bid, "high": bid, "low": bid, "close": bid},
            "yes_ask": {"open": ask, "high": ask, "low": ask, "close": ask}}


Hook = Callable[[str, dict[str, str]], httpx.Response | None]


@dataclass
class FakeKalshi:
    """In-memory SYNTHETIC Kalshi API served through ``httpx.MockTransport``."""

    cutoff: dict[str, str] = field(default_factory=lambda: {
        "market_settled_ts": CUTOFF, "trades_created_ts": CUTOFF,
        "orders_updated_ts": CUTOFF, "market_positions_last_updated_ts": CUTOFF})
    live_markets: dict[str, dict[str, Any]] = field(default_factory=dict)
    hist_markets: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: dict[str, dict[str, Any]] = field(default_factory=dict)
    live_trades: list[dict[str, Any]] = field(default_factory=list)
    hist_trades: list[dict[str, Any]] = field(default_factory=list)
    live_candles: dict[tuple[str, int], list[dict[str, Any]]] = field(default_factory=dict)
    hist_candles: dict[tuple[str, int], list[dict[str, Any]]] = field(default_factory=dict)
    series: list[dict[str, Any]] = field(default_factory=list)
    orderbooks: dict[str, dict[str, Any]] = field(default_factory=dict)
    hooks: list[Hook] = field(default_factory=list)
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    def add_market(self, m: dict[str, Any], *, historical: bool = False,
                   series: str = "SYNSERIES") -> None:
        (self.hist_markets if historical else self.live_markets)[m["ticker"]] = m
        self.events.setdefault(m["event_ticker"], {
            "event_ticker": m["event_ticker"], "series_ticker": series,
            "title": "Synthetic event", "sub_title": "", "collateral_return_type": "",
            "mutually_exclusive": True, "settlement_sources": [], "category": "Financials"})

    # ----------------------------------------------------------------- server
    def handler(self, request: httpx.Request) -> httpx.Response:
        url = urlparse(str(request.url))
        path = url.path.removeprefix("/trade-api/v2")
        params = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
        self.calls.append((path, params))
        for hook in self.hooks:
            resp = hook(path, params)
            if resp is not None:
                return resp
        return self.route(path, params)

    def route(self, path: str, params: dict[str, str]) -> httpx.Response:
        parts = path.strip("/").split("/")
        if path == "/historical/cutoff":
            return _ok(self.cutoff)
        if path == "/exchange/status":
            return _ok({"exchange_active": True})
        if path == "/series":
            return _ok({"series": self.series})
        if parts[0] == "events" and len(parts) == 2:
            ev = self.events.get(parts[1])
            return _ok({"event": ev, "markets": []}) if ev else _err(404, "not_found")
        if path in ("/markets/trades", "/historical/trades"):
            src = self.live_trades if path == "/markets/trades" else self.hist_trades
            return self._trades(src, params)
        if path in ("/markets", "/historical/markets"):
            src = self.live_markets if path == "/markets" else self.hist_markets
            items = [m for m in src.values()
                     if ("series_ticker" not in params
                         or self.events[m["event_ticker"]]["series_ticker"]
                         == params["series_ticker"])
                     and ("event_ticker" not in params
                          or m["event_ticker"] == params["event_ticker"])]
            return _page(items, params, "markets")
        if parts[0] == "markets" and len(parts) == 2:
            m = self.live_markets.get(parts[1])
            return _ok({"market": m}) if m else _err(404, "not_found")
        if parts[:2] == ["historical", "markets"] and len(parts) == 3:
            m = self.hist_markets.get(parts[2])
            return _ok({"market": m}) if m else _err(404, "not_found")
        if parts[0] == "markets" and len(parts) == 3 and parts[2] == "orderbook":
            return _ok(self.orderbooks.get(parts[1],
                                           {"orderbook_fp": {"yes_dollars": [],
                                                             "no_dollars": []}}))
        if parts[-1] == "candlesticks":
            historical = parts[0] == "historical"
            ticker = parts[2] if historical else parts[3]
            if historical and ticker not in self.hist_markets:
                return _err(404, "not_found")
            if not historical and ticker not in self.live_markets:
                return _err(404, "not_found")
            interval = int(params["period_interval"])
            s, e = int(params["start_ts"]), int(params["end_ts"])
            if (e - s) / 60 / interval > 5000:
                return _err(400, "bad_request", "requested time range too large")
            src = self.hist_candles if historical else self.live_candles
            items = [c for c in src.get((ticker, interval), [])
                     if s <= c["end_period_ts"] <= e]
            return _ok({"ticker": ticker, "candlesticks": items})
        return _err(404, "not_found")

    def _trades(self, src: list[dict[str, Any]], params: dict[str, str]) -> httpx.Response:
        items = [t for t in src if t["ticker"] == params.get("ticker")]
        lo, hi = float(params.get("min_ts", 0)), float(params.get("max_ts", 1e12))
        items = [t for t in items if lo <= ts_of(t) <= hi]
        items.sort(key=lambda t: (ts_of(t), t["trade_id"]), reverse=True)  # newest first
        return _page(items, params, "trades")

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


def ts_of(t: dict[str, Any]) -> float:
    return datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")).timestamp()


def _page(items: list[dict[str, Any]], params: dict[str, str], key: str) -> httpx.Response:
    limit = int(params.get("limit", 100))
    cursor = params.get("cursor", "")
    offset = int(cursor[1:]) if cursor.startswith("c") else 0  # bad cursor -> first page
    chunk = items[offset:offset + limit]
    nxt = f"c{offset + limit}" if offset + limit < len(items) else ""
    return _ok({key: chunk, "cursor": nxt})


def _ok(body: Any) -> httpx.Response:
    return httpx.Response(200, content=json.dumps(body).encode(),
                          headers={"content-type": "application/json",
                                   "date": "Mon, 16 Nov 2026 00:00:00 GMT"})


def _err(status: int, code: str, details: str | None = None) -> httpx.Response:
    body: dict[str, Any] = {"error": {"code": code, "message": code.replace("_", " ")}}
    if details:
        body["error"]["details"] = details
    return httpx.Response(status, content=json.dumps(body).encode(),
                          headers={"content-type": "application/json"})
