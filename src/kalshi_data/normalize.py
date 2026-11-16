"""Convert raw API items into normalized records.

Design rules:

* Prices/quantities become ``Decimal`` via :func:`to_decimal` (never float).
* Missing optional values stay ``None``; nothing is zero-filled or forward-filled.
* Each parser returns ``(record_or_None, issues)``. An **error** issue means an
  essential field is missing/malformed; the record is *quarantined* (stored in
  ``validation_issues`` with its raw JSON) instead of being written. A
  **warning** means the record is usable but something is odd (e.g. a field
  we could not parse is left NULL, or an unexpected value).
* Field sets differ by endpoint. In particular the historical candlestick
  schema uses ``open``/``volume``/``open_interest`` while the live schema uses
  ``open_dollars``/``volume_fp``/``open_interest_fp``; each has an explicit adapter.

Bump ``PARSER_VERSION`` whenever normalization output changes so stored rows
can be traced to the parser that produced them.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .decimals import dumps_exact, to_decimal
from .timeutil import from_unix, parse_api_time

PARSER_VERSION = "1"

Record = dict[str, Any]


@dataclass
class Issue:
    """A validation finding for one raw record."""

    severity: str  # "error" (record quarantined) or "warning"
    code: str
    message: str
    record_key: str | None = None
    raw: Any = None


@dataclass
class _Ctx:
    """Collects issues while parsing a single record."""

    dataset: str
    key: str | None
    raw: Any
    issues: list[Issue] = field(default_factory=list)

    def error(self, code: str, msg: str) -> None:
        self.issues.append(Issue("error", code, f"{self.dataset}: {msg}", self.key, self.raw))

    def warn(self, code: str, msg: str) -> None:
        self.issues.append(Issue("warning", code, f"{self.dataset}: {msg}", self.key, self.raw))

    @property
    def failed(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    def dec(
        self, obj: dict[str, Any], name: str, *, essential: bool = False, label: str | None = None
    ) -> Decimal | None:
        label = label or name
        if name not in obj or obj[name] is None:
            if essential:
                self.error("missing_field", f"essential field '{label}' missing or null")
            return None
        try:
            return to_decimal(obj[name])
        except ValueError as exc:
            if essential:
                self.error("malformed_field", f"'{label}' not a decimal: {exc}")
            else:
                self.warn("malformed_field", f"'{label}' not a decimal, stored NULL: {exc}")
            return None

    def time(self, obj: dict[str, Any], name: str, *, essential: bool = False) -> datetime | None:
        value = obj.get(name)
        try:
            parsed = parse_api_time(value) if isinstance(value, str | type(None)) else None
            if value is not None and not isinstance(value, str):
                raise ValueError(f"expected string, got {type(value).__name__}")
        except ValueError as exc:
            (self.error if essential else self.warn)(
                "malformed_field", f"'{name}' not a timestamp: {exc}"
            )
            return None
        if parsed is None and essential:
            self.error("missing_field", f"essential field '{name}' missing or empty")
        return parsed

    def text(self, obj: dict[str, Any], name: str, *, essential: bool = False) -> str | None:
        value = obj.get(name)
        if value is None or value == "":
            if essential:
                self.error("missing_field", f"essential field '{name}' missing or empty")
            return None if value is None else ""
        if not isinstance(value, str):
            (self.error if essential else self.warn)(
                "malformed_field", f"'{name}' expected string, got {type(value).__name__}"
            )
            return None
        return value


def _json_or_none(value: Any) -> str | None:
    return None if value is None else dumps_exact(value, sort_keys=True)


def _bool_or_none(ctx: _Ctx, obj: dict[str, Any], name: str) -> bool | None:
    v = obj.get(name)
    if v is None or isinstance(v, bool):
        return v
    ctx.warn("malformed_field", f"'{name}' expected boolean")
    return None


def _check_price(ctx: _Ctx, name: str, value: Decimal | None) -> None:
    if value is not None and not (Decimal(0) <= value <= Decimal(1)):
        ctx.warn("out_of_range", f"{name}={value} outside [0, 1] dollars")


# --------------------------------------------------------------------------
# Known field sets, used to report (not drop) unknown fields.
# --------------------------------------------------------------------------
TRADE_FIELDS = frozenset(
    {
        "trade_id",
        "ticker",
        "count_fp",
        "yes_price_dollars",
        "no_price_dollars",
        "taker_side",
        "taker_outcome_side",
        "taker_book_side",
        "created_time",
        "is_block_trade",
    }
)
MARKET_SCALAR_FIELDS = (
    "market_type",
    "title",
    "subtitle",
    "yes_sub_title",
    "no_sub_title",
    "status",
    "result",
    "strike_type",
    "functional_strike",
    "rules_primary",
    "rules_secondary",
    "price_level_structure",
    "expiration_value",
    "mve_collection_ticker",
    "settlement_bounds_type",
    "early_close_condition",
)
MARKET_TIME_FIELDS = (
    "created_time",
    "open_time",
    "close_time",
    "expected_expiration_time",
    "latest_expiration_time",
    "expiration_time",
    "settlement_ts",
    "occurrence_datetime",
    "updated_time",
    "fee_waiver_expiration_time",
)
MARKET_DECIMAL_FIELDS = (
    "floor_strike",
    "cap_strike",
    "notional_value_dollars",
    "settlement_value_dollars",
    "settlement_floor_dollars",
    "last_price_dollars",
    "previous_price_dollars",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "no_bid_dollars",
    "no_ask_dollars",
    "previous_yes_bid_dollars",
    "previous_yes_ask_dollars",
    "yes_bid_size_fp",
    "yes_ask_size_fp",
    "volume_fp",
    "volume_24h_fp",
    "open_interest_fp",
)
MARKET_OTHER_FIELDS = frozenset(
    {
        "ticker",
        "event_ticker",
        "custom_strike",
        "price_ranges",
        "can_close_early",
        "settlement_timer_seconds",
        "mve_selected_legs",
        "primary_participant_key",
        "is_provisional",
        "exchange_index",
    }
)
MARKET_FIELDS = (
    frozenset(MARKET_SCALAR_FIELDS)
    | frozenset(MARKET_TIME_FIELDS)
    | frozenset(MARKET_DECIMAL_FIELDS)
    | MARKET_OTHER_FIELDS
)
CANDLE_LIVE_FIELDS = frozenset(
    {"end_period_ts", "yes_bid", "yes_ask", "price", "volume_fp", "open_interest_fp"}
)
CANDLE_HIST_FIELDS = frozenset(
    {"end_period_ts", "yes_bid", "yes_ask", "price", "volume", "open_interest"}
)


def unknown_fields(item: dict[str, Any], known: frozenset[str]) -> list[str]:
    """Top-level keys we do not map (kept in raw files; reported as warnings)."""
    return sorted(k for k in item if k not in known)


# --------------------------------------------------------------------------
# Parsers
# --------------------------------------------------------------------------
def parse_trade(item: Any) -> tuple[Record | None, list[Issue]]:
    """Normalize one trade from /markets/trades or /historical/trades.

    Units: ``yes_price``/``no_price`` in dollars per contract, ``count`` in
    contracts (may be fractional, e.g. ``34.46``, observed live).
    """
    if not isinstance(item, dict):
        ctx = _Ctx("trade", None, item)
        ctx.error("malformed_record", "trade is not a JSON object")
        return None, ctx.issues
    ctx = _Ctx("trade", str(item.get("trade_id")), item)
    rec: Record = {
        "trade_id": ctx.text(item, "trade_id", essential=True),
        "ticker": ctx.text(item, "ticker", essential=True),
        "created_time": ctx.time(item, "created_time", essential=True),
        "count": ctx.dec(item, "count_fp", essential=True),
        "yes_price": ctx.dec(item, "yes_price_dollars", essential=True),
        "no_price": ctx.dec(item, "no_price_dollars"),
        "taker_outcome_side": ctx.text(item, "taker_outcome_side"),
        "taker_book_side": ctx.text(item, "taker_book_side"),
        "taker_side_deprecated": ctx.text(item, "taker_side"),
        "is_block_trade": _bool_or_none(ctx, item, "is_block_trade"),
    }
    if ctx.failed:
        return None, ctx.issues
    _check_price(ctx, "yes_price", rec["yes_price"])
    _check_price(ctx, "no_price", rec["no_price"])
    if rec["count"] is not None and rec["count"] <= 0:
        ctx.warn("out_of_range", f"non-positive count {rec['count']}")
    if (
        rec["yes_price"] is not None
        and rec["no_price"] is not None
        and (rec["yes_price"] + rec["no_price"] != Decimal(1))
    ):
        ctx.warn("price_mismatch", "yes_price + no_price != 1.00 (check notional)")
    return rec, ctx.issues


def parse_market(item: Any) -> tuple[Record | None, list[Issue]]:
    """Normalize one market object (live or historical; same documented schema).

    ``floor_strike``/``cap_strike`` arrive as JSON numbers and are decoded as
    Decimal by :func:`loads_exact`. ``expiration_time`` is undocumented but
    observed live; it is kept separately from the documented
    ``latest_expiration_time`` / ``expected_expiration_time``.
    """
    if not isinstance(item, dict):
        ctx = _Ctx("market", None, item)
        ctx.error("malformed_record", "market is not a JSON object")
        return None, ctx.issues
    ctx = _Ctx("market", str(item.get("ticker")), item)
    rec: Record = {
        "ticker": ctx.text(item, "ticker", essential=True),
        "event_ticker": ctx.text(item, "event_ticker", essential=True),
    }
    for name in MARKET_SCALAR_FIELDS:
        rec[name] = ctx.text(item, name)
    for name in MARKET_TIME_FIELDS:
        rec[name] = ctx.time(item, name)
    for name in MARKET_DECIMAL_FIELDS:
        rec[name] = ctx.dec(item, name)
    rec["custom_strike"] = _json_or_none(item.get("custom_strike"))
    rec["price_ranges"] = _json_or_none(item.get("price_ranges"))
    rec["can_close_early"] = _bool_or_none(ctx, item, "can_close_early")
    if ctx.failed:
        return None, ctx.issues
    return rec, ctx.issues


def parse_series(item: Any) -> tuple[Record | None, list[Issue]]:
    """Normalize one series object."""
    if not isinstance(item, dict):
        ctx = _Ctx("series", None, item)
        ctx.error("malformed_record", "series is not a JSON object")
        return None, ctx.issues
    ctx = _Ctx("series", str(item.get("ticker")), item)
    rec: Record = {
        "ticker": ctx.text(item, "ticker", essential=True),
        "title": ctx.text(item, "title"),
        "category": ctx.text(item, "category"),
        "categories": _json_or_none(item.get("categories")),
        "tags": _json_or_none(item.get("tags")),
        "frequency": ctx.text(item, "frequency"),
        "settlement_sources": _json_or_none(item.get("settlement_sources")),
        "contract_url": ctx.text(item, "contract_url"),
        "contract_terms_url": ctx.text(item, "contract_terms_url"),
        "last_updated_ts": ctx.time(item, "last_updated_ts"),
    }
    return (None if ctx.failed else rec), ctx.issues


def parse_event(item: Any) -> tuple[Record | None, list[Issue]]:
    """Normalize one event object. ``series_ticker`` comes from the API
    (observed: legacy event INXD-23DEC29 reports series KXINX, so ticker
    prefixes are not a reliable guide)."""
    if not isinstance(item, dict):
        ctx = _Ctx("event", None, item)
        ctx.error("malformed_record", "event is not a JSON object")
        return None, ctx.issues
    ctx = _Ctx("event", str(item.get("event_ticker")), item)
    rec: Record = {
        "event_ticker": ctx.text(item, "event_ticker", essential=True),
        "series_ticker": ctx.text(item, "series_ticker"),
        "title": ctx.text(item, "title"),
        "sub_title": ctx.text(item, "sub_title"),
        "category": ctx.text(item, "category"),
        "strike_date": ctx.time(item, "strike_date"),
        "strike_period": ctx.text(item, "strike_period"),
        "mutually_exclusive": _bool_or_none(ctx, item, "mutually_exclusive"),
        "collateral_return_type": ctx.text(item, "collateral_return_type"),
        "settlement_sources": _json_or_none(item.get("settlement_sources")),
        "last_updated_ts": ctx.time(item, "last_updated_ts"),
    }
    return (None if ctx.failed else rec), ctx.issues


_OHLC = ("open", "high", "low", "close")


def _ohlc(
    ctx: _Ctx,
    obj: Any,
    prefix: str,
    suffix: str,
    fields: tuple[str, ...],
    out: Record,
    out_prefix: str,
) -> None:
    """Copy an OHLC sub-object; a missing/empty object yields NULLs (observed
    live: ``"price": {}`` when no trade occurred in the period)."""
    if obj is None:
        obj = {}
    if not isinstance(obj, dict):
        ctx.warn("malformed_field", f"'{prefix}' is not an object; stored NULL")
        obj = {}
    for f in fields:
        out[f"{out_prefix}_{f}"] = ctx.dec(obj, f + suffix, label=f"{prefix}.{f}{suffix}")


CANDLE_TRADE_FIELDS = ("open", "high", "low", "close", "mean", "previous", "min", "max")


def parse_candle(
    item: Any, ticker: str, interval: int, schema: str
) -> tuple[Record | None, list[Issue]]:
    """Normalize one candlestick.

    Args:
        item: raw candlestick object.
        ticker: market ticker (candles do not repeat it per item).
        interval: period length in minutes (1, 60, 1440).
        schema: ``"live"`` (``*_dollars``, ``volume_fp``) or ``"historical"``
            (``open``, ``volume``) - the two documented shapes.

    Output columns keep traded-price, YES-bid and YES-ask OHLC separate:
    ``trade_*``, ``yes_bid_*``, ``yes_ask_*``. ``trade_*`` is NULL when no
    trade happened in the period. ``trade_previous`` is the last traded price
    *before* the period (a carried value, not an observation in the period).
    ``trade_min``/``trade_max`` exist only in the live schema.
    """
    if schema not in ("live", "historical"):
        raise ValueError(schema)
    if not isinstance(item, dict):
        ctx = _Ctx("candle", None, item)
        ctx.error("malformed_record", "candle is not a JSON object")
        return None, ctx.issues
    end_ts = item.get("end_period_ts")
    ctx = _Ctx("candle", f"{ticker}|{interval}|{end_ts}", item)
    if not isinstance(end_ts, int) or isinstance(end_ts, bool):
        ctx.error("missing_field", "end_period_ts missing or not an integer")
        return None, ctx.issues
    rec: Record = {
        "ticker": ticker,
        "interval_minutes": interval,
        "end_period_ts": end_ts,
        "end_period_time": from_unix(end_ts),
        "source_schema": schema,
    }
    sfx = "_dollars" if schema == "live" else ""
    trade_fields = CANDLE_TRADE_FIELDS if schema == "live" else CANDLE_TRADE_FIELDS[:6]
    _ohlc(ctx, item.get("price"), "price", sfx, trade_fields, rec, "trade")
    if schema == "historical":
        rec["trade_min"] = rec["trade_max"] = None
    for side in ("yes_bid", "yes_ask"):
        if item.get(side) is None:
            ctx.warn("missing_field", f"'{side}' missing; stored NULL")
        _ohlc(ctx, item.get(side), side, sfx, _OHLC, rec, side)
    vol_name = "volume_fp" if schema == "live" else "volume"
    oi_name = "open_interest_fp" if schema == "live" else "open_interest"
    rec["volume"] = ctx.dec(item, vol_name)
    rec["open_interest"] = ctx.dec(item, oi_name)
    if vol_name not in item:
        # Schema mismatch is the most likely cause (e.g. wrong adapter).
        other = "volume" if schema == "live" else "volume_fp"
        if other in item:
            ctx.error(
                "schema_mismatch", f"found '{other}' but parsing with the {schema} candle schema"
            )
            return None, ctx.issues
    for k in [k for k in rec if k.startswith(("trade_", "yes_bid_", "yes_ask_"))]:
        _check_price(ctx, k, rec[k])
    return rec, ctx.issues


def parse_orderbook(body: Any, ticker: str) -> tuple[list[Record] | None, list[Issue]]:
    """Normalize ``{"orderbook_fp": {"yes_dollars": [[price, qty], ...], ...}}``.

    Kalshi returns *bids only*: ``yes`` levels are bids to buy YES and ``no``
    levels are bids to buy NO, each sorted ascending (best bid last). Asks are
    not returned; any ask we compute is derived (see storage) and labelled so.
    Returns level records with ``side``, ``price``, ``quantity``, ``rank``
    (1 = best bid on that side).
    """
    ctx = _Ctx("orderbook", ticker, body)
    book = body.get("orderbook_fp") if isinstance(body, dict) else None
    if not isinstance(book, dict):
        ctx.error("missing_field", "'orderbook_fp' object missing")
        return None, ctx.issues
    levels: list[Record] = []
    for side, key in (("yes", "yes_dollars"), ("no", "no_dollars")):
        raw_levels = book.get(key)
        if raw_levels is None:
            raw_levels = []
        if not isinstance(raw_levels, list):
            ctx.error("malformed_field", f"'{key}' is not a list")
            return None, ctx.issues
        n = len(raw_levels)
        for i, lvl in enumerate(raw_levels):
            if not (isinstance(lvl, list) and len(lvl) == 2):
                ctx.error("malformed_field", f"{key}[{i}] is not a [price, quantity] pair")
                return None, ctx.issues
            try:
                price, qty = to_decimal(lvl[0]), to_decimal(lvl[1])
            except ValueError as exc:
                ctx.error("malformed_field", f"{key}[{i}]: {exc}")
                return None, ctx.issues
            levels.append({"side": side, "price": price, "quantity": qty, "rank": n - i})
    return levels, ctx.issues


def raw_json(item: Any) -> str:
    """Exact JSON text of a raw item for quarantine storage."""
    return dumps_exact(item, sort_keys=True)


def content_hash(rec: Record, exclude: tuple[str, ...] = ()) -> str:
    """Stable hash of a normalized record, used for change/conflict detection."""
    import hashlib

    def norm(v: Any) -> Any:
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, Decimal):
            return str(v)
        return v

    payload = {k: norm(v) for k, v in sorted(rec.items()) if k not in exclude}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


Parser = Callable[[Any], tuple[Record | None, list[Issue]]]
