"""Thin adapters for the Kalshi REST endpoints this project uses.

Each adapter knows the path, the documented query parameters and the JSON
key holding the items. Adapters return :class:`ResponseRecord` objects (so
callers can persist raw bodies for provenance) rather than parsed models;
normalization lives in :mod:`kalshi_data.normalize`.

All endpoints used here are public and were called without credentials
(see docs/api_findings.md).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .errors import PaginationError, ResponseFormatError
from .http import ResponseRecord, Transport


class Tier(StrEnum):
    """Which Kalshi data partition an endpoint reads from."""

    LIVE = "live"
    HISTORICAL = "historical"


@dataclass
class Page:
    """One page of a cursor-paginated listing."""

    record: ResponseRecord
    items: list[dict[str, Any]]
    next_cursor: str | None  # None means the API signalled the end
    index: int  # 0-based page number within this iteration


def paginate(
    transport: Transport,
    path: str,
    params: dict[str, Any],
    items_key: str,
    *,
    start_cursor: str | None = None,
    max_pages: int | None = None,
    identity: Callable[[dict[str, Any]], str] | None = None,
) -> Iterator[Page]:
    """Follow Kalshi cursors until exhaustion.

    Rules (documented behaviour plus defensive checks):

    * The same filters are re-sent on every page; only ``cursor`` changes.
    * Completion is signalled only by an empty/absent cursor - a short or even
      empty page with a cursor is *not* treated as the end.
    * A cursor that repeats raises :class:`PaginationError` (infinite loop guard).
    * Kalshi documents that an undecodable cursor is silently ignored and the
      first page returned. If ``identity`` is given and a later page starts with
      the same item as page 0, :class:`PaginationError` is raised.
    * ``max_pages`` stops early; the last yielded page then still carries a
      non-None ``next_cursor`` so the caller can record the work as incomplete.
    """
    cursor = start_cursor
    seen: set[str] = set()
    if cursor:
        seen.add(cursor)
    first_identity: str | None = None
    index = 0
    while True:
        query = dict(params)
        if cursor:
            query["cursor"] = cursor
        record = transport.get(path, query)
        body = record.json()
        if not isinstance(body, dict) or not isinstance(body.get(items_key), list):
            raise ResponseFormatError(f"{path}: expected a '{items_key}' list in response")
        items: list[dict[str, Any]] = body[items_key]
        if identity is not None and items:
            ident = identity(items[0])
            if index == 0 and not start_cursor:
                first_identity = ident
            elif first_identity is not None and ident == first_identity:
                raise PaginationError(
                    f"{path}: page {index} restarted at the first item; cursor was not honoured"
                )
        raw_next = body.get("cursor")
        next_cursor = raw_next if isinstance(raw_next, str) and raw_next else None
        if next_cursor is not None and next_cursor in seen:
            raise PaginationError(f"{path}: cursor repeated at page {index}; stopping")
        if next_cursor is not None:
            seen.add(next_cursor)
        yield Page(record=record, items=items, next_cursor=next_cursor, index=index)
        index += 1
        if next_cursor is None:
            return
        if max_pages is not None and index >= max_pages:
            return
        cursor = next_cursor


class KalshiClient:
    """Endpoint adapters bound to a :class:`Transport`."""

    def __init__(self, transport: Transport, page_limit: int = 1000) -> None:
        self.transport = transport
        self.page_limit = page_limit

    # ---- exchange-wide -------------------------------------------------
    def historical_cutoff(self) -> ResponseRecord:
        """GET /historical/cutoff - per-dataset archive boundaries."""
        return self.transport.get("/historical/cutoff")

    def exchange_status(self) -> ResponseRecord:
        """GET /exchange/status - cheap reachability probe."""
        return self.transport.get("/exchange/status")

    # ---- series / events ----------------------------------------------
    def series_pages(
        self, category: str | None = None, tags: str | None = None
    ) -> Iterator[Page]:
        """GET /series. Observed live: returns every matching series in one
        response without a cursor; cursors are still followed if present."""
        params = {"category": category, "tags": tags}
        return paginate(self.transport, "/series", params, "series",
                        identity=lambda s: str(s.get("ticker")))

    def series(self, series_ticker: str) -> ResponseRecord:
        """GET /series/{series_ticker}."""
        return self.transport.get(f"/series/{series_ticker}")

    def event_pages(
        self,
        series_ticker: str | None = None,
        status: str | None = None,
        max_pages: int | None = None,
    ) -> Iterator[Page]:
        """GET /events (limit max 200). Events are never archived."""
        params = {"series_ticker": series_ticker, "status": status, "limit": 200}
        return paginate(self.transport, "/events", params, "events", max_pages=max_pages,
                        identity=lambda e: str(e.get("event_ticker")))

    def event(self, event_ticker: str) -> ResponseRecord:
        """GET /events/{event_ticker} (markets returned top-level; archived
        markets settled before the cutoff are NOT included)."""
        return self.transport.get(f"/events/{event_ticker}")

    # ---- markets -------------------------------------------------------
    def market_pages(
        self,
        tier: Tier,
        *,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        tickers: list[str] | None = None,
        status: str | None = None,
        max_pages: int | None = None,
    ) -> Iterator[Page]:
        """GET /markets or /historical/markets with the given filters.

        The historical endpoint documents its filters as mutually exclusive
        and has no ``status`` filter.
        """
        params: dict[str, Any] = {"limit": self.page_limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if tickers:
            params["tickers"] = ",".join(tickers)
        if status and tier is Tier.LIVE:
            params["status"] = status
        n_filters = sum(bool(x) for x in (series_ticker, event_ticker, tickers))
        if tier is Tier.HISTORICAL and n_filters > 1:
            raise ValueError("/historical/markets filters are mutually exclusive")
        path = "/markets" if tier is Tier.LIVE else "/historical/markets"
        return paginate(self.transport, path, params, "markets", max_pages=max_pages,
                        identity=lambda m: str(m.get("ticker")))

    def market(self, tier: Tier, ticker: str) -> ResponseRecord:
        """GET /markets/{ticker} or /historical/markets/{ticker} (404 if absent)."""
        prefix = "/markets" if tier is Tier.LIVE else "/historical/markets"
        return self.transport.get(f"{prefix}/{ticker}")

    # ---- trades --------------------------------------------------------
    def trade_pages(
        self,
        tier: Tier,
        ticker: str,
        min_ts: int,
        max_ts: int,
        *,
        start_cursor: str | None = None,
        max_pages: int | None = None,
    ) -> Iterator[Page]:
        """GET /markets/trades or /historical/trades for one market.

        ``min_ts``/``max_ts`` are integer Unix seconds. Observed live: they are
        compared with the sub-second ``created_time``; callers pad by one second
        and filter locally to the exact half-open window. Results are newest first.
        """
        path = "/markets/trades" if tier is Tier.LIVE else "/historical/trades"
        params = {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts,
                  "limit": self.page_limit}
        return paginate(self.transport, path, params, "trades", start_cursor=start_cursor,
                        max_pages=max_pages, identity=lambda t: str(t.get("trade_id")))

    # ---- candlesticks --------------------------------------------------
    def candlesticks(
        self,
        tier: Tier,
        ticker: str,
        series_ticker: str | None,
        start_ts: int,
        end_ts: int,
        period_interval: int,
    ) -> ResponseRecord:
        """Live: GET /series/{series}/markets/{ticker}/candlesticks.
        Historical: GET /historical/markets/{ticker}/candlesticks.

        Both bounds are inclusive on ``end_period_ts`` (observed live). At most
        5,000 periods per request (documented, confirmed live: 5,001 -> 400).
        ``include_latest_before_start`` is deliberately never sent so no
        synthetic continuity candle is created.
        """
        params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval}
        if tier is Tier.LIVE:
            if not series_ticker:
                raise ValueError("live candlesticks require the market's series ticker")
            return self.transport.get(
                f"/series/{series_ticker}/markets/{ticker}/candlesticks", params)
        return self.transport.get(f"/historical/markets/{ticker}/candlesticks", params)

    # ---- order book ----------------------------------------------------
    def orderbook(self, ticker: str, depth: int | None = None) -> ResponseRecord:
        """GET /markets/{ticker}/orderbook (current bids only; 200 with empty
        arrays for unknown tickers, per docs)."""
        params = {"depth": depth} if depth else None
        return self.transport.get(f"/markets/{ticker}/orderbook", params)
