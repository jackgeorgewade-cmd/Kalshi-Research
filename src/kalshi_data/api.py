"""High-level Python interface.

Two classes with deliberately separate responsibilities:

* :class:`KalshiData` talks to the API. Its ``iter_*``/``markets``/``market``
  methods stream results *without* storing them; ``collect``, ``resume``,
  ``discover``, ``snapshot_orderbook`` and ``poll`` persist into the local store
  (taking the single-writer lock while they run).
* :class:`LocalStore` only reads the local SQLite database. Nothing it does
  touches the network, so notebook queries are fast and reproducible.

Example::

    from kalshi_data import KalshiData, LocalStore

    with KalshiData() as kd:
        run = kd.collect(tickers=["SOME-TICKER"], start="2025-01-01", end="2025-02-01",
                         datasets=["markets", "trades", "candles"], candle_interval=60)
    with LocalStore() as db:
        df = db.to_dataframe("trades", tickers=["SOME-TICKER"])
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import VALID_INTERVALS, Settings, load_config
from .coverage import market_coverage, run_status
from .endpoints import KalshiClient, Tier
from .errors import ConfigError, NotFoundError
from .export import DATASETS, export, query_rows
from .http import RequestBudget, Transport
from .ingest import Ingestor, JobRequest, ProgressFn, RunResult
from .lock import WriterLock
from .normalize import (
    Record,
    parse_candle,
    parse_event,
    parse_market,
    parse_orderbook,
    parse_trade,
)
from .rawstore import RawStore
from .routing import Cutoff, candle_cells, market_tier, split_trade_window
from .storage import Store
from .timeutil import parse_api_time, parse_user_time, to_unix, utcnow, validate_range

log = logging.getLogger("kalshi_data.api")

TimeLike = str | datetime | date


def _range(start: TimeLike, end: TimeLike) -> tuple[datetime, datetime]:
    s, e = parse_user_time(start), parse_user_time(end)
    validate_range(s, e)
    return s, e


class KalshiData:
    """Remote retrieval and persistent collection.

    Args:
        settings: fully built settings; if omitted they are loaded with the
            documented precedence (``config_path`` TOML, env vars, defaults).
        config_path: optional TOML file.
        request_budget: cap on HTTP attempts for this object's lifetime.
        progress: optional callback ``(event_kind, data)`` for ingestion progress.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        config_path: Path | None = None,
        request_budget: int | None = None,
        progress: ProgressFn | None = None,
    ) -> None:
        self.settings = settings or load_config(config_path).settings
        budget = request_budget or self.settings.collection.request_budget
        self.transport = Transport(self.settings.api, budget=RequestBudget(budget))
        self.client = KalshiClient(self.transport, self.settings.api.page_limit)
        self.progress = progress

    def __enter__(self) -> KalshiData:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.transport.close()

    # ------------------------------------------------------- remote, no storage
    def cutoff(self) -> Cutoff:
        """Current ``/historical/cutoff`` values."""
        return Cutoff.from_body(self.client.historical_cutoff().json())

    def market(self, ticker: str) -> tuple[Record, Tier]:
        """Fetch one market's current metadata/quote, trying live then historical.

        Raises:
            NotFoundError: if neither tier has the ticker.
        """
        for tier in (Tier.LIVE, Tier.HISTORICAL):
            try:
                body = self.client.market(tier, ticker).json()
            except NotFoundError:
                continue
            rec, issues = parse_market(body.get("market"))
            if rec is None:
                raise ConfigError(f"market {ticker} failed validation: {issues}")
            return rec, tier
        raise NotFoundError(404, f"/markets/{ticker}", message="not found in either tier")

    def markets(
        self,
        *,
        series: str | None = None,
        event: str | None = None,
        include_historical: bool = True,
        max_pages: int = 20,
    ) -> list[Record]:
        """List markets for a series or event, merged across tiers by ticker.

        Each record gets a ``tier`` key. Raises ConfigError if ``max_pages``
        would truncate the listing (never truncates silently).
        """
        if not (series or event):
            raise ConfigError("give a series or an event (refusing to list the whole exchange)")
        merged: dict[str, Record] = {}
        tiers = [Tier.LIVE, Tier.HISTORICAL] if include_historical else [Tier.LIVE]
        for tier in tiers:
            last = None
            for page in self.client.market_pages(
                tier,
                series_ticker=series,
                event_ticker=None if series else event,
                max_pages=max_pages,
            ):
                last = page
                for item in page.items:
                    rec, _ = parse_market(item)
                    if rec is not None:
                        rec["tier"] = tier.value
                        merged.setdefault(rec["ticker"], rec)
            if last is not None and last.next_cursor:
                raise ConfigError(f"listing exceeded {max_pages} pages; raise max_pages")
        return sorted(merged.values(), key=lambda r: (r.get("close_time") or utcnow(), r["ticker"]))

    def events(self, series: str, *, status: str | None = None, max_pages: int = 1) -> list[Record]:
        """List events of a series (newest first as returned by the API), not stored.

        ``status`` is one of ``unopened``, ``open``, ``closed``, ``settled``.
        Events are never archived, so this covers old events too; their
        *markets* may be archived (use :meth:`markets` with ``event=``).
        """
        out: list[Record] = []
        for page in self.client.event_pages(
            series_ticker=series, status=status, max_pages=max_pages
        ):
            for item in page.items:
                rec, _ = parse_event(item)
                if rec is not None:
                    out.append(rec)
        return out

    def iter_trades(self, ticker: str, start: TimeLike, end: TimeLike) -> Iterator[Record]:
        """Stream trades in ``[start, end)`` (UTC) across tiers, newest first per tier.

        Nothing is stored. Records failing validation are skipped with a log
        warning (use ``collect`` to keep a quarantine record). Trades on the
        tier boundary are de-duplicated by ``trade_id``.
        """
        s, e = _range(start, end)
        cutoff = self.cutoff()
        boundary_ids: set[str] = set()
        near = timedelta(seconds=2)
        for tier, ps, pe in split_trade_window(s, e, cutoff):
            for page in self.client.trade_pages(tier, ticker, to_unix(ps) - 1, to_unix(pe) + 1):
                for item in page.items:
                    rec, issues = parse_trade(item)
                    if rec is None:
                        log.warning("skipping invalid trade: %s", [i.message for i in issues])
                        continue
                    t = rec["created_time"]
                    if not (s <= t < e):
                        continue
                    if abs(t - cutoff.trades_created) <= near:
                        if rec["trade_id"] in boundary_ids:
                            continue
                        boundary_ids.add(rec["trade_id"])
                    rec["tier"] = tier.value
                    yield rec

    def iter_candles(
        self, ticker: str, start: TimeLike, end: TimeLike, interval: int = 60
    ) -> Iterator[Record]:
        """Stream candles whose ``end_period_ts`` is in ``[start, end)``, oldest first.

        The tier is chosen from where the market's metadata is served
        (archived markets -> historical endpoint), never from candle dates.
        """
        if interval not in VALID_INTERVALS:
            raise ConfigError(f"interval must be one of {VALID_INTERVALS}")
        s, e = _range(start, end)
        rec, _found_in = self.market(ticker)
        tier = market_tier(rec["settlement_ts"], self.cutoff())
        series = None
        if tier is Tier.LIVE:
            ev = self.client.event(rec["event_ticker"]).json()
            series = ev.get("event", {}).get("series_ticker")
        for cs, ce in candle_cells(s, e, interval):
            body = self.client.candlesticks(
                tier, ticker, series, to_unix(cs), to_unix(ce) - 1, interval
            ).json()
            for item in body.get("candlesticks", []):
                c, issues = parse_candle(item, ticker, interval, tier.value)
                if c is None:
                    log.warning("skipping invalid candle: %s", [i.message for i in issues])
                    continue
                if to_unix(cs) <= c["end_period_ts"] < to_unix(ce):
                    c["tier"] = tier.value
                    yield c

    def orderbook(self, ticker: str, depth: int | None = None) -> list[Record]:
        """Current order book levels (bids only), not stored."""
        levels, issues = parse_orderbook(self.client.orderbook(ticker, depth).json(), ticker)
        if levels is None:
            raise ConfigError(f"invalid order book response: {issues}")
        return levels

    # ------------------------------------------------------------- persistent
    @contextmanager
    def _writer(self) -> Iterator[Ingestor]:
        st = self.settings.storage
        with WriterLock(st.lock_path), Store(st.db_path) as store:
            yield Ingestor(
                self.settings,
                store,
                RawStore(st.raw_dir, st.raw_retention),
                self.client,
                self.progress,
            )

    def collect(
        self,
        *,
        start: TimeLike,
        end: TimeLike,
        tickers: list[str] | None = None,
        series: list[str] | None = None,
        events: list[str] | None = None,
        datasets: list[str] | None = None,
        candle_interval: int = 60,
        kind: str = "collect",
    ) -> RunResult:
        """Download and store data for an explicit scope; returns the run result.

        Datasets: any of ``markets``, ``trades``, ``candles``, ``orderbook``.
        """
        s, e = _range(start, end)
        datasets = datasets or ["markets", "trades", "candles"]
        bad = set(datasets) - {"markets", "trades", "candles", "orderbook"}
        if bad:
            raise ConfigError(f"unknown datasets: {sorted(bad)}")
        if candle_interval not in VALID_INTERVALS:
            raise ConfigError(f"candle interval must be one of {VALID_INTERVALS}")
        if not (tickers or series or events):
            raise ConfigError("collection needs tickers, series or events")
        req = JobRequest(
            kind=kind,
            start=s,
            end=e,
            datasets=list(datasets),
            tickers=list(tickers or []),
            series=list(series or []),
            events=list(events or []),
            candle_interval=candle_interval,
        )
        with self._writer() as ing:
            return ing.run(req)

    def resume(self, run_id: str) -> RunResult:
        """Continue an unfinished run from its checkpoints."""
        with self._writer() as ing:
            row = ing.store.get_run(run_id)
            if row is None:
                raise ConfigError(f"unknown run {run_id}")
            import json

            req = JobRequest.from_json(json.loads(row["spec_json"]))
            return ing.run(req, run_id=run_id)

    def discover(
        self, query: str | None = None, category: str | None = None, tags: str | None = None
    ) -> list[Record]:
        """Fetch the series list (server-side ``category``/``tags`` filters only),
        cache it locally, and filter by keyword *locally* (Kalshi has no
        documented text-search parameter for series)."""
        with self._writer() as ing:
            series = ing.store_series_list(None, category=category, tags=tags)
        return filter_series(series, query)

    def snapshot_orderbook(self, ticker: str, depth: int | None = None) -> Record:
        """Fetch and store one order book snapshot plus the market quote."""
        with self._writer() as ing:
            info = ing.resolve_market(None, ticker)
            return ing.snapshot_orderbook(None, ticker, depth, info.notional)

    def poll(
        self,
        tickers: list[str],
        interval_seconds: float,
        *,
        duration_seconds: float | None = None,
        max_samples: int | None = None,
        depth: int | None = None,
        on_sample: Callable[[int, str, Record], None] | None = None,
    ) -> RunResult:
        """Bounded polling of order books (and market quotes) going forward.

        Exactly one of ``duration_seconds``/``max_samples`` bounds the loop
        (both may be given; whichever comes first stops it). Ctrl-C stops
        cleanly and the run is recorded as ``completed`` with a note. REST
        polling sees only the book at each request; updates in between are not
        captured, and nothing here reconstructs books before polling started.
        """
        if duration_seconds is None and max_samples is None:
            raise ConfigError("polling needs --duration or --samples (it must be bounded)")
        if interval_seconds < 1:
            raise ConfigError("poll interval must be at least 1 second")
        start = utcnow()
        req = JobRequest(
            kind="poll",
            start=start,
            end=start
            + timedelta(seconds=duration_seconds or interval_seconds * (max_samples or 1)),
            datasets=["orderbook"],
            tickers=list(tickers),
        )
        with self._writer() as ing:
            run_id = ing.start_run(req)
            ing.always_snapshot_markets = True
            samples, note = 0, None
            used0 = self.transport.budget.used
            try:
                infos = {t: ing.resolve_market(run_id, t) for t in tickers}
                t0 = time.monotonic()
                while True:
                    for t, info in infos.items():
                        rec = ing.snapshot_orderbook(run_id, t, depth, info.notional)
                        ing.resolve_market(run_id, t)  # quote snapshot alongside the book
                        if on_sample:
                            on_sample(samples + 1, t, rec)
                    samples += 1
                    if max_samples is not None and samples >= max_samples:
                        break
                    if duration_seconds is not None and (
                        time.monotonic() - t0 + interval_seconds > duration_seconds
                    ):
                        break
                    time.sleep(interval_seconds)
            except KeyboardInterrupt:
                note = f"stopped by user after {samples} samples"
            used = self.transport.budget.used - used0
            with ing.store.transaction():
                ing.store.finish_run(
                    run_id, "completed", used, note or f"{samples} samples collected"
                )
            return RunResult(run_id, "completed", note, {"samples": samples}, used)


def filter_series(series: list[Record], query: str | None) -> list[Record]:
    """Case-insensitive local keyword match on ticker, title, category and tags.

    All whitespace-separated words must match (AND).
    """
    if not query:
        return series
    words = query.lower().split()
    out = []
    for s in series:
        hay = " ".join(
            str(s.get(k) or "") for k in ("ticker", "title", "category", "tags", "categories")
        ).lower()
        if all(w in hay for w in words):
            out.append(s)
    return out


class LocalStore:
    """Read-only access to the local research database (no network).

    Args:
        data_dir: data directory (defaults to settings: ``./data``).
    """

    def __init__(
        self, data_dir: Path | str | None = None, settings: Settings | None = None
    ) -> None:
        settings = settings or load_config().settings
        root = Path(data_dir) if data_dir else settings.storage.data_dir
        self.data_dir = root
        self.store = Store(root / "kalshi.sqlite", readonly=True)

    def __enter__(self) -> LocalStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.store.close()

    def rows(
        self,
        dataset: str,
        *,
        tickers: list[str] | None = None,
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        interval: int | None = None,
        typed: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows of ``trades``, ``candles``, ``markets`` or ``orderbook``.

        With ``typed=True`` decimal columns are ``Decimal`` and timestamps are
        aware ``datetime``; NULL stays ``None``.
        """
        s = parse_user_time(start) if start is not None else None
        e = parse_user_time(end) if end is not None else None
        if dataset not in DATASETS:
            raise ConfigError(f"unknown dataset {dataset!r}; choose from {sorted(DATASETS)}")
        _sql, dec, ts, _t = DATASETS[dataset]
        for row in query_rows(
            self.store, dataset, tickers=tickers, start=s, end=e, interval=interval
        ):
            if typed:
                for k in dec:
                    if row.get(k) is not None:
                        row[k] = Decimal(row[k])
                for k in ts:
                    if row.get(k) is not None:
                        row[k] = parse_api_time(row[k])
            yield row

    def to_dataframe(self, dataset: str, *, numeric: bool = False, **filters: Any) -> Any:
        """Load a dataset into pandas.

        Args:
            numeric: if False (default) decimal columns hold exact ``Decimal``
                objects (dtype ``object``). If True they are converted to
                ``float64`` - convenient for statistics but **lossy**; do it
                knowingly. NULL becomes NaN in numeric mode, None otherwise.
        """
        import pandas as pd

        df = pd.DataFrame(list(self.rows(dataset, **filters)))
        if numeric and not df.empty:
            for c in DATASETS[dataset][1]:
                if c in df.columns:
                    df[c] = pd.to_numeric(
                        df[c].map(lambda v: None if v is None else str(v)), errors="raise"
                    )
        return df

    def search(self, query: str) -> dict[str, list[dict[str, Any]]]:
        """Keyword search over locally cached series, events and markets titles."""
        like = f"%{query}%"
        conn = self.store.conn
        return {
            "series": [
                dict(r)
                for r in conn.execute(
                    "SELECT ticker, title, category, tags FROM series WHERE ticker LIKE ? OR "
                    "title LIKE ? OR tags LIKE ? ORDER BY ticker",
                    (like, like, like),
                )
            ],
            "events": [
                dict(r)
                for r in conn.execute(
                    "SELECT event_ticker, series_ticker, title FROM events "
                    "WHERE event_ticker LIKE ? OR title LIKE ? ORDER BY event_ticker",
                    (like, like),
                )
            ],
            "markets": [
                dict(r)
                for r in conn.execute(
                    "SELECT ticker, event_ticker, title, status, close_time FROM markets WHERE "
                    "ticker LIKE ? OR title LIKE ? OR yes_sub_title LIKE ? ORDER BY ticker",
                    (like, like, like),
                )
            ],
        }

    def coverage(self, ticker: str, run_id: str | None = None) -> dict[str, Any]:
        """Coverage report for one market (see :mod:`kalshi_data.coverage`)."""
        return market_coverage(self.store, ticker, run_id)

    def run_status(self, run_id: str) -> dict[str, Any]:
        return run_status(self.store, run_id)

    def runs(self, limit: int = 50) -> list[dict[str, Any]]:
        return [dict(r) for r in self.store.list_runs(limit)]

    def issues(self, run_id: str | None = None) -> list[dict[str, Any]]:
        q = "SELECT * FROM validation_issues"
        args: tuple[Any, ...] = ()
        if run_id:
            q += " WHERE run_id=?"
            args = (run_id,)
        return [dict(r) for r in self.store.conn.execute(q + " ORDER BY issue_id", args)]

    def export(self, dataset: str, output: Path | str, fmt: str = "parquet", **filters: Any) -> int:
        """Export to CSV/Parquet; returns rows written (see :mod:`kalshi_data.export`)."""
        for k in ("start", "end"):
            if isinstance(filters.get(k), str | date):
                filters[k] = parse_user_time(filters[k])
        return export(self.store, dataset, Path(output), fmt, **filters)
