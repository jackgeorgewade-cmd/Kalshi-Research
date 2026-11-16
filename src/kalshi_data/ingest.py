"""Ingestion runs: planning, execution, checkpointing and resume.

Transaction order for every API response (the crash-safety contract):

1. The HTTP response is received in memory.
2. The raw body is written atomically to ``raw/<run>/<request>.json.gz``.
3. ONE SQLite transaction inserts the ``requests`` row, the normalized
   records, any validation issues, and advances the work-unit checkpoint
   (cursor / phase / status).

A checkpoint therefore never moves past data that is not durable. A crash
between 2 and 3 leaves an orphan raw file (reported by ``doctor``) and the
page is simply fetched again on resume; inserts are idempotent.

Work units (one row in ``work_units``) are identified by a deterministic key
``dataset|ticker|interval|window_start|window_end``. A unit is skipped when
another run already completed the same key *and* that completion happened at
least ``overlap_seconds`` after the window ended (so late trades / revised
candles had time to settle). Otherwise it is re-fetched; duplicates are
removed by primary keys and candle revisions are versioned.

Unit states: ``pending``, ``running``, ``complete`` (rows may be zero: see
``rows_received``), ``skipped`` (identical final unit exists), ``failed``
(request error), ``not_found`` (market absent from both tiers),
``incomplete`` (stopped by the request budget or interrupted).
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .config import Settings
from .decimals import dumps_exact
from .endpoints import KalshiClient, Page, Tier
from .errors import (
    APIError,
    BadRequestError,
    BudgetExhaustedError,
    ConfigError,
    CursorRejectedError,
    KalshiDataError,
    NotFoundError,
    PaginationError,
)
from .http import ResponseRecord
from .normalize import (
    MARKET_FIELDS,
    PARSER_VERSION,
    TRADE_FIELDS,
    Issue,
    Record,
    parse_candle,
    parse_event,
    parse_market,
    parse_orderbook,
    parse_series,
    parse_trade,
    unknown_fields,
)
from .rawstore import RawStore, body_sha256
from .routing import (
    Cutoff,
    candle_cells,
    clip_to_market_life,
    market_tier,
    split_trade_window,
    trade_cells,
)
from .storage import Store
from .timeutil import iso, parse_api_time, to_unix, utcnow

log = logging.getLogger("kalshi_data.ingest")

CUTOFF_REFRESH_SECONDS = 300


@dataclass
class JobRequest:
    """What a run should collect. Stored as JSON with the run (used by resume)."""

    kind: str
    start: datetime
    end: datetime
    datasets: list[str]
    tickers: list[str] = field(default_factory=list)
    series: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    candle_interval: int = 60
    resolved_tickers: list[str] | None = None

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["start"], d["end"] = iso(self.start), iso(self.end)
        return d

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> JobRequest:
        d = dict(d)
        d["start"] = parse_api_time(d["start"])
        d["end"] = parse_api_time(d["end"])
        return cls(**d)


@dataclass
class MarketInfo:
    """What the engine needs to know about a market to route requests."""

    ticker: str
    event_ticker: str
    series_ticker: str | None
    found_tier: Tier
    created: datetime | None
    settled: datetime | None
    status: str | None
    notional: Decimal | None


@dataclass
class RunResult:
    run_id: str
    status: str
    message: str | None
    units: dict[str, int]
    requests_used: int


ProgressFn = Callable[[str, dict[str, Any]], None]


def unit_key(dataset: str, ticker: str, interval: int | None, start: datetime,
             end: datetime) -> str:
    """Deterministic identity of a bounded unit of work."""
    return f"{dataset}|{ticker}|{interval or '-'}|{iso(start)}|{iso(end)}"


def new_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


class Ingestor:
    """Executes ingestion runs against one store."""

    def __init__(self, settings: Settings, store: Store, raw: RawStore, client: KalshiClient,
                 progress: ProgressFn | None = None) -> None:
        self.settings = settings
        self.store = store
        self.raw = raw
        self.client = client
        self.progress = progress or (lambda kind, data: None)
        self._cutoff: Cutoff | None = None
        self._cutoff_at: datetime | None = None
        self._events_done: set[str] = set()
        self._unknown_reported: set[tuple[str, str]] = set()
        # When True every market observation is kept in market_snapshots (used by
        # polling so quote history is preserved); otherwise only content changes.
        self.always_snapshot_markets = False

    # ------------------------------------------------------------ plumbing
    def _persist(self, run_id: str | None, record: ResponseRecord, tier: str | None,
                 item_count: int | None, write: Callable[[], None]) -> None:
        """Raw file first, then one transaction (request row + data + checkpoint)."""
        raw_path = self.raw.write(run_id or "adhoc", record, PARSER_VERSION)
        with self.store.transaction():
            self.store.insert_request(
                request_id=record.request_id, run_id=run_id, path=record.path,
                params=record.params, url=record.url, status=record.status,
                requested_at=record.requested_at, received_at=record.received_at,
                attempts=record.attempts, tier=tier, raw_path=raw_path,
                sha256=body_sha256(record.body), body_bytes=len(record.body),
                parser_version=PARSER_VERSION, item_count=item_count)
            write()

    def _record_failure(self, run_id: str | None, path: str, exc: Exception,
                        tier: str | None) -> None:
        with self.store.transaction():
            self.store.insert_request(
                request_id=uuid.uuid4().hex, run_id=run_id, path=path, params={}, url=None,
                status=getattr(exc, "status", None), requested_at=utcnow(), received_at=None,
                attempts=None, tier=tier, raw_path=None, sha256=None, body_bytes=None,
                parser_version=None, item_count=None, error=str(exc)[:1000])

    def _unknown(self, run_id: str | None, dataset: str, item: dict[str, Any],
                 known: frozenset[str]) -> list[Issue]:
        out = []
        for f in unknown_fields(item, known):
            if (dataset, f) not in self._unknown_reported:
                self._unknown_reported.add((dataset, f))
                out.append(Issue("warning", "unknown_field",
                                 f"{dataset}: unmapped field '{f}' (kept in raw file)", f))
        return out

    # -------------------------------------------------------------- cutoff
    def fetch_cutoff(self, run_id: str | None) -> Cutoff:
        """Read /historical/cutoff, persist it, and remember it for routing."""
        record = self.client.historical_cutoff()
        cutoff = Cutoff.from_body(record.json())

        def write() -> None:
            if run_id:
                self.store.set_run_cutoff(run_id, cutoff.raw, record.request_id)

        self._persist(run_id, record, None, None, write)
        self._cutoff, self._cutoff_at = cutoff, utcnow()
        return cutoff

    def current_cutoff(self, run_id: str | None, *, max_age: float = CUTOFF_REFRESH_SECONDS
                       ) -> Cutoff:
        if self._cutoff is None or self._cutoff_at is None or (
                utcnow() - self._cutoff_at).total_seconds() > max_age:
            return self.fetch_cutoff(run_id)
        return self._cutoff

    # ------------------------------------------------------------ metadata
    def store_market_page(self, run_id: str | None, record: ResponseRecord, items: list[Any],
                          tier: Tier) -> list[Record]:
        """Normalize and persist a list of market objects from one response."""
        recs: list[Record] = []
        issues: list[Issue] = []
        for item in items:
            rec, iss = parse_market(item)
            issues.extend(iss)
            if isinstance(item, dict):
                issues.extend(self._unknown(run_id, "market", item, MARKET_FIELDS))
            if rec is not None:
                recs.append(rec)

        def write() -> None:
            for rec in recs:
                self.store.upsert_market(rec, tier=tier.value, request_id=record.request_id,
                                         retrieved_at=record.received_at,
                                         always_snapshot=self.always_snapshot_markets)
            self.store.add_issues(issues, run_id=run_id, request_id=record.request_id,
                                  dataset="market")

        self._persist(run_id, record, tier.value, len(items), write)
        return recs

    def ensure_event(self, run_id: str | None, event_ticker: str) -> str | None:
        """Fetch event metadata once per session; returns its series ticker."""
        if event_ticker not in self._events_done:
            record = self.client.event(event_ticker)
            body = record.json()
            rec, issues = parse_event(body.get("event") if isinstance(body, dict) else None)

            def write() -> None:
                if rec is not None:
                    self.store.upsert_event(rec, record.request_id, record.received_at)
                self.store.add_issues(issues, run_id=run_id, request_id=record.request_id,
                                      dataset="event")

            self._persist(run_id, record, None, 1, write)
            self._events_done.add(event_ticker)
        row = self.store.get_event(event_ticker)
        return None if row is None else row["series_ticker"]

    def resolve_market(self, run_id: str | None, ticker: str) -> MarketInfo:
        """Look a market up in the live tier, then the historical tier.

        Raises:
            NotFoundError: if neither tier knows the ticker.
        """
        record: ResponseRecord | None = None
        tier = Tier.LIVE
        for tier in (Tier.LIVE, Tier.HISTORICAL):
            try:
                record = self.client.market(tier, ticker)
                break
            except NotFoundError:
                continue
        if record is None:
            raise NotFoundError(404, f"/markets/{ticker}", code="not_found",
                                message=f"market {ticker} not found in live or historical tier")
        body = record.json()
        item = body.get("market") if isinstance(body, dict) else None
        recs = self.store_market_page(run_id, record, [item], tier)
        if not recs:
            raise KalshiDataError(f"market {ticker}: response failed validation (see issues)")
        rec = recs[0]
        series = self.ensure_event(run_id, rec["event_ticker"])
        return MarketInfo(ticker=rec["ticker"], event_ticker=rec["event_ticker"],
                          series_ticker=series, found_tier=tier, created=rec["created_time"],
                          settled=rec["settlement_ts"], status=rec["status"],
                          notional=rec["notional_value_dollars"])

    def discover_markets(self, run_id: str | None, *, series: str | None = None,
                         event: str | None = None, include_historical: bool = True,
                         max_markets: int | None = None) -> list[Record]:
        """List markets for a series or event from both tiers, merged by ticker.

        Never truncates silently: if ``max_markets`` would be exceeded a
        ConfigError is raised telling the user to narrow the scope.
        """
        tiers = [Tier.LIVE, Tier.HISTORICAL] if include_historical else [Tier.LIVE]
        merged: dict[str, Record] = {}
        limit = max_markets
        pages_cap = None if limit is None else limit // self.client.page_limit + 2
        for tier in tiers:
            pages: Iterable[Page] = self.client.market_pages(
                tier, series_ticker=series, event_ticker=None if series else event,
                max_pages=pages_cap)
            last: Page | None = None
            for page in pages:
                last = page
                for rec in self.store_market_page(run_id, page.record, page.items, tier):
                    # Live wins over historical for the same ticker (newer view).
                    merged.setdefault(rec["ticker"], rec)
                self.progress("discover", {"tier": tier.value, "markets": len(merged)})
                if limit is not None and len(merged) > limit:
                    raise ConfigError(
                        f"{series or event} has more than {limit} markets; list specific "
                        "tickers/events or raise collection.max_markets_per_series")
            if last is not None and last.next_cursor is not None:
                raise ConfigError(f"market listing for {series or event} hit the page cap; "
                                  "narrow the scope")
        return list(merged.values())

    # ------------------------------------------------------------- runs
    def start_run(self, request: JobRequest) -> str:
        run_id = new_run_id()
        safe = self.settings.model_dump(mode="json")
        with self.store.transaction():
            self.store.create_run(run_id, request.kind, request.to_json(), safe)
        return run_id

    def _save_spec(self, run_id: str, request: JobRequest) -> None:
        with self.store.transaction():
            self.store.conn.execute("UPDATE runs SET spec_json=? WHERE run_id=?",
                                    (dumps_exact(request.to_json(), sort_keys=True), run_id))

    def run(self, request: JobRequest, run_id: str | None = None) -> RunResult:
        """Execute (or resume, when ``run_id`` is given) a collection run."""
        resuming = run_id is not None
        if run_id is None:
            run_id = self.start_run(request)
        used_before = self.client.transport.budget.used
        status, message = "completed", None
        self.progress("run", {"run_id": run_id, "resuming": resuming})
        try:
            old_cutoff = None
            if resuming:
                row = self.store.get_run(run_id)
                old_cutoff = row["cutoff_json"] if row else None
            cutoff = self.fetch_cutoff(run_id)
            if old_cutoff and json.loads(old_cutoff) != cutoff.raw:
                log.info("Historical cutoff changed since the run started; unfinished units "
                         "will be re-routed")
                self.progress("note", {"message": "historical cutoff changed; re-routing"})
            markets = self._resolve_scope(run_id, request)
            units = self._plan(run_id, request, markets)
            self.progress("planned", {"units": len(units)})
            self._execute(run_id, units, markets, request)
        except BudgetExhaustedError as exc:
            status, message = "incomplete", f"request budget exhausted: {exc}"
            self._mark_running_incomplete(run_id, "request budget exhausted")
        except KeyboardInterrupt:
            status, message = "interrupted", "interrupted by user"
            self._mark_running_incomplete(run_id, "interrupted")
            self._finish(run_id, status, message, used_before)
            raise
        except KalshiDataError as exc:
            status, message = "failed", str(exc)
            self._mark_running_incomplete(run_id, f"run failed: {exc}")
        units_summary = self._summary(run_id)
        if status == "completed" and any(units_summary.get(s, 0) for s in
                                         ("failed", "not_found", "incomplete", "pending",
                                          "running")):
            status = "incomplete"
            message = "some work units did not complete; see `kalshi-data status`"
        used = self._finish(run_id, status, message, used_before)
        return RunResult(run_id, status, message, units_summary, used)

    def _finish(self, run_id: str, status: str, message: str | None, used_before: int) -> int:
        used = self.client.transport.budget.used - used_before
        with self.store.transaction():
            self.store.finish_run(run_id, status, used, message)
        self.progress("finished", {"run_id": run_id, "status": status})
        return used

    def _mark_running_incomplete(self, run_id: str, note: str) -> None:
        with self.store.transaction():
            self.store.conn.execute(
                "UPDATE work_units SET status='incomplete', note=?, updated_at=? "
                "WHERE run_id=? AND status IN ('running')", (note, iso(utcnow()), run_id))

    def _summary(self, run_id: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.store.conn.execute(
                "SELECT status, COUNT(*) n FROM work_units WHERE run_id=? GROUP BY status",
                (run_id,)):
            out[r["status"]] = r["n"]
        return out

    def _resolve_scope(self, run_id: str, request: JobRequest) -> dict[str, MarketInfo | None]:
        """Map every ticker in scope to MarketInfo (None = not found)."""
        if request.resolved_tickers is None:
            tickers = list(dict.fromkeys(request.tickers))
            cap = self.settings.collection.max_markets_per_series
            for s in request.series:
                tickers += [r["ticker"] for r in self.discover_markets(
                    run_id, series=s, max_markets=cap)]
            for e in request.events:
                tickers += [r["ticker"] for r in self.discover_markets(
                    run_id, event=e, max_markets=cap)]
            request.resolved_tickers = list(dict.fromkeys(tickers))
            self._save_spec(run_id, request)
        markets: dict[str, MarketInfo | None] = {}
        for t in request.resolved_tickers:
            try:
                markets[t] = self.resolve_market(run_id, t)
            except NotFoundError as exc:
                self._record_failure(run_id, f"/markets/{t}", exc, None)
                markets[t] = None
            self.progress("market", {"ticker": t, "found": markets[t] is not None})
        return markets

    def _plan(self, run_id: str, request: JobRequest,
              markets: dict[str, MarketInfo | None]) -> list[int]:
        unit_ids: list[int] = []
        with self.store.transaction():
            for ticker, info in markets.items():
                for dataset in request.datasets:
                    if dataset == "markets":
                        continue  # metadata already stored during resolution
                    interval = request.candle_interval if dataset == "candles" else None
                    if dataset == "orderbook":
                        cells = [(request.start, request.end)]
                    else:
                        clipped = (request.start, request.end) if info is None else \
                            clip_to_market_life(request.start, request.end, info.created,
                                                info.settled)
                        if clipped is None:
                            continue  # requested range lies outside the market's life
                        cells = (trade_cells(*clipped) if dataset == "trades"
                                 else candle_cells(*clipped, request.candle_interval))
                    for s, e in cells:
                        key = unit_key(dataset, ticker, interval, s, e)
                        row = self.store.ensure_unit(run_id, key, dataset, ticker, interval,
                                                     s, e)
                        if info is None and row["status"] == "pending":
                            self.store.update_unit(row["unit_id"], status="not_found",
                                                   error="market not found in either tier")
                        unit_ids.append(row["unit_id"])
        return unit_ids

    def _execute(self, run_id: str, unit_ids: list[int],
                 markets: dict[str, MarketInfo | None], request: JobRequest) -> None:
        overlap = self.settings.collection.overlap_seconds
        for n, uid in enumerate(unit_ids, 1):
            unit = self.store.get_unit(uid)
            if unit["status"] in ("complete", "skipped", "not_found"):
                continue
            info = markets.get(unit["ticker"])
            if info is None:
                continue
            if unit["dataset"] != "orderbook":
                prior = self.store.final_unit_elsewhere(unit["unit_key"], overlap, run_id)
                if prior is not None:
                    with self.store.transaction():
                        self.store.update_unit(
                            uid, status="skipped", completed_at=utcnow(),
                            note=f"final copy exists from run {prior['run_id']}")
                    continue
            self.progress("unit", {"n": n, "total": len(unit_ids), "dataset": unit["dataset"],
                                   "ticker": unit["ticker"], "window": unit["window_start"]})
            try:
                if unit["dataset"] == "trades":
                    self._run_trade_unit(run_id, uid, info)
                elif unit["dataset"] == "candles":
                    self._run_candle_unit(run_id, uid, info)
                elif unit["dataset"] == "orderbook":
                    self.snapshot_orderbook(run_id, info.ticker, notional=info.notional)
                    with self.store.transaction():
                        self.store.update_unit(uid, status="complete", completed_at=utcnow(),
                                               rows_received=1, rows_new=1)
            except BudgetExhaustedError:
                raise
            except (APIError, PaginationError, KalshiDataError) as exc:
                if isinstance(exc, ConfigError):
                    raise
                log.warning("unit %s failed: %s", unit["unit_key"], exc)
                with self.store.transaction():
                    self.store.update_unit(uid, status="failed", error=str(exc)[:1000])
                self.progress("unit_failed", {"key": unit["unit_key"], "error": str(exc)})

    # ------------------------------------------------------------- trades
    def _run_trade_unit(self, run_id: str, uid: int, info: MarketInfo) -> None:
        unit = self.store.get_unit(uid)
        start = parse_api_time(unit["window_start"])
        end = parse_api_time(unit["window_end"])
        assert start is not None and end is not None
        cutoff = self.current_cutoff(run_id)
        phases = split_trade_window(start, end, cutoff)
        phase_ids = [f"{t.value}:{iso(s)}:{iso(e)}" for t, s, e in phases]
        idx, cursor = 0, None
        notes = []
        if unit["phase"] in phase_ids:
            idx, cursor = phase_ids.index(unit["phase"]), unit["cursor"]
        elif unit["phase"]:
            notes.append("routing changed since checkpoint; unit restarted")
        with self.store.transaction():
            self.store.update_unit(uid, status="running", attempts=unit["attempts"] + 1,
                                   error=None)
        for i in range(idx, len(phases)):
            tier, ps, pe = phases[i]
            nxt = phase_ids[i + 1] if i + 1 < len(phases) else None
            try:
                self._trade_phase(run_id, uid, info.ticker, start, end, tier, ps, pe,
                                  phase_ids[i], nxt, cursor if i == idx else None)
            except PaginationError as exc:
                # A cursor the API rejected, or a cursor sequence we cannot trust:
                # restart this bounded phase from its first page, once. Rows already
                # stored are de-duplicated by trade_id. A second failure propagates.
                notes.append(f"cursor problem ({exc}); phase restarted from first page")
                self._trade_phase(run_id, uid, info.ticker, start, end, tier, ps, pe,
                                  phase_ids[i], nxt, None)
        # Bounded reconciliation for trades that may have moved live -> historical
        # while we were reading: if the cutoff advanced into this window, read the
        # newly archived slice from the historical tier once.
        if any(t is Tier.LIVE for t, _, _ in phases):
            fresh = self.current_cutoff(run_id)
            old_c, new_c = cutoff.trades_created, fresh.trades_created
            if new_c > old_c and new_c > start and old_c < end:
                rs, re_ = max(start, old_c), min(end, new_c)
                notes.append(f"cutoff advanced; reconciled historical {iso(rs)}..{iso(re_)}")
                self._trade_phase(run_id, uid, info.ticker, start, end, Tier.HISTORICAL,
                                  rs, re_, f"reconcile:{iso(rs)}:{iso(re_)}", None, None)
        unit = self.store.get_unit(uid)
        if unit["rows_received"] == 0:
            notes.append("no trades returned")
        with self.store.transaction():
            self.store.update_unit(uid, status="complete", completed_at=utcnow(), cursor=None,
                                   note="; ".join(notes) or None)

    def _trade_phase(self, run_id: str, uid: int, ticker: str, start: datetime, end: datetime,
                     tier: Tier, ps: datetime, pe: datetime, phase_id: str,
                     next_phase_id: str | None, cursor: str | None) -> None:
        """Page through one tier segment, committing a checkpoint per page.

        Kalshi compares ``min_ts``/``max_ts`` with sub-second trade times, so we
        pad the request by one second on both sides and keep only trades with
        ``start <= created_time < end`` (exact half-open filter on the unit).
        """
        unit = self.store.get_unit(uid)
        stored_first = unit["first_item_key"] if cursor else None
        pages = self.client.trade_pages(tier, ticker, to_unix(ps) - 1, to_unix(pe) + 1,
                                        start_cursor=cursor)
        sent_cursor = cursor  # cursor carried by the next request
        try:
            for page in pages:
                sent_cursor = page.next_cursor
                self._trade_page(run_id, uid, ticker, start, end, tier, page, phase_id,
                                 next_phase_id, cursor, stored_first)
        except BadRequestError as exc:
            if sent_cursor:
                raise CursorRejectedError(f"cursor rejected: {exc}") from exc
            raise

    def _trade_page(self, run_id: str, uid: int, ticker: str, start: datetime, end: datetime,
                    tier: Tier, page: Page, phase_id: str, next_phase_id: str | None,
                    cursor: str | None, stored_first: str | None) -> None:
        """Normalize one page of trades and commit it with the checkpoint."""
        if stored_first and page.items and page.items[0].get("trade_id") == stored_first \
                and page.index == 0:
            log.info("cursor for %s was not honoured (restarted at first page)", ticker)
        recs: list[Record] = []
        issues: list[Issue] = []
        for item in page.items:
            rec, iss = parse_trade(item)
            issues.extend(iss)
            if isinstance(item, dict):
                issues.extend(self._unknown(run_id, "trade", item, TRADE_FIELDS))
            if rec is not None and start <= rec["created_time"] < end:
                if rec["ticker"] != ticker:
                    issues.append(Issue("error", "wrong_market",
                                        f"trade for {rec['ticker']} returned for {ticker}",
                                        rec["trade_id"], item))
                    continue
                recs.append(rec)
        done = page.next_cursor is None
        first_key = (page.items[0].get("trade_id") if page.items and page.index == 0
                     and cursor is None else None)

        def write(recs: list[Record] = recs, issues: list[Issue] = issues,
                  page: Page = page, done: bool = done,
                  first_key: Any = first_key) -> None:
            new, _same, _conf = self.store.insert_trades(
                recs, tier=tier.value, request_id=page.record.request_id,
                retrieved_at=page.record.received_at, parser_version=PARSER_VERSION,
                run_id=run_id)
            self.store.add_issues(issues, run_id=run_id, request_id=page.record.request_id,
                                  dataset="trade")
            cur = self.store.get_unit(uid)
            times = [r["created_time"] for r in recs]
            earliest = min([iso(t) for t in times] + ([cur["earliest_obs"]]
                                                      if cur["earliest_obs"] else []),
                           default=None)
            latest = max([iso(t) for t in times] + ([cur["latest_obs"]]
                                                    if cur["latest_obs"] else []),
                         default=None)
            tiers = set(filter(None, cur["tiers_used"].split(","))) | {tier.value}
            fields: dict[str, Any] = dict(
                pages=cur["pages"] + 1, rows_received=cur["rows_received"] + len(recs),
                rows_new=cur["rows_new"] + new, tiers_used=",".join(sorted(tiers)),
                earliest_obs=earliest, latest_obs=latest)
            if first_key:
                fields["first_item_key"] = first_key
            if done and next_phase_id:
                fields.update(phase=next_phase_id, cursor=None)
            else:
                fields.update(phase=phase_id, cursor=page.next_cursor)
            self.store.update_unit(uid, **fields)

        self._persist(run_id, page.record, tier.value, len(page.items), write)
        self.progress("page", {"dataset": "trades", "ticker": ticker,
                               "rows": len(recs), "tier": tier.value})

    # ------------------------------------------------------------ candles
    def _run_candle_unit(self, run_id: str, uid: int, info: MarketInfo) -> None:
        unit = self.store.get_unit(uid)
        start = parse_api_time(unit["window_start"])
        end = parse_api_time(unit["window_end"])
        assert start is not None and end is not None
        interval = int(unit["interval_minutes"])
        with self.store.transaction():
            self.store.update_unit(uid, status="running", attempts=unit["attempts"] + 1,
                                   error=None)
        cutoff = self.current_cutoff(run_id)
        # Candles follow the market's archival status (documented rule: markets
        # that settled before market_settled_ts are served by the historical
        # candle endpoint), never the candle timestamps. We do not use "the tier
        # where metadata was found" as the rule: we observed markets settled just
        # before the cutoff being served by BOTH tiers during the handoff.
        tier = market_tier(info.settled, cutoff)
        notes = []
        if info.found_tier is not tier:
            notes.append(f"metadata served by {info.found_tier.value} tier; candles routed to "
                         f"{tier.value} by settlement time")
        s_ts, e_ts = to_unix(start), to_unix(end)
        record: ResponseRecord | None = None
        for attempt in range(2):
            try:
                series = info.series_ticker
                if tier is Tier.LIVE and not series:
                    series = self.ensure_event(run_id, info.event_ticker)
                record = self.client.candlesticks(tier, info.ticker, series, s_ts, e_ts - 1,
                                                  interval)
                break
            except NotFoundError:
                if attempt == 1:
                    raise
                # Bounded fallback: the market may have moved tiers during the run.
                fresh = self.resolve_market(run_id, info.ticker)
                other = Tier.HISTORICAL if tier is Tier.LIVE else Tier.LIVE
                notes.append(f"404 from {tier.value} candles; retried {other.value} "
                             f"(metadata now in {fresh.found_tier.value})")
                tier = other
                info = fresh
        assert record is not None
        body = record.json()
        items = body.get("candlesticks") if isinstance(body, dict) else None
        issues: list[Issue] = []
        recs: list[Record] = []
        if not isinstance(items, list):
            issues.append(Issue("error", "malformed_response", "candlesticks list missing",
                                info.ticker, body))
            items = []
        for item in items:
            rec, iss = parse_candle(item, info.ticker, interval, tier.value)
            issues.extend(iss)
            if rec is not None and s_ts <= rec["end_period_ts"] < e_ts:
                recs.append(rec)
        if not recs:
            notes.append("no candles returned")

        def write() -> None:
            new, _rev, _same = self.store.upsert_candles(
                recs, tier=tier.value, request_id=record.request_id,
                retrieved_at=record.received_at, parser_version=PARSER_VERSION)
            self.store.add_issues(issues, run_id=run_id, request_id=record.request_id,
                                  dataset="candle")
            ends = [r["end_period_time"] for r in recs]
            self.store.update_unit(
                uid, status="complete", completed_at=utcnow(), pages=1,
                rows_received=len(recs), rows_new=new, tiers_used=tier.value,
                earliest_obs=min(ends) if ends else None,
                latest_obs=max(ends) if ends else None, note="; ".join(notes) or None)

        self._persist(run_id, record, tier.value, len(items), write)
        self.progress("page", {"dataset": "candles", "ticker": info.ticker, "rows": len(recs),
                               "tier": tier.value})

    # ---------------------------------------------------------- orderbook
    def snapshot_orderbook(self, run_id: str | None, ticker: str, depth: int | None = None,
                           notional: Decimal | None = None) -> Record:
        """Fetch and store one current order book snapshot (bids only).

        ``requested_at``/``received_at`` are local clock times; ``server_date``
        is the HTTP Date header. This is a point-in-time observation; REST
        polling cannot capture every book update.
        """
        record = self.client.orderbook(ticker, depth)
        levels, issues = parse_orderbook(record.json(), ticker)
        summary: Record = {}

        def write() -> None:
            nonlocal summary
            self.store.add_issues(issues, run_id=run_id, request_id=record.request_id,
                                  dataset="orderbook")
            if levels is not None:
                summary = self.store.insert_orderbook(
                    snapshot_id=record.request_id, run_id=run_id, ticker=ticker,
                    requested_at=record.requested_at, received_at=record.received_at,
                    server_date=record.headers.get("date"), depth=depth, levels=levels,
                    notional=notional)

        self._persist(run_id, record, Tier.LIVE.value, None if levels is None else len(levels),
                      write)
        if levels is None:
            raise KalshiDataError(f"order book for {ticker} failed validation (see issues)")
        return summary

    # ------------------------------------------------------------ discovery
    def store_series_list(self, run_id: str | None, category: str | None = None,
                          tags: str | None = None) -> list[Record]:
        """Fetch /series (optionally filtered server-side) and cache locally."""
        out: list[Record] = []
        for page in self.client.series_pages(category=category, tags=tags):
            recs, issues = [], []
            for item in page.items:
                rec, iss = parse_series(item)
                issues.extend(iss)
                if rec is not None:
                    recs.append(rec)

            def write(recs: list[Record] = recs, issues: list[Issue] = issues,
                      page: Page = page) -> None:
                for r in recs:
                    self.store.upsert_series(r, page.record.request_id, page.record.received_at)
                self.store.add_issues(issues, run_id=run_id, request_id=page.record.request_id,
                                      dataset="series")

            self._persist(run_id, page.record, None, len(page.items), write)
            out.extend(recs)
        return out

    def store_events(self, run_id: str | None, series_ticker: str,
                     max_pages: int | None = None) -> list[Record]:
        """Fetch /events for a series and cache them locally."""
        out: list[Record] = []
        for page in self.client.event_pages(series_ticker=series_ticker, max_pages=max_pages):
            recs, issues = [], []
            for item in page.items:
                rec, iss = parse_event(item)
                issues.extend(iss)
                if rec is not None:
                    recs.append(rec)

            def write(recs: list[Record] = recs, issues: list[Issue] = issues,
                      page: Page = page) -> None:
                for r in recs:
                    self.store.upsert_event(r, page.record.request_id, page.record.received_at)
                self.store.add_issues(issues, run_id=run_id, request_id=page.record.request_id,
                                      dataset="event")

            self._persist(run_id, page.record, None, len(page.items), write)
            out.extend(recs)
        return out

