"""``kalshi-data`` command-line interface.

Exit codes:
    0  success (run completed)
    1  run finished but is incomplete/failed (see ``kalshi-data status``)
    2  invalid input or configuration (detected before network calls)
    3  API error (not found, bad request, permission, server error after retries)
    4  data directory locked by another writer
    5  network failure after retries
    6  request budget exhausted before any run could start
    130 interrupted (Ctrl-C); the run can be resumed
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, NoReturn, TypeVar

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .api import KalshiData, LocalStore, filter_series
from .config import (
    ALTERNATE_BASE_URL,
    VALID_INTERVALS,
    LoadedConfig,
    Settings,
    load_config,
)
from .coverage import market_coverage, run_status
from .decimals import json_default
from .errors import (
    APIError,
    BudgetExhaustedError,
    ConfigError,
    KalshiDataError,
    LockError,
    TransportError,
)
from .export import DATASETS
from .ingest import RunResult
from .routing import candle_cells, trade_cells
from .storage import Store
from .timeutil import iso, parse_user_time, utcnow, validate_range

app = typer.Typer(
    help="Kalshi Research Data Engine: discover markets, download historical trades and "
         "candlesticks, snapshot order books, and export tidy CSV/Parquet.",
    no_args_is_help=True, add_completion=False)
fetch_app = typer.Typer(help="Download one dataset for one market (stored locally).",
                        no_args_is_help=True)
app.add_typer(fetch_app, name="fetch")

console = Console(stderr=True)
out = Console()

EXIT_PARTIAL, EXIT_INPUT, EXIT_API, EXIT_LOCK, EXIT_NET, EXIT_BUDGET = 1, 2, 3, 4, 5, 6
T = TypeVar("T")


@dataclass
class Ctx:
    config_path: Path | None
    overrides: dict[str, dict[str, Any]]
    _loaded: LoadedConfig | None = None

    @property
    def loaded(self) -> LoadedConfig:
        if self._loaded is None:
            self._loaded = load_config(self.config_path, self.overrides)
        return self._loaded

    @property
    def settings(self) -> Settings:
        return self.loaded.settings


def _fail(msg: str, code: int) -> NoReturn:
    console.print(f"[red]Error:[/red] {msg}")
    raise typer.Exit(code)


def guarded(fn: Callable[[], T]) -> T:
    """Run ``fn`` translating package errors into documented exit codes."""
    try:
        return fn()
    except ConfigError as exc:
        _fail(str(exc), EXIT_INPUT)
    except LockError as exc:
        _fail(str(exc), EXIT_LOCK)
    except BudgetExhaustedError as exc:
        _fail(str(exc), EXIT_BUDGET)
    except TransportError as exc:
        _fail(f"{exc}\nCheck connectivity, or try --base-url {ALTERNATE_BASE_URL}", EXIT_NET)
    except APIError as exc:
        _fail(str(exc), EXIT_API)
    except KalshiDataError as exc:
        _fail(str(exc), EXIT_API)
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted.[/yellow] Resume with `kalshi-data resume RUN_ID` "
                      "(see `kalshi-data runs`).")
        raise typer.Exit(130) from None


def _dump(obj: Any) -> None:
    out.print_json(json.dumps(obj, default=_json_default))


def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return iso(o)
    return json_default(o)


def _time(value: str, name: str) -> datetime:
    try:
        return parse_user_time(value)
    except ConfigError as exc:
        _fail(f"--{name}: {exc}", EXIT_INPUT)


class Progress:
    """Rich status line: counters only (no invented percentages for unknown totals)."""

    def __init__(self, kd_holder: list[KalshiData]) -> None:
        self.kd_holder = kd_holder
        self.rows = 0
        self.units = (0, 0)
        self.status = console.status("starting...")
        self.status.start()

    def __call__(self, kind: str, data: dict[str, Any]) -> None:
        stats = self.kd_holder[0].transport.stats if self.kd_holder else None
        if kind == "page":
            self.rows += data.get("rows", 0)
        elif kind == "unit":
            self.units = (data["n"], data["total"])
        elif kind == "unit_failed":
            console.print(f"[yellow]unit failed[/yellow] {data['key']}: {data['error']}")
        elif kind == "note":
            console.print(f"[cyan]note[/cyan] {data['message']}")
        elif kind == "run":
            console.print(f"run [bold]{data['run_id']}[/bold]"
                          + (" (resuming)" if data.get("resuming") else ""))
        req = f"requests {stats.requests} retries {stats.retries}" if stats else ""
        unit = f"unit {self.units[0]}/{self.units[1]} " if self.units[1] else ""
        self.status.update(f"{unit}rows {self.rows} {req}")

    def stop(self) -> None:
        self.status.stop()


def _run_with_progress(ctx: Ctx, action: Callable[[KalshiData], RunResult],
                       budget: int | None = None) -> RunResult:
    holder: list[KalshiData] = []
    prog = Progress(holder)
    try:
        with KalshiData(ctx.settings, request_budget=budget, progress=prog) as kd:
            holder.append(kd)
            return action(kd)
    finally:
        prog.stop()


def _report_run(result: RunResult, settings: Settings) -> None:
    colour = {"completed": "green", "incomplete": "yellow"}.get(result.status, "red")
    console.print(f"Run [bold]{result.run_id}[/bold]: [{colour}]{result.status}[/{colour}]"
                  + (f" - {result.message}" if result.message else ""))
    console.print(f"  units: {result.units}  HTTP attempts: {result.requests_used}")
    console.print(f"  database: {settings.storage.db_path}")
    console.print(f"  raw responses: {settings.storage.raw_dir / result.run_id}")
    console.print(f"  details: kalshi-data status {result.run_id}")
    if result.status != "completed":
        console.print(f"  resume:  kalshi-data resume {result.run_id}")
        raise typer.Exit(EXIT_PARTIAL)


# ---------------------------------------------------------------- callback
@app.callback()
def main(
    ctx: typer.Context,
    config: Annotated[Path | None, typer.Option(
        "--config", "-c", help="TOML settings file (job tables are used by `collect`).")] = None,
    data_dir: Annotated[Path | None, typer.Option(
        help="Data directory (database, raw responses). Default: ./data")] = None,
    base_url: Annotated[str | None, typer.Option(
        help="API base URL. Default: https://external-api.kalshi.com/trade-api/v2")] = None,
    min_interval: Annotated[float | None, typer.Option(
        help="Minimum seconds between requests (our pacing; default 0.5).")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Debug logging.")] = False,
) -> None:
    """Global options apply to every command."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s")
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    ctx.obj = Ctx(config, {"api": {"base_url": base_url, "min_request_interval": min_interval},
                           "storage": {"data_dir": data_dir}})


def _ctx(ctx: typer.Context) -> Ctx:
    c: Ctx = ctx.obj
    guarded(lambda: c.loaded)
    return c


# ------------------------------------------------------------------ doctor
@app.command()
def doctor(
    ctx: typer.Context,
    offline: Annotated[bool, typer.Option(help="Skip the API reachability check.")] = False,
    as_json: Annotated[bool, typer.Option("--json", help="Machine-readable output.")] = False,
) -> None:
    """Check configuration, storage, dependencies and (briefly) API reachability.

    Uses at most 3 HTTP requests.
    """
    c = _ctx(ctx)
    s = c.settings
    checks: list[dict[str, Any]] = []

    def check(name: str, fn: Callable[[], str]) -> None:
        try:
            checks.append({"check": name, "ok": True, "detail": fn()})
        except Exception as exc:  # report every failure, keep going
            checks.append({"check": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}"})

    check("python", lambda: sys.version.split()[0] + (
        "" if sys.version_info >= (3, 11) else " (3.11+ required)"))

    def deps() -> str:
        import httpx
        import pyarrow
        import pydantic
        import rich  # noqa: F401
        try:
            import pandas
            pd = pandas.__version__
        except ImportError:
            pd = "not installed (optional: pip install 'kalshi-data[research]')"
        return (f"httpx {httpx.__version__}, pydantic {pydantic.__version__}, "
                f"pyarrow {pyarrow.__version__}, pandas {pd}")

    check("dependencies", deps)
    check("configuration", lambda: f"sources: {', '.join(c.loaded.sources)}; "
                                   f"base_url {s.api.base_url}")

    def storage() -> str:
        s.storage.data_dir.mkdir(parents=True, exist_ok=True)
        probe = s.storage.data_dir / ".write-probe"
        probe.write_text("ok")
        probe.unlink()
        with Store(s.storage.db_path) as st:
            n = st.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        return f"{s.storage.data_dir.resolve()} writable; database ok ({n} runs)"

    check("storage", storage)

    def raw_state() -> str:
        from .rawstore import RawStore, find_orphans
        rs = RawStore(s.storage.raw_dir)
        if not s.storage.db_path.exists():
            return "no database yet"
        with Store(s.storage.db_path, readonly=True) as st:
            orphans = find_orphans(rs, st.raw_paths())
        tmps = rs.temp_files()
        if orphans or tmps:
            raise RuntimeError(f"{len(orphans)} orphan raw files, {len(tmps)} temp files "
                               "(from interrupted writes; run `kalshi-data reconcile`)")
        return "no orphan raw files"

    check("raw files", raw_state)
    check("writer lock", lambda: "held: " + s.storage.lock_path.read_text()
          if s.storage.lock_path.exists() else "free")
    if not offline:
        def api() -> str:
            with KalshiData(s, request_budget=3) as kd:
                kd.transport.settings = s.api.model_copy(update={"max_retries": 1})
                status = kd.client.exchange_status().json()
                cut = kd.client.historical_cutoff().json()
            return (f"reachable; exchange_active={status.get('exchange_active')}; "
                    f"historical cutoff markets={cut.get('market_settled_ts')} "
                    f"trades={cut.get('trades_created_ts')}")
        check("api", api)
    if as_json:
        _dump(checks)
    else:
        for ch in checks:
            mark = "[green]OK[/green]  " if ch["ok"] else "[red]FAIL[/red]"
            out.print(f"{mark} {ch['check']}: {ch['detail']}")
        if any(not ch["ok"] and ch["check"] == "api" for ch in checks):
            out.print(f"Hint: if the host is blocked on your network, try "
                      f"--base-url {ALTERNATE_BASE_URL} (documented as also supported).")
    if not all(ch["ok"] for ch in checks):
        raise typer.Exit(EXIT_PARTIAL)


# --------------------------------------------------------------- discovery
@app.command()
def discover(
    ctx: typer.Context,
    query: Annotated[str | None, typer.Option(
        "--query", "-q", help="Keywords matched locally against ticker/title/tags (AND).")] = None,
    category: Annotated[str | None, typer.Option(
        help="Server-side category filter, exact and case-sensitive (e.g. Financials).")] = None,
    tags: Annotated[str | None, typer.Option(help="Server-side tags filter.")] = None,
    offline: Annotated[bool, typer.Option(help="Search the local cache only.")] = False,
    limit: Annotated[int, typer.Option(help="Max rows to print.")] = 50,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Find series by keyword. Fetches the series list (cached locally) and filters
    by keyword locally; Kalshi documents no server-side text search for series."""
    c = _ctx(ctx)
    if offline:
        with LocalStore(settings=c.settings) as db:
            rows = [dict(r) for r in db.store.conn.execute("SELECT * FROM series")]
        found = filter_series(rows, query)
    else:
        def go() -> list[dict[str, Any]]:
            with KalshiData(c.settings) as kd:
                return kd.discover(query, category=category, tags=tags)
        found = guarded(go)
    if as_json:
        _dump(found[:limit])
        return
    table = Table(title=f"{len(found)} series match" + (f" '{query}'" if query else ""))
    for col in ("ticker", "title", "category", "frequency", "tags"):
        table.add_column(col)
    for s in found[:limit]:
        table.add_row(s["ticker"], s.get("title") or "", s.get("category") or "",
                      s.get("frequency") or "", str(s.get("tags") or ""))
    out.print(table)
    if not found:
        out.print("No series matched. Matching is local and literal; try fewer/other words.")
    elif len(found) > limit:
        out.print(f"(showing {limit}; use --limit or --json)")


@app.command()
def markets(
    ctx: typer.Context,
    series: Annotated[str | None, typer.Option(help="Series ticker, e.g. KXINX.")] = None,
    event: Annotated[str | None, typer.Option(help="Event ticker.")] = None,
    include_historical: Annotated[bool, typer.Option(
        "--include-historical/--live-only",
        help="Also list archived markets from /historical/markets.")] = True,
    limit: Annotated[int, typer.Option(help="Max rows to print.")] = 100,
    max_markets: Annotated[int, typer.Option(help="Refuse listings larger than this.")] = 5000,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List markets of a series or event across live and historical tiers
    (merged by ticker) and cache their metadata locally."""
    c = _ctx(ctx)
    if not (series or event) or (series and event):
        _fail("give exactly one of --series or --event", EXIT_INPUT)

    def go() -> list[dict[str, Any]]:
        with KalshiData(c.settings) as kd, kd._writer() as ing:
            recs = ing.discover_markets(None, series=series, event=event,
                                        include_historical=include_historical,
                                        max_markets=max_markets)
            tiers = {r["ticker"]: r["tier"] for r in ing.store.conn.execute(
                "SELECT ticker, tier FROM markets")}
            for r in recs:
                r["tier"] = tiers.get(r["ticker"])
            return recs

    recs = guarded(go)
    recs.sort(key=lambda r: (r.get("close_time") or utcnow(), r["ticker"]), reverse=True)
    if as_json:
        _dump(recs[:limit])
        return
    table = Table(title=f"{len(recs)} markets for {series or event} (newest close first)")
    for col in ("ticker", "status", "result", "close_time", "settlement_ts", "tier", "volume"):
        table.add_column(col)
    for r in recs[:limit]:
        table.add_row(r["ticker"], r.get("status") or "", r.get("result") or "",
                      iso(r.get("close_time")) or "", iso(r.get("settlement_ts")) or "",
                      r.get("tier") or "",
                      "" if r.get("volume_fp") is None else str(r["volume_fp"]))
    out.print(table)


@app.command()
def market(
    ctx: typer.Context,
    ticker: Annotated[str, typer.Argument(help="Market ticker.")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show (and cache) one market's current metadata and quote."""
    c = _ctx(ctx)

    def go() -> dict[str, Any]:
        with KalshiData(c.settings) as kd, kd._writer() as ing:
            info = ing.resolve_market(None, ticker)
            row = ing.store.get_market(ticker)
            assert row is not None
            d = dict(row)
            d["found_in_tier"] = info.found_tier.value
            return d

    d = guarded(go)
    if as_json:
        _dump(d)
        return
    keys = ("ticker", "title", "yes_sub_title", "event_ticker", "series_ticker", "status",
            "result", "strike_type", "floor_strike", "cap_strike", "open_time", "close_time",
            "expected_expiration_time", "latest_expiration_time", "settlement_ts",
            "settlement_value_dollars", "yes_bid_dollars", "yes_ask_dollars",
            "last_price_dollars", "volume_fp", "open_interest_fp", "price_level_structure",
            "found_in_tier", "retrieved_at")
    table = Table(show_header=False)
    for k in keys:
        table.add_row(k, "" if d.get(k) is None else str(d.get(k)))
    out.print(table)
    out.print("Note: quotes/metadata are as retrieved now (retrieved_at), not historical.")


# --------------------------------------------------------------- fetching
def _dry_run(datasets: list[str], ticker_desc: str, start: datetime, end: datetime,
             interval: int) -> None:
    out.print(f"[bold]Dry run[/bold] (no network): {ticker_desc}")
    out.print(f"  range [{iso(start)}, {iso(end)})  datasets: {', '.join(datasets)}")
    if "trades" in datasets:
        cells = trade_cells(start, end)
        out.print(f"  trades: up to {len(cells)} monthly windows per market "
                  "(each >=1 request; pages depend on volume; split at the trade cutoff)")
    if "candles" in datasets:
        cells = candle_cells(start, end, interval)
        out.print(f"  candles ({interval} min): up to {len(cells)} requests per market "
                  "(5,000 periods max each)")
    out.print("  Windows are clipped to each market's life (creation .. settlement + 1 day) "
              "once metadata is fetched; plus ~3 metadata requests per market and 1 cutoff read.")


def _validate_range(start: str, end: str) -> tuple[datetime, datetime]:
    s, e = _time(start, "start"), _time(end, "end")
    try:
        validate_range(s, e)
    except ConfigError as exc:
        _fail(str(exc), EXIT_INPUT)
    return s, e


@fetch_app.command("trades")
def fetch_trades(
    ctx: typer.Context,
    ticker: Annotated[str, typer.Option(help="Market ticker.")],
    start: Annotated[str, typer.Option(help="UTC start, inclusive (YYYY-MM-DD or ISO 8601).")],
    end: Annotated[str, typer.Option(help="UTC end, exclusive (YYYY-MM-DD = that midnight).")],
    budget: Annotated[int | None, typer.Option(help="Max HTTP requests for this run.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Show the plan without network calls.")] = False,
) -> None:
    """Download public trades for one market in [start, end), routed across tiers."""
    c = _ctx(ctx)
    s, e = _validate_range(start, end)
    if dry_run:
        _dry_run(["trades"], ticker, s, e, 60)
        return
    r = guarded(lambda: _run_with_progress(c, lambda kd: kd.collect(
        tickers=[ticker], start=s, end=e, datasets=["markets", "trades"],
        kind="fetch-trades"), budget))
    _report_run(r, c.settings)


@fetch_app.command("candles")
def fetch_candles(
    ctx: typer.Context,
    ticker: Annotated[str, typer.Option(help="Market ticker.")],
    start: Annotated[str, typer.Option(help="UTC start, inclusive (on candle period end).")],
    end: Annotated[str, typer.Option(help="UTC end, exclusive (on candle period end).")],
    interval: Annotated[int, typer.Option(help="Minutes per candle: 1, 60 or 1440.")] = 60,
    budget: Annotated[int | None, typer.Option(help="Max HTTP requests for this run.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Show the plan without network calls.")] = False,
) -> None:
    """Download candlesticks (traded price, YES bid, YES ask OHLC) for one market."""
    c = _ctx(ctx)
    if interval not in VALID_INTERVALS:
        _fail(f"--interval must be one of {VALID_INTERVALS}", EXIT_INPUT)
    s, e = _validate_range(start, end)
    if dry_run:
        _dry_run(["candles"], ticker, s, e, interval)
        return
    r = guarded(lambda: _run_with_progress(c, lambda kd: kd.collect(
        tickers=[ticker], start=s, end=e, datasets=["markets", "candles"],
        candle_interval=interval, kind="fetch-candles"), budget))
    _report_run(r, c.settings)


@fetch_app.command("orderbook")
def fetch_orderbook(
    ctx: typer.Context,
    ticker: Annotated[str, typer.Option(help="Market ticker.")],
    depth: Annotated[int | None, typer.Option(help="Levels per side (1-100); default all.")]
    = None,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Take one current order book snapshot (bids only) and store it."""
    c = _ctx(ctx)
    if depth is not None and not 1 <= depth <= 100:
        _fail("--depth must be 1..100", EXIT_INPUT)

    def go() -> dict[str, Any]:
        with KalshiData(c.settings) as kd:
            return kd.snapshot_orderbook(ticker, depth)

    snap = guarded(go)
    if as_json:
        _dump(snap)
        return
    for k, v in snap.items():
        out.print(f"{k}: {v}")
    out.print("best_*_bid are quoted bids; derived_* values are computed (1 - opposite bid), "
              "not quoted by the API, and are not executable trade prices.")


@app.command()
def collect(
    ctx: typer.Context,
    budget: Annotated[int | None, typer.Option(help="Max HTTP requests for this run.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Validate and show the plan only.")] = False,
) -> None:
    """Run the job table defined in the --config TOML file (see configs/example.toml)."""
    c = _ctx(ctx)
    job = c.loaded.job
    if job is None:
        _fail("collect needs --config FILE with a [job] table (see configs/example.toml)",
              EXIT_INPUT)
    desc = (f"tickers={job.tickers} series={job.series} events={job.events}")
    if dry_run:
        _dry_run(list(job.datasets), desc, job.start, job.end, job.candle_interval)
        return
    r = guarded(lambda: _run_with_progress(c, lambda kd: kd.collect(
        tickers=job.tickers, series=job.series, events=job.events, start=job.start,
        end=job.end, datasets=list(job.datasets), candle_interval=job.candle_interval,
        kind=f"collect:{job.name}"), budget))
    _report_run(r, c.settings)


@app.command()
def poll(
    ctx: typer.Context,
    ticker: Annotated[list[str], typer.Option(help="Market ticker (repeatable).")],
    interval: Annotated[float, typer.Option(help="Seconds between samples (>= 1).")] = 60.0,
    duration: Annotated[float | None, typer.Option(help="Stop after this many seconds.")]
    = None,
    samples: Annotated[int | None, typer.Option(help="Stop after this many samples.")] = None,
    depth: Annotated[int | None, typer.Option(help="Order book levels per side.")] = None,
) -> None:
    """Collect order book + quote snapshots going forward, bounded by --duration or
    --samples. Ctrl-C stops cleanly. REST polling does not capture every update."""
    c = _ctx(ctx)
    if duration is None and samples is None:
        _fail("give --duration or --samples (polling must be bounded)", EXIT_INPUT)

    def on_sample(n: int, t: str, rec: dict[str, Any]) -> None:
        out.print(f"sample {n} {t} received {iso(rec['received_at'])} best_yes_bid "
                  f"{rec['best_yes_bid']} best_no_bid {rec['best_no_bid']}")

    def go() -> RunResult:
        with KalshiData(c.settings) as kd:
            return kd.poll(ticker, interval, duration_seconds=duration, max_samples=samples,
                           depth=depth, on_sample=on_sample)

    r = guarded(go)
    _report_run(r, c.settings)


@app.command()
def resume(
    ctx: typer.Context,
    run_id: Annotated[str, typer.Argument(help="Run to continue (see `kalshi-data runs`).")],
    budget: Annotated[int | None, typer.Option(help="Max HTTP requests for this attempt.")]
    = None,
) -> None:
    """Continue an interrupted/incomplete run from its durable checkpoints."""
    c = _ctx(ctx)
    r = guarded(lambda: _run_with_progress(c, lambda kd: kd.resume(run_id), budget))
    _report_run(r, c.settings)


# ---------------------------------------------------------------- inspect
def _local(c: Ctx) -> LocalStore:
    try:
        return LocalStore(settings=c.settings)
    except FileNotFoundError:
        _fail(f"no database at {c.settings.storage.db_path}; run a fetch first", EXIT_INPUT)


@app.command()
def runs(
    ctx: typer.Context,
    limit: Annotated[int, typer.Option(help="How many recent runs.")] = 20,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List recent runs."""
    with _local(_ctx(ctx)) as db:
        rows = db.runs(limit)
    if as_json:
        _dump(rows)
        return
    table = Table(title="Runs (newest first)")
    for col in ("run_id", "kind", "status", "created_at", "requests_used", "message"):
        table.add_column(col)
    for r in rows:
        table.add_row(r["run_id"], r["kind"], r["status"], r["created_at"],
                      str(r["requests_used"]), r["message"] or "")
    out.print(table)


@app.command()
def status(
    ctx: typer.Context,
    run_id: Annotated[str, typer.Argument(help="Run id.")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show what a run retrieved, what failed, and what remains unfinished."""
    with _local(_ctx(ctx)) as db:
        try:
            rep = run_status(db.store, run_id)
        except KeyError:
            _fail(f"unknown run {run_id}", EXIT_INPUT)
    if as_json:
        _dump(rep)
    else:
        for k, v in rep.items():
            if k == "unfinished":
                out.print(f"unfinished ({len(v)}):")
                for u in v[:30]:
                    out.print(f"   {u['status']:<10} {u['key']}  {u['error'] or u['note'] or ''}")
            else:
                out.print(f"{k}: {v}")
    if rep["status"] != "completed":
        raise typer.Exit(EXIT_PARTIAL)


@app.command()
def coverage(
    ctx: typer.Context,
    ticker: Annotated[str, typer.Option(help="Market ticker.")],
    run: Annotated[str | None, typer.Option(help="Limit to one run.")] = None,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Coverage report: requested vs covered windows, rows, tiers, empty/failed work, gaps."""
    with _local(_ctx(ctx)) as db:
        rep = market_coverage(db.store, ticker, run)
    if as_json:
        _dump(rep)
        return
    out.print(f"[bold]{ticker}[/bold] status={rep['market_status']} "
              f"settled={rep['settlement_ts']} orderbook snapshots={rep['orderbook_snapshots']}")
    if not rep["datasets"]:
        out.print("No downloads recorded for this ticker.")
    for d in rep["datasets"]:
        name = d["dataset"] + (f" ({d['interval_minutes']} min)" if d["interval_minutes"] else "")
        out.print(f"\n[bold]{name}[/bold]")
        for k in ("requested", "uncovered", "units", "pages_completed", "tiers_used",
                  "rows_stored", "earliest", "latest", "all_planned_requests_completed",
                  "largest_gap_between_trades", "missing_periods_between_first_and_last",
                  "periods_without_trades", "incomplete_periods_at_retrieval",
                  "revised_candles"):
            if k in d:
                out.print(f"  {k}: {d[k]}")
        if d["empty_windows"]:
            out.print(f"  empty windows (API returned no rows): {len(d['empty_windows'])}")
        for u in d["unfinished_or_failed"]:
            out.print(f"  [yellow]{u['status']}[/yellow] {u['window_start']}..{u['window_end']}"
                      f" {u['error'] or u['note'] or ''}")
    out.print("\nLimitations: first/last timestamps do not prove completeness; see "
              "docs/research_usage.md.")


@app.command()
def issues(
    ctx: typer.Context,
    run: Annotated[str | None, typer.Option(help="Limit to one run.")] = None,
    limit: Annotated[int, typer.Option()] = 50,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List validation issues (errors = quarantined records, warnings)."""
    with _local(_ctx(ctx)) as db:
        rows = db.issues(run)
    if as_json:
        _dump(rows[:limit])
        return
    out.print(f"{len(rows)} issues")
    for r in rows[:limit]:
        out.print(f"{r['severity']:<7} {r['code']:<16} {r['dataset']:<9} {r['record_key']} "
                  f"{r['message']}")


@app.command("export")
def export_cmd(
    ctx: typer.Context,
    dataset: Annotated[str, typer.Option(help=f"One of {sorted(DATASETS)}.")],
    output: Annotated[Path, typer.Option(help="Output file path.")],
    fmt: Annotated[str, typer.Option("--format", help="csv or parquet.")] = "parquet",
    ticker: Annotated[list[str] | None, typer.Option(help="Ticker filter (repeatable).")] = None,
    start: Annotated[str | None, typer.Option(help="UTC start (inclusive).")] = None,
    end: Annotated[str | None, typer.Option(help="UTC end (exclusive).")] = None,
    interval: Annotated[int | None, typer.Option(help="Candle interval filter.")] = None,
) -> None:
    """Export stored data to CSV or Parquet (exact decimals, NULLs preserved,
    provenance columns included)."""
    c = _ctx(ctx)
    if dataset not in DATASETS:
        _fail(f"--dataset must be one of {sorted(DATASETS)}", EXIT_INPUT)
    if fmt not in ("csv", "parquet"):
        _fail("--format must be csv or parquet", EXIT_INPUT)
    s = _time(start, "start") if start else None
    e = _time(end, "end") if end else None
    with _local(c) as db:
        n = guarded(lambda: db.export(dataset, output, fmt, tickers=ticker or None,
                                      start=s, end=e, interval=interval))
    out.print(f"Wrote {n} rows to {output.resolve()} (+ {output.name}.meta.json)")


@app.command()
def reconcile(
    ctx: typer.Context,
    delete: Annotated[bool, typer.Option(help="Delete orphan raw/temp files.")] = False,
) -> None:
    """Find raw files without a database record (left by a crash between the raw write
    and the database commit). They are redundant: the data is re-fetched on resume."""
    from .lock import WriterLock
    from .rawstore import RawStore, find_orphans
    c = _ctx(ctx)
    s = c.settings.storage

    def go() -> None:
        with WriterLock(s.lock_path), Store(s.db_path) as st:
            rs = RawStore(s.raw_dir)
            orphans = find_orphans(rs, st.raw_paths()) + rs.temp_files()
            out.print(f"{len(orphans)} orphan/temporary raw files")
            for p in orphans[:20]:
                out.print(f"  {p}")
            if delete:
                for p in orphans:
                    p.unlink(missing_ok=True)
                out.print("deleted")

    guarded(go)


@app.command()
def version() -> None:
    """Print the package version."""
    out.print(__version__)


if __name__ == "__main__":  # pragma: no cover
    app()
