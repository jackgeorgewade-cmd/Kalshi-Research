# Implementation status

Status as of 2026-11-16. Verification levels:

* **Unit/integration** – covered by the offline test suite (synthetic fixtures, mocked HTTP).
* **Live** – also exercised against the real API (`api.elections.kalshi.com`, unauthenticated)
  during development or by the opt-in live smoke test.

| Feature | Status | Verified |
| --- | --- | --- |
| Configuration (TOML, env, flags; documented precedence) | Done | Unit; live (CLI flags, TOML job) |
| HTTP transport: pooling, timeouts, pacing, bounded retries, jitter, Retry-After, typed errors, budget | Done | Unit; live (real 429s were retried successfully) |
| Cursor pagination (empty pages, repeated cursor, first-page restart, page caps) | Done | Unit; live (multi-page with small `page_limit`) |
| `/historical/cutoff` read + stored per run | Done | Integration; live |
| Trade routing by trade time, split at cutoff, boundary de-duplication | Done | Integration; live (window across 2026-09-17) |
| Candle routing by market settlement (not candle dates), one 404 fallback | Done | Integration; live (archived 2023 market, live market spanning cutoff, both-tier overlap market) |
| Bounded reconciliation when the cutoff advances mid-run | Done | Integration only (cannot be provoked live) |
| Candle windowing ≤ 5,000 periods, inclusive-bound handling, half-open semantics | Done | Unit; live (limit and boundaries probed) |
| Decimal precision, fractional quantities, zero vs NULL | Done | Unit; live data (fractional counts, sub-cent prices) |
| Live/historical candle schema adapters | Done | Unit; live |
| Validation: errors (quarantine) vs warnings | Done | Unit/integration; live run produced 0 issues |
| SQLite store, idempotent writes, conflict policy, candle revisions | Done | Unit/integration; live (re-runs: no duplicates) |
| Raw response archive (atomic, gzip, SHA-256) and provenance links | Done | Integration; live |
| Work units, per-page checkpoints, resume, invalid-cursor restart | Done | Integration; live (budget stop → resume) |
| Crash between raw write and commit (orphans, checkpoint not advanced) | Done | Integration |
| Incremental update with overlap; skipping final windows | Done | Integration; live |
| Single-writer lock | Done | Integration (CLI) |
| Discovery: series (local keyword filter), events, markets across tiers | Done | Integration; live |
| Current market quote + order book snapshot; derived (labelled) asks | Done | Integration; live |
| Bounded polling (`--duration`/`--samples`, Ctrl-C) | Done | Live (2 samples); Ctrl-C path not automated |
| Coverage / status / issues reports, JSON output | Done | Integration; live |
| CSV/Parquet export with exact decimals, NULLs, provenance, sidecar | Done | Unit; live |
| CLI with validation before network, `--dry-run`, exit codes | Done | Integration |
| Python API (`KalshiData`, `LocalStore`, streaming iterators, DataFrames) | Done | Integration; live via examples |
| Examples (walkthrough, pandas) | Done | Live |
| Opt-in live smoke test | Done | Passed 2026-11-16 (14 requests) |

## Blocked / not verified

* `https://external-api.kalshi.com/trade-api/v2` (documented recommended host and the
  default) could not be reached from the build environment (proxy policy). All live checks
  used the documented alternate host. Run `kalshi-data doctor` on your machine.
* See "Remaining uncertainties" in [api_findings.md](api_findings.md).

## Deferred (out of initial scope)

* Authentication / API keys (no endpoint used requires it).
* WebSocket streaming (would capture every order-book update; REST polling does not).
* Historical order-book reconstruction (no official historical order-book endpoint found).
* Multivariate-event (`/events/multivariate`) discovery; MVE markets do appear in market
  listings and are handled like other markets.
* Batch candle endpoint (`/markets/candlesticks`); per-market requests are simpler to
  checkpoint.
* Concurrency (unnecessary under unauthenticated rate limits).
* Hedging optimiser (explicitly out of scope).

## Checklist used during the build

1. [x] Inspect repository (empty) and instructions.
2. [x] Verify API behaviour; write `docs/api_findings.md`.
3. [x] One working end-to-end historical retrieval path (archived 2023 market, live).
4. [x] Storage, provenance and resumability.
5. [x] Discovery, current snapshots, exports, examples.
6. [x] Tests and documentation.
7. [x] Review against the specification (see final summary).
