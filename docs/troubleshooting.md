# Troubleshooting

## `doctor` says the API is unreachable

* Network/proxy may block `external-api.kalshi.com`. Try the other documented production
  host: `--base-url https://api.elections.kalshi.com/trade-api/v2` (or set
  `KALSHI_DATA_BASE_URL`).
* Exit code 5 means network failure after retries; 3 means the API answered with an error.

## HTTP 429 / slow downloads

* 429 = rate limited. The engine retries with exponential backoff (bounded by
  `max_retries`). Kalshi does not publish unauthenticated limits.
* If you see many 429 warnings, increase pacing: `--min-interval 1.0` or
  `[api].min_request_interval`.
* If a run ends with `RateLimitError`, the unit is `failed`; just `resume RUN_ID` later.

## Empty results

Check `kalshi-data coverage --ticker T`:

* `empty_windows` – the API answered successfully with no rows ("no trades returned").
  Common for thin markets or for ranges outside trading activity.
* `unfinished_or_failed` with `failed` – a request failed (see `error`); this is *not*
  "no data". Resume to retry.
* `not_found` – the ticker exists in neither tier. Check the spelling with
  `kalshi-data markets --event ...`.
* Windows are clipped to the market's life (creation .. settlement + 1 day). A range
  entirely outside it produces no work units.
* Candle date filters apply to the candle's **period end**; a daily candle for day D ends
  after D, so include the following day in `--end`.

## A market from the past cannot be found

* `kalshi-data market TICKER` checks the live tier and then the historical tier.
* Listing old markets by *series* may not work for legacy products (e.g. `INX`, `INXD`
  returned nothing on 2026-11-16, and the legacy event `INXD-23DEC29` belongs to series
  `KXINX`). List by **event** instead: `kalshi-data markets --event INXD-23DEC29`.

## Resume

* `kalshi-data runs` → find runs with status `interrupted`/`incomplete`/`failed`.
* `kalshi-data status RUN_ID` → see which units are unfinished and why.
* `kalshi-data resume RUN_ID [--budget N]`. Completed units are skipped; unfinished trade
  units continue from their last committed page cursor. If the historical cutoff moved
  since the run started, affected units are re-routed and restarted (no duplicates result).
* "Data directory is locked" (exit 4): another `kalshi-data` process is writing. If none
  is running (e.g. after a hard crash on another host), delete `data/.writer.lock`.
  Stale locks from dead processes on the same host are cleared automatically.

## Orphan raw files

`doctor` may report orphan raw files after a crash between writing a raw response and
committing it to the database. They are harmless and redundant (the page is re-fetched on
resume). `kalshi-data reconcile --delete` removes them.

## Validation issues

`kalshi-data issues --run RUN_ID`. `error` issues are quarantined records (not stored;
the raw record is in `raw_json`). `warning` issues mark stored records with oddities
(unparseable optional field stored as NULL, unknown new API field, conflicting re-download).

## Request budget

`--budget N` caps HTTP attempts (including retries). When reached, the run stops with
status `incomplete` (exit 1) and can be resumed with a new budget.

## Re-running gave "skipped" units

That is the incremental-update rule: a window fetched at least `overlap_seconds` after its
end is final and not fetched again. To force a re-download, use a fresh `--data-dir`, or
raise `[collection].overlap_seconds` so that fewer completed windows count as final.
