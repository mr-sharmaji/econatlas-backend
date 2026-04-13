# Backend error log

Last updated: 2026-04-14

Source: `GET /ops/logs` (in-memory ring buffer, ~5000 entries, ~2 min
retention window because `market_service` is spamming ~14 log/sec).

---

# FIXED BUGS (this session, 2026-04-11 → 2026-04-14)

| Issue | Fix | Commit |
|-------|-----|--------|
| MF upsert aborts on single-row failure | Per-row try/except | `76bb979` |
| Stock upsert aborts on single-row failure | Per-row try/except | `0f1f832` |
| %change mismatch (Sensex +0.28% vs -0.91%) | Dedup output + DB prev_close override | `ed9fa6e`, `7415a43`, `9641208` |
| Historical data missing (Sensex 3→1232 rows, Silver 6431) | gap_backfill seeds <90-row assets | `ed9fa6e` |
| Holiday notifications (Sunday pre-market) | `is_trading_day` gate | `f7c5af0` |
| Gift Nifty sign flip (+0.6% vs -0.6%) | `previous_close` baseline | `f7c5af0` |
| Gift Nifty duplicate notifications (3+ per morning) | 30-min cooldown, 1% bands, max 3/day | `e85ee47` |
| Commodity false spike alerts | DB-computed change, 3% bands, 2hr cooldown | `f1f73b5` |
| Stock intraday source_timestamp missing | actual Yahoo quote_ts | `f1f73b5` |
| Stock 1D chart empty (Upstox .NS suffix) | Strip .NS/.BO before lookup | `e3c91e5` |
| Macro forex_reserves dropped (unit mismatch) | Auto-scale USD millions→billions | `3972978` |
| Pull-to-refresh broken (OfflineInterceptor) | Disabled request blocking | `c7eb5af` |
| Widget refresh lag (Handler deferred in doze) | AlarmManager.setExactAndAllowWhileIdle | `e90ef85` |
| App crash (WidgetRefreshService missing SCHEDULE_EXACT_ALARM) | Added permission + fallback | `f614279` |
| 43% stale stock prices after daily job | Parallel screener pre-fetch + reconciliation | `b77043d` |
| Nifty Midcap 150 wrong sign (stale Yahoo prev_close) | DB-computed previous_close override | `9641208` |
| Market_prices duplicates (3834 rows) | Cleaned via ops/sql DELETE | manual |
| Corrupted intraday index (Sensex) | REINDEX | manual |
| MF top_holdings parse crash (company_name vs name) | Accept both field shapes | `98a1e1d` |
| Commodity deep links showing USD instead of INR | instrumentTypeHint from route | `98a1e1d` |
| MF detail 404 for stale scheme codes | Fallback name-search provider | `98a1e1d` |

---

# PENDING / OPEN BUGS

---

## 1. Stock discovery — shareholding section parse miss

**Scope:** 157 unique symbols in one stock-job window (sample: POWERICA,
SAIPARENT, GSPCROP, YASHO, HINDWAREAP, WAAREEINDO, SATIN, LANDMARK,
SOLARA, TCC, HITECH, ORIENTHOT, INDNIPPON, SOMANYCERA, MBEL, …).

**Symptom:**
```
WARNING  app.scheduler.discover_stock_job
  Shareholding JSONB MISSING for POWERICA: html_len=93593, section_found=True
```

**What it means:** The HTML was fetched fine (93 KB), the shareholding
section was located in the DOM, but the JSON-LD / structured block that
carries the quarterly shareholding breakdown didn't parse. So these
stocks are ingested but with `shareholding_quarterly = NULL`.

**Likely cause:** screener.in / source page changed the embedded JSON
shape or moved the block behind a nested element the parser isn't
walking into. 157 misses in one run is notable — this is NOT an
occasional flake.

**Where to look:** `app/scheduler/discover_stock_job.py` — search for
`Shareholding JSONB MISSING` to find the parse site, then compare
against a fresh fetch of one of the failing symbols
(`https://www.screener.in/company/POWERICA/consolidated/`).

---

## 2. `brief_job` — duplicate (market, symbol) in executemany batch

**Status:** FIXED in commit `3972978` locally — **STILL NOT DEPLOYED**.
Confirmed still crashing in production as of 2026-04-14 (MARUTI.NS).

**Symptom:**
```
ERROR  app.scheduler.brief_job  Brief stock job failed
asyncpg.exceptions.UniqueViolationError:
  duplicate key value violates unique constraint
  "idx_stock_snapshots_market_symbol_unique"
DETAIL: Key (market, symbol)=(IN, MARUTI.NS) already exists.
```

**Root cause:** Yahoo movers feed occasionally emits the same symbol
twice in one scrape (e.g. appears in both top-gainers and top-volume
slices). The `upsert_stock_snapshots` function was feeding all rows
straight to `executemany(... ON CONFLICT DO UPDATE)`, but Postgres's
ON CONFLICT only reconciles against already-committed rows, NOT against
other rows in the same batch — so same-batch duplicates always raise.

**Fix (local):** dedupe by `(market, symbol)` keeping the last
occurrence before `executemany`. See `app/services/brief_service.py`.

---

## 3. `macro_job` — forex reserves unit mismatch

**Status:** ✅ FIXED and deployed.

**Symptom:**
```
WARNING  app.scheduler.macro_job
  Macro drop out-of-range value: IN/forex_reserves
  source=trading_economics value=697120.0

WARNING  app.scheduler.macro_job
  Macro drop out-of-range value: IN/forex_reserves
  source=trading_economics value=688060.0
```

**Root cause:** `VALUE_RANGES["forex_reserves"] = (0, 5000)` assumes USD
billions. `trading_economics` reports India's reserves in USD millions
(~697120 ≈ $697 B). Every macro run dropped these rows instead of
ingesting them, so the macro dashboard was stuck on the
`bloomberg`/`rbi` feed only.

**Fix (local):** auto-scale `forex_reserves` by 1/1000 when
`value > 10_000`. See `app/scheduler/macro_job.py::_select_best`.

---

## 4. MF discovery — ETMoney two category pages 5xx

**Symptom:**
```
WARNING  discover_mutual_fund_job
  ET Money scrape failed
  url=https://www.etmoney.com/mutual-funds/best-mutual-funds
WARNING  discover_mutual_fund_job
  ET Money scrape failed
  url=https://www.etmoney.com/mutual-funds/direct-plans
```

**Impact:** Two ETMoney hub pages can't be scraped. BFS still reaches
most detail pages via other category entry points (confirmed by the
`parallel_map(www.etmoney.com) done — 48 items, 0 errors` run — 28
batches succeeding). This is why coverage isn't zero, but a long tail
of funds that were only reachable from these two pages would go
missing.

**Likely cause:** ETMoney is cloudflare-protected and intermittently
rejects the scraper UA on these specific paths. Worth adding a retry
with a second UA, or fall back to the Wayback Machine copy.

**Where to look:** `app/scheduler/discover_mutual_fund_job.py` —
search for `ET Money scrape failed` / the category_pages fetch loop.

---

## 5. MF discovery — `startup_discover_mutual_funds expired`

**Symptom:**
```
WARNING  arq.worker  job startup_discover_mutual_funds expired
```

**What it means:** When an MF direct-run is holding the worker slot for
many minutes (ETMoney BFS + mfapi.in enrichment + Groww enrichment +
upsert ≈ 8–12 min), the ARQ-scheduled `startup_discover_mutual_funds`
job hits its expiry timeout without ever getting dispatched. ARQ logs
a warning and silently drops it.

**Impact:** A scheduled run gets skipped if a manual / direct run is
in progress. Not data-loss, but can hide a "nothing refreshed today"
state because the log line looks benign.

**Possible fix:** bump `job_timeout` / expire window on the
`startup_discover_mutual_funds` entry in the ARQ settings, or make the
MF job check-in early so ARQ registers it as active.

---

## 6. Trading Economics scraper — selector stale for 2 indicators

**Symptom:**
```
WARNING  scheduler.trading_economics_scraper  TE parse failed for US/pmi_manufacturing
WARNING  scheduler.trading_economics_scraper  TE parse failed for IN/iip
```

**Scope:** Two fixed indicators fail on every macro run. Others
(`inflation`, `gdp_growth`, `repo_rate`, …) still parse cleanly.

**Likely cause:** tradingeconomics.com changed the DOM / table
structure for the US PMI Manufacturing and IN IIP pages. CSS selectors
in the scraper need updating.

**Where to look:** `app/scheduler/trading_economics_scraper.py` —
the per-indicator extraction tables.

---

## 7. Tax service — ClearTax parser broken

**Symptom (from earlier log windows):**
```
WARNING  app.scheduler.tax_job
  Tax sync failed for version=None:
  Failed to fetch ClearTax tax sources.
  errors=official_source_fetch_failed:
    Unable to parse old-regime basic exemption table.
```

**What it means:** The tax job is running on schedule but
`tax_service.fetch_cleartax_sources()` can no longer locate the
"old regime basic exemption" HTML table on ClearTax's slab page. The
site has redesigned its FY breakdown. No new tax-slab data is being
ingested until this is fixed.

**Where to look:** `app/services/tax_service.py` (or wherever the
ClearTax parser lives) — find `basic exemption` selector.

---

## 8. MF detail API — `top_holdings` shape drift (already fixed client-side)

**Status:** FIXED client-side on Flutter in commit `98a1e1d`
(MfHolding.fromJson now accepts both `{name, percentage}` and
`{company_name, corpus_per}`).

**Server-side note (optional cleanup):** The backend still returns
`top_holdings` with mixed shapes depending on whether the row came
from the legacy path or the ETMoney / Groww enrichment path. Ideally
the API layer should normalize to a single shape before serializing
so future clients don't have to fork.

**Where to look:** `app/services/discover_service.py::get_mf_by_scheme_code`
or the response builder that shapes `DiscoverMutualFundItemResponse`.

---

## 9. Log ring buffer saturated by `app.services.market_service`

**Symptom:** `/ops/logs?limit=2000` returns a ~75–100 second window
because `app.services.market_service` is emitting ~14 log lines/sec
(1459/2000 entries in 100s in one sample). Historical MF/stock job
errors roll out of the buffer in under 2 minutes, making
post-mortem debugging nearly impossible.

**Traced the exact offenders** (sampled one window):

| Count | Message prefix |
|-------|----------------|
| 153   | `Incomplete rolling window points: asset=<X>` (9 assets × 17 polls) |
| 171+  | `Rolling latest missing intraday data: asset=<X>` (9 assets × 19 polls) |
| ~200  | `Rolling latest computed: asset=<CUR>/INR` (40+ currency pairs, one per pair per poll) |

**Root cause — sub-issue 9a**: 9 commodities with no intraday feed
(`coal`, `dap fertilizer`, `iron ore`, `palm oil`, `potash`, `rubber`,
`tsp fertilizer`, `urea`, `zinc`) get logged EVERY poll cycle at
DEBUG/INFO level:

```
DEBUG  app.services.market_service
  Rolling latest missing intraday data:
  asset=coal type=commodity phase=closed stale=False
```

These are agricultural / base-metal / fertilizer commodities that
legitimately don't have a live tick feed — they're sourced from daily
spot prices only. The log is firing on expected missing data. Should
be silenced (or demoted to TRACE) OR the list of "no-intraday-expected"
assets should be checked before logging.

**Root cause — sub-issue 9b**: 40+ currency pairs (every `X/INR` from
the forex matrix) log `Rolling latest computed: asset=EUR/INR …` on
every poll. At ~40 pairs × poll frequency, that's ~80% of the
`market_service` spam. Should be batched into one summary log per
poll (`Rolling latest computed: 42 currency pairs`) or moved to TRACE.

**Where to look:** `app/services/market_service.py` — search for
`Rolling latest missing intraday`, `Incomplete rolling window`, and
`Rolling latest computed`. All three should move out of the
INFO/DEBUG hot path. Demoting them would extend the log buffer
window from ~100 s to ~30 min and unblock post-mortem of intermittent
job failures.

---

## 10a. screener.in 429 rate-limiting 20+ symbols per run

**Scope:** At least 23 unique symbols in a single ~100 s log window
returning HTTP 429 from screener.in (sample: `RSL`, `RATNAVEER`,
`SCODATUBES`, `MVGJL`, `PDMJEPAPER`, `SGIL`, `RUBYMILLS`, `KUANTUM`,
`UNIENTER`, `SHREERAMA`, `SNOWMAN`, `TAKE`, `SVLL`, `SATIA`,
`PREMIERPOL`, `PATELRMART`, `RBZJEWEL`, `KHAICHEM`, `TIRUPATIFL`,
`RAMANEWS`, `PPL`, `PDMJEPAPER`, …). These are all small-caps
processed at the tail of the stock universe.

**Symptom:**
```
DEBUG  urllib3.connectionpool
  https://www.screener.in:443 "GET /company/RSL/ HTTP/1.1" 429 1621
```

**What it means:** The stock-discovery scraper is hitting screener.in
faster than their rate limit allows. These symbols silently fall back
to partial data (no shareholding, no consolidated financials) or get
skipped entirely from the current refresh.

**Why no parallelism is protecting us:** Related to the stock-job
audit — there's no `_parallel_map(host="www.screener.in", workers=N,
per_call_delay=X)` guard like the MF job uses. Fetches go out at
whatever pace the for-loop runs. Adding per-host rate-limit back-off
(retry after N seconds) and lowering concurrency would fix this.

**Where to look:** `app/scheduler/discover_stock_job.py` — find the
`screener.in` fetch site, wrap in a retry-with-backoff on 429, and
lower parallelism for that host specifically.

---

## 10b. nseindia.com 403 (anti-bot)

**Symptom (5 occurrences in window):**
```
DEBUG  urllib3.connectionpool
  https://www.nseindia.com:443 "GET / HTTP/1.1" 403 370
```

**What it means:** NSE website bot-blocking our scraper on the root
page. If anything depends on fetching NSE's bhav-copy / indices page
directly (not api.nseindia.com which is a different CDN), it silently
fails. Needs headless browser or the official NSE API with proper
cookie warm-up.

---

## 10c. ETMoney category pages 404 (not 5xx)

**Update to issue #4:** The two failing ETMoney URLs return HTTP
**404**, not 5xx — so this isn't Cloudflare or a rate limit. ETMoney
has genuinely removed / renamed these two hub pages:
- `https://www.etmoney.com/mutual-funds/best-mutual-funds` → 404
- `https://www.etmoney.com/mutual-funds/direct-plans` → 404

Fix needs to find new entry-point URLs (likely
`/mutual-funds/featured` or similar) and update the category page
list.

---

## 11. `arq.worker` queue back-pressure on long MF runs

**Symptom:** While the MF direct-run is in flight, ARQ repeatedly
logs:
```
DEBUG  arq.worker  job startup_discover_mutual_funds already running elsewhere
DEBUG  arq.worker  job discover_stock already running elsewhere
DEBUG  arq.worker  job market already running elsewhere
```
at ~1/sec. These are DEBUG so they don't break anything, but they
flood the log buffer and contribute to issue #9.

**Possible fix:** bump the "already running" log level to TRACE, or
back off the poll interval when a slot is held.

---

## Committed but not deployed (this session)

| Commit    | Scope | File |
|-----------|-------|------|
| `76bb979` | MF upsert per-row try/except | `app/services/discover_service.py` |
| `0f1f832` | Stock upsert per-row try/except | `app/services/discover_service.py` |
| `3972978` | `brief_job` dedupe + macro forex unit fix | `app/services/brief_service.py`, `app/scheduler/macro_job.py` |

## Frontend fixes (app repo, already committed)

| Commit    | Scope |
|-----------|-------|
| `98a1e1d` | MfHolding.fromJson accepts both backend shapes; MarketDetailScreen instrumentTypeHint for commodity/crypto deep links; widget refresh flicker |
| `76bb979` (backend) | paired with client-side resilience |

---

---

# App / UX Bugs (reported 2026-04-13)

## A1. %change mismatch: market list shows +0.28%, detail shows -0.91%

**CRITICAL — affects every user on every page load.**

**Root cause: DUPLICATE rows in `market_prices` for the same asset+timestamp,
with different `change_percent` values.**

SQL proof (from `/market/latest`):
```
Sensex  price=76847.57  change%=-0.91  prev_close=77550.25  (CORRECT)
Sensex  price=76847.57  change%=+0.28  prev_close=76631.65  (STALE prev_close from April 9!)
```

Two sources (`yahoo_finance_api` + `google_finance_html`) feed different
`previous_close` values. The `get_latest_prices` API returns BOTH. The
Flutter market list screen picks the WRONG one.

**Fix needed (backend):** `get_latest_prices` must `DISTINCT ON (asset)`
keeping the most recent source per asset. OR the upsert should deduplicate
by asset + date so only the best source per day survives.

**Where:** `app/services/market_service.py::get_latest_prices` (line ~398).

---

## A2. Historical data missing for recently added assets (3Y/5Y charts empty)

**CRITICAL — broken charts for Sensex, Gift Nifty, Nifty Smallcap 250+.**

```
Nifty 50:           4568 rows, back to 2007  ✅
Nifty Bank:         4582 rows, back to 2007  ✅
Sensex:                3 rows, back to Apr 10 ❌
Gift Nifty:           38 rows, back to Mar 7  ❌
Nifty Smallcap 250:   38 rows, back to Mar 6  ❌
```

**Root cause:** Historical backfill (`yahoo_chart_api_backfill`) was never
run for these recently added assets. The daily job only writes 1 row per
day; the initial multi-year seed is missing.

**Fix:** Run the `gap_backfill` job targeting these specific assets, or add
them to the backfill asset list if they're missing.

**Where:** `app/scheduler/gap_backfill_job.py` or the asset list that
drives `yahoo_chart_api_backfill`.

---

## A3. Holiday notifications firing on Sunday

**Symptom:** User received Gift Nifty pre-market, Nifty open, and market
close notifications on Sunday April 12 (non-trading day for ALL markets).

**Root cause (suspected):** The notification scheduler runs every 30 seconds
and checks `is_trading_day_markets()`. This function falls through to
`_get_nse()` or `_get_nyse()` calendars — if neither is available (import
failure, `exchange_calendars` not installed, etc.), it falls back to
`utc_now.weekday() < 5` which returns True for weekdays. But April 12 is
Sunday (weekday=6), so that fallback would return False.

**Alternative root cause:** The Gift Nifty notification check might use a
DIFFERENT trading-day check (commodity-aware) that marks weekends as open
for international derivatives. Gift Nifty futures trade Sunday evening
on SGX — the notification might be intentionally firing, but the %change
is wrong because it's comparing against stale data.

**Where:** `app/scheduler/notification_job.py:1112-1121` (India trading
day check) and `app/scheduler/trading_calendar.py:335-437`.

---

## A4. Gift Nifty notification %change sign flipped

**Symptom:** Gift Nifty was actually down -0.6%, but notification said
"+0.6%". The absolute value was roughly correct but the sign was wrong.

**Root cause (suspected):** The notification text generation compares
the current Gift Nifty price against `previous_close`. If the
`previous_close` in the DB is stale (from a different session), the
calculated change can flip sign.

Specifically: Gift Nifty at 23,724 vs previous_close=23,584 (stale,
from 2 days ago) = +0.6%. But vs actual last close=23,868 = -0.6%.

**Related to A1:** the same duplicate-source problem that gives wrong
`previous_close` for Sensex also affects Gift Nifty.

**Where:** `app/scheduler/notification_job.py` — find Gift Nifty notification
builder and trace where `previous_close` comes from.

---

## A5. Nifty Midcap 150 showing positive when negative

**Root cause:** Same as A1. Multiple rows in `get_latest_prices` with
different `change_percent` values:
```
Nifty Midcap 150  price=21177.6  change%=1.01  prev_close=20965.5  (WRONG baseline)
Nifty Midcap 150  price=21177.6  change%=1.01  prev_close=20965.5  (duplicate)
```

The market was actually negative for Midcap 150, but the previous_close
from the source used is stale.

---

## A6. Widget not updating in real-time

**Symptom:** Widget shows stale data during live market hours, requires
multiple refreshes, data mismatch between widget and app.

**Root causes (multiple):**
1. **Refresh interval:** WorkManager periodic refresh is set to 15 min
   minimum (Android OS constraint). During live market, this is too slow.
2. **Data pipeline:** Widget snapshot reads from the same providers as the
   app, which depend on `latestMarketPricesProvider` → `/market/latest`.
   If the backend's latest prices are stale (wrong %change from A1), the
   widget inherits the same stale/wrong data.
3. **Background fetch reliability:** `HomeWidget.updateWidget` may silently
   fail if the app process is killed, and WorkManager's minimum interval
   is 15 min — can't go lower on Android.

**Fix ideas:**
- Add a foreground service or use `AlarmManager.setExact` for 2-min refresh
  during market hours (requires explicit user permission)
- Show "Loading..." placeholder while refresh is in flight (already
  partially implemented with the spinner)
- Fix A1 first so the data that does arrive is at least correct

**Where:** `android/app/src/main/kotlin/.../DashboardHomeWidgetProvider.kt`,
`lib/presentation/providers/dashboard_widget_providers.dart`,
and `android/app/src/main/AndroidManifest.xml` (WorkManager config).

---

## A7. Live stock data not updating during trading day

**Symptom:** Stock prices showing Thursday April 10 data on Monday April 13.
`source_timestamp=2026-04-10T10:30:00` despite `ingested_at=2026-04-13T11:23`.

**Root cause:** The `discover_stock` job ran today and ingested rows, but
the Yahoo v10 API it queries only returned Thursday's close — Friday April
11 was likely a holiday (Ram Navami / Eid-ul-Fitr 2026) so Yahoo didn't
have a Friday session. But TODAY (Monday) the market IS open and has
live prices. The job should be fetching TODAY's live quotes, not stale
Thursday ones.

**Possible sub-cause:** The discover_stock job runs via screener.in +
Yahoo v10. Screener.in might cache aggressively and not reflect intraday
prices. Yahoo v10 might need `interval=1d&period=1d` for today's
live quote instead of the historical endpoint.

**Where:** `app/scheduler/discover_stock_job.py` — the Yahoo v10 / screener
fetch, and whether the intraday job (`discover_stock_intraday`) is running.

---

## A8. Multiple/duplicate Gift Nifty notifications

**Symptom:** User received both "Gift Nifty -1.1% at 6:37 AM" and "Gift
Nifty -0.6% at 6:32 AM" and then "Gift Nifty +0.6% at 9:11 AM" — three
Gift Nifty notifications in one morning.

**Root cause:** The notification dedup key format is
`{today_str}_market_open_{market}`. If Gift Nifty fires as a pre-market
alert AND then again as a market-open alert, they'd have different dedup
keys and both fire. Or the 30-second poll interval catches different
price levels and fires separate "significant move" alerts.

**Where:** `app/scheduler/notification_job.py` — search for Gift Nifty
notification generation and the dedup key logic.

---

## Priority order

1. **A1 — %change mismatch (duplicate latest prices)** — highest-impact UX bug
2. **A2 — Historical backfill for new assets** — broken charts
3. **A3 + A4 — Holiday notifications + Gift Nifty sign flip** — notification trust
4. **A6 — Widget refresh reliability** — daily usage
5. **A7 — Live stock data freshness** — market hours UX
6. **Deploy the 4 committed backend fixes** — upsert resilience
7. **Shareholding parser (#1)** — largest scope (157 symbols/run)
8. **Log buffer spam (#9)** — unblocks all future post-mortems
9. **ClearTax tax parser (#7)** — no new tax data landing
10. **TE scraper selectors (#6)** — 2 indicators stale
