# EconAtlas — Improvement Roadmap

Last updated: 2026-04-14

---

## 🔴 Critical (should fix soon)

### 1. Stock discovery parallelism isn't fully working
We added parallel screener.in pre-fetch but the Yahoo v10 enrichment is still sequential. On trading days, ~23 symbols still get 429'd. The stock job takes 15+ minutes when it should take 5.

**Where:** `app/scheduler/discover_stock_job.py`

### 2. discover_stock_intraday table is always empty
The INSERT inside the transaction may be silently failing. We added logging but haven't verified on a trading day yet. Without this, the 1D chart for stocks relies entirely on the Upstox fallback which is slow.

**Where:** `app/scheduler/discover_stock_intraday_job.py` — monitor logs on next trading day (Wednesday April 15)

### 3. Google Finance FX data is broken for 15+ currencies
Google returns USD/INR rate ($94.55) for ALL exotic currency pairs (PHP, PKR, IDR, VND, etc.). The FX sanity guard catches it but we're showing stale reference prices. Need a proper FX data source.

**Where:** `app/scheduler/market_job.py` — Google Finance scraper for FX

---

## 🟡 High (significant UX improvement)

### 4. Portfolio tracker
Users star stocks/MFs but there's no P&L tracking. Add buy price, quantity, date → compute returns, XIRR, allocation pie chart. This is the #1 feature that keeps users coming back daily.

**Scope:** New DB table (`portfolio_holdings`), new API routes (`/portfolio/*`), new Flutter screen

### 5. Price alerts
"Alert me when Nifty crosses 24,000" or "RELIANCE drops below ₹1,300". Push notification on threshold cross. The notification infrastructure is already built — just need alert rules per user.

**Scope:** New DB table (`price_alerts`), check in notification_job every 30s, new Flutter UI for creating/managing alerts

### 6. Artha chatbot improvements
- **Thinking text leak** (23/859 responses) — fixed, needs deploy verification
- **Watchlist inconsistency** — client sometimes sends empty `starred_items`
- **No memory across sessions** — user asks "check my watchlist" every time
- **Slow response time** for complex queries (stock screener + compare)
- **Hindi detection** — improved but needs more Romanized Hindi markers

**Where:** `app/services/chat_service.py`, `econatlas-app/lib/data/datasources/artha_data_source.dart`

### 7. Offline mode
App shows errors when offline. Should cache last known data and show "Last updated X ago" with a clear offline banner. The data models and providers need a local SQLite cache layer.

**Scope:** Add SQLite cache in Flutter app, cache responses from major APIs (market/latest, screener/stocks, etc.), show cached data with staleness indicator

---

## 🟢 Medium (nice to have)

### 8. SIP/Lumpsum calculator
Simple tool: enter monthly SIP amount + duration + expected return → shows projected corpus. MF detail screen could have "Start SIP" with pre-filled scheme. Pure frontend — no backend needed.

**Scope:** New Flutter screen, math-only (no API)

### 9. Stock/MF comparison tool
Side-by-side comparison: select 2-3 stocks → show PE, ROE, returns, score in a table. Artha can do this but a dedicated comparison screen would be cleaner.

**Scope:** New Flutter screen, uses existing `/screener/stocks/{symbol}/detail` API

### 10. Better charts
- Candlestick option for stocks (we have OHLC data)
- Volume overlay
- Moving averages (20/50/200 DMA)
- Pinch-to-zoom on chart
- Crosshair with price readout

**Scope:** Flutter chart library upgrade (fl_chart → interactive_chart or candlesticks package)

### 11. News integration in detail screens
Stock detail screen → show 3-5 recent news articles about that stock. Backend already has `news_articles` table with search. Just need to wire it to the detail screen.

**Where:** `app/api/routes/screener.py` — add news section to stock detail, Flutter detail screen

### 12. IPO calendar improvements
Upcoming IPOs with subscription status, GMP (grey market premium), allotment dates. The IPO service exists but could be richer.

**Where:** `app/services/ipo_service.py`, `econatlas-app/lib/presentation/screens/ipo/`

---

## 🔵 Low (future roadmap)

### 13. Multi-device sync
Currently watchlist/starred items are device-local (SharedPreferences). If user switches phones, everything is lost. Backend already has `device_id` — add server-side watchlist storage.

**Scope:** New API routes (`/watchlist/sync`), Flutter migration from SharedPreferences to server + local cache

### 14. Screener saved filters
"Show me stocks with PE < 20, ROE > 15%, in IT sector" — let users save and name these filters for quick access.

**Scope:** New DB table (`saved_filters`), Flutter UI for save/load

### 15. Tax harvesting suggestions
"You have ₹1.2L in STCG this year. Selling XYZ (₹8K loss) would offset ₹8K and save ₹1,200 in tax." Needs portfolio data from #4.

**Scope:** Depends on portfolio tracker (#4), tax service integration

### 16. Dividend tracker
Show upcoming ex-dates for starred stocks, estimated annual dividend income.

**Scope:** Scrape ex-date data from BSE/NSE, new Flutter section in stock detail

### 17. Performance analytics
Weekly/monthly digest: "Your watchlist gained 3.2% this week. Top: HDFC +5%, Worst: TCS -2%". Push notification every Sunday.

**Scope:** New scheduled job, notification template, optional email digest

---

## 🏗️ Infrastructure

### 18. API response caching
`/market/latest` is called every time the app opens (~2-3s response). Add Redis cache with 15s TTL → sub-100ms responses.

**Where:** `app/core/cache.py` — extend `RedisCacheMiddleware` to cover more endpoints

### 19. CDN for static data
Stock screener list, MF list, macro indicators — these change daily, not per-second. Cache aggressively at the CDN layer (Cloudflare).

**Scope:** Set `Cache-Control` headers on slow-changing endpoints, Cloudflare page rules

### 20. Rate limiting
No API rate limiting currently. A single user (or bot) can hammer the API. Add per-device rate limits.

**Scope:** FastAPI middleware with Redis-backed sliding window counter

### 21. Error tracking (Sentry)
Integrate Sentry or similar for crash reporting — both Flutter app and backend. Currently relying on logs which are insufficient for post-mortem debugging.

**Scope:** `sentry-sdk` for Python, `sentry_flutter` for Dart, free tier (5K events/month)

---

## Bugs fixed this session (2026-04-11 → 2026-04-14)

See `econatlas-backend/ERRORS.md` for the full list of 25+ bugs fixed, including:
- %change mismatch across all indices
- Holiday notification firing on weekends
- Gift Nifty sign flip and notification spam
- Stock data staleness (43% stale snapshots)
- Historical data backfill for new assets
- Widget foreground service with WakeLock
- Commodity false spike from cross-source comparison
- Crude oil wrong contract symbol (CLW00 → dynamic CLK26)
- Pull-to-refresh broken (OfflineInterceptor)
- Artha thinking text leak
- DB index corruption (REINDEX + weekly maintenance)
- Prometheus + Grafana monitoring stack
- Telegram alerting (5 critical rules)
