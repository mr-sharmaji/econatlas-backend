# Daily AI-generated video content — implementation plan (v2, locked)

**Status**: design locked, ready for Phase 0 build.
All decisions below are final — this doc was iterated across a design
session and every "open question" from v1 has been resolved.

---

## 1. Locked decisions (at a glance)

| # | Decision | Value |
|---|---|---|
| 1 | **Feature scope** | In-app only (no cross-posting in MVP) |
| 2 | **Video types** | Daily recap + weekly roundup + weekly educational explainer |
| 3 | **Language** | English only |
| 4 | **Audience** | Mixed general, neutral-accessible tone |
| 5 | **Compliance stance** | Factual reporting + named stocks, NO advice language |
| 6 | **Moderation flow** | Auto-publish with pre-render validators |
| 7 | **Personalization** | Same video for all users |
| 8 | **Resolution** | 1080 × 1920 H.264, NVENC-encoded |
| 9 | **Voice** | `en-IN-NeerjaNeural` (Edge TTS) for all video types |
| 10 | **Captions** | Always-on burned into frames, word-by-word from TTS boundaries |
| 11 | **Music** | Soft ambient, ducked under voice (Pixabay royalty-free, pre-downloaded) |
| 12 | **Bumpers** | 2 s intro + 2 s outro, MoviePy-generated using app theme colors |
| 13 | **Chart style** | Matplotlib themed to match `lib/core/theme.dart` (#0F1E31 bg, #00E676/#FF5252) |
| 14 | **Backgrounds** | Pollinations.ai, pre-cached 20–30 images, rotated |
| 15 | **LLM** | **Reuse existing `ai_service.call_ai()` via OpenRouter `gpt-oss-120b:free`** |
| 16 | **GPU strategy** | Tier 1: NVENC encoding + MiDaS depth parallax (4 GB GTX 1050 Ti) |
| 17 | **Thumbnails** | Custom MoviePy composition (chart snapshot + title overlay) |
| 18 | **Holiday handling** | Skip cron entirely when market is closed |
| 19 | **Flutter nav** | Card on Overview screen + "See All" → dedicated feed screen |
| 20 | **Player UX** | Classic card list + tap-to-play (not TikTok swipe) |
| 21 | **Notifications** | In-app "new video" badge only, no push |
| 22 | **Analytics** | None in MVP — ship and decide by vibes |
| 23 | **Retention** | Daily: 30 days · Weekly: 12 weeks · Explainers: forever |
| 24 | **Phase 0 test topic** | "What is P/E ratio?" with Reliance as example |
| 25 | **Status bar copy** | `● NSE CLOSED · 15:30 IST` (NOT "LIVE" — video is generated post-close). Red static dot, no pulse. |
| 26 | **FII/DII gating** | Polling cron (every 30 min from 17:00 to 21:30 IST) — waits for `macro_indicators.fii_net_cash` / `dii_net_cash` row for today's date before generating. Irregular data arrival (sometimes 17:00, sometimes 20:00+) means a single-time cron isn't reliable. |
| 27 | **Video layout** | Multi-shot edit (5 shots daily, 7 shots weekly, 5 shots explainer) — NOT a single static layout held for the whole duration. See §4. |
| 28 | **New data to fetch** | **Corporate earnings calendar** (new scraper, new table) — defer to Phase 3. **F&O expiry dates** — computable via calendar helper, no scraping. See §13.5. |

---

## 2. What we're building

Short-form (45–75 second) vertical videos auto-generated from EconAtlas
market data + AI-generated backgrounds + themed data charts + natural
English narration, served through the app as a feed on the Overview
screen.

**Free-forever pipeline**: every component runs on the existing Windows
11 + Docker Desktop + GTX 1050 Ti host. No paid APIs, no GPU rental,
no third-party licensing fees beyond OpenRouter's free `gpt-oss-120b`
tier (1000 req/day, we'll use ~7/day).

**Key principle — multi-shot edit, not one static layout.** A single
template held for 45 s feels cheap and auto-generated. Each video type
is broken into **5–7 distinct shots** cut every 5–15 s, each with its
own layout, entry animation, and transition out. This is the single
biggest "looks like a real production" lever.

Total duration: **45 s (daily, 5 shots)** · **75 s (weekly, 7 shots)** · **60 s (explainer, 5 shots)**.

Full shot-by-shot layouts are in §2a (daily), §2b (weekly), §2c (explainer), §2d (techniques).

---

### Shared visual language (applies to every shot of every video type)

**Colors** (extends `lib/core/theme.dart`):
```
#0F1E31   bg / base navy
#0A1628   bg deeper — darkest layer
#00E676   accent-up — green (positive)
#FF5252   accent-down — red (negative)
#FFFFFF   primary text
#A0B5CF   muted text (labels, timestamps)
#00D4FF   data-highlight — cyan (keyframes, pointers)
#FFAB40   warning-amber (neutral/mixed)
```

**Typography** (Inter or equivalent):
```
Display   140pt / 900 weight   Hero numbers (Nifty close, headline %)
Title     72pt  / 800 weight   Section titles, stock names
Heading   44pt  / 700 weight   Card titles
Body      34pt  / 500 weight   Captions, descriptions
Label     24pt  / 600 weight   Muted labels, timestamps
Micro     18pt  / 500 weight   Status bar, disclaimers
```

**Motion principles**:
```
Entry          ease-out cubic, 400–600ms, staggered 100ms between siblings
Exit           ease-in cubic, 300ms
Scale-in       start at 0.85×, grow to 1.0×
Translate      entering from direction of reading (L→R for English)
Camera push    continuous 1.00→1.04× scale over each 5s shot
Parallax       background moves at 30% of foreground speed
Number counter ease-out, 1–1.5s per 1000 units counted
Chart draw     ease-out, with leading cursor dot at current time
Beat sync      section transitions align with music bar (120 BPM = 0.5s)
```

**Persistent UI chrome (every shot, every video)**:

```
┌─────────────────────────────┐
│ ● NSE CLOSED · 15:30 IST    │← top status bar (18pt Micro)
│   14 Apr 2026  ·  1/5       │    red solid dot (NO pulse — market closed)
│                             │    step indicator (1/5, 2/5, …)
│        [shot content]       │
│                             │
│ ░░░░░░▓▓▓▓▓▓░░░░░░░░░░░░░  │← progress bar (fills with video time)
│ Educational · Not advice    │← disclaimer (burned, always visible)
└─────────────────────────────┘
```

**Status bar state by video type** (no "LIVE" anywhere — market is always closed at generation time):

| Video type | Status bar leading text | Dot state |
|---|---|---|
| Daily recap | `● NSE CLOSED · 15:30 IST` | Red solid, no pulse |
| Weekly roundup | `● WEEK CLOSE · Fri 15:30 IST` | Red solid, no pulse |
| Explainer | `● LEARN · Ep 01 · P/E Ratio` | Cyan solid (data-highlight color, not a market indicator) |

**Why no pulsing dot**: the original mock had a `● LIVE` indicator
which was misleading — the videos are generated post-close using
settled data (and in the daily case, after FII/DII arrives hours after
close). Pretending it's live would be dishonest. Static red dot +
"NSE CLOSED" label is both truthful and still visually reads as a
broadcast badge.

---

### §2a. Daily Recap — 5 shots (45 s)

#### Shot 1 · Title Reveal (0:00–0:05)

```
┌─────────────────────────────┐
│ ● NSE CLOSED · 15:30 IST    │
│   14 Apr 2026 · 1/5         │
│                             │
│  [parallax bg @ 35% opacity,│
│   MiDaS depth push-in]      │
│                             │
│       ┏━━━━━━━━━━━━━┓       │
│       ┃   MARKET    ┃       │← 140pt display
│       ┃   TODAY     ┃       │    #FFFFFF
│       ┗━━━━━━━━━━━━━┛       │
│     ━━━━━━━━━━━━━━━━━       │← animated underline
│                             │
│     Mon, 14 Apr 2026        │← 24pt label
│     ┌──────────┐            │
│     │ + 0.52%  │            │← headline chip (accent-up)
│     └──────────┘            │
│                             │
│ ░░░░░░░░░░░░░░░░░░░░░░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: bg fades + slow push (1.0→1.04 over 5 s). Status bar slides from top (400 ms ease-out). "MARKET TODAY" types on letter-by-letter (0:01–0:02.5). Underline sweeps L→R. Headline chip scales in from 0.8× with bounce.

#### Shot 2 · Index Dashboard (0:05–0:20)

```
┌─────────────────────────────┐
│ ● NSE CLOSED · 15:30 IST    │
│   14 Apr 2026 · 2/5         │
│                             │
│  NIFTY 50                   │← 24pt label
│  ╔═══════════════════════╗  │
│  ║      22,450           ║  │← 140pt, counter anim
│  ║  ↑ +0.52%   +117 pts  ║  │    #00E676 with glow
│  ║  ╱╲    ╱╲╱╲ ╱●        ║  │← intraday sparkline
│  ╚═══════════════════════╝  │    with leading dot
│                             │
│  ┌───────────┬───────────┐  │
│  │ SENSEX    │ BANK NIFTY│  │
│  │  73,842   │  48,520   │  │← 72pt
│  │  ↑ 0.48%  │  ↓ 0.12%  │  │
│  │  ╱╲ ╱ ╱   │  ╲╱╲ ╲    │  │
│  └───────────┴───────────┘  │
│                             │
│ ░░░▓▓▓▓▓▓░░░░░░░░░░░░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: Previous shot slides up + fades. Nifty panel slides up from bottom. Counter ticks 22,000→22,450 over 1.5 s. Sparkline draws L→R with cursor dot. Sensex + Bank Nifty cards slide in from left/right, 150 ms stagger.

#### Shot 3 · Movers Showcase (0:20–0:35)

```
┌─────────────────────────────┐
│ ● NSE CLOSED · 15:30 IST    │
│   14 Apr 2026 · 3/5         │
│                             │
│       TOP GAINER            │
│  ┌─────────────────────┐    │
│  │ 🟢  TCS             │    │← glass panel (blur)
│  │     Tata Cons.      │    │
│  │   ₹ 4,125           │    │← 100pt counter
│  │   ↑  +3.2%          │    │← accent-up badge
│  │   ╱╲  ╱╲╱  ╱●       │    │← sparkline
│  └─────────────────────┘    │
│                             │
│       TOP LOSER             │
│  ┌─────────────────────┐    │
│  │ 🔴  WIPRO           │    │
│  │     Wipro Ltd       │    │
│  │   ₹ 512             │    │
│  │   ↓  -2.1%          │    │
│  │   ╲  ╲╱╲ ╲●         │    │
│  └─────────────────────┘    │
│                             │
│  BEST SECTOR: IT  +1.8%     │← pill at bottom
│                             │
│ ░░░░░░░░░░▓▓▓▓▓▓▓░░░░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: Zoom-punch transition from Shot 2 (scale 1.0→1.1 + fade, 300 ms). Gainer whips in from right with 8° rotation, settles. Counter + sparkline. Delay 1 s. Loser whips in from left (mirrored). Sector pill slides up last.

#### Shot 4 · Institutional Flows (0:35–0:40)

```
┌─────────────────────────────┐
│ ● NSE CLOSED · 15:30 IST    │
│   14 Apr 2026 · 4/5         │
│                             │
│     INSTITUTIONAL FLOWS     │← 44pt heading
│     ────────────────        │
│                             │
│  FII  (Foreign Inst.)       │
│  ┌─────────────────────┐    │
│  │ + ₹ 2,450 Cr        │    │← counter
│  │ ▓▓▓▓▓▓▓▓▓▓▓▓░░░░░░  │    │← green bar fills L→R
│  └─────────────────────┘    │
│                             │
│  DII  (Domestic Inst.)      │
│  ┌─────────────────────┐    │
│  │ + ₹ 1,280 Cr        │    │
│  │ ▓▓▓▓▓▓▓░░░░░░░░░░░  │    │
│  └─────────────────────┘    │
│                             │
│  ┌─────────────────────┐    │
│  │  Net: +₹ 3,730 Cr   │    │← summary chip
│  │  Buying sentiment   │    │
│  └─────────────────────┘    │
│                             │
│ ░░░░░░░░░░░░░░░▓▓▓▓▓░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: Previous slides left. Heading + underline. Bars fill L→R with counters in sync (200 ms stagger). Summary chip scales in.

*(Note: this shot requires FII/DII data to exist in `macro_indicators` for today. The polling cron in §9 waits for this before generating.)*

#### Shot 5 · Outro (0:40–0:45)

```
┌─────────────────────────────┐
│                             │
│    [bg fades to dark]       │
│                             │
│        ┏━━━━━━━━━┓          │
│        ┃  logo   ┃          │← 200pt, subtle
│        ┗━━━━━━━━━┛          │    scale-pulse breath
│                             │
│    That's your market       │← 44pt
│    snapshot for today       │
│   ─────────────────────     │
│    Explore the full         │← 24pt muted
│    analysis in the app      │
│                             │
│ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ │← full
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: Cross-dissolve to darker bg. Logo fades in + breath scale 0.9→1.0→0.98. Text types on. Underline sweeps. Music fades out over 1 s.

---

### §2b. Weekly Roundup — 7 shots (75 s)

Same visual language as daily, different shot sequence:

| # | Shot | Duration | New layout element |
|---|---|---|---|
| 1 | Title reveal ("This week in markets" + 5-day calendar chip) | 0:00–0:05 | Mini week calendar with daily up/down dots |
| 2 | Index performance grid | 0:05–0:20 | Full-week line with daily closing dots |
| 3 | Sector leaderboard (bar race, top 7 sectors) | 0:20–0:35 | Horizontal bars grow L→R, ranked |
| 4 | Top movers showcase (3 stocks) | 0:35–0:55 | Same card style as daily Shot 3 but 3 stacked |
| 5 | FII/DII 5-day flows | 0:55–1:05 | 5 daily mini-bars + weekly total chip |
| 6 | Coming up next week | 1:05–1:15 | Event cards — **data constraints see below** |
| 7 | Outro | 1:10–1:15 | Same as daily outro with weekly copy |

#### Shot 6 · Coming Up Next Week — data dependencies

This shot shows 3 event cards. What we can populate in MVP:

| Event type | Source | Status |
|---|---|---|
| **Macro events** (RBI MPC, CPI data, Fed meeting) | `economic_calendar` table via `macro_service.get_upcoming_events(days_ahead=7)` — already populated by `econ_calendar.py` cron | ✅ **Available now** |
| **IPO calendar** (new IPO opens, closes, listings) | `ipo_snapshots` table, filter `status='upcoming'` | ✅ **Available now** |
| **F&O expiry** (Nifty weekly, Bank Nifty weekly, monthly expiry) | **Calendar rule helper** — last Thursday for monthly, Thursday for Nifty weekly, Wednesday for Bank Nifty weekly (post Dec 2024). NO scraping needed. | ⚙️ **Build in Phase 1** (new module `app/scheduler/video/fno_expiry_calendar.py`, ~50 lines) |
| **Corporate earnings** (TCS Q4, Infosys Q4, HDFC AGM etc.) | **NOT scraped anywhere currently** — best free sources: NSE corporate announcements RSS, Moneycontrol earnings calendar, Screener.in | ❌ **Deferred to Phase 3** — see §13.5 data backlog. MVP skips corporate earnings in Shot 6. |

**MVP Shot 6 content rules**: pick the 3 most-important upcoming events across macro + IPO + F&O expiry, ranked by importance (RBI/Fed always wins, then IPO opens, then F&O expiries). If fewer than 3 events in the next 7 days, show whatever is available and leave the layout with fewer cards.

```
┌─────────────────────────────┐
│ ● WEEK CLOSE · Fri 15:30    │
│   11 Apr 2026 · 6/7         │
│                             │
│     COMING UP NEXT WEEK     │
│     ─────────────────       │
│                             │
│  ┌─────────────────────┐    │
│  │ 📅  WED 16 APR      │    │← RBI MPC (macro)
│  │ RBI MPC Meeting     │    │   from economic_calendar
│  └─────────────────────┘    │
│                             │
│  ┌─────────────────────┐    │
│  │ 📅  WED 16 APR      │    │← F&O weekly expiry
│  │ Bank Nifty expiry   │    │   (computed, not scraped)
│  └─────────────────────┘    │
│                             │
│  ┌─────────────────────┐    │
│  │ 📅  THU 17 APR      │    │← Nifty weekly expiry
│  │ Nifty weekly expiry │    │   (computed)
│  └─────────────────────┘    │
│                             │
│ ░░░░░░░░░░░░░░░░▓▓▓▓░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```

Motion: Event cards slide in from right, staggered 200 ms. Feels like a calendar opening page-by-page.

---

### §2c. Educational Explainer — 5 shots (60 s)

Different visual vocabulary — concept-focused, not data-heavy.

| # | Shot | Duration | Purpose |
|---|---|---|---|
| 1 | Concept title card with animated `?` | 0:00–0:05 | "What is P/E ratio?" hero |
| 2 | Definition panel (text card + formula) | 0:05–0:20 | One-sentence def + math-style formula |
| 3 | Example visualization (real Indian stock) | 0:20–0:45 | Ticker + price + EPS + calculated P/E reveal |
| 4 | Takeaway bullets (3 cards) | 0:45–0:55 | "What to remember" |
| 5 | Outro | 0:55–1:00 | Same outro template, "More explainers in Learn tab" CTA |

#### Shot 3 · Example Visualization (the heaviest shot)

```
┌─────────────────────────────┐
│ ● LEARN · Ep 01             │
│   P/E Ratio · 3/5           │
│                             │
│  A REAL EXAMPLE             │
│                             │
│  ┌─────────────────────┐    │
│  │ 🔵  RELIANCE        │    │
│  │     Reliance Inds   │    │
│  └─────────────────────┘    │
│                             │
│  Share Price                │
│  ┌─────────────────────┐    │
│  │     ₹ 2,850         │    │← counter 0→2850
│  │   ↑ +1.2% today     │    │
│  └─────────────────────┘    │
│                             │
│  Earnings Per Share         │
│  ┌─────────────────────┐    │
│  │     ₹ 100           │    │← counter 0→100
│  │   annualized        │    │
│  └─────────────────────┘    │
│                             │
│  P/E = 2,850 / 100          │← formula types on
│     ┌──────────────────┐    │
│     │       28.5       │    │← BIG result
│     │  times earnings  │    │    scales in with bounce
│     └──────────────────┘    │    glow pulse
│                             │
│ ░░░░░░▓▓▓▓▓▓▓▓▓▓░░░░░░░░░ │
│ Educational · Not advice    │
└─────────────────────────────┘
```
**Motion**: stock header slides in. Price counter ticks 0→2,850. EPS counter ticks 0→100. Formula line types on. Result card scales in from 0 with bounce, glow pulse. Voice says "twenty-eight point five" in sync with the counter reveal.

---

### §2d. The 10 "feels real" techniques used across all videos

| # | Technique | Where | Why it works |
|---|---|---|---|
| 1 | **`● NSE CLOSED` status badge** (red solid, not pulsing) | All shots | Truthful — market is closed when we generate. No fake "live" pretence. Still reads as a broadcast indicator. |
| 2 | **Real NSE close timestamp** (15:30 IST) | All shots | Grounds the video in actual market reality |
| 3 | **Progress bar** at bottom that fills with video time | All shots | Mimics a real video player's chrome. Subconscious "I know where I am" cue. |
| 4 | **Step indicator** (1/5, 2/5, …) in top bar | All shots | Structural cue, like "topic 1 of 4" |
| 5 | **Number counters** that tick up instead of appearing | Every stat | Real dashboards have this. Dead numbers feel fake. |
| 6 | **Chart draws with leading cursor dot** | Every sparkline | Universal finance-viz trope |
| 7 | **Multi-shot edit**, cut every 5–15 s | All videos | One static layout = cheap automation. Multi-shot = TV broadcast. |
| 8 | **Whip / zoom-punch transitions** on beats | Between shots | Aligned to music beats = "edited to rhythm", not auto |
| 9 | **Camera push-in** (1.00 → 1.04×) on every shot | Continuous | Subtle but omnipresent. Static images feel "filmed". |
| 10 | **Glass-morphism panels** (blur bg, rounded 16px, translucent) | Data cards | Mimics app's own UI. Video feels like a tour of the app. |

Audio: Neerja narration + ducked ambient music (music -14 dB LUFS under
voice, lifts to -20 dB when voice is silent).

---

## 3. Content types (cron schedule)

### 3a. Daily Market Recap
- **Fires**: Mon–Fri — **polling cron every 30 min from 17:00 to 21:30 IST**
  (waits for today's FII/DII data in `macro_indicators`, which arrives at
  irregular times post-close — sometimes 17:00, sometimes 20:00+)
- **Skip condition**: NSE holiday — quick sentinel check on
  `market_prices_intraday` row count for today (<50 rows = closed)
- **Idempotency**: unique index on `(type, generated_at::date)` ensures
  only one daily recap per date regardless of how many cron firings hit
- **Duration**: 45 s (~130 words @ Neerja pace)
- **Structure** (timing):
  - 0:00–0:02 — Branded intro bumper (logo + date)
  - 0:02–0:07 — Hook: *"Indian markets today — here's the 45-second recap."*
  - 0:07–0:22 — Indices (Nifty, Sensex, Bank Nifty) with percent moves
  - 0:22–0:37 — Top gainer + top loser + top sector
  - 0:37–0:42 — FII/DII flows
  - 0:42–0:45 — Outro bumper with disclaimer
- **Chart**: animated Nifty intraday line (today's 30-min ticks), progressive draw from 0:10 to 0:25
- **Data**: `market_prices_intraday`, `market_prices`, `discover_stock_snapshots`, `institutional_flows_overview`

### 3b. Weekly Roundup
- **Fires**: Friday — **polling cron from 17:30 to 22:00 IST** (waits for Friday's FII/DII data before generating, same pattern as daily)
- **Duration**: 75 s (~220 words)
- **Structure**:
  - 0:00–0:02 — Intro bumper
  - 0:02–0:08 — Hook: *"This week in Indian markets…"*
  - 0:08–0:23 — Weekly indices with day-by-day highs/lows annotated
  - 0:23–0:38 — Best & worst sectors of the week (bar race leaderboard)
  - 0:38–0:53 — Top 3 individual stock moves
  - 0:53–1:03 — FII/DII weekly total + 5-day mini-bars
  - 1:03–1:10 — **"Coming up next week"** — 3 event cards from the data we have
  - 1:10–1:15 — Outro + disclaimer
- **Chart**: animated weekly Nifty line with daily closing dots
- **Data sources**:
  - Index series → `market_prices`
  - Sector performance → `discover_stock_snapshots` weekly aggregation
  - Top movers → `discover_stock_snapshots` ORDER BY weekly percent_change
  - FII/DII weekly → `macro_indicators.fii_net_cash` / `dii_net_cash` (5 rows SUM)
  - Next-week events → **3 sources**, ranked by importance:
    1. **Macro events** from `economic_calendar` (RBI MPC, CPI data, Fed meeting) via `macro_service.get_upcoming_events(days_ahead=7)` ✅ ready
    2. **F&O expiries** computed from `fno_expiry_calendar.get_upcoming_expiries()` ⚙️ build in Phase 1
    3. **IPO calendar** from `ipo_snapshots WHERE status='upcoming'` ✅ ready
  - **Corporate earnings dates** (TCS Q4, Infosys Q4 etc.) — ❌ NOT scraped anywhere yet, deferred to Phase 3. See §13.5.

### 3c. Educational Explainer
- **Fires**: Wednesday **10:00 IST** (weekly, any day)
- **Duration**: 60 s (~170 words)
- **Topic rotation**: 52 pre-seeded topics (see §16) — one per week, one year of non-repeating content
- **Structure**:
  - 0:00–0:02 — Intro bumper
  - 0:02–0:12 — Hook: *"Let's decode {concept} in 60 seconds…"*
  - 0:12–0:27 — Definition (one simple sentence, one clarifying sentence)
  - 0:27–0:47 — Concrete Indian example using real Nifty/Reliance/HDFC numbers
  - 0:47–0:57 — Practical takeaway
  - 0:57–1:00 — Outro + disclaimer
- **Chart**: concept-specific (e.g., for P/E ratio: a simple bar showing price vs earnings)
- **Data**: evergreen, no live DB dependency

---

## 4. The final tech stack

| Stage | Tool | Location | Notes |
|---|---|---|---|
| **Script generation** | `app/services/ai_service.py::call_ai()` — OpenRouter `gpt-oss-120b:free` → fallback chain | Existing, inside Docker | Already battle-tested, already handles rate limits + preamble cleanup. **No new LLM integration needed.** |
| **Script validation** | New `script_validator.py` — regex for forbidden phrases + numeric-grounding check | Inside Docker | Critical safety rail. Rejects + retries any script with advice language or hallucinated numbers. |
| **Voice (TTS)** | `edge-tts` — `en-IN-NeerjaNeural` voice | Inside Docker (Python) | Returns MP3 + word-boundary JSON for caption timing |
| **Background images** | Pollinations.ai, pre-cached 20–30 images in repo | Fetched once during setup | Eliminates per-video API calls, survives Pollinations downtime |
| **Background motion (Tier 1 GPU win #1)** | **MiDaS DPT-Hybrid** depth estimation + 3D camera synthesis | On GPU (~1.5 GB VRAM) | Turns a still Pollinations image into a cinematic 2.5D camera move. This is the "cool trick" that makes videos feel like real footage. |
| **Background music** | Pixabay royalty-free tracks, 5–10 pre-downloaded | Static repo assets | No attribution required, no API call, no licensing risk |
| **Chart frames** | `matplotlib` themed to `lib/core/theme.dart` colors | Inside Docker, CPU | ~30 frames × 33 ms = 1 s of animation. Rendered as transparent PNGs. |
| **Compositing** | `moviepy` (wraps ffmpeg) | Inside Docker, CPU orchestration | Layers: bg parallax → chart → captions → bumpers → music duck |
| **Caption timing** | Edge TTS word boundaries (no ML needed) | In-process Python | Each word has start/end offset already — just synthesize TextClip per word |
| **Final encoding (Tier 1 GPU win #2)** | **ffmpeg with `h264_nvenc`** hardware encoder | On GPU (~200 MB VRAM) | 5–10× faster than `libx264`. 60 s video: ~20 s encode vs ~5 min on CPU. |
| **Thumbnail** | Custom MoviePy composition (chart snapshot + title overlay) | Inside Docker | Rendered as separate JPEG, not extracted from MP4 |
| **Storage** | Local disk volume `/app/static/videos/` | Docker volume | ~200–300 MB steady state with retention policy |
| **Serving** | New `app/api/routes/videos.py` with HTTP Range support | Existing FastAPI | Byte-range streaming for mobile seek |
| **Playback** | Flutter `video_player: ^2.9` plugin | Existing Flutter app | Official plugin, handles MP4 + range requests |

### New dependencies to add

**Python (`requirements.txt`)**:
```
edge-tts>=6.1
moviepy>=1.0
torch>=2.1           # already present? check
torchvision>=0.16    # for MiDaS
pillow>=10.0
numpy>=1.24
```

**Flutter (`pubspec.yaml`)**:
```
video_player: ^2.9
```

**System (already installed on the host)**:
- `ffmpeg` with NVENC support — verify via `ffmpeg -encoders | grep nvenc`
- NVIDIA drivers + CUDA runtime (for MiDaS + NVENC)
- Docker GPU passthrough **OR** run the video pipeline as a standalone
  Windows-host service (see §5 for the trade-off)

---

## 5. GPU integration strategy

The 1050 Ti has 4 GB VRAM and sits in the production server. Two options
for accessing it from the backend:

### Option A: Docker GPU passthrough (one-time setup)

1. Enable GPU support in Docker Desktop settings
2. Ensure NVIDIA Container Toolkit is installed on Windows (comes with Docker Desktop 4.20+)
3. Add `gpus: all` to the `app:` service in `docker-compose.yml`
4. Use a CUDA-enabled base image (`nvidia/cuda:12.2.0-runtime-ubuntu22.04` or similar)

Pro: single Python process, single Dockerfile, single deployment.
Con: larger image, GPU contention if other services in the compose also
use GPU.

### Option B: Standalone host service (recommended for MVP)

1. Run the video generation pipeline as a standalone Python service on
   the Windows host, **not** in Docker
2. Listens on `localhost:9100/generate` (HTTP API, POST with video type)
3. ARQ worker in the Docker container calls
   `http://host.docker.internal:9100/generate`
4. Generated MP4 is written to a shared volume that both sides can read

Pro: zero Docker GPU configuration, trivial NVIDIA driver access on
Windows, keeps the video pipeline isolated from the main API. If SD or
MiDaS crashes its Python process, the API stays up.
Con: one more process to manage (systemd equivalent on Windows = NSSM or
Task Scheduler).

**Recommendation: Option B for Phase 0 and 1, consider A later**. The
extra isolation is worth it and the setup is simpler.

### VRAM budget during active generation

| Step | VRAM peak | Duration | Held concurrently with |
|---|---|---|---|
| MiDaS depth estimation | ~1.5 GB | ~1 s | nothing (exclusive) |
| NVENC encoding | ~200 MB | ~20 s | can overlap with anything |
| matplotlib rendering | 0 (CPU) | ~15 s | runs in parallel |
| Edge TTS + HTTP calls | 0 (cloud) | ~5 s | runs in parallel |

**Peak VRAM at any moment: ~1.5 GB (MiDaS) or ~200 MB (NVENC)**.
Never both at once. We explicitly call `torch.cuda.empty_cache()` and
`del model` between steps. 4 GB is more than enough.

### Fallback chain (when GPU is busy/unavailable)

```
NVENC encode → libx264 CPU encode (slower but always works)
MiDaS depth → plain Ken-Burns pan via ffmpeg (no depth, just scale+translate)
Pollinations unreachable → pre-cached background image rotation
Edge TTS unreachable → Piper TTS local fallback (optional Phase 3)
OpenRouter rate-limited → existing ai_service retry chain already handles this
```

Every step degrades gracefully. At worst, we produce a CPU-only video
with a Ken-Burns still background — still ships, still watchable.

---

## 6. Architecture in the existing codebase

```
app/
├── scheduler/
│   ├── runner.py                         [EDIT] add 3 cron jobs
│   └── video/                            [NEW package]
│       ├── __init__.py
│       ├── video_job.py                  ARQ task entry
│       ├── orchestrator.py               pipeline runner
│       ├── script_generator.py           prompt templates + ai_service wrapper
│       ├── script_validator.py           forbidden-phrase + numeric-grounding
│       ├── tts.py                        edge-tts + word-boundary extraction
│       ├── chart_renderer.py             themed matplotlib → PNG frames
│       ├── background_generator.py       Pollinations fetch + local cache
│       ├── depth_parallax.py             MiDaS + 3D camera synthesis
│       ├── compositor.py                 moviepy assembly + music ducking
│       ├── bumpers.py                    MoviePy-generated intro/outro
│       ├── encoder.py                    ffmpeg NVENC wrapper + CPU fallback
│       ├── thumbnail.py                  custom composition (not ffmpeg extract)
│       ├── templates/
│       │   ├── daily_recap.py            data → script → render
│       │   ├── weekly_roundup.py
│       │   └── explainer.py
│       └── assets/
│           ├── backgrounds/              pre-cached Pollinations images
│           ├── music/                    pre-downloaded Pixabay tracks
│           ├── fonts/                    brand font (from theme)
│           └── logos/                    app icon for bumpers
│
├── queue/
│   ├── tasks.py                          [EDIT] add task_video_{daily,weekly,explainer}
│   └── settings.py                       [EDIT] register in get_arq_functions()
│
├── api/routes/
│   └── videos.py                         [NEW] /videos list + stream + thumbnail
│
├── services/
│   ├── ai_service.py                     [REUSE] call_ai() for script generation
│   └── video_service.py                  [NEW] DB queries + file resolution
│
└── schemas/
    └── video_schema.py                   [NEW] pydantic response models

sql/init.sql                              [EDIT] add video_content table

/app/static/videos/                       Docker volume
├── daily/YYYY-MM-DD.mp4
├── daily/YYYY-MM-DD.jpg
├── weekly/YYYY-WNN.mp4
└── explainer/{slug}.mp4

# Standalone GPU service (Option B — recommended)
~/econatlas-video-service/                Windows host, outside Docker
├── server.py                             FastAPI on localhost:9100
├── requirements.txt                      torch, diffusers, moviepy, etc.
└── run.ps1                               starts as a Windows service
```

Flutter side:
```
lib/
├── data/models/
│   └── video_content.dart                [NEW] VideoContent model
├── data/datasources/
│   └── remote_data_source.dart           [EDIT] getVideos(), getVideoMetadata()
├── presentation/providers/
│   └── video_providers.dart              [NEW] FutureProvider with cache
└── presentation/screens/
    ├── overview/
    │   └── overview_screen.dart          [EDIT] add video card + "See All" link
    └── videos/
        ├── video_feed_screen.dart        [NEW] list + categories
        └── video_player_screen.dart      [NEW] full-screen player
```

---

## 7. Prompt templates (final, locked)

### 7a. Daily recap

```
You are writing a voiceover script for a daily Indian stock market recap.
Target: ~45 seconds of narration at 3.3 words/second = 120-140 words total.

TODAY'S DATA ({date}):
- Nifty 50: closed at {nifty_close}, {nifty_direction} {nifty_change_pct}%
- Sensex: closed at {sensex_close}, {sensex_direction} {sensex_change_pct}%
- Bank Nifty: closed at {banknifty_close}, {banknifty_direction} {banknifty_change_pct}%
- Top gainer: {top_gainer_name} ({top_gainer_symbol}) up {top_gainer_pct}%
- Top loser: {top_loser_name} ({top_loser_symbol}) down {top_loser_pct}%
- Best sector: {best_sector} (+{best_sector_pct}%)
- FII net: {fii_net} cr
- DII net: {dii_net} cr

STRICT RULES:
1. EXACTLY 120–140 words. Count carefully.
2. Use the EXACT numbers above — never round, never invent, never predict.
3. Conversational tone like explaining to a friend over tea, not a news anchor.
4. FORBIDDEN phrases (your script will be auto-rejected if any appear):
   "should buy", "should sell", "should hold", "will go up", "will go down",
   "expect", "likely to", "recommend", "target price", "going to",
   "predicts", "is a good buy", "bullish on", "bearish on".
5. Structure:
   - Line 1: Opening hook (~5 sec)
   - Lines 2-3: Index moves (~15 sec)
   - Lines 4-5: Top gainer + loser + best sector (~15 sec)
   - Line 6: FII/DII flows (~5 sec)
   - Line 7: Close with "Educational only. Not investment advice."
6. Output ONLY the script text. No headers, no markdown, no stage directions.

BEGIN:
```

### 7b. Weekly roundup

```
You are writing a 75-second weekly Indian markets roundup.
Target: 200–240 words @ 3.3 wps.

THIS WEEK ({week_start} → {week_end}):
- Nifty weekly: {nifty_weekly_pct}% ({nifty_start} → {nifty_end})
- Sensex weekly: {sensex_weekly_pct}%
- Bank Nifty weekly: {banknifty_weekly_pct}%
- Best sector: {best_sector} (+{best_sector_pct}%)
- Worst sector: {worst_sector} ({worst_sector_pct}%)
- Top 3 movers: {top_movers_list}
- FII cumulative: {fii_cumulative} cr
- DII cumulative: {dii_cumulative} cr
- Notable events: {events}
- Next week to watch: {upcoming_events}

RULES:
1. 200-240 words exactly.
2. EXACT numbers only — no rounding, inventing, or predicting.
3. Same FORBIDDEN phrases as daily template (see 7a).
4. Structure: opening hook → index summary → sector highlights →
   top movers → flows → what's ahead → disclaimer.
5. End with: "Educational only. Not investment advice."
6. Output ONLY the script.

BEGIN:
```

### 7c. Educational explainer

```
You are writing a 60-second educational explainer for first-time Indian retail investors.
Target: 150–180 words @ 3.0 wps (slower pace for clarity).

TOPIC: {concept}
SHORT DESCRIPTION: {concept_description}
EXAMPLE CONTEXT: {example_data_if_any}

RULES:
1. 150-180 words.
2. Structure:
   - Hook (10 s): "Let's decode {concept} in 60 seconds…"
   - Definition (15 s): one simple sentence, one clarifying sentence.
   - Indian example (20 s): use REAL numbers from Indian stocks (Nifty, Reliance, TCS, HDFC Bank).
   - Practical takeaway (10 s): what the viewer now understands.
   - Disclaimer (5 s): "Educational content only. Not investment advice."
3. No jargon without immediate explanation.
4. FORBIDDEN phrases same as 7a. No "you should do X" language.
5. Output ONLY the script.

BEGIN:
```

### 7d. Grounding + validation (always run post-generation)

```python
# In app/scheduler/video/script_validator.py
FORBIDDEN = [
    "should buy", "should sell", "should hold", "must buy", "must sell",
    "will go up", "will go down", "is going to", "going to rise",
    "going to fall", "expect the", "expected to", "likely to rise",
    "likely to fall", "recommend", "recommended", "target price",
    "price target", "is a good buy", "is a bad buy", "bullish on",
    "bearish on", "predicts", "forecasting", "outperform",
    "underperform", "strong buy", "strong sell", "overweight",
    "underweight", "accumulate", "book profit", "exit at",
]

def validate_script(script: str, allowed_numbers: set[str]) -> ValidationResult:
    low = script.lower()
    # 1. Forbidden phrase check
    hit = next((p for p in FORBIDDEN if p in low), None)
    if hit:
        return ValidationResult(ok=False, reason=f"forbidden phrase: {hit}")
    # 2. Numeric grounding — every number in the script must be one we passed in
    found = set(re.findall(r'(\d+(?:\.\d+)?)', script))
    stray = found - allowed_numbers - {"1", "2", "3", "45", "60", "75", "100"}  # allow counting numbers
    if stray:
        return ValidationResult(ok=False, reason=f"hallucinated numbers: {stray}")
    # 3. Length check
    word_count = len(script.split())
    if not (100 <= word_count <= 250):
        return ValidationResult(ok=False, reason=f"word count {word_count} out of range")
    return ValidationResult(ok=True)
```

On validation failure, regenerate with a stricter reminder appended to
the prompt. Give up after 3 attempts and skip today's video (worse than
publishing a bad one).

---

## 8. DB schema (unchanged from v1)

```sql
CREATE TABLE IF NOT EXISTS video_content (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type           TEXT NOT NULL CHECK (type IN ('daily_recap', 'weekly_roundup', 'explainer')),
    title          TEXT NOT NULL,
    description    TEXT,
    script         TEXT NOT NULL,
    duration_seconds REAL NOT NULL,
    file_path      TEXT NOT NULL,
    thumbnail_path TEXT,
    file_size_bytes BIGINT,
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status         TEXT NOT NULL DEFAULT 'ready'
                    CHECK (status IN ('generating', 'ready', 'failed', 'archived'))
);

CREATE INDEX idx_video_content_type_published
    ON video_content(type, published_at DESC NULLS LAST);
CREATE UNIQUE INDEX idx_video_daily_unique
    ON video_content((generated_at::date))
    WHERE type = 'daily_recap';
```

No `views_count` column — analytics deferred.

---

## 9. Cron schedule — polling pattern (not single-time)

### The FII/DII timing problem

Both daily and weekly videos need FII/DII net figures from
`macro_indicators.fii_net_cash` / `dii_net_cash`. This data arrives
at **irregular times post market close** — sometimes by 17:00 IST,
frequently 18:00–19:00, occasionally as late as 20:00. A single-time
cron at 17:00 would mostly miss it.

`app/scheduler/notification_job.py::_check_fii_dii` already implements
exactly the pattern we need (poll from 17:00 IST, return None if FII or
DII is missing for today, gate with dedup state). We'll reuse the same
gate inside the video job.

### Polling cron — `video_daily`

```python
# Mon-Fri, every 30 minutes from 17:00 to 21:30 IST
_scheduler.add_job(_run_video_daily, "cron",
    day_of_week="mon-fri",
    hour="17-21",          # 17:00, 17:30, 18:00, …, 21:30
    minute="0,30",
    timezone="Asia/Kolkata",
    id="video_daily",
    replace_existing=True, max_instances=1, coalesce=True,
    misfire_grace_time=1800)
```

Task body (`task_video_daily` in `app/queue/tasks.py`):

```python
async def task_video_daily(ctx: dict) -> None:
    """
    Polling task — fires up to 10 times Mon-Fri between 17:00 and 21:30 IST.
    Each firing:
      1. Skip if today's video already exists in DB (idempotent)
      2. Skip if market was closed today (<50 rows in market_prices_intraday)
      3. Skip if FII/DII data for today is missing in macro_indicators
      4. All conditions met → generate the video, insert DB row
      5. Give up at the 21:30 firing if still no FII/DII — generate
         "partial" video without the flows shot and log a warning,
         OR skip entirely (decision in orchestrator)
    """
    from app.core.database import get_pool
    from app.scheduler.video.orchestrator import generate_daily_recap

    pool = await get_pool()
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    today = now_ist.date()

    # 1. Idempotency — already generated today?
    existing = await pool.fetchval(
        "SELECT id FROM video_content "
        "WHERE type = 'daily_recap' AND generated_at::date = $1 "
        "LIMIT 1",
        today,
    )
    if existing:
        logger.debug("video_daily: already generated for %s", today)
        return

    # 2. Holiday gate — was the market open today?
    intraday_rows = await pool.fetchval(
        "SELECT COUNT(*) FROM market_prices_intraday "
        "WHERE timestamp::date = $1",
        today,
    )
    if (intraday_rows or 0) < 50:
        logger.info("video_daily: market closed today (%d rows) — skip", intraday_rows)
        return

    # 3. FII/DII gate — wait for flows to land
    # Reuses the pattern from notification_job._check_fii_dii
    flows = await pool.fetch(
        """
        SELECT DISTINCT ON (indicator_name) indicator_name, value, timestamp
        FROM macro_indicators
        WHERE indicator_name IN ('fii_net_cash', 'dii_net_cash')
          AND unit = 'inr_cr'
        ORDER BY indicator_name, timestamp DESC
        """
    )
    fii = dii = None
    for row in flows:
        row_date = row["timestamp"].astimezone(ZoneInfo("Asia/Kolkata")).date()
        if row_date != today:
            continue  # stale/yesterday's data — not ready yet
        if row["indicator_name"] == "fii_net_cash":
            fii = float(row["value"])
        elif row["indicator_name"] == "dii_net_cash":
            dii = float(row["value"])

    if fii is None or dii is None:
        # Final firing at 21:30 — generate without flows, log warning
        if now_ist.hour >= 21:
            logger.warning(
                "video_daily: FII/DII missing at 21:30 — generating partial video"
            )
            await generate_daily_recap(
                for_date=today, fii_net=None, dii_net=None,
            )
            return
        # Earlier firing — wait for next tick
        logger.debug(
            "video_daily: waiting for FII/DII (have fii=%s, dii=%s)",
            fii, dii,
        )
        return

    # 4. All gates passed — generate
    logger.info(
        "video_daily: generating for %s (fii=%.0f dii=%.0f)",
        today, fii, dii,
    )
    await generate_daily_recap(for_date=today, fii_net=fii, dii_net=dii)
```

**Key properties**:
- **Idempotent**: first successful run inserts a row with unique index
  on `(type, generated_at::date)`. Subsequent firings see the row and
  exit immediately. Safe to fire 10× per day.
- **Self-healing**: if FII/DII arrives at 19:47, the 20:00 firing
  catches it.
- **Graceful degradation**: if FII/DII still missing at 21:30, we
  generate a "partial" video with Shot 4 (flows panel) replaced by a
  shorter filler shot — better than publishing nothing.
- **Holiday-safe**: row-count sentinel on `market_prices_intraday`
  handles NSE holidays without needing a holiday calendar lookup.

### Weekly roundup — same pattern, Friday only

```python
# Friday, every 30 min from 17:30 to 22:00 IST
_scheduler.add_job(_run_video_weekly, "cron",
    day_of_week="fri",
    hour="17-22",
    minute="0,30",
    timezone="Asia/Kolkata",
    id="video_weekly",
    replace_existing=True, max_instances=1, coalesce=True,
    misfire_grace_time=1800)
```

Same gate logic as daily, but aggregates the full week's FII/DII once
Friday's row lands. Idempotent via unique index on `(type, week_iso)`.

### Educational explainer — single-time cron (no data dependency)

```python
# Wednesday 10:00 IST — no market data needed, evergreen content
_scheduler.add_job(_run_video_explainer, "cron",
    day_of_week="wed", hour=10, minute=0,
    timezone="Asia/Kolkata",
    id="video_explainer",
    replace_existing=True, max_instances=1, coalesce=True,
    misfire_grace_time=10800)
```

### Weekly cleanup — unchanged

```python
# Sunday 2 AM IST — delete expired videos
_scheduler.add_job(_run_video_cleanup, "cron",
    day_of_week="sun", hour=2, minute=0,
    timezone="Asia/Kolkata",
    id="video_cleanup",
    replace_existing=True, max_instances=1, coalesce=True,
    misfire_grace_time=43200)
```

### ARQ registration + `_VALID_JOBS`

```python
# app/queue/settings.py
func(task_video_daily,     name="video_daily",     timeout=600),
func(task_video_weekly,    name="video_weekly",    timeout=900),
func(task_video_explainer, name="video_explainer", timeout=600),
func(task_video_cleanup,   name="video_cleanup",   timeout=120),

# app/api/routes/ops.py::_VALID_JOBS
"video_daily", "video_weekly", "video_explainer", "video_cleanup",
```

---

## 10. FastAPI routes (`app/api/routes/videos.py`)

```
GET  /videos
     ?type=daily_recap|weekly_roundup|explainer
     ?limit=20&offset=0
     → list of VideoContent metadata

GET  /videos/{id}
     → full metadata including script text (for transcript view)

GET  /videos/{id}/stream
     → MP4 with HTTP Range support (critical for Flutter video_player seek)
     Sets Cache-Control: public, max-age=86400

GET  /videos/{id}/thumbnail
     → 1080×1920 JPEG
```

Byte-range streaming must be implemented from scratch (no existing
pattern in the codebase). ~50 lines of `StreamingResponse` + header
parsing. This is a minor Phase 1 task.

---

## 11. Flutter integration (locked)

### Overview screen card
```dart
// lib/presentation/screens/overview/overview_screen.dart
// Add below the existing "Market Sentiment" section:

_DailyVideoCard(
  onTap: () => context.push('/videos'),
)
```

The card shows:
- Latest daily recap thumbnail (1080×1920 JPEG, displayed as 16:9 crop)
- Title overlay
- Duration chip
- "See All →" link on the right edge
- Red dot badge if a new video was generated since the user's last
  `prefLastVideoSeenAt` timestamp

### Dedicated feed screen
```dart
// lib/presentation/screens/videos/video_feed_screen.dart
// Route: /videos

Column(
  children: [
    SegmentedControl(['Daily', 'Weekly', 'Learn']),
    Expanded(
      child: ListView.builder(
        itemBuilder: (ctx, i) => VideoCard(
          thumbnail: video.thumbnailUrl,
          title: video.title,
          date: video.publishedAt,
          duration: video.duration,
          onTap: () => context.push('/videos/${video.id}'),
        ),
      ),
    ),
  ],
)
```

### Player screen
```dart
// lib/presentation/screens/videos/video_player_screen.dart
// Route: /videos/:id

Stack(
  children: [
    VideoPlayer(
      controller,
      aspectRatio: 9 / 16,
    ),
    // Top bar: back button, title
    // Bottom: play/pause, scrubber, mute, share
    // Right side: full-screen toggle
  ],
)
```

Provider:
```dart
// lib/presentation/providers/video_providers.dart
final videoFeedProvider = FutureProvider.autoDispose
    .family<List<VideoContent>, String>((ref, type) async {
  // Same caching pattern as broker_charges_providers
  // - SharedPreferences cache key: prefCacheVideos_{type}
  // - Network fallback
  // - Cache-version gate
});
```

---

## 12. Disclaimers & compliance rails

1. **Burned-in footer watermark** on every frame:
   `Educational · Not Investment Advice`
   Non-removable, always at bottom 10% of frame.

2. **Script ends with**: `"Educational only. Not investment advice."`
   — enforced by the validator in §7d. Retry if missing.

3. **Forbidden-phrase filter**: 28 phrases auto-reject the script. List
   in §7d. Can be extended without touching generation code.

4. **Numeric grounding**: every digit in the final script must be a
   number we passed in as prompt data. Stray numbers = validator fail.

5. **Audit trail**: `video_content.metadata` stores:
   - LLM model name
   - Prompt template version
   - Validator pass count
   - Any retry reasons
   - Generation timestamp

6. **Feedback channel**: defer to post-MVP — users can complain via
   existing feedback form (already in app).

---

## 13. Phase 0 concrete task list (4 days)

**Goal**: one working MP4 of "What is P/E ratio?" saved to local disk,
playable in VLC, validates the whole pipeline end-to-end.

### Day 1: scaffolding + script generation
- [ ] Create `app/scheduler/video/` package + all module stubs
- [ ] Create `script_generator.py` with the explainer prompt template
- [ ] Wire to existing `ai_service.call_ai()`
- [ ] Create `script_validator.py` with forbidden-phrase + numeric checks
- [ ] CLI: `python -m app.scheduler.video.orchestrator explainer --topic "P/E ratio"`
- **Exit**: prints a valid 150-180 word script about P/E ratio using Reliance

### Day 2: TTS + backgrounds + charts
- [ ] Create `tts.py` with edge-tts + word-boundary extraction
- [ ] Pre-fetch 10 Pollinations backgrounds (finance theme) into `assets/backgrounds/`
- [ ] Create `chart_renderer.py` with matplotlib themed to #0F1E31/#00E676/#FF5252
- [ ] Render a P/E-specific chart (simple bar: Reliance price ÷ Reliance EPS)
- **Exit**: `out/voice.mp3` + 30 chart PNG frames + background.jpg all exist

### Day 3: composition + GPU pipeline
- [ ] Install `torch`, `torchvision`, `diffusers` on Windows host
- [ ] Set up standalone `~/econatlas-video-service/server.py` FastAPI on localhost:9100
  OR: enable Docker GPU passthrough
- [ ] Implement `depth_parallax.py` with MiDaS DPT-Hybrid
- [ ] Verify `ffmpeg -encoders | grep nvenc` finds `h264_nvenc`
- [ ] Implement `encoder.py` with NVENC + CPU fallback
- [ ] Implement `compositor.py` with layered MoviePy
- [ ] Implement `bumpers.py` with themed intro/outro
- **Exit**: end-to-end pipeline produces `out/pe-ratio.mp4`, 60 s, 1080×1920, NVENC-encoded

### Day 4: polish + thumbnail + test
- [ ] Implement `thumbnail.py` (custom composition, not ffmpeg extract)
- [ ] Implement music ducking in `compositor.py` (Pixabay soft track)
- [ ] Run the full pipeline 5+ times, fix any rough edges
- [ ] Document any gotchas in `VIDEO-CONTENT-PLAN.md` appendix
- **Exit**: producing consistent 60 s videos that look and sound good
  enough to show a user. Sharable with you for review.

**No app integration yet.** Phase 0 is just the offline pipeline.

---

## 13.5. Data fetch backlog — new data we need

The original plan assumed we already had every data source. Design
review turned up two gaps: **corporate earnings calendar** (not
scraped anywhere) and **F&O expiry dates** (computable, no scraper
needed). Both are needed for Weekly Roundup Shot 6 ("Coming Up Next
Week") and for future stock-specific explainers.

### 13.5a. F&O expiry calendar helper (Phase 1, ~50 lines)

**Not a scraper — a calendar rule function.** Indian F&O expiry
schedule follows deterministic rules we can encode once and compute
forever without any external dependency.

```
Monthly expiry:
  Nifty 50, Nifty 100, Bank Nifty, Fin Nifty, Stock F&O
  = LAST THURSDAY of the expiry month
  Unless it's an NSE holiday → previous working day

Weekly expiry:
  Nifty 50            = every Thursday
  Bank Nifty          = every Wednesday (since Dec 2024; was Thursday before)
  Fin Nifty           = every Tuesday
  Nifty Next 50       = every Friday
  (Stock F&O has no weekly expiry — monthly only)
```

**New module**: `app/scheduler/video/fno_expiry_calendar.py`

```python
from datetime import date, timedelta
from typing import Literal, NamedTuple

class FnOExpiry(NamedTuple):
    date: date
    instrument: str           # "NIFTY", "BANKNIFTY", "FINNIFTY", ...
    expiry_type: Literal["weekly", "monthly"]
    description: str          # "Nifty weekly expiry", "Monthly F&O expiry", ...

def get_upcoming_expiries(from_date: date, days_ahead: int = 14) -> list[FnOExpiry]:
    """Return all F&O expiries in the next N days, sorted by date."""
    ...

def next_monthly_expiry(month: int, year: int) -> date:
    """Last Thursday of (month, year), adjusted for NSE holidays."""
    # Start from last day of month, walk back to Thursday
    last_day = date(year, month + 1, 1) - timedelta(days=1) if month < 12 \
               else date(year, 12, 31)
    days_back = (last_day.weekday() - 3) % 7  # 3 = Thursday
    expiry = last_day - timedelta(days=days_back)
    return _adjust_for_nse_holiday(expiry)
```

**NSE holiday list** can be hardcoded per year from the NSE official
calendar (published annually in advance, never changes mid-year). One
Python constant per year. We can reuse `app/scheduler/trading_calendar.py`'s
holiday list if it exists.

Usage in the weekly roundup Shot 6:
```python
from app.scheduler.video.fno_expiry_calendar import get_upcoming_expiries

next_week_monday = today + timedelta(days=(7 - today.weekday()))
expiries = get_upcoming_expiries(from_date=next_week_monday, days_ahead=5)
# → [FnOExpiry(date=2026-04-15, instrument='BANKNIFTY', ...), ...]
```

**Effort**: 1 day. Can be built in Phase 1, not Phase 0.

---

### 13.5b. Corporate earnings calendar scraper (Phase 3, ~3–4 days)

**Deferred from MVP** — Weekly Shot 6 will show only macro + IPO + F&O
expiry for now. Corporate earnings is the richest signal for a weekly
roundup, but:

1. It's not a 1-day task (scraper + table + cron + integration)
2. The data is available from other sources (Moneycontrol, Screener)
   with meaningfully different schemas
3. We can ship MVP without it and add it in Phase 3

When we build it:

**Sources to evaluate (ranked by free-tier reliability)**:

1. **NSE corporate announcements RSS**
   `https://www.nseindia.com/api/corporate-announcements?index=equities`
   Pros: official, free. Cons: NSE API is hostile to scraping, needs
   browser-like headers + cookie priming. High maintenance.

2. **Moneycontrol earnings calendar**
   `https://www.moneycontrol.com/stocks/earnings-calendar/`
   Pros: clean HTML table, stable URL structure. Cons: needs HTML
   parsing, no API.

3. **Screener.in** (has an API that powers its screens)
   Pros: Python-friendly, structured JSON in their `/api/` endpoints.
   Cons: unofficial, TOS gray area.

4. **Yahoo Finance** `finance.yahoo.com/calendar/earnings`
   Pros: works via `yfinance` Python package. Cons: Indian coverage
   is patchy for mid-caps.

**Recommended**: Moneycontrol + Yahoo as fallback. Moneycontrol has the
best India coverage and a scraper-friendly HTML structure.

**New schema**:
```sql
CREATE TABLE IF NOT EXISTS corporate_earnings_calendar (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol       TEXT NOT NULL,
    company_name TEXT NOT NULL,
    event_date   DATE NOT NULL,
    event_type   TEXT NOT NULL,    -- 'quarterly_results', 'agm', 'dividend', 'bonus', 'split'
    fiscal_period TEXT,            -- 'Q4 FY25', 'Q1 FY26', etc.
    time_slot    TEXT,             -- 'before_market', 'after_market', 'during_market', NULL
    estimated_eps DOUBLE PRECISION,
    consensus_eps DOUBLE PRECISION,
    source       TEXT NOT NULL,
    scraped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, event_date, event_type)
);
CREATE INDEX idx_earnings_calendar_date
    ON corporate_earnings_calendar(event_date);
```

**New cron**: weekly Sunday 3 AM IST (scrapes next 2 weeks of upcoming
earnings). ARQ task `task_earnings_calendar`, retry policy `(2, 60)`.

**Integration point**: Weekly Shot 6 would use this to show earnings
for nifty constituents + any large-cap names with results in the
next 7 days.

**Blocking for video MVP?**: No. Shot 6 works fine with macro + IPO +
F&O expiry only.

---

### 13.5c. Summary — what Phase 0 vs Phase 1 vs Phase 3 needs

| Data | Needed for | Status | Action |
|---|---|---|---|
| Nifty / Sensex / Bank Nifty intraday | Daily Shot 2 | ✅ `market_prices_intraday` | None |
| Top gainer/loser + sectors | Daily Shot 3 | ✅ `discover_stock_snapshots` | None |
| FII/DII net flows | Daily Shot 4, Weekly Shot 5 | ✅ `macro_indicators` | **Polling cron in §9** |
| Weekly index series | Weekly Shot 2 | ✅ `market_prices` | None |
| Sector weekly performance | Weekly Shot 3 | ✅ `discover_stock_snapshots` aggregation | None |
| Macro events next week | Weekly Shot 6 | ✅ `economic_calendar` via `macro_service.get_upcoming_events()` | None |
| IPO calendar next week | Weekly Shot 6 | ✅ `ipo_snapshots` WHERE status='upcoming' | None |
| F&O expiry next week | Weekly Shot 6 | ⚙️ **Computable, not scraped** | **Build in Phase 1** |
| Corporate earnings next week | Weekly Shot 6 (enriched) | ❌ Not scraped | **Defer to Phase 3** |
| Reliance price + EPS | Explainer Shot 3 (P/E demo) | ✅ `discover_stock_snapshots` has fundamental fields | None |

Everything MVP-critical is ready **except** the F&O expiry helper,
which is a ~1-day build and fits cleanly into Phase 1.

---

## 14. Phases beyond 0

### Phase 1 — backend MVP (4–5 days)
- `video_content` DB migration
- ARQ tasks + cron schedule
- `/videos` routes with byte-range streaming
- Cleanup cron
- Grafana panels for video generation metrics
- **Exit**: cron fires, video is produced, `/videos` API returns it, `curl /videos/{id}/stream | vlc -` plays it

### Phase 2 — Flutter MVP (3–4 days)
- `VideoContent` model + provider + caching
- Overview screen card
- `/videos` feed screen with segmented control
- `/videos/:id` player screen
- "New video" badge on Overview card
- **Exit**: user opens app, sees today's video card on Overview, taps, watches

### Phase 3 — polish (1–2 weeks, ongoing)
- Prompt engineering refinement (watch 20 videos, iterate)
- Bumper motion design improvements
- Music library expansion
- Chart motion design upgrades
- Error state handling (no video today, offline, etc.)

### Phase 4 — growth (later)
- Push notifications
- Analytics (view count, completion rate)
- Sharing
- Hindi language track
- Cross-posting to YouTube Shorts

---

## 15. Risk register

| Risk | Mitigation |
|---|---|
| LLM hallucinates a stock price | Strict numeric validator + retry chain + data-grounded template |
| OpenRouter free tier shrinks | Existing `ai_service` has fallback chain across multiple free models |
| Edge TTS blocked by Microsoft | Piper TTS local fallback in Phase 3 |
| Pollinations.ai down | Pre-cached 20 backgrounds, rotation |
| SD 1.5 OOM on 1050 Ti (Phase 4) | Currently not used — MiDaS + NVENC only, both tested at ~2 GB peak |
| MiDaS OOM mid-generation | `torch.cuda.empty_cache()` between steps, CPU fallback (Ken Burns) |
| NVENC unavailable on host | ffmpeg falls back to libx264 (slower but works) |
| SEBI complaint about content | Burned disclaimer + forbidden-phrase filter + audit trail + no forward advice |
| Disk fills with videos | 30 d / 12 w / forever retention cron + Prometheus gauge alert |
| Video generation slows API traffic | Off-hours, short duration (~90 s), GPU isolation via standalone host service |
| Production server restart during generation | Idempotent writes: regenerate today's video on next cron if DB row missing |
| **FII/DII data arrives unusually late** (after 21:30) | Polling cron fires 10× between 17:00–21:30; at the final firing, generate a "partial" video that replaces Shot 4 with a filler shot. Rare — most days FII/DII lands 17:00–19:00. |
| **FII/DII data never arrives** (source outage) | Partial video publishes at 21:30 without the flows panel + logs warning to Grafana. Viewers see a slightly shorter video, no error state. |
| **F&O expiry calendar drift** (e.g. new weekly instrument introduced) | Calendar rules hardcoded in `fno_expiry_calendar.py`. NSE announces changes months in advance (e.g. Bank Nifty move to Wed in Dec 2024). Review rules quarterly. |
| **Misleading "LIVE" badge** | Fixed in design review — status bar now says `● NSE CLOSED · 15:30 IST` with a static red dot, no pulse. The video is explicitly a post-close recap. |
| **Weekly Shot 6 missing data** (e.g. no IPOs, no macro events, no F&O that week) | Shot gracefully degrades — shows 1 or 2 cards instead of 3, or a single "Quiet week ahead" placeholder card if zero events. |

---

## 16. Educational explainer backlog (52 topics, 1 year)

**Basics** (8)
1. What is P/E ratio?  ← *Phase 0 test topic*
2. What is NAV?
3. Nominal vs real returns
4. Simple vs compound interest
5. Bid-ask spread
6. Market cap (large / mid / small)
7. What counts as blue chip?
8. Index vs active mutual fund

**Taxes** (5)
9. STT explained
10. LTCG vs STCG
11. Dividend tax
12. ₹1 lakh LTCG exemption
13. Section 80C in 60 seconds

**F&O** (5)
14. Lot size — why it matters
15. Strike price in one minute
16. Call vs Put — simplest explanation
17. Premium vs intrinsic value
18. Why F&O is NOT for beginners

**IPOs** (5)
19. What happens in an IPO
20. Anchor investors
21. Grey market premium
22. How allotment actually works
23. Mainboard vs SME IPOs

**Macro** (5)
24. What is CPI?
25. Repo rate explained
26. Why RBI meetings matter
27. FII vs DII flows
28. Fiscal deficit in one minute

**Products** (5)
29. SIP vs lump sum
30. ELSS tax saving
31. Gilt funds
32. Gold ETFs vs SGBs
33. NPS vs EPF vs PPF

**Risk** (5)
34. Standard deviation as risk
35. Beta explained
36. Sharpe ratio in 60 seconds
37. Why diversification actually works
38. Rebalancing — the boring secret

**Valuation** (4)
39. P/B ratio
40. ROE and why it's not everything
41. Debt-to-equity
42. Dividend yield

**Brokerage & charges** (5)
43. What 0.03% brokerage actually costs
44. DP charges explained
45. BSDA vs regular demat
46. When minimum brokerage kicks in
47. Clearing vs execution costs

**Behavioral** (3)
48. Why cost averaging feels wrong but works
49. The sunk cost fallacy in stocks
50. Anchoring bias — the ₹100 stock trap

**Derivatives advanced** (2)
51. Open interest
52. Max pain theory

---

## 17. What's locked, what's not

**Locked** (24 items in §1) — ready to build.

**Deferred to Phase 3+ (not needed for MVP)**:
- Hindi language track
- Push notifications
- Analytics / view counting
- Share functionality
- Offline download
- Transcript view
- Rating / feedback on individual videos
- Cross-posting to social platforms

**Will decide during Phase 0 as we go** (minor operational details):
- Exact Pollinations.ai prompts for background images
- Specific Pixabay tracks to download
- Exact matplotlib color/font tuning
- MiDaS parallax intensity knobs

None of the deferred items blocks Phase 0.

---

## 18. Go signal

When you say go, I start Day 1 of Phase 0:

1. Create the `app/scheduler/video/` package
2. Write `script_generator.py` + explainer prompt for P/E ratio
3. Wire to `ai_service.call_ai()`
4. Write `script_validator.py`
5. Produce the first valid script and show it to you

Four days later you'll have a playable MP4 of the P/E ratio video. No
Flutter work yet, no app integration, just the raw pipeline proof.

Review that video. If the quality is acceptable → Phase 1 + 2 (~2 more
weeks). If the quality needs more work → we iterate on the prompt /
chart / bumpers before moving on.
