# Daily AI-generated video content — implementation plan

Status: **design draft, not yet implemented**
Target: **MVP free-forever stack**, hybrid visual approach (AI background + real data overlay)
Cadence: **daily market recap + weekly roundup + weekly educational explainer**

---

## 1. What we're building

Short-form (45–90 second) vertical videos auto-generated from EconAtlas
market data, served through the app as a "Daily Insights" feed.
Three content types running on independent cron schedules.

Every video follows the same visual template for MVP — brandable,
templated, cheap to produce:

```
┌─────────────────────────────────┐   ← 1080 × 1920 (9:16 vertical)
│   [AI-generated background]      │      Ken-Burns slow pan (30s loop)
│                                  │
│  ┌──────────────────────────┐   │
│  │    TITLE                 │   │   ← 80px, bold, brand color
│  │    Date · Category chip  │   │
│  └──────────────────────────┘   │
│                                  │
│     ┌─────────────────────┐     │
│     │                     │     │   ← real matplotlib chart
│     │   Animated chart    │     │      (progressive draw)
│     │   (Nifty today)     │     │
│     └─────────────────────┘     │
│                                  │
│    "Nifty closed at 22,450..."  │   ← live caption, word-by-word
│                                  │
│  ┌──────────────────────────┐   │
│  │  Not investment advice   │   │   ← always visible footer
│  │  Educational content      │   │
│  └──────────────────────────┘   │
└─────────────────────────────────┘
```

Total duration: 45s (daily), 75s (weekly roundup), 60s (explainer).

---

## 2. Content types

### 2a. Daily Market Recap
- **Fires**: Mon–Fri 17:00 IST (90 min after market close at 15:30)
- **Duration**: 45 s (≈ 130 words of narration)
- **Structure**:
  1. Hook (5 s): *"Indian markets today — here's the 45-second recap."*
  2. Indices (15 s): Nifty, Sensex, Bank Nifty with percent moves
  3. Highlights (15 s): top gainer, top loser, top sector
  4. Flows (5 s): FII/DII net
  5. Close + disclaimer (5 s)
- **Chart**: animated Nifty intraday line, progressive draw
- **Data source**: `market_prices_intraday`, `market_prices`, `discover_stock_snapshots`, `institutional_flows_overview`

### 2b. Weekly Roundup
- **Fires**: Friday 17:30 IST (after Friday's daily recap)
- **Duration**: 75 s (≈ 220 words)
- **Structure**:
  1. Hook (5 s): *"This week in Indian markets…"*
  2. Weekly indices (15 s)
  3. Best & worst sectors (15 s)
  4. Notable individual stocks (20 s)
  5. FII/DII cumulative for the week (10 s)
  6. Events ahead next week (10 s)
  7. Disclaimer (5 s)
- **Chart**: animated weekly Nifty line with annotations on daily highs/lows
- **Data source**: same tables, aggregated over 5 trading days

### 2c. Educational Explainer
- **Fires**: Wednesday 10:00 IST (weekly, any day of the week)
- **Duration**: 60 s (≈ 170 words)
- **Topic pool**: pre-seeded list of 52 concepts (one year of weekly rotation)
  - "What is P/E ratio?"
  - "NAV explained"
  - "How does STT work?"
  - "Index vs active fund"
  - "Lot size in F&O"
  - … (complete list at end of doc)
- **Structure**:
  1. Hook: "Let's decode [concept] in 60 seconds"
  2. Simple definition
  3. Concrete Indian example
  4. Practical takeaway
- **Chart**: concept-specific illustration (optional for MVP — can use only text + background)
- **Data source**: none (evergreen content, script-only)

---

## 3. The free stack

Every component has been picked for "no API key needed OR generous free tier,
runs on existing Windows 11 + Docker Desktop + CPU-only setup".

| Stage | Tool | Free tier | Fallback |
|---|---|---|---|
| **Script generation** | Gemini 2.0 Flash | 1500 req/day, 1M tokens/day | Ollama local (llama3.2:3b) |
| **Text-to-speech** | `edge-tts` (Microsoft Edge voices) | Unlimited, no key | Piper TTS (local, CPU) |
| **Background images** | Pollinations.ai | Unlimited, no key | Pre-baked asset library |
| **Background music** | Pixabay Music API | Free, no attribution required | Silent (MVP skip) |
| **Chart rendering** | `matplotlib` → PNG frames | Local Python | — |
| **Video composition** | `moviepy` (wraps ffmpeg) | Local, no limits | Raw ffmpeg scripts |
| **Caption timing** | Edge TTS word boundaries | Provided by edge-tts | `faster-whisper` local transcription |
| **Storage** | Local disk `/app/static/videos` | Free | Cloudflare R2 (10 GB free tier) |
| **Serving** | Existing FastAPI | Already running | — |
| **Playback** | Flutter `video_player` package | Free plugin | — |

Dependencies to add to `requirements.txt`:
```
edge-tts>=6.1
moviepy>=1.0
google-generativeai>=0.8   # Gemini Flash
```

---

## 4. Architecture — how it fits into the existing backend

```
app/
├── scheduler/
│   ├── runner.py                       ← add 3 new cron jobs here
│   └── video/                          ← NEW package
│       ├── __init__.py
│       ├── video_job.py                ← ARQ task entry point
│       ├── orchestrator.py             ← end-to-end pipeline
│       ├── script_generator.py         ← Gemini Flash wrapper
│       ├── tts.py                      ← edge-tts wrapper
│       ├── chart_renderer.py           ← matplotlib → PNG frames
│       ├── background_generator.py     ← Pollinations.ai wrapper + cache
│       ├── compositor.py               ← moviepy assembly
│       ├── templates/
│       │   ├── base.py                 ← shared visual template
│       │   ├── daily_recap.py          ← daily flow (data → script → render)
│       │   ├── weekly_roundup.py       ← weekly flow
│       │   └── explainer.py            ← educational flow
│       └── assets/
│           ├── backgrounds/            ← pre-baked AI backgrounds (cache)
│           ├── music/                  ← pre-downloaded Pixabay tracks
│           ├── fonts/                  ← brand font TTF
│           └── logos/                  ← app logo, watermark
│
├── queue/
│   ├── tasks.py                        ← add task_video_generate
│   └── settings.py                     ← register in get_arq_functions()
│
├── api/routes/
│   └── videos.py                       ← NEW: GET /videos, stream, thumbnail
│
├── services/
│   └── video_service.py                ← NEW: DB queries, file resolution
│
└── schemas/
    └── video_schema.py                 ← NEW: pydantic response models

sql/init.sql                            ← add video_content table

/app/static/videos/                     ← mounted volume
    ├── daily/
    │   ├── 2026-04-14.mp4
    │   ├── 2026-04-14.jpg              ← thumbnail (first frame)
    │   └── 2026-04-14.json             ← metadata (script, word timings)
    ├── weekly/
    │   └── 2026-W15.mp4
    └── explainer/
        └── pe-ratio.mp4
```

---

## 5. Data flow — one video, end to end

**Example: daily recap on 14 Apr 2026**

```
1. Cron fires at 17:00 IST
     app/scheduler/runner.py → _run_video_daily()
     → queues ARQ job `video_daily`

2. ARQ worker picks it up
     app/queue/tasks.py::task_video_daily
     → app/scheduler/video/orchestrator.py::generate("daily_recap")

3. Collect data (from existing Postgres tables)
     SELECT * FROM market_prices_intraday WHERE date = today AND asset IN (Nifty, Sensex, BankNifty)
     SELECT * FROM discover_stock_snapshots ORDER BY percent_change DESC LIMIT 1   # top gainer
     SELECT * FROM discover_stock_snapshots ORDER BY percent_change ASC LIMIT 1    # top loser
     SELECT * FROM institutional_flows_overview WHERE date = today                 # FII/DII

4. Generate script (Gemini Flash)
     Prompt template (see §7) + data dict
     → 120-140 word script

5. Generate TTS audio (edge-tts)
     voice = "en-IN-NeerjaNeural"
     → out.mp3 + word-boundary timestamps

6. Pick background
     check app/scheduler/video/assets/backgrounds/
     if fewer than 10 cached: generate one via Pollinations.ai
     pick random (or hash of date for determinism)
     → 1080x1920 JPEG

7. Render chart frames (matplotlib)
     read today's Nifty intraday prices
     for t in range(30):  # 30 frames × 33ms = 1 second of animation
         plot progressive slice
         savefig(f"frame_{t:03d}.png", transparent=True)
     → 30 PNG frames

8. Composite (moviepy)
     background = ImageClip(bg).set_duration(45).resize((1080, 1920)).set_fps(30)
     chart_clip = ImageSequenceClip(frames).set_start(10).set_duration(20).set_position("center")
     title = TextClip("Today's Market Recap", ...)
     caption = word-by-word text overlay from edge-tts word boundaries
     footer = TextClip("Not investment advice · Educational content", ...)
     logo = ImageClip("logo.png").set_position((50, 50))
     audio = AudioFileClip("out.mp3")
     final = CompositeVideoClip([background, chart_clip, title, caption, footer, logo]).set_audio(audio)
     final.write_videofile("2026-04-14.mp4", codec="libx264", fps=30, audio_codec="aac")

9. Write thumbnail
     ffmpeg -i 2026-04-14.mp4 -vf "select=eq(n\,30)" -frames:v 1 2026-04-14.jpg

10. Insert DB row
     INSERT INTO video_content (
         type, title, description, script, duration_seconds,
         file_path, thumbnail_path, generated_at, published_at, metadata
     ) VALUES (
         'daily_recap', 'Market Recap · 14 Apr 2026', '...',
         <script>, 45.2,
         '/videos/daily/2026-04-14.mp4',
         '/videos/daily/2026-04-14.jpg',
         NOW(), NOW(),
         '{"voice": "en-IN-NeerjaNeural", "template": "v1"}'::jsonb
     );

11. Invalidate Flutter cache (optional)
     Redis pub/sub → push notification to watching clients
```

Expected total wall-time on the Windows 11 host: **~60 seconds per video**,
CPU-only, no GPU required.

---

## 6. Database schema

```sql
CREATE TABLE IF NOT EXISTS video_content (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type           TEXT NOT NULL CHECK (type IN ('daily_recap', 'weekly_roundup', 'explainer')),
    title          TEXT NOT NULL,
    description    TEXT,
    script         TEXT NOT NULL,
    duration_seconds REAL NOT NULL,
    file_path      TEXT NOT NULL,       -- /videos/daily/2026-04-14.mp4
    thumbnail_path TEXT,                -- /videos/daily/2026-04-14.jpg
    file_size_bytes BIGINT,
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,
    views_count    INTEGER NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'ready'
                    CHECK (status IN ('generating', 'ready', 'failed', 'archived'))
);

CREATE INDEX idx_video_content_type_published
    ON video_content(type, published_at DESC NULLS LAST);

CREATE INDEX idx_video_content_generated
    ON video_content(generated_at DESC);

-- One daily recap per day (retry-safe idempotency)
CREATE UNIQUE INDEX idx_video_daily_unique
    ON video_content((generated_at::date))
    WHERE type = 'daily_recap';
```

**Retention policy**: keep last 30 days of daily, 12 weeks of weekly,
unlimited explainers (they're evergreen). Weekly cleanup cron.

---

## 7. Prompt templates (Gemini Flash)

### Daily recap

```
You are writing a voiceover script for a daily Indian stock market recap.

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
1. EXACTLY 120–140 words (≈45 seconds at 3.3 wps narration).
2. Use the EXACT numbers above — never round, never invent, never predict.
3. Conversational tone like a friend explaining over tea, not a news anchor.
4. NO phrases like "should buy", "will go up", "expect", "likely to",
   "recommend", "target price", "should hold" — do not give advice.
5. Open with a 5-second hook, end with: "Educational only. Not investment advice."
6. Output ONLY the script text. No headers, no markdown, no stage directions.

BEGIN:
```

### Weekly roundup

```
You are writing a 75-second weekly roundup voiceover for Indian markets.

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

STRICT RULES:
1. 200–240 words (≈75 seconds at 3.3 wps).
2. Structure: opening hook → index summary → sector highlights →
   top movers → flows → what's ahead → disclaimer.
3. Use EXACT numbers; no estimations.
4. NO buy/sell/hold/target/predict language.
5. End with: "Educational only. Not investment advice."
6. Output ONLY the script.

BEGIN:
```

### Educational explainer

```
You are writing a 60-second educational explainer for retail investors in India.

TOPIC: {concept}
SHORT DESCRIPTION: {concept_description}
AUDIENCE: First-time investors, beginner level.

STRICT RULES:
1. 150–180 words (≈60 seconds at 3.0 wps for clearer pacing).
2. Structure:
   - Hook (10 s): "Let's decode {concept} in 60 seconds…"
   - Definition (15 s): one simple sentence, then one clarifying sentence.
   - Indian example (20 s): concrete, relatable — use everyday rupee amounts,
     familiar names (Nifty, Reliance, HDFC Bank).
   - Practical takeaway (10 s): what the viewer should now understand.
   - Disclaimer (5 s): "Educational content only. Not investment advice."
3. NO jargon without immediate explanation.
4. NO specific "you should" or "everyone must" advice.
5. Output ONLY the script text.

BEGIN:
```

### Grounding discipline (critical)

**Never let the LLM emit numbers.** The script template in code will
`.format()` the actual DB values INTO the generated text, then validate
that the final script contains only numbers we passed in. If the LLM
hallucinates a number, we detect it and retry the generation with a
stricter reminder.

```python
def _validate_script(script: str, allowed_numbers: set[str]) -> bool:
    found = set(re.findall(r'\d+(?:\.\d+)?', script))
    stray = found - allowed_numbers
    return len(stray) == 0
```

---

## 8. Cron schedule (additions to `app/scheduler/runner.py`)

```python
# Daily market recap — 17:00 IST Mon-Fri (90 min after 15:30 close)
_scheduler.add_job(
    _run_video_daily,
    "cron",
    day_of_week="mon-fri",
    hour=17,
    minute=0,
    timezone="Asia/Kolkata",
    id="video_daily",
    replace_existing=True,
    max_instances=1,
    coalesce=True,
    misfire_grace_time=3600,
)

# Weekly roundup — 17:30 IST Friday
_scheduler.add_job(
    _run_video_weekly,
    "cron",
    day_of_week="fri",
    hour=17,
    minute=30,
    timezone="Asia/Kolkata",
    id="video_weekly",
    replace_existing=True,
    max_instances=1,
    coalesce=True,
    misfire_grace_time=3600,
)

# Educational explainer — 10:00 IST Wednesday
_scheduler.add_job(
    _run_video_explainer,
    "cron",
    day_of_week="wed",
    hour=10,
    minute=0,
    timezone="Asia/Kolkata",
    id="video_explainer",
    replace_existing=True,
    max_instances=1,
    coalesce=True,
    misfire_grace_time=10800,
)
```

ARQ task registration in `app/queue/settings.py::get_arq_functions()`:
```python
func(task_video_daily,     name="video_daily",     timeout=600),
func(task_video_weekly,    name="video_weekly",    timeout=900),
func(task_video_explainer, name="video_explainer", timeout=600),
```

Add `"video_daily"`, `"video_weekly"`, `"video_explainer"` to
`_VALID_JOBS` in `app/api/routes/ops.py` so they're manually triggerable.

---

## 9. FastAPI routes (`app/api/routes/videos.py`)

```
GET  /videos
     ?type=daily_recap|weekly_roundup|explainer
     ?limit=20&offset=0
     → list of video metadata (id, title, thumbnail, duration, published_at)

GET  /videos/{id}
     → full metadata including script text for transcripts

GET  /videos/{id}/stream
     → streams the MP4 with HTTP Range support (critical for mobile seek)
     Sets Cache-Control: public, max-age=86400 (1 day)

GET  /videos/{id}/thumbnail
     → 1080x1920 JPEG first-frame

POST /videos/{id}/view
     → increment views_count (fire-and-forget, no auth)
```

Range support is non-trivial — use `StreamingResponse` with byte-range
parsing. Existing code in `app/api/routes/` does not have this pattern
so it'll need to be added from scratch. ~50 lines.

---

## 10. Flutter integration

New screen: `lib/presentation/screens/insights/insights_screen.dart`

Route: `/insights` added to bottom tab bar (or under the existing
Dashboard/Discover/Artha structure — TBD).

Widget tree:
```
InsightsScreen (ConsumerStatefulWidget)
├── SegmentedControl (Daily | Weekly | Learn)
├── VerticalVideoFeed
│   └── PageView.builder (one video per page, vertical swipe)
│       └── VideoCard
│           ├── BackgroundVideoPlayer (video_player plugin)
│           ├── OverlayMetadata (title, date, description)
│           ├── MuteButton
│           └── ShareButton
└── NoConnectionBanner (when offline, show cached thumbnails)
```

Model: `VideoContent` class with `fromJson`/`toJson`, cached in
SharedPreferences same pattern as `broker_charges` provider.

Provider: `videoFeedProvider.family<List<VideoContent>, VideoType>`
that fetches and caches by type.

Plugin: `video_player: ^2.9` (official) — supports HLS and MP4,
handles range requests automatically.

**Offline mode**: MP4s are small (~5 MB each). Download the 3 latest
per type on Wi-Fi, play from local cache. Optional for MVP.

**Autoplay rules**:
- On tab open: autoplay first video, muted by default
- Tap to unmute
- Swipe to next video = autoplay next
- Swipe away / leave screen = pause

---

## 11. Disclaimers & compliance

Because this is financial content served to Indian users, SEBI has opinions.
**Do all of these**:

1. **Watermark every frame** with "Educational · Not Investment Advice"
   in a fixed bottom-left overlay that cannot be cropped out.
2. **Every script must end** with the same disclaimer sentence —
   enforced by the prompt AND validated before the video is rendered.
3. **No stock-specific recommendations** — the daily recap can say
   "TCS was today's top gainer" but NOT "TCS is a good buy."
   The LLM prompt explicitly forbids this; a regex validator catches
   forbidden phrases before TTS runs:
   ```
   forbidden_phrases = [
       "should buy", "should sell", "should hold",
       "will go up", "will go down", "expect",
       "target price", "recommend", "buy now",
       "likely to", "going to", "predicts",
   ]
   ```
4. **Video metadata** stored in `video_content.metadata` includes the
   LLM model + version + prompt template version so we can audit
   what generated each clip if a complaint arrives.
5. **Appeal channel** — in-app "Report content" button on every video
   that creates a row in a new `video_reports` table.
6. **Lawyer review recommended** before first public release.
   This is cheap insurance — a 1-hour consultation with an Indian
   securities lawyer before launch.

---

## 12. Phased rollout

### Phase 0 — foundations (3–4 days)

Goal: the pipeline runs end-to-end and produces one working video
file to your desktop. No app integration yet.

- `app/scheduler/video/` package scaffolded
- `script_generator.py` with Gemini Flash
- `tts.py` with edge-tts
- `chart_renderer.py` producing 30 chart frames
- `compositor.py` producing final MP4
- One manual trigger: `python -m app.scheduler.video.orchestrator daily_recap`
- Output: `./out/test.mp4` playable in VLC

**Exit criterion**: you can run one command and get a valid MP4.

### Phase 1 — backend MVP (4–5 days)

- `video_content` DB table (migration)
- ARQ task + cron schedule
- FastAPI routes (`/videos`, `/videos/{id}/stream`, `/videos/{id}/thumbnail`)
- Byte-range streaming
- File layout on disk
- Retention cron (delete >30 day old daily, >12 week old weekly)

**Exit criterion**: cron fires, video gets stored, `curl /videos` returns
the new clip, `curl /videos/{id}/stream | vlc -` plays it.

### Phase 2 — Flutter MVP (3–4 days)

- `VideoContent` model
- `videoFeedProvider`
- `InsightsScreen` with `PageView.builder` + `video_player`
- Entry point (new bottom tab or item in existing tab)
- SharedPreferences caching

**Exit criterion**: user opens app, swipes to Insights tab, sees today's
recap video, taps play, hears narration with captions.

### Phase 3 — polish & iterate (1–2 weeks, ongoing)

- Better prompt engineering (watch 5 videos, refine)
- Better chart motion design (easing, annotations)
- Background music selection + ducking under voice
- Multiple background image themes (morning / evening / weekend)
- View count + basic analytics
- In-app share button (pre-generated share link to MP4)

### Phase 4 — education content library (1 week upfront)

- Author the 52-topic list for explainers
- Pre-generate the first 4 explainers
- Review quality before enabling the weekly cron

**Total effort: ~3 weeks of focused work for a shippable v1.**

---

## 13. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Gemini free tier shrinks | Medium | High | Ollama local fallback ready |
| edge-tts gets blocked by Microsoft | Low | High | Piper TTS local fallback |
| Pollinations.ai goes down | Medium | Low | Pre-bake 30 backgrounds, rotate |
| LLM hallucinates a number | **High** | **Critical** | Strict numeric validator (§7), retry on fail |
| Users hear "robotic" voice | High | Medium | Multiple voices per video type, better prompt pacing |
| SEBI complaint | Low | **Critical** | Disclaimer watermark + phrase filter + lawyer review |
| Disk fills up | Medium | Medium | Retention cron, size monitoring via Grafana |
| Video generation runs during high-load cron | Low | Low | Stagger: video at 17:00, broker at 04:00, etc. |
| ffmpeg encoding CPU-spikes | Medium | Medium | nice/ionice in Docker container, profile first |

---

## 14. Open questions

1. **Background music?** Pixabay has good royalty-free tracks but
   adds complexity. MVP: no music (voice only), add in Phase 3.

2. **Multiple voices?** edge-tts has ~8 Indian English voices.
   One-voice-per-content-type is simpler but less interesting.
   MVP: fixed voice `en-IN-NeerjaNeural` for all.

3. **Where does "Insights" live in the app nav?**
   - Option A: new bottom tab (requires nav restructure)
   - Option B: card on Dashboard screen
   - Option C: under Artha AI section (conceptually fits)
   - Recommendation: **B for MVP** (1 card on Dashboard), promote to
     tab in Phase 4 if engagement is there

4. **Language?** MVP is English only. Hindi (TTS via
   `hi-IN-SwaraNeural`) is an obvious Phase-5 addition.

5. **Thumbnails**: first frame of MP4 is usually ugly (background only,
   no chart yet). Better to capture the 10th-second frame when the
   chart is mid-draw. `ffmpeg -ss 00:00:10 -frames:v 1`.

6. **Pre-generation vs on-demand**: pre-generate at 17:00 and serve
   static files (MVP approach) vs generate per-request (wasteful). MVP
   pre-generates.

7. **What happens on market holidays?** Cron doesn't know about NSE
   holidays. Skip if today's `market_prices_intraday` has <50 rows
   (a quick sentinel for "markets were closed").

8. **Can this be triggered ad-hoc via `/ops/jobs/trigger/video_daily`?**
   Yes — same mechanism as other ARQ tasks.

---

## 15. Educational topic backlog (52 topics for weekly cron)

Basics:
- What is P/E ratio?
- What is NAV?
- Nominal vs real returns
- Simple vs compound interest
- Bid-ask spread
- Market cap explained (large / mid / small)
- Blue chip stocks — what actually counts?
- Index vs active mutual fund

Taxes:
- STT explained in 60 seconds
- LTCG vs STCG
- How dividend tax works
- The ₹1 lakh exemption
- Section 80C in 60 seconds

F&O:
- Lot size — why does it matter?
- Strike price in one minute
- Call vs Put — the simplest explanation
- Premium vs intrinsic value
- Why F&O is NOT for beginners

IPOs:
- What happens in an IPO
- Anchor investors
- Grey market premium
- How allotment actually works
- Mainboard vs SME IPOs

Macro:
- What is CPI?
- Repo rate explained
- Why RBI meetings matter
- FII vs DII flows
- Fiscal deficit — one minute explainer

Products:
- SIP vs lump sum
- ELSS tax saving
- Gilt funds
- Gold ETFs vs SGBs
- NPS vs EPF vs PPF

Risk:
- Standard deviation as risk
- Beta explained
- Sharpe ratio in 60 seconds
- Why diversification actually works
- Rebalancing — the boring secret

Valuation:
- P/B ratio
- ROE and why it's not everything
- Debt-to-equity
- Dividend yield

Brokerage & charges:
- What does 0.03% brokerage actually cost you
- DP charges explained
- BSDA vs regular demat
- When minimum brokerage kicks in
- Clearing vs execution costs

Behavioral:
- Why cost averaging feels wrong but works
- The sunk cost fallacy in stocks
- Anchoring bias — the ₹100 stock trap

Derivatives advanced:
- Open interest
- Max pain theory
- India VIX
- Futures rollover

Total: 52 topics = 1 year of weekly content before repetition.

---

## 16. What I need from you before building

1. **Go / no-go** on the hybrid visual approach (AI bg + data overlay).
2. **Nav placement** — dashboard card (Option B) OK for MVP?
3. **First test topic** for explainer — pick one from §15 so we can
   validate the pipeline end-to-end with a real script.
4. **Lawyer** — do you have one lined up or should we start with
   maximally conservative disclaimers and self-audit?
5. **Any branding constraints** — logo, colors, fonts, disallowed
   phrases? The compositor needs these pinned before Phase 0.

Once those are answered I'll start Phase 0 and have a playable MP4
to show within ~4 days.
