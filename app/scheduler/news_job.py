from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Sequence, Set
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import feedparser
import requests
from bs4 import BeautifulSoup

from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.services import event_service, news_service

logger = logging.getLogger(__name__)

MAX_ARTICLES_PER_RUN = 300
NEWS_COOLDOWN_MINUTES = 90


@dataclass(frozen=True)
class NewsSource:
    name: str
    feed_url: Optional[str]
    listing_urls: Sequence[str]
    credibility: float


@dataclass
class NewsArticle:
    source: str
    title: str
    summary: str
    body: str
    url: str
    published_at: datetime
    language: str
    author: Optional[str] = None
    section: Optional[str] = None


ENTITY_PATTERNS: Dict[str, tuple[str, ...]] = {
    "gold": ("gold", "xau"),
    "silver": ("silver", "xag"),
    "crude_oil": ("crude oil", "wti", "brent"),
    "natural_gas": ("natural gas", "lng", "henry hub"),
    "copper": ("copper", "industrial metal"),
    "sp500": ("s&p500", "s&p 500", "spx"),
    "nasdaq": ("nasdaq", "ixic"),
    "dow_jones": ("dow jones", "dow", "djia"),
    "nifty_50": ("nifty 50", "nifty"),
    "sensex": ("sensex", "bse sensex"),
    "usd_inr": ("usd/inr", "rupee", "inr"),
    "eur_usd": ("eur/usd", "euro-dollar", "euro usd"),
    "jpy_usd": ("jpy/usd", "yen", "japanese yen"),
    "us10y": ("us 10y", "10-year treasury", "10y treasury"),
}

IMPACT_PATTERNS: Dict[str, tuple[str, ...]] = {
    "risk_on": ("rally", "optimism", "surge", "gains", "bullish"),
    "risk_off": ("selloff", "slump", "plunge", "fear", "bearish"),
    "inflation_signal": ("inflation", "cpi", "price pressure"),
    "growth_signal": ("gdp", "growth", "expansion", "manufacturing"),
    "policy_signal": ("fed", "ecb", "rbi", "rate cut", "rate hike", "policy"),
}


def _source_registry() -> List[NewsSource]:
    return [
        NewsSource("CNBC Finance", "https://www.cnbc.com/id/10000664/device/rss/rss.html", ["https://www.cnbc.com/finance/"], 0.9),
        NewsSource("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html", ["https://www.cnbc.com/economy/"], 0.9),
        NewsSource("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html", ["https://www.cnbc.com/"], 0.88),
        NewsSource("MarketWatch Top Stories", "http://feeds.marketwatch.com/marketwatch/topstories/", [], 0.86),
        NewsSource("MarketWatch Markets", "http://feeds.marketwatch.com/marketwatch/marketpulse/", [], 0.86),
        NewsSource("CNN Business", "http://rss.cnn.com/rss/money_latest.rss", [], 0.78),
        NewsSource("Fox Business", "https://moxie.foxbusiness.com/google-publisher/latest.xml", [], 0.74),
        NewsSource("Forbes Business", "https://www.forbes.com/business/feed/", [], 0.76),
        NewsSource("Yahoo Finance", "https://finance.yahoo.com/news/rssindex", ["https://finance.yahoo.com/news/"], 0.84),
        NewsSource("The Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", ["https://economictimes.indiatimes.com/markets"], 0.78),
        NewsSource("The Economic Times Economy", "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms", ["https://economictimes.indiatimes.com/news/economy"], 0.78),
        NewsSource("Mint Markets", "https://www.livemint.com/rss/markets", ["https://www.livemint.com/market"], 0.76),
        NewsSource("Mint Economy", "https://www.livemint.com/rss/economy", ["https://www.livemint.com/economy"], 0.76),
        NewsSource("Moneycontrol Markets Google Feed", "https://news.google.com/rss/search?q=site:moneycontrol.com+markets&hl=en-IN&gl=IN&ceid=IN:en", [], 0.62),
        NewsSource("Seeking Alpha Market News", "https://seekingalpha.com/market_currents.xml", ["https://seekingalpha.com/market-news"], 0.8),
        NewsSource("Investopedia", None, ["https://www.investopedia.com/news-4427706"], 0.72),
        NewsSource("Nasdaq Google Feed", "https://news.google.com/rss/search?q=site:nasdaq.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.61),
        NewsSource("CoinDesk Markets", "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml", ["https://www.coindesk.com/markets/"], 0.7),
        NewsSource("The Guardian Business", "https://www.theguardian.com/business/rss", ["https://www.theguardian.com/business"], 0.73),
        NewsSource("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml", [], 0.75),
        NewsSource("NPR Business", "https://feeds.npr.org/1006/rss.xml", ["https://www.npr.org/sections/business/"], 0.7),
        NewsSource("AP Business", None, ["https://apnews.com/hub/business"], 0.74),
        NewsSource("Financial Express Markets", "https://www.financialexpress.com/market/feed/", ["https://www.financialexpress.com/market/"], 0.7),
        NewsSource("Reuters Google Feed", "https://news.google.com/rss/search?q=site:reuters.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.68),
        NewsSource("Bloomberg Google Feed", "https://news.google.com/rss/search?q=site:bloomberg.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.67),
        NewsSource("FT Google Feed", "https://news.google.com/rss/search?q=site:ft.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.67),
        NewsSource("WSJ Google Feed", "https://news.google.com/rss/search?q=site:wsj.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.66),
        NewsSource("Business Standard Google Feed", "https://news.google.com/rss/search?q=site:business-standard.com+markets&hl=en-IN&gl=IN&ceid=IN:en", [], 0.63),
        NewsSource("TheStreet Google Feed", "https://news.google.com/rss/search?q=site:thestreet.com+markets&hl=en-US&gl=US&ceid=US:en", [], 0.62),
        NewsSource("Zee Business Google Feed", "https://news.google.com/rss/search?q=site:zeebiz.com+markets&hl=en-IN&gl=IN&ceid=IN:en", [], 0.6),
        NewsSource("NDTV Profit Google Feed", "https://news.google.com/rss/search?q=site:ndtvprofit.com+markets&hl=en-IN&gl=IN&ceid=IN:en", [], 0.6),
    ]


class NewsScraper(BaseScraper):

    def __init__(self) -> None:
        super().__init__()
        self._emitted_cache: Dict[str, datetime] = {}
        self._blocked_domains: Set[str] = set()

    @staticmethod
    def _canonicalize(url: str) -> str:
        parsed = urlparse(url)
        query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if not k.lower().startswith("utm_")]
        cleaned = parsed._replace(scheme=parsed.scheme or "https", netloc=parsed.netloc.lower(), query=urlencode(query), fragment="")
        return urlunparse(cleaned)

    def _parse_dt(self, value: str | None) -> datetime:
        if not value:
            return self.utc_now()
        try:
            p = parsedate_to_datetime(value)
            return p.replace(tzinfo=timezone.utc) if p.tzinfo is None else p.astimezone(timezone.utc)
        except Exception:
            return self.utc_now()

    def _extract_body(self, url: str) -> str:
        domain = urlparse(url).netloc.lower()
        if domain in self._blocked_domains:
            return ""
        try:
            html = self._get_text(url, timeout=10.0, retries=0)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status and status in {401, 403, 404, 410, 451, 500, 502, 503}:
                self._blocked_domains.add(domain)
            return ""
        except requests.RequestException:
            self._blocked_domains.add(domain)
            return ""
        soup = BeautifulSoup(html, "html.parser")
        for sel in ["article p", "main p", ".article-body p", ".story-body p", "p"]:
            blocks = [p.get_text(" ", strip=True) for p in soup.select(sel)]
            blocks = [b for b in blocks if len(b) > 40]
            if blocks:
                return "\n".join(blocks[:80]).strip()
        return ""

    def _discover_feeds(self, source: NewsSource) -> List[NewsArticle]:
        if not source.feed_url:
            return []
        raw = self._get_text(source.feed_url, timeout=8.0, retries=0)
        feed = feedparser.parse(raw)
        articles = []
        for entry in feed.entries:
            title = str(entry.get("title", "")).strip()
            link = str(entry.get("link", "")).strip()
            if not title or not link:
                continue
            summary = BeautifulSoup(str(entry.get("summary", "")), "html.parser").get_text(" ", strip=True)
            lang = str(entry.get("language") or feed.feed.get("language") or "en").lower()
            articles.append(NewsArticle(
                source=source.name, title=title, summary=summary[:800], body="",
                url=self._canonicalize(link),
                published_at=self._parse_dt(entry.get("published") or entry.get("updated")),
                language=lang,
                author=str(entry.get("author")) if entry.get("author") else None,
            ))
        return articles

    def _discover_listings(self, source: NewsSource) -> List[NewsArticle]:
        articles = []
        for listing in source.listing_urls[:3]:
            try:
                html = self._get_text(listing, timeout=8.0, retries=0)
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.select("a[href]")[:400]:
                    href = a.get("href", "").strip()
                    title = a.get_text(" ", strip=True)
                    if not href or len(title) < 15:
                        continue
                    full = urljoin(listing, href)
                    if urlparse(full).netloc != urlparse(listing).netloc:
                        continue
                    if any(s in urlparse(full).path.lower() for s in ["/video", "/live", "/podcast"]):
                        continue
                    articles.append(NewsArticle(
                        source=source.name, title=title[:280], summary="", body="",
                        url=self._canonicalize(full), published_at=self.utc_now(), language="en",
                    ))
            except Exception:
                pass
        return articles

    def _enrich(self, raw: Sequence[NewsArticle]) -> List[NewsArticle]:
        seen_hashes: Set[str] = set()
        seen_fps: Set[str] = set()
        result = []
        for article in raw:
            if not any(article.language.startswith(c) for c in ["en"]):
                continue
            h = hashlib.sha256(f"{article.url}|{article.title.strip().lower()}".encode()).hexdigest()
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            try:
                body = self._extract_body(article.url)
            except Exception:
                body = ""
            enriched = NewsArticle(
                source=article.source, title=article.title, summary=article.summary,
                body=body[:12000], url=article.url, published_at=article.published_at,
                language=article.language, author=article.author,
            )
            norm = " ".join(f"{enriched.title} {enriched.summary[:240]} {enriched.body[:320]}".lower().split())
            fp = hashlib.sha256(norm.encode()).hexdigest()
            if fp in seen_fps:
                continue
            seen_fps.add(fp)
            result.append(enriched)
            if len(result) >= MAX_ARTICLES_PER_RUN:
                break
        return result

    def fetch_all(self) -> List[NewsArticle]:
        discovered = []
        for source in _source_registry():
            items = []
            try:
                items = self._discover_feeds(source)
            except Exception:
                pass
            if not items:
                try:
                    items = self._discover_listings(source)
                except Exception:
                    pass
            discovered.extend(items)
        return self._enrich(discovered)

    def to_records(self, articles: Sequence[NewsArticle]) -> List[Dict]:
        records = []
        for a in articles:
            text = f"{a.title}\n{a.summary}\n{a.body}"
            entities = _extract_entities(text)
            impact = _detect_impact(text)
            confidence = min(0.95, 0.5 + 0.08 * min(3, len(entities)))
            records.append({
                "title": a.title, "summary": a.summary, "body": a.body,
                "timestamp": a.published_at.isoformat(), "source": a.source,
                "url": a.url, "primary_entity": entities[0] if entities else "market_news",
                "impact": impact, "confidence": confidence,
            })
        return records

    def generate_events(self, articles: Sequence[NewsArticle]) -> List[Dict]:
        events = []
        for a in articles:
            text = f"{a.title}\n{a.summary}\n{a.body}"
            entities = _extract_entities(text)
            if not entities:
                continue
            impact = _detect_impact(text)
            src_score = next((s.credibility for s in _source_registry() if s.name == a.source), 0.65)
            hours = max(0.0, (self.utc_now() - a.published_at).total_seconds() / 3600)
            bonus = 0.15 if hours <= 6 else 0.08 if hours <= 24 else 0.03
            for entity in entities[:3]:
                key = f"{entity}|{impact}|{hashlib.sha256(a.title.lower().encode()).hexdigest()[:16]}"
                now = self.utc_now()
                prev = self._emitted_cache.get(key)
                if prev and (now - prev).total_seconds() / 60 < NEWS_COOLDOWN_MINUTES:
                    continue
                self._emitted_cache[key] = now
                conf = min(0.98, 0.35 + src_score * 0.35 + min(0.2, 0.05 * len(entities)) + bonus)
                events.append({
                    "event_type": "news_market_linked_signal",
                    "entity": entity, "impact": impact, "confidence": round(conf, 4),
                })
        return events


def _extract_entities(text: str) -> List[str]:
    low = text.lower()
    return [e for e, patterns in ENTITY_PATTERNS.items() if any(p in low for p in patterns)]


def _detect_impact(text: str) -> str:
    low = text.lower()
    for impact, patterns in IMPACT_PATTERNS.items():
        if any(p in low for p in patterns):
            return impact
    return "market_signal"


_scraper = NewsScraper()


def _fetch_news_data_sync() -> tuple:
    """Sync fetch and transform; run in thread executor so main loop is not blocked. Returns (records, events)."""
    articles = _scraper.fetch_all()
    records = _scraper.to_records(articles)
    events = _scraper.generate_events(articles)
    return (records, events)


async def run_news_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        records, events = await loop.run_in_executor(
            get_job_executor("news"),
            _fetch_news_data_sync,
        )
        accepted = 0
        for rec in records:
            try:
                await news_service.upsert_article(rec)
                accepted += 1
            except Exception:
                logger.warning("News upsert failed for: %s", rec.get("title", "")[:60])
        ev_ok = 0
        for ev in events:
            try:
                await event_service.insert_event_dict(ev)
                ev_ok += 1
            except Exception:
                pass
        logger.info("News job complete: articles=%d accepted=%d events=%d", len(records), accepted, ev_ok)
    except Exception:
        logger.exception("News job failed")
