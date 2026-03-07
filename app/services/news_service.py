import logging

from app.core.database import get_pool, record_to_dict

TABLE = "news_articles"
logger = logging.getLogger(__name__)


async def upsert_article(payload: dict) -> dict:
    """Upsert a news article row and return the saved row."""
    pool = await get_pool()
    filtered = {
        "title": payload["title"],
        "summary": payload.get("summary"),
        "body": payload.get("body"),
        "timestamp": payload["timestamp"],
        "source": payload.get("source"),
        "url": payload.get("url"),
        "primary_entity": payload.get("primary_entity"),
        "impact": payload.get("impact"),
        "confidence": payload.get("confidence"),
    }
    if filtered.get("url"):
        row = await pool.fetchrow(
            f"""
            INSERT INTO {TABLE}
            (title, summary, body, timestamp, source, url, primary_entity, impact, confidence)
            VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7, $8, $9)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                body = EXCLUDED.body,
                timestamp = EXCLUDED.timestamp,
                source = EXCLUDED.source,
                primary_entity = EXCLUDED.primary_entity,
                impact = EXCLUDED.impact,
                confidence = EXCLUDED.confidence
            RETURNING *
            """,
            filtered["title"],
            filtered["summary"],
            filtered["body"],
            filtered["timestamp"],
            filtered["source"],
            filtered["url"],
            filtered["primary_entity"],
            filtered["impact"],
            filtered["confidence"],
        )
        return record_to_dict(row)
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE}
        (title, summary, body, timestamp, source, url, primary_entity, impact, confidence)
        VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7, $8, $9)
        RETURNING *
        """,
        filtered["title"],
        filtered["summary"],
        filtered["body"],
        filtered["timestamp"],
        filtered["source"],
        filtered["url"],
        filtered["primary_entity"],
        filtered["impact"],
        filtered["confidence"],
    )
    return record_to_dict(row)


async def get_articles(
    entity: str | None = None,
    impact: str | None = None,
    source: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch news articles with optional filters, ordered by most recent."""
    pool = await get_pool()
    conditions = []
    args = []
    n = 1
    if entity:
        conditions.append(f"primary_entity = ${n}")
        args.append(entity)
        n += 1
    if impact:
        conditions.append(f"impact = ${n}")
        args.append(impact)
        n += 1
    if source:
        conditions.append(f"source = ${n}")
        args.append(source)
        n += 1
    where = " AND ".join(conditions) if conditions else "TRUE"
    args.extend([limit, offset])
    rows = await pool.fetch(
        f"SELECT * FROM {TABLE} WHERE {where} ORDER BY timestamp DESC LIMIT ${n} OFFSET ${n + 1}",
        *args,
    )
    return [record_to_dict(r) for r in rows]
