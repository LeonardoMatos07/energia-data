"""Hourly collection of public sector news into `news_item`.

Only what the source publishes in its listing is read: title, summary, date and link. The text is
kept as published, the pipeline never writes the admin columns (status, editor_*, related_links),
and an item is only updated when its content hash changed.
"""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from energy_pipeline.config import Settings
from energy_pipeline.news import parsers
from energy_pipeline.news.fetch import PoliteClient, decode
from energy_pipeline.news.normalize import canonical_url, content_hash, to_utc
from energy_pipeline.news.parsers import RawItem
from energy_pipeline.news.topics import FALLBACK_TOPIC, classify
from energy_pipeline.notify import request_revalidation
from energy_pipeline.storage import Lake
from energy_pipeline.warehouse import Session, Warehouse

log = logging.getLogger(__name__)

NEWS_TAGS = ["news"]
MAX_FUTURE = timedelta(days=1)
STALE_AFTER = timedelta(days=7)
ANEEL_SEARCH_PARAMS = "portal_type=News%20Item&sort_on=effective&sort_order=descending&b_size=30"

# The domains each source may link to: a link that leaves them is rejected.
ALLOWED_DOMAINS = {
    "aneel": ("gov.br",),
    "mme": ("gov.br",),
    "anp": ("gov.br",),
    "epe": ("epe.gov.br",),
}
# ANP covers oil, gas and biofuels too: only items on an electricity topic are kept.
SECTOR_ONLY = {"anp"}

Parser = Callable[[str, str], list[RawItem]]


def _plone_api(text: str, _base: str) -> list[RawItem]:
    return parsers.parse_plone_api(text)


PARSERS: dict[str, Parser] = {
    "api": _plone_api,
    "mme": parsers.parse_plone_listing,
    "anp": parsers.parse_plone_listing,
    "epe": parsers.parse_epe_listing,
}


@dataclass(frozen=True)
class NewsRecord:
    source_slug: str
    url: str
    title: str
    summary: str | None
    published_at: datetime
    source_updated_at: datetime | None
    content_hash: str
    topics: list[str]


@dataclass
class SourceResult:
    slug: str
    fetched: int = 0
    stored: int = 0
    rejected: list[str] = field(default_factory=list)
    skipped_off_topic: int = 0
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


def _domain_ok(url: str, domains: tuple[str, ...]) -> bool:
    host = urlsplit(url).hostname or ""
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def build_records(
    slug: str, items: list[RawItem], now: datetime
) -> tuple[list[NewsRecord], SourceResult]:
    """Gates and normalization of the raw items of one source."""
    result = SourceResult(slug=slug, fetched=len(items))
    records: list[NewsRecord] = []
    for item in items:
        if not item.title or not item.url:
            result.rejected.append(f"missing title or url: {item.url or item.title!r}")
            continue
        url = canonical_url(item.url)
        if not _domain_ok(url, ALLOWED_DOMAINS.get(slug, ())):
            result.rejected.append(f"domain not allowed: {url}")
            continue
        if item.published_at is None or to_utc(item.published_at) > now + MAX_FUTURE:
            result.rejected.append(f"missing or future date: {url}")
            continue
        topics = classify(item.title, item.summary)
        if slug in SECTOR_ONLY and topics == [FALLBACK_TOPIC]:
            result.skipped_off_topic += 1
            continue
        records.append(
            NewsRecord(
                source_slug=slug,
                url=url,
                title=item.title,
                summary=item.summary,
                published_at=to_utc(item.published_at),
                source_updated_at=(
                    to_utc(item.source_updated_at) if item.source_updated_at else None
                ),
                content_hash=content_hash(item.title, item.summary),
                topics=topics,
            )
        )
    if records and max(r.published_at for r in records) < now - STALE_AFTER:
        result.warnings.append(f"{slug}: no item newer than {STALE_AFTER.days} days (feed broken?)")
    return records, result


UPSERT = """
INSERT INTO news_item (source_id, url, title, summary, published_at, source_updated_at,
                       fetched_at, content_hash, topics)
VALUES ((SELECT id FROM news_source WHERE slug = %s), %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (url) DO UPDATE SET
  title = EXCLUDED.title,
  summary = EXCLUDED.summary,
  published_at = EXCLUDED.published_at,
  source_updated_at = CASE
    WHEN news_item.content_hash <> EXCLUDED.content_hash
      THEN COALESCE(EXCLUDED.source_updated_at, EXCLUDED.fetched_at)
    ELSE news_item.source_updated_at END,
  fetched_at = EXCLUDED.fetched_at,
  content_hash = EXCLUDED.content_hash,
  topics = EXCLUDED.topics
"""


def store(session: Session, records: list[NewsRecord], fetched_at: datetime) -> None:
    """Upsert by URL. Only the collection columns are written, never the admin ones."""
    for record in records:
        session.execute(
            UPSERT,
            (
                record.source_slug,
                record.url,
                record.title,
                record.summary,
                record.published_at,
                record.source_updated_at,
                fetched_at,
                record.content_hash,
                json.dumps(record.topics),
            ),
        )


def _fetch(client: PoliteClient, slug: str, method: str, url: str) -> tuple[str, list[RawItem]]:
    if method == "api":
        text = decode(client.get(f"{url}?{ANEEL_SEARCH_PARAMS}", accept="application/json"))
        return text, PARSERS["api"](text, url)
    text = decode(client.get(url))
    return text, PARSERS[slug](text, url)


def _archive(settings: Settings, slug: str, text: str, now: datetime) -> None:
    """The raw response goes to the bronze layer, dated, for audit."""
    key = f"bronze/news/{slug}/{now:%Y/%m/%d}/{now:%H%M}.{'json' if text[:1] == '{' else 'html'}"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "raw"
            path.write_text(text, encoding="utf-8")
            Lake(settings.lake_bucket, settings.s3_endpoint_url).upload_file(path, key)
    except Exception:
        log.warning("could not archive %s (lake not available?)", key, exc_info=True)


def run_news(
    settings: Settings,
    only: list[str] | None = None,
    client: PoliteClient | None = None,
    now: datetime | None = None,
) -> list[SourceResult]:
    now = now or datetime.now(UTC)
    client = client or PoliteClient(settings.http_timeout_seconds)
    warehouse = Warehouse(settings.warehouse_dsn)
    with warehouse.session() as session:
        sources = session.fetchall(
            "SELECT slug, fetch_method, feed_url FROM news_source "
            "WHERE active AND fetch_method IN ('api', 'rss', 'listing') ORDER BY slug"
        )
    results: list[SourceResult] = []
    for slug, method, feed_url in sources:
        if only and slug not in only:
            continue
        if slug not in ALLOWED_DOMAINS or not feed_url:
            log.warning("no collector for source %s; skipping", slug)
            continue
        try:
            text, items = _fetch(client, slug, method, feed_url)
            records, result = build_records(slug, items, now)
            _archive(settings, slug, text, now)
            with warehouse.session() as session:
                store(session, records, now)
            result.stored = len(records)
        except Exception as error:  # one broken source must not stop the others
            log.exception("news source %s failed", slug)
            result = SourceResult(slug=slug, error=str(error))
        for line in result.rejected + result.warnings:
            log.warning("%s", line)
        results.append(result)
    if any(r.stored for r in results):
        request_revalidation(settings, NEWS_TAGS)
    return results
