"""Official acts (ANEEL, MME) from the DOU into `regulatory_act`.

There is no collector for a real channel yet, on purpose: the DOU portal (in.gov.br) forbids
automated access in its robots.txt, and the official XML channel (INLABS) needs a registration
that has to be made by a person. This module is the channel-agnostic part: it takes acts already
parsed by an `ActSource`, normalizes and classifies them, applies the gates and stores them
idempotently. When a legitimate source exists, only an `ActSource` has to be written.

Acts are official acts (Lei 9.610/1998, art. 8, IV): the text is kept whole, with the full DOU
reference. The pipeline never writes `status` (the admin hides an act).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

from energy_pipeline.config import Settings
from energy_pipeline.news.normalize import clean_text
from energy_pipeline.news.topics import classify
from energy_pipeline.notify import request_revalidation
from energy_pipeline.warehouse import Session, Warehouse

log = logging.getLogger(__name__)

ORGANS = ("ANEEL", "MME")
MAX_FUTURE = timedelta(days=1)
DOU_HOST = "in.gov.br"


class ActSource(Protocol):
    """A legitimate way to read the acts of one DOU day (to be written once one is available)."""

    def acts_for(self, day: date) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class RawAct:
    organ: str
    act_type: str
    act_number: str | None
    act_date: date | None
    title: str
    summary: str | None
    body: str | None
    dou_date: date
    dou_section: int | None
    dou_edition: str | None
    dou_page: str | None
    dou_url: str


@dataclass(frozen=True)
class ActRecord:
    act: RawAct
    topics: list[str]
    content_hash: str


@dataclass
class DouResult:
    day: date
    fetched: int = 0
    stored: int = 0
    rejected: list[str] | None = None
    skipped: str | None = None


def build_act_records(acts: list[RawAct], now: datetime) -> tuple[list[ActRecord], list[str]]:
    """Gates: an allowed organ, a title, a DOU url and a date that is not in the future."""
    records: list[ActRecord] = []
    rejected: list[str] = []
    for act in acts:
        host = act.dou_url.split("/")[2] if act.dou_url.startswith("https://") else ""
        if act.organ.upper() not in ORGANS:
            rejected.append(f"organ not followed: {act.organ}")
        elif not act.title.strip():
            rejected.append(f"missing title: {act.dou_url}")
        elif not (host == DOU_HOST or host.endswith(f".{DOU_HOST}")):
            rejected.append(f"not a DOU url: {act.dou_url}")
        elif datetime(act.dou_date.year, act.dou_date.month, act.dou_date.day, tzinfo=UTC) > (
            now + MAX_FUTURE
        ):
            rejected.append(f"future DOU date: {act.dou_url}")
        else:
            text = " ".join(part for part in (act.title, act.summary) if part)
            digest = hashlib.sha256(
                f"{act.title}\n{act.summary or ''}\n{act.body or ''}".encode()
            ).hexdigest()
            records.append(
                ActRecord(
                    act=RawAct(
                        **{
                            **act.__dict__,
                            "organ": act.organ.upper(),
                            "title": clean_text(act.title) or act.title,
                            "summary": clean_text(act.summary),
                        }
                    ),
                    topics=classify(text, None),
                    content_hash=digest,
                )
            )
    return records, rejected


UPSERT = """
INSERT INTO regulatory_act (organ, act_type, act_number, act_date, title, summary, body, dou_date,
                            dou_section, dou_edition, dou_page, dou_url, topics, content_hash,
                            fetched_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
ON CONFLICT (dou_url, content_hash) DO UPDATE SET
  topics = EXCLUDED.topics,
  fetched_at = EXCLUDED.fetched_at
"""


def store_acts(session: Session, records: list[ActRecord], fetched_at: datetime) -> None:
    """Idempotent: the same act with the same text is one row; a corrected text is a new row."""
    for record in records:
        act = record.act
        session.execute(
            UPSERT,
            (
                act.organ,
                act.act_type,
                act.act_number,
                act.act_date,
                act.title,
                act.summary,
                act.body,
                act.dou_date,
                act.dou_section,
                act.dou_edition,
                act.dou_page,
                act.dou_url,
                json.dumps(record.topics),
                record.content_hash,
                fetched_at,
            ),
        )


def dou_enabled() -> bool:
    return os.getenv("DOU_ENABLED", "false").lower() in ("1", "true", "yes")


def run_dou(
    settings: Settings,
    day: date,
    source: ActSource | None = None,
    now: datetime | None = None,
    parse: Callable[[dict[str, Any]], RawAct] | None = None,
) -> DouResult:
    """Load one DOU day. Without an enabled channel it does nothing and says why."""
    result = DouResult(day=day)
    if source is None or parse is None or not dou_enabled():
        result.skipped = (
            "no DOU channel is configured (the portal forbids scraping; INLABS needs a "
            "registration made by a person); set DOU_ENABLED=true and provide an ActSource"
        )
        log.warning("DOU skipped: %s", result.skipped)
        return result
    now = now or datetime.now(UTC)
    raw = [parse(item) for item in source.acts_for(day)]
    records, rejected = build_act_records(raw, now)
    result.fetched, result.rejected = len(raw), rejected
    with Warehouse(settings.warehouse_dsn).session() as session:
        store_acts(session, records, now)
    result.stored = len(records)
    if records:
        request_revalidation(settings, ["news"])
    return result
