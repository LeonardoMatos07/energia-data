"""Normalization of what the sources publish. Text is never rewritten: only whitespace, HTML
entities and tags that are not part of the text are cleaned (CC BY-ND: no derivative versions)."""

from __future__ import annotations

import hashlib
import html
import re
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _local_timezone() -> tzinfo:
    try:
        return ZoneInfo("America/Sao_Paulo")
    except ZoneInfoNotFoundError:  # no tzdata installed; Brazil has had no DST since 2019
        return timezone(timedelta(hours=-3))


LOCAL_TIMEZONE = _local_timezone()
TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "ref", "ref_src"}
_TAG = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"\s+")


def clean_text(value: str | None) -> str | None:
    """Plain text with the same content: no tags, no entities, single spaces."""
    if value is None:
        return None
    text = _SPACES.sub(" ", html.unescape(_TAG.sub(" ", value))).strip()
    return text or None


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMS
    ]
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, urlencode(query), ""))


def to_utc(value: datetime) -> datetime:
    """Aware datetimes are converted; naive ones are Brasilia time."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=LOCAL_TIMEZONE)
    return value.astimezone(UTC)


def local_midnight_utc(day: date) -> datetime:
    """Sources that publish only a date: midnight in Brasilia time, as UTC."""
    return datetime(day.year, day.month, day.day, tzinfo=LOCAL_TIMEZONE).astimezone(UTC)


def parse_br_date(value: str) -> datetime | None:
    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", value)
    if match is None:
        return None
    day, month, year = (int(group) for group in match.groups())
    try:
        return local_midnight_utc(date(year, month, day))
    except ValueError:
        return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return to_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def content_hash(title: str, summary: str | None) -> str:
    return hashlib.sha256(f"{title}\n{summary or ''}".encode()).hexdigest()
