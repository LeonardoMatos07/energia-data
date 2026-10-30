"""Turns what each source publishes into raw items (title, summary, date, link) without ever
reading the article body."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from energy_pipeline.news.normalize import (
    clean_text,
    parse_br_date,
    parse_iso_datetime,
)


@dataclass(frozen=True)
class RawItem:
    url: str
    title: str | None
    summary: str | None
    published_at: datetime | None
    source_updated_at: datetime | None = None


def parse_plone_api(payload: str | bytes) -> list[RawItem]:
    """ANEEL: the JSON search of the portal (the one its own front end uses)."""
    items = json.loads(payload).get("items", [])
    return [
        RawItem(
            url=item.get("@id", ""),
            title=clean_text(item.get("title")),
            summary=clean_text(item.get("description")),
            published_at=parse_iso_datetime(item.get("effective")),
            source_updated_at=parse_iso_datetime(item.get("modified")),
        )
        for item in items
    ]


def parse_plone_listing(html_text: str, base_url: str) -> list[RawItem]:
    """MME and ANP: `ul.noticias > li` with the title link, `span.data` and a description line."""
    tree = HTMLParser(html_text)
    items: list[RawItem] = []
    for entry in tree.css("ul.noticias li div.conteudo"):
        link = entry.css_first("h2.titulo a")
        if link is None:
            continue
        date_node = entry.css_first("span.data")
        published_at = parse_br_date(date_node.text()) if date_node else None
        description = entry.css_first("span.descricao")
        summary = None
        if description is not None:
            if date_node is not None:
                date_node.decompose()
            summary = clean_text(description.text(separator=" ").strip().lstrip("-").strip())
        items.append(
            RawItem(
                url=urljoin(base_url, link.attributes.get("href") or ""),
                title=clean_text(link.text()),
                summary=summary,
                published_at=published_at,
            )
        )
    return items


def parse_epe_listing(html_text: str, base_url: str) -> list[RawItem]:
    """EPE (SharePoint): `div.news > div.item` with the link, title, date and a summary."""
    tree = HTMLParser(html_text)
    items: list[RawItem] = []
    for entry in tree.css("div.news div.item"):
        link = entry.css_first("a[href]")
        title = entry.css_first("h2")
        small = entry.css_first("p.small")
        if link is None or title is None:
            continue
        date_node = small.css_first("span.date") if small else None
        published_at = parse_br_date(date_node.text()) if date_node else None
        summary = None
        if small is not None:
            for node in small.css("a.tag-area, span.date"):
                node.decompose()
            summary = clean_text(small.text(separator=" ").strip().lstrip("-").strip())
        items.append(
            RawItem(
                url=urljoin(base_url, link.attributes.get("href") or ""),
                title=clean_text(title.text()),
                summary=summary,
                published_at=published_at,
            )
        )
    return items
