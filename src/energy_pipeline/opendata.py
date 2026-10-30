"""Polite access to the CKAN open-data portals (CCEE, ANEEL).

The portals forbid ``/api/`` in robots.txt, so datasets are read from their public pages, which
list direct download links. The portals ask for a 10 s crawl delay; requests are spaced out.
"""

from __future__ import annotations

import hashlib
import html
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path

import requests

log = logging.getLogger(__name__)

USER_AGENT = "SpillwayPortal (contact: leonardomatos.bcet@gmail.com)"
CRAWL_DELAY_SECONDS = 10.0
_CHUNK = 1024 * 1024

_last_request: dict[str, float] = {}


@dataclass(frozen=True)
class Resource:
    name: str
    url: str


def _wait(host: str, delay: float) -> None:
    elapsed = time.monotonic() - _last_request.get(host, 0.0)
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_request[host] = time.monotonic()


def _host(url: str) -> str:
    return url.split("/")[2]


def fetch_text(url: str, timeout: int = 120, delay: float = CRAWL_DELAY_SECONDS) -> str:
    _wait(_host(url), delay)
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    return response.text


def download(
    url: str, destination: Path, timeout: int = 300, delay: float = CRAWL_DELAY_SECONDS
) -> tuple[str, str | None]:
    """Download a file; return its sha256 and the file name from Content-Disposition."""
    _wait(_host(url), delay)
    checksum = hashlib.sha256()
    with requests.get(
        url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=timeout
    ) as response:
        response.raise_for_status()
        disposition = response.headers.get("Content-Disposition", "")
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=_CHUNK):
                file.write(chunk)
                checksum.update(chunk)
    match = re.search(r'filename="([^"]+)"', disposition)
    return checksum.hexdigest(), match.group(1) if match else None


def ckan_resources(page_html: str) -> list[Resource]:
    """Resources of a CKAN dataset page: the name of each one and its direct download link."""
    names = [
        html.unescape(name)
        for name in re.findall(
            r'<a[^>]*href="/dataset/[^"]*/resource/[^"]*"[^>]*title="([^"]*)"', page_html
        )
    ]
    urls = re.findall(r'href="(https?://[^"]*(?:/download/|/content)[^"]*)"', page_html)
    urls = list(dict.fromkeys(urls))
    if len(names) == len(urls):
        return [Resource(name, url) for name, url in zip(names, urls, strict=True)]
    return [Resource(url.rsplit("/", 1)[-1], url) for url in urls]
