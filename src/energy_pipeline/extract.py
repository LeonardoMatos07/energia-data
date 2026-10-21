"""Download raw files from the ONS open data portal."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import requests

log = logging.getLogger(__name__)

_CHUNK_SIZE = 1024 * 1024


class SourceNotPublishedError(RuntimeError):
    """The requested file does not exist (yet) on the ONS portal."""


def remote_etag(url: str, timeout_seconds: int) -> str | None:
    """ETag of a published file: it changes whenever ONS revises the file."""
    response = requests.head(url, timeout=timeout_seconds)
    if response.status_code in (403, 404):
        raise SourceNotPublishedError(f"{url} returned HTTP {response.status_code}")
    response.raise_for_status()
    etag = response.headers.get("ETag")
    return etag.strip('"') if etag else None


def download(url: str, destination: Path, timeout_seconds: int) -> str:
    """Stream ``url`` into ``destination`` and return the file's SHA-256 checksum."""
    log.info("Downloading %s", url)
    checksum = hashlib.sha256()
    with requests.get(url, stream=True, timeout=timeout_seconds) as response:
        # The ONS bucket answers 403 instead of 404 for keys that do not exist.
        if response.status_code in (403, 404):
            raise SourceNotPublishedError(f"{url} returned HTTP {response.status_code}")
        response.raise_for_status()
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                file.write(chunk)
                checksum.update(chunk)
    log.info("Downloaded %.1f MB", destination.stat().st_size / 1e6)
    return checksum.hexdigest()
