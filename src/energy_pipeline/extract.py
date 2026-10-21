from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

_CHUNK_SIZE = 1024 * 1024


def _session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


_http = _session()


class SourceNotPublishedError(RuntimeError):
    pass


def remote_etag(url: str, timeout_seconds: int) -> str | None:
    response = _http.head(url, timeout=timeout_seconds)
    if response.status_code in (403, 404):
        raise SourceNotPublishedError(f"{url} returned HTTP {response.status_code}")
    response.raise_for_status()
    etag = response.headers.get("ETag")
    return etag.strip('"') if etag else None


def download(url: str, destination: Path, timeout_seconds: int) -> str:
    log.info("Downloading %s", url)
    checksum = hashlib.sha256()
    with _http.get(url, stream=True, timeout=timeout_seconds) as response:
        if response.status_code in (403, 404):
            raise SourceNotPublishedError(f"{url} returned HTTP {response.status_code}")
        response.raise_for_status()
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                file.write(chunk)
                checksum.update(chunk)
    log.info("Downloaded %.1f MB", destination.stat().st_size / 1e6)
    return checksum.hexdigest()
