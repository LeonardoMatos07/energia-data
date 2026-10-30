"""Polite HTTP: robots.txt, an identifying User-Agent and at most one request per second per
domain. The sources send no ETag or Last-Modified, so changes are found by content hash."""

from __future__ import annotations

import logging
import time
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import requests

log = logging.getLogger(__name__)

USER_AGENT = "SpillwayPortal (contact: leonardomatos.bcet@gmail.com)"
MIN_INTERVAL_SECONDS = 1.0


class RobotsDisallowedError(RuntimeError):
    pass


class PoliteClient:
    def __init__(self, timeout_seconds: int = 30, min_interval: float = MIN_INTERVAL_SECONDS):
        self._timeout = timeout_seconds
        self._min_interval = min_interval
        self._last_request: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser | None] = {}
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def _wait(self, host: str) -> None:
        elapsed = time.monotonic() - self._last_request.get(host, 0.0)
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request[host] = time.monotonic()

    def _robots_for(self, scheme: str, host: str) -> RobotFileParser | None:
        key = f"{scheme}://{host}"
        if key not in self._robots:
            self._wait(host)
            parser = RobotFileParser()
            try:
                response = self._session.get(f"{key}/robots.txt", timeout=self._timeout)
                parser.parse(response.text.splitlines() if response.ok else [])
                self._robots[key] = parser
            except requests.RequestException:
                log.warning("robots.txt of %s is unreachable; not collecting from it", host)
                self._robots[key] = None
        return self._robots[key]

    def allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        robots = self._robots_for(parts.scheme, parts.netloc)
        return robots is not None and robots.can_fetch(USER_AGENT, url)

    def get(self, url: str, accept: str = "text/html") -> requests.Response:
        if not self.allowed(url):
            raise RobotsDisallowedError(f"robots.txt does not allow {url}")
        self._wait(urlsplit(url).netloc)
        response = self._session.get(url, headers={"Accept": accept}, timeout=self._timeout)
        response.raise_for_status()
        return response


def decode(response: requests.Response) -> str:
    """The Plone and SharePoint pages are not always UTF-8: try it first, then Windows-1252."""
    try:
        return response.content.decode("utf-8")
    except UnicodeDecodeError:
        return response.content.decode("cp1252", errors="replace")
