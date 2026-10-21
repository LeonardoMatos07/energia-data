from __future__ import annotations

import logging

import requests

from energy_pipeline.config import Settings

log = logging.getLogger(__name__)

PORTAL_TAGS = ["curtailment", "plants"]


def machine_token(settings: Settings) -> str | None:
    if not (settings.cognito_token_url and settings.data_client_id and settings.data_client_secret):
        return None
    response = requests.post(
        settings.cognito_token_url,
        data={"grant_type": "client_credentials", "scope": settings.api_scope},
        auth=(settings.data_client_id, settings.data_client_secret),
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
    token: str = response.json()["access_token"]
    return token


def request_revalidation(settings: Settings, tags: list[str] = PORTAL_TAGS) -> dict:
    if not settings.api_url:
        log.info("API_URL is not set; skipping portal revalidation")
        return {"skipped": True}
    token = machine_token(settings)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = requests.post(
        f"{settings.api_url.rstrip('/')}/v1/admin/revalidate",
        json={"tags": tags},
        headers=headers,
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
    result: dict = response.json()
    log.info("Portal revalidation requested for %s: %s", tags, result)
    return result
