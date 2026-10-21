from dataclasses import replace

import pytest

from energy_pipeline import notify
from energy_pipeline.config import Settings


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload


@pytest.fixture
def settings(monkeypatch):
    for name in ("API_URL", "COGNITO_TOKEN_URL", "DATA_CLIENT_ID", "DATA_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    return Settings.from_env()


def test_skips_without_api_url(settings):
    assert notify.request_revalidation(settings) == {"skipped": True}


def test_local_call_sends_no_token(settings, monkeypatch):
    calls = []
    monkeypatch.setattr(
        notify.requests, "post", lambda url, **kw: calls.append((url, kw)) or FakeResponse({})
    )

    notify.request_revalidation(replace(settings, api_url="http://localhost:8000/"))

    url, kwargs = calls[0]
    assert url == "http://localhost:8000/v1/admin/revalidate"
    assert kwargs["json"] == {"tags": ["curtailment", "plants"]}
    assert kwargs["headers"] == {}


def test_production_call_uses_machine_token(settings, monkeypatch):
    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        if "oauth2/token" in url:
            return FakeResponse({"access_token": "abc"})
        return FakeResponse({"web": "ok"})

    monkeypatch.setattr(notify.requests, "post", fake_post)
    configured = replace(
        settings,
        api_url="https://api.example.com",
        cognito_token_url="https://auth.example.com/oauth2/token",
        data_client_id="client",
        data_client_secret="secret",
    )

    assert notify.request_revalidation(configured) == {"web": "ok"}
    token_call, api_call = calls
    assert token_call[1]["data"] == {
        "grant_type": "client_credentials",
        "scope": "energia-api/admin",
    }
    assert token_call[1]["auth"] == ("client", "secret")
    assert api_call[1]["headers"] == {"Authorization": "Bearer abc"}
