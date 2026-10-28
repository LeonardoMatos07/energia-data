from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from energy_pipeline import notify, steps
from energy_pipeline.config import Settings

RUN = datetime(2026, 10, 3, 6, tzinfo=UTC)  # the October run loads September


class Response:
    def __init__(self, status: int = 202, body: dict | None = None, text: str = "") -> None:
        self.status_code = status
        self._body = body or {}
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._body


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    for name in ("API_URL", "COGNITO_TOKEN_URL", "DATA_CLIENT_ID", "DATA_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    return replace(Settings.from_env(), api_url="http://api.test")


def test_the_regular_monthly_run_announces_the_newest_month() -> None:
    periods = ["2026-07", "2026-08", "2026-09"]
    assert steps.month_to_announce(periods, RUN, backfill=False) == "2026-09"


@pytest.mark.parametrize(
    ("periods", "backfill"),
    [
        (["2021-10", "2026-03"], True),  # a backfill never mails anyone
        (["2026-05", "2026-06", "2026-07"], False),  # a re-run of older months
        ([], False),
    ],
)
def test_nothing_is_announced_for_backfills_or_old_months(
    periods: list[str], backfill: bool
) -> None:
    assert steps.month_to_announce(periods, RUN, backfill) is None


def test_the_api_is_asked_to_mail_the_month(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def post(url: str, **kwargs: object) -> Response:
        calls.append({"url": url, **kwargs})
        return Response(202, {"id": 7, "status": "running"})

    monkeypatch.setattr(notify.requests, "post", post)

    result = steps.announce_report(["2026-08", "2026-09"], RUN, False, settings)

    assert result == {"id": 7, "status": "running"}
    assert calls[0]["url"] == "http://api.test/v1/admin/dispatches"
    assert calls[0]["json"] == {"kind": "report", "month": "2026-09", "retry": False}


def test_a_month_that_was_already_mailed_counts_as_done(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        notify.requests,
        "post",
        lambda url, **kw: Response(
            409, text='{"detail":"the report for 2026-09 was already sent (done)"}'
        ),
    )
    assert notify.request_report_dispatch(settings, "2026-09") == {"already_sent": "2026-09"}


def test_a_send_that_stopped_partway_is_finished_with_retry(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    bodies: list[dict] = []

    def post(url: str, **kwargs: dict) -> Response:
        bodies.append(kwargs["json"])
        if not kwargs["json"]["retry"]:
            return Response(409, text="already sent (partial)")
        return Response(202, {"status": "running"})

    monkeypatch.setattr(notify.requests, "post", post)

    assert notify.request_report_dispatch(settings, "2026-09") == {"status": "running"}
    assert [b["retry"] for b in bodies] == [False, True]


def test_a_real_failure_is_not_swallowed(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        notify.requests,
        "post",
        lambda url, **kw: Response(422, text="there is no published report"),
    )
    with pytest.raises(RuntimeError):
        notify.request_report_dispatch(settings, "2026-09")


def test_the_e_mail_can_be_switched_off_and_needs_an_api(settings: Settings) -> None:
    off = replace(settings, dispatch_report_email=False)
    assert "DISPATCH_REPORT_EMAIL" in steps.announce_report(["2026-09"], RUN, False, off)["skipped"]
    no_api = replace(settings, api_url=None)
    assert steps.announce_report(["2026-09"], RUN, False, no_api) == {"skipped": "no API_URL"}


def test_reports_are_published_for_the_months_with_data_and_the_portal_refreshed(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from energy_pipeline import report

    saved: list[str] = []
    refreshed: list[list[str]] = []

    def fetch(_settings: Settings, period: object) -> object:
        if str(period) == "2026-07":
            raise LookupError("no data")
        return str(period)

    monkeypatch.setattr(report, "fetch_report_data", fetch)
    monkeypatch.setattr(
        report, "build_report", lambda data: type("R", (), {"slug": f"curtailment-{data}"})()
    )
    monkeypatch.setattr(report, "submit", lambda s, r, publish: saved.append(r.slug) or {})
    monkeypatch.setattr(
        notify, "request_revalidation", lambda s, tags: refreshed.append(tags) or {}
    )

    result = steps.publish_reports(["2026-07", "2026-08", "2026-09"], settings)

    assert result == {"published": ["curtailment-2026-08", "curtailment-2026-09"]}
    assert refreshed == [["reports"]]
