from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from energy_pipeline.news import parsers
from energy_pipeline.news.fetch import PoliteClient, RobotsDisallowedError, decode
from energy_pipeline.news.normalize import (
    canonical_url,
    clean_text,
    content_hash,
    local_midnight_utc,
    parse_br_date,
)
from energy_pipeline.news.parsers import RawItem
from energy_pipeline.news.run import UPSERT, build_records
from energy_pipeline.news.topics import classify

FIXTURES = Path(__file__).parent / "fixtures" / "news"
NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)


def read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_canonical_url_drops_tracking_and_fragments():
    url = "HTTPS://www.gov.br/aneel/noticia/?utm_source=x&id=7&fbclid=abc#topo"

    assert canonical_url(url) == "https://www.gov.br/aneel/noticia?id=7"


def test_clean_text_only_normalizes_whitespace_entities_and_tags():
    assert (
        clean_text("  Leil&atilde;o\n de <b>energia</b>  &amp; mais ") == "Leilão de energia & mais"
    )
    assert clean_text("   ") is None and clean_text(None) is None


def test_date_only_sources_become_midnight_in_brasilia_as_utc():
    assert parse_br_date("29/10/2026") == local_midnight_utc(datetime(2026, 10, 29).date())
    assert parse_br_date("29/10/2026") == datetime(2026, 10, 29, 3, tzinfo=UTC)
    assert parse_br_date("sem data") is None


def test_content_hash_changes_when_the_source_corrects_the_text():
    assert content_hash("A", "b") == content_hash("A", "b")
    assert content_hash("A", "b") != content_hash("A", "c")


def test_aneel_api_items_keep_title_summary_and_utc_date():
    items = parsers.parse_plone_api(read("aneel_search.json"))

    assert items[0].title == "ANEEL promove encontro com analistas de investimentos"
    assert items[0].summary == (
        "Executivos de instituições financeiras debateram aspectos do setor elétrico"
    )
    assert items[0].published_at == datetime(2026, 10, 29, 21, 28, 12, tzinfo=UTC)
    assert items[0].url.startswith("https://www.gov.br/aneel/pt-br/assuntos/noticias/")


@pytest.mark.parametrize("name", ["mme", "anp"])
def test_plone_listing_items_have_title_link_date_and_summary(name):
    items = parsers.parse_plone_listing(read(f"{name}_listing.html"), "https://www.gov.br")

    assert len(items) >= 3
    assert all(item.title and item.url.startswith("https://www.gov.br/") for item in items)
    assert all(item.published_at == datetime(2026, 10, 29, 3, tzinfo=UTC) for item in items[:2])
    assert items[0].summary and not items[0].summary.startswith(("-", "29/10"))


def test_epe_listing_drops_the_area_tags_from_the_summary():
    items = parsers.parse_epe_listing(read("epe_listing.html"), "https://www.epe.gov.br")

    assert items[0].title.startswith("EPE debate desenvolvimento da energia eólica")
    assert items[0].url == (
        "https://www.epe.gov.br/pt/imprensa/noticias/"
        "epe-debate-desenvolvimento-da-energia-eolica-na-brazil-windpower-2026"
    )
    assert items[0].published_at == datetime(2026, 10, 27, 3, tzinfo=UTC)
    assert "Comunicação e Imprensa" not in (items[0].summary or "")
    assert items[0].summary.startswith("Para contribuir")


def test_topics_are_found_by_keywords_ignoring_accents_and_case():
    assert "leiloes" in classify("Leilão de transmissão", None)
    assert "transmissao" in classify("Leilão de transmissão", None)
    assert classify("ANEEL aprova REAJUSTE da tarifa", None) == ["tarifas"]
    assert classify("Evento institucional", "Sem relação") == ["outros"]
    assert set(classify("Corte de geração eólica no Nordeste", None)) >= {
        "curtailment",
        "eolica-solar",
    }


def item(**overrides):
    base = {
        "url": "https://www.gov.br/aneel/pt-br/assuntos/noticias/2026/a",
        "title": "ANEEL aprova leilão de transmissão",
        "summary": "Resumo",
        "published_at": NOW - timedelta(hours=2),
    }
    return RawItem(**{**base, **overrides})


def test_gates_reject_missing_title_foreign_domain_and_future_date():
    records, result = build_records(
        "aneel",
        [
            item(),
            item(title=None, url="https://www.gov.br/aneel/b"),
            item(url="https://outro-site.com/x"),
            item(url="https://www.gov.br/aneel/c", published_at=NOW + timedelta(days=3)),
            item(url="https://www.gov.br/aneel/d", published_at=None),
        ],
        NOW,
    )

    assert [r.url for r in records] == ["https://www.gov.br/aneel/pt-br/assuntos/noticias/2026/a"]
    assert len(result.rejected) == 4


def test_epe_domain_is_its_own():
    records, result = build_records(
        "epe", [item(url="https://www.gov.br/x"), item(url="https://www.epe.gov.br/pt/y")], NOW
    )

    assert [r.url for r in records] == ["https://www.epe.gov.br/pt/y"]
    assert len(result.rejected) == 1


def test_anp_keeps_only_items_on_an_electricity_topic():
    records, result = build_records(
        "anp",
        [
            item(url="https://www.gov.br/anp/a", title="Expediente no protocolo", summary=None),
            item(url="https://www.gov.br/anp/b", title="Leilão de energia de reserva"),
        ],
        NOW,
    )

    assert [r.url for r in records] == ["https://www.gov.br/anp/b"]
    assert result.skipped_off_topic == 1


def test_a_source_without_recent_items_raises_a_warning_not_an_error():
    records, result = build_records("aneel", [item(published_at=NOW - timedelta(days=9))], NOW)

    assert len(records) == 1 and result.warnings and not result.rejected


def test_records_keep_the_source_text_untouched():
    records, _ = build_records("aneel", [item(title="Título  exato", summary="Resumo exato")], NOW)

    assert records[0].title == "Título  exato" and records[0].summary == "Resumo exato"


def test_upsert_never_touches_the_admin_columns():
    for column in ("status", "editor_note", "editor_note_by", "editor_note_at", "related_links"):
        assert column not in UPSERT


class FakeResponse:
    def __init__(self, content: bytes, ok: bool = True, text: str = ""):
        self.content, self.ok, self.text = content, ok, text

    def raise_for_status(self) -> None:
        pass


def test_decode_falls_back_to_windows_1252():
    assert decode(FakeResponse("energia elétrica".encode("cp1252"))) == "energia elétrica"
    assert decode(FakeResponse("energia elétrica".encode())) == "energia elétrica"


def test_client_obeys_robots_txt_and_waits_between_requests(monkeypatch):
    calls: list[str] = []

    def fake_get(self, url, **kwargs):
        calls.append(url)
        if url.endswith("/robots.txt"):
            return FakeResponse(b"", text="User-agent: *\nDisallow: /privado")
        return FakeResponse(b"ok")

    sleeps: list[float] = []
    monkeypatch.setattr("requests.Session.get", fake_get)
    monkeypatch.setattr("energy_pipeline.news.fetch.time.sleep", sleeps.append)
    client = PoliteClient(min_interval=1.0)

    assert client.get("https://exemplo.gov.br/publico").content == b"ok"
    client.get("https://exemplo.gov.br/publico2")
    with pytest.raises(RobotsDisallowedError):
        client.get("https://exemplo.gov.br/privado/x")
    assert sum(1 for c in calls if c.endswith("/robots.txt")) == 1
    assert sleeps and all(0 < s <= 1.0 for s in sleeps)
