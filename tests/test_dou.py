from datetime import UTC, date, datetime

from energy_pipeline.news.dou import UPSERT, RawAct, build_act_records, dou_enabled, run_dou

NOW = datetime(2026, 10, 30, 12, tzinfo=UTC)


def act(**overrides):
    base = {
        "organ": "aneel",
        "act_type": "Resolução Normativa",
        "act_number": "1.000",
        "act_date": date(2026, 10, 28),
        "title": "Dispõe sobre  tarifas",
        "summary": "Ementa sobre reajuste",
        "body": "Texto integral do ato",
        "dou_date": date(2026, 10, 29),
        "dou_section": 1,
        "dou_edition": "207",
        "dou_page": "45",
        "dou_url": "https://www.in.gov.br/web/dou/-/resolucao-1000",
    }
    return RawAct(**{**base, **overrides})


def test_acts_keep_the_whole_text_and_the_full_dou_reference():
    records, rejected = build_act_records([act()], NOW)

    assert not rejected and records[0].act.organ == "ANEEL"
    assert records[0].act.body == "Texto integral do ato"
    assert (records[0].act.dou_section, records[0].act.dou_edition, records[0].act.dou_page) == (
        1,
        "207",
        "45",
    )
    assert "tarifas" in records[0].topics


def test_gates_reject_other_organs_missing_title_foreign_url_and_future_date():
    _, rejected = build_act_records(
        [
            act(organ="ANP"),
            act(title="  "),
            act(dou_url="https://example.com/x"),
            act(dou_date=date(2026, 12, 1)),
        ],
        NOW,
    )

    assert len(rejected) == 4


def test_the_content_hash_changes_when_the_published_text_changes():
    first, _ = build_act_records([act()], NOW)
    second, _ = build_act_records([act(body="Texto retificado")], NOW)

    assert first[0].content_hash != second[0].content_hash


def test_upsert_is_keyed_on_url_and_hash_and_never_writes_status():
    assert "ON CONFLICT (dou_url, content_hash)" in UPSERT
    assert "status" not in UPSERT


def test_without_a_channel_the_run_does_nothing_and_says_why(monkeypatch):
    monkeypatch.delenv("DOU_ENABLED", raising=False)

    result = run_dou(settings=None, day=date(2026, 10, 29))  # type: ignore[arg-type]

    assert not dou_enabled()
    assert result.stored == 0 and result.skipped and "INLABS" in result.skipped
