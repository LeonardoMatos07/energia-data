from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from energy_pipeline import aneel_reliability, ccee, opendata
from energy_pipeline.quality import DataQualityError

HOURLY_QUOTED = (
    '"MES_REFERENCIA";"SUBMERCADO";"PERIODO_COMERCIALIZACAO";"DIA";"HORA";"PLD_HORA"\n'
    '"202101";NORDESTE;1;"01";"00";204.36\n'
    '"202101";NORTE;1;"01";"00";204.37\n'
    '"202101";SUDESTE;1;"01";"00";204.37\n'
    '"202101";SUL;1;"01";"00";204.37\n'
    '"202101";SUL;2;"01";"01";60.00\n'
)
SHADOW_BARE = (
    "MES_REFERENCIA;SUBMERCADO;PERIODO_COMERCIALIZACAO;DIA;HORA;TIPO_SOMBRA;PLD_SOMBRA\n"
    "202408;NORDESTE;1;1;0;NEWAVE_HIBRIDO;197.26\n"
    "202408;NORTE;1;1;0;NEWAVE_HIBRIDO;197.3\n"
    "202408;SUDESTE;1;1;0;NEWAVE_HIBRIDO;197.28\n"
    "202408;SUL;1;1;0;NEWAVE_HIBRIDO;197.24\n"
)


def _csv(tmp_path: Path, text: str, encoding: str = "utf-8") -> pl.DataFrame:
    path = tmp_path / "file.csv"
    path.write_bytes(text.encode(encoding))
    return ccee.read_csv(path)


def test_read_csv_handles_both_encodings_and_quoting(tmp_path):
    quoted = _csv(tmp_path, HOURLY_QUOTED, "iso8859-2")
    bare = _csv(tmp_path, SHADOW_BARE)

    assert quoted.columns[-1] == "PLD_HORA" and quoted.height == 5
    assert bare["TIPO_SOMBRA"][0] == "NEWAVE_HIBRIDO"


def test_transform_hourly_official(tmp_path):
    valid, rejected = ccee.transform_hourly(_csv(tmp_path, HOURLY_QUOTED), "official")

    assert rejected.height == 0 and valid.height == 5
    assert set(valid["subsystem"]) == {"N", "NE", "S", "SE"}
    first = valid.filter(pl.col("subsystem") == "NE").row(0, named=True)
    assert first["hour"] == datetime(2021, 1, 1, 0)
    assert first["hour_utc"] == datetime(2021, 1, 1, 3)  # Brasilia is UTC-3 in January 2021
    assert first["variant"] == "official" and first["method"] == "official"
    assert first["pld_brl_per_mwh"] == pytest.approx(204.36)


def test_transform_hourly_shadow_keeps_the_method(tmp_path):
    valid, _ = ccee.transform_hourly(_csv(tmp_path, SHADOW_BARE), "shadow")

    assert set(valid["variant"]) == {"shadow"} and set(valid["method"]) == {"NEWAVE_HIBRIDO"}


def test_transform_hourly_rejects_unparseable_rows(tmp_path):
    raw = _csv(tmp_path, HOURLY_QUOTED).with_columns(
        pl.when(pl.col("DIA") == "01").then(pl.lit("xx")).otherwise(pl.col("DIA")).alias("DIA")
    )

    valid, rejected = ccee.transform_hourly(raw, "official")

    assert valid.height == 0 and rejected.height == 5


def test_hourly_checks_pass_for_clean_data_and_warn_outside_the_regulatory_range(tmp_path):
    valid, rejected = ccee.transform_hourly(_csv(tmp_path, HOURLY_QUOTED), "official")
    # one hour only has the SUL submarket: the four-submarkets gate must fail
    with pytest.raises(DataQualityError):
        ccee.raise_on_errors(ccee.hourly_checks(valid, rejected, "official"))

    complete = valid.filter(pl.col("hour").dt.hour() == 0)
    warnings = ccee.raise_on_errors(ccee.hourly_checks(complete, rejected, "official"))
    assert warnings == []

    cheap = complete.with_columns(pl.lit(1.0).alias("pld_brl_per_mwh"))
    warnings = ccee.raise_on_errors(ccee.hourly_checks(cheap, rejected, "official"))
    assert any("within_regulatory_floor_and_cap" in warning for warning in warnings)


def test_hourly_checks_block_negative_or_absurd_prices(tmp_path):
    valid, rejected = ccee.transform_hourly(_csv(tmp_path, SHADOW_BARE), "shadow")
    absurd = valid.with_columns(pl.lit(99999.0).alias("pld_brl_per_mwh"))

    with pytest.raises(DataQualityError):
        ccee.raise_on_errors(ccee.hourly_checks(absurd, rejected, "shadow"))


def test_weekly_transform_parses_dates_and_integers():
    raw = pl.DataFrame(
        {
            "MES_REFERENCIA": ["200106", "200106"],
            "SUBMERCADO": ["NORDESTE", "NORTE"],
            "SEMANA": ["30/06/2001", "30/06/2001"],
            "PLD_MEDIA_SEMANA": ["684", "684,5"],
        }
    )

    valid, rejected = ccee.transform_weekly(raw)

    assert rejected.height == 0
    assert valid["pld_avg_brl_per_mwh"].to_list() == [684.5, 684.0] or set(
        valid["pld_avg_brl_per_mwh"]
    ) == {684.0, 684.5}
    assert str(valid["week_start"][0]) == "2001-06-30"


def test_aggregate_counts_hours_at_the_floor():
    hours = pl.DataFrame(
        {
            "hour": [datetime(2026, 1, 1, h) for h in range(3)],
            "subsystem": ["SE"] * 3,
            "variant": ["official"] * 3,
            "pld_brl_per_mwh": [57.31, 57.31, 100.0],
        }
    )

    daily = ccee.aggregate(hours, "day")
    monthly = ccee.aggregate(hours, "month")

    assert daily["hours_at_floor"][0] == 2 and daily["hours"][0] == 3
    assert monthly["pld_max_brl_per_mwh"][0] == 100.0
    assert str(monthly["month"][0]) == "2026-01-01"


def test_years_to_load():
    now = datetime(2026, 10, 30, tzinfo=UTC)

    assert ccee.years_to_load(2021, 2026, False, now) == [2025, 2026]
    assert ccee.years_to_load(2021, 2026, True, now) == [2021, 2022, 2023, 2024, 2025, 2026]
    assert ccee.years_to_load(2022, 2026, False, datetime(2022, 3, 1, tzinfo=UTC)) == [2022]


def test_ckan_resources_pairs_names_with_download_links():
    page = (
        '<a class="heading" href="/dataset/x/resource/1" title="pld_horario_2021">a</a>'
        '<a class="heading" href="/dataset/x/resource/2" title="pld_horario_2022">b</a>'
        '<a href="https://pda-download.ccee.org.br/AAA/content">d</a>'
        '<a href="https://pda-download.ccee.org.br/BBB/content">d</a>'
    )

    resources = opendata.ckan_resources(page)

    assert [(r.name, r.url.split("/")[3]) for r in resources] == [
        ("pld_horario_2021", "AAA"),
        ("pld_horario_2022", "BBB"),
    ]


def _values() -> pl.DataFrame:
    rows = [
        # (cnpj, acronym, set_id, set_name, indicator, year, month, value)
        ("1", "EDP ES   ", "100", "Guacui", "DEC", "2026", "1", ",50"),
        ("1", "EDP ES   ", "100", "Guacui", "FEC", "2026", "1", "0,25"),
        ("1", "EDP ES   ", "100", "Guacui", "NumCon", "2026", "1", "36993,00"),
        ("1", "EDP ES   ", "100", "Guacui", "DECXN", "2026", "1", ",10"),
        ("1", "EDP ES   ", "100", "Guacui", "DEC", "2026", "2", "1,00"),
    ]
    return pl.DataFrame(
        {
            "DatGeracaoConjuntoDados": ["2026-10-05"] * len(rows),
            "NumCNPJ": [r[0] for r in rows],
            "SigAgente": [r[1] for r in rows],
            "IdeConjUndConsumidoras": [r[2] for r in rows],
            "DscConjUndConsumidoras": [r[3] for r in rows],
            "SigIndicador": [r[4] for r in rows],
            "AnoIndice": [r[5] for r in rows],
            "NumPeriodoIndice": [r[6] for r in rows],
            "VlrIndiceEnviado": [r[7] for r in rows],
        }
    )


def test_reliability_values_become_one_row_per_set_and_month():
    valid, rejected = aneel_reliability.transform_values(_values())

    january = valid.filter(pl.col("month_number") == 1).row(0, named=True)
    assert rejected.height == 0 and valid.height == 2
    assert january["dec_hours"] == pytest.approx(0.5)  # ",50" has no leading zero
    assert january["fec_count"] == pytest.approx(0.25)
    assert january["consumers"] == 36993
    assert january["distributor_acronym"] == "EDP ES"
    assert str(january["month"]) == "2026-01-01"


def test_reliability_limits_are_attached_by_set_and_year():
    limits_raw = pl.DataFrame(
        {
            "NumCNPJ": ["1", "1"],
            "IdeConjUndConsumidoras": ["100", "100"],
            "SigIndicador": ["DEC", "FEC"],
            "AnoLimiteQualidade": ["2026", "2026"],
            "VlrLimite": ["12,50", "8,00"],
        }
    )
    values, _ = aneel_reliability.transform_values(_values())

    joined = aneel_reliability.attach_limits(values, aneel_reliability.transform_limits(limits_raw))

    assert set(joined["dec_limit_hours"]) == {12.5} and set(joined["fec_limit_count"]) == {8.0}


def test_reliability_checks_block_impossible_values():
    values, rejected = aneel_reliability.transform_values(_values())
    now = datetime(2026, 10, 30, tzinfo=UTC)
    assert ccee.raise_on_errors(aneel_reliability.reliability_checks(values, rejected, now)) == []

    broken = values.with_columns(pl.lit(9999.0).alias("dec_hours"))
    with pytest.raises(DataQualityError):
        ccee.raise_on_errors(aneel_reliability.reliability_checks(broken, rejected, now))
