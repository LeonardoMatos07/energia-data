import polars as pl
import pytest

from energy_pipeline import ccee, market_extra
from energy_pipeline.quality import DataQualityError


def test_flags_parse_commas_without_leading_zero_and_keep_one_flag_per_month():
    raw = pl.DataFrame(
        {
            "DatGeracaoConjuntoDados": ["2026-10-26"] * 4,
            "DatCompetencia": ["2026-08-01", "2026-09-01", "2026-10-01", "2026-10-01"],
            "NomBandeiraAcionada": ["Amarela", "Amarela", "Verde", "Verde"],
            "VlrAdicionalBandeira": ["18,85", "18,85", ",00", ",00"],
        }
    )

    valid, rejected = market_extra.transform_flags(raw)

    assert valid.height == 3 and rejected.height == 0
    assert valid["additional_brl_per_mwh"].to_list() == [18.85, 18.85, 0.0]
    assert ccee.raise_on_errors(market_extra.flag_checks(valid, rejected)) == []


def test_an_unknown_flag_blocks_the_load():
    raw = pl.DataFrame(
        {
            "DatCompetencia": ["2026-10-01"],
            "NomBandeiraAcionada": ["Roxa"],
            "VlrAdicionalBandeira": ["10,00"],
        }
    )
    valid, rejected = market_extra.transform_flags(raw)

    with pytest.raises(DataQualityError):
        ccee.raise_on_errors(market_extra.flag_checks(valid, rejected))


def test_consumption_and_agents_are_market_wide_monthly_values():
    consumption, rejected = market_extra.transform_consumption(
        pl.DataFrame(
            {
                "MES_REFERENCIA": ["202312", "202311"],
                "CONSUMO_TOTAL_ACR": ["47645.68845415457", "47165.14981380833"],
                "CONSUMO_TOTAL_ACL": ["25300.465460936826", "26545.627427431944"],
            }
        )
    )
    agents, agents_rejected = market_extra.transform_agents(
        pl.DataFrame(
            {
                "MES_REFERENCIA": ["202312", "202312"],
                "CLASSE": ["Autoprodutor", "Consumidor Especial"],
                "QUANTIDADE_AGENTE_CONTABILIZACAO": ["104", "9672"],
            }
        )
    )

    assert str(consumption["month"][0]) == "2023-11-01"
    assert consumption["free_mw_avg"][1] == pytest.approx(25300.4654609)
    assert ccee.raise_on_errors(market_extra.consumption_checks(consumption, rejected)) == []
    assert agents["agents"].to_list() == [104, 9672]
    assert ccee.raise_on_errors(market_extra.agents_checks(agents, agents_rejected)) == []


def test_negative_consumption_is_rejected_by_the_gate():
    consumption, rejected = market_extra.transform_consumption(
        pl.DataFrame(
            {
                "MES_REFERENCIA": ["202312"],
                "CONSUMO_TOTAL_ACR": ["-1"],
                "CONSUMO_TOTAL_ACL": ["10"],
            }
        )
    )
    with pytest.raises(DataQualityError):
        ccee.raise_on_errors(market_extra.consumption_checks(consumption, rejected))
