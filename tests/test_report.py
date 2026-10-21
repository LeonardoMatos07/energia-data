from __future__ import annotations

from energy_pipeline.period import Period
from energy_pipeline.report import (
    RankedPlant,
    RankedState,
    ReportData,
    Totals,
    build_report,
    linkedin_post,
    shift,
)


def totals(curtailed: float, pct: float = 20.0) -> Totals:
    return Totals(curtailed, 4 * curtailed, pct, 2_500_000.0, 12)


def data(**overrides: object) -> ReportData:
    values: dict[str, object] = {
        "period": Period(2026, 9),
        "national": totals(5000.0),
        "previous_month": totals(4000.0, 18.0),
        "previous_year": None,
        "by_source": {"wind": totals(3000.0), "solar": totals(2000.0)},
        "by_subsystem": {"NE": totals(4000.0), "S": totals(1000.0)},
        "reasons_mwh": {"rel": 500.0, "cnf": 1500.0, "ene": 3000.0, "other": 0.0},
        "states": [RankedState("RN", 3000.0, 40.0)],
        "plants": [RankedPlant("Cumaru I", "cumaru-i-x", "wind", "RN", 1000.0, 70.0)],
    }
    return ReportData(**{**values, **overrides})  # type: ignore[arg-type]


def test_shift_crosses_year_boundaries() -> None:
    assert shift(Period(2026, 1), -1) == Period(2025, 12)
    assert shift(Period(2026, 9), -12) == Period(2025, 9)


def test_report_identity_and_headline() -> None:
    report = build_report(data())
    assert report.slug == "curtailment-2026-09"
    assert report.month.isoformat() == "2026-09-01"
    assert "5,0 GWh" in report.title
    assert "Rio Grande do Norte foi o estado mais afetado" in report.summary


def test_report_compares_with_the_previous_month_only_when_available() -> None:
    content = build_report(data()).content
    assert "**Energia cortada frente ao mês anterior:** +25,0%" in content
    assert "frente a setembro de 2025" not in content


def test_report_drops_empty_reasons_and_links_plants() -> None:
    content = build_report(data()).content
    assert "Outros motivos" not in content
    assert "[Cumaru I](/usinas/cumaru-i-x)" in content


def test_linkedin_post_links_to_the_report() -> None:
    report = build_report(data())
    assert linkedin_post(report, "https://example.org/").endswith(
        "Relatório completo: https://example.org/relatorios/curtailment-2026-09\n"
    )
