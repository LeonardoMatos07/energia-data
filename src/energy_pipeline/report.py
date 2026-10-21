from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date

import requests

from energy_pipeline.config import Settings
from energy_pipeline.notify import machine_token
from energy_pipeline.period import Period
from energy_pipeline.warehouse import Warehouse

MONTH_NAMES = [
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
]
SOURCE_NAMES = {"wind": "eólica", "solar": "solar"}
SUBSYSTEM_NAMES = {"N": "Norte", "NE": "Nordeste", "S": "Sul", "SE": "Sudeste/Centro-Oeste"}
REASON_NAMES = {
    "rel": "Confiabilidade elétrica",
    "cnf": "Indisponibilidade externa",
    "ene": "Razão energética",
    "other": "Outros motivos",
}
STATE_NAMES = {
    "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas", "AP": "Amapá", "BA": "Bahia",
    "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MG": "Minas Gerais", "MS": "Mato Grosso do Sul", "MT": "Mato Grosso",
    "PA": "Pará", "PB": "Paraíba", "PE": "Pernambuco", "PI": "Piauí", "PR": "Paraná",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RO": "Rondônia", "RR": "Roraima",
    "RS": "Rio Grande do Sul", "SC": "Santa Catarina", "SE": "Sergipe", "SP": "São Paulo",
    "TO": "Tocantins",
}  # fmt: skip
TOP_STATES = 5
TOP_PLANTS = 10
TOTAL_COLUMNS = (
    "curtailed_mwh, verified_generation_mwh, curtailment_pct, estimated_loss_brl, affected_plants"
)


@dataclass(frozen=True)
class Totals:
    curtailed_mwh: float
    verified_mwh: float
    pct: float | None
    loss_brl: float | None
    affected_plants: int


@dataclass(frozen=True)
class RankedState:
    state: str
    curtailed_mwh: float
    pct: float | None


@dataclass(frozen=True)
class RankedPlant:
    name: str
    slug: str
    source: str
    state: str
    curtailed_mwh: float
    pct: float | None


@dataclass(frozen=True)
class ReportData:
    period: Period
    national: Totals
    previous_month: Totals | None
    previous_year: Totals | None
    by_source: dict[str, Totals]
    by_subsystem: dict[str, Totals]
    reasons_mwh: dict[str, float]
    states: list[RankedState]
    plants: list[RankedPlant]


@dataclass(frozen=True)
class Report:
    month: date
    title: str
    slug: str
    summary: str
    content: str


def shift(period: Period, months: int) -> Period:
    index = period.year * 12 + period.month - 1 + months
    return Period(index // 12, index % 12 + 1)


def _optional(value: object) -> float | None:
    return None if value is None else float(value)  # type: ignore[arg-type]


def _totals(row: tuple[object, ...]) -> Totals:
    curtailed, verified, pct, loss, plants = row
    return Totals(
        float(curtailed),  # type: ignore[arg-type]
        float(verified),  # type: ignore[arg-type]
        _optional(pct),
        _optional(loss),
        int(plants),  # type: ignore[call-overload]
    )


def fetch_report_data(settings: Settings, period: Period) -> ReportData:
    month = period.start.date()
    with Warehouse(settings.warehouse_dsn).session() as session:

        def national(target: Period, source: str) -> Totals | None:
            rows = session.fetchall(
                f"select {TOTAL_COLUMNS} from subsystem_monthly_curtailment "
                "where subsystem = 'SIN' and source = %s and month = %s",
                (source, target.start.date()),
            )
            return _totals(rows[0]) if rows else None

        current = national(period, "total")
        if current is None:
            raise LookupError(f"No published curtailment for {period}")
        by_source = {
            source: totals
            for source in SOURCE_NAMES
            if (totals := national(period, source)) is not None
        }
        by_subsystem = {
            str(row[0]): _totals(row[1:])
            for row in session.fetchall(
                f"select subsystem, {TOTAL_COLUMNS} from subsystem_monthly_curtailment "
                "where subsystem <> 'SIN' and source = 'total' and month = %s",
                (month,),
            )
        }
        reasons = session.fetchall(
            "select sum(curtailed_rel_mwh), sum(curtailed_cnf_mwh), sum(curtailed_ene_mwh), "
            "sum(curtailed_other_mwh) from subsystem_monthly_curtailment "
            "where subsystem <> 'SIN' and source = 'total' and month = %s",
            (month,),
        )[0]
        states = [
            RankedState(str(state), float(curtailed), _optional(pct))
            for state, curtailed, pct in session.fetchall(
                "select state, curtailed_mwh, curtailment_pct from state_monthly_curtailment "
                "where source = 'total' and month = %s order by curtailed_mwh desc limit %s",
                (month, TOP_STATES),
            )
        ]
        plants = [
            RankedPlant(
                str(name), str(slug), str(source), str(state), float(curtailed), _optional(pct)
            )
            for name, slug, source, state, curtailed, pct in session.fetchall(
                "select p.name, p.slug, p.source, p.state, m.curtailed_mwh, m.curtailment_pct "
                "from plant_monthly_curtailment m join plant p on p.id = m.plant_id "
                "where p.kind = 'plant' and m.month = %s order by m.curtailed_mwh desc limit %s",
                (month, TOP_PLANTS),
            )
        ]
        return ReportData(
            period=period,
            national=current,
            previous_month=national(shift(period, -1), "total"),
            previous_year=national(shift(period, -12), "total"),
            by_source=by_source,
            by_subsystem=by_subsystem,
            reasons_mwh=dict(zip(REASON_NAMES, (float(value) for value in reasons), strict=True)),
            states=states,
            plants=plants,
        )


def _number(value: float, digits: int = 0) -> str:
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _gwh(mwh: float) -> str:
    return f"{_number(mwh / 1000, 1)} GWh"


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{_number(value, 1)}%"


def _brl(value: float | None) -> str:
    if value is None:
        return "—"
    if abs(value) >= 1e6:
        return f"R$ {_number(value / 1e6, 1)} milhões"
    return f"R$ {_number(value / 1e3, 0)} mil"


def _month_label(period: Period) -> str:
    return f"{MONTH_NAMES[period.month - 1]} de {period.year}"


def _comparison(current: Totals, reference: Totals | None) -> str | None:
    if reference is None or not reference.curtailed_mwh:
        return None
    delta = (current.curtailed_mwh - reference.curtailed_mwh) / reference.curtailed_mwh * 100
    sign = "+" if delta >= 0 else "−"
    return (
        f"{sign}{_number(abs(delta), 1)}% "
        f"({_gwh(reference.curtailed_mwh)}, {_pct(reference.pct)} de corte)"
    )


def _table(header: list[str], rows: list[list[str]], numeric_from: int) -> list[str]:
    alignment = ["---"] * numeric_from + ["---:"] * (len(header) - numeric_from)
    return [
        "| " + " | ".join(header) + " |",
        "|" + "|".join(alignment) + "|",
        *("| " + " | ".join(row) + " |" for row in rows),
    ]


def build_report(data: ReportData) -> Report:
    period, now = data.period, data.national
    label = _month_label(period)
    summary = (
        f"Em {label}, o ONS mandou cortar {_gwh(now.curtailed_mwh)} de geração eólica e solar, "
        f"{_pct(now.pct)} do que as usinas poderiam ter entregue, com perda estimada de "
        f"{_brl(now.loss_brl)}."
    )
    if data.states:
        top = data.states[0]
        summary += f" {STATE_NAMES.get(top.state, top.state)} foi o estado mais afetado."

    lines = [
        "## O mês em números",
        "",
        f"- **Energia cortada:** {_gwh(now.curtailed_mwh)}",
        f"- **Percentual de corte:** {_pct(now.pct)} (cortada ÷ gerada + cortada)",
        f"- **Perda estimada:** {_brl(now.loss_brl)}",
        f"- **Usinas afetadas:** {_number(now.affected_plants)}",
    ]
    previous_year_label = f"{MONTH_NAMES[period.month - 1]} de {period.year - 1}"
    for name, reference in (
        ("ao mês anterior", data.previous_month),
        (f"a {previous_year_label}", data.previous_year),
    ):
        if text := _comparison(now, reference):
            lines.append(f"- **Energia cortada frente {name}:** {text}")

    if data.by_source:
        lines += ["", "## Eólica e solar", ""]
        lines += _table(
            ["Fonte", "Energia cortada", "% de corte", "Perda estimada"],
            [
                [
                    SOURCE_NAMES[source].capitalize(),
                    _gwh(totals.curtailed_mwh),
                    _pct(totals.pct),
                    _brl(totals.loss_brl),
                ]
                for source, totals in data.by_source.items()
            ],
            1,
        )

    lines += ["", "## Onde os cortes se concentraram", ""]
    ranked = sorted(data.by_subsystem.items(), key=lambda item: item[1].curtailed_mwh, reverse=True)
    lines += _table(
        ["Subsistema", "Energia cortada", "% de corte", "Parcela do total"],
        [
            [
                SUBSYSTEM_NAMES.get(subsystem, subsystem),
                _gwh(totals.curtailed_mwh),
                _pct(totals.pct),
                _pct(totals.curtailed_mwh / now.curtailed_mwh * 100 if now.curtailed_mwh else None),
            ]
            for subsystem, totals in ranked
        ],
        1,
    )
    lines += ["", f"### Os {len(data.states)} estados com mais energia cortada", ""]
    lines += _table(
        ["Estado", "Energia cortada", "% de corte"],
        [
            [STATE_NAMES.get(s.state, s.state), _gwh(s.curtailed_mwh), _pct(s.pct)]
            for s in data.states
        ],
        1,
    )
    lines += ["", f"### As {len(data.plants)} usinas com mais energia cortada", ""]
    lines += _table(
        ["Usina", "Fonte", "Estado", "Energia cortada", "% de corte"],
        [
            [
                f"[{p.name}](/usinas/{p.slug})",
                SOURCE_NAMES[p.source].capitalize(),
                p.state,
                _gwh(p.curtailed_mwh),
                _pct(p.pct),
            ]
            for p in data.plants
        ],
        3,
    )

    total_reasons = sum(data.reasons_mwh.values())
    if total_reasons:
        lines += ["", "## Por que a energia foi cortada", ""]
        lines += _table(
            ["Motivo", "Energia cortada", "Parcela"],
            [
                [REASON_NAMES[key], _gwh(mwh), _pct(mwh / total_reasons * 100)]
                for key, mwh in sorted(
                    data.reasons_mwh.items(), key=lambda item: item[1], reverse=True
                )
                if mwh > 0
            ],
            1,
        )

    lines += [
        "",
        "## Como ler estes números",
        "",
        "Os dados vêm dos arquivos públicos de restrição de operação (*constrained-off*) do ONS. "
        "A energia cortada é a geração não realizada apurada pelo ONS; o percentual de corte "
        "divide essa energia pela soma do que foi gerado e do que foi cortado. A perda em reais "
        "é uma **estimativa**: multiplica a energia cortada pelo preço de referência do período "
        "(PLD). O ONS pode revisar dados já publicados, então os números dos meses mais recentes "
        "podem mudar. Veja a [metodologia](/metodologia).",
    ]
    return Report(
        month=period.start.date(),
        title=f"Curtailment em {label}: {_gwh(now.curtailed_mwh)} cortados",
        slug=f"curtailment-{period.year:04d}-{period.month:02d}",
        summary=summary,
        content="\n".join(lines) + "\n",
    )


def linkedin_post(report: Report, site_url: str) -> str:
    link = f"{site_url.rstrip('/')}/relatorios/{report.slug}"
    return f"{report.title}\n\n{report.summary}\n\nRelatório completo: {link}\n"


def submit(settings: Settings, report: Report, publish: bool) -> dict:
    if not settings.api_url:
        raise RuntimeError("API_URL is not set")
    token = machine_token(settings)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = requests.post(
        f"{settings.api_url.rstrip('/')}/v1/admin/reports",
        json={
            "month": report.month.strftime("%Y-%m"),
            "title": report.title,
            "slug": report.slug,
            "summary": report.summary,
            "content": report.content,
            "publish": publish,
        },
        headers=headers,
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
    result: dict = response.json()
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="energy_pipeline.report")
    parser.add_argument("--month", type=Period.parse, required=True)
    parser.add_argument("--submit", action="store_true", help="save the report through the API")
    parser.add_argument("--publish", action="store_true", help="make the submitted report public")
    parser.add_argument("--site-url", default="http://localhost:3000")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    report = build_report(fetch_report_data(settings, args.month))
    print(f"# {report.title}\n\n_{report.summary}_\n\n{report.content}")
    print("--- LinkedIn ---\n" + linkedin_post(report, args.site_url))
    if args.submit:
        saved = submit(settings, report, args.publish)
        print(f"saved {saved['slug']} (published_at={saved['published_at']})")


if __name__ == "__main__":
    main()
