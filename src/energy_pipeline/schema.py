from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from energy_pipeline.transform import (
    clean_text,
    parse_date,
    parse_float,
    parse_int,
    parse_timestamp,
)

MAX_UNPARSEABLE_SHARE = 0.01

PARSERS: dict[str, Callable[[str], pl.Expr]] = {
    "float": parse_float,
    "int": parse_int,
    "timestamp": parse_timestamp,
    "date": parse_date,
}

_CLUSTER_CURTAILMENT = {
    "din_instante": "timestamp",
    "id_ons": "text",
    "nom_usina": "text",
    "id_subsistema": "text",
    "id_estado": "text",
    "id_pontoconexao": "text",
    "nom_pontoconexao": "text",
    "nom_agenteoperador": "text",
    "val_geracao": "float",
    "val_geracaolimitada": "float",
    "val_disponibilidade": "float",
    "val_geracaoreferencia": "float",
    "val_geracaoreferenciafinal": "float",
    "val_geracaonaorealizadaapurada": "float",
    "cod_razaorestricao": "text",
    "cod_origemrestricao": "text",
    "dsc_restricao": "text",
    "num_minutos_rel": "int",
    "num_minutos_cnf": "int",
    "num_minutos_ene": "int",
    "num_minutos_restricao": "int",
}

_PLANT_CURTAILMENT = {
    "din_instante": "timestamp",
    "id_ons": "text",
    "nom_usina": "text",
    "ceg": "text",
    "id_ons_conjuntousina": "text",
    "nom_conjuntousina": "text",
    "id_subsistema": "text",
    "id_estado": "text",
    "nom_modalidadeoperacao": "text",
    "val_geracaoestimada": "float",
    "val_geracaoverificada": "float",
    "val_geracaoreferenciafinal": "float",
    "nom_origemgeracaoreferencia": "text",
    "flg_geracaorestrita": "int",
}

SCHEMAS: dict[str, dict[str, str]] = {
    "load_curve": {
        "id_subsistema": "text",
        "nom_subsistema": "text",
        "din_instante": "timestamp",
        "val_cargaenergiahomwmed": "float",
    },
    "plant_generation": {
        "din_instante": "timestamp",
        "id_ons": "text",
        "nom_usina": "text",
        "ceg": "text",
        "nom_tipousina": "text",
        "nom_tipocombustivel": "text",
        "cod_modalidadeoperacao": "text",
        "id_subsistema": "text",
        "id_estado": "text",
        "val_geracao": "float",
    },
    "generation_capacity": {
        "cod_equipamento": "text",
        "nom_unidadegeradora": "text",
        "num_unidadegeradora": "text",
        "nom_usina": "text",
        "ceg": "text",
        "nom_tipousina": "text",
        "nom_combustivel": "text",
        "nom_modalidadeoperacao": "text",
        "nom_agenteproprietario": "text",
        "id_subsistema": "text",
        "id_estado": "text",
        "dat_entradateste": "date",
        "dat_entradaoperacao": "date",
        "dat_desativacao": "date",
        "val_potenciaefetiva": "float",
    },
    "marginal_cost": {
        "id_subsistema": "text",
        "din_instante": "timestamp",
        "val_cmo": "float",
    },
    "wind_cluster_curtailment": _CLUSTER_CURTAILMENT,
    "solar_cluster_curtailment": _CLUSTER_CURTAILMENT,
    "wind_plant_curtailment": _PLANT_CURTAILMENT
    | {"val_ventoverificado": "float", "flg_dadoventoinvalido": "int"},
    "solar_plant_curtailment": _PLANT_CURTAILMENT
    | {"val_irradianciaverificado": "float", "flg_dadoirradianciainvalido": "int"},
}


class SchemaChangedError(Exception):
    pass


@dataclass(frozen=True)
class ColumnProblem:
    column: str
    problem: str

    def __str__(self) -> str:
        return f"{self.column}: {self.problem}"


def find_schema_problems(raw: pl.DataFrame, schema: dict[str, str]) -> list[ColumnProblem]:
    problems = [ColumnProblem(column, "missing") for column in schema if column not in raw.columns]
    typed = {
        column: kind for column, kind in schema.items() if kind in PARSERS and column in raw.columns
    }
    if not typed or raw.height == 0:
        return problems

    counts = raw.select(
        *[clean_text(column).is_not_null().sum().alias(f"{column}:filled") for column in typed],
        *[
            (clean_text(column).is_not_null() & PARSERS[kind](column).is_null())
            .sum()
            .alias(f"{column}:bad")
            for column, kind in typed.items()
        ],
    ).row(0, named=True)
    for column, kind in typed.items():
        filled, bad = counts[f"{column}:filled"], counts[f"{column}:bad"]
        if filled and bad / filled > MAX_UNPARSEABLE_SHARE:
            sample = (
                raw.filter(clean_text(column).is_not_null() & PARSERS[kind](column).is_null())
                .get_column(column)
                .head(3)
                .to_list()
            )
            problems.append(
                ColumnProblem(
                    column,
                    f"{bad}/{filled} values ({bad / filled:.1%}) are not {kind}, e.g. {sample}",
                )
            )
    return problems


def check_schema(source_name: str, raw: pl.DataFrame) -> None:
    problems = find_schema_problems(raw, SCHEMAS[source_name])
    if problems:
        details = "; ".join(str(problem) for problem in problems)
        raise SchemaChangedError(f"{source_name}: raw file layout changed: {details}")
