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

# dispatch reason -> (programmed column, verified column) of the ONS thermal dispatch file.
# Some reasons overlap (merit order contains its above-inflexibility part, and so on).
THERMAL_REASON_COLUMNS: dict[str, tuple[str, str]] = {
    "total": ("val_proggeracao", "val_verifgeracao"),
    "merit_order": ("val_progordemmerito", "val_verifordemmerito"),
    "merit_order_above_inflexibility": (
        "val_progordemdemeritoacimadainflex",
        "val_verifordemdemeritoacimadainflex",
    ),
    "inflexibility": ("val_proginflexibilidade", "val_verifinflexibilidade"),
    "inflexibility_in_merit": ("val_proginflexembutmerito", "val_verifinflexembutmerito"),
    "inflexibility_pure": ("val_proginflexpura", "val_verifinflexpura"),
    "electrical_reason": ("val_prograzaoeletrica", "val_verifrazaoeletrica"),
    "energy_security": ("val_proggarantiaenergetica", "val_verifgarantiaenergetica"),
    "gfom": ("val_proggfom", "val_verifgfom"),
    "loss_replacement": ("val_progreposicaoperdas", "val_verifreposicaoperdas"),
    "export": ("val_progexportacao", "val_verifexportacao"),
    "power_reserve": ("val_progreservapotencia", "val_verifreservapotencia"),
    "gsub": ("val_proggsub", "val_verifgsub"),
    "unit_commitment": ("val_progunitcommitment", "val_verifunitcommitment"),
    "constrained_off": ("val_progconstrainedoff", "val_verifconstrainedoff"),
}

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
    "energy_balance": {
        "id_subsistema": "text",
        "nom_subsistema": "text",
        "din_instante": "timestamp",
        "val_gerhidraulica": "float",
        "val_gertermica": "float",
        "val_gereolica": "float",
        "val_gersolar": "float",
        "val_carga": "float",
        "val_intercambio": "float",
    },
    "national_interchange": {
        "din_instante": "timestamp",
        "id_subsistema_origem": "text",
        "id_subsistema_destino": "text",
        "val_intercambiomwmed": "float",
        "val_intercambioprogmwmed": "float",
    },
    "international_interchange": {
        "din_instante": "timestamp",
        "nom_paisdestino": "text",
        "val_intercambiomwmed": "float",
        "val_intercambioprogmwmed": "float",
    },
    "thermal_dispatch": {
        "din_instante": "timestamp",
        "id_subsistema": "text",
        "nom_usina": "text",
        "cod_usinaplanejamento": "text",
        "ceg": "text",
        "nom_combustivel": "text",
        **{column: "float" for pair in THERMAL_REASON_COLUMNS.values() for column in pair},
    },
    "reservoirs": {
        "res_id": "text",
        "nom_reservatorio": "text",
        "tip_reservatorio": "text",
        "cod_resplanejamento": "text",
        "cod_posto": "text",
        "nom_usina": "text",
        "ceg": "text",
        "id_subsistema": "text",
        "nom_bacia": "text",
        "nom_rio": "text",
        "nom_ree": "text",
        "dat_entrada": "date",
        "val_cotamaxima": "float",
        "val_cotaminima": "float",
        "val_volmax": "float",
        "val_volmin": "float",
        "val_volutiltot": "float",
        "val_latitude": "float",
        "val_longitude": "float",
    },
    "subsystem_storage": {
        "id_subsistema": "text",
        "ear_data": "date",
        "ear_max_subsistema": "float",
        "ear_verif_subsistema_mwmes": "float",
        "ear_verif_subsistema_percentual": "float",
    },
    "subsystem_inflow": {
        "id_subsistema": "text",
        "ena_data": "date",
        "ena_bruta_regiao_mwmed": "float",
        "ena_bruta_regiao_percentualmlt": "float",
        "ena_armazenavel_regiao_mwmed": "float",
        "ena_armazenavel_regiao_percentualmlt": "float",
    },
    "reservoir_storage": {
        "cod_resplanejamento": "text",
        "nom_reservatorio": "text",
        "id_subsistema": "text",
        "ear_data": "date",
        "ear_total_mwmes": "float",
        "ear_maxima_total_mwmes": "float",
        "ear_reservatorio_percentual": "float",
    },
    "reservoir_inflow": {
        "cod_resplanejamento": "text",
        "nom_reservatorio": "text",
        "ena_data": "date",
        "ena_bruta_res_mwmed": "float",
        "ena_bruta_res_percentualmlt": "float",
        "ena_armazenavel_res_mwmed": "float",
        "ena_armazenavel_res_percentualmlt": "float",
    },
    "hydro_operations": {
        "id_reservatorio": "text",
        "din_instante": "timestamp",
        "val_nivelmontante": "float",
        "val_niveljusante": "float",
        "val_volumeutilcon": "float",
        "val_vazaoafluente": "float",
        "val_vazaoturbinada": "float",
        "val_vazaovertida": "float",
        "val_vazaodefluente": "float",
        "val_vazaonatural": "float",
    },
    "spilled_turbinable_energy": {
        "id_subsistema": "text",
        "nom_bacia": "text",
        "nom_rio": "text",
        "nom_agente": "text",
        "nom_reservatorio": "text",
        "cod_usina": "text",
        "din_instante": "timestamp",
        "val_energiavertida": "float",
        "val_energiavertidaturbinavel": "float",
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
