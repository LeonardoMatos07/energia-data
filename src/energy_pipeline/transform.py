from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from energy_pipeline.period import Period

SOURCE_TYPES = {
    "HIDROELETRICA": "HYDRO",
    "HIDRAULICA": "HYDRO",
    "EOLIELETRICA": "WIND",
    "EOLICA": "WIND",
    "FOTOVOLTAICA": "SOLAR",
    "SOLAR": "SOLAR",
    "TERMICA": "THERMAL",
    "TERMELETRICA": "THERMAL",
    "NUCLEAR": "NUCLEAR",
}
RENEWABLE_SOURCE_TYPES = ["HYDRO", "WIND", "SOLAR"]
RENEWABLE_FUEL_PATTERN = "BIOMASSA|BAGACO|BIOGAS"

_NULL_MARKERS = ["", "-"]
_TIMESTAMP_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]


@dataclass(frozen=True)
class TransformResult:
    valid: pl.DataFrame
    rejected: pl.DataFrame
    duplicates_removed: int
    missing_measurements: int = 0


def read_raw_csv(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, separator=";", infer_schema_length=0, encoding="utf8-lossy")
    return df.rename({column: column.lstrip("﻿").strip().lower() for column in df.columns})


def read_raw_parquet(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    df = df.with_columns(
        pl.col(pl.Datetime).dt.strftime(_TIMESTAMP_FORMATS[0]),
        pl.col(pl.Date).dt.strftime("%Y-%m-%d"),
    ).cast(pl.String)
    return df.rename({column: column.strip().lower() for column in df.columns})


def read_raw(path: Path, file_format: str) -> pl.DataFrame:
    return read_raw_parquet(path) if file_format == "parquet" else read_raw_csv(path)


def clean_text(column: str) -> pl.Expr:
    value = pl.col(column).str.strip_chars().str.replace_all(r"\s+", " ")
    return pl.when(value.is_in(_NULL_MARKERS)).then(None).otherwise(value)


def parse_code(column: str) -> pl.Expr:
    return clean_text(column).str.to_uppercase()


def parse_float(column: str) -> pl.Expr:
    return clean_text(column).str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)


def parse_int(column: str) -> pl.Expr:
    return parse_float(column).cast(pl.Int32, strict=False)


def parse_timestamp(column: str) -> pl.Expr:
    value = clean_text(column)
    candidates = [
        value.str.to_datetime(fmt, strict=False, time_unit="us") for fmt in _TIMESTAMP_FORMATS
    ]
    return pl.coalesce(candidates)


def parse_date(column: str) -> pl.Expr:
    return clean_text(column).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)


def normalize_label(value: str | None) -> str | None:
    if value is None:
        return None
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return " ".join(ascii_value.upper().split()) or None


def normalize_labels(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    expressions = []
    for column in columns:
        mapping = {value: normalize_label(value) for value in df[column].unique().drop_nulls()}
        if not mapping:
            continue
        expressions.append(
            pl.col(column).replace_strict(mapping, default=None, return_dtype=pl.String)
        )
    return df.with_columns(expressions)


def with_source_type(df: pl.DataFrame, column: str) -> pl.DataFrame:
    return df.with_columns(
        pl.col(column)
        .replace_strict(SOURCE_TYPES, default="OTHER", return_dtype=pl.String)
        .fill_null("OTHER")
        .alias("source_type")
    ).drop(column)


def is_renewable() -> pl.Expr:
    renewable_fuel = pl.col("fuel_type").fill_null("").str.contains(RENEWABLE_FUEL_PATTERN)
    return pl.col("source_type").is_in(RENEWABLE_SOURCE_TYPES) | renewable_fuel


def split_rejected(df: pl.DataFrame, required: list[str]) -> tuple[pl.DataFrame, pl.DataFrame]:
    has_nulls = pl.any_horizontal([pl.col(column).is_null() for column in required])
    return df.filter(~has_nulls), df.filter(has_nulls)


def deduplicate(df: pl.DataFrame, keys: list[str]) -> tuple[pl.DataFrame, int]:
    unique = df.unique(subset=keys, keep="last", maintain_order=True)
    return unique, df.height - unique.height


def in_period_or_unparsed(column: str, period: Period) -> pl.Expr:
    return pl.col(column).is_null() | (
        (pl.col(column) >= period.start) & (pl.col(column) < period.end)
    )


def transform_load_curve(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_code("id_subsistema").alias("subsystem_id"),
        clean_text("nom_subsistema").alias("subsystem_name"),
        parse_timestamp("din_instante").alias("measured_at"),
        parse_float("val_cargaenergiahomwmed").alias("load_mw_avg"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = normalize_labels(df, ["subsystem_name"])

    valid, rejected = split_rejected(df, ["subsystem_id", "measured_at", "load_mw_avg"])
    valid, duplicates = deduplicate(valid, ["subsystem_id", "measured_at"])
    return TransformResult(valid.sort("subsystem_id", "measured_at"), rejected, duplicates)


def transform_energy_balance(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_code("id_subsistema").alias("subsystem_id"),
        clean_text("nom_subsistema").alias("subsystem_name"),
        parse_timestamp("din_instante").alias("measured_at"),
        parse_float("val_gerhidraulica").alias("hydro_mw_avg"),
        parse_float("val_gertermica").alias("thermal_mw_avg"),
        parse_float("val_gereolica").alias("wind_mw_avg"),
        parse_float("val_gersolar").alias("solar_mw_avg"),
        parse_float("val_carga").alias("load_mw_avg"),
        parse_float("val_intercambio").alias("net_interchange_mw_avg"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = normalize_labels(df, ["subsystem_name"])

    valid, rejected = split_rejected(df, ["subsystem_id", "measured_at"])
    valid, duplicates = deduplicate(valid, ["subsystem_id", "measured_at"])
    return TransformResult(valid.sort("subsystem_id", "measured_at"), rejected, duplicates)


def transform_national_interchange(raw: pl.DataFrame, period: Period) -> TransformResult:
    """The ONS lists each link in the direction of the flow of that hour, with a magnitude. The
    silver layer gives every link one fixed direction (alphabetical: N to NE, NE to SE, ...) and a
    signed flow: positive means origin to destination, negative the opposite."""
    origin = parse_code("id_subsistema_origem")
    destination = parse_code("id_subsistema_destino")
    forward = origin < destination
    sign = pl.when(forward).then(1.0).otherwise(-1.0)
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        pl.when(forward).then(origin).otherwise(destination).alias("origin"),
        pl.when(forward).then(destination).otherwise(origin).alias("destination"),
        (parse_float("val_intercambiomwmed") * sign).alias("flow_mw_avg"),
        (parse_float("val_intercambioprogmwmed") * sign).alias("scheduled_mw_avg"),
    ).filter(in_period_or_unparsed("measured_at", period))

    valid, rejected = split_rejected(df, ["measured_at", "origin", "destination"])
    valid, duplicates = deduplicate(valid, ["origin", "destination", "measured_at"])
    return TransformResult(valid.sort("origin", "destination", "measured_at"), rejected, duplicates)


def transform_international_interchange(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        clean_text("nom_paisdestino").alias("country"),
        parse_float("val_intercambiomwmed").alias("flow_mw_avg"),
        parse_float("val_intercambioprogmwmed").alias("scheduled_mw_avg"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = normalize_labels(df, ["country"]).rename({"country": "destination"})
    df = df.with_columns(pl.lit("BR").alias("origin"))

    valid, rejected = split_rejected(df, ["measured_at", "destination"])
    valid, duplicates = deduplicate(valid, ["origin", "destination", "measured_at"])
    columns = ["measured_at", "origin", "destination", "flow_mw_avg", "scheduled_mw_avg"]
    return TransformResult(
        valid.select(columns).sort("destination", "measured_at"),
        rejected.select(columns),
        duplicates,
    )


def transform_thermal_dispatch(raw: pl.DataFrame, period: Period) -> TransformResult:
    from energy_pipeline.schema import THERMAL_REASON_COLUMNS

    reasons = [
        parse_float(column).alias(f"{kind}_{reason}_mwh")
        for reason, columns in THERMAL_REASON_COLUMNS.items()
        for kind, column in zip(("programmed", "verified"), columns, strict=True)
    ]
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        parse_code("id_subsistema").alias("subsystem_id"),
        clean_text("nom_usina").alias("plant_name"),
        parse_code("cod_usinaplanejamento").alias("plant_code"),
        parse_code("ceg").alias("ceg"),
        clean_text("nom_combustivel").alias("fuel_type"),
        *reasons,
    ).filter(in_period_or_unparsed("measured_at", period))
    df = normalize_labels(df, ["fuel_type"])
    # new plants may not have a planning code yet: fall back to the CEG, then to the name
    df = df.with_columns(
        pl.coalesce(
            pl.col("plant_code"),
            pl.lit("CEG:") + pl.col("ceg"),
            pl.lit("NAME:") + pl.col("plant_name").str.to_uppercase(),
        ).alias("plant_code")
    )

    valid, rejected = split_rejected(
        df, ["measured_at", "subsystem_id", "plant_name", "plant_code", "verified_total_mwh"]
    )
    valid, duplicates = deduplicate(valid, ["plant_code", "measured_at"])
    return TransformResult(valid.sort("plant_code", "measured_at"), rejected, duplicates)


def in_period_days_or_unparsed(column: str, period: Period) -> pl.Expr:
    return pl.col(column).is_null() | (
        (pl.col(column) >= period.start.date()) & (pl.col(column) < period.end.date())
    )


def _daily(
    raw: pl.DataFrame, period: Period, keys: list[str], columns: list[pl.Expr]
) -> TransformResult:
    df = raw.select(*columns).filter(in_period_days_or_unparsed("day", period))
    valid, rejected = split_rejected(df, ["day", *keys])
    valid, duplicates = deduplicate(valid, [*keys, "day"])
    return TransformResult(valid.sort(*keys, "day"), rejected, duplicates)


def transform_reservoirs(raw: pl.DataFrame, snapshot_date: date) -> TransformResult:
    df = raw.select(
        parse_code("res_id").alias("reservoir_id"),
        clean_text("nom_reservatorio").alias("name"),
        clean_text("tip_reservatorio").alias("reservoir_type"),
        parse_int("cod_resplanejamento").alias("planning_code"),
        parse_int("cod_posto").alias("station_code"),
        clean_text("nom_usina").alias("plant_name"),
        parse_code("ceg").alias("ceg"),
        parse_code("id_subsistema").alias("subsystem_id"),
        clean_text("nom_ree").alias("ree"),
        clean_text("nom_bacia").alias("basin"),
        clean_text("nom_rio").alias("river"),
        parse_date("dat_entrada").alias("commissioned_on"),
        parse_float("val_cotamaxima").alias("max_level_m"),
        parse_float("val_cotaminima").alias("min_level_m"),
        parse_float("val_volmax").alias("max_volume_hm3"),
        parse_float("val_volmin").alias("min_volume_hm3"),
        parse_float("val_volutiltot").alias("useful_volume_hm3"),
        parse_float("val_latitude").alias("latitude"),
        parse_float("val_longitude").alias("longitude"),
    ).with_columns(pl.lit(snapshot_date).alias("snapshot_date"))
    valid, rejected = split_rejected(df, ["reservoir_id", "name"])
    valid, duplicates = deduplicate(valid, ["reservoir_id"])
    return TransformResult(valid.sort("reservoir_id"), rejected, duplicates)


def transform_subsystem_storage(raw: pl.DataFrame, period: Period) -> TransformResult:
    return _daily(
        raw,
        period,
        ["subsystem_id"],
        [
            parse_code("id_subsistema").alias("subsystem_id"),
            parse_date("ear_data").alias("day"),
            parse_float("ear_max_subsistema").alias("storage_max_mw_month"),
            parse_float("ear_verif_subsistema_mwmes").alias("storage_mw_month"),
            parse_float("ear_verif_subsistema_percentual").alias("storage_pct"),
        ],
    )


def transform_subsystem_inflow(raw: pl.DataFrame, period: Period) -> TransformResult:
    return _daily(
        raw,
        period,
        ["subsystem_id"],
        [
            parse_code("id_subsistema").alias("subsystem_id"),
            parse_date("ena_data").alias("day"),
            parse_float("ena_bruta_regiao_mwmed").alias("ena_gross_mw_avg"),
            parse_float("ena_bruta_regiao_percentualmlt").alias("ena_gross_pct_mlt"),
            parse_float("ena_armazenavel_regiao_mwmed").alias("ena_storable_mw_avg"),
            parse_float("ena_armazenavel_regiao_percentualmlt").alias("ena_storable_pct_mlt"),
        ],
    )


def transform_reservoir_storage(raw: pl.DataFrame, period: Period) -> TransformResult:
    return _daily(
        raw,
        period,
        ["reservoir_name"],
        [
            parse_int("cod_resplanejamento").alias("planning_code"),
            clean_text("nom_reservatorio").alias("reservoir_name"),
            parse_code("id_subsistema").alias("subsystem_id"),
            parse_date("ear_data").alias("day"),
            parse_float("ear_total_mwmes").alias("storage_mw_month"),
            parse_float("ear_maxima_total_mwmes").alias("storage_max_mw_month"),
            parse_float("ear_reservatorio_percentual").alias("storage_pct"),
        ],
    )


def transform_reservoir_inflow(raw: pl.DataFrame, period: Period) -> TransformResult:
    return _daily(
        raw,
        period,
        ["reservoir_name"],
        [
            parse_int("cod_resplanejamento").alias("planning_code"),
            clean_text("nom_reservatorio").alias("reservoir_name"),
            parse_date("ena_data").alias("day"),
            parse_float("ena_bruta_res_mwmed").alias("ena_gross_mw_avg"),
            parse_float("ena_bruta_res_percentualmlt").alias("ena_gross_pct_mlt"),
            parse_float("ena_armazenavel_res_mwmed").alias("ena_storable_mw_avg"),
            parse_float("ena_armazenavel_res_percentualmlt").alias("ena_storable_pct_mlt"),
        ],
    )


def transform_hydro_operations(raw: pl.DataFrame, period: Period) -> TransformResult:
    return _daily(
        raw,
        period,
        ["reservoir_id"],
        [
            parse_code("id_reservatorio").alias("reservoir_id"),
            parse_timestamp("din_instante").dt.date().alias("day"),
            parse_float("val_volumeutilcon").alias("useful_volume_pct"),
            parse_float("val_vazaoafluente").alias("inflow_m3s"),
            parse_float("val_vazaonatural").alias("natural_inflow_m3s"),
            parse_float("val_vazaodefluente").alias("outflow_m3s"),
            parse_float("val_vazaoturbinada").alias("turbined_m3s"),
            parse_float("val_vazaovertida").alias("spilled_m3s"),
            parse_float("val_nivelmontante").alias("upstream_level_m"),
            parse_float("val_niveljusante").alias("downstream_level_m"),
        ],
    )


MAX_SPILLED_ENERGY_MWH_PER_HOUR = 1_000_000


def transform_spilled_turbinable_energy(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        parse_code("cod_usina").alias("plant_code"),
        clean_text("nom_reservatorio").alias("plant_name"),
        parse_code("id_subsistema").alias("subsystem_id"),
        clean_text("nom_bacia").alias("basin"),
        clean_text("nom_rio").alias("river"),
        clean_text("nom_agente").alias("agent"),
        parse_float("val_energiavertida").alias("spilled_energy_mwh"),
        parse_float("val_energiavertidaturbinavel").alias("spilled_turbinable_energy_mwh"),
    ).filter(in_period_or_unparsed("measured_at", period))
    # an hour of spill cannot be worth more than 1 TWh: such rows are data errors, quarantined
    absurd = (pl.col("spilled_energy_mwh").abs() > MAX_SPILLED_ENERGY_MWH_PER_HOUR) | (
        pl.col("spilled_turbinable_energy_mwh").abs() > MAX_SPILLED_ENERGY_MWH_PER_HOUR
    )
    valid, rejected = split_rejected(
        df.with_columns(
            pl.when(absurd).then(None).otherwise(pl.col("plant_code")).alias("plant_code")
        ),
        ["measured_at", "plant_code", "plant_name", "spilled_turbinable_energy_mwh"],
    )
    valid, duplicates = deduplicate(valid, ["plant_code", "measured_at"])
    return TransformResult(valid.sort("plant_code", "measured_at"), rejected, duplicates)


def transform_transmission_lines(raw: pl.DataFrame, snapshot_date: date) -> TransformResult:
    df = raw.select(
        parse_code("cod_equipamento").alias("equipment_code"),
        clean_text("nom_linhadetransmissao").alias("name"),
        parse_float("val_niveltensao_kv").alias("voltage_kv"),
        clean_text("nom_tipolinha").alias("line_type"),
        clean_text("nom_tipoderede").alias("network_type"),
        clean_text("nom_agenteproprietario").alias("owner_agent"),
        clean_text("nom_subestacao_de").alias("from_substation"),
        clean_text("nom_subestacao_para").alias("to_substation"),
        parse_code("id_subsistema_terminalde").alias("from_subsystem"),
        parse_code("id_subsistema_terminalpara").alias("to_subsystem"),
        parse_code("id_estado_terminalde").alias("from_state"),
        parse_code("id_estado_terminalpara").alias("to_state"),
        parse_float("val_comprimento").alias("length_km"),
        parse_float("val_resistencia").alias("positive_seq_resistance"),
        parse_float("val_reatancia").alias("positive_seq_reactance"),
        parse_float("val_shunt").alias("positive_seq_shunt"),
        parse_float("val_capacoperlongasemlimit").alias("capacity_long_no_limit_a"),
        parse_float("val_capacoperlongacomlimit").alias("capacity_long_with_limit_a"),
        parse_float("val_capacopercurtasemlimit").alias("capacity_short_no_limit_a"),
        parse_float("val_capacopercurtacomlimit").alias("capacity_short_with_limit_a"),
        parse_date("dat_entradaoperacao").alias("commissioned_on"),
        parse_date("dat_desativacao").alias("decommissioned_on"),
        parse_date("dat_prevista").alias("planned_on"),
        parse_int("num_barra_de").alias("from_bus"),
        parse_int("num_barra_para").alias("to_bus"),
    ).with_columns(pl.lit(snapshot_date).alias("snapshot_date"))
    valid, rejected = split_rejected(df, ["equipment_code"])
    valid, duplicates = deduplicate(valid, ["equipment_code"])
    return TransformResult(valid.sort("equipment_code"), rejected, duplicates)


def transform_substations(raw: pl.DataFrame, snapshot_date: date) -> TransformResult:
    df = raw.select(
        parse_code("id_estacao").alias("station_id"),
        parse_code("id_subestacao").alias("substation_id"),
        clean_text("nom_subestacao").alias("name"),
        parse_float("val_niveltensao").alias("voltage_kv"),
        parse_code("id_subsistema").alias("subsystem"),
        parse_code("id_estado").alias("state"),
        clean_text("nom_agente_principal").alias("main_agent"),
        parse_int("num_barra").alias("bus_number"),
        parse_float("val_latitude").alias("latitude"),
        parse_float("val_longitude").alias("longitude"),
    ).with_columns(pl.lit(snapshot_date).alias("snapshot_date"))
    valid, rejected = split_rejected(df, ["station_id"])
    valid, duplicates = deduplicate(valid, ["station_id"])
    return TransformResult(valid.sort("station_id"), rejected, duplicates)


def transform_transformation_capacity(raw: pl.DataFrame, snapshot_date: date) -> TransformResult:
    df = raw.select(
        parse_code("cod_equipamento").alias("equipment_code"),
        clean_text("nom_transformador").alias("name"),
        clean_text("nom_tipotransformador").alias("transformer_type"),
        clean_text("nom_subestacao").alias("substation"),
        clean_text("nom_agenteproprietario").alias("owner_agent"),
        parse_code("id_subsistema").alias("subsystem"),
        parse_code("id_estado").alias("state"),
        parse_float("val_tensaoprimario_kv").alias("primary_kv"),
        parse_float("val_tensaosecundario_kv").alias("secondary_kv"),
        parse_float("val_tensaoterciario_kv").alias("tertiary_kv"),
        parse_float("val_potencianominal_mva").alias("rated_mva"),
        clean_text("nom_tipoderede").alias("network_type"),
        parse_date("dat_entradaoperacao").alias("commissioned_on"),
        parse_date("dat_desativacao").alias("decommissioned_on"),
        parse_int("num_barra_primario").alias("primary_bus"),
        parse_int("num_barra_secundario").alias("secondary_bus"),
    ).with_columns(pl.lit(snapshot_date).alias("snapshot_date"))
    valid, rejected = split_rejected(df, ["equipment_code"])
    valid, duplicates = deduplicate(valid, ["equipment_code"])
    return TransformResult(valid.sort("equipment_code"), rejected, duplicates)


def transform_plant_generation(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        parse_code("id_ons").alias("ons_plant_id"),
        clean_text("nom_usina").alias("plant_name"),
        parse_code("ceg").alias("ceg"),
        clean_text("nom_tipousina").alias("plant_type"),
        clean_text("nom_tipocombustivel").alias("fuel_type"),
        clean_text("cod_modalidadeoperacao").alias("operation_mode"),
        parse_code("id_subsistema").alias("subsystem_id"),
        parse_code("id_estado").alias("state_id"),
        parse_float("val_geracao").alias("generation_mwh"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = normalize_labels(df, ["plant_type", "fuel_type"])
    df = with_source_type(df, "plant_type")

    df = df.with_columns(
        pl.coalesce(
            pl.col("ons_plant_id"),
            pl.lit("NAME:") + pl.col("plant_name").str.to_uppercase(),
        ).alias("plant_id"),
        is_renewable().alias("is_renewable"),
    ).drop("ons_plant_id")

    valid, rejected = split_rejected(df, ["measured_at", "plant_id", "plant_name", "subsystem_id"])
    missing = valid.filter(pl.col("generation_mwh").is_null()).height
    valid = valid.filter(pl.col("generation_mwh").is_not_null())
    valid, duplicates = deduplicate(valid, ["plant_id", "measured_at"])
    return TransformResult(
        valid.sort("plant_id", "measured_at"), rejected, duplicates, missing_measurements=missing
    )


def transform_generation_capacity(raw: pl.DataFrame, snapshot_date: date) -> TransformResult:
    df = raw.select(
        parse_code("cod_equipamento").alias("unit_id"),
        clean_text("nom_unidadegeradora").alias("unit_name"),
        parse_code("num_unidadegeradora").alias("unit_number"),
        clean_text("nom_usina").alias("plant_name"),
        parse_code("ceg").alias("ceg"),
        clean_text("nom_tipousina").alias("plant_type"),
        clean_text("nom_combustivel").alias("fuel_type"),
        clean_text("nom_modalidadeoperacao").alias("operation_mode"),
        clean_text("nom_agenteproprietario").alias("owner_agent"),
        parse_code("id_subsistema").alias("subsystem_id"),
        parse_code("id_estado").alias("state_id"),
        parse_date("dat_entradateste").alias("test_start_date"),
        parse_date("dat_entradaoperacao").alias("operation_start_date"),
        parse_date("dat_desativacao").alias("decommission_date"),
        parse_float("val_potenciaefetiva").alias("capacity_mw"),
    )
    df = normalize_labels(df, ["plant_type", "fuel_type"])
    df = with_source_type(df, "plant_type").with_columns(
        is_renewable().alias("is_renewable"),
        pl.lit(snapshot_date).alias("snapshot_date"),
    )

    valid, rejected = split_rejected(df, ["unit_id", "capacity_mw"])
    valid, duplicates = deduplicate(valid, ["unit_id"])
    return TransformResult(valid.sort("unit_id"), rejected, duplicates)


RESOURCE_COLUMNS = {
    "WIND": ("val_ventoverificado", "flg_dadoventoinvalido"),
    "SOLAR": ("val_irradianciaverificado", "flg_dadoirradianciainvalido"),
}


def transform_marginal_cost(raw: pl.DataFrame, period: Period) -> TransformResult:
    df = raw.select(
        parse_code("id_subsistema").alias("subsystem_id"),
        parse_timestamp("din_instante").alias("measured_at"),
        parse_float("val_cmo").alias("cmo_brl_mwh"),
    ).filter(in_period_or_unparsed("measured_at", period))

    valid, rejected = split_rejected(df, ["subsystem_id", "measured_at", "cmo_brl_mwh"])
    valid, duplicates = deduplicate(valid, ["subsystem_id", "measured_at"])
    return TransformResult(valid.sort("subsystem_id", "measured_at"), rejected, duplicates)


def transform_cluster_curtailment(
    raw: pl.DataFrame, period: Period, source_type: str
) -> TransformResult:
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        parse_code("id_ons").alias("cluster_id"),
        clean_text("nom_usina").alias("cluster_name"),
        parse_code("id_subsistema").alias("subsystem_id"),
        parse_code("id_estado").alias("state_id"),
        parse_code("id_pontoconexao").alias("connection_point_id"),
        clean_text("nom_pontoconexao").alias("connection_point_name"),
        clean_text("nom_agenteoperador").alias("operator_agent"),
        parse_float("val_geracao").alias("generation_mw_avg"),
        parse_float("val_geracaolimitada").alias("limit_mw"),
        parse_float("val_disponibilidade").alias("availability_mw_avg"),
        parse_float("val_geracaoreferencia").alias("reference_mw_avg"),
        parse_float("val_geracaoreferenciafinal").alias("reference_final_mw_avg"),
        parse_float("val_geracaonaorealizadaapurada").alias("gnra_mw_avg"),
        parse_code("cod_razaorestricao").alias("reason_code"),
        parse_code("cod_origemrestricao").alias("origin_code"),
        clean_text("dsc_restricao").alias("restriction_detail"),
        parse_int("num_minutos_rel").alias("minutes_rel"),
        parse_int("num_minutos_cnf").alias("minutes_cnf"),
        parse_int("num_minutos_ene").alias("minutes_ene"),
        parse_int("num_minutos_restricao").alias("minutes_restricted"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = df.with_columns(pl.lit(source_type).alias("source_type"))

    valid, rejected = split_rejected(df, ["measured_at", "cluster_id", "subsystem_id"])
    valid, duplicates = deduplicate(valid, ["cluster_id", "measured_at"])
    return TransformResult(valid.sort("cluster_id", "measured_at"), rejected, duplicates)


def transform_plant_curtailment(
    raw: pl.DataFrame, period: Period, source_type: str
) -> TransformResult:
    resource_column, invalid_flag_column = RESOURCE_COLUMNS[source_type]
    cluster_id = parse_code("id_ons_conjuntousina")
    df = raw.select(
        parse_timestamp("din_instante").alias("measured_at"),
        parse_code("id_ons").alias("plant_id"),
        clean_text("nom_usina").alias("plant_name"),
        parse_code("ceg").alias("ceg"),
        pl.coalesce(cluster_id, parse_code("id_ons")).alias("cluster_id"),
        pl.coalesce(clean_text("nom_conjuntousina"), clean_text("nom_usina")).alias("cluster_name"),
        cluster_id.is_not_null().alias("_has_cluster"),
        parse_code("id_subsistema").alias("subsystem_id"),
        parse_code("id_estado").alias("state_id"),
        clean_text("nom_modalidadeoperacao").alias("operation_mode"),
        parse_float(resource_column).alias("resource_value"),
        (parse_int(invalid_flag_column) == 1).alias("resource_invalid"),
        parse_float("val_geracaoestimada").alias("estimated_mw_avg"),
        parse_float("val_geracaoverificada").alias("verified_mw_avg"),
        parse_float("val_geracaoreferenciafinal").alias("reference_final_mw_avg"),
        clean_text("nom_origemgeracaoreferencia").alias("reference_origin"),
        (parse_int("flg_geracaorestrita") == 1).fill_null(False).alias("is_restricted"),
    ).filter(in_period_or_unparsed("measured_at", period))
    df = df.with_columns(pl.lit(source_type).alias("source_type"))

    valid, rejected = split_rejected(df, ["measured_at", "plant_id", "cluster_id", "subsystem_id"])
    valid = valid.sort("_has_cluster", maintain_order=True)
    valid, duplicates = deduplicate(valid, ["plant_id", "measured_at"])
    return TransformResult(
        valid.drop("_has_cluster").sort("plant_id", "measured_at"),
        rejected.drop("_has_cluster"),
        duplicates,
    )
