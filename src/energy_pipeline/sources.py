from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from energy_pipeline.period import Period

DEFAULT_ONS_BASE_URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset"


class Partitioning(StrEnum):
    YEAR = "year"
    MONTH = "month"
    SNAPSHOT = "snapshot"


class FileFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


@dataclass(frozen=True)
class Source:
    name: str
    ons_dataset: str
    file_pattern: str
    partitioning: Partitioning
    description: str
    file_format: FileFormat = FileFormat.CSV
    available_from: Period | None = None
    depends_on: tuple[str, ...] = ()
    source_type: str | None = None
    # Some datasets are published as one file per year up to a point and per month afterwards.
    yearly_until: Period | None = None
    yearly_file_pattern: str | None = None

    def is_yearly(self, period: Period) -> bool:
        return (
            self.yearly_until is not None
            and self.yearly_file_pattern is not None
            and period <= self.yearly_until
        )

    def formats(self) -> tuple[FileFormat, ...]:
        """The configured format first, then the other one the ONS may publish instead."""
        other = FileFormat.CSV if self.file_format is FileFormat.PARQUET else FileFormat.PARQUET
        return (self.file_format, other)

    def file_name(self, period: Period, file_format: FileFormat | None = None) -> str:
        pattern = self.yearly_file_pattern if self.is_yearly(period) else self.file_pattern
        name = (pattern or self.file_pattern).format(year=period.year, month=period.month)
        if file_format is None or file_format is self.file_format:
            return name
        return f"{name.rsplit('.', 1)[0]}.{file_format}"

    def url(self, base_url: str, period: Period, file_format: FileFormat | None = None) -> str:
        return f"{base_url.rstrip('/')}/{self.ons_dataset}/{self.file_name(period, file_format)}"

    def partition(self, period: Period, snapshot_date: date) -> str:
        if self.partitioning is Partitioning.SNAPSHOT:
            return f"snapshot_date={snapshot_date.isoformat()}"
        if self.partitioning is Partitioning.YEAR:
            return f"year={period.year:04d}"
        return f"year={period.year:04d}/month={period.month:02d}"

    def processed_partition(self, period: Period, snapshot_date: date) -> str:
        if self.partitioning is Partitioning.SNAPSHOT:
            return self.partition(period, snapshot_date)
        return f"year={period.year:04d}/month={period.month:02d}"

    def periods_to_process(self, periods: list[Period]) -> list[Period]:
        if self.partitioning is Partitioning.SNAPSHOT:
            return periods[-1:]
        if self.available_from is None:
            return periods
        return [period for period in periods if period >= self.available_from]


LOAD_CURVE = Source(
    name="load_curve",
    ons_dataset="curva-carga-ho",
    file_pattern="CURVA_CARGA_{year}.csv",
    partitioning=Partitioning.YEAR,
    description="Hourly electricity load (demand) per subsystem, in average MW.",
)

PLANT_GENERATION = Source(
    name="plant_generation",
    ons_dataset="geracao_usina_2_ho",
    file_pattern="GERACAO_USINA-2_{year}_{month:02d}.csv",
    yearly_file_pattern="GERACAO_USINA-2_{year}.csv",
    yearly_until=Period(2021, 12),
    partitioning=Partitioning.MONTH,
    description="Hourly generation per power plant, in average MW (= MWh per hour).",
    available_from=Period(2022, 1),
)

ENERGY_BALANCE = Source(
    name="energy_balance",
    ons_dataset="balanco_energia_subsistema_ho",
    file_pattern="BALANCO_ENERGIA_SUBSISTEMA_{year}.parquet",
    partitioning=Partitioning.YEAR,
    description="Hourly energy balance per subsystem: generation by source, load, interchange.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2000, 1),
)

NATIONAL_INTERCHANGE = Source(
    name="national_interchange",
    ons_dataset="intercambio_nacional_ho",
    file_pattern="INTERCAMBIO_NACIONAL_{year}.parquet",
    partitioning=Partitioning.YEAR,
    description="Hourly power exchanged between subsystems, in average MW (origin to destination).",
    file_format=FileFormat.PARQUET,
    available_from=Period(2000, 1),
)

INTERNATIONAL_INTERCHANGE = Source(
    name="international_interchange",
    ons_dataset="intercambio_internacional_ho",
    file_pattern="INTERCAMBIO_INTERNACIONAL_{year}.parquet",
    partitioning=Partitioning.YEAR,
    description="Hourly power exchanged with neighbouring countries, in average MW.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2000, 1),
)

THERMAL_DISPATCH = Source(
    name="thermal_dispatch",
    ons_dataset="geracao_termica_despacho_2_ho",
    file_pattern="GERACAO_TERMICA_DESPACHO-2_{year}_{month:02d}.parquet",
    yearly_file_pattern="GERACAO_TERMICA_DESPACHO-2_{year}.parquet",
    yearly_until=Period(2021, 12),
    partitioning=Partitioning.MONTH,
    description="Hourly programmed and verified thermal generation per plant and dispatch reason.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2022, 1),
)

GENERATION_CAPACITY = Source(
    name="generation_capacity",
    ons_dataset="capacidade-geracao",
    file_pattern="CAPACIDADE_GERACAO.csv",
    partitioning=Partitioning.SNAPSHOT,
    description="Installed capacity per generating unit, with commissioning dates.",
)


MARGINAL_COST = Source(
    name="marginal_cost",
    ons_dataset="cmo_tm",
    file_pattern="CMO_SEMIHORARIO_{year}.csv",
    partitioning=Partitioning.YEAR,
    description="Semi-hourly marginal operating cost (CMO) per subsystem, in R$/MWh.",
    available_from=Period(2020, 1),
)

WIND_CLUSTER_CURTAILMENT = Source(
    name="wind_cluster_curtailment",
    ons_dataset="restricao_coff_eolica_tm",
    file_pattern="RESTRICAO_COFF_EOLICA_{year}_{month:02d}.parquet",
    partitioning=Partitioning.MONTH,
    description="Semi-hourly curtailment of wind clusters: official GNRa and reasons.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2021, 10),
    depends_on=(MARGINAL_COST.name,),
    source_type="WIND",
)

SOLAR_CLUSTER_CURTAILMENT = Source(
    name="solar_cluster_curtailment",
    ons_dataset="restricao_coff_fotovoltaica_tm",
    file_pattern="RESTRICAO_COFF_FOTOVOLTAICA_{year}_{month:02d}.parquet",
    partitioning=Partitioning.MONTH,
    description="Semi-hourly curtailment of solar clusters: official GNRa and reasons.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2024, 4),
    depends_on=(MARGINAL_COST.name,),
    source_type="SOLAR",
)

WIND_PLANT_CURTAILMENT = Source(
    name="wind_plant_curtailment",
    ons_dataset="restricao_coff_eolica_detail_tm",
    file_pattern="RESTRICAO_COFF_EOLICA_DETAIL_{year}_{month:02d}.parquet",
    partitioning=Partitioning.MONTH,
    description="Semi-hourly estimated vs. verified generation of each wind plant.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2021, 10),
    depends_on=(MARGINAL_COST.name, WIND_CLUSTER_CURTAILMENT.name),
    source_type="WIND",
)

SOLAR_PLANT_CURTAILMENT = Source(
    name="solar_plant_curtailment",
    ons_dataset="restricao_coff_fotovoltaica_detail_tm",
    file_pattern="RESTRICAO_COFF_FOTOVOLTAICA_DETAIL_{year}_{month:02d}.parquet",
    partitioning=Partitioning.MONTH,
    description="Semi-hourly estimated vs. verified generation of each solar plant.",
    file_format=FileFormat.PARQUET,
    available_from=Period(2024, 4),
    depends_on=(MARGINAL_COST.name, SOLAR_CLUSTER_CURTAILMENT.name),
    source_type="SOLAR",
)


def _registry(*sources: Source) -> dict[str, Source]:
    return {source.name: source for source in sources}


GRID_SOURCES = _registry(LOAD_CURVE, PLANT_GENERATION, GENERATION_CAPACITY)
SYSTEM_SOURCES = _registry(
    ENERGY_BALANCE,
    LOAD_CURVE,
    PLANT_GENERATION,
    GENERATION_CAPACITY,
    NATIONAL_INTERCHANGE,
    INTERNATIONAL_INTERCHANGE,
    THERMAL_DISPATCH,
)
CURTAILMENT_SOURCES = _registry(
    MARGINAL_COST,
    GENERATION_CAPACITY,
    WIND_CLUSTER_CURTAILMENT,
    SOLAR_CLUSTER_CURTAILMENT,
    WIND_PLANT_CURTAILMENT,
    SOLAR_PLANT_CURTAILMENT,
)
PIPELINES: dict[str, dict[str, Source]] = {
    "grid": GRID_SOURCES,
    "curtailment": CURTAILMENT_SOURCES,
    "system": SYSTEM_SOURCES,
}
SOURCES: dict[str, Source] = {**GRID_SOURCES, **CURTAILMENT_SOURCES, **SYSTEM_SOURCES}
