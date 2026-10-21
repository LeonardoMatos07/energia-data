from datetime import date, datetime

import polars as pl
import pytest

from energy_pipeline.period import Period
from energy_pipeline.transform import (
    normalize_label,
    transform_generation_capacity,
    transform_load_curve,
    transform_plant_generation,
)

JANUARY_2024 = Period(2024, 1)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("HIDROELÉTRICA", "HIDROELETRICA"),
        ("  Óleo   Diesel ", "OLEO DIESEL"),
        ("   ", None),
        (None, None),
    ],
)
def test_normalize_label(value, expected):
    assert normalize_label(value) == expected


class TestLoadCurve:
    def test_types_and_cleaning(self, raw_load_curve):
        result = transform_load_curve(raw_load_curve, JANUARY_2024)

        assert result.valid.schema == {
            "subsystem_id": pl.String,
            "subsystem_name": pl.String,
            "measured_at": pl.Datetime("us"),
            "load_mw_avg": pl.Float64,
        }
        south = result.valid.filter(pl.col("subsystem_id") == "S").row(0, named=True)
        assert south["subsystem_name"] == "SUL"
        assert south["load_mw_avg"] == pytest.approx(9966.972)

    def test_keeps_only_the_period(self, raw_load_curve):
        result = transform_load_curve(raw_load_curve, JANUARY_2024)

        assert result.valid["measured_at"].max() < datetime(2024, 2, 1)

    def test_keeps_last_revision_of_duplicates(self, raw_load_curve):
        result = transform_load_curve(raw_load_curve, JANUARY_2024)

        southeast = result.valid.filter(pl.col("subsystem_id") == "SE")
        assert result.duplicates_removed == 1
        assert southeast["load_mw_avg"].to_list() == [38300.0]

    def test_rejects_unparseable_rows(self, raw_load_curve):
        result = transform_load_curve(raw_load_curve, JANUARY_2024)

        assert result.rejected.height == 2
        assert result.valid.height == 3


class TestPlantGeneration:
    def test_maps_source_types_and_renewables(self, raw_plant_generation):
        valid = transform_plant_generation(raw_plant_generation, JANUARY_2024).valid
        by_plant = {row["plant_id"]: row for row in valid.iter_rows(named=True)}

        assert by_plant["AMBA"]["source_type"] == "HYDRO"
        assert by_plant["PQU_ACRAC_GD"]["source_type"] == "SOLAR"
        assert by_plant["PECANA"]["source_type"] == "THERMAL"
        assert by_plant["PECANA"]["is_renewable"] is True
        assert by_plant["RJGAS"]["is_renewable"] is False

    def test_parses_scientific_notation_and_placeholders(self, raw_plant_generation):
        valid = transform_plant_generation(raw_plant_generation, JANUARY_2024).valid
        solar = valid.filter(pl.col("plant_id") == "PQU_ACRAC_GD").row(0, named=True)

        assert solar["generation_mwh"] == pytest.approx(0.0)
        assert solar["ceg"] is None

    def test_plants_without_ons_id_are_keyed_by_name(self, raw_plant_generation):
        valid = transform_plant_generation(raw_plant_generation, JANUARY_2024).valid

        assert "NAME:PQU MMAM MMGD" in valid["plant_id"].to_list()

    def test_missing_measurements_are_counted_not_rejected(self, raw_plant_generation):
        result = transform_plant_generation(raw_plant_generation, JANUARY_2024)

        assert result.missing_measurements == 1
        assert result.rejected.height == 1
        assert result.valid["generation_mwh"].null_count() == 0


class TestGenerationCapacity:
    def test_cleans_padded_fields(self, raw_generation_capacity):
        result = transform_generation_capacity(raw_generation_capacity, date(2024, 1, 31))
        unit = result.valid.filter(pl.col("unit_id") == "ALUXG-0UG5").row(0, named=True)

        assert unit["unit_name"] == "UG 527 MW USINA XINGO 5 AL"
        assert unit["unit_number"] == "5"
        assert unit["source_type"] == "HYDRO"
        assert unit["fuel_type"] == "HIDRAULICA"
        assert unit["operation_start_date"] == date(1995, 1, 31)
        assert unit["decommission_date"] is None
        assert unit["snapshot_date"] == date(2024, 1, 31)

    def test_rejects_units_without_id(self, raw_generation_capacity):
        result = transform_generation_capacity(raw_generation_capacity, date(2024, 1, 31))

        assert result.valid.height == 2
        assert result.rejected.height == 1
