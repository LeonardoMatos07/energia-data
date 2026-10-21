from datetime import date

import polars as pl
import pytest

from energy_pipeline import steps
from energy_pipeline.period import Period
from energy_pipeline.schema import SCHEMAS, SchemaChangedError, check_schema, find_schema_problems
from energy_pipeline.sources import SOURCES


def frame(columns: dict[str, list[str | None]]) -> pl.DataFrame:
    return pl.DataFrame(columns, schema=dict.fromkeys(columns, pl.String))


@pytest.mark.parametrize("source_name", sorted(SCHEMAS))
def test_schema_lists_every_column_the_transform_reads(source_name):
    raw = frame(dict.fromkeys(SCHEMAS[source_name], []))
    steps._standardize(SOURCES[source_name], raw, Period(2026, 9), date(2026, 10, 1))


def test_every_source_has_a_schema():
    assert set(SCHEMAS) == set(SOURCES)


def test_matching_file_passes():
    raw = frame(
        {
            "id_subsistema": ["NE", "S"],
            "din_instante": ["2026-09-01 00:00:00", "2026-09-01 00:30:00"],
            "val_cmo": ["12,5", "-"],
        }
    )
    check_schema("marginal_cost", raw)


def test_missing_column_fails():
    raw = frame({"id_subsistema": ["NE"], "din_instante": ["2026-09-01 00:00:00"]})
    with pytest.raises(SchemaChangedError, match="val_cmo: missing"):
        check_schema("marginal_cost", raw)


def test_changed_value_format_fails():
    raw = frame(
        {
            "id_subsistema": ["NE", "NE"],
            "din_instante": ["01/09/2026 00:00", "01/09/2026 00:30"],
            "val_cmo": ["10", "11"],
        }
    )
    problems = find_schema_problems(raw, SCHEMAS["marginal_cost"])
    assert [p.column for p in problems] == ["din_instante"]
    assert "2/2 values (100.0%) are not timestamp" in str(problems[0])


def test_isolated_bad_values_are_tolerated():
    values = ["1.5"] * 199 + ["n/a"]
    raw = frame(
        {
            "id_subsistema": ["NE"] * 200,
            "din_instante": ["2026-09-01 00:00:00"] * 200,
            "val_cmo": values,
        }
    )
    assert find_schema_problems(raw, SCHEMAS["marginal_cost"]) == []
