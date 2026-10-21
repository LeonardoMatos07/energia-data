from __future__ import annotations

import io
import itertools
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any

import polars as pl
import psycopg2
from psycopg2 import sql

log = logging.getLogger(__name__)

REQUIRED_TABLES = [
    "plant",
    "plant_monthly_curtailment",
    "state_monthly_curtailment",
    "subsystem_monthly_curtailment",
    "load_run",
]


class SchemaNotReadyError(RuntimeError):
    pass


def _table(name: str) -> sql.Composable:
    return sql.Identifier(*name.split("."))


def _columns(names: Sequence[str]) -> sql.Composable:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)


class Session:
    _stage_ids = itertools.count()

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def execute(self, query: str | sql.Composable, params: Sequence[Any] | None = None) -> None:
        self._cursor.execute(query, params)

    def fetchall(
        self, query: str | sql.Composable, params: Sequence[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        self._cursor.execute(query, params)
        rows: list[tuple[Any, ...]] = self._cursor.fetchall()
        return rows

    def replace_range(
        self,
        table: str,
        df: pl.DataFrame,
        *,
        column: str,
        start: date | datetime,
        end: date | datetime,
    ) -> int:
        stage = self._stage(table, df)
        self.execute(
            sql.SQL("DELETE FROM {} WHERE {} >= %s AND {} < %s").format(
                _table(table), sql.Identifier(column), sql.Identifier(column)
            ),
            (start, end),
        )
        deleted = self._cursor.rowcount
        self._insert_from_stage(table, stage, df.columns)
        log.info("%s: replaced %d rows with %d rows", table, deleted, df.height)
        return df.height

    def lock(self, name: str) -> None:
        self.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (name,))

    def upsert(
        self,
        table: str,
        df: pl.DataFrame,
        *,
        keys: Sequence[str],
        keep: Sequence[str] = (),
        touch_column: str | None = "updated_at",
    ) -> int:
        stage = self._stage(table, df)
        assignments = [
            sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(column))
            for column in df.columns
            if column not in keys and column not in keep
        ]
        if touch_column:
            assignments.append(sql.SQL("{} = now()").format(sql.Identifier(touch_column)))
        self.execute(
            sql.SQL(
                "INSERT INTO {table} ({columns}) SELECT {columns} FROM {stage} "
                "ON CONFLICT ({keys}) DO UPDATE SET {assignments}"
            ).format(
                table=_table(table),
                columns=_columns(df.columns),
                stage=sql.Identifier(stage),
                keys=_columns(keys),
                assignments=sql.SQL(", ").join(assignments),
            )
        )
        log.info("%s: upserted %d rows", table, df.height)
        return df.height

    def _stage(self, table: str, df: pl.DataFrame) -> str:
        stage = f"stage_{next(self._stage_ids)}"
        self.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(stage), _table(table)
            )
        )
        buffer = io.BytesIO()
        df.write_csv(
            buffer,
            include_header=False,
            datetime_format="%Y-%m-%d %H:%M:%S",
            date_format="%Y-%m-%d",
        )
        buffer.seek(0)
        self._cursor.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                sql.Identifier(stage), _columns(df.columns)
            ),
            buffer,
        )
        return stage

    def _insert_from_stage(self, table: str, stage: str, columns: Sequence[str]) -> None:
        self.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                _table(table), _columns(columns), _columns(columns), sql.Identifier(stage)
            )
        )


class Warehouse:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @contextmanager
    def session(self) -> Iterator[Session]:
        connection = psycopg2.connect(self.dsn)
        try:
            with connection, connection.cursor() as cursor:
                yield Session(cursor)
        finally:
            connection.close()

    def check_schema(self, tables: Sequence[str] = REQUIRED_TABLES) -> None:
        with self.session() as session:
            rows = session.fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = ANY(%s)",
                (list(tables),),
            )
        missing = sorted(set(tables) - {row[0] for row in rows})
        if missing:
            raise SchemaNotReadyError(
                f"Missing tables {missing}: run the energia-api migrations "
                "(alembic upgrade head) before loading data."
            )

    def record_load(
        self,
        *,
        dataset: str,
        status: str,
        started_at: datetime,
        finished_at: datetime | None = None,
        period: date | None = None,
        source_file: str | None = None,
        checksum: str | None = None,
        rows: int | None = None,
        detail: str | None = None,
    ) -> None:
        with self.session() as session:
            session.execute(
                "INSERT INTO load_run "
                "(dataset, period, source_file, checksum, row_count, started_at, finished_at, "
                "status, detail) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    dataset,
                    period,
                    source_file,
                    checksum,
                    rows,
                    started_at,
                    finished_at,
                    status,
                    detail,
                ),
            )
