"""ClickHouse connection helpers and DDL bootstrap."""

from __future__ import annotations

import os
import time
from typing import Any, Optional, Sequence, Set

from .utils import chunked


class CH:
    """Light wrapper around the native ClickHouse driver."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        from clickhouse_driver import Client as Native

        self._params = dict(host=host, port=port, user=user, password=password, database=database)
        self.db = database
        self.batch_size = int(os.getenv("CH_INSERT_BATCH", "500"))
        self.retries = int(os.getenv("CH_INSERT_RETRIES", "3"))
        self._settings = {"send_retries": 2, "retry_timeout": 5}
        self._Native = Native
        self._connect()

    def _connect(self) -> None:
        self._client = self._Native(**self._params, compression=False, settings=self._settings)

    def reconnect(self) -> None:
        try:
            self._client.disconnect()
        except Exception:
            pass
        self._connect()

    def exec(self, sql: str) -> Any:
        return self._client.execute(sql)

    def scalar(self, sql: str) -> Any:
        rows = self._client.execute(sql)
        return rows[0][0] if rows else None

    def rows(self, sql: str):
        return self._client.execute(sql)

    def insert(self, table: str, columns: Sequence[str], rows_data) -> None:
        if not rows_data:
            return
        cols = ",".join(columns)
        from clickhouse_driver.errors import NetworkError, SocketTimeoutError

        for chunk in chunked(rows_data, self.batch_size):
            attempt = 0
            while True:
                try:
                    self._client.execute(f"INSERT INTO {self.db}.{table} ({cols}) VALUES", chunk)
                    break
                except (NetworkError, SocketTimeoutError):
                    attempt += 1
                    if attempt <= self.retries:
                        time.sleep(1.5 * attempt)
                        self.reconnect()
                        continue
                    raise

    def existing_ids(self, table: str, idcol: str, ids) -> Set[str]:
        values = [value for value in ids if value]
        if not values:
            return set()
        existing: Set[str] = set()
        for group in chunked(values, 1000):
            rows = self._client.execute(
                f"SELECT {idcol} FROM {self.db}.{table} WHERE {idcol} IN %(ids)s",
                {"ids": tuple(group)},
            )
            existing.update(row[0] for row in rows)
        return existing


def ensure_tables(ch: CH, db: Optional[str] = None) -> None:
    """Create the ClickHouse schema required for ingestion jobs."""
    db = db or ch.db
    ddl_statements = [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"""
        CREATE TABLE IF NOT EXISTS {db}.mit_registry (
            country String,
            inserted_at DateTime DEFAULT now(),
            is_actual UInt8,
            manufacturer String,
            mit_number String,
            mit_title String,
            mpi String,
            notation String,
            order_date Nullable(Date),
            order_num String,
            production_type UInt8,
            valid_to Nullable(Date)
        ) ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY mit_number
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.verifications (
            applicability UInt8,
            inserted_at DateTime DEFAULT now(),
            mi_modification String,
            mi_number String,
            mit_notation String,
            mit_number String,
            mit_title String,
            org_title LowCardinality(String),
            valid_date Nullable(Date),
            verification_date Date,
            vri_id String
        ) ENGINE = ReplacingMergeTree(inserted_at)
        PARTITION BY toYYYYMM(verification_date)
        ORDER BY (verification_date, vri_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.sync_state (
            key String,
            last_run DateTime,
            updated_at DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        ORDER BY key
        """,
    ]

    for ddl in ddl_statements:
        ch.exec(ddl)

    # Minimal schema only: no analytics views here.
