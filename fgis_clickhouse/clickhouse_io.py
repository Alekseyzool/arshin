"""ClickHouse connection helpers and DDL bootstrap."""

from __future__ import annotations

import os
import time
from typing import Any, Sequence, Set

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


def ensure_tables(ch: CH) -> None:
    """Create the ClickHouse schema required for ingestion jobs."""
    db = ch.db
    ddl_statements = [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"""
        CREATE TABLE IF NOT EXISTS {db}.vri_search_raw (
            vri_id String,
            org_title String,
            mi_mitnumber String,
            mi_mititle String,
            mi_mitype String,
            mi_modification String,
            mi_number String,
            verification_date DateTime DEFAULT toDateTime(0),
            valid_date        DateTime DEFAULT toDateTime(0),
            applicability UInt8,
            result_docnum String,
            sticker_num String,
            run_id String,
            source_tag String,
            ingest_ts DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        PARTITION BY toYYYYMM(verification_date)
        ORDER BY vri_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.vri_details_raw (
            vri_id String,
            payload_json String,
            payload_hash UInt64,
            run_id String,
            source_tag String,
            ingest_ts DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        ORDER BY vri_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.vri_details (
            vri_id String,
            mitype_number String,
            mitype_type String,
            mitype_title String,
            mitype_url String,
            manufacture_num String,
            manufacture_year Int32,
            modification String,
            org_title String,
            sign_cipher String,
            mi_owner String,
            vri_type String,
            vri_type_label String,
            vrf_date Date,
            valid_date Date,
            doc_title String,
            applicable_cert_num String,
            applicable_sign_pass UInt8,
            applicable_sign_mi UInt8,
            brief_indicator UInt8,
            run_id String,
            source_tag String,
            ingest_ts DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        PARTITION BY toYYYYMM(vrf_date)
        ORDER BY vri_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.vri_mieta (
            vri_id String,
            reg_number String,
            mitype_number String,
            mitype_title String,
            mitype_url String,
            notation String,
            modification String,
            manufacture_num String,
            manufacture_year Int32,
            rank_code String,
            rank_title String,
            schema_title String,
            row_hash UInt64,
            run_id String,
            source_tag String,
            ingest_ts DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        ORDER BY (vri_id, reg_number)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.vri_mis (
            vri_id String,
            mitype_number String,
            mitype_title String,
            mitype_url String,
            number String,
            row_hash UInt64,
            run_id String,
            source_tag String,
            ingest_ts DateTime DEFAULT now(),
            ver UInt64 DEFAULT toUnixTimestamp(now())
        ) ENGINE = ReplacingMergeTree(ver)
        ORDER BY (vri_id, mitype_number, number)
        """,
    ]

    for ddl in ddl_statements:
        ch.exec(ddl)

    try:
        ch.exec(f"DROP VIEW IF EXISTS {db}.v_vri_with_type")
    except Exception:
        ch.exec(f"DROP TABLE IF EXISTS {db}.v_vri_with_type")

    ch.exec(
        f"""
        CREATE VIEW {db}.v_vri_with_type AS
        SELECT
            v.vri_id,
            v.verification_date,
            v.valid_date,
            v.org_title,
            v.mi_mitnumber,
            v.mi_mititle,
            v.mi_mitype,
            v.mi_modification,
            v.mi_number,
            m.mit_uuid,
            m.title AS mit_title,
            m.notation AS mit_notation,
            m.manufacturers
        FROM {db}.vri_search_raw AS v
        ANY LEFT JOIN (
            SELECT
                mit_uuid,
                title,
                notation,
                manufacturers,
                number,
                replaceRegexpAll(number, '\\\\s+', '') AS number_clean
            FROM {db}.mit_search_raw
        ) AS m ON replaceRegexpAll(v.mi_mitnumber, '\\\\s+', '') = m.number_clean
        """
    )
