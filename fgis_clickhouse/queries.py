"""Reusable ClickHouse queries for Streamlit callbacks."""

from __future__ import annotations

import re
from typing import List, Optional, Sequence, Tuple

from .clickhouse_io import CH


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _split_terms(raw: str) -> List[str]:
    parts = re.split(r"[+,;|]", raw or "")
    return [part.strip() for part in parts if part.strip()]


def _manufacturer_filter(term: str) -> str:
    terms = _split_terms(term)
    if not terms:
        return "1"
    clauses = []
    for item in terms:
        safe = _escape_literal(item)
        clauses.append(f"positionCaseInsensitive(manufacturer, '{safe}') > 0")
    if len(clauses) == 1:
        return clauses[0]
    return "(" + " OR ".join(clauses) + ")"


def _year_filter(column: str, year_from: Optional[int], year_to: Optional[int]) -> str:
    if year_from and year_to:
        y_from, y_to = sorted((int(year_from), int(year_to)))
        return f"toYear({column}) BETWEEN {y_from} AND {y_to}"
    if year_from:
        return f"toYear({column}) >= {int(year_from)}"
    if year_to:
        return f"toYear({column}) <= {int(year_to)}"
    return "1"


def count_mit_for_manufacturer(ch: CH, manufacturer_term: str) -> int:
    where = _manufacturer_filter(manufacturer_term)
    sql = f"SELECT countDistinct(mit_number) FROM {ch.db}.mit_registry WHERE {where}"
    return int(ch.scalar(sql) or 0)


def count_vri_for_manufacturer(
    ch: CH,
    manufacturer_term: str,
    year_from: Optional[int],
    year_to: Optional[int],
    only_applicable: bool,
) -> int:
    where = _manufacturer_filter(manufacturer_term)
    if only_applicable:
        where += " AND applicability = 1"
    year_sql = _year_filter("verification_date", year_from, year_to)
    if year_sql != "1":
        where += f" AND {year_sql}"
    sql = f"SELECT countDistinct(vri_id) FROM {ch.db}.v_vri_with_type WHERE {where}"
    return int(ch.scalar(sql) or 0)


def query_mit_for_manufacturer(ch: CH, manufacturer_term: str, limit: int) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    where = _manufacturer_filter(manufacturer_term)
    sql = (
        "SELECT mit_number, mit_title, notation, manufacturer, country, production_type, is_actual, valid_to "
        f"FROM {ch.db}.mit_registry WHERE {where} "
        "ORDER BY mit_number DESC "
        f"LIMIT {int(limit)}"
    )
    return (
        ["mit_number", "mit_title", "notation", "manufacturer", "country", "production_type", "is_actual", "valid_to"],
        ch.rows(sql),
    )


def query_vri_for_manufacturer(
    ch: CH,
    manufacturer_term: str,
    year_from: Optional[int],
    year_to: Optional[int],
    only_applicable: bool,
    limit: int,
) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    where = _manufacturer_filter(manufacturer_term)
    if only_applicable:
        where += " AND applicability = 1"
    year_sql = _year_filter("verification_date", year_from, year_to)
    if year_sql != "1":
        where += f" AND {year_sql}"
    sql = (
        "SELECT vri_id, verification_date, valid_date, org_title, mit_number, "
        "mit_title, mi_modification, mi_number, mit_notation, manufacturer, applicability "
        f"FROM {ch.db}.v_vri_with_type WHERE {where} "
        "ORDER BY verification_date DESC "
        f"LIMIT {int(limit)}"
    )
    return (
        [
            "vri_id",
            "verification_date",
            "valid_date",
            "org_title",
            "mit_number",
            "mit_title",
            "mi_modification",
            "mi_number",
            "mit_notation",
            "manufacturer",
            "applicability",
        ],
        ch.rows(sql),
    )


def top_manufacturers_by_mit(ch: CH, limit: int) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    sql = (
        "SELECT manufacturer, countDistinct(mit_number) AS approvals "
        f"FROM {ch.db}.mit_registry WHERE manufacturer != '' "
        "GROUP BY manufacturer ORDER BY approvals DESC "
        f"LIMIT {int(limit)}"
    )
    return (["manufacturer", "approvals"], ch.rows(sql))


def top_manufacturers_by_vri(
    ch: CH,
    limit: int,
    year_from: Optional[int],
    year_to: Optional[int],
    only_applicable: bool,
) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    where = "manufacturer != ''"
    if only_applicable:
        where += " AND applicability = 1"
    year_sql = _year_filter("verification_date", year_from, year_to)
    if year_sql != "1":
        where += f" AND {year_sql}"
    sql = (
        "SELECT manufacturer, countDistinct(vri_id) AS verifications "
        f"FROM {ch.db}.v_vri_with_type WHERE {where} "
        "GROUP BY manufacturer ORDER BY verifications DESC "
        f"LIMIT {int(limit)}"
    )
    return (["manufacturer", "verifications"], ch.rows(sql))


def report_mit_by_manufacturer(
    ch: CH,
    manufacturer_term: str,
    limit: int,
) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    where = _manufacturer_filter(manufacturer_term)
    sql = (
        "SELECT manufacturer, countDistinct(mit_number) AS approvals "
        f"FROM {ch.db}.mit_registry WHERE {where} "
        "GROUP BY manufacturer ORDER BY approvals DESC "
        f"LIMIT {int(limit)}"
    )
    return (["manufacturer", "approvals"], ch.rows(sql))


def report_vri_by_manufacturer(
    ch: CH,
    manufacturer_term: str,
    year_from: Optional[int],
    year_to: Optional[int],
    only_applicable: bool,
    limit: int,
) -> Tuple[List[str], Sequence[Tuple[object, ...]]]:
    where = _manufacturer_filter(manufacturer_term)
    if only_applicable:
        where += " AND applicability = 1"
    year_sql = _year_filter("verification_date", year_from, year_to)
    if year_sql != "1":
        where += f" AND {year_sql}"
    sql = (
        "SELECT manufacturer, countDistinct(vri_id) AS verifications "
        f"FROM {ch.db}.v_vri_with_type WHERE {where} "
        "GROUP BY manufacturer ORDER BY verifications DESC "
        f"LIMIT {int(limit)}"
    )
    return (["manufacturer", "verifications"], ch.rows(sql))
