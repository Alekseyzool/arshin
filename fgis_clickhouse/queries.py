"""Reusable ClickHouse queries for Streamlit callbacks."""

from __future__ import annotations

from typing import List, Optional

from .clickhouse_io import CH


def distinct_mit_numbers(ch: CH, limit: Optional[int] = None) -> List[str]:
    """Return distinct MIT registration numbers stored in `mit_search_raw`."""
    sql = f"SELECT DISTINCT number FROM {ch.db}.mit_search_raw WHERE number != '' ORDER BY number"
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"
    rows = ch.rows(sql)
    return [row[0] for row in rows if row and row[0]]

