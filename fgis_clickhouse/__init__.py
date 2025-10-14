"""FGIS → ClickHouse Streamlit app helpers."""

from .clickhouse_io import CH, ensure_tables
from .fgis_api import FGISClient
from .ingestion import ingest_mit, ingest_vri
from .parsing import parse_vri_payload
from .queries import distinct_mit_numbers
from .ui_helpers import ch_connect_from_sidebar, read_optional_dataframe
from .utils import (
    collect_mit_batches,
    collect_vri_batches,
    h64,
    parse_date_ddmmyyyy,
    parse_dt_value,
    serial_variants,
    ts_compact,
    try_parse_since,
)

__all__ = [
    "CH",
    "FGISClient",
    "ingest_mit",
    "ingest_vri",
    "parse_vri_payload",
    "ch_connect_from_sidebar",
    "read_optional_dataframe",
    "collect_mit_batches",
    "collect_vri_batches",
    "distinct_mit_numbers",
    "h64",
    "parse_date_ddmmyyyy",
    "parse_dt_value",
    "serial_variants",
    "ts_compact",
    "try_parse_since",
    "ensure_tables",
]
