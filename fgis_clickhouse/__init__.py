"""FGIS → ClickHouse sync helpers."""

from .clickhouse_io import CH, ensure_tables

__all__ = ["CH", "FGISClient", "ensure_tables"]


def __getattr__(name: str):
    """Load network-facing helpers only when callers explicitly ask for them."""
    if name == "FGISClient":
        from .fgis_api import FGISClient

        return FGISClient
    raise AttributeError(name)
