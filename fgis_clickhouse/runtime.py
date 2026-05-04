"""Runtime policy for the backend sync process.

This module intentionally keeps operational choices in one place: how often
the timer should do work, how far back each safety check looks, how state is
stored in ClickHouse, and how overlapping processes are prevented.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - deployed target is Linux.
    fcntl = None

from .clickhouse_io import CH


# Data range and page sizes used by the simplified pipeline.
DEFAULT_START_DATE = date(2010, 1, 1)
DEFAULT_VRI_ROWS = 9999
DEFAULT_VRI_SLEEP = 0.0
DEFAULT_VRI_MIN_ROWS = 10

# Operational schedule. systemd can start the script several times per day; the
# gates below choose one VRI safety window per run. Wider windows cover smaller
# ones, so a monthly year check also satisfies the daily month and weekly
# quarter checks.
MIT_SYNC_EVERY = timedelta(days=1)
DAILY_MONTH_DAYS = 31
DAILY_MONTH_EVERY = timedelta(days=1)
WEEKLY_3M_DAYS = 93
WEEKLY_3M_EVERY = timedelta(days=7)
MONTHLY_YEAR_DAYS = 365
MONTHLY_YEAR_EVERY = timedelta(days=30)
HALFYEAR_2Y_DAYS = 730
HALFYEAR_2Y_EVERY = timedelta(days=182)
PROD_SYNC_HOUR = 21
PROD_DEFAULT_DAYS = DAILY_MONTH_DAYS

# FGIS cannot reliably use start-based fallback beyond this offset.
MAX_VRI_START_FALLBACK = 9999

# Process-wide advisory lock. It prevents a systemd timer and a manual command
# from writing to the same ClickHouse tables at the same time.
PROCESS_LOCK = "/tmp/fgis_arshin_backend_sync.lock"
_LOCK_HANDLE = None


def env_pick(*names: str, default: str = "") -> str:
    """Return the first non-empty env value from a list of legacy aliases."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value
    return default


def acquire_process_lock(path: str = PROCESS_LOCK) -> bool:
    """Return False when another backend sync process already owns the lock."""
    global _LOCK_HANDLE
    if fcntl is None:
        return True
    _LOCK_HANDLE = open(path, "w", encoding="utf-8")
    try:
        fcntl.flock(_LOCK_HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    _LOCK_HANDLE.write(f"{os.getpid()}\n")
    _LOCK_HANDLE.flush()
    return True


def state_get(ch: CH, key: str) -> Optional[datetime]:
    """Read the latest timestamp for a logical sync checkpoint."""
    try:
        value = ch.scalar(f"SELECT max(last_run) FROM {ch.db}.sync_state WHERE key = '{key}'")
    except Exception:
        return None
    if isinstance(value, datetime):
        return value
    return None


def state_get_any(ch: CH, keys: Iterable[str]) -> Optional[datetime]:
    """Read the newest timestamp across current and legacy checkpoint names."""
    values = [value for key in keys if (value := state_get(ch, key))]
    return max(values) if values else None


def state_set(ch: CH, key: str, when: datetime) -> None:
    """Append a checkpoint row; ReplacingMergeTree keeps the newest version."""
    ch.insert("sync_state", ["key", "last_run"], [(key, when)])


def should_run(ch: CH, key: str, every: timedelta, now: datetime) -> bool:
    """Return True when a checkpoint is missing or older than `every`."""
    last = state_get(ch, key)
    if not last:
        return True
    return now - last >= every


def should_run_any(ch: CH, keys: Iterable[str], every: timedelta, now: datetime) -> bool:
    """Like should_run(), but accepts legacy aliases for migration safety."""
    last = state_get_any(ch, keys)
    if not last:
        return True
    return now - last >= every
