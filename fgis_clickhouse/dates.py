"""Date parsing and range iteration helpers used by sync jobs."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional


def parse_ymd(raw: str) -> Optional[date]:
    """Parse an ISO calendar date from env/config text."""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_date_any(raw: Any) -> Optional[date]:
    """Parse the date formats returned by FGIS and ClickHouse drivers."""
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    text = text.replace("Z", "")
    if "T" in text:
        text = text.split("T")[0]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def iter_month_starts(start: date, end: date) -> Iterable[date]:
    """Yield the first day of every month that intersects a date range."""
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def iter_year_ranges(start: date, end: date) -> Iterable[tuple[date, date]]:
    """Split a range into year-sized closed intervals."""
    for year in range(start.year, end.year + 1):
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        yield max(start, year_start), min(end, year_end)


def iter_month_ranges(start: date, end: date) -> Iterable[tuple[date, date]]:
    """Split a range into month-sized closed intervals."""
    for month_start in iter_month_starts(start, end):
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        yield max(start, month_start), min(end, month_end)


def iter_days(start: date, end: date) -> Iterable[date]:
    """Yield every day in a closed date range."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)
