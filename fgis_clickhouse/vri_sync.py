"""VRI loading, reconciliation, and prod/test comparison logic."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Optional

from .clickhouse_io import CH
from .dates import (
    iter_days,
    iter_month_ranges,
    iter_year_ranges,
    parse_date_any,
)
from .fgis_api import FGISClient
from .runtime import (
    DEFAULT_VRI_MIN_ROWS,
    MAX_VRI_START_FALLBACK,
)


log = logging.getLogger("fgis_backend")

def fq_for_day(d: date) -> str:
    ds = d.strftime("%Y-%m-%d")
    return f"verification_date:[{ds}T00:00:00Z TO {ds}T23:59:59Z]"

def fq_for_range(start: date, end: date) -> str:
    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")
    return f"verification_date:[{start_s}T00:00:00Z TO {end_s}T23:59:59Z]"

def pick_start_date(ch: CH, start_date: Optional[date], tail_days: int) -> Optional[date]:
    if start_date is None:
        try:
            last = ch.scalar(f"SELECT max(verification_date) FROM {ch.db}.verifications")
        except Exception:
            last = None
        if not last:
            return None
        if isinstance(last, datetime):
            last_date = last.date()
        elif isinstance(last, date):
            last_date = last
        else:
            return None
        return last_date - timedelta(days=tail_days)
    try:
        last = ch.scalar(f"SELECT max(verification_date) FROM {ch.db}.verifications")
        if last:
            last_date = last.date() if isinstance(last, datetime) else last
            return max(start_date, last_date - timedelta(days=tail_days))
    except Exception:
        pass
    return start_date

def resolve_vri_start_date(ch: CH, start_date: Optional[date], tail_days: int) -> Optional[date]:
    """Honor an explicit start date; otherwise derive a rolling tail from local data."""
    if start_date is not None:
        return start_date
    return pick_start_date(ch, None, tail_days)

def local_vri_stats(ch: CH, d: date) -> tuple[int, int]:
    ds = d.strftime("%Y-%m-%d")
    try:
        rows = int(
            ch.scalar(
                f"SELECT count() FROM {ch.db}.verifications "
                f"WHERE verification_date = toDate('{ds}')"
            )
            or 0
        )
        final_rows = int(
            ch.scalar(
                f"SELECT count() FROM {ch.db}.verifications FINAL "
                f"WHERE verification_date = toDate('{ds}')"
            )
            or 0
        )
        return rows, final_rows
    except Exception:
        return 0, 0

def local_vri_counts_range(ch: CH, start: date, end: date) -> tuple[int, int]:
    """Return raw and deduplicated row counts for a VRI range.

    `uniqExact(vri_id)` is memory-heavy on large ClickHouse ranges. The table is
    keyed by `(verification_date, vri_id)`, so `FINAL` gives the same duplicate
    signal without building a huge hash set in memory.
    """
    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")
    try:
        where = f"verification_date >= toDate('{start_s}') AND verification_date <= toDate('{end_s}')"
        rows = int(ch.scalar(f"SELECT count() FROM {ch.db}.verifications WHERE {where}") or 0)
        final_rows = int(ch.scalar(f"SELECT count() FROM {ch.db}.verifications FINAL WHERE {where}") or 0)
        return rows, final_rows
    except Exception:
        return 0, 0

def remote_vri_count(client: FGISClient, fq: str) -> int:
    if hasattr(client, "vri_count"):
        try:
            return int(client.vri_count(fq) or 0)
        except Exception as exc:
            log.warning("remote_vri_count: vri_count failed: %s", exc)
    if hasattr(client, "vri_cursor"):
        try:
            _docs, num_found, _next = client.vri_cursor(fq=fq, rows=0, cursor_mark="*")
            return int(num_found or 0)
        except Exception as exc:
            log.warning("remote_vri_count: vri_cursor fallback failed: %s", exc)
    raise RuntimeError("FGIS remote VRI count unavailable after vri_count and vri_cursor fallback errors.")

def iter_vri_pages(
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
    min_rows: Optional[int] = None,
) -> Iterator[tuple[list[dict[str, Any]], int, int, int, str]]:
    """Yield all VRI pages for one day with defensive FGIS pagination.

    Normal path uses Solr cursorMark because it is stable for large days. If a
    page starts failing with retryable HTTP errors, the page size is repeatedly
    reduced down to `min_rows`. Only when cursor pagination is still broken do
    we fall back to start-based pagination, and only while the offset is inside
    the known FGIS limit.
    """
    fq = fq_for_day(d)
    cursor_mark = "*"
    seen_docs = 0
    page_num = 0
    use_offset = False
    configured_rows = max(rows, 1)
    current_rows = configured_rows
    if min_rows is None:
        min_rows = min(DEFAULT_VRI_MIN_ROWS, current_rows)
    min_rows = max(1, min(min_rows, current_rows))
    log.info("VRI %s: start rows=%s min_rows=%s", d, current_rows, min_rows)
    while True:
        page_num += 1
        page_rows = current_rows
        page_cursor = cursor_mark
        page_start = seen_docs
        num_found = 0
        next_cursor: Optional[str] = None
        while True:
            try:
                if use_offset:
                    docs, num_found = client.vri_page(fq=fq, rows=page_rows, start=page_start)
                else:
                    docs, num_found, next_cursor = client.vri_cursor(
                        fq=fq,
                        rows=page_rows,
                        cursor_mark=cursor_mark,
                    )
                if page_rows != current_rows:
                    current_rows = page_rows
                    if use_offset:
                        log.info(
                            "VRI %s: page=%s start=%s fetch recovered with rows=%s",
                            d,
                            page_num,
                            page_start,
                            current_rows,
                        )
                    else:
                        log.info(
                            "VRI %s: page=%s cursor=%s fetch recovered with rows=%s",
                            d,
                            page_num,
                            page_cursor,
                            current_rows,
                        )
                break
            except Exception as exc:
                if page_rows > min_rows:
                    next_rows = max(min_rows, page_rows // 2)
                    if next_rows == page_rows:
                        next_rows = min_rows
                    if use_offset:
                        log.warning(
                            "VRI %s: page=%s start=%s fetch failed with rows=%s: %s -> retry rows=%s",
                            d,
                            page_num,
                            page_start,
                            page_rows,
                            exc,
                            next_rows,
                        )
                    else:
                        log.warning(
                            "VRI %s: page=%s cursor=%s fetch failed with rows=%s: %s -> retry rows=%s",
                            d,
                            page_num,
                            page_cursor,
                            page_rows,
                            exc,
                            next_rows,
                        )
                    page_rows = next_rows
                    continue
                if not use_offset:
                    if page_start > MAX_VRI_START_FALLBACK:
                        raise RuntimeError(
                            "FGIS VRI start fallback is unavailable beyond start="
                            f"{MAX_VRI_START_FALLBACK}; cursor failed at page={page_num} "
                            f"cursor={page_cursor} rows={page_rows} offset={page_start}. "
                            "Retry with lower FGIS_RPS / higher backoff to stay on cursor pagination."
                        ) from exc
                    failed_rows = page_rows
                    use_offset = True
                    current_rows = configured_rows
                    page_rows = current_rows
                    log.warning(
                        "VRI %s: page=%s cursor=%s fetch failed with rows=%s: %s -> switch to start=%s reset rows=%s",
                        d,
                        page_num,
                        page_cursor,
                        failed_rows,
                        exc,
                        page_start,
                        current_rows,
                    )
                    continue
                log.error(
                    "VRI %s: page=%s start=%s fetch failed with rows=%s: %s",
                    d,
                    page_num,
                    page_start,
                    page_rows,
                    exc,
                )
                raise RuntimeError(
                    f"VRI day fetch failed for {d} page={page_num} start={page_start} rows={page_rows}: {exc}"
                ) from exc
        if not docs:
            if use_offset and seen_docs < num_found:
                log.warning(
                    "VRI %s: page=%s start=%s returned docs=0 before remote_total=%s -> stop",
                    d,
                    page_num,
                    page_start,
                    num_found,
                )
            break
        page_mode = "start" if use_offset else "cursor"
        yield docs, num_found, page_num, page_rows, page_mode
        seen_docs += len(docs)
        if use_offset:
            if seen_docs >= num_found:
                break
        else:
            if not next_cursor or next_cursor == cursor_mark:
                break
            cursor_mark = next_cursor
        if sleep_s > 0:
            time.sleep(sleep_s)

def delete_vri_day(ch: CH, d: date) -> None:
    day_str = d.strftime("%Y-%m-%d")
    ch.exec(
        f"ALTER TABLE {ch.db}.verifications "
        f"DELETE WHERE verification_date = toDate(%(day)s) "
        f"SETTINGS mutations_sync=1",
        {"day": day_str},
    )

def reload_vri_day_from_remote(
    ch: CH,
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
) -> bool:
    remote_rows = remote_vri_count(client, fq_for_day(d))
    delete_vri_day(ch, d)
    if remote_rows > 0:
        sync_vri_day(ch, client, d, rows, sleep_s, skip_existing=False, remote_total=remote_rows)
    local_rows, local_uniq = local_vri_stats(ch, d)
    if local_rows == local_uniq == remote_rows:
        log.info("REMOTE %s: OK (rows=%s uniq=%s remote=%s)", d, local_rows, local_uniq, remote_rows)
        return True
    log.warning(
        "REMOTE %s: mismatch after reload (rows=%s uniq=%s remote=%s)",
        d,
        local_rows,
        local_uniq,
        remote_rows,
    )
    return False

def reconcile_day_with_remote(
    ch: CH,
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
) -> bool:
    remote_rows = remote_vri_count(client, fq_for_day(d))
    local_rows, local_uniq = local_vri_stats(ch, d)
    if local_rows == local_uniq == remote_rows:
        log.info("REMOTE %s: OK (rows=%s uniq=%s remote=%s)", d, local_rows, local_uniq, remote_rows)
        return True
    log.warning(
        "REMOTE %s: reload (rows=%s uniq=%s remote=%s)",
        d,
        local_rows,
        local_uniq,
        remote_rows,
    )
    return reload_vri_day_from_remote(ch, client, d, rows, sleep_s)

def reconcile_remote_by_year_month(
    ch: CH,
    client: FGISClient,
    start: date,
    end: date,
    rows: int,
    sleep_s: float,
) -> bool:
    """Compare local VRI with FGIS using cheap ranges before expensive reloads.

    A year-level count catches fully healthy years with one remote request.
    Only mismatching years are split into months, and only mismatching months
    are split into days. This keeps weekly/monthly checks practical on hundreds
    of millions of rows.
    """
    ok = True
    for year_start, year_end in iter_year_ranges(start, end):
        remote_rows = remote_vri_count(client, fq_for_range(year_start, year_end))
        local_rows, local_uniq = local_vri_counts_range(ch, year_start, year_end)
        if local_rows == local_uniq == remote_rows:
            log.info(
                "REMOTE YEAR %s: OK (rows=%s uniq=%s remote=%s)",
                year_start.year,
                local_rows,
                local_uniq,
                remote_rows,
            )
            continue
        log.warning(
            "REMOTE YEAR %s: mismatch (rows=%s uniq=%s remote=%s)",
            year_start.year,
            local_rows,
            local_uniq,
            remote_rows,
        )
        for month_start, month_end in iter_month_ranges(year_start, year_end):
            remote_rows = remote_vri_count(client, fq_for_range(month_start, month_end))
            local_rows, local_uniq = local_vri_counts_range(ch, month_start, month_end)
            label = month_start.strftime("%Y-%m")
            if local_rows == local_uniq == remote_rows:
                log.info(
                    "REMOTE MONTH %s: OK (rows=%s uniq=%s remote=%s)",
                    label,
                    local_rows,
                    local_uniq,
                    remote_rows,
                )
                continue
            log.warning(
                "REMOTE MONTH %s: mismatch (rows=%s uniq=%s remote=%s)",
                label,
                local_rows,
                local_uniq,
                remote_rows,
            )
            for day in iter_days(month_start, month_end):
                ok = reconcile_day_with_remote(ch, client, day, rows, sleep_s) and ok
    return ok

def replace_vri_day_from_test(ch_src: CH, ch_dst: CH, d: date, *, dedup: bool) -> None:
    delete_vri_day(ch_dst, d)
    from_clause = f"{ch_src.db}.verifications FINAL" if dedup else f"{ch_src.db}.verifications"
    day_str = d.strftime("%Y-%m-%d")
    ch_dst.exec(
        f"INSERT INTO {ch_dst.db}.verifications "
        f"SELECT * FROM {from_clause} "
        f"WHERE verification_date = toDate(%(day)s)",
        {"day": day_str},
    )

def reconcile_day_with_test(ch_test: CH, ch_prod: CH, d: date, *, dedup: bool) -> bool:
    test_rows, test_uniq = local_vri_stats(ch_test, d)
    prod_rows, prod_uniq = local_vri_stats(ch_prod, d)
    if test_rows == test_uniq and prod_rows == prod_uniq and test_uniq == prod_uniq:
        log.info(
            "PROD %s: OK (test=%s prod=%s)",
            d,
            test_uniq,
            prod_uniq,
        )
        return True
    log.warning(
        "PROD %s: reload (test_rows=%s test_uniq=%s prod_rows=%s prod_uniq=%s)",
        d,
        test_rows,
        test_uniq,
        prod_rows,
        prod_uniq,
    )
    if test_uniq == 0:
        delete_vri_day(ch_prod, d)
    else:
        replace_vri_day_from_test(ch_test, ch_prod, d, dedup=dedup)
    prod_rows, prod_uniq = local_vri_stats(ch_prod, d)
    if prod_rows == prod_uniq == test_uniq:
        log.info("PROD %s: OK after reload (rows=%s uniq=%s)", d, prod_rows, prod_uniq)
        return True
    log.warning(
        "PROD %s: mismatch after reload (rows=%s uniq=%s test=%s)",
        d,
        prod_rows,
        prod_uniq,
        test_uniq,
    )
    return False

def reconcile_prod_by_year_month(
    ch_test: CH,
    ch_prod: CH,
    start: date,
    end: date,
    *,
    dedup: bool,
) -> bool:
    """Make prod match test while avoiding unnecessary day rewrites.

    The function mirrors the remote reconcile strategy: compare broad ranges
    first, then rewrite only mismatching days with DELETE + INSERT from test.
    """
    ok = True
    for year_start, year_end in iter_year_ranges(start, end):
        test_rows, test_uniq = local_vri_counts_range(ch_test, year_start, year_end)
        prod_rows, prod_uniq = local_vri_counts_range(ch_prod, year_start, year_end)
        if (
            test_rows == test_uniq
            and prod_rows == prod_uniq
            and test_uniq == prod_uniq
        ):
            log.info(
                "PROD YEAR %s: OK (test=%s prod=%s)",
                year_start.year,
                test_uniq,
                prod_uniq,
            )
            continue
        log.warning(
            "PROD YEAR %s: mismatch (test_rows=%s test_uniq=%s prod_rows=%s prod_uniq=%s)",
            year_start.year,
            test_rows,
            test_uniq,
            prod_rows,
            prod_uniq,
        )
        for month_start, month_end in iter_month_ranges(year_start, year_end):
            test_rows, test_uniq = local_vri_counts_range(ch_test, month_start, month_end)
            prod_rows, prod_uniq = local_vri_counts_range(ch_prod, month_start, month_end)
            label = month_start.strftime("%Y-%m")
            if (
                test_rows == test_uniq
                and prod_rows == prod_uniq
                and test_uniq == prod_uniq
            ):
                log.info(
                    "PROD MONTH %s: OK (test=%s prod=%s)",
                    label,
                    test_uniq,
                    prod_uniq,
                )
                continue
            log.warning(
                "PROD MONTH %s: mismatch (test_rows=%s test_uniq=%s prod_rows=%s prod_uniq=%s)",
                label,
                test_rows,
                test_uniq,
                prod_rows,
                prod_uniq,
            )
            for day in iter_days(month_start, month_end):
                ok = reconcile_day_with_test(ch_test, ch_prod, day, dedup=dedup) and ok
    return ok

def build_vri_row(doc: dict[str, Any], inserted_at: datetime) -> Optional[tuple[Any, ...]]:
    vri_id = (doc.get("vri_id") or "").strip()
    if not vri_id:
        return None
    verification_date = parse_date_any(doc.get("verification_date")) or date(1970, 1, 1)
    valid_date = parse_date_any(doc.get("valid_date"))
    return (
        1 if doc.get("applicability") else 0,
        inserted_at,
        doc.get("mi.modification", "") or "",
        doc.get("mi.number", "") or "",
        doc.get("mi.mitype", "") or "",
        doc.get("mi.mitnumber", "") or "",
        doc.get("mi.mititle", "") or "",
        doc.get("org_title", "") or "",
        valid_date,
        verification_date,
        vri_id,
    )

def insert_verifications(ch: CH, rows: list[tuple[Any, ...]]) -> None:
    ch.insert(
        "verifications",
        [
            "applicability",
            "inserted_at",
            "mi_modification",
            "mi_number",
            "mit_notation",
            "mit_number",
            "mit_title",
            "org_title",
            "valid_date",
            "verification_date",
            "vri_id",
        ],
        rows,
    )

def sync_vri_day(
    ch: CH,
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
    skip_existing: bool,
    remote_total: Optional[int] = None,
    min_rows: Optional[int] = None,
) -> int:
    """Load one VRI day into ClickHouse, optionally skipping already seen IDs."""
    total = 0
    for docs, num_found, page_num, page_rows, page_mode in iter_vri_pages(
        client,
        d,
        rows,
        sleep_s,
        min_rows=min_rows,
    ):
        if remote_total is None:
            remote_total = num_found
        docs_to_insert = docs
        if skip_existing:
            ids = [doc.get("vri_id") for doc in docs if doc.get("vri_id")]
            existing = ch.existing_ids_for_date("verifications", "vri_id", ids, date_col="verification_date", day=d)
            if existing:
                docs_to_insert = [doc for doc in docs if doc.get("vri_id") and doc["vri_id"] not in existing]
        rows_to_insert = []
        inserted_at = datetime.now()
        for doc in docs_to_insert:
            row = build_vri_row(doc, inserted_at)
            if row:
                rows_to_insert.append(row)
        if rows_to_insert:
            insert_verifications(ch, rows_to_insert)
            total += len(rows_to_insert)
        log.info(
            "%s: page=%s mode=%s docs=%s rows=%s +%s (loaded=%s / remote=%s)",
            d,
            page_num,
            page_mode,
            len(docs),
            page_rows,
            len(rows_to_insert),
            total,
            remote_total if remote_total is not None else num_found,
        )
    return total
