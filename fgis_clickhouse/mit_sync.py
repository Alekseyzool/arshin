"""MIT registry extraction and ClickHouse loading."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from typing import Any, Optional

from .clickhouse_io import CH
from .dates import parse_date_any
from .fgis_api import FGISClient


log = logging.getLogger("fgis_backend")

def _json_loads_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return None
    return None

def _extract_text(value: Any, keys: tuple[str, ...]) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in keys:
            val = value.get(key)
            if val:
                return str(val).strip()
        for val in value.values():
            if isinstance(val, str) and val.strip():
                return val.strip()
        return ""
    if isinstance(value, list):
        for item in value:
            text = _extract_text(item, keys)
            if text:
                return text
    return ""

def detect_country(manuf_str: str) -> str:
    if not manuf_str:
        return "Не указано"
    s = str(manuf_str).upper()
    rules = [
        ("РОССИЯ", "Россия"),
        ("RUSSIA", "Россия"),
        ("БЕЛАРУС", "Беларусь"),
        ("BELARUS", "Беларусь"),
        ("КАЗАХСТАН", "Казахстан"),
        ("KAZAKH", "Казахстан"),
        ("КИТАЙ", "Китай"),
        ("CHINA", "Китай"),
        ("США", "США"),
        ("USA", "США"),
        ("UNITED STATES", "США"),
        ("ГЕРМАН", "Германия"),
        ("GERMANY", "Германия"),
        ("GMBH", "Германия"),
        ("ЯПОНИ", "Япония"),
        ("JAPAN", "Япония"),
        ("ВЕЛИКОБРИТАН", "Великобритания"),
        ("UNITED KINGDOM", "Великобритания"),
        (" UK", "Великобритания"),
        ("ФРАНЦ", "Франция"),
        ("FRANCE", "Франция"),
        ("ИТАЛ", "Италия"),
        ("ITALY", "Италия"),
        ("ТАЙВАН", "Тайвань"),
        ("TAIWAN", "Тайвань"),
        ("КОРЕЯ", "Корея"),
        ("KOREA", "Корея"),
        ("МАЛАЙЗ", "Малайзия"),
        ("MALAYSIA", "Малайзия"),
        ("ШВЕЙЦАР", "Швейцария"),
        ("SWITZERLAND", "Швейцария"),
    ]
    for needle, country in rules:
        if needle in s:
            return country
    return "Прочие"

def _extract_manufacturer(details: dict[str, Any], list_doc: dict[str, Any]) -> tuple[str, Any]:
    for key in ("manufacturers", "manufacturer", "j_manufacturers"):
        val = details.get(key)
        if not val:
            continue
        parsed = _json_loads_maybe(val)
        if parsed is not None:
            name = _extract_text(parsed, ("title", "name", "manufacturer", "org_title", "orgTitle"))
            if name:
                return name, parsed
        if isinstance(val, str) and val.strip():
            return val.strip(), val
    list_val = list_doc.get("manufacturers")
    if list_val:
        return str(list_val).strip(), list_val
    return "", None

def _extract_country(source: Any, fallback_name: str) -> str:
    if isinstance(source, dict):
        for key in ("country", "countryTitle", "country_name", "countryName"):
            val = source.get(key)
            if val:
                return str(val).strip()
    if isinstance(source, list):
        for item in source:
            val = _extract_country(item, "")
            if val:
                return val
    if fallback_name:
        return detect_country(fallback_name)
    return "Не указано"

def _clean_scalar_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text in {"", "[]", "{}", "null", "None"}:
        return ""
    return text

def _extract_modifications(details: dict[str, Any]) -> str:
    for key in ("j_modification", "modification", "modifications"):
        val = details.get(key)
        if not val:
            continue

        parsed = _json_loads_maybe(val)
        source = parsed if parsed is not None else val
        parts: list[str] = []

        if isinstance(source, list):
            for item in source:
                if isinstance(item, dict):
                    text = _extract_text(item, ("modification", "name", "title", "value"))
                else:
                    text = _clean_scalar_text(item)
                if text:
                    parts.append(text)
        elif isinstance(source, dict):
            text = _extract_text(source, ("modification", "name", "title", "value"))
            if text:
                parts.append(text)
        else:
            text = _clean_scalar_text(source)
            if text:
                parts.append(text)

        if parts:
            return ", ".join(parts)

    return ""

def _extract_mpi(details: dict[str, Any]) -> str:
    for key in ("mpi", "mpis", "j_mpis"):
        val = details.get(key)
        if not val:
            continue
        parsed = _json_loads_maybe(val)
        if parsed is not None:
            text = _extract_text(parsed, ("mpi", "name", "title"))
            if text:
                return text
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""

def _extract_order(details: dict[str, Any]) -> tuple[str, Optional[date]]:
    for key in ("orders", "j_orders"):
        val = details.get(key)
        if not val:
            continue
        parsed = _json_loads_maybe(val)
        if parsed is None:
            parsed = val
        if isinstance(parsed, list) and parsed:
            parsed = parsed[0]
        order_num = _extract_text(parsed, ("order_num", "orderNum", "orderNumber", "num", "number"))
        order_date_raw = _extract_text(parsed, ("order_date", "orderDate", "date"))
        order_date = parse_date_any(order_date_raw) if order_date_raw else None
        if order_num or order_date:
            return order_num, order_date
    return "", None

def build_mit_row(list_doc: dict[str, Any], details: dict[str, Any], inserted_at: datetime) -> Optional[tuple[Any, ...]]:
    mit_number = (details.get("number") or list_doc.get("number") or "").strip()
    if not mit_number:
        return None
    manufacturer, manuf_source = _extract_manufacturer(details, list_doc)
    country = _extract_country(manuf_source, manufacturer)
    mit_title = (details.get("title") or list_doc.get("title") or "").strip()
    notation = (details.get("notation") or list_doc.get("notation") or "").strip()
    production_type = int(details.get("production_type") or details.get("productionType") or 0)
    is_actual = 1 if details.get("is_actual") or details.get("isActual") else 0
    valid_to = parse_date_any(details.get("valid_to") or details.get("validTo"))
    mpi = _extract_mpi(details)
    order_num, order_date = _extract_order(details)
    j_mod = _extract_modifications(details)

    return (
        country,
        inserted_at,
        is_actual,
        manufacturer,
        mit_number,
        mit_title,
        mpi,
        notation,
        j_mod,
        order_date,
        order_num,
        production_type,
        valid_to,
    )

def insert_mit_registry(ch: CH, rows: list[tuple[Any, ...]]) -> None:
    ch.insert(
        "mit_registry",
        [
            "country",
            "inserted_at",
            "is_actual",
            "manufacturer",
            "mit_number",
            "mit_title",
            "mpi",
            "notation",
            "j_modification",
            "order_date",
            "order_num",
            "production_type",
            "valid_to",
        ],
        rows,
    )

def sync_mit_registry(
    ch: CH,
    client: FGISClient,
    rows: int,
    sleep_s: float,
    fetch_details: bool,
    stop_on_existing: bool,
    empty_pages_limit: int = 0,
    min_rows: int = 10,
) -> int:
    """Insert missing MIT rows discovered by cursor-scanning the registry.

    Existing MIT numbers are skipped before details are requested, so regular
    runs scan the lightweight list endpoint and only pay the details cost for
    new instruments.
    """
    cursor_mark = "*"
    total = 0
    page_num = 0
    empty_pages_in_row = 0
    stop_reason = "completed"
    current_rows = max(rows, 1)
    min_rows = max(1, min(min_rows, current_rows))
    log.info(
        "MIT list: start rows=%s min_rows=%s fetch_details=%s stop_on_existing=%s empty_pages_limit=%s",
        current_rows,
        min_rows,
        fetch_details,
        stop_on_existing,
        max(empty_pages_limit, 0),
    )
    while True:
        page_num += 1
        page_cursor = cursor_mark
        page_rows = current_rows
        while True:
            try:
                docs, next_cursor = client.mit_list_cursor(cursor_mark=cursor_mark, rows=page_rows)
                if page_rows != current_rows:
                    current_rows = page_rows
                    log.info(
                        "MIT list: page=%s cursor=%s fetch recovered with rows=%s",
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
                    log.warning(
                        "MIT list: page=%s cursor=%s fetch failed with rows=%s: %s -> retry rows=%s",
                        page_num,
                        page_cursor,
                        page_rows,
                        exc,
                        next_rows,
                    )
                    page_rows = next_rows
                    continue
                log.error(
                    "MIT list: page=%s cursor=%s fetch failed with rows=%s: %s",
                    page_num,
                    page_cursor,
                    page_rows,
                    exc,
                )
                raise RuntimeError(
                    f"MIT list fetch failed on page={page_num} cursor={page_cursor} rows={page_rows}: {exc}"
                ) from exc
        if not docs:
            stop_reason = f"page={page_num}: empty response"
            log.info(
                "MIT list: page=%s cursor=%s docs=0 existing=0 missing=0 next_cursor=%s -> stop (%s)",
                page_num,
                page_cursor,
                next_cursor or "",
                stop_reason,
            )
            break
        numbers = [doc.get("number") for doc in docs if doc.get("number")]
        existing = ch.existing_ids("mit_registry", "mit_number", numbers) if numbers else set()
        missing_docs = [doc for doc in docs if doc.get("number") and doc.get("number") not in existing]
        existing_count = len(existing)
        missing_count = len(missing_docs)
        if missing_count == 0:
            empty_pages_in_row += 1
        else:
            empty_pages_in_row = 0
        log.info(
            "MIT list: page=%s cursor=%s docs=%s existing=%s missing=%s next_cursor=%s empty_streak=%s",
            page_num,
            page_cursor,
            len(docs),
            existing_count,
            missing_count,
            next_cursor or "",
            empty_pages_in_row,
        )
        if not missing_docs:
            if stop_on_existing and empty_pages_limit > 0 and empty_pages_in_row >= empty_pages_limit:
                stop_reason = (
                    f"page={page_num}: {empty_pages_in_row} consecutive pages without new mit_number"
                )
                log.info("MIT list: stop_on_existing threshold reached -> stop (%s)", stop_reason)
                break
            if not next_cursor or next_cursor == cursor_mark:
                stop_reason = f"page={page_num}: cursor exhausted after empty page"
                log.info("MIT list: cursor exhausted -> stop (%s)", stop_reason)
                break
            cursor_mark = next_cursor
            if sleep_s > 0:
                time.sleep(sleep_s)
            continue
        buffer: list[tuple[Any, ...]] = []
        inserted_now = 0
        for doc in missing_docs:
            mit_uuid = doc.get("mit_uuid")
            details = {}
            if fetch_details and mit_uuid:
                try:
                    details = client.mit_details(mit_uuid)
                except Exception as exc:
                    log.warning("MIT details failed for %s: %s", mit_uuid, exc)
            row = build_mit_row(doc, details, datetime.now())
            if row:
                buffer.append(row)
            if len(buffer) >= ch.batch_size:
                insert_mit_registry(ch, buffer)
                inserted_now += len(buffer)
                total += len(buffer)
                buffer.clear()
            if sleep_s > 0:
                time.sleep(sleep_s)
        if buffer:
            insert_mit_registry(ch, buffer)
            inserted_now += len(buffer)
            total += len(buffer)
        log.info("MIT list: page=%s inserted=%s total=%s", page_num, inserted_now, total)
        if not next_cursor or next_cursor == cursor_mark:
            stop_reason = f"page={page_num}: cursor exhausted after inserts"
            log.info("MIT list: cursor exhausted -> stop (%s)", stop_reason)
            break
        cursor_mark = next_cursor
        if sleep_s > 0:
            time.sleep(sleep_s)
    log.info("MIT list: done pages=%s inserted_total=%s stop_reason=%s", page_num, total, stop_reason)
    return total
