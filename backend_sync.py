"""Backend sync: incremental MIT registry + VRI updates."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

from dotenv import load_dotenv

from fgis_clickhouse.clickhouse_io import CH, ensure_tables
from fgis_clickhouse.fgis_api import FGISClient


load_dotenv()

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger("fgis_backend")


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no"}


def parse_ymd(raw: str) -> Optional[date]:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_date_any(raw: Any) -> Optional[date]:
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


def fq_for_day(d: date) -> str:
    ds = d.strftime("%Y-%m-%d")
    return f"verification_date:[{ds}T00:00:00Z TO {ds}T23:59:59Z]"


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


def local_vri_count(ch: CH, d: date) -> int:
    ds = d.strftime("%Y-%m-%d")
    sql = f"SELECT countDistinct(vri_id) FROM {ch.db}.verifications WHERE verification_date = toDate('{ds}')"
    try:
        return int(ch.scalar(sql) or 0)
    except Exception:
        return 0


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
    return (
        country,
        inserted_at,
        is_actual,
        manufacturer,
        mit_number,
        mit_title,
        mpi,
        notation,
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
            "order_date",
            "order_num",
            "production_type",
            "valid_to",
        ],
        rows,
    )


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


def sync_mit_registry(
    ch: CH,
    client: FGISClient,
    rows: int,
    sleep_s: float,
    fetch_details: bool,
) -> int:
    cursor_mark = "*"
    total = 0
    while True:
        docs, next_cursor = client.mit_list_cursor(cursor_mark=cursor_mark, rows=rows)
        if not docs:
            break
        numbers = [doc.get("number") for doc in docs if doc.get("number")]
        existing = ch.existing_ids("mit_registry", "mit_number", numbers) if numbers else set()
        missing_docs = [doc for doc in docs if doc.get("number") and doc.get("number") not in existing]
        if not missing_docs:
            log.info("MIT list: page has no new numbers -> stop")
            break
        buffer: list[tuple[Any, ...]] = []
        inserted_now = 0
        for doc in missing_docs:
            mit_uuid = doc.get("mit_uuid")
            details = client.mit_details(mit_uuid) if fetch_details and mit_uuid else {}
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
        log.info("MIT list: inserted %s (total=%s)", inserted_now, total)
        if not next_cursor or next_cursor == cursor_mark:
            break
        cursor_mark = next_cursor
        if sleep_s > 0:
            time.sleep(sleep_s)
    return total


def sync_vri_day(
    ch: CH,
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
    skip_existing: bool,
) -> int:
    cursor_mark = "*"
    total = 0
    fq = fq_for_day(d)
    while True:
        docs, num_found, next_cursor = client.vri_cursor(fq=fq, rows=rows, cursor_mark=cursor_mark)
        if not docs:
            break
        docs_to_insert = docs
        if skip_existing:
            ids = [doc.get("vri_id") for doc in docs if doc.get("vri_id")]
            existing = ch.existing_ids("verifications", "vri_id", ids)
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
        log.info("%s: +%s (total=%s/%s)", d, len(rows_to_insert), total, num_found)
        if not next_cursor or next_cursor == cursor_mark:
            break
        cursor_mark = next_cursor
        if sleep_s > 0:
            time.sleep(sleep_s)
    return total


def sync_vri_range(
    ch: CH,
    client: FGISClient,
    start: date,
    end: date,
    rows: int,
    sleep_s: float,
    skip_existing: bool,
) -> None:
    cur = start
    while cur <= end:
        remote_cnt = client.vri_count(fq_for_day(cur))
        if remote_cnt == 0:
            cur += timedelta(days=1)
            continue
        local_cnt = local_vri_count(ch, cur)
        if local_cnt >= remote_cnt:
            cur += timedelta(days=1)
            continue
        log.info("VRI %s: local=%s remote=%s -> sync", cur, local_cnt, remote_cnt)
        loaded = sync_vri_day(ch, client, cur, rows, sleep_s, skip_existing)
        log.info("VRI %s: loaded=%s", cur, loaded)
        cur += timedelta(days=1)


def main() -> None:
    host = os.getenv("CH_HOST", "127.0.0.1")
    port = int(os.getenv("CH_PORT", "9001"))
    user = os.getenv("CH_USER_INGEST") or os.getenv("CH_USER_READ") or "default"
    password = os.getenv("CH_PASS_INGEST") or os.getenv("CH_PASS_READ") or ""
    database = os.getenv("CH_DB", "fgis")

    rps = float(os.getenv("FGIS_RPS", "1.0"))
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None

    mit_rows = int(os.getenv("MIT_ROWS", "1000"))
    vri_rows = int(os.getenv("VRI_ROWS", "1000"))
    mit_sleep = float(os.getenv("MIT_SLEEP", "0.2"))
    vri_sleep = float(os.getenv("VRI_SLEEP", "0.2"))
    tail_days = int(os.getenv("VRI_TAIL_DAYS", "3"))
    start_date_env = os.getenv("START_DATE", "").strip()
    end_date_env = os.getenv("END_DATE", "").strip()
    start_date = parse_ymd(start_date_env) if start_date_env else None
    end_date = parse_ymd(end_date_env) if end_date_env else date.today()
    if end_date is None:
        end_date = date.today()

    sync_mit = _env_flag("SYNC_MIT", True)
    sync_vri = _env_flag("SYNC_VRI", True)
    fetch_details = _env_flag("MIT_FETCH_DETAILS", True)
    skip_existing_vri = _env_flag("VRI_SKIP_EXISTING", True)

    ch = CH(host, port, user, password, database)
    ensure_tables(ch)

    client = FGISClient(proxy=proxy, rps=rps)

    if sync_mit:
        log.info("MIT registry sync: start")
        total = sync_mit_registry(ch, client, mit_rows, mit_sleep, fetch_details)
        log.info("MIT registry sync: done (inserted=%s)", total)

    if sync_vri:
        log.info("VRI sync: start")
        start = pick_start_date(ch, start_date, tail_days)
        if not start:
            log.info("VRI sync: start date is empty -> skip")
        elif start > end_date:
            log.info("VRI sync: start date after end date -> skip")
        else:
            sync_vri_range(ch, client, start, end_date, vri_rows, vri_sleep, skip_existing_vri)
        log.info("VRI sync: done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped.")
