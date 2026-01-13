"""Backend sync: ingest into fgis_test and transfer to fgis_prod."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional, Iterable

from dotenv import load_dotenv

from fgis_clickhouse.clickhouse_io import CH, ensure_tables
from fgis_clickhouse.fgis_api import FGISClient
from fgis_clickhouse.utils import chunked


load_dotenv()

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger("fgis_backend")


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_pick(*names: str, default: str = "") -> str:
    for name in names:
        if name in os.environ:
            return os.environ[name]
    return default


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


def parse_datetime_any(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, date):
        return datetime(raw.year, raw.month, raw.day)
    text = str(raw).strip()
    if not text:
        return None
    text = text.replace("Z", "")
    if "T" in text:
        text = text.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _state_get(ch: CH, key: str) -> Optional[datetime]:
    try:
        value = ch.scalar(f"SELECT max(last_run) FROM {ch.db}.sync_state WHERE key = '{key}'")
    except Exception:
        return None
    if isinstance(value, datetime):
        return value
    return None


def _state_set(ch: CH, key: str, when: datetime) -> None:
    ch.insert("sync_state", ["key", "last_run"], [(key, when)])


def _format_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def transfer_table(
    ch_src: CH,
    ch_dst: CH,
    *,
    table: str,
    ts_col: str,
    state_key: str,
    dedup: bool = False,
) -> int:
    last = _state_get(ch_dst, state_key)
    where = "1"
    if last:
        where = f"{ts_col} > toDateTime('{_format_dt(last)}')"
    from_clause = f"{ch_src.db}.{table} FINAL" if dedup else f"{ch_src.db}.{table}"
    pending = int(ch_src.scalar(f"SELECT count() FROM {from_clause} WHERE {where}") or 0)
    if pending == 0:
        log.info("TRANSFER %s: no new rows", table)
        return 0
    ch_dst.exec(f"INSERT INTO {ch_dst.db}.{table} SELECT * FROM {from_clause} WHERE {where}")
    max_src = ch_src.scalar(f"SELECT max({ts_col}) FROM {from_clause} WHERE {where}")
    max_dt = parse_datetime_any(max_src) or datetime.now()
    _state_set(ch_dst, state_key, max_dt)
    log.info("TRANSFER %s: inserted %s", table, pending)
    return pending


def promote_vri_range(
    ch_src: CH,
    ch_dst: CH,
    start: date,
    end: date,
    *,
    dedup: bool,
    delete_before_insert: bool,
    verify: bool,
    dry_run: bool,
    max_rows: int,
) -> bool:
    cur = start
    ok = True
    from_clause = f"{ch_src.db}.verifications FINAL" if dedup else f"{ch_src.db}.verifications"
    while cur <= end:
        test_rows = local_vri_rows_range(ch_src, cur, cur, use_final=dedup)
        prod_rows_final = local_vri_rows_range(ch_dst, cur, cur, use_final=dedup)
        prod_rows_raw = (
            local_vri_rows_range(ch_dst, cur, cur, use_final=False) if dedup else prod_rows_final
        )
        needs_sync = test_rows != prod_rows_final or (dedup and prod_rows_raw != prod_rows_final)
        if not needs_sync:
            cur += timedelta(days=1)
            continue
        if max_rows and test_rows > max_rows:
            log.warning("PROMOTE %s: test_rows=%s > max=%s -> skip", cur, test_rows, max_rows)
            ok = False
            cur += timedelta(days=1)
            continue
        log.info(
            "PROMOTE %s: test=%s prod_raw=%s prod_final=%s -> sync",
            cur,
            test_rows,
            prod_rows_raw,
            prod_rows_final,
        )
        if dry_run:
            cur += timedelta(days=1)
            continue
        day_str = cur.strftime("%Y-%m-%d")
        if delete_before_insert:
            ch_dst.exec(
                f"ALTER TABLE {ch_dst.db}.verifications "
                f"DELETE WHERE verification_date = toDate(%(day)s) "
                f"SETTINGS mutations_sync=1",
                {"day": day_str},
            )
        if test_rows > 0:
            ch_dst.exec(
                f"INSERT INTO {ch_dst.db}.verifications "
                f"SELECT * FROM {from_clause} "
                f"WHERE verification_date = toDate(%(day)s)",
                {"day": day_str},
            )
        if verify:
            prod_after = local_vri_rows_range(ch_dst, cur, cur, use_final=dedup)
            prod_after_raw = (
                local_vri_rows_range(ch_dst, cur, cur, use_final=False) if dedup else prod_after
            )
            if prod_after != test_rows or (dedup and prod_after_raw != prod_after):
                log.warning(
                    "PROMOTE %s: mismatch after sync (test=%s prod_final=%s prod_raw=%s)",
                    cur,
                    test_rows,
                    prod_after,
                    prod_after_raw,
                )
                ok = False
        cur += timedelta(days=1)
    return ok


def _should_run(ch: CH, key: str, every: timedelta, now: datetime) -> bool:
    last = _state_get(ch, key)
    if not last:
        return True
    return now - last >= every


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


def local_vri_stats(ch: CH, d: date) -> tuple[int, int]:
    ds = d.strftime("%Y-%m-%d")
    sql = (
        f"SELECT count(), countDistinct(vri_id) FROM {ch.db}.verifications "
        f"WHERE verification_date = toDate('{ds}')"
    )
    try:
        rows = ch.rows(sql)
        if rows:
            return int(rows[0][0] or 0), int(rows[0][1] or 0)
    except Exception:
        return 0, 0
    return 0, 0


def local_vri_rows_range(ch: CH, start: date, end: date, *, use_final: bool = False) -> int:
    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")
    from_clause = f"{ch.db}.verifications FINAL" if use_final else f"{ch.db}.verifications"
    sql = (
        f"SELECT count() FROM {from_clause} "
        f"WHERE verification_date >= toDate('{start_s}') AND verification_date <= toDate('{end_s}')"
    )
    try:
        return int(ch.scalar(sql) or 0)
    except Exception:
        return 0


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
    raise RuntimeError("FGISClient has neither vri_count nor vri_cursor fallback.")


def remote_vri_ids_for_day(
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
) -> set[str]:
    fq = fq_for_day(d)
    cursor_mark = "*"
    ids: set[str] = set()
    while True:
        docs, _num_found, next_cursor = client.vri_cursor(fq=fq, rows=rows, cursor_mark=cursor_mark)
        if not docs:
            break
        for doc in docs:
            vri_id = doc.get("vri_id")
            if vri_id:
                ids.add(str(vri_id))
        if not next_cursor or next_cursor == cursor_mark:
            break
        cursor_mark = next_cursor
        if sleep_s > 0:
            time.sleep(sleep_s)
    return ids


def local_vri_ids_for_day(ch: CH, d: date) -> set[str]:
    ds = d.strftime("%Y-%m-%d")
    sql = f"SELECT vri_id FROM {ch.db}.verifications WHERE verification_date = toDate('{ds}')"
    try:
        rows = ch.rows(sql)
    except Exception:
        return set()
    return {row[0] for row in rows if row and row[0]}


def optimize_verifications_partition(ch: CH, d: date) -> None:
    partition = d.strftime("%Y%m")
    ch.exec(f"OPTIMIZE TABLE {ch.db}.verifications PARTITION {partition} FINAL")


def iter_month_starts(start: date, end: date) -> Iterable[date]:
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def optimize_verifications_range(ch: CH, start: date, end: date) -> None:
    for month_start in iter_month_starts(start, end):
        log.info("DEDUP %s: optimize partition", month_start.strftime("%Y-%m"))
        optimize_verifications_partition(ch, month_start)


def delete_vri_extras_for_day(
    ch: CH,
    client: FGISClient,
    d: date,
    rows: int,
    sleep_s: float,
    *,
    max_remote: int,
    optimize_partition: bool,
    remote_rows: Optional[int] = None,
) -> int:
    if max_remote and remote_rows and remote_rows > max_remote:
        log.warning("DELETE %s: remote rows=%s > max=%s -> skip", d, remote_rows, max_remote)
        return 0
    remote_ids = remote_vri_ids_for_day(client, d, rows, sleep_s)
    if max_remote and len(remote_ids) > max_remote:
        log.warning("DELETE %s: remote ids=%s > max=%s -> skip", d, len(remote_ids), max_remote)
        return 0
    local_ids = local_vri_ids_for_day(ch, d)
    extra_ids = local_ids - remote_ids
    if not extra_ids:
        log.info("DELETE %s: no extra ids", d)
        return 0
    day_str = d.strftime("%Y-%m-%d")
    log.warning("DELETE %s: removing %s extra ids", d, len(extra_ids))
    for group in chunked(list(extra_ids), 1000):
        ch.exec(
            f"ALTER TABLE {ch.db}.verifications "
            f"DELETE WHERE verification_date = toDate(%(day)s) AND vri_id IN %(ids)s "
            f"SETTINGS mutations_sync=1",
            {"day": day_str, "ids": tuple(group)},
        )
    if optimize_partition:
        optimize_verifications_partition(ch, d)
    return len(extra_ids)


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
    stop_on_existing: bool,
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
            log.info("MIT list: page has no new numbers")
            if stop_on_existing:
                log.info("MIT list: stop_on_existing -> stop")
                break
            if not next_cursor or next_cursor == cursor_mark:
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
    remote_total: Optional[int] = None,
) -> int:
    cursor_mark = "*"
    total = 0
    fq = fq_for_day(d)
    while True:
        docs, num_found, next_cursor = client.vri_cursor(fq=fq, rows=rows, cursor_mark=cursor_mark)
        if not docs:
            break
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
            "%s: +%s (loaded=%s / remote=%s)",
            d,
            len(rows_to_insert),
            total,
            remote_total if remote_total is not None else num_found,
        )
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
        fq = fq_for_day(cur)
        remote_rows = remote_vri_count(client, fq)
        if remote_rows == 0:
            cur += timedelta(days=1)
            continue
        local_rows, local_uniq = local_vri_stats(ch, cur)
        local_dup = local_rows - local_uniq
        if local_rows >= remote_rows:
            log.info(
                "VRI %s: OK (local_rows=%s remote_rows=%s uniq=%s dup=%s) -> skip",
                cur,
                local_rows,
                remote_rows,
                local_uniq,
                local_dup,
            )
            cur += timedelta(days=1)
            continue
        log.info(
            "VRI %s: NEED SYNC (local_rows=%s remote_rows=%s uniq=%s dup=%s)",
            cur,
            local_rows,
            remote_rows,
            local_uniq,
            local_dup,
        )
        loaded = sync_vri_day(ch, client, cur, rows, sleep_s, skip_existing, remote_total=remote_rows)
        log.info("VRI %s: loaded=%s", cur, loaded)
        cur += timedelta(days=1)


def parse_backfill_dates(env_value: str) -> list[date]:
    env_value = (env_value or "").strip()
    if not env_value:
        return []
    out: list[date] = []
    for part in env_value.split(","):
        parsed = parse_ymd(part.strip())
        if parsed:
            out.append(parsed)
    return sorted(set(out))


def backfill_dates(
    ch: CH,
    client: FGISClient,
    dates: Iterable[date],
    rows: int,
    sleep_s: float,
    skip_existing: bool,
) -> None:
    for d in dates:
        log.info("BACKFILL %s: start", d)
        sync_vri_range(ch, client, d, d, rows, sleep_s, skip_existing)
        log.info("BACKFILL %s: done", d)


def reconcile_vri_range(
    ch: CH,
    client: FGISClient,
    start: date,
    end: date,
    rows: int,
    sleep_s: float,
    skip_existing: bool,
    *,
    delete_extra: bool,
    delete_max_remote: int,
    optimize_partitions: bool,
    verify_after: bool,
    use_final: bool,
    min_days: int,
    max_depth: int,
    depth: int = 0,
) -> bool:
    if start > end:
        return True
    span = (end - start).days + 1
    fq = fq_for_range(start, end)
    remote_rows = remote_vri_count(client, fq)
    local_rows = local_vri_rows_range(ch, start, end, use_final=use_final)
    if remote_rows == 0:
        if local_rows == 0:
            log.info("RECONCILE %s → %s: remote=0 local=0 -> skip", start, end)
            return True
        log.warning("RECONCILE %s → %s: remote=0 local=%s", start, end, local_rows)
        if not delete_extra:
            return False
        if span <= max(1, min_days) or depth >= max_depth:
            if span == 1:
                delete_vri_extras_for_day(
                    ch,
                    client,
                    start,
                    rows,
                    sleep_s,
                    max_remote=delete_max_remote,
                    optimize_partition=optimize_partitions,
                    remote_rows=remote_rows,
                )
                return True if not verify_after else local_vri_rows_range(ch, start, end, use_final=use_final) == 0
            cur = start
            ok = True
            while cur <= end:
                ok = reconcile_vri_range(
                    ch,
                    client,
                    cur,
                    cur,
                    rows,
                    sleep_s,
                    skip_existing,
                    delete_extra=delete_extra,
                    delete_max_remote=delete_max_remote,
                    optimize_partitions=optimize_partitions,
                    verify_after=verify_after,
                    use_final=use_final,
                    min_days=min_days,
                    max_depth=max_depth,
                    depth=depth + 1,
                ) and ok
                cur += timedelta(days=1)
            return ok
        mid = start + timedelta(days=(span - 1) // 2)
        left_ok = reconcile_vri_range(
            ch,
            client,
            start,
            mid,
            rows,
            sleep_s,
            skip_existing,
            delete_extra=delete_extra,
            delete_max_remote=delete_max_remote,
            optimize_partitions=optimize_partitions,
            verify_after=verify_after,
            use_final=use_final,
            min_days=min_days,
            max_depth=max_depth,
            depth=depth + 1,
        )
        right_ok = reconcile_vri_range(
            ch,
            client,
            mid + timedelta(days=1),
            end,
            rows,
            sleep_s,
            skip_existing,
            delete_extra=delete_extra,
            delete_max_remote=delete_max_remote,
            optimize_partitions=optimize_partitions,
            verify_after=verify_after,
            use_final=use_final,
            min_days=min_days,
            max_depth=max_depth,
            depth=depth + 1,
        )
        return left_ok and right_ok
    if local_rows == remote_rows:
        log.info("RECONCILE %s → %s: OK (local=%s remote=%s)", start, end, local_rows, remote_rows)
        return True
    if span <= max(1, min_days) or depth >= max_depth:
        if local_rows < remote_rows:
            log.info(
                "RECONCILE %s → %s: NEED SYNC (local=%s remote=%s delta=%s span=%s)",
                start,
                end,
                local_rows,
                remote_rows,
                remote_rows - local_rows,
                span,
            )
            if span == 1:
                log.info("RECONCILE %s: backfill day", start)
                sync_vri_day(ch, client, start, rows, sleep_s, skip_existing, remote_total=remote_rows)
            else:
                log.info("RECONCILE %s → %s: backfill range", start, end)
                sync_vri_range(ch, client, start, end, rows, sleep_s, skip_existing)
        else:
            log.warning(
                "RECONCILE %s → %s: local>%s remote=%s delta=%s",
                start,
                end,
                local_rows,
                remote_rows,
                local_rows - remote_rows,
            )
            if not delete_extra:
                return False
            if span == 1:
                delete_vri_extras_for_day(
                    ch,
                    client,
                    start,
                    rows,
                    sleep_s,
                    max_remote=delete_max_remote,
                    optimize_partition=optimize_partitions,
                    remote_rows=remote_rows,
                )
            else:
                cur = start
                ok = True
                while cur <= end:
                    ok = reconcile_vri_range(
                        ch,
                        client,
                        cur,
                        cur,
                        rows,
                        sleep_s,
                        skip_existing,
                        delete_extra=delete_extra,
                        delete_max_remote=delete_max_remote,
                        optimize_partitions=optimize_partitions,
                        verify_after=verify_after,
                        use_final=use_final,
                        min_days=min_days,
                        max_depth=max_depth,
                        depth=depth + 1,
                    ) and ok
                    cur += timedelta(days=1)
                return ok
        if verify_after:
            local_after = local_vri_rows_range(ch, start, end, use_final=use_final)
            if local_after > remote_rows and delete_extra and span == 1:
                delete_vri_extras_for_day(
                    ch,
                    client,
                    start,
                    rows,
                    sleep_s,
                    max_remote=delete_max_remote,
                    optimize_partition=optimize_partitions,
                    remote_rows=remote_rows,
                )
                local_after = local_vri_rows_range(ch, start, end, use_final=use_final)
            if local_after != remote_rows:
                log.warning(
                    "RECONCILE %s → %s: mismatch after fix (local=%s remote=%s)",
                    start,
                    end,
                    local_after,
                    remote_rows,
                )
                return False
        return True
    mid = start + timedelta(days=(span - 1) // 2)
    left_ok = reconcile_vri_range(
        ch,
        client,
        start,
        mid,
        rows,
        sleep_s,
        skip_existing,
        delete_extra=delete_extra,
        delete_max_remote=delete_max_remote,
        optimize_partitions=optimize_partitions,
        verify_after=verify_after,
        use_final=use_final,
        min_days=min_days,
        max_depth=max_depth,
        depth=depth + 1,
    )
    right_ok = reconcile_vri_range(
        ch,
        client,
        mid + timedelta(days=1),
        end,
        rows,
        sleep_s,
        skip_existing,
        delete_extra=delete_extra,
        delete_max_remote=delete_max_remote,
        optimize_partitions=optimize_partitions,
        verify_after=verify_after,
        use_final=use_final,
        min_days=min_days,
        max_depth=max_depth,
        depth=depth + 1,
    )
    return left_ok and right_ok


def sync_vri_scheduled(
    ch: CH,
    client: FGISClient,
    rows: int,
    sleep_s: float,
    skip_existing: bool,
    start_date: Optional[date],
    end_date: date,
) -> None:
    now = datetime.now()
    schedule = [
        ("vri_hourly", _env_int("VRI_HOURLY_EVERY_HOURS", 1), _env_int("VRI_HOURLY_DAYS", 7)),
        ("vri_daily", _env_int("VRI_DAILY_EVERY_HOURS", 24), _env_int("VRI_DAILY_DAYS", 30)),
        ("vri_weekly", _env_int("VRI_WEEKLY_EVERY_HOURS", 168), _env_int("VRI_WEEKLY_DAYS", 60)),
        ("vri_monthly", _env_int("VRI_MONTHLY_EVERY_HOURS", 720), _env_int("VRI_MONTHLY_DAYS", 180)),
    ]

    for key, every_hours, days in schedule:
        if every_hours <= 0 or days <= 0:
            continue
        if not _should_run(ch, key, timedelta(hours=every_hours), now):
            continue
        range_start = now.date() - timedelta(days=days)
        if start_date:
            range_start = max(range_start, start_date)
        range_end = min(end_date, now.date())
        if range_start > range_end:
            continue
        log.info("VRI schedule %s: %s → %s", key, range_start, range_end)
        sync_vri_range(ch, client, range_start, range_end, rows, sleep_s, skip_existing)
        _state_set(ch, key, datetime.now())


def main() -> None:
    host = _env_pick("CH_HOST", default="127.0.0.1")
    port = int(_env_pick("CH_PORT", default="9001"))
    user = _env_pick("CH_USER_INGEST", "CH_USER", "CH_USER_READ", default="default")
    password = _env_pick("CH_PASS_INGEST", "CH_PASS", "CH_PASS_READ", default="")
    db_test = _env_pick("CH_DB_TEST", "CH_DB", default="fgis_test").strip() or "fgis_test"
    db_prod = _env_pick("CH_DB_PROD", default="fgis_prod").strip() or "fgis_prod"

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
    backfill_env = os.getenv("BACKFILL_DATES", "")
    backfill_list = parse_backfill_dates(backfill_env)

    sync_mit = _env_flag("SYNC_MIT", True)
    sync_vri = _env_flag("SYNC_VRI", True)
    reconcile_vri = _env_flag("RECONCILE_VRI", False)
    fetch_details = _env_flag("MIT_FETCH_DETAILS", True)
    skip_existing_vri = _env_flag("VRI_SKIP_EXISTING", True)
    vri_scheduled = _env_flag("VRI_SCHEDULED", True)
    mit_full_scan = _env_flag("MIT_FULL_SCAN", False)
    mit_every_hours = _env_int("MIT_EVERY_HOURS", 3)
    transfer_enabled = _env_flag("TRANSFER_TO_PROD", True)
    transfer_every_hours = _env_int("TRANSFER_EVERY_HOURS", 1)
    transfer_force = _env_flag("TRANSFER_FORCE", False)
    reconcile_start_env = os.getenv("RECONCILE_START_DATE", "").strip()
    reconcile_end_env = os.getenv("RECONCILE_END_DATE", "").strip()
    reconcile_start = parse_ymd(reconcile_start_env) if reconcile_start_env else start_date
    reconcile_end = parse_ymd(reconcile_end_env) if reconcile_end_env else end_date
    reconcile_min_days = _env_int("RECONCILE_MIN_DAYS", 1)
    reconcile_max_depth = _env_int("RECONCILE_MAX_DEPTH", 20)
    reconcile_delete_extra = _env_flag("RECONCILE_DELETE_EXTRA", False)
    reconcile_delete_max_remote = _env_int("RECONCILE_DELETE_MAX_REMOTE", 0)
    reconcile_optimize_final = _env_flag("RECONCILE_OPTIMIZE_FINAL", False)
    reconcile_verify = _env_flag("RECONCILE_VERIFY", True)
    reconcile_use_final = _env_flag("RECONCILE_USE_FINAL", False)
    reconcile_dedup_final = _env_flag("RECONCILE_DEDUP_FINAL", False)
    transfer_dedup = _env_flag("TRANSFER_DEDUP", False)
    promote_vri = _env_flag("PROMOTE_VRI", False)
    promote_start_env = os.getenv("PROMOTE_START_DATE", "").strip()
    promote_end_env = os.getenv("PROMOTE_END_DATE", "").strip()
    promote_start = parse_ymd(promote_start_env) if promote_start_env else None
    promote_end = parse_ymd(promote_end_env) if promote_end_env else None
    promote_dedup = _env_flag("PROMOTE_DEDUP", True)
    promote_delete = _env_flag("PROMOTE_DELETE", True)
    promote_verify = _env_flag("PROMOTE_VERIFY", True)
    promote_dry_run = _env_flag("PROMOTE_DRY_RUN", False)
    promote_max_rows = _env_int("PROMOTE_MAX_ROWS", 0)
    if not promote_start:
        promote_start = reconcile_start or start_date
    if not promote_end:
        promote_end = reconcile_end or end_date

    ch_admin = CH(host, port, user, password, "default")
    ensure_tables(ch_admin, db_test)
    ensure_tables(ch_admin, db_prod)
    ch_test = CH(host, port, user, password, db_test)
    ch_prod = CH(host, port, user, password, db_prod)

    client = FGISClient(proxy=proxy, rps=rps)

    now = datetime.now()
    run_ok = True
    reconcile_ok = True

    if backfill_list:
        log.info("BACKFILL: %s dates", len(backfill_list))
        try:
            backfill_dates(ch_test, client, backfill_list, vri_rows, vri_sleep, skip_existing_vri)
        except Exception:
            run_ok = False
            log.exception("BACKFILL: failed")

    if reconcile_vri:
        if not reconcile_start:
            log.warning("RECONCILE: start date is empty -> skip")
        elif not reconcile_end or reconcile_start > reconcile_end:
            log.warning("RECONCILE: invalid date range -> skip")
        else:
            log.info("RECONCILE: start")
            try:
                reconcile_ok = reconcile_vri_range(
                    ch_test,
                    client,
                    reconcile_start,
                    reconcile_end,
                    vri_rows,
                    vri_sleep,
                    skip_existing_vri,
                    delete_extra=reconcile_delete_extra,
                    delete_max_remote=reconcile_delete_max_remote,
                    optimize_partitions=reconcile_optimize_final,
                    verify_after=reconcile_verify,
                    use_final=reconcile_use_final,
                    min_days=reconcile_min_days,
                    max_depth=reconcile_max_depth,
                )
                log.info("RECONCILE: done")
                if reconcile_ok and reconcile_dedup_final:
                    log.info("DEDUP: optimize partitions in range")
                    optimize_verifications_range(ch_test, reconcile_start, reconcile_end)
                if not reconcile_ok:
                    run_ok = False
                    log.warning("RECONCILE: mismatches remain -> transfer skipped")
            except Exception:
                run_ok = False
                log.exception("RECONCILE: failed")

    if sync_mit:
        if mit_every_hours <= 0 or _should_run(ch_test, "mit_registry", timedelta(hours=mit_every_hours), now):
            log.info("MIT registry sync (test): start")
            try:
                total = sync_mit_registry(ch_test, client, mit_rows, mit_sleep, fetch_details, not mit_full_scan)
                log.info("MIT registry sync (test): done (inserted=%s)", total)
                _state_set(ch_test, "mit_registry", datetime.now())
            except Exception:
                run_ok = False
                log.exception("MIT registry sync (test): failed")
        else:
            log.info("MIT registry sync (test): skipped by schedule")

    if sync_vri:
        log.info("VRI sync (test): start")
        try:
            if vri_scheduled:
                sync_vri_scheduled(ch_test, client, vri_rows, vri_sleep, skip_existing_vri, start_date, end_date)
            else:
                start = pick_start_date(ch_test, start_date, tail_days)
                if not start:
                    log.info("VRI sync (test): start date is empty -> skip")
                elif start > end_date:
                    log.info("VRI sync (test): start date after end date -> skip")
                else:
                    sync_vri_range(ch_test, client, start, end_date, vri_rows, vri_sleep, skip_existing_vri)
            log.info("VRI sync (test): done")
        except Exception:
            run_ok = False
            log.exception("VRI sync (test): failed")

    if transfer_enabled:
        if transfer_every_hours <= 0 and not transfer_force:
            log.info("TRANSFER: disabled by schedule")
        elif not transfer_force and not _should_run(ch_prod, "transfer_prod", timedelta(hours=transfer_every_hours), now):
            log.info("TRANSFER: skipped by schedule")
        elif not run_ok:
            log.warning("TRANSFER: skipped because test sync failed")
        else:
            log.info("TRANSFER: start")
            try:
                transfer_table(
                    ch_test,
                    ch_prod,
                    table="mit_registry",
                    ts_col="inserted_at",
                    state_key="transfer_mit",
                    dedup=transfer_dedup,
                )
                if promote_vri:
                    if not reconcile_vri:
                        log.warning("PROMOTE: reconcile disabled -> skip")
                        run_ok = False
                    elif not reconcile_ok:
                        log.warning("PROMOTE: skipped because reconcile not OK")
                        run_ok = False
                    elif not promote_start or not promote_end or promote_start > promote_end:
                        log.warning("PROMOTE: invalid date range -> skip")
                        run_ok = False
                    else:
                        promote_ok = promote_vri_range(
                            ch_test,
                            ch_prod,
                            promote_start,
                            promote_end,
                            dedup=promote_dedup,
                            delete_before_insert=promote_delete,
                            verify=promote_verify,
                            dry_run=promote_dry_run,
                            max_rows=promote_max_rows,
                        )
                        if not promote_ok:
                            run_ok = False
                            log.warning("PROMOTE: mismatches remain")
                        elif not promote_dry_run:
                            _state_set(ch_prod, "transfer_vri", datetime.now())
                else:
                    transfer_table(
                        ch_test,
                        ch_prod,
                        table="verifications",
                        ts_col="inserted_at",
                        state_key="transfer_vri",
                        dedup=transfer_dedup,
                    )
                _state_set(ch_prod, "transfer_prod", datetime.now())
                log.info("TRANSFER: done")
            except Exception:
                log.exception("TRANSFER: failed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped.")
