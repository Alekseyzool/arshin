"""Insert helpers that convert payload dictionaries into ClickHouse rows."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .clickhouse_io import CH
from .parsing import parse_vri_payload
from .utils import h64, parse_dt_value


def _with_meta(rows: Iterable[Tuple[Any, ...]], run_id: str, tag: str) -> List[Tuple[Any, ...]]:
    return [row + (run_id, tag) for row in rows]


def insert_vri_search(ch: CH, docs: Iterable[Dict[str, Any]], run_id: str, tag: str) -> int:
    """Insert VRI search rows into ClickHouse."""
    rows = [
        (
            doc.get("vri_id", "") or "",
            doc.get("org_title", "") or "",
            doc.get("mi.mitnumber", "") or "",
            doc.get("mi.mititle", "") or "",
            doc.get("mi.mitype", "") or "",
            doc.get("mi.modification", "") or "",
            doc.get("mi.number", "") or "",
            parse_dt_value(doc.get("verification_date")),
            parse_dt_value(doc.get("valid_date")),
            1 if doc.get("applicability") else 0,
            doc.get("result_docnum", "") or "",
            doc.get("sticker_num", "") or "",
            run_id,
            tag,
        )
        for doc in docs
    ]
    ch.insert(
        "vri_search_raw",
        [
            "vri_id",
            "org_title",
            "mi_mitnumber",
            "mi_mititle",
            "mi_mitype",
            "mi_modification",
            "mi_number",
            "verification_date",
            "valid_date",
            "applicability",
            "result_docnum",
            "sticker_num",
            "run_id",
            "source_tag",
        ],
        rows,
    )
    return len(rows)


def insert_vri_details(ch: CH, pairs: Sequence[Tuple[str, Dict[str, Any]]], run_id: str, tag: str) -> Tuple[int, int, int]:
    """Insert raw payloads and parsed rows for the provided VRI ids."""
    if not pairs:
        return 0, 0, 0

    raw_rows = []
    vri_ids: List[str] = []
    for vri_id, payload in pairs:
        payload_json = json.dumps(payload, ensure_ascii=False)
        raw_rows.append((vri_id, payload_json, h64(payload_json), run_id, tag))
        vri_ids.append(vri_id)
    ch.insert("vri_details_raw", ["vri_id", "payload_json", "payload_hash", "run_id", "source_tag"], raw_rows)

    existing = ch.existing_ids("vri_details", "vri_id", vri_ids)
    to_parse = [(vri_id, payload) for vri_id, payload in pairs if vri_id not in existing]
    if not to_parse:
        return 0, 0, 0

    details_rows: List[Tuple[Any, ...]] = []
    mieta_rows: List[Tuple[Any, ...]] = []
    mis_rows: List[Tuple[Any, ...]] = []

    for vri_id, payload in to_parse:
        details_row, items_mieta, items_mis = parse_vri_payload(vri_id, payload)
        details_rows.append(details_row)
        mieta_rows.extend(items_mieta)
        mis_rows.extend(items_mis)

    ch.insert(
        "vri_details",
        [
            "vri_id",
            "mitype_number",
            "mitype_type",
            "mitype_title",
            "mitype_url",
            "manufacture_num",
            "manufacture_year",
            "modification",
            "org_title",
            "sign_cipher",
            "mi_owner",
            "vri_type",
            "vri_type_label",
            "vrf_date",
            "valid_date",
            "doc_title",
            "applicable_cert_num",
            "applicable_sign_pass",
            "applicable_sign_mi",
            "brief_indicator",
            "run_id",
            "source_tag",
        ],
        _with_meta(details_rows, run_id, tag),
    )

    ch.insert(
        "vri_mieta",
        [
            "vri_id",
            "reg_number",
            "mitype_number",
            "mitype_title",
            "mitype_url",
            "notation",
            "modification",
            "manufacture_num",
            "manufacture_year",
            "rank_code",
            "rank_title",
            "schema_title",
            "row_hash",
            "run_id",
            "source_tag",
        ],
        _with_meta(mieta_rows, run_id, tag),
    )

    ch.insert(
        "vri_mis",
        [
            "vri_id",
            "mitype_number",
            "mitype_title",
            "mitype_url",
            "number",
            "row_hash",
            "run_id",
            "source_tag",
        ],
        _with_meta(mis_rows, run_id, tag),
    )
    return len(details_rows), len(mieta_rows), len(mis_rows)


def insert_mit_search(ch: CH, docs: Iterable[Dict[str, Any]], run_id: str, tag: str) -> int:
    """Insert MIT search results into ClickHouse."""
    rows = [
        (
            doc.get("mit_uuid", "") or "",
            doc.get("number", "") or "",
            doc.get("title", "") or "",
            doc.get("notation", "") or "",
            doc.get("manufacturers", "") or "",
            int(doc.get("num1") or 0),
            int(doc.get("num2") or 0),
            run_id,
            tag,
        )
        for doc in docs
    ]
    ch.insert(
        "mit_search_raw",
        ["mit_uuid", "number", "title", "notation", "manufacturers", "num1", "num2", "run_id", "source_tag"],
        rows,
    )
    return len(rows)


def insert_mit_details(ch: CH, pairs: Sequence[Tuple[str, Dict[str, Any]]], run_id: str, tag: str) -> int:
    """Insert MIT detail payloads as raw JSON."""
    rows = []
    for mit_uuid, payload in pairs:
        payload_json = json.dumps(payload, ensure_ascii=False)
        rows.append((mit_uuid, payload_json, h64(payload_json), run_id, tag))
    ch.insert("mit_details_raw", ["mit_uuid", "payload_json", "payload_hash", "run_id", "source_tag"], rows)
    return len(rows)

