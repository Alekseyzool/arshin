"""Logic for parsing VRI detail payloads into normalized tuples."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .utils import VRI_TYPE_LABELS, h64, parse_date_ddmmyyyy, safe_get


def _str(value: Any) -> str:
    """Normalize nullable string fields to plain strings."""
    if value is None:
        return ""
    return str(value)


def parse_vri_payload(vri_id: str, payload: Dict[str, Any]) -> Tuple[Tuple[Any, ...], List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
    """Split the VRI JSON payload into detail, mieta, and mis rows."""
    result = payload.get("result") or {}
    single = safe_get(result, "miInfo", "singleMI", default={}) or {}
    vri_info = safe_get(result, "vriInfo", default={}) or {}
    applicable = vri_info.get("applicable") or {}
    info = result.get("info") or {}

    mitype_number = single.get("mitypeNumber") or single.get("mitypeNum") or ""
    details_row = (
        vri_id,
        mitype_number,
        _str(single.get("mitypeType")),
        _str(single.get("mitypeTitle")),
        _str(single.get("mitypeURL")),
        _str(single.get("manufactureNum")),
        int(single.get("manufactureYear") or 0),
        _str(single.get("modification")),
        _str(vri_info.get("organization")),
        _str(vri_info.get("signCipher")),
        _str(vri_info.get("miOwner")),
        str(vri_info.get("vriType", "") or ""),
        VRI_TYPE_LABELS.get(str(vri_info.get("vriType", "") or ""), ""),
        parse_date_ddmmyyyy(vri_info.get("vrfDate")),
        parse_date_ddmmyyyy(vri_info.get("validDate")),
        _str(vri_info.get("docTitle")),
        _str(applicable.get("certNum")),
        1 if applicable.get("signPass") else 0,
        1 if applicable.get("signMi") else 0,
        1 if info.get("briefIndicator") else 0,
    )

    mieta_rows: List[Tuple[Any, ...]] = []
    for item in safe_get(result, "means", "mieta", default=[]) or []:
        reg_number = _str(item.get("regNumber"))
        mitype_num = _str(item.get("mitypeNumber"))
        notation = _str(item.get("notation"))
        modification = _str(item.get("modification"))
        manufacture_num = _str(item.get("manufactureNum"))
        manufacture_year = int(item.get("manufactureYear") or 0)
        rank_code = _str(item.get("rankCode"))
        row_hash = h64(
            f"{vri_id}|{reg_number}|{mitype_num}|{notation}|{modification}|{manufacture_num}|{manufacture_year}|{rank_code}"
        )
        mieta_rows.append(
            (
                vri_id,
                reg_number,
                mitype_num,
                _str(item.get("mitypeTitle")),
                _str(item.get("mitypeURL")),
                notation,
                modification,
                manufacture_num,
                manufacture_year,
                rank_code,
                _str(item.get("rankTitle")),
                _str(item.get("schemaTitle")),
                row_hash,
            )
        )

    mis_rows: List[Tuple[Any, ...]] = []
    for item in safe_get(result, "means", "mis", default=[]) or []:
        mitype_num = _str(item.get("mitypeNumber"))
        number = _str(item.get("number"))
        row_hash = h64(f"{vri_id}|{mitype_num}|{number}")
        mis_rows.append(
            (
                vri_id,
                mitype_num,
                _str(item.get("mitypeTitle")),
                _str(item.get("mitypeURL")),
                number,
                row_hash,
            )
        )

    return details_row, mieta_rows, mis_rows
