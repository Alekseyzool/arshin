"""Shared helper utilities for the Streamlit app."""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd

BatchVRI = Tuple[Optional[str], Optional[str], Optional[str]]
BatchMIT = Tuple[str, Optional[str], Optional[str]]

EPOCH_DT = dt.datetime(1970, 1, 1, 0, 0, 0)
# ClickHouse DateTime (UInt32) upper bound: 2106-02-07 06:28:15
MAX_CH_DATETIME = dt.datetime(2106, 2, 7, 6, 28, 15)
SUB = str.maketrans({"–": "-", "—": "-", "−": "-", " ": ""})
CONF = {
    "О": "O",
    "о": "O",
    "А": "A",
    "В": "B",
    "Е": "E",
    "К": "K",
    "М": "M",
    "Н": "H",
    "Р": "P",
    "С": "C",
    "Т": "T",
    "У": "Y",
    "Х": "X",
    "A": "А",
    "B": "В",
    "E": "Е",
    "K": "К",
    "M": "М",
    "H": "Н",
    "O": "О",
    "P": "Р",
    "C": "С",
    "T": "Т",
    "Y": "У",
    "X": "Х",
    "0": "O",
    "O": "0",
    "1": "I",
    "I": "1",
    "l": "1",
    "|": "1",
}

VRI_TYPE_LABELS = {
    "1": "Первичная",
    "2": "Периодическая",
    "3": "Внеочередная",
    "4": "Инспекционная",
}


def serial_variants(serial: str, max_variants: int = 16) -> List[str]:
    """Return a list of common serial permutations to brute-force searches."""
    serial = re.sub(r"[\u200B-\u200D\uFEFF\u00A0]", "", (serial or "").upper()).translate(SUB)
    out = {serial}
    for idx, char in enumerate(serial):
        if char in CONF and len(out) < max_variants:
            out.add(serial[:idx] + CONF[char] + serial[idx + 1 :])
    return sorted(out)


def parse_dt_value(raw: Any) -> dt.datetime:
    """Parse ClickHouse DateTime fields into naive datetime instances."""
    if not raw:
        return EPOCH_DT

    if isinstance(raw, dt.datetime):
        parsed = raw.replace(tzinfo=None)
    elif isinstance(raw, dt.date):
        parsed = dt.datetime(raw.year, raw.month, raw.day)
    else:
        clean = str(raw).strip().replace("Z", "").replace("T", " ")
        parsed: Optional[dt.datetime] = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = dt.datetime.strptime(clean, fmt).replace(tzinfo=None)
                break
            except ValueError:
                continue

    if parsed is None:
        return EPOCH_DT

    if parsed < EPOCH_DT:
        return EPOCH_DT
    if parsed > MAX_CH_DATETIME:
        return MAX_CH_DATETIME
    return parsed


def parse_date_ddmmyyyy(raw: Optional[str]) -> dt.date:
    """Convert dates in 'ДД.ММ.ГГГГ' format into datetime.date objects."""
    if not raw:
        return dt.date(1970, 1, 1)
    try:
        day, month, year = raw.strip().split(".")
        return dt.date(int(year), int(month), int(day))
    except Exception:
        return dt.date(1970, 1, 1)


def ts_compact() -> str:
    """Return a compact timestamp for run identifiers."""
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def h64(value: str) -> int:
    """Calculate a short hash used for idempotent inserts."""
    digest = hashlib.sha256(value.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "little")


def chunked(seq: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    """Yield slices of a sequence with the configured size."""
    for start in range(0, len(seq), size):
        yield seq[start : start + size]


def safe_get(data: Dict[str, Any], *path: str, default: Any = None) -> Any:
    """Walk a nested dict and return a value if every key exists."""
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur.get(key)
    return cur


def collect_vri_batches(
    year: int,
    verifier: str,
    mitnumber: str,
    serial: str,
    mititle: str,
    docnum: str,
    since_iso: Optional[str],
    df: Optional[pd.DataFrame],
) -> List[BatchVRI]:
    """Compose search batches from the form inputs and optional CSV/XLSX (deduplicated)."""
    batches: List[BatchVRI] = []
    seen: set[BatchVRI] = set()

    def _normalize(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        text = str(value).strip()
        return text or None

    def _add(batch: BatchVRI) -> None:
        if batch in seen:
            return
        seen.add(batch)
        batches.append(batch)

    if any([year, verifier, mitnumber, serial, mititle, docnum, since_iso]):
        _add((_normalize(mitnumber), _normalize(serial), _normalize(mititle)))
    if df is not None:
        for _, row in df.iterrows():
            _add(
                (
                    _normalize(row.get("mi_mitnumber")),
                    _normalize(row.get("mi_number")),
                    _normalize(row.get("mi_mititle")),
                )
            )
    if not batches:
        _add((None, None, None))
    return batches


def collect_mit_batches(
    manufacturer: str,
    title: str,
    notation: str,
    df: Optional[pd.DataFrame],
) -> List[BatchMIT]:
    """Compose MIT search batches from manual input and uploaded file (deduplicated)."""
    batches: List[BatchMIT] = []
    seen: set[BatchMIT] = set()

    def _normalize(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        text = str(value).strip()
        return text or None

    def _add(batch: BatchMIT) -> None:
        if batch in seen:
            return
        seen.add(batch)
        batches.append(batch)

    manual_batch = (_normalize(manufacturer), _normalize(title), _normalize(notation))
    if manual_batch[0]:
        _add(manual_batch)
    if df is not None:
        for _, row in df.iterrows():
            man = _normalize(row.get("manufacturer"))
            if not man:
                continue
            _add((man, _normalize(row.get("title")), _normalize(row.get("notation"))))
    return batches


def try_parse_since(value: str) -> Optional[str]:
    """Normalize date filter input into ISO `YYYY-MM-DD` format."""
    clean = re.sub(r"[^\d\-\.]", "", value.strip())
    if not clean:
        return None
    if "." in clean:
        try:
            day, month, year = clean.split(".")
            return f"{year}-{month}-{day}"
        except ValueError:
            return None
    return clean
