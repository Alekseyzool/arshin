"""Minimal shared helpers for FGIS ingestion."""

from __future__ import annotations

import re
from typing import Any, Iterator, List, Sequence

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


def serial_variants(serial: str, max_variants: int = 16) -> List[str]:
    """Return a list of common serial permutations to brute-force searches."""
    serial = re.sub(r"[\u200B-\u200D\uFEFF\u00A0]", "", (serial or "").upper()).translate(SUB)
    out = {serial}
    for idx, char in enumerate(serial):
        if char in CONF and len(out) < max_variants:
            out.add(serial[:idx] + CONF[char] + serial[idx + 1 :])
    return sorted(out)


def chunked(seq: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    """Yield slices of a sequence with the configured size."""
    for start in range(0, len(seq), size):
        yield seq[start : start + size]
