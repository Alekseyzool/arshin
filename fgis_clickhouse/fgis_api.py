"""Bindings for FGIS search and details endpoints."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from requests import utils as request_utils

from .http_client import HttpClient
from .utils import serial_variants

VRI_FL = (
    "vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,"
    "verification_date,valid_date,applicability,result_docnum,sticker_num"
)
VRI_SEARCH_BASE = "https://fgis.gost.ru/fundmetrology/cm/xcdb/vri/select"
VRI_DETAILS_BASE = "https://fgis.gost.ru/fundmetrology/cm/iaux/vri/{vri_id}"
MIT_SEARCH_BASE = "https://fgis.gost.ru/fundmetrology/cm/xcdb/mit24/list"
MIT_DETAILS_BASE = "https://fgis.gost.ru/fundmetrology/cm/xcdb/mit24/get"


def fq_like(field: str, value: str) -> str:
    """Build a Solr-like `fq` filter for partial matching."""
    return "fq=" + request_utils.quote(f"{field}:*{value}*", safe=":*()/ -")


class FGISClient:
    """Typed helper around the JSON endpoints the UI requires."""

    def __init__(self, proxy: Optional[str], rps: float):
        self._http = HttpClient(proxy=proxy, rps=rps)

    def vri_search(
        self,
        year: Optional[int],
        verifier: Optional[str],
        mitnumber: Optional[str],
        serial: Optional[str],
        mititle: Optional[str],
        docnum: Optional[str],
        since_iso: Optional[str],
        rows: int,
        start: int,
    ) -> Tuple[List[Dict[str, str]], int]:
        """Query the VRI search endpoint with all supported filters."""
        params: List[str] = []
        if year:
            params.append("fq=" + request_utils.quote(f"verification_year:{year}", safe=":*"))
        if verifier:
            params.append(fq_like("org_title", verifier))
        if mitnumber:
            params.append(fq_like("mi.mitnumber", mitnumber))
        if mititle:
            params.append(fq_like("mi.mititle", mititle))
        if serial:
            ors = " OR ".join(f"mi.number:*{variant}*" for variant in serial_variants(serial))
            params.append("fq=" + request_utils.quote(f"({ors})", safe=":*()/ "))
        if docnum:
            params.append(fq_like("result_docnum", docnum))
        if since_iso:
            params.append("fq=" + request_utils.quote(f"verification_date:[{since_iso}T00:00:00Z TO *]", safe=":*[]/ "))

        params.extend(
            [
                "q=*",
                f"fl={VRI_FL}",
                "sort=verification_date+desc,org_title+asc",
                f"rows={rows}",
                f"start={start}",
            ]
        )
        url = VRI_SEARCH_BASE + "?" + "&".join(params)
        payload = self._http.json(url)
        response = payload.get("response") or {}
        docs = response.get("docs") or []
        return docs, int(response.get("numFound", 0))

    def vri_details(self, vri_id: str) -> Dict[str, str]:
        """Download the expanded VRI record."""
        return self._http.json(VRI_DETAILS_BASE.format(vri_id=vri_id))

    def mit_search(
        self,
        manufacturer: str,
        title: Optional[str],
        notation: Optional[str],
        rows: int,
        start: int,
    ) -> Tuple[List[Dict[str, str]], int]:
        """Query MIT catalogue with manufacturer and optional filters."""
        params: List[str] = []
        manufacturer_tokens = [token for token in re.split(r"[,\s]+", manufacturer.strip()) if token]
        # Веб-интерфейс FGIS разбивает производитель на токены и применяет их как отдельные фильтры.
        # Используем тот же подход — фильтры комбинируются по AND, что повторяет поведение UI.
        if manufacturer_tokens:
            params.extend(fq_like("manufacturers", token) for token in manufacturer_tokens)
        if title:
            tokens = [token for token in re.split(r"[,\s]+", title.strip()) if token]
            params.extend(fq_like("title", token) for token in tokens)
        if notation:
            params.append(fq_like("notation", notation))
        params.extend(["sort=num1+desc,num2+desc", f"start={start}", f"rows={rows}"])
        url = MIT_SEARCH_BASE + "?" + "&".join(params)
        payload = self._http.json(url)
        response = payload.get("response") or {}
        docs = response.get("docs") or []
        return docs, int(response.get("numFound", 0))

    def mit_details(self, mit_uuid: str) -> Dict[str, str]:
        """Pull detailed MIT payload for the provided UUID."""
        url = MIT_DETAILS_BASE + "?q=" + request_utils.quote(f"mit_uuid:{mit_uuid}")
        payload = self._http.json(url)
        docs = (payload.get("response") or {}).get("docs") or []
        return docs[0] if docs else {}
