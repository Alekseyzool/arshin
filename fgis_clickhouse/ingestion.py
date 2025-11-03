"""Data ingestion loops shared between Streamlit callbacks."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

import streamlit as st

from .clickhouse_io import CH
from .fgis_api import FGISClient
from .inserts import insert_mit_details, insert_mit_search, insert_vri_details, insert_vri_search

def paginate(
    fetch,
    *,
    rows: int,
    start: int,
    all_pages: bool,
    max_pages: int,
):
    """Generator that yields `(docs, total, page_idx)` triples for Solr-like pagination."""
    current_start = start
    page = 0
    while True:
        docs, total = fetch(rows=rows, start=current_start)
        yield docs, total, page
        page += 1
        current_start += rows
        if not all_pages or not total:
            break
        if current_start >= total or page >= max_pages:
            break


def ingest_vri(
    ch: CH,
    client: FGISClient,
    batches: List[Tuple[Optional[str], Optional[str], Optional[str]]],
    *,
    year: int,
    verifier: str,
    docnum: str,
    since_iso: Optional[str],
    rows: int,
    start: int,
    all_pages: bool,
    max_pages: int,
    skip_existing_rows: bool,
    skip_existing_details: bool,
    run_id: str,
    tag: str,
) -> Tuple[int, int, int, int]:
    """Drive VRI search/detail ingestion and surface progress in Streamlit."""
    total_new_search = 0
    total_parsed = 0
    total_mieta = 0
    total_mis = 0

    log_box = st.empty()
    progress = st.progress(0.0)
    completed_steps = 0

    def tick(step_weight: int = 1) -> None:
        nonlocal completed_steps
        completed_steps += step_weight
        progress.progress(min(0.99, completed_steps / 800.0))

    for batch_index, (mitnumber, serial, mititle) in enumerate(batches, start=1):

        def fetch_page(rows: int, start: int):
            return client.vri_search(
                year=int(year) if year > 0 else None,
                verifier=verifier or None,
                mitnumber=mitnumber or None,
                serial=serial or None,
                mititle=mititle or None,
                docnum=docnum or None,
                since_iso=since_iso,
                rows=rows,
                start=start,
            )

        for docs, num_found, page in paginate(
            fetch_page,
            rows=rows,
            start=start,
            all_pages=all_pages,
            max_pages=max_pages,
        ):
            if not docs:
                log_box.write(f"[{batch_index}/{len(batches)}] страниц больше нет.")
                break

            docs_to_insert = docs
            if skip_existing_rows:
                ids = [doc.get("vri_id") for doc in docs if doc.get("vri_id")]
                existing = ch.existing_ids("vri_search_raw", "vri_id", ids)
                # Форсируем вставку только новых vri_id, чтобы не плодить дубликаты.
                docs_to_insert = [doc for doc in docs if doc.get("vri_id") and doc["vri_id"] not in existing]
            inserted = insert_vri_search(ch, docs_to_insert, run_id, tag)
            total_new_search += inserted
            log_box.write(f"[{batch_index}/{len(batches)}] page {page}, получено {len(docs)}, новых {inserted} (из {num_found})")
            tick()

            vri_ids_all = [doc.get("vri_id") for doc in docs if doc.get("vri_id")]
            ids_to_fetch = vri_ids_all
            if skip_existing_details and vri_ids_all:
                existing = ch.existing_ids("vri_details_raw", "vri_id", vri_ids_all)
                ids_to_fetch = [vid for vid in vri_ids_all if vid not in existing]

            buffer: List[Tuple[str, Dict[str, object]]] = []
            for idx, vri_id in enumerate(ids_to_fetch, start=1):
                # Подкачиваем детали блоками, чтобы и сеть, и CH загружались равномерно.
                buffer.append((vri_id, client.vri_details(vri_id)))
                if len(buffer) >= ch.batch_size or idx == len(ids_to_fetch):
                    parsed, mieta, mis = insert_vri_details(ch, buffer, run_id, tag)
                    total_parsed += parsed
                    total_mieta += mieta
                    total_mis += mis
                    buffer.clear()
                    log_box.write(f"  детали {idx}/{len(ids_to_fetch)} (+parsed {parsed}, mieta {mieta}, mis {mis})")
                    tick()

            if not all_pages:
                break

    progress.progress(1.0)
    return total_new_search, total_parsed, total_mieta, total_mis


def ingest_mit(
    ch: CH,
    client: FGISClient,
    batches: List[Tuple[str, Optional[str], Optional[str]]],
    *,
    rows: int,
    start: int,
    all_pages: bool,
    max_pages: int,
    skip_existing_search: bool,
    auto_details: bool,
    skip_existing_details: bool,
    run_id: str,
    tag: str,
) -> Tuple[int, int]:
    """Drive MIT search ingestion with optional detail loading."""
    total_new_search = 0
    total_details = 0

    log_box = st.empty()
    progress = st.progress(0.0)
    completed_steps = 0

    def tick(step_weight: int = 1) -> None:
        nonlocal completed_steps
        completed_steps += step_weight
        progress.progress(min(0.99, completed_steps / 500.0))

    for batch_index, (manufacturer, title, notation) in enumerate(batches, start=1):

        def fetch_page(rows: int, start: int):
            return client.mit_search(
                manufacturer=manufacturer,
                title=title,
                notation=notation,
                rows=rows,
                start=start,
            )

        for docs, num_found, page in paginate(
            fetch_page,
            rows=rows,
            start=start,
            all_pages=all_pages,
            max_pages=max_pages,
        ):
            if not docs:
                log_box.write(f"[{batch_index}/{len(batches)}] страниц больше нет.")
                break

            docs_to_insert = docs
            if skip_existing_search:
                ids = [doc.get("mit_uuid") for doc in docs if doc.get("mit_uuid")]
                existing = ch.existing_ids("mit_search_raw", "mit_uuid", ids)
                # Аналогично VRI сохраняем только новые типы.
                docs_to_insert = [doc for doc in docs if doc.get("mit_uuid") and doc["mit_uuid"] not in existing]
            inserted = insert_mit_search(ch, docs_to_insert, run_id, tag)
            total_new_search += inserted
            log_box.write(f"[{batch_index}/{len(batches)}] page {page}, получено {len(docs)}, новых {inserted} (из {num_found})")
            tick()

            if auto_details:
                mit_ids_all = [doc.get("mit_uuid") for doc in docs if doc.get("mit_uuid")]
                ids_to_fetch = mit_ids_all
                if skip_existing_details and mit_ids_all:
                    existing = ch.existing_ids("mit_details_raw", "mit_uuid", mit_ids_all)
                    ids_to_fetch = [mit_uuid for mit_uuid in mit_ids_all if mit_uuid not in existing]

                buffer: List[Tuple[str, Dict[str, object]]] = []
                for idx, mit_uuid in enumerate(ids_to_fetch, start=1):
                    # Загрузка подробностей chunk'ами позволяет не упираться в лимиты по RPS.
                    buffer.append((mit_uuid, client.mit_details(mit_uuid)))
                    if len(buffer) >= ch.batch_size or idx == len(ids_to_fetch):
                        inserted_details = insert_mit_details(ch, buffer, run_id, tag)
                        total_details += inserted_details
                        buffer.clear()
                        log_box.write(f"  детали {idx}/{len(ids_to_fetch)} (вставлено chunk)")
                        tick()

            if not all_pages:
                break

    progress.progress(1.0)
    return total_new_search, total_details
