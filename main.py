"""Streamlit entrypoint for the FGIS → ClickHouse loader."""

from __future__ import annotations

import warnings
import streamlit as st

from fgis_clickhouse.fgis_api import FGISClient
from fgis_clickhouse.http_client import MIN_RPS
from fgis_clickhouse.ingestion import ingest_mit, ingest_vri
from fgis_clickhouse.queries import distinct_mit_numbers
from fgis_clickhouse.ui_helpers import ch_connect_from_sidebar, read_optional_dataframe
from fgis_clickhouse.utils import (
    collect_mit_batches,
    collect_vri_batches,
    ts_compact,
    try_parse_since,
)

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")


def run_vri_tab(ch, client: FGISClient, tag: str) -> None:
    """Render the VRI ingestion tab."""
    st.subheader("Поиск поверок → ClickHouse (+ детали распарсены)")
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.number_input("Год поверки", 0, 2100, 0, key="vri_year")
        verifier = st.text_input("Поверитель (org_title)", key="vri_org")
        mitnumber = st.text_input("Рег. номер типа СИ (mi.mitnumber)", key="vri_mitnum")
    with col2:
        serial = st.text_input("Заводской номер (mi.number)", key="vri_serial")
        mititle = st.text_input("Наименование типа СИ (mi.mititle)", key="vri_title")
        docnum = st.text_input("Номер свидетельства (result_docnum)", key="vri_doc")
    with col3:
        since_txt = st.text_input("С даты (verification_date ≥) — ДД.ММ.ГГГГ", key="vri_since")
        rows_v = st.number_input("rows", 1, 10000, 500, key="vri_rows")
        start_v = st.number_input("start", 0, 10_000_000, 0, key="vri_start")

    all_pages = st.checkbox("Забрать все страницы (пагинация)", True, key="vri_allpages")
    max_pages = st.number_input("Макс. страниц (страховка)", 1, 10000, 1000, key="vri_max_pages")
    skip_existing_rows = st.checkbox("Не вставлять уже существующие поверки", True, key="vri_skip_existing")
    skip_existing_details = st.checkbox("Не скачивать детали, если уже есть", True, key="vri_skip_det")
    st.caption("Пакетный поиск CSV/XLSX: колонки `mi_mitnumber`, `mi_number`, `mi_mititle` (любые из них)")
    df_vri = read_optional_dataframe(st.file_uploader("Загрузить CSV/XLSX для VRI", type=["csv", "xlsx"], key="file_vri"))

    run_vri = st.button("▶ Запустить поиск и загрузку", key="btn_vri", disabled=("_ch" not in st.session_state))
    if run_vri and ch is not None:
        run_id = f"vri-{ts_compact()}"
        since_iso = try_parse_since(since_txt)
        batches = collect_vri_batches(
            year=int(year),
            verifier=verifier,
            mitnumber=mitnumber,
            serial=serial,
            mititle=mititle,
            docnum=docnum,
            since_iso=since_iso,
            df=df_vri,
        )
        new_rows, parsed, mieta, mis = ingest_vri(
            ch,
            client,
            batches,
            year=int(year),
            verifier=verifier,
            docnum=docnum,
            since_iso=since_iso,
            rows=int(rows_v),
            start=int(start_v),
            all_pages=all_pages,
            max_pages=int(max_pages),
            skip_existing_rows=skip_existing_rows,
            skip_existing_details=skip_existing_details,
            run_id=run_id,
            tag=tag,
        )
        st.success(f"Готово. Новых VRI={new_rows}, распарсено={parsed}, mieta={mieta}, mis={mis}, run_id={run_id}")


def run_mit_tab(ch, client: FGISClient, tag: str) -> None:
    """Render the MIT ingestion tab."""
    st.subheader("Поиск утверждений типа → ClickHouse (без изменений)")
    col1, col2, col3 = st.columns(3)
    with col1:
        manufacturer = st.text_input("Изготовитель (обязательно для формы)", key="mit_man")
    with col2:
        title = st.text_input("Наименование (tokens)", key="mit_title")
    with col3:
        notation = st.text_input("Обозначение (notation)", key="mit_not")

    rows_m = st.number_input("rows", 1, 10000, 500, key="mit_rows")
    start_m = st.number_input("start", 0, 10_000_000, 0, key="mit_start")
    all_pages_m = st.checkbox("Забрать все страницы (пагинация)", True, key="mit_allpages")
    max_pages_m = st.number_input("Макс. страниц (страховка)", 1, 10000, 1000, key="mit_max_pages")
    skip_existing_m = st.checkbox("Не вставлять уже существующие типы", True, key="mit_skip_existing")
    autodet_m = st.checkbox("Сразу загрузить детали (mit_details)", True, key="mit_autodet")
    skip_existing_mdet = st.checkbox("Не скачивать детали, если уже есть", True, key="mit_skip_det")

    st.caption("Пакетный поиск CSV/XLSX: колонки `manufacturer`, `title`, `notation`")
    df_mit = read_optional_dataframe(st.file_uploader("Загрузить CSV/XLSX для MIT", type=["csv", "xlsx"], key="file_mit"))

    run_mit = st.button("▶ Запустить поиск по типам", key="btn_mit", disabled=("_ch" not in st.session_state))
    if run_mit and ch is not None:
        batches = collect_mit_batches(manufacturer, title, notation, df_mit)
        if not batches:
            st.error("Нужен изготовитель в форме или файл.")
        else:
            run_id = f"mit-{ts_compact()}"
            new_rows, details = ingest_mit(
                ch,
                client,
                batches,
                rows=int(rows_m),
                start=int(start_m),
                all_pages=all_pages_m,
                max_pages=int(max_pages_m),
                skip_existing_search=skip_existing_m,
                auto_details=autodet_m,
                skip_existing_details=skip_existing_mdet,
                run_id=run_id,
                tag=tag,
            )
            st.success(f"Готово. Новых MIT={new_rows}, деталей={details}, run_id={run_id}")

    st.markdown("### Поверки по скачанным типам")
    st.caption("Берём все номера утверждений типа из ClickHouse и ищем по ним поверки (mi.mitnumber).")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        limit_numbers = st.number_input("Макс. типов (0 = все)", 0, 100_000, 0, key="mit_vri_limit")
        rows_bridge = st.number_input("rows", 1, 10_000, 500, key="mit_vri_rows")
        start_bridge = st.number_input("start", 0, 10_000_000, 0, key="mit_vri_start")
    with col_b:
        all_pages_bridge = st.checkbox("Забрать все страницы", True, key="mit_vri_allpages")
        max_pages_bridge = st.number_input("Макс. страниц", 1, 10_000, 1_000, key="mit_vri_max_pages")
        skip_existing_rows_bridge = st.checkbox("Пропустить уже существующие поверки", True, key="mit_vri_skip_rows")
    with col_c:
        skip_existing_details_bridge = st.checkbox("Пропустить детали, если уже есть", True, key="mit_vri_skip_details")
        st.markdown("&nbsp;", unsafe_allow_html=True)
        load_vri_btn = st.button("▶ Получить поверки для всех номеров", key="btn_mit_vri", disabled=("_ch" not in st.session_state))

    if load_vri_btn and ch is not None:
        numbers = distinct_mit_numbers(ch, limit_numbers if limit_numbers > 0 else None)
        if not numbers:
            st.warning("В ClickHouse нет номеров в таблице mit_search_raw.")
        else:
            st.info(f"Обрабатываем {len(numbers)} типов СИ.")
            batches = [(number, None, None) for number in numbers]
            run_id = f"vri-from-mit-{ts_compact()}"
            new_rows, parsed, mieta, mis = ingest_vri(
                ch,
                client,
                batches,
                year=0,
                verifier="",
                docnum="",
                since_iso=None,
                rows=int(rows_bridge),
                start=int(start_bridge),
                all_pages=all_pages_bridge,
                max_pages=int(max_pages_bridge),
                skip_existing_rows=skip_existing_rows_bridge,
                skip_existing_details=skip_existing_details_bridge,
                run_id=run_id,
                tag=tag,
            )
            st.success(
                f"Готово. Новых VRI={new_rows}, распарсено={parsed}, mieta={mieta}, mis={mis}, run_id={run_id}"
            )


def main() -> None:
    """Streamlit entrypoint."""
    st.set_page_config(page_title="FGIS → ClickHouse", layout="wide")

    st.sidebar.header("ClickHouse")
    ch = ch_connect_from_sidebar()
    proxy_default = st.session_state.get("proxy", "")
    proxy = st.sidebar.text_input("HTTP(S) Proxy (опц.)", proxy_default, key="proxy")
    rps = st.sidebar.slider("RPS (req/sec)", MIN_RPS, 5.0, 1.5, 0.1, key="rps")
    tag = st.sidebar.text_input("Source tag", "manual", key="tag")
    client = FGISClient(proxy or None, float(rps))

    tab_vri, tab_mit = st.tabs(["🔎 Поверки (VRI)", "📚 Утверждения типов (MIT)"])
    with tab_vri:
        run_vri_tab(ch, client, tag)
    with tab_mit:
        run_mit_tab(ch, client, tag)


if __name__ == "__main__":
    main()
