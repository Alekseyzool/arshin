"""Streamlit analytics for FGIS data stored in ClickHouse."""

from __future__ import annotations

import re
import warnings
from datetime import date
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from fgis_clickhouse.clickhouse_io import CH
from fgis_clickhouse.reporting import ReportParams, build_word_report
from fgis_clickhouse.queries import (
    count_mit_for_manufacturer,
    count_vri_for_manufacturer,
    query_mit_for_manufacturer,
    query_vri_for_manufacturer,
    report_mit_by_manufacturer,
    report_vri_by_manufacturer,
    top_manufacturers_by_mit,
    top_manufacturers_by_vri,
)
from fgis_clickhouse.ui_helpers import ch_connect_auto

load_dotenv()
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")


def run_analytics_tab(ch: Optional[CH]) -> None:
    """Render analytics for manufacturers based on loaded data."""
    st.subheader("Аналитика по производителям")
    if ch is None:
        st.info("Данные временно недоступны. Попробуйте позже.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        manufacturer_term = st.text_input(
            "Производитель (подстрока, можно через +)",
            key="ana_manufacturer",
            placeholder="Например: VXI+Информ",
        )
        st.caption("Можно указать несколько фирм через +, например: VXI+Информ")
        st.caption("Похожие названия объединяются (например, ВНИИФТРИ, Keysight).")
        only_applicable = st.checkbox("Только пригодные поверки (applicability=1)", True, key="ana_applicable")
    with col2:
        use_year_filter = st.checkbox("Фильтр по годам", False, key="ana_year_filter")
        if use_year_filter:
            default_from = max(1900, date.today().year - 5)
            year_from = st.number_input("Год с", 1900, 2100, default_from, key="ana_year_from")
            year_to = st.number_input("Год по", 1900, 2100, date.today().year, key="ana_year_to")
        else:
            year_from = None
            year_to = None
        mit_type_label = st.selectbox(
            "Тип утверждения",
            ("Любой", "Серийное", "Единичное"),
            index=0,
            key="ana_mit_type",
        )
        mit_type = {"Любой": None, "Серийное": "serial", "Единичное": "single"}[mit_type_label]
    with col3:
        export_limit = st.number_input("Лимит выгрузки (строк)", 100, 1_000_000, 5000, key="ana_limit")
        top_n = st.number_input("ТОП N", 5, 100, 20, key="ana_top")

    col_a, col_b = st.columns(2)
    with col_a:
        run_company = st.button("▶ Рассчитать по фирме", key="ana_run")
    with col_b:
        run_top = st.button("▶ Обновить ТОП", key="ana_top_run")

    if run_company:
        if not manufacturer_term.strip():
            st.warning("Введите часть названия производителя.")
        else:
            mit_count = count_mit_for_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                mit_type,
            )
            vri_count = count_vri_for_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                only_applicable,
            )
            m1, m2 = st.columns(2)
            m1.metric("Утверждений типа", mit_count)
            m2.metric("Поверок приборов", vri_count)

            st.markdown("### Утверждения типа (отчет по фирмам)")
            report_mit_cols, report_mit_rows = report_mit_by_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                mit_type,
                int(top_n),
            )
            df_report_mit = pd.DataFrame(report_mit_rows, columns=report_mit_cols)
            if df_report_mit.empty:
                st.info("Нет данных по утверждениям типа.")
            else:
                st.dataframe(df_report_mit, use_container_width=True)
                report_mit_csv = df_report_mit.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Скачать отчет по типам (CSV)",
                    data=report_mit_csv,
                    file_name="mit_report.csv",
                    mime="text/csv",
                    key="download_mit_report",
                )

            st.markdown("### Утверждения типа (выгрузка)")
            mit_cols, mit_rows = query_mit_for_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                mit_type,
                int(export_limit),
            )
            df_mit = pd.DataFrame(mit_rows, columns=mit_cols)
            if df_mit.empty:
                st.info("Нет данных по утверждениям типа.")
            else:
                st.dataframe(df_mit, use_container_width=True)
                mit_csv = df_mit.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Скачать утверждения типа (CSV)",
                    data=mit_csv,
                    file_name="mit_selected.csv",
                    mime="text/csv",
                    key="download_mit_selected",
                )

            st.markdown("### Поверки приборов (отчет по фирмам)")
            report_vri_cols, report_vri_rows = report_vri_by_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                only_applicable,
                int(top_n),
            )
            df_report_vri = pd.DataFrame(report_vri_rows, columns=report_vri_cols)
            if df_report_vri.empty:
                st.info("Нет данных по поверкам.")
            else:
                st.dataframe(df_report_vri, use_container_width=True)
                report_vri_csv = df_report_vri.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Скачать отчет по поверкам (CSV)",
                    data=report_vri_csv,
                    file_name="vri_report.csv",
                    mime="text/csv",
                    key="download_vri_report",
                )

            st.markdown("### Поверки приборов (выгрузка)")
            vri_cols, vri_rows = query_vri_for_manufacturer(
                ch,
                manufacturer_term,
                int(year_from) if year_from else None,
                int(year_to) if year_to else None,
                only_applicable,
                int(export_limit),
            )
            df_vri = pd.DataFrame(vri_rows, columns=vri_cols)
            if df_vri.empty:
                st.info("Нет данных по поверкам.")
            else:
                st.dataframe(df_vri, use_container_width=True)
                vri_csv = df_vri.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Скачать поверки (CSV)",
                    data=vri_csv,
                    file_name="vri_selected.csv",
                    mime="text/csv",
                    key="download_vri_selected",
                )

    if run_top:
        st.markdown("### ТОП производителей по утверждениям типа")
        top_mit_cols, top_mit_rows = top_manufacturers_by_mit(
            ch,
            int(top_n),
            int(year_from) if year_from else None,
            int(year_to) if year_to else None,
            mit_type,
        )
        df_top_mit = pd.DataFrame(top_mit_rows, columns=top_mit_cols)
        if df_top_mit.empty:
            st.info("Нет данных по утверждениям типа.")
        else:
            st.dataframe(df_top_mit, use_container_width=True)
            st.bar_chart(df_top_mit.set_index("manufacturer")["approvals"])

        st.markdown("### ТОП производителей по поверкам")
        top_vri_cols, top_vri_rows = top_manufacturers_by_vri(
            ch,
            int(top_n),
            int(year_from) if year_from else None,
            int(year_to) if year_to else None,
            only_applicable,
        )
        df_top_vri = pd.DataFrame(top_vri_rows, columns=top_vri_cols)
        if df_top_vri.empty:
            st.info("Нет данных по поверкам.")
        else:
            st.dataframe(df_top_vri, use_container_width=True)
            st.bar_chart(df_top_vri.set_index("manufacturer")["verifications"])

    st.markdown("### Word-отчет")
    with st.expander("Сформировать Word-отчет", expanded=False):
        default_from = int(year_from) if year_from else max(1900, date.today().year - 5)
        default_to = int(year_to) if year_to else date.today().year

        col_r1, col_r2 = st.columns(2)
        with col_r1:
            report_year_from = st.number_input("Год с", 1900, 2100, default_from, key="rep_year_from")
            report_year_to = st.number_input("Год по", 1900, 2100, default_to, key="rep_year_to")
            report_top_n = st.number_input("ТОП N (типы)", 5, 100, int(top_n), key="rep_top_n")
            report_competitors = st.number_input("ТОП конкурентов", 5, 50, 10, key="rep_competitors")
        with col_r2:
            report_group = st.text_input("Название своей группы", "VXI + Информтест", key="rep_group")
            report_regex = st.text_input("Regex для поиска своих", "VXI|Inftest|Информтест", key="rep_regex")
            report_only_applicable = st.checkbox("Только пригодные поверки", True, key="rep_only_applicable")
            report_serial_or_us = st.checkbox("Только серийные + наша группа", True, key="rep_serial_or_us")

        mit_numbers_raw = st.text_area(
            "MIT номера для анализа поверок (опционально, через запятую/перевод строки)",
            "",
            key="rep_mit_numbers",
        )
        mit_numbers = [item.strip() for item in re.split(r"[,\n;]+", mit_numbers_raw or "") if item.strip()]
        mit_numbers = mit_numbers or None

        if st.button("▶ Сформировать Word-отчет", key="rep_run"):
            with st.spinner("Готовим Word-отчет..."):
                try:
                    params = ReportParams(
                        year_from=int(report_year_from),
                        year_to=int(report_year_to),
                        our_group_name=report_group.strip() or "VXI + Информтест",
                        our_org_regex=report_regex.strip() or "VXI|Inftest|Информтест",
                        top_n=int(report_top_n),
                        competitors_n=int(report_competitors),
                        only_applicable=bool(report_only_applicable),
                        serial_or_us=bool(report_serial_or_us),
                        mit_numbers=mit_numbers,
                    )
                    report_bytes = build_word_report(ch, params)
                    st.session_state["rep_docx"] = report_bytes
                    st.session_state["rep_docx_name"] = (
                        f"Отчет_типы_поверки_{int(report_year_from)}_{int(report_year_to)}.docx"
                    )
                    st.success("Отчет готов.")
                except Exception as exc:  # pragma: no cover - UI feedback
                    st.error(f"Не удалось сформировать отчет: {exc}")

        if st.session_state.get("rep_docx"):
            st.download_button(
                "⬇️ Скачать Word-отчет",
                data=st.session_state["rep_docx"],
                file_name=st.session_state.get("rep_docx_name", "report.docx"),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key="rep_download",
            )


def main() -> None:
    """Streamlit entrypoint."""
    st.set_page_config(page_title="FGIS → ClickHouse", layout="wide")

    ch = ch_connect_auto()
    run_analytics_tab(ch)


if __name__ == "__main__":
    main()
