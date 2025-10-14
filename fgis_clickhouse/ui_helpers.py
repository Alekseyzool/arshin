"""Streamlit sidebar helpers."""

from __future__ import annotations

import os
from typing import Any, Optional

import pandas as pd
import streamlit as st

from .clickhouse_io import CH, ensure_tables


def ch_connect_from_sidebar() -> Optional[CH]:
    """Render connection form and return an active ClickHouse client."""
    host = st.sidebar.text_input("CH_HOST", os.getenv("CH_HOST", "127.0.0.1"), key="ch_host")
    port = st.sidebar.number_input("CH_PORT", 1, 65535, int(os.getenv("CH_PORT", "9001")), key="ch_port")
    user = st.sidebar.text_input(
        "CH_USER",
        os.getenv("CH_USER_INGEST") or os.getenv("CH_USER_READ") or "default",
        key="ch_user",
    )
    password = st.sidebar.text_input(
        "CH_PASS",
        os.getenv("CH_PASS_INGEST") or os.getenv("CH_PASS_READ") or "",
        type="password",
        key="ch_pass",
    )
    database = st.sidebar.text_input("CH_DB", os.getenv("CH_DB", "fgis"), key="ch_db")
    skip_ddl = st.sidebar.checkbox("Не выполнять DDL (read-only)", value=False, key="skipddl")

    if "_ch" in st.session_state and not st.sidebar.button("Переподключить ClickHouse", key="btn_ch_reconnect"):
        st.sidebar.info("Соединение активно.")
        return st.session_state["_ch"]

    connect_btn = st.sidebar.button("Подключиться к ClickHouse", key="btn_ch")
    if not connect_btn:
        return st.session_state.get("_ch")

    try:
        ch = CH(host, int(port), user, password, database)
        version = ch.scalar("SELECT version()")
        st.sidebar.info(f"Версия ClickHouse: {version}")
        if not skip_ddl:
            ensure_tables(ch)
        st.session_state["_ch"] = ch
        st.session_state["_skipddl"] = skip_ddl
        st.sidebar.success("OK, подключено.")
        return ch
    except Exception as exc:  # pragma: no cover - displayed in UI
        st.sidebar.error(f"Ошибка подключения/DDL: {exc}")
        return None


def read_optional_dataframe(uploaded_file: Optional[Any]) -> Optional[pd.DataFrame]:
    """Read uploaded CSV/XLSX and surface warning in Streamlit on failure."""
    if uploaded_file is None:
        return None
    try:
        if uploaded_file.name.lower().endswith(".csv"):
            return pd.read_csv(uploaded_file)
        return pd.read_excel(uploaded_file)
    except Exception as exc:
        st.warning(f"Не удалось прочитать файл {uploaded_file.name}: {exc}")
        return None

