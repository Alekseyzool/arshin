"""High-level backend sync pipeline.

This module is the operational layer: it decides what is due in this run and
calls MIT/VRI helpers. Low-level FGIS pagination and row mapping stay in the
MIT/VRI modules.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

from .clickhouse_io import CH, ensure_tables
from .dates import iter_days, parse_ymd
from .fgis_api import FGISClient
from .mit_sync import sync_mit_registry
from .runtime import (
    DEFAULT_START_DATE,
    DEFAULT_VRI_ROWS,
    DEFAULT_VRI_SLEEP,
    FULL_RECONCILE_EVERY,
    PROD_SYNC_DAYS,
    PROD_SYNC_HOUR,
    SYNC_RECENT_DAYS,
    TEST_SYNC_EVERY,
    YEAR_RECONCILE_DAYS,
    YEAR_RECONCILE_EVERY,
    acquire_process_lock,
    env_pick,
    should_run,
    should_run_any,
    state_get,
    state_get_any,
    state_set,
)
from .vri_sync import (
    reconcile_day_with_remote,
    reconcile_prod_by_year_month,
    reconcile_remote_by_year_month,
)


log = logging.getLogger("fgis_backend")

def _window_start(start_date: date, end_date: date, days: int) -> date:
    """Clamp a rolling window to the configured historical lower bound."""
    return max(start_date, end_date - timedelta(days=max(days - 1, 0)))

def _should_run_daily_after(ch: CH, key: str, now: datetime, hour: int) -> bool:
    """Gate a once-per-day job until the configured local hour is reached."""
    if now.hour < hour:
        return False
    last = state_get(ch, key)
    return last is None or last.date() < now.date()

def _remote_checked_after_last_prod(ch_test: CH, ch_prod: CH) -> bool:
    """Return True when prod has not yet consumed the latest full test check."""
    last_full = state_get_any(ch_test, ("test_full_reconcile", "simple_remote_full"))
    last_prod = state_get_any(ch_prod, ("prod_full_sync", "simple_prod_full"))
    return last_full is not None and (last_prod is None or last_full > last_prod)

def sync_test_mit(ch: CH, client: FGISClient) -> bool:
    """Refresh MIT in test by inserting missing registry numbers and merging duplicates."""
    log.info("TEST MIT: start")
    total = sync_mit_registry(
        ch,
        client,
        rows=1000,
        sleep_s=0.0,
        fetch_details=True,
        stop_on_existing=False,
        empty_pages_limit=0,
        min_rows=10,
    )
    ch.exec(f"OPTIMIZE TABLE {ch.db}.mit_registry FINAL")
    state_set(ch, "test_mit_sync", datetime.now())
    log.info("TEST MIT: done (inserted=%s)", total)
    return True

def sync_test_recent_vri(ch: CH, client: FGISClient, start_date: date, end_date: date) -> bool:
    """Keep the hot VRI tail correct; recent FGIS data changes most often."""
    start = _window_start(start_date, end_date, SYNC_RECENT_DAYS)
    log.info("TEST VRI recent: %s -> %s", start, end_date)
    ok = True
    for day in iter_days(start, end_date):
        ok = reconcile_day_with_remote(ch, client, day, DEFAULT_VRI_ROWS, DEFAULT_VRI_SLEEP) and ok
    if ok:
        state_set(ch, "test_recent_vri", datetime.now())
    return ok

def reconcile_test_year(ch: CH, client: FGISClient, start_date: date, end_date: date) -> bool:
    """Weekly safety check for the last year of VRI data."""
    start = _window_start(start_date, end_date, YEAR_RECONCILE_DAYS)
    log.info("TEST VRI year check: %s -> %s", start, end_date)
    ok = reconcile_remote_by_year_month(ch, client, start, end_date, DEFAULT_VRI_ROWS, DEFAULT_VRI_SLEEP)
    if ok:
        state_set(ch, "test_year_reconcile", datetime.now())
    return ok

def reconcile_test_full(ch: CH, client: FGISClient, start_date: date, end_date: date) -> bool:
    """Monthly full-history VRI safety check against FGIS."""
    log.info("TEST VRI full check: %s -> %s", start_date, end_date)
    ok = reconcile_remote_by_year_month(ch, client, start_date, end_date, DEFAULT_VRI_ROWS, DEFAULT_VRI_SLEEP)
    if ok:
        now = datetime.now()
        state_set(ch, "test_full_reconcile", now)
        state_set(ch, "test_year_reconcile", now)
    return ok

def replace_prod_mit_from_test(ch_test: CH, ch_prod: CH) -> bool:
    """Publish MIT by replacing prod from deduplicated test data."""
    source_rows = int(ch_test.scalar(f"SELECT count() FROM {ch_test.db}.mit_registry FINAL") or 0)
    if source_rows == 0:
        log.warning("PROD MIT: test table is empty -> skip")
        return False
    log.info("PROD MIT: replace from test (rows=%s)", source_rows)
    ch_prod.exec(f"TRUNCATE TABLE {ch_prod.db}.mit_registry")
    ch_prod.exec(
        f"INSERT INTO {ch_prod.db}.mit_registry "
        f"SELECT * FROM {ch_test.db}.mit_registry FINAL"
    )
    prod_rows = int(ch_prod.scalar(f"SELECT count() FROM {ch_prod.db}.mit_registry") or 0)
    ok = prod_rows == source_rows
    if ok:
        state_set(ch_prod, "prod_mit_sync", datetime.now())
        log.info("PROD MIT: OK (rows=%s)", prod_rows)
    else:
        log.warning("PROD MIT: mismatch after replace (test=%s prod=%s)", source_rows, prod_rows)
    return ok

def sync_prod_from_test(ch_test: CH, ch_prod: CH, start_date: date, end_date: date) -> bool:
    """Publish test to prod and choose full/year scope based on test checks."""
    full = _remote_checked_after_last_prod(ch_test, ch_prod)
    vri_start = start_date if full else _window_start(start_date, end_date, PROD_SYNC_DAYS)
    scope = "full" if full else "year"
    log.info("PROD sync: start (%s, %s -> %s)", scope, vri_start, end_date)
    mit_ok = replace_prod_mit_from_test(ch_test, ch_prod)
    vri_ok = reconcile_prod_by_year_month(ch_test, ch_prod, vri_start, end_date, dedup=True)
    if mit_ok and vri_ok:
        now = datetime.now()
        state_set(ch_prod, "prod_daily_sync", now)
        if full:
            state_set(ch_prod, "prod_full_sync", now)
    log.info("PROD sync: done (mit=%s vri=%s)", mit_ok, vri_ok)
    return mit_ok and vri_ok

def maybe_sync_prod(ch_test: CH, ch_prod: CH, start_date: date, upstream_ok: bool) -> tuple[bool, bool]:
    """Run the daily prod publish when the local time window is open."""
    prod_now = datetime.now()
    if not _should_run_daily_after(ch_prod, "prod_daily_sync", prod_now, PROD_SYNC_HOUR):
        log.info("PROD sync: skipped")
        return upstream_ok, False
    if not upstream_ok:
        log.warning("PROD sync: skipped because test sync/check failed")
        return False, True
    try:
        return sync_prod_from_test(ch_test, ch_prod, start_date, prod_now.date()), True
    except Exception:
        log.exception("PROD sync: failed")
        return False, True

def run_pipeline(ch_test: CH, ch_prod: CH, client: FGISClient, start_date: date, end_date: date) -> bool:
    """Execute one idempotent sync cycle.

    systemd starts this script at fixed wall-clock times. The state gates below
    decide which pieces are due, so reruns are safe and long-running checks do
    not require separate cron entries.
    """
    now = datetime.now()
    ok = True

    # Fast path first: keep the user-facing test database fresh three times per
    # day without scanning the full historical dataset.
    if should_run(ch_test, "test_recent_vri", TEST_SYNC_EVERY, now):
        try:
            sync_test_mit(ch_test, client)
            ok = sync_test_recent_vri(ch_test, client, start_date, end_date) and ok
        except Exception:
            ok = False
            log.exception("TEST recent sync: failed")
    else:
        log.info("TEST recent sync: skipped")

    # If this is the 21:00 run, publish before starting an expensive weekly or
    # monthly check. If a long check crosses 21:00, we try again at the end.
    ok, prod_done = maybe_sync_prod(ch_test, ch_prod, start_date, ok)

    # Safety checks are intentionally range-first: healthy months/years finish
    # with counts only; only mismatches degrade to day reloads.
    if should_run_any(ch_test, ("test_full_reconcile", "simple_remote_full"), FULL_RECONCILE_EVERY, now):
        try:
            ok = reconcile_test_full(ch_test, client, start_date, end_date) and ok
        except Exception:
            ok = False
            log.exception("TEST full check: failed")
    elif should_run(ch_test, "test_year_reconcile", YEAR_RECONCILE_EVERY, now):
        try:
            ok = reconcile_test_year(ch_test, client, start_date, end_date) and ok
        except Exception:
            ok = False
            log.exception("TEST year check: failed")
    else:
        log.info("TEST reconcile: skipped")

    if not prod_done:
        ok, _prod_done = maybe_sync_prod(ch_test, ch_prod, start_date, ok)

    return ok

def main() -> None:
    """Wire environment, ClickHouse clients, and the simplified pipeline."""
    if not acquire_process_lock():
        log.info("Another backend_sync.py is already running; exiting.")
        return

    host = env_pick("CH_HOST", default="127.0.0.1")
    port = int(env_pick("CH_PORT", default="9001"))
    user = env_pick("CH_USER_INGEST", "CH_USER", "CH_USER_READ", default="default")
    password = env_pick("CH_PASS_INGEST", "CH_PASS", "CH_PASS_READ", default="")
    db_test = env_pick("CH_DB_TEST", "CH_DB", default="fgis_test").strip() or "fgis_test"
    db_prod = env_pick("CH_DB_PROD", default="fgis_prod").strip() or "fgis_prod"

    rps = float(os.getenv("FGIS_RPS", "0.2"))
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None
    start_date_env = os.getenv("START_DATE", "").strip()
    start_date = parse_ymd(start_date_env) if start_date_env else DEFAULT_START_DATE
    if start_date is None:
        start_date = DEFAULT_START_DATE
    end_date = date.today()

    ch_admin = CH(host, port, user, password, "default")
    ensure_tables(ch_admin, db_test)
    ensure_tables(ch_admin, db_prod)
    ch_test = CH(host, port, user, password, db_test)
    ch_prod = CH(host, port, user, password, db_prod)

    client = FGISClient(proxy=proxy, rps=rps)
    log.info("Pipeline: start_date=%s end_date=%s rps=%s", start_date, end_date, rps)
    run_pipeline(ch_test, ch_prod, client, start_date, end_date)
