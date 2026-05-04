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
from .dates import parse_ymd
from .fgis_api import FGISClient
from .mit_sync import sync_mit_registry
from .runtime import (
    DAILY_MONTH_DAYS,
    DAILY_MONTH_EVERY,
    DEFAULT_START_DATE,
    DEFAULT_VRI_ROWS,
    DEFAULT_VRI_SLEEP,
    HALFYEAR_2Y_DAYS,
    HALFYEAR_2Y_EVERY,
    MIT_SYNC_EVERY,
    MONTHLY_YEAR_DAYS,
    MONTHLY_YEAR_EVERY,
    PROD_DEFAULT_DAYS,
    PROD_SYNC_HOUR,
    WEEKLY_3M_DAYS,
    WEEKLY_3M_EVERY,
    acquire_process_lock,
    env_pick,
    should_run,
    state_get,
    state_get_any,
    state_set,
)
from .vri_sync import (
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

def reconcile_test_vri_window(
    ch: CH,
    client: FGISClient,
    start_date: date,
    end_date: date,
    *,
    days: int,
    label: str,
    state_keys: tuple[str, ...],
) -> bool:
    """Reconcile one rolling VRI window and mark all windows it covers.

    The job does not blindly reload the whole window. It compares year/month/day
    counts with FGIS and reloads only days where local rows differ from remote.
    """
    start = _window_start(start_date, end_date, days)
    log.info("TEST VRI %s check: %s -> %s", label, start, end_date)
    ok = reconcile_remote_by_year_month(ch, client, start, end_date, DEFAULT_VRI_ROWS, DEFAULT_VRI_SLEEP)
    if ok:
        now = datetime.now()
        for key in state_keys:
            state_set(ch, key, now)
    return ok

def _due_test_vri_scope(ch: CH, now: datetime) -> tuple[str, int, tuple[str, ...]] | None:
    """Pick the widest due test-vs-FGIS VRI window for this run."""
    scopes = [
        (
            "month",
            DAILY_MONTH_DAYS,
            DAILY_MONTH_EVERY,
            ("test_daily_month", "test_recent_vri"),
            ("test_daily_month",),
        ),
        (
            "3m",
            WEEKLY_3M_DAYS,
            WEEKLY_3M_EVERY,
            ("test_weekly_3m", "test_year_reconcile"),
            ("test_weekly_3m", "test_daily_month"),
        ),
        (
            "year",
            MONTHLY_YEAR_DAYS,
            MONTHLY_YEAR_EVERY,
            ("test_monthly_year", "test_year_reconcile", "test_full_reconcile", "simple_remote_full"),
            ("test_monthly_year", "test_weekly_3m", "test_daily_month"),
        ),
        (
            "2y",
            HALFYEAR_2Y_DAYS,
            HALFYEAR_2Y_EVERY,
            ("test_halfyear_2y", "test_full_reconcile", "simple_remote_full"),
            ("test_halfyear_2y", "test_monthly_year", "test_weekly_3m", "test_daily_month"),
        ),
    ]
    states = [(scope, state_get_any(ch, scope[3])) for scope in scopes]

    # On first deployment, initialize missing scopes from small to large across
    # separate timer runs instead of immediately launching the two-year window.
    for scope, last in states:
        if last is None:
            label, days, _every, _aliases, mark_keys = scope
            return label, days, mark_keys

    # Once all scopes have checkpoints, run the widest expired one. A wider
    # successful check refreshes the smaller checkpoints too.
    for scope, last in reversed(states):
        label, days, every, _aliases, mark_keys = scope
        if now - last >= every:
            return label, days, mark_keys
    return None

def reconcile_due_test_vri(ch: CH, client: FGISClient, start_date: date, end_date: date) -> bool:
    """Run the scheduled test-vs-FGIS VRI window that is due now."""
    scope = _due_test_vri_scope(ch, datetime.now())
    if not scope:
        log.info("TEST VRI reconcile: skipped")
        return True
    label, days, state_keys = scope
    return reconcile_test_vri_window(
        ch,
        client,
        start_date,
        end_date,
        days=days,
        label=label,
        state_keys=state_keys,
    )

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

def _latest_test_scope_for_prod(ch_test: CH, ch_prod: CH) -> tuple[str, int, tuple[str, ...]]:
    """Choose how much prod must consume based on completed test windows."""
    scopes = [
        (
            "2y",
            HALFYEAR_2Y_DAYS,
            ("test_halfyear_2y", "test_full_reconcile", "simple_remote_full"),
            ("prod_2y_sync", "prod_full_sync", "simple_prod_full"),
            ("prod_2y_sync", "prod_year_sync", "prod_3m_sync", "prod_month_sync"),
        ),
        (
            "year",
            MONTHLY_YEAR_DAYS,
            ("test_monthly_year", "test_year_reconcile"),
            ("prod_year_sync",),
            ("prod_year_sync", "prod_3m_sync", "prod_month_sync"),
        ),
        (
            "3m",
            WEEKLY_3M_DAYS,
            ("test_weekly_3m",),
            ("prod_3m_sync",),
            ("prod_3m_sync", "prod_month_sync"),
        ),
    ]
    for label, days, test_keys, prod_keys, mark_keys in scopes:
        last_test = state_get_any(ch_test, test_keys)
        last_prod = state_get_any(ch_prod, prod_keys)
        if last_test is not None and (last_prod is None or last_test > last_prod):
            return label, days, mark_keys
    return "month", PROD_DEFAULT_DAYS, ("prod_month_sync",)

def sync_prod_from_test(ch_test: CH, ch_prod: CH, start_date: date, end_date: date) -> bool:
    """Publish test to prod using the newest completed test-check scope."""
    scope, days, prod_scope_keys = _latest_test_scope_for_prod(ch_test, ch_prod)
    vri_start = _window_start(start_date, end_date, days)
    log.info("PROD sync: start (%s, %s -> %s)", scope, vri_start, end_date)
    mit_ok = replace_prod_mit_from_test(ch_test, ch_prod)
    vri_ok = reconcile_prod_by_year_month(ch_test, ch_prod, vri_start, end_date, dedup=True)
    if mit_ok and vri_ok:
        now = datetime.now()
        state_set(ch_prod, "prod_daily_sync", now)
        for key in prod_scope_keys:
            state_set(ch_prod, key, now)
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

    # MIT is cheap compared to VRI and is refreshed daily before VRI windows.
    if should_run(ch_test, "test_mit_sync", MIT_SYNC_EVERY, now):
        try:
            sync_test_mit(ch_test, client)
        except Exception:
            ok = False
            log.exception("TEST MIT sync: failed")
    else:
        log.info("TEST MIT sync: skipped")

    # If this is the 21:00 run, publish before starting an expensive weekly or
    # monthly check. If a long check crosses 21:00, we try again at the end.
    ok, prod_done = maybe_sync_prod(ch_test, ch_prod, start_date, ok)

    # VRI checks are range-first: healthy years/months finish with counts only;
    # mismatching months degrade to day reloads through a staging table.
    try:
        ok = reconcile_due_test_vri(ch_test, client, start_date, end_date) and ok
    except Exception:
        ok = False
        log.exception("TEST VRI reconcile: failed")

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
