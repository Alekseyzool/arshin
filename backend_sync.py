"""Compatibility entry point for the FGIS backend sync.

The implementation is split into focused modules under ``fgis_clickhouse``.
This file intentionally re-exports the public helpers that older notebooks and
tests imported from ``backend_sync`` while keeping command-line behavior the
same: ``python backend_sync.py`` runs the scheduled pipeline once.
"""

from __future__ import annotations

import logging

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # Allows tests/imports before `pip install -r requirements.txt`.
    def load_dotenv(*_args, **_kwargs):
        return False

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger("fgis_backend")

from fgis_clickhouse.clickhouse_io import CH, ensure_tables
from fgis_clickhouse.dates import (
    iter_days,
    iter_month_ranges,
    iter_month_starts,
    iter_year_ranges,
    parse_date_any,
    parse_ymd,
)
from fgis_clickhouse.fgis_api import FGISClient
from fgis_clickhouse.mit_sync import (
    build_mit_row,
    detect_country,
    insert_mit_registry,
    sync_mit_registry,
)
from fgis_clickhouse.pipeline import (
    main,
    maybe_sync_prod,
    reconcile_due_test_vri,
    reconcile_test_vri_window,
    replace_prod_mit_from_test,
    run_manual_vri_reconcile,
    run_pipeline,
    sync_prod_from_test,
    sync_test_mit,
)
from fgis_clickhouse.runtime import (
    DAILY_MONTH_DAYS,
    DAILY_MONTH_EVERY,
    DEFAULT_START_DATE,
    DEFAULT_VRI_MIN_ROWS,
    DEFAULT_VRI_ROWS,
    DEFAULT_VRI_SLEEP,
    HALFYEAR_2Y_DAYS,
    HALFYEAR_2Y_EVERY,
    MIT_SYNC_EVERY,
    MONTHLY_YEAR_DAYS,
    MONTHLY_YEAR_EVERY,
    MAX_VRI_START_FALLBACK,
    PROCESS_LOCK,
    PROD_DEFAULT_DAYS,
    PROD_SYNC_HOUR,
    WEEKLY_3M_DAYS,
    WEEKLY_3M_EVERY,
    acquire_process_lock,
    env_pick,
    should_run,
    should_run_any,
    state_get,
    state_get_any,
    state_set,
)
from fgis_clickhouse.vri_sync import (
    build_vri_row,
    delete_vri_day,
    fq_for_day,
    fq_for_range,
    insert_verifications,
    iter_vri_pages,
    local_vri_counts_range,
    local_vri_digest_range,
    local_vri_stats,
    pick_start_date,
    reconcile_day_with_remote,
    reconcile_day_with_test,
    reconcile_prod_by_year_month,
    reconcile_remote_by_year_month,
    reload_vri_day_from_remote,
    remote_vri_count,
    replace_vri_day_from_test,
    resolve_vri_start_date,
    sync_vri_day,
    top_up_vri_day_from_remote,
)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped.")
