from __future__ import annotations

import sys
import types
import unittest
from datetime import date
from urllib.parse import quote


dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_stub)

requests_stub = types.ModuleType("requests")
requests_utils_stub = types.ModuleType("requests.utils")
requests_utils_stub.quote = quote


class _DummySession:
    pass


requests_stub.Session = _DummySession
requests_stub.utils = requests_utils_stub
sys.modules.setdefault("requests", requests_stub)
sys.modules.setdefault("requests.utils", requests_utils_stub)

import backend_sync


def _doc(vri_id: str) -> dict[str, str]:
    return {
        "applicability": True,
        "mi.modification": "",
        "mi.number": "",
        "mi.mitnumber": "",
        "mi.mititle": "",
        "mi.mitype": "",
        "org_title": "",
        "valid_date": "2026-04-10",
        "verification_date": "2026-04-03",
        "vri_id": vri_id,
    }


class _FakeCH:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []

    def insert(self, table: str, columns: list[str], rows: list[tuple[object, ...]]) -> None:
        self.rows.extend(rows)

    def scalar(self, sql: str):
        return date(2026, 4, 16)


class _ReloadFakeCH:
    db = "fgis_test"

    def __init__(self) -> None:
        self.main_rows: list[tuple[object, ...]] = [_doc("old")]
        self.stage_rows: list[tuple[object, ...]] = []

    def exec(self, sql: str, params: dict | None = None) -> None:
        if "TRUNCATE TABLE" in sql and "verifications_reload_tmp" in sql:
            self.stage_rows.clear()
        elif "ALTER TABLE" in sql and "DELETE WHERE" in sql:
            self.main_rows.clear()
        elif "INSERT INTO fgis_test.verifications SELECT" in sql:
            self.main_rows.extend(self.stage_rows)

    def insert(self, table: str, columns: list[str], rows: list[tuple[object, ...]]) -> None:
        if table == "verifications_reload_tmp":
            self.stage_rows.extend(rows)
        else:
            self.main_rows.extend(rows)

    def rows(self, sql: str):
        if "verifications_reload_tmp" in sql:
            ids = {row[-1] for row in self.stage_rows}
            return [(len(self.stage_rows), len(ids))]
        return []

    def scalar(self, sql: str):
        if "FROM fgis_test.verifications" in sql:
            return len(self.main_rows)
        return 0


class CursorFallbackTests(unittest.TestCase):
    def test_resolve_vri_start_date_honors_explicit_start(self) -> None:
        explicit = date(2026, 4, 2)
        self.assertEqual(
            backend_sync.resolve_vri_start_date(_FakeCH(), explicit, tail_days=3),
            explicit,
        )

    def test_sync_vri_day_switches_from_cursor_to_start(self) -> None:
        docs = [_doc("1"), _doc("2"), _doc("3"), _doc("4")]

        class FakeClient:
            def __init__(self) -> None:
                self.page_calls: list[tuple[int, int]] = []

            def vri_cursor(self, *, fq: str | None, rows: int, cursor_mark: str, sort: str = "vri_id asc"):
                if cursor_mark == "*":
                    return docs[:2], len(docs), "broken-cursor"
                raise RuntimeError(f"HTTP 500 on cursor {cursor_mark} rows={rows}")

            def vri_page(self, *, fq: str | None, rows: int, start: int, sort: str = "vri_id asc"):
                self.page_calls.append((start, rows))
                return docs[start : start + rows], len(docs)

        ch = _FakeCH()
        client = FakeClient()

        total = backend_sync.sync_vri_day(
            ch,
            client,
            date(2026, 4, 3),
            rows=4,
            sleep_s=0.0,
            skip_existing=False,
            min_rows=2,
        )

        self.assertEqual(total, 4)
        self.assertEqual(len(ch.rows), 4)
        self.assertEqual([row[-1] for row in ch.rows], ["1", "2", "3", "4"])
        self.assertEqual(client.page_calls, [(2, 4)])

    def test_iter_vri_pages_keeps_rows_fixed_after_switch_to_start(self) -> None:
        docs = [_doc("1"), _doc("2"), _doc("3"), _doc("4")]

        class FakeClient:
            def __init__(self) -> None:
                self.start_attempts: list[tuple[int, int]] = []

            def vri_cursor(self, *, fq: str | None, rows: int, cursor_mark: str, sort: str = "vri_id asc"):
                if cursor_mark == "*":
                    return docs[:2], len(docs), "broken-cursor"
                raise RuntimeError(f"HTTP 500 on cursor {cursor_mark} rows={rows}")

            def vri_page(self, *, fq: str | None, rows: int, start: int, sort: str = "vri_id asc"):
                self.start_attempts.append((start, rows))
                return docs[start : start + rows], len(docs)

        client = FakeClient()
        pages = list(
            backend_sync.iter_vri_pages(
                client,
                date(2026, 4, 3),
                rows=4,
                sleep_s=0.0,
                min_rows=1,
            )
        )

        self.assertEqual(
            [(page_num, page_rows, page_mode, [doc["vri_id"] for doc in page_docs]) for page_docs, _, page_num, page_rows, page_mode in pages],
            [
                (1, 4, "cursor", ["1", "2"]),
                (2, 4, "start", ["3", "4"]),
            ],
        )
        self.assertEqual(client.start_attempts, [(2, 4)])

    def test_iter_vri_pages_raises_clear_error_when_start_limit_would_be_exceeded(self) -> None:
        docs = [_doc("1")] * 10001

        class FakeClient:
            def vri_cursor(self, *, fq: str | None, rows: int, cursor_mark: str, sort: str = "vri_id asc"):
                if cursor_mark == "*":
                    return docs[:10000], 20000, "broken-cursor"
                raise RuntimeError(f"HTTP 429 on cursor {cursor_mark} rows={rows}")

            def vri_page(self, *, fq: str | None, rows: int, start: int, sort: str = "vri_id asc"):
                raise AssertionError("start fallback must not be attempted beyond the FGIS limit")

        pages = backend_sync.iter_vri_pages(
            FakeClient(),
            date(2026, 4, 7),
            rows=10000,
            sleep_s=0.0,
            min_rows=10,
        )
        next(pages)
        with self.assertRaisesRegex(RuntimeError, r"start fallback is unavailable beyond start=9999"):
            next(pages)

    def test_reload_accepts_stage_when_remote_count_changes_during_load(self) -> None:
        docs = [_doc("1"), _doc("2"), _doc("3")]

        class FakeClient:
            def __init__(self) -> None:
                self.counts = iter([2, 3])

            def vri_count(self, fq: str | None) -> int:
                return next(self.counts)

            def vri_cursor(self, *, fq: str | None, rows: int, cursor_mark: str, sort: str = "vri_id asc"):
                return docs, 2, cursor_mark

        ch = _ReloadFakeCH()

        ok = backend_sync.reload_vri_day_from_remote(
            ch,
            FakeClient(),
            date(2026, 4, 3),
            rows=9999,
            sleep_s=0.0,
        )

        self.assertTrue(ok)
        self.assertEqual([row[-1] for row in ch.main_rows], ["1", "2", "3"])
        self.assertEqual(ch.stage_rows, [])
