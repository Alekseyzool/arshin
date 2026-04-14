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


class CursorFallbackTests(unittest.TestCase):
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

    def test_iter_vri_pages_can_keep_shrinking_after_switch_to_start(self) -> None:
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
                if start == 2 and rows == 4:
                    raise RuntimeError("HTTP 500 on start page")
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
                (2, 2, "start", ["3", "4"]),
            ],
        )
        self.assertEqual(client.start_attempts, [(2, 4), (2, 2)])
