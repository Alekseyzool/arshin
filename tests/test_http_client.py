from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch
from urllib.parse import quote


requests_stub = types.ModuleType("requests")
requests_utils_stub = types.ModuleType("requests.utils")
requests_utils_stub.quote = quote


class _DummySession:
    pass


requests_stub.Session = _DummySession
requests_stub.utils = requests_utils_stub
sys.modules.setdefault("requests", requests_stub)
sys.modules.setdefault("requests.utils", requests_utils_stub)

from fgis_clickhouse.http_client import HttpClient


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)

    def get(self, url: str, headers: dict, timeout: int, proxies: dict | None):
        return self._responses.pop(0)


class HttpClientTests(unittest.TestCase):
    def test_wait_for_slot_enforces_min_interval(self) -> None:
        with patch("fgis_clickhouse.http_client.requests.Session", return_value=_FakeSession([])):
            client = HttpClient(proxy=None, rps=2.0)

        sleep_calls: list[float] = []
        monotonic_values = iter([10.0, 10.0, 10.0, 10.5])

        with patch("fgis_clickhouse.http_client.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("fgis_clickhouse.http_client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)):
                client._wait_for_slot()
                client._wait_for_slot()

        self.assertEqual(sleep_calls, [0.5])

    def test_retry_after_header_overrides_short_backoff(self) -> None:
        session = _FakeSession(
            [
                _FakeResponse(429, headers={"Retry-After": "7"}),
                _FakeResponse(200, payload={"response": {"docs": []}}),
            ]
        )

        with patch("fgis_clickhouse.http_client.requests.Session", return_value=session):
            client = HttpClient(proxy=None, rps=5.0)

        sleep_calls: list[float] = []
        monotonic_values = iter([1.0, 1.0, 8.0, 8.0])

        with patch("fgis_clickhouse.http_client.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("fgis_clickhouse.http_client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)):
                payload = client.json("https://example.test")

        self.assertEqual(payload, {"response": {"docs": []}})
        self.assertEqual(sleep_calls, [7.0])
