"""HTTP utilities for talking to the FGIS endpoints."""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

import requests

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]
# Keep a small floor only to avoid division by zero. If the operator chooses a
# very slow poll rate, we should honor it instead of silently speeding up.
MIN_RPS = 0.05
RETRY_STATUSES = {429, 500, 502, 503, 504}


def _headers() -> Dict[str, str]:
    """Return a randomized header set that mimics a browser."""
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://fgis.gost.ru/fundmetrology/",
    }


@dataclass
class HttpClient:
    """Rate-limited session wrapper used for all FGIS requests."""

    proxy: Optional[str]
    rps: float
    timeout: int = 120

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._rps = max(self.rps, MIN_RPS)
        self._min_interval = 1.0 / self._rps
        self._next_request_at = 0.0
        self._proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        try:
            self.timeout = max(1, int(os.getenv("HTTP_TIMEOUT", str(self.timeout))))
        except Exception:
            self.timeout = 120
        self._max_retries = max(1, int(os.getenv("HTTP_MAX_RETRIES", "10")))
        self._base_sleep = float(os.getenv("HTTP_BASE_SLEEP", "5.0"))
        self._max_sleep = float(os.getenv("HTTP_MAX_SLEEP", "120.0"))

    def _wait_for_slot(self) -> None:
        """Ensure request start rate does not exceed the configured RPS."""
        now = time.monotonic()
        if self._next_request_at > now:
            time.sleep(self._next_request_at - now)
        started_at = time.monotonic()
        self._next_request_at = started_at + self._min_interval

    def _retry_after_seconds(self, header_value: Optional[str]) -> Optional[float]:
        """Parse Retry-After as either delay seconds or an HTTP date."""
        if not header_value:
            return None
        value = str(header_value).strip()
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())

    def _retry_sleep_seconds(self, attempt: int, retry_after_header: Optional[str] = None) -> float:
        """Return exponential backoff, honoring Retry-After when present."""
        backoff_s = min(self._max_sleep, self._base_sleep * (2**attempt))
        retry_after_s = self._retry_after_seconds(retry_after_header)
        if retry_after_s is not None:
            return max(backoff_s, retry_after_s)
        return backoff_s + random.random()

    def json(self, url: str) -> Dict[str, Any]:
        """Fetch JSON with throttling and shared session."""
        last_err: Optional[Exception] = None
        last_detail = ""
        for attempt in range(self._max_retries):
            self._wait_for_slot()
            try:
                response = self._session.get(url, headers=_headers(), timeout=self.timeout, proxies=self._proxies)
                if response.status_code == 200:
                    return response.json()
                if response.status_code in RETRY_STATUSES:
                    last_detail = f"HTTP {response.status_code}"
                    if attempt + 1 < self._max_retries:
                        sleep_s = self._retry_sleep_seconds(attempt, response.headers.get("Retry-After"))
                        time.sleep(sleep_s)
                    continue
                response.raise_for_status()
            except Exception as exc:
                last_err = exc
                last_detail = str(exc)
                if attempt + 1 < self._max_retries:
                    sleep_s = self._retry_sleep_seconds(attempt)
                    time.sleep(sleep_s)
        detail = last_detail or str(last_err) or "unknown error"
        raise RuntimeError(f"Failed after {self._max_retries} retries: {detail}")
