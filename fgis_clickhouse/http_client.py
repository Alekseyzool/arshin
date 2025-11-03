"""HTTP utilities for talking to the FGIS endpoints."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]
MIN_RPS = 0.2


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
    timeout: int = 40

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._rps = max(self.rps, MIN_RPS)
        self._proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

    def json(self, url: str) -> Dict[str, Any]:
        """Fetch JSON with throttling and shared session."""
        delay = random.uniform(0, 1.0 / self._rps)
        if delay > 0:
            time.sleep(delay)
        response = self._session.get(url, headers=_headers(), timeout=self.timeout, proxies=self._proxies)
        response.raise_for_status()
        return response.json()

