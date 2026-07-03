"""Simple JSON HTTP client helpers."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from ingestion.config import load_config, typed_http_config


class HttpClientError(RuntimeError):
    """Raised when HTTP requests fail."""


@dataclass(frozen=True)
class HttpRequestConfig:
    """Resolved HTTP retry and timeout settings."""

    timeout_s: float
    max_retries: int
    retry_backoff_s: float


@lru_cache(maxsize=1)
def default_http_request_config() -> HttpRequestConfig:
    """Load default HTTP settings once per process."""

    http_config = typed_http_config(load_config())
    return HttpRequestConfig(
        timeout_s=http_config.timeout_s,
        max_retries=http_config.max_retries,
        retry_backoff_s=http_config.retry_backoff_s,
    )


def _retry_sleep(attempt: int, backoff_s: float) -> None:
    """Sleep with exponential backoff based on retry attempt index."""

    time.sleep(backoff_s * (2**attempt))


def _is_retryable_http_error(exc: HTTPError) -> bool:
    """Return True when HTTP status is generally transient."""

    return exc.code == 429 or exc.code >= 500


def get_json(
    url: str,
    params: dict[str, Any] | None = None,
    timeout_s: float | None = None,
    max_retries: int | None = None,
    retry_backoff_s: float | None = None,
) -> Any:
    """Fetch and decode JSON from an HTTP GET endpoint.

    Args:
        url (str): Base URL.
        params (dict[str, Any] | None): Optional query params.
        timeout_s (float | None): Socket timeout in seconds.
        max_retries (int | None): Number of retries after first request failure.
        retry_backoff_s (float | None): Base backoff in seconds used between retries.

    Returns:
        Any: Parsed JSON payload.

    Raises:
        HttpClientError: If request fails or payload is invalid JSON.
    """

    query = urlencode(params or {})
    request_url = f"{url}?{query}" if query else url
    defaults = default_http_request_config()
    timeout_value = timeout_s if timeout_s is not None else defaults.timeout_s
    retries = max_retries if max_retries is not None else defaults.max_retries
    backoff = retry_backoff_s if retry_backoff_s is not None else defaults.retry_backoff_s

    for attempt in range(retries + 1):
        try:
            with urlopen(request_url, timeout=timeout_value) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw)
        except HTTPError as exc:
            if _is_retryable_http_error(exc) and attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"HTTP error {exc.code} for {request_url}") from exc
        except URLError as exc:
            if attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"Connection error for {request_url}: {exc.reason}") from exc
        except TimeoutError as exc:
            if attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"Connection timeout for {request_url}") from exc
        except json.JSONDecodeError as exc:
            raise HttpClientError(f"Invalid JSON from {request_url}") from exc

    raise HttpClientError(f"Connection error for {request_url}: max retries exceeded")
