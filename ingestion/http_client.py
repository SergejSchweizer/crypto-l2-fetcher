"""Simple JSON HTTP client helpers."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


class HttpClientError(RuntimeError):
    """Raised when HTTP requests fail."""



def get_json(url: str, params: dict[str, Any] | None = None, timeout_s: float = 15.0) -> Any:
    """Fetch and decode JSON from an HTTP GET endpoint.

    Args:
        url: Base URL.
        params: Optional query params.
        timeout_s: Socket timeout in seconds.

    Returns:
        Parsed JSON payload.

    Raises:
        HttpClientError: If request fails or payload is invalid JSON.
    """

    query = urlencode(params or {})
    request_url = f"{url}?{query}" if query else url

    try:
        with urlopen(request_url, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw)
    except HTTPError as exc:
        raise HttpClientError(f"HTTP error {exc.code} for {request_url}") from exc
    except URLError as exc:
        raise HttpClientError(f"Connection error for {request_url}: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise HttpClientError(f"Invalid JSON from {request_url}") from exc
