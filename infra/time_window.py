"""Utilities for CLI date-window handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

FetchMode = Literal["full", "last_n_days"]


@dataclass(frozen=True)
class FetchWindow:
    """Inclusive time window used by ingestion jobs.

    Attributes:
        start: UTC start datetime.
        end: UTC end datetime.
    """

    start: datetime
    end: datetime



def utc_now() -> datetime:
    """Return current UTC time."""

    return datetime.now(timezone.utc)



def resolve_window(mode: FetchMode, now: datetime, days: int | None, full_start: datetime) -> FetchWindow:
    """Resolve fetch window based on CLI mode.

    Args:
        mode: Fetch behavior, either ``full`` or ``last_n_days``.
        now: Current UTC datetime.
        days: Number of days for ``last_n_days`` mode.
        full_start: Earliest timestamp used by ``full`` mode.

    Returns:
        Resolved UTC time window.

    Raises:
        ValueError: If input arguments are inconsistent.
    """

    if now.tzinfo is None:
        raise ValueError("'now' must be timezone-aware")
    if full_start.tzinfo is None:
        raise ValueError("'full_start' must be timezone-aware")

    if mode == "full":
        if full_start >= now:
            raise ValueError("full_start must be earlier than now")
        return FetchWindow(start=full_start, end=now)

    if days is None:
        raise ValueError("'days' is required for last_n_days mode")
    if days <= 0:
        raise ValueError("'days' must be positive")

    start = now - timedelta(days=days)
    return FetchWindow(start=start, end=now)
