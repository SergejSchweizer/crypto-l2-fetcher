"""Spot-market data ingestion adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from ingestion.http_client import get_json

BINANCE_SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)


@dataclass(frozen=True)
class SpotCandle:
    """OHLCV candle for a spot instrument.

    Attributes:
        exchange: Exchange identifier.
        symbol: Spot symbol such as ``BTCUSDT``.
        interval: Candle interval string accepted by the exchange.
        open_time: Candle open timestamp (UTC).
        close_time: Candle close timestamp (UTC).
        open_price: Open price.
        high_price: High price.
        low_price: Low price.
        close_price: Close price.
        volume: Base asset volume.
        quote_volume: Quote asset volume.
        trade_count: Number of trades in candle.
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trade_count: int



def _ms_to_utc(ts_ms: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""

    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)



def parse_binance_kline(symbol: str, interval: str, row: list[object]) -> SpotCandle:
    """Parse a single Binance kline row into a typed object."""

    return SpotCandle(
        exchange="binance",
        symbol=symbol,
        interval=interval,
        open_time=_ms_to_utc(int(row[0])),
        close_time=_ms_to_utc(int(row[6])),
        open_price=float(row[1]),
        high_price=float(row[2]),
        low_price=float(row[3]),
        close_price=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
    )


def list_binance_supported_intervals() -> tuple[str, ...]:
    """Return Binance-supported kline intervals."""

    return BINANCE_SUPPORTED_INTERVALS


def normalize_timeframe(value: str) -> str:
    """Normalize user-provided timeframe aliases into Binance interval format.

    Supports forms like ``M1``, ``m5``, ``H1``, ``D1``, ``W1``, and ``MN1``.
    """

    raw = value.strip()
    if not raw:
        raise ValueError("timeframe cannot be empty")

    lowered = raw.lower()
    if lowered.startswith("mn") and raw[2:].isdigit():
        candidate = f"{raw[2:]}M"
    elif raw[0].isalpha() and raw[1:].isdigit():
        candidate = f"{raw[1:]}{raw[0].lower()}"
    elif raw[:-1].isdigit() and raw[-1].isalpha():
        unit = raw[-1]
        if unit == "M":
            candidate = f"{raw[:-1]}M"
        else:
            candidate = f"{raw[:-1]}{unit.lower()}"
    else:
        candidate = lowered

    if candidate in BINANCE_SUPPORTED_INTERVALS:
        return candidate

    raise ValueError(
        f"Unsupported timeframe '{value}'. Supported values: {', '.join(BINANCE_SUPPORTED_INTERVALS)}"
    )



def fetch_binance_spot_candles(symbol: str, interval: str = "1h", limit: int = 100) -> list[SpotCandle]:
    """Fetch spot OHLCV candles from Binance public REST API.

    Args:
        symbol: Spot symbol, for example ``BTCUSDT`` or ``ETHUSDT``.
        interval: Binance interval string, for example ``1m``, ``5m``, ``1h``, ``1d``.
        limit: Number of candles (max 1000 on Binance).

    Returns:
        List of parsed spot candles.

    Raises:
        ValueError: If ``limit`` is invalid.
    """

    if limit <= 0 or limit > 1000:
        raise ValueError("limit must be in [1, 1000]")
    normalized_interval = normalize_timeframe(interval)

    payload = get_json(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": symbol.upper(), "interval": normalized_interval, "limit": limit},
    )

    if not isinstance(payload, list):
        raise ValueError("Unexpected Binance response format")

    return [parse_binance_kline(symbol.upper(), normalized_interval, row) for row in payload]
