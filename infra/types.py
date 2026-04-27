"""Domain models for market data ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

Side = Literal["bid", "ask"]
MarketType = Literal["spot", "perp"]


@dataclass(frozen=True)
class OrderBookLevel:
    """A single L2 order-book level.

    Attributes:
        side: Book side, either ``bid`` or ``ask``.
        price: Price at the level.
        size: Quantity available at the level.
        level: Zero-based depth index.
    """

    side: Side
    price: float
    size: float
    level: int


@dataclass(frozen=True)
class OrderBookSnapshot:
    """L2 order-book snapshot for an instrument.

    Attributes:
        exchange: Exchange name (for example ``deribit``).
        symbol: Instrument identifier.
        market_type: Market type (``spot`` or ``perp``).
        captured_at: UTC capture timestamp.
        levels: Flattened list of bid/ask levels.
    """

    exchange: str
    symbol: str
    market_type: MarketType
    captured_at: datetime
    levels: list[OrderBookLevel]


@dataclass(frozen=True)
class PriceSnapshot:
    """Top-of-book price snapshot for a symbol.

    Attributes:
        exchange: Exchange name.
        symbol: Instrument or pair identifier.
        market_type: Market type (``spot`` or ``perp``).
        captured_at: UTC capture timestamp.
        last_price: Latest traded/mark price.
        best_bid: Best bid price.
        best_ask: Best ask price.
    """

    exchange: str
    symbol: str
    market_type: MarketType
    captured_at: datetime
    last_price: float
    best_bid: float
    best_ask: float


@dataclass(frozen=True)
class FundingRatePoint:
    """Funding observation for perpetual instruments.

    Attributes:
        exchange: Exchange name.
        symbol: Perpetual symbol.
        funding_time: UTC timestamp for the funding period.
        funding_rate: Funding rate value.
    """

    exchange: str
    symbol: str
    funding_time: datetime
    funding_rate: float
