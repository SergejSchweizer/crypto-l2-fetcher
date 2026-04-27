"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime

from ingestion.spot import (
    SpotCandle,
    fetch_binance_spot_candles,
    list_binance_supported_intervals,
    normalize_timeframe,
)



def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Convert a ``SpotCandle`` into JSON-safe dictionary."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data



def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="L2 Synchronizer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    spot_parser = subparsers.add_parser("fetch-spot", help="Fetch spot candles from Binance")
    spot_parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"], help="Spot symbols")
    spot_parser.add_argument(
        "--timeframe",
        "--interval",
        dest="timeframe",
        default="1h",
        help="Candle timeframe, e.g. M1, M5, H1, D1, 1m, 1h, 1d",
    )
    spot_parser.add_argument("--limit", type=int, default=10, help="Number of candles per symbol")

    subparsers.add_parser("list-spot-timeframes", help="List Binance-supported spot candle timeframes")

    return parser



def main() -> None:
    """CLI entrypoint."""

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "fetch-spot":
        output: dict[str, list[dict[str, object]]] = {}
        timeframe = normalize_timeframe(args.timeframe)
        for symbol in args.symbols:
            candles = fetch_binance_spot_candles(symbol=symbol, interval=timeframe, limit=args.limit)
            output[symbol.upper()] = [_serialize_candle(item) for item in candles]
        print(json.dumps(output, indent=2))
    elif args.command == "list-spot-timeframes":
        print(json.dumps({"exchange": "binance", "timeframes": list(list_binance_supported_intervals())}, indent=2))


if __name__ == "__main__":
    main()
