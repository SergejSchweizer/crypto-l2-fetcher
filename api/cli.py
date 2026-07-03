"""Command-line interface for Deribit L2 order book ingestion."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from typing import TypeAlias

from api.commands.bronze import dispatch_command, log_module_name_for_args
from api.constants import (
    BRONZE_BUILDER_COMMAND,
    FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
    INDEX_PRICE_BRONZE_BUILDER_COMMAND,
    INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
    LEGACY_BRONZE_BUILDER_COMMAND,
    LEGACY_L2_BRONZE_BUILDER_COMMAND,
    LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND,
    OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
    OPTION_L2_BRONZE_BUILDER_COMMAND,
    OPTIONS_BRONZE_BUILDER_COMMAND,
    RECENT_TRADES_BRONZE_BUILDER_COMMAND,
    VALIDATE_SYMBOLS_COMMAND,
    VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
)
from api.runtime import configure_logging
from ingestion.config import (
    Config,
    config_bool,
    config_float,
    config_int,
    config_section,
    config_str,
    config_str_list,
    load_config,
)
from ingestion.futures_summary import (
    FUTURES_SUMMARY_SCHEMA_VERSION,
    FUTURES_SUMMARY_SOURCE,
    FuturesSummarySnapshotRow,
)
from ingestion.index_price import (
    INDEX_PRICE_SCHEMA_VERSION,
    INDEX_PRICE_SOURCE,
    IndexPriceSnapshotRow,
)
from ingestion.instrument_metadata import (
    INSTRUMENT_METADATA_SCHEMA_VERSION,
    INSTRUMENT_METADATA_SOURCE,
    InstrumentMetadataSnapshotRow,
)
from ingestion.l2 import L2Snapshot
from ingestion.option_instrument_ticker import (
    OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION,
    OPTION_INSTRUMENT_TICKER_SOURCE,
    OptionInstrumentTickerSnapshotRow,
)
from ingestion.option_l2 import (
    OPTION_L2_SCHEMA_VERSION,
    OPTION_L2_SOURCE,
    OptionL2SnapshotRow,
)
from ingestion.options import (
    OPTION_TICKER_SCHEMA_VERSION,
    OPTION_TICKER_SOURCE,
    OptionTickerSnapshotRow,
)
from ingestion.recent_trades import (
    RECENT_TRADE_SCHEMA_VERSION,
    RECENT_TRADE_SOURCE,
    RecentTradeSnapshotRow,
)
from ingestion.volatility_index import (
    VOLATILITY_INDEX_SCHEMA_VERSION,
    VOLATILITY_INDEX_SOURCE,
    VolatilityIndexSnapshotRow,
)

__all__ = ["build_parser", "main"]

SnapshotsBySymbol = dict[str, list[L2Snapshot]]
OptionRowsByCurrency = dict[str, list[OptionTickerSnapshotRow]]
OptionL2RowsByInstrument = dict[str, OptionL2SnapshotRow]
OptionInstrumentTickerRowsByInstrument = dict[str, OptionInstrumentTickerSnapshotRow]
InstrumentMetadataRowsByDate = dict[str, list[InstrumentMetadataSnapshotRow]]
IndexPriceRowsBySymbol = dict[str, list[IndexPriceSnapshotRow]]
RecentTradeRowsByScope = dict[str, list[RecentTradeSnapshotRow]]
FuturesSummaryRowsByCurrency = dict[str, list[FuturesSummarySnapshotRow]]
VolatilityIndexRowsByCurrency = dict[str, list[VolatilityIndexSnapshotRow]]
CommandHandler: TypeAlias = Callable[[argparse.Namespace, logging.Logger, Config], None]


def _boolean_optional_flag(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool,
    help_text: str,
) -> None:
    parser.add_argument(
        f"--{name}",
        action=argparse.BooleanOptionalAction,
        default=default,
        help=help_text,
    )


def _debug_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG-level logging for this command",
    )


def _lake_root_flag(parser: argparse.ArgumentParser, config: Config) -> None:
    parser.add_argument(
        "--lake-root",
        default=config_str(config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )


def _save_parquet_lake_flag(
    parser: argparse.ArgumentParser,
    config: Config,
    *,
    default: bool,
    help_text: str,
) -> None:
    _boolean_optional_flag(
        parser,
        "save-parquet-lake",
        config_bool(config, "save_parquet_lake", default),
        help_text,
    )


def _source_schema_flags(
    parser: argparse.ArgumentParser,
    config: Config,
    *,
    source_default: str,
    schema_version_default: str,
) -> None:
    parser.add_argument(
        "--source",
        default=config_str(config, "source", source_default),
        help="Source identifier written to bronze rows",
    )
    parser.add_argument(
        "--schema-version",
        default=config_str(config, "schema_version", schema_version_default),
        help="Schema version tag to annotate each normalized row",
    )


def _json_output_flag(parser: argparse.ArgumentParser, config: Config, fallback_config: Config) -> None:
    _boolean_optional_flag(
        parser,
        "json-output",
        config_bool(config, "json_output", config_bool(fallback_config, "json_output", True)),
        "Print JSON output",
    )


def build_parser(config: Config | None = None) -> argparse.ArgumentParser:
    """Create the CLI parser."""

    config = config or load_config()
    ingestion_config = config_section(config, "ingestion")
    parser = argparse.ArgumentParser(description="crypto-live-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    l2_parser = subparsers.add_parser(
        BRONZE_BUILDER_COMMAND,
        aliases=[LEGACY_L2_BRONZE_BUILDER_COMMAND, LEGACY_BRONZE_BUILDER_COMMAND],
        help="Fetch Deribit perpetual L2 snapshots and persist raw rows to the bronze parquet lake",
    )
    l2_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    l2_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
        type=str,
        help="Symbols to fetch, separated by spaces or commas",
    )
    l2_parser.add_argument(
        "--levels",
        type=int,
        default=config_int(ingestion_config, "levels", 50),
        help="Number of book levels per side to request",
    )
    l2_parser.add_argument(
        "--snapshot-count",
        type=int,
        default=config_int(ingestion_config, "snapshot_count", 5),
        help="Polling ticks per symbol to collect per run",
    )
    l2_parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=config_float(ingestion_config, "poll_interval_s", 10.0),
        help="Sleep interval between polling ticks",
    )
    _lake_root_flag(l2_parser, ingestion_config)
    l2_parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=config_float(ingestion_config, "max_runtime_s", 50.0),
        help="Maximum collection runtime in seconds; 0 disables the budget",
    )
    _save_parquet_lake_flag(
        l2_parser,
        ingestion_config,
        default=False,
        help_text="Save raw perpetual L2 snapshots to bronze parquet lake partitions",
    )
    _json_output_flag(l2_parser, ingestion_config, ingestion_config)
    _debug_flag(l2_parser)

    options_config = config_section(ingestion_config, "options")
    options_parser = subparsers.add_parser(
        OPTIONS_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit option ticker snapshots and persist raw rows to the bronze parquet lake",
    )
    options_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    options_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(options_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Option symbols/currencies to fetch, separated by spaces or commas",
    )
    _lake_root_flag(options_parser, options_config)
    options_parser.add_argument(
        "--save-parquet-lake",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save raw option ticker snapshots to bronze parquet lake partitions",
    )
    _source_schema_flags(
        options_parser,
        options_config,
        source_default=OPTION_TICKER_SOURCE,
        schema_version_default=OPTION_TICKER_SCHEMA_VERSION,
    )
    _json_output_flag(options_parser, options_config, ingestion_config)
    _debug_flag(options_parser)

    futures_summary_config = config_section(ingestion_config, "futures_summary")
    futures_summary_parser = subparsers.add_parser(
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit futures summary snapshots and persist raw rows to the bronze parquet lake",
    )
    futures_summary_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    futures_summary_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(futures_summary_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Futures symbols/currencies to fetch, separated by spaces or commas",
    )
    _lake_root_flag(futures_summary_parser, futures_summary_config)
    _save_parquet_lake_flag(
        futures_summary_parser,
        futures_summary_config,
        default=True,
        help_text="Save raw futures summary rows to bronze parquet lake partitions",
    )
    _source_schema_flags(
        futures_summary_parser,
        futures_summary_config,
        source_default=FUTURES_SUMMARY_SOURCE,
        schema_version_default=FUTURES_SUMMARY_SCHEMA_VERSION,
    )
    _json_output_flag(futures_summary_parser, futures_summary_config, ingestion_config)
    _debug_flag(futures_summary_parser)

    option_l2_config = config_section(ingestion_config, "option_l2")
    option_l2_parser = subparsers.add_parser(
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        aliases=[LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND],
        help="Fetch Deribit selected option order-book snapshots into the bronze parquet lake",
    )
    option_l2_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    option_l2_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(option_l2_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Option symbols/currencies used when discovering active instruments",
    )
    option_l2_parser.add_argument(
        "--instruments",
        nargs="+",
        default=config_str_list(option_l2_config, "instruments", []),
        type=str,
        help="Explicit Deribit option instruments to fetch; skips currency discovery when provided",
    )
    option_l2_parser.add_argument(
        "--depth",
        type=int,
        default=config_int(option_l2_config, "depth", 20),
        help="Number of order-book levels per side to request for each option",
    )
    _lake_root_flag(option_l2_parser, option_l2_config)
    option_l2_parser.add_argument(
        "--max-instruments-per-run",
        type=int,
        default=config_int(option_l2_config, "max_instruments_per_run", 60),
        help="Maximum selected option instruments to fetch sequentially per currency",
    )
    _save_parquet_lake_flag(
        option_l2_parser,
        option_l2_config,
        default=True,
        help_text="Save selected option order-book snapshots to bronze parquet lake partitions",
    )
    _source_schema_flags(
        option_l2_parser,
        option_l2_config,
        source_default=OPTION_L2_SOURCE,
        schema_version_default=OPTION_L2_SCHEMA_VERSION,
    )
    _json_output_flag(option_l2_parser, option_l2_config, ingestion_config)
    _debug_flag(option_l2_parser)

    option_instrument_ticker_config = config_section(ingestion_config, "option_instrument_ticker")
    option_instrument_ticker_parser = subparsers.add_parser(
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit per-option ticker IV and Greeks into the bronze parquet lake",
    )
    option_instrument_ticker_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    option_instrument_ticker_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(option_instrument_ticker_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Option symbols/currencies used when discovering active instruments",
    )
    option_instrument_ticker_parser.add_argument(
        "--instruments",
        nargs="+",
        default=config_str_list(option_instrument_ticker_config, "instruments", []),
        type=str,
        help="Explicit Deribit option instruments to fetch; skips currency discovery when provided",
    )
    _lake_root_flag(option_instrument_ticker_parser, option_instrument_ticker_config)
    option_instrument_ticker_parser.add_argument(
        "--max-instruments-per-run",
        type=int,
        default=config_int(option_instrument_ticker_config, "max_instruments_per_run", 60),
        help="Maximum selected option instruments to fetch sequentially per currency",
    )
    _save_parquet_lake_flag(
        option_instrument_ticker_parser,
        option_instrument_ticker_config,
        default=True,
        help_text="Save per-option ticker snapshots to bronze parquet lake partitions",
    )
    _source_schema_flags(
        option_instrument_ticker_parser,
        option_instrument_ticker_config,
        source_default=OPTION_INSTRUMENT_TICKER_SOURCE,
        schema_version_default=OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION,
    )
    _json_output_flag(option_instrument_ticker_parser, option_instrument_ticker_config, ingestion_config)
    _debug_flag(option_instrument_ticker_parser)

    instrument_metadata_config = config_section(ingestion_config, "instrument_metadata")
    instrument_metadata_parser = subparsers.add_parser(
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit instrument metadata snapshots and persist raw rows to the bronze parquet lake",
    )
    instrument_metadata_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(instrument_metadata_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Base symbols/currencies to fetch instrument metadata for",
    )
    instrument_metadata_parser.add_argument(
        "--kind",
        default=config_str(instrument_metadata_config, "kind", "option"),
        help="Deribit instrument kind filter, for example option or future",
    )
    _boolean_optional_flag(
        instrument_metadata_parser,
        "include-inactive",
        config_bool(instrument_metadata_config, "include_inactive", False),
        "Include expired/inactive instruments from Deribit",
    )
    _lake_root_flag(instrument_metadata_parser, instrument_metadata_config)
    _save_parquet_lake_flag(
        instrument_metadata_parser,
        instrument_metadata_config,
        default=True,
        help_text="Save raw instrument metadata rows to bronze parquet lake partitions",
    )
    _source_schema_flags(
        instrument_metadata_parser,
        instrument_metadata_config,
        source_default=INSTRUMENT_METADATA_SOURCE,
        schema_version_default=INSTRUMENT_METADATA_SCHEMA_VERSION,
    )
    _json_output_flag(instrument_metadata_parser, instrument_metadata_config, ingestion_config)
    _debug_flag(instrument_metadata_parser)

    index_price_config = config_section(ingestion_config, "index_price")
    index_price_parser = subparsers.add_parser(
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit index prices and persist minutely raw rows to the bronze parquet lake",
    )
    index_price_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(index_price_config, "symbols", ["btc_usd", "eth_usd", "sol_usdc"]),
        type=str,
        help="Deribit index names to fetch, separated by spaces or commas",
    )
    _lake_root_flag(index_price_parser, index_price_config)
    _save_parquet_lake_flag(
        index_price_parser,
        index_price_config,
        default=True,
        help_text="Save raw index price rows to bronze parquet lake partitions",
    )
    _source_schema_flags(
        index_price_parser,
        index_price_config,
        source_default=INDEX_PRICE_SOURCE,
        schema_version_default=INDEX_PRICE_SCHEMA_VERSION,
    )
    _json_output_flag(index_price_parser, index_price_config, ingestion_config)
    _debug_flag(index_price_parser)

    volatility_index_config = config_section(ingestion_config, "volatility_index")
    volatility_index_parser = subparsers.add_parser(
        VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit volatility-index candles and persist raw rows to the bronze parquet lake",
    )
    volatility_index_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(volatility_index_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Volatility-index currencies to fetch, separated by spaces or commas",
    )
    volatility_index_parser.add_argument(
        "--resolution",
        type=int,
        default=config_int(volatility_index_config, "resolution", 60),
        help="Deribit volatility-index candle resolution",
    )
    volatility_index_parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=config_int(volatility_index_config, "lookback_seconds", 600),
        help="Overlap window start in seconds before the run snapshot minute",
    )
    _lake_root_flag(volatility_index_parser, volatility_index_config)
    _save_parquet_lake_flag(
        volatility_index_parser,
        volatility_index_config,
        default=True,
        help_text="Save volatility-index candles to bronze parquet lake partitions",
    )
    _source_schema_flags(
        volatility_index_parser,
        volatility_index_config,
        source_default=VOLATILITY_INDEX_SOURCE,
        schema_version_default=VOLATILITY_INDEX_SCHEMA_VERSION,
    )
    _json_output_flag(volatility_index_parser, volatility_index_config, ingestion_config)
    _debug_flag(volatility_index_parser)

    recent_trades_config = config_section(ingestion_config, "recent_trades")
    recent_trades_parser = subparsers.add_parser(
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit recent option/future trades and persist raw rows to the bronze parquet lake",
    )
    recent_trades_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    recent_trades_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(recent_trades_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Trade tape symbols/currencies to fetch, separated by spaces or commas",
    )
    recent_trades_parser.add_argument(
        "--kinds",
        nargs="+",
        default=config_str_list(recent_trades_config, "kinds", ["option", "future"]),
        type=str,
        help="Deribit trade kinds to fetch, for example option and future",
    )
    recent_trades_parser.add_argument(
        "--count",
        type=int,
        default=config_int(recent_trades_config, "count", 1000),
        help="Maximum trades to request per currency/kind",
    )
    recent_trades_parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=config_int(recent_trades_config, "lookback_seconds", 90),
        help="Overlap window start in seconds before the run snapshot minute",
    )
    recent_trades_parser.add_argument(
        "--start-timestamp-ms",
        type=int,
        default=None,
        help="Explicit Deribit start_timestamp override in Unix milliseconds",
    )
    recent_trades_parser.add_argument(
        "--sorting",
        choices=["asc", "desc", "default"],
        default=config_str(recent_trades_config, "sorting", "asc"),
        help="Deribit trade sorting direction",
    )
    _lake_root_flag(recent_trades_parser, recent_trades_config)
    _save_parquet_lake_flag(
        recent_trades_parser,
        recent_trades_config,
        default=True,
        help_text="Save raw recent trade rows to bronze parquet lake partitions",
    )
    _source_schema_flags(
        recent_trades_parser,
        recent_trades_config,
        source_default=RECENT_TRADE_SOURCE,
        schema_version_default=RECENT_TRADE_SCHEMA_VERSION,
    )
    _json_output_flag(recent_trades_parser, recent_trades_config, ingestion_config)
    _debug_flag(recent_trades_parser)

    validate_parser = subparsers.add_parser(
        VALIDATE_SYMBOLS_COMMAND,
        help="Resolve symbols and check whether Deribit returns a usable L2 book",
    )
    validate_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    validate_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
        type=str,
        help="Symbols to validate, separated by spaces or commas",
    )
    validate_parser.add_argument(
        "--levels",
        type=int,
        default=1,
        help="Number of book levels per side to request for validation",
    )
    validate_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print JSON output",
    )
    _debug_flag(validate_parser)

    return parser


def main() -> None:
    """CLI entrypoint."""

    config = load_config()
    parser = build_parser(config)
    args = parser.parse_args()
    logger = configure_logging(module_name=log_module_name_for_args(args), config=config)
    dispatch_command(args=args, logger=logger, config=config)


if __name__ == "__main__":
    main()
