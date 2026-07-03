"""Bronze dataset command runners and dispatch helpers."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from time import perf_counter
from typing import TypeAlias, cast

from api.commands.runtime import (
    DatasetCommandResult,
    emit_dataset_command_result,
)
from api.commands.runtime import (
    emit_json_output as _emit_json_output,
)
from api.commands.runtime import (
    log_dataset_debug_event as _log_dataset_debug_event,
)
from api.commands.runtime import (
    log_dataset_event as _log_dataset_event,
)
from api.commands.runtime import (
    log_job_event as _log_job_event,
)
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
from domain.models import RawSnapshot
from ingestion.config import (
    Config,
)
from ingestion.futures_summary import (
    FUTURES_SUMMARY_DATASET_TYPE,
    FuturesSummarySnapshotRow,
    normalize_futures_summary_rows,
)
from ingestion.futures_summary import (
    snapshot_time_floor_minute as futures_summary_snapshot_time_floor_minute,
)
from ingestion.futures_summary import (
    utc_run_id as futures_summary_utc_run_id,
)
from ingestion.futures_summary_lake import save_futures_summary_snapshot_parquet_lake
from ingestion.index_price import (
    INDEX_PRICE_DATASET_TYPE,
    IndexPriceSnapshotRow,
    normalize_index_price_snapshot_row,
)
from ingestion.index_price import (
    snapshot_time_floor_minute as index_snapshot_time_floor_minute,
)
from ingestion.index_price import (
    utc_run_id as index_utc_run_id,
)
from ingestion.index_price_lake import save_index_price_snapshot_parquet_lake
from ingestion.instrument_metadata import (
    FUTURE_INSTRUMENT_METADATA_DATASET_TYPE,
    INSTRUMENT_METADATA_DATASET_TYPE,
    InstrumentMetadataSnapshotRow,
    normalize_instrument_metadata_rows,
    snapshot_date_utc,
)
from ingestion.instrument_metadata import (
    utc_run_id as instrument_utc_run_id,
)
from ingestion.instrument_metadata_lake import save_instrument_metadata_snapshot_parquet_lake
from ingestion.l2 import L2Snapshot, fetch_perps_l2_snapshot_1m_for_symbols
from ingestion.lake import save_perps_l2_snapshot_1m_parquet_lake
from ingestion.option_instrument_ticker import (
    OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
    OptionInstrumentTickerSnapshotRow,
    normalize_option_instrument_ticker_rows,
)
from ingestion.option_instrument_ticker import (
    snapshot_time_floor_minute as option_instrument_ticker_snapshot_time_floor_minute,
)
from ingestion.option_instrument_ticker import (
    utc_run_id as option_instrument_ticker_utc_run_id,
)
from ingestion.option_instrument_ticker_lake import save_option_instrument_ticker_snapshot_parquet_lake
from ingestion.option_l2 import (
    OPTION_L2_DATASET_TYPE,
    OptionL2SnapshotRow,
    normalize_option_l2_snapshot_rows,
)
from ingestion.option_l2 import (
    snapshot_time_floor_minute as option_l2_snapshot_time_floor_minute,
)
from ingestion.option_l2 import (
    utc_run_id as option_l2_utc_run_id,
)
from ingestion.option_l2_lake import save_option_l2_snapshot_parquet_lake
from ingestion.option_ticker_universe import select_option_ticker_prediction_universe
from ingestion.options import (
    OPTION_TICKER_DATASET_TYPE,
    OptionTickerSnapshotRow,
    normalize_options_ticker_rows,
    snapshot_time_floor_minute,
    utc_run_id,
)
from ingestion.options_lake import save_options_ticker_snapshot_parquet_lake
from ingestion.recent_trades import (
    RECENT_TRADE_DATASET_TYPE,
    RecentTradeSnapshotRow,
    normalize_recent_trade_rows,
    overlap_start_timestamp_ms,
)
from ingestion.recent_trades import (
    snapshot_time_floor_minute as recent_trade_snapshot_time_floor_minute,
)
from ingestion.recent_trades import (
    utc_run_id as recent_trade_utc_run_id,
)
from ingestion.recent_trades_lake import save_recent_trade_snapshot_parquet_lake
from ingestion.volatility_index import (
    VOLATILITY_INDEX_DATASET_TYPE,
    VolatilityIndexSnapshotRow,
    normalize_volatility_index_candles,
)
from ingestion.volatility_index import (
    overlap_start_timestamp_ms as volatility_index_overlap_start_timestamp_ms,
)
from ingestion.volatility_index import (
    snapshot_time_floor_minute as volatility_index_snapshot_time_floor_minute,
)
from ingestion.volatility_index import (
    snapshot_timestamp_ms as volatility_index_snapshot_timestamp_ms,
)
from ingestion.volatility_index import (
    utc_run_id as volatility_index_utc_run_id,
)
from ingestion.volatility_index_lake import save_volatility_index_snapshot_parquet_lake
from sources.deribit_futures import fetch_futures_book_summary_rows
from sources.deribit_index_price import fetch_index_price
from sources.deribit_instruments import fetch_instruments
from sources.deribit_option_order_book import fetch_option_order_book
from sources.deribit_option_ticker import fetch_option_ticker
from sources.deribit_options import (
    fetch_option_book_summary_rows,
    resolve_options_currency_request,
)
from sources.deribit_trades import (
    TradesCurrencyRequest,
    fetch_last_trades_by_currency,
    resolve_trades_currency_request,
)
from sources.deribit_volatility_index import fetch_volatility_index_candles
from sources.registry import source_adapter_for_exchange

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


def _enable_debug_logging(args: argparse.Namespace, logger: logging.Logger) -> None:
    if not bool(getattr(args, "debug", False)):
        return
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)


def _serialize_perps_l2_snapshot_1m(item: L2Snapshot) -> dict[str, object]:
    """Convert an L2 snapshot into a JSON-safe output dictionary."""

    data = asdict(item)
    timestamp = data["timestamp"]
    if not hasattr(timestamp, "isoformat"):
        raise ValueError("timestamp must be datetime-like")
    data["timestamp"] = timestamp.isoformat()
    return data


def _normalize_cli_symbols(values: list[str]) -> list[str]:
    """Normalize CLI symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


def _normalize_cli_currencies(values: list[str]) -> list[str]:
    """Normalize CLI currency values from space- or comma-delimited inputs."""

    currencies: list[str] = []
    for value in values:
        currencies.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not currencies:
        raise ValueError("at least one currency is required")
    return currencies


def _normalize_cli_index_symbols(values: list[str]) -> list[str]:
    """Normalize CLI index-symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().lower() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one index symbol is required")
    return symbols


def _normalize_cli_trade_kinds(values: list[str]) -> list[str]:
    """Normalize CLI trade kind values from space- or comma-delimited inputs."""

    kinds: list[str] = []
    for value in values:
        kinds.extend(item.strip().lower() for item in value.replace(",", " ").split() if item.strip())
    if not kinds:
        raise ValueError("at least one trade kind is required")
    return sorted(dict.fromkeys(kinds))


def _normalize_cli_instruments(values: list[str]) -> list[str]:
    """Normalize CLI instrument values from space- or comma-delimited inputs."""

    instruments: list[str] = []
    for value in values:
        instruments.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    return sorted(dict.fromkeys(instruments))


def _fetch_options_rows_for_currencies(
    currencies: list[str],
) -> tuple[dict[str, list[dict[str, object]]], dict[str, str], dict[str, str]]:
    """Fetch option summary rows sequentially for all requested currencies."""

    rows_by_currency: dict[str, list[dict[str, object]]] = {}
    source_currency_by_requested: dict[str, str] = {}
    errors: dict[str, str] = {}

    for currency in currencies:
        request = resolve_options_currency_request(currency)
        source_currency_by_requested[request.requested_currency] = request.source_currency
        try:
            rows_by_currency[request.requested_currency] = fetch_option_book_summary_rows(request)
        except Exception as exc:  # noqa: BLE001
            errors[request.requested_currency] = str(exc)
    return rows_by_currency, source_currency_by_requested, errors


def _fetch_option_ticker_rows_for_instruments(
    instruments: list[str],
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """Fetch per-instrument option ticker rows sequentially."""

    rows_by_instrument: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}

    for instrument_name in instruments:
        try:
            rows_by_instrument[instrument_name] = fetch_option_ticker(instrument_name)
        except Exception as exc:  # noqa: BLE001
            errors[instrument_name] = str(exc)
    return rows_by_instrument, errors


def _fetch_option_l2_rows_for_instruments(
    instruments: list[str],
    *,
    depth: int,
) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
    """Fetch per-instrument option order-book rows sequentially."""

    rows_by_instrument: dict[str, dict[str, object]] = {}
    fetch_durations_s: dict[str, float] = {}
    errors: dict[str, str] = {}

    for instrument_name in instruments:
        started_at = perf_counter()
        try:
            rows_by_instrument[instrument_name] = fetch_option_order_book(
                instrument_name=instrument_name,
                depth=depth,
            )
            fetch_durations_s[instrument_name] = perf_counter() - started_at
        except Exception as exc:  # noqa: BLE001
            fetch_durations_s[instrument_name] = perf_counter() - started_at
            errors[instrument_name] = str(exc)
    return rows_by_instrument, fetch_durations_s, errors


def _fetch_recent_trade_rows_for_requests(
    requests: list[TradesCurrencyRequest],
    *,
    count: int,
    start_timestamp: int | None,
    sorting: str,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, str]]:
    """Fetch recent trade rows sequentially for all requested currency/kind pairs."""

    rows_by_scope: dict[str, list[dict[str, object]]] = {}
    errors: dict[str, str] = {}
    for request in requests:
        scope_key = _recent_trade_scope_key(request.requested_currency, request.kind)
        try:
            rows_by_scope[scope_key] = fetch_last_trades_by_currency(
                request,
                count=count,
                start_timestamp=start_timestamp,
                sorting=sorting,
            )
        except Exception as exc:  # noqa: BLE001
            errors[scope_key] = str(exc)
    return rows_by_scope, errors


def _recent_trade_scope_key(currency: str, kind: str) -> str:
    return f"{currency.strip().upper()}:{kind.strip().lower()}"


def _log_bronze_builder_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level bronze-builder summary."""

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    _log_dataset_event(
        logger,
        logging.INFO,
        BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type="perps_l2_snapshot_1m",
        elapsed_s=elapsed_s,
        errors=1 if parquet_error is not None else 0,
        exchange=exchange,
        parquet_error=parquet_error,
        parquet_files=len(parquet_files),
        snapshots_collected=collected_total,
        snapshots_requested=requested_total,
        status=status,
        symbols=[symbol.upper() for symbol in symbols],
    )


def _build_snapshot_output(
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    logger: logging.Logger,
) -> dict[str, object]:
    """Build JSON output for raw bronze snapshots and log per-symbol collection status."""

    output: dict[str, object] = {exchange: {}}
    exchange_output = cast(dict[str, object], output[exchange])

    for symbol in symbols:
        symbol_key = symbol.upper()
        snapshots = snapshots_by_symbol.get(symbol_key, [])
        _log_partial_snapshot_warning(
            logger=logger,
            symbol=symbol_key,
            collected_snapshots=len(snapshots),
            requested_snapshots=requested_snapshots,
        )
        exchange_output[symbol_key] = [_serialize_perps_l2_snapshot_1m(item) for item in snapshots]
        _log_dataset_event(
            logger,
            logging.INFO,
            BRONZE_BUILDER_COMMAND,
            "snapshot_stats",
            dataset_type="perps_l2_snapshot_1m",
            exchange=exchange,
            snapshots_collected=len(snapshots),
            snapshots_requested=requested_snapshots,
            symbol=symbol_key,
        )

    return output


def _log_partial_snapshot_warning(
    logger: logging.Logger,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Log a warning when the run collected fewer snapshots than requested."""

    if collected_snapshots >= requested_snapshots:
        return
    logger.warning(
        "bronze-builder collected partial snapshots symbol=%s collected=%s requested=%s",
        symbol,
        collected_snapshots,
        requested_snapshots,
    )


def _persist_bronze_snapshots(
    snapshots_by_symbol: SnapshotsBySymbol,
    lake_root: str,
    depth: int,
    enabled: bool,
    output: dict[str, object],
    logger: logging.Logger,
) -> tuple[list[str], str | None]:
    """Persist raw L2 snapshots when requested and annotate the CLI output."""

    if not enabled:
        return [], None

    try:
        parquet_files = save_perps_l2_snapshot_1m_parquet_lake(
            snapshots_by_symbol=snapshots_by_symbol,
            lake_root=lake_root,
            depth=depth,
        )
        output["_parquet_files"] = parquet_files
        return parquet_files, None
    except Exception as exc:  # noqa: BLE001
        parquet_error = str(exc)
        output["_parquet_error"] = parquet_error
        logger.exception("bronze-builder raw snapshot parquet write failed")
        return [], parquet_error


def _estimated_poll_runtime_s(snapshot_count: int, poll_interval_s: float) -> float:
    """Estimate runtime spent sleeping between polling ticks."""

    return max(0, snapshot_count - 1) * poll_interval_s


def _warn_for_long_poll_schedule(
    logger: logging.Logger,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float,
) -> None:
    """Warn when bronze-builder polling settings are likely to collide with minute cron runs."""

    estimated_s = _estimated_poll_runtime_s(snapshot_count=snapshot_count, poll_interval_s=poll_interval_s)
    if max_runtime_s > 0 and estimated_s >= max_runtime_s:
        logger.warning(
            "bronze-builder polling sleep budget may exceed max runtime estimated_sleep_s=%.3f max_runtime_s=%.3f",
            estimated_s,
            max_runtime_s,
        )
    if estimated_s >= 60:
        logger.warning(
            "bronze-builder polling sleep budget is at least one minute estimated_sleep_s=%.3f; cron runs may overlap",
            estimated_s,
        )


def _run_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Run L2 snapshot collection and optional raw bronze persistence."""

    started_at = perf_counter()
    exchange = cast(str, args.exchange)
    symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
    requested_snapshots = int(args.snapshot_count)
    max_runtime_s = float(args.max_runtime_s)
    _log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type="perps_l2_snapshot_1m",
        exchange=exchange,
        depth=int(args.levels),
        lake_root=cast(str, args.lake_root),
        max_runtime_s=max_runtime_s,
        poll_interval_s=float(args.poll_interval_s),
        save_parquet_lake=bool(args.save_parquet_lake),
        snapshot_count=requested_snapshots,
        symbols=symbols,
    )
    _warn_for_long_poll_schedule(
        logger=logger,
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s,
    )
    snapshots_by_symbol = fetch_perps_l2_snapshot_1m_for_symbols(
        exchange=exchange,
        symbols=symbols,
        depth=int(args.levels),
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s if max_runtime_s > 0 else None,
    )
    _log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "collection_complete",
        dataset_type="perps_l2_snapshot_1m",
        exchange=exchange,
        snapshots_collected=sum(len(snapshots) for snapshots in snapshots_by_symbol.values()),
        snapshots_by_symbol={symbol: len(snapshots_by_symbol.get(symbol, [])) for symbol in symbols},
        snapshots_requested=requested_snapshots * len(symbols),
    )

    output = _build_snapshot_output(
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        logger=logger,
    )
    parquet_files, parquet_error = _persist_bronze_snapshots(
        snapshots_by_symbol=snapshots_by_symbol,
        lake_root=cast(str, args.lake_root),
        depth=int(args.levels),
        enabled=bool(args.save_parquet_lake),
        output=output,
        logger=logger,
    )
    _log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type="perps_l2_snapshot_1m",
        exchange=exchange,
        files=len(parquet_files),
        output_files=parquet_files,
        parquet_error=parquet_error,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    _emit_json_output(bool(args.json_output), output)
    _log_bronze_builder_summary(
        logger=logger,
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        parquet_files=parquet_files,
        elapsed_s=perf_counter() - started_at,
        parquet_error=parquet_error,
    )


def _run_options_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Run Deribit options chain snapshot collection and optional bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    run_id = utc_run_id()
    snapshot_time = snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    _log_dataset_debug_event(
        logger,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=OPTION_TICKER_DATASET_TYPE,
        currencies=currencies,
        lake_root=cast(str, args.lake_root),
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
    )

    raw_rows_by_currency, source_currency_by_requested, fetch_errors = _fetch_options_rows_for_currencies(
        currencies=currencies
    )
    _log_dataset_debug_event(
        logger,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "fetch_complete",
        dataset_type=OPTION_TICKER_DATASET_TYPE,
        currencies=currencies,
        errors=len(fetch_errors),
        raw_rows_by_currency={currency: len(raw_rows_by_currency.get(currency, [])) for currency in currencies},
        source_currency_by_requested=source_currency_by_requested,
    )

    rows_by_currency: OptionRowsByCurrency = {}
    normalization_errors: list[str] = []
    for currency in currencies:
        source_currency = source_currency_by_requested.get(currency, "")
        raw_rows = raw_rows_by_currency.get(currency, [])
        rows, row_errors = normalize_options_ticker_rows(
            raw_rows,
            requested_currency=currency,
            source_currency=source_currency,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=cast(str, args.source),
            schema_version=cast(str, args.schema_version),
        )
        rows_by_currency[currency] = rows
        normalization_errors.extend(row_errors)
    _log_dataset_debug_event(
        logger,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "normalization_complete",
        dataset_type=OPTION_TICKER_DATASET_TYPE,
        normalized_rows=sum(len(rows) for rows in rows_by_currency.values()),
        normalization_errors=len(normalization_errors),
        rows_by_currency={currency: len(rows_by_currency.get(currency, [])) for currency in currencies},
    )

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_options_ticker_snapshot_parquet_lake(
            rows_by_currency=rows_by_currency,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=OPTION_TICKER_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    currency_results: dict[str, dict[str, object]] = {}
    for currency in currencies:
        if currency in fetch_errors:
            currency_results[currency] = {
                "rows": 0,
                "status": "error",
                "error": fetch_errors[currency],
            }
            continue
        currency_results[currency] = {
            "rows": len(rows_by_currency.get(currency, [])),
            "status": "ok",
            "source_currency": source_currency_by_requested.get(currency, ""),
        }

    errors = list(fetch_errors.values()) + normalization_errors
    output = {
        "command": OPTIONS_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": OPTION_TICKER_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "rows_written": sum(len(rows) for rows in rows_by_currency.values()),
        "currency_results": currency_results,
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_dataset_event(
        logger,
        logging.INFO,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=OPTION_TICKER_DATASET_TYPE,
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        rows_written=output["rows_written"],
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _run_futures_summary_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit futures summary snapshot collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    run_id = futures_summary_utc_run_id()
    snapshot_time = futures_summary_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    _log_dataset_debug_event(
        logger,
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=FUTURES_SUMMARY_DATASET_TYPE,
        currencies=currencies,
        lake_root=cast(str, args.lake_root),
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
    )

    rows_by_currency: FuturesSummaryRowsByCurrency = {}
    errors: list[str] = []
    source_currency_by_requested: dict[str, str] = {}
    raw_rows_by_currency: dict[str, int] = {}
    for currency in currencies:
        try:
            raw_rows, source_currency = fetch_futures_book_summary_rows(currency)
            raw_rows_by_currency[currency] = len(raw_rows)
            source_currency_by_requested[currency] = source_currency
            rows, row_errors = normalize_futures_summary_rows(
                raw_rows,
                requested_currency=currency,
                source_currency=source_currency,
                run_id=run_id,
                snapshot_time=snapshot_time,
                ingested_at=ingested_at,
                source=cast(str, args.source),
                schema_version=cast(str, args.schema_version),
            )
            rows_by_currency[currency] = rows
            errors.extend(row_errors)
        except Exception as exc:  # noqa: BLE001
            rows_by_currency[currency] = []
            raw_rows_by_currency[currency] = 0
            errors.append(f"{currency}: {exc}")
    _log_dataset_debug_event(
        logger,
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        "collection_complete",
        dataset_type=FUTURES_SUMMARY_DATASET_TYPE,
        errors=len(errors),
        normalized_rows=sum(len(rows) for rows in rows_by_currency.values()),
        raw_rows_by_currency=raw_rows_by_currency,
        rows_by_currency={currency: len(rows_by_currency.get(currency, [])) for currency in currencies},
        source_currency_by_requested=source_currency_by_requested,
    )

    all_rows = [row for rows in rows_by_currency.values() for row in rows]
    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_futures_summary_snapshot_parquet_lake(
            rows=all_rows,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=FUTURES_SUMMARY_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    currency_results = {
        currency: {
            "rows": len(rows_by_currency.get(currency, [])),
            "source_currency": source_currency_by_requested.get(currency, ""),
        }
        for currency in currencies
    }
    output = {
        "command": FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": FUTURES_SUMMARY_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "currency_results": currency_results,
        "rows_written": len(all_rows),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_dataset_event(
        logger,
        logging.INFO,
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=FUTURES_SUMMARY_DATASET_TYPE,
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        rows_written=len(all_rows),
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _select_option_ticker_prediction_universe_by_currency(
    currencies: list[str],
    explicit_instruments: list[str],
    max_instruments_per_currency: int,
) -> tuple[dict[str, list[str]], list[str]]:
    """Return explicit or summary-selected option instruments grouped by requested currency."""

    normalized_explicit = _normalize_cli_instruments(explicit_instruments)
    if normalized_explicit:
        return {"explicit": normalized_explicit}, []

    instruments_by_currency: dict[str, list[str]] = {}
    errors: list[str] = []
    for currency in currencies:
        try:
            request = resolve_options_currency_request(currency)
            rows = fetch_option_book_summary_rows(request)
            instruments_by_currency[currency] = select_option_ticker_prediction_universe(
                rows,
                max_instruments=max_instruments_per_currency,
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{currency}: {exc}")
            instruments_by_currency[currency] = []
    return instruments_by_currency, errors


def _limit_option_ticker_instruments_by_currency(
    instruments_by_currency: dict[str, list[str]],
    *,
    explicit_instruments: list[str],
    max_instruments_per_run: int,
) -> list[str]:
    """Limit selected prediction-universe instruments per requested option currency."""

    if explicit_instruments:
        return instruments_by_currency.get("explicit", [])

    selected: list[str] = []
    requested_count = max(1, max_instruments_per_run)
    for currency_instruments in instruments_by_currency.values():
        selected.extend(currency_instruments[:requested_count])
    return selected


def _run_option_l2_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Run selected Deribit option order-book collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    explicit_instruments = _normalize_cli_instruments(cast(list[str], args.instruments))
    depth = int(args.depth)
    if depth <= 0:
        raise ValueError("depth must be positive")
    max_instruments_per_currency = max(1, int(args.max_instruments_per_run))
    _log_dataset_debug_event(
        logger,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=OPTION_L2_DATASET_TYPE,
        currencies=currencies,
        depth=depth,
        explicit_instruments=len(explicit_instruments),
        lake_root=cast(str, args.lake_root),
        max_instruments_per_currency=max_instruments_per_currency,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
    )
    instruments_by_currency, discovery_errors = _select_option_ticker_prediction_universe_by_currency(
        currencies=currencies,
        explicit_instruments=explicit_instruments,
        max_instruments_per_currency=max_instruments_per_currency,
    )
    run_id = option_l2_utc_run_id()
    snapshot_time = option_l2_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    instruments = _limit_option_ticker_instruments_by_currency(
        instruments_by_currency,
        explicit_instruments=explicit_instruments,
        max_instruments_per_run=max_instruments_per_currency,
    )
    _log_dataset_debug_event(
        logger,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "universe_selected",
        dataset_type=OPTION_L2_DATASET_TYPE,
        discovery_errors=len(discovery_errors),
        instruments_by_currency={
            currency: len(currency_instruments)
            for currency, currency_instruments in sorted(instruments_by_currency.items())
        },
        instruments_requested=len(instruments),
        run_id=run_id,
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
    )

    raw_rows_by_instrument, fetch_durations_s, fetch_errors = _fetch_option_l2_rows_for_instruments(
        instruments=instruments,
        depth=depth,
    )
    _log_dataset_debug_event(
        logger,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "fetch_complete",
        dataset_type=OPTION_L2_DATASET_TYPE,
        depth=depth,
        fetch_errors=len(fetch_errors),
        instruments_requested=len(instruments),
        raw_rows=len(raw_rows_by_instrument),
        slowest_fetch_s=max(fetch_durations_s.values(), default=0.0),
    )
    rows, normalization_errors = normalize_option_l2_snapshot_rows(
        raw_rows_by_instrument,
        run_id=run_id,
        snapshot_time=snapshot_time,
        ingested_at=ingested_at,
        depth=depth,
        fetch_durations_s=fetch_durations_s,
        source=cast(str, args.source),
        schema_version=cast(str, args.schema_version),
    )
    _log_dataset_debug_event(
        logger,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "normalization_complete",
        dataset_type=OPTION_L2_DATASET_TYPE,
        ask_levels=sum(row.ask_levels for row in rows),
        bid_levels=sum(row.bid_levels for row in rows),
        normalization_errors=len(normalization_errors),
        rows_written=len(rows),
    )

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_option_l2_snapshot_parquet_lake(
            rows=rows,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=OPTION_L2_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    errors = discovery_errors + list(fetch_errors.values()) + normalization_errors
    instruments_discovered = sum(len(currency_instruments) for currency_instruments in instruments_by_currency.values())
    currency_results = {
        currency: {
            "instruments_discovered": len(currency_instruments),
            "instruments_requested": len(
                [instrument for instrument in instruments if instrument in currency_instruments]
            ),
        }
        for currency, currency_instruments in instruments_by_currency.items()
    }
    output = {
        "command": OPTION_L2_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": OPTION_L2_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "depth": depth,
        "instruments_discovered": instruments_discovered,
        "instruments_requested": len(instruments),
        "currency_results": currency_results,
        "rows_written": len(rows),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_dataset_event(
        logger,
        logging.INFO,
        OPTION_L2_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=OPTION_L2_DATASET_TYPE,
        currencies=currencies,
        depth=depth,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        instruments_discovered=instruments_discovered,
        instruments_requested=len(instruments),
        rows_written=len(rows),
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _run_option_instrument_ticker_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Run Deribit per-instrument option ticker collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    explicit_instruments = _normalize_cli_instruments(cast(list[str], args.instruments))
    max_instruments_per_currency = max(1, int(args.max_instruments_per_run))
    _log_dataset_debug_event(
        logger,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        currencies=currencies,
        explicit_instruments=len(explicit_instruments),
        lake_root=cast(str, args.lake_root),
        max_instruments_per_currency=max_instruments_per_currency,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
    )
    instruments_by_currency, discovery_errors = _select_option_ticker_prediction_universe_by_currency(
        currencies=currencies,
        explicit_instruments=explicit_instruments,
        max_instruments_per_currency=max_instruments_per_currency,
    )
    run_id = option_instrument_ticker_utc_run_id()
    snapshot_time = option_instrument_ticker_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    instruments = _limit_option_ticker_instruments_by_currency(
        instruments_by_currency,
        explicit_instruments=explicit_instruments,
        max_instruments_per_run=max_instruments_per_currency,
    )
    _log_dataset_debug_event(
        logger,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "universe_selected",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        discovery_errors=len(discovery_errors),
        instruments_by_currency={
            currency: len(currency_instruments)
            for currency, currency_instruments in sorted(instruments_by_currency.items())
        },
        instruments_requested=len(instruments),
        run_id=run_id,
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
    )

    raw_rows_by_instrument, fetch_errors = _fetch_option_ticker_rows_for_instruments(instruments=instruments)
    _log_dataset_debug_event(
        logger,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "fetch_complete",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        fetch_errors=len(fetch_errors),
        instruments_requested=len(instruments),
        raw_rows=len(raw_rows_by_instrument),
    )
    rows, normalization_errors = normalize_option_instrument_ticker_rows(
        raw_rows_by_instrument,
        run_id=run_id,
        snapshot_time=snapshot_time,
        ingested_at=ingested_at,
        source=cast(str, args.source),
        schema_version=cast(str, args.schema_version),
    )
    _log_dataset_debug_event(
        logger,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "normalization_complete",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        normalization_errors=len(normalization_errors),
        rows_written=len(rows),
    )

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_option_instrument_ticker_snapshot_parquet_lake(
            rows=rows,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    errors = discovery_errors + list(fetch_errors.values()) + normalization_errors
    instruments_discovered = sum(len(currency_instruments) for currency_instruments in instruments_by_currency.values())
    currency_results = {
        currency: {
            "instruments_discovered": len(currency_instruments),
            "instruments_requested": len(
                [instrument for instrument in instruments if instrument in currency_instruments]
            ),
        }
        for currency, currency_instruments in instruments_by_currency.items()
    }
    output = {
        "command": OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "instruments_discovered": instruments_discovered,
        "instruments_requested": len(instruments),
        "currency_results": currency_results,
        "rows_written": len(rows),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_dataset_event(
        logger,
        logging.INFO,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        instruments_discovered=instruments_discovered,
        instruments_requested=len(instruments),
        rows_written=len(rows),
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _run_instrument_metadata_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit instrument metadata snapshot collection and optional persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    kind = cast(str, args.kind).strip().lower()
    include_inactive = bool(args.include_inactive)
    run_id = instrument_utc_run_id()
    ingested_at = datetime.now(UTC)
    snapshot_date = snapshot_date_utc()
    dataset_type = FUTURE_INSTRUMENT_METADATA_DATASET_TYPE if kind == "future" else INSTRUMENT_METADATA_DATASET_TYPE
    _log_dataset_debug_event(
        logger,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=dataset_type,
        currencies=currencies,
        include_inactive=include_inactive,
        kind=kind,
        lake_root=cast(str, args.lake_root),
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
        snapshot_date=snapshot_date.isoformat(),
    )

    raw_rows: list[dict[str, object]] = []
    fetch_errors: list[str] = []
    raw_rows_by_currency: dict[str, int] = {}
    for currency in currencies:
        try:
            currency_rows = fetch_instruments(currency=currency, kind=kind, expired=include_inactive)
            raw_rows_by_currency[currency] = len(currency_rows)
            raw_rows.extend(currency_rows)
        except Exception as exc:  # noqa: BLE001
            raw_rows_by_currency[currency] = 0
            fetch_errors.append(f"{currency}: {exc}")
    _log_dataset_debug_event(
        logger,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "fetch_complete",
        dataset_type=dataset_type,
        errors=len(fetch_errors),
        raw_rows=len(raw_rows),
        raw_rows_by_currency=raw_rows_by_currency,
    )

    rows, normalize_errors = normalize_instrument_metadata_rows(
        raw_rows,
        run_id=run_id,
        snapshot_date=snapshot_date,
        ingested_at=ingested_at,
        source=cast(str, args.source),
        schema_version=cast(str, args.schema_version),
    )
    _log_dataset_debug_event(
        logger,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "normalization_complete",
        dataset_type=dataset_type,
        normalization_errors=len(normalize_errors),
        rows_written=len(rows),
    )
    rows_by_date: InstrumentMetadataRowsByDate = {snapshot_date.isoformat(): rows}
    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_instrument_metadata_snapshot_parquet_lake(
            rows_by_date=rows_by_date,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=dataset_type,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    output = {
        "command": INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "exchange": "deribit",
        "dataset_type": dataset_type,
        "run_id": run_id,
        "snapshot_date": snapshot_date.isoformat(),
        "requested_currencies": currencies,
        "kind": kind,
        "include_inactive": include_inactive,
        "rows_written": len(rows),
        "output_files": output_files,
        "errors": fetch_errors + normalize_errors,
    }
    _emit_json_output(bool(args.json_output), output)
    error_count = len(fetch_errors) + len(normalize_errors)
    _log_dataset_event(
        logger,
        logging.INFO,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=dataset_type,
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=error_count,
        files=len(output_files),
        kind=kind,
        rows_written=len(rows),
        run_id=run_id,
        snapshot_date=snapshot_date.isoformat(),
        status="complete" if error_count == 0 else "partial",
    )


def _run_index_price_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit index-price snapshot collection and optional persistence."""

    started_at = perf_counter()
    symbols = _normalize_cli_index_symbols(cast(list[str], args.symbols))
    run_id = index_utc_run_id()
    ingested_at = datetime.now(UTC)
    snapshot_time = index_snapshot_time_floor_minute()
    _log_dataset_debug_event(
        logger,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=INDEX_PRICE_DATASET_TYPE,
        lake_root=cast(str, args.lake_root),
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
        symbols=symbols,
    )

    rows_by_symbol: IndexPriceRowsBySymbol = {}
    errors: list[str] = []
    for symbol in symbols:
        try:
            price = fetch_index_price(symbol)
            row = normalize_index_price_snapshot_row(
                index_name=symbol,
                price=price,
                run_id=run_id,
                snapshot_time=snapshot_time,
                ingested_at=ingested_at,
                source=cast(str, args.source),
                schema_version=cast(str, args.schema_version),
            )
            rows_by_symbol[symbol] = [row]
        except Exception as exc:  # noqa: BLE001
            rows_by_symbol[symbol] = []
            errors.append(f"{symbol}: {exc}")
    _log_dataset_debug_event(
        logger,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "collection_complete",
        dataset_type=INDEX_PRICE_DATASET_TYPE,
        errors=len(errors),
        rows_by_symbol={symbol: len(rows_by_symbol.get(symbol, [])) for symbol in symbols},
        rows_written=sum(len(rows) for rows in rows_by_symbol.values()),
    )

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_index_price_snapshot_parquet_lake(
            rows_by_index=rows_by_symbol,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=INDEX_PRICE_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    output = {
        "command": INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "exchange": "deribit",
        "dataset_type": INDEX_PRICE_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_symbols": symbols,
        "rows_written": sum(len(rows) for rows in rows_by_symbol.values()),
        "output_files": output_files,
        "errors": errors,
    }
    emit_dataset_command_result(
        logger,
        DatasetCommandResult(
            command=INDEX_PRICE_BRONZE_BUILDER_COMMAND,
            dataset_type=INDEX_PRICE_DATASET_TYPE,
            payload=output,
            json_output=bool(args.json_output),
            summary_fields={
                "elapsed_s": perf_counter() - started_at,
                "errors": len(errors),
                "files": len(output_files),
                "rows_written": output["rows_written"],
                "run_id": run_id,
                "snapshot_time": output["snapshot_time"],
                "status": "complete" if not errors else "partial",
                "symbols": symbols,
            },
        ),
    )


def _run_volatility_index_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit volatility-index candle collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    resolution = int(args.resolution)
    if resolution <= 0:
        raise ValueError("resolution must be positive")
    run_id = volatility_index_utc_run_id()
    snapshot_time = volatility_index_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    start_timestamp = volatility_index_overlap_start_timestamp_ms(
        snapshot_time=snapshot_time,
        lookback_seconds=int(args.lookback_seconds),
    )
    end_timestamp = volatility_index_snapshot_timestamp_ms(snapshot_time)
    _log_dataset_debug_event(
        logger,
        VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=VOLATILITY_INDEX_DATASET_TYPE,
        currencies=currencies,
        end_timestamp=end_timestamp,
        lake_root=cast(str, args.lake_root),
        lookback_seconds=int(args.lookback_seconds),
        resolution=resolution,
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        source=cast(str, args.source),
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
        start_timestamp=start_timestamp,
    )

    rows_by_currency: VolatilityIndexRowsByCurrency = {}
    source_currency_by_requested: dict[str, str] = {}
    errors: list[str] = []
    candles_by_currency: dict[str, int] = {}
    for currency in currencies:
        try:
            candles, source_currency = fetch_volatility_index_candles(
                currency,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                resolution=resolution,
            )
            candles_by_currency[currency] = len(candles)
            source_currency_by_requested[currency] = source_currency
            rows, row_errors = normalize_volatility_index_candles(
                candles,
                currency=currency,
                source_currency=source_currency,
                resolution=resolution,
                run_id=run_id,
                snapshot_time=snapshot_time,
                ingested_at=ingested_at,
                source=cast(str, args.source),
                schema_version=cast(str, args.schema_version),
            )
            rows_by_currency[currency] = rows
            errors.extend(row_errors)
        except Exception as exc:  # noqa: BLE001
            rows_by_currency[currency] = []
            candles_by_currency[currency] = 0
            errors.append(f"{currency}: {exc}")
    _log_dataset_debug_event(
        logger,
        VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
        "collection_complete",
        dataset_type=VOLATILITY_INDEX_DATASET_TYPE,
        candles_by_currency=candles_by_currency,
        errors=len(errors),
        rows_by_currency={currency: len(rows_by_currency.get(currency, [])) for currency in currencies},
        source_currency_by_requested=source_currency_by_requested,
    )

    all_rows = [row for rows in rows_by_currency.values() for row in rows]
    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_volatility_index_snapshot_parquet_lake(
            rows=all_rows,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=VOLATILITY_INDEX_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    currency_results = {
        currency: {
            "rows": len(rows_by_currency.get(currency, [])),
            "source_currency": source_currency_by_requested.get(currency, ""),
        }
        for currency in currencies
    }
    output = {
        "command": VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
        "exchange": "deribit",
        "dataset_type": VOLATILITY_INDEX_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "resolution": resolution,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "currency_results": currency_results,
        "rows_written": len(all_rows),
        "output_files": output_files,
        "errors": errors,
    }
    emit_dataset_command_result(
        logger,
        DatasetCommandResult(
            command=VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
            dataset_type=VOLATILITY_INDEX_DATASET_TYPE,
            payload=output,
            json_output=bool(args.json_output),
            summary_fields={
                "currencies": currencies,
                "elapsed_s": perf_counter() - started_at,
                "errors": len(errors),
                "files": len(output_files),
                "resolution": resolution,
                "rows_written": len(all_rows),
                "run_id": run_id,
                "snapshot_time": output["snapshot_time"],
                "status": "complete" if not errors else "partial",
            },
        ),
    )


def _run_recent_trades_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit recent trade tape collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    kinds = _normalize_cli_trade_kinds(cast(list[str], args.kinds))
    count = int(args.count)
    if count <= 0:
        raise ValueError("count must be positive")
    run_id = recent_trade_utc_run_id()
    snapshot_time = recent_trade_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    start_timestamp = cast(int | None, args.start_timestamp_ms)
    if start_timestamp is None:
        start_timestamp = overlap_start_timestamp_ms(
            snapshot_time=snapshot_time,
            lookback_seconds=int(args.lookback_seconds),
        )
    _log_dataset_debug_event(
        logger,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        count=count,
        currencies=currencies,
        kinds=kinds,
        lake_root=cast(str, args.lake_root),
        lookback_seconds=int(args.lookback_seconds),
        run_id=run_id,
        save_parquet_lake=bool(args.save_parquet_lake),
        schema_version=cast(str, args.schema_version),
        sorting=cast(str, args.sorting),
        source=cast(str, args.source),
        snapshot_time=snapshot_time.isoformat().replace("+00:00", "Z"),
        start_timestamp=start_timestamp,
    )

    requests = [resolve_trades_currency_request(currency, kind) for currency in currencies for kind in kinds]
    _log_dataset_debug_event(
        logger,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "request_scopes_resolved",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        request_scopes=[
            f"{request.requested_currency}:{request.source_currency}:{request.kind}" for request in requests
        ],
        requests=len(requests),
    )
    raw_rows_by_scope, fetch_errors = _fetch_recent_trade_rows_for_requests(
        requests,
        count=count,
        start_timestamp=start_timestamp,
        sorting=cast(str, args.sorting),
    )
    _log_dataset_debug_event(
        logger,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "fetch_complete",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        errors=len(fetch_errors),
        raw_rows_by_scope={scope: len(rows) for scope, rows in sorted(raw_rows_by_scope.items())},
    )

    rows_by_scope: RecentTradeRowsByScope = {}
    normalization_errors: list[str] = []
    for request in requests:
        scope_key = _recent_trade_scope_key(request.requested_currency, request.kind)
        raw_rows = raw_rows_by_scope.get(scope_key, [])
        rows, row_errors = normalize_recent_trade_rows(
            raw_rows,
            requested_currency=request.requested_currency,
            source_currency=request.source_currency,
            kind=request.kind,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=cast(str, args.source),
            schema_version=cast(str, args.schema_version),
        )
        rows_by_scope[scope_key] = rows
        normalization_errors.extend(row_errors)
    _log_dataset_debug_event(
        logger,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "normalization_complete",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        normalization_errors=len(normalization_errors),
        rows_by_scope={scope: len(rows) for scope, rows in sorted(rows_by_scope.items())},
        rows_written=sum(len(rows) for rows in rows_by_scope.values()),
    )

    all_rows = [row for rows in rows_by_scope.values() for row in rows]
    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_recent_trade_snapshot_parquet_lake(
            rows=all_rows,
            lake_root=cast(str, args.lake_root),
        )
    _log_dataset_debug_event(
        logger,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        files=len(output_files),
        output_files=output_files,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    scope_results: dict[str, dict[str, object]] = {}
    for request in requests:
        scope_key = _recent_trade_scope_key(request.requested_currency, request.kind)
        if scope_key in fetch_errors:
            scope_results[scope_key] = {
                "status": "error",
                "rows": 0,
                "error": fetch_errors[scope_key],
            }
            continue
        scope_results[scope_key] = {
            "status": "ok",
            "rows": len(rows_by_scope.get(scope_key, [])),
            "source_currency": request.source_currency,
        }

    errors = list(fetch_errors.values()) + normalization_errors
    output = {
        "command": RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": RECENT_TRADE_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "kinds": kinds,
        "count": count,
        "start_timestamp": start_timestamp,
        "sorting": cast(str, args.sorting),
        "scope_results": scope_results,
        "rows_written": len(all_rows),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_dataset_event(
        logger,
        logging.INFO,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        kinds=kinds,
        rows_written=len(all_rows),
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _valid_top_of_book(snapshot: RawSnapshot) -> bool:
    """Return whether snapshot top-of-book values are present and ordered."""

    if not snapshot.bids or not snapshot.asks:
        return False
    top_bid = snapshot.bids[0]
    top_ask = snapshot.asks[0]
    return top_bid.price > 0 and top_ask.price > 0 and top_bid.price < top_ask.price


def _validate_symbol(exchange: str, symbol: str, depth: int) -> dict[str, object]:
    """Validate one symbol by fetching a shallow order book from one adapter."""

    adapter = source_adapter_for_exchange(exchange)
    normalized_symbol = adapter.normalize_symbol(symbol)
    try:
        snapshot = adapter.fetch_snapshot(symbol=symbol, depth=depth)
        valid_book = _valid_top_of_book(snapshot)
        return {
            "symbol": symbol,
            "normalized_symbol": snapshot.symbol,
            "valid_book": valid_book,
            "bid_levels": len(snapshot.bids),
            "ask_levels": len(snapshot.asks),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "symbol": symbol,
            "normalized_symbol": normalized_symbol,
            "valid_book": False,
            "bid_levels": 0,
            "ask_levels": 0,
            "error": str(exc),
        }


def _run_validate_symbols(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run symbol alias and order book validation."""

    symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
    depth = int(args.levels)
    if depth <= 0:
        raise ValueError("levels must be positive")

    exchange = cast(str, args.exchange)
    results = [_validate_symbol(exchange=exchange, symbol=symbol, depth=depth) for symbol in symbols]
    output = {
        "exchange": exchange,
        "symbols": results,
        "all_valid": all(bool(item["valid_book"]) for item in results),
    }
    _emit_json_output(bool(args.json_output), output)
    _log_job_event(
        logger,
        logging.INFO,
        VALIDATE_SYMBOLS_COMMAND,
        "run_summary",
        all_valid=output["all_valid"],
        exchange=args.exchange,
        status="complete",
        symbols=symbols,
    )


def _handle_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _run_bronze_builder(args=args, logger=logger, config=config)


def _handle_options_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _run_options_bronze_builder(args=args, logger=logger, config=config)


def _handle_futures_summary_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_futures_summary_bronze_builder(args=args, logger=logger)


def _handle_option_l2_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
    config: Config,
) -> None:
    _ = config
    _run_option_l2_bronze_builder(args=args, logger=logger)


def _handle_option_instrument_ticker_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
    config: Config,
) -> None:
    _ = config
    _run_option_instrument_ticker_bronze_builder(args=args, logger=logger)


def _handle_instrument_metadata_bronze_builder(
    args: argparse.Namespace, logger: logging.Logger, config: Config
) -> None:
    _ = config
    _run_instrument_metadata_bronze_builder(args=args, logger=logger)


def _handle_index_price_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_index_price_bronze_builder(args=args, logger=logger)


def _handle_volatility_index_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_volatility_index_bronze_builder(args=args, logger=logger)


def _handle_recent_trades_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_recent_trades_bronze_builder(args=args, logger=logger)


def _handle_validate_symbols(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_validate_symbols(args=args, logger=logger)


def command_handlers() -> dict[str, CommandHandler]:
    """Return command handler registry used by CLI dispatch."""

    return {
        BRONZE_BUILDER_COMMAND: _handle_bronze_builder,
        LEGACY_L2_BRONZE_BUILDER_COMMAND: _handle_bronze_builder,
        LEGACY_BRONZE_BUILDER_COMMAND: _handle_bronze_builder,
        OPTIONS_BRONZE_BUILDER_COMMAND: _handle_options_bronze_builder,
        FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND: _handle_futures_summary_bronze_builder,
        OPTION_L2_BRONZE_BUILDER_COMMAND: _handle_option_l2_bronze_builder,
        LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND: _handle_option_l2_bronze_builder,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND: _handle_option_instrument_ticker_bronze_builder,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND: _handle_instrument_metadata_bronze_builder,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND: _handle_index_price_bronze_builder,
        VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND: _handle_volatility_index_bronze_builder,
        RECENT_TRADES_BRONZE_BUILDER_COMMAND: _handle_recent_trades_bronze_builder,
        VALIDATE_SYMBOLS_COMMAND: _handle_validate_symbols,
    }


def dispatch_command(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Dispatch one parsed command to its registered handler."""

    _enable_debug_logging(args=args, logger=logger)
    handlers = command_handlers()
    command = cast(str, args.command)
    _log_job_event(
        logger,
        logging.DEBUG,
        command,
        "dispatch",
        args={key: value for key, value in sorted(vars(args).items()) if key != "command"},
    )
    handler = handlers.get(command)
    if handler is None:
        raise ValueError(f"Unsupported command '{command}'")
    handler(args, logger, config)


def log_module_name_for_args(args: argparse.Namespace) -> str:
    """Resolve the dataset log module name for parsed CLI arguments."""

    command = str(args.command)
    if command == INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND and getattr(args, "kind", "option") == "future":
        return FUTURE_INSTRUMENT_METADATA_DATASET_TYPE
    return command
