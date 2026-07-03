"""Perpetual L2 Bronze command runner implementation."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import asdict
from time import perf_counter
from typing import cast

from api.commands.runtime import emit_json_output, log_dataset_debug_event, log_dataset_event
from api.constants import BRONZE_BUILDER_COMMAND
from ingestion.l2 import PERPS_L2_DATASET_TYPE, L2Snapshot

SnapshotsBySymbol = dict[str, list[L2Snapshot]]
FetchPerpsSnapshots = Callable[..., SnapshotsBySymbol]
SavePerpsSnapshots = Callable[..., list[str]]
NormalizeSymbols = Callable[[list[str]], list[str]]


def serialize_perps_l2_snapshot_1m(item: L2Snapshot) -> dict[str, object]:
    """Convert an L2 snapshot into a JSON-safe output dictionary.

    Args:
        item (L2Snapshot): Normalized perpetual L2 snapshot.

    Returns:
        dict[str, object]: JSON-safe snapshot payload.

    Raises:
        ValueError: If the snapshot timestamp does not expose ``isoformat``.
    """

    data = asdict(item)
    timestamp = data["timestamp"]
    if not hasattr(timestamp, "isoformat"):
        raise ValueError("timestamp must be datetime-like")
    data["timestamp"] = timestamp.isoformat()
    return data


def log_bronze_builder_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level perpetual L2 builder summary.

    Args:
        logger (logging.Logger): Command logger.
        exchange (str): Exchange identifier.
        symbols (list[str]): Requested base symbols.
        snapshots_by_symbol (SnapshotsBySymbol): Collected snapshots keyed by requested symbol.
        requested_snapshots (int): Requested snapshots per symbol.
        parquet_files (list[str]): Written parquet files.
        elapsed_s (float): Runtime in seconds.
        parquet_error (str | None): Optional persistence failure message.

    Returns:
        None: This function only writes structured logs.
    """

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    log_dataset_event(
        logger,
        logging.INFO,
        BRONZE_BUILDER_COMMAND,
        "run_summary",
        dataset_type=PERPS_L2_DATASET_TYPE,
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


def build_snapshot_output(
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    logger: logging.Logger,
) -> dict[str, object]:
    """Build JSON output for raw perpetual L2 snapshots and log per-symbol status.

    Args:
        exchange (str): Exchange identifier.
        symbols (list[str]): Requested base symbols.
        snapshots_by_symbol (SnapshotsBySymbol): Collected snapshots keyed by requested symbol.
        requested_snapshots (int): Requested snapshots per symbol.
        logger (logging.Logger): Command logger.

    Returns:
        dict[str, object]: JSON-safe command output.
    """

    output: dict[str, object] = {exchange: {}}
    exchange_output = cast(dict[str, object], output[exchange])

    for symbol in symbols:
        symbol_key = symbol.upper()
        snapshots = snapshots_by_symbol.get(symbol_key, [])
        log_partial_snapshot_warning(
            logger=logger,
            symbol=symbol_key,
            collected_snapshots=len(snapshots),
            requested_snapshots=requested_snapshots,
        )
        exchange_output[symbol_key] = [serialize_perps_l2_snapshot_1m(item) for item in snapshots]
        log_dataset_event(
            logger,
            logging.INFO,
            BRONZE_BUILDER_COMMAND,
            "snapshot_stats",
            dataset_type=PERPS_L2_DATASET_TYPE,
            exchange=exchange,
            snapshots_collected=len(snapshots),
            snapshots_requested=requested_snapshots,
            symbol=symbol_key,
        )

    return output


def log_partial_snapshot_warning(
    logger: logging.Logger,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Log a warning when the run collected fewer snapshots than requested.

    Args:
        logger (logging.Logger): Command logger.
        symbol (str): Requested symbol.
        collected_snapshots (int): Actual snapshots collected for the symbol.
        requested_snapshots (int): Requested snapshots for the symbol.

    Returns:
        None: This function only writes logs.
    """

    if collected_snapshots >= requested_snapshots:
        return
    logger.warning(
        "bronze-builder collected partial snapshots symbol=%s collected=%s requested=%s",
        symbol,
        collected_snapshots,
        requested_snapshots,
    )


def persist_bronze_snapshots(
    snapshots_by_symbol: SnapshotsBySymbol,
    lake_root: str,
    depth: int,
    enabled: bool,
    output: dict[str, object],
    logger: logging.Logger,
    save_snapshots: SavePerpsSnapshots,
) -> tuple[list[str], str | None]:
    """Persist raw perpetual L2 snapshots when requested and annotate CLI output.

    Args:
        snapshots_by_symbol (SnapshotsBySymbol): Collected snapshots keyed by requested symbol.
        lake_root (str): Bronze lake root path.
        depth (int): Book depth used for the snapshot collection.
        enabled (bool): Whether persistence is enabled.
        output (dict[str, object]): Mutable command output payload.
        logger (logging.Logger): Command logger.
        save_snapshots (SavePerpsSnapshots): Persistence adapter.

    Returns:
        tuple[list[str], str | None]: Written files and optional persistence error.
    """

    if not enabled:
        return [], None

    try:
        parquet_files = save_snapshots(
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


def estimated_poll_runtime_s(snapshot_count: int, poll_interval_s: float) -> float:
    """Estimate runtime spent sleeping between polling ticks.

    Args:
        snapshot_count (int): Requested snapshots per symbol.
        poll_interval_s (float): Sleep interval between polling ticks.

    Returns:
        float: Estimated sleep budget in seconds.
    """

    return max(0, snapshot_count - 1) * poll_interval_s


def warn_for_long_poll_schedule(
    logger: logging.Logger,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float,
) -> None:
    """Warn when perpetual L2 polling settings may collide with minute cron runs.

    Args:
        logger (logging.Logger): Command logger.
        snapshot_count (int): Requested snapshots per symbol.
        poll_interval_s (float): Sleep interval between polling ticks.
        max_runtime_s (float): Configured max runtime.

    Returns:
        None: This function only writes warning logs.
    """

    estimated_s = estimated_poll_runtime_s(snapshot_count=snapshot_count, poll_interval_s=poll_interval_s)
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


def run_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
    *,
    normalize_symbols: NormalizeSymbols,
    fetch_snapshots: FetchPerpsSnapshots,
    save_snapshots: SavePerpsSnapshots,
) -> None:
    """Run perpetual L2 snapshot collection and optional raw Bronze persistence.

    Args:
        args (argparse.Namespace): Parsed CLI arguments.
        logger (logging.Logger): Command logger.
        normalize_symbols (NormalizeSymbols): CLI symbol normalization strategy.
        fetch_snapshots (FetchPerpsSnapshots): Source fetch adapter.
        save_snapshots (SavePerpsSnapshots): Bronze persistence adapter.

    Returns:
        None: This function writes logs, optional parquet files, and optional JSON output.
    """

    started_at = perf_counter()
    exchange = cast(str, args.exchange)
    symbols = normalize_symbols(cast(list[str], args.symbols))
    requested_snapshots = int(args.snapshot_count)
    max_runtime_s = float(args.max_runtime_s)
    log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "run_start",
        dataset_type=PERPS_L2_DATASET_TYPE,
        exchange=exchange,
        depth=int(args.levels),
        lake_root=cast(str, args.lake_root),
        max_runtime_s=max_runtime_s,
        poll_interval_s=float(args.poll_interval_s),
        save_parquet_lake=bool(args.save_parquet_lake),
        snapshot_count=requested_snapshots,
        symbols=symbols,
    )
    warn_for_long_poll_schedule(
        logger=logger,
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s,
    )
    snapshots_by_symbol = fetch_snapshots(
        exchange=exchange,
        symbols=symbols,
        depth=int(args.levels),
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s if max_runtime_s > 0 else None,
    )
    log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "collection_complete",
        dataset_type=PERPS_L2_DATASET_TYPE,
        exchange=exchange,
        snapshots_collected=sum(len(snapshots) for snapshots in snapshots_by_symbol.values()),
        snapshots_by_symbol={symbol: len(snapshots_by_symbol.get(symbol, [])) for symbol in symbols},
        snapshots_requested=requested_snapshots * len(symbols),
    )

    output = build_snapshot_output(
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        logger=logger,
    )
    parquet_files, parquet_error = persist_bronze_snapshots(
        snapshots_by_symbol=snapshots_by_symbol,
        lake_root=cast(str, args.lake_root),
        depth=int(args.levels),
        enabled=bool(args.save_parquet_lake),
        output=output,
        logger=logger,
        save_snapshots=save_snapshots,
    )
    log_dataset_debug_event(
        logger,
        BRONZE_BUILDER_COMMAND,
        "persistence_complete",
        dataset_type=PERPS_L2_DATASET_TYPE,
        exchange=exchange,
        files=len(parquet_files),
        output_files=parquet_files,
        parquet_error=parquet_error,
        save_parquet_lake=bool(args.save_parquet_lake),
    )

    emit_json_output(bool(args.json_output), output)
    log_bronze_builder_summary(
        logger=logger,
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        parquet_files=parquet_files,
        elapsed_s=perf_counter() - started_at,
        parquet_error=parquet_error,
    )
