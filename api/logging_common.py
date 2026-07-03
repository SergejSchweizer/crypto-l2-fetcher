"""Shared log formatting helpers used across CLI and scripts."""

from __future__ import annotations

import logging

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
    VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
)
from domain.datasets import (
    BRONZE_DATASET_CONTRACTS,
    FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY,
    FUTURES_SUMMARY_SNAPSHOT_1M,
    INDEX_PRICE_SNAPSHOT_1M,
    INSTRUMENT_METADATA_SNAPSHOT_DAILY,
    OPTION_INSTRUMENT_TICKER_SNAPSHOT_1M,
    OPTIONS_L2_SNAPSHOT_1M,
    OPTIONS_TICKER_SNAPSHOT_1M,
    PERPS_L2_SNAPSHOT_1M,
    RECENT_TRADE_SNAPSHOT_1M,
    VOLATILITY_INDEX_SNAPSHOT_1M,
)

UNIFIED_LOG_FORMAT = "%(asctime)s %(levelname)s %(module_scope)s %(name)s %(message)s"

FUTURES_SUMMARY_LOG_SCOPE = FUTURES_SUMMARY_SNAPSHOT_1M.log_stem
INDEX_PRICE_LOG_SCOPE = INDEX_PRICE_SNAPSHOT_1M.log_stem
INSTRUMENT_METADATA_LOG_SCOPE = INSTRUMENT_METADATA_SNAPSHOT_DAILY.log_stem
FUTURE_INSTRUMENT_METADATA_LOG_SCOPE = FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY.log_stem
PERPS_L2_LOG_SCOPE = PERPS_L2_SNAPSHOT_1M.log_stem
OPTION_L2_LOG_SCOPE = OPTIONS_L2_SNAPSHOT_1M.log_stem
OPTION_INSTRUMENT_TICKER_LOG_SCOPE = OPTION_INSTRUMENT_TICKER_SNAPSHOT_1M.log_stem
OPTIONS_TICKER_LOG_SCOPE = OPTIONS_TICKER_SNAPSHOT_1M.log_stem
RECENT_TRADE_LOG_SCOPE = RECENT_TRADE_SNAPSHOT_1M.log_stem
VOLATILITY_INDEX_LOG_SCOPE = VOLATILITY_INDEX_SNAPSHOT_1M.log_stem

COMMAND_LOG_SCOPES = {
    FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND: FUTURES_SUMMARY_LOG_SCOPE,
    INDEX_PRICE_BRONZE_BUILDER_COMMAND: INDEX_PRICE_LOG_SCOPE,
    INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND: INSTRUMENT_METADATA_LOG_SCOPE,
    BRONZE_BUILDER_COMMAND: PERPS_L2_LOG_SCOPE,
    LEGACY_L2_BRONZE_BUILDER_COMMAND: PERPS_L2_LOG_SCOPE,
    LEGACY_BRONZE_BUILDER_COMMAND: PERPS_L2_LOG_SCOPE,
    OPTION_L2_BRONZE_BUILDER_COMMAND: OPTION_L2_LOG_SCOPE,
    LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND: OPTION_L2_LOG_SCOPE,
    OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND: OPTION_INSTRUMENT_TICKER_LOG_SCOPE,
    OPTIONS_BRONZE_BUILDER_COMMAND: OPTIONS_TICKER_LOG_SCOPE,
    RECENT_TRADES_BRONZE_BUILDER_COMMAND: RECENT_TRADE_LOG_SCOPE,
    VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND: VOLATILITY_INDEX_LOG_SCOPE,
}

DATASET_LOG_SCOPES = frozenset(contract.log_stem for contract in BRONZE_DATASET_CONTRACTS)


def log_scope_for_module_name(module_name: str) -> str:
    """Resolve a stable dataset scope for command loggers."""

    logger_name = module_name.lower()
    for command, scope in sorted(COMMAND_LOG_SCOPES.items(), key=lambda item: len(item[0]), reverse=True):
        if command in logger_name:
            return scope
    for scope in DATASET_LOG_SCOPES:
        if scope in logger_name:
            return scope
    return "core"


def log_file_stem_for_module_name(module_name: str) -> str:
    """Return the dataset-aligned logfile stem for command loggers."""

    scope = log_scope_for_module_name(module_name)
    return module_name if scope == "core" else scope


class ModuleScopeFilter(logging.Filter):
    """Inject a stable module scope field for unified log formatting."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.module_scope = log_scope_for_module_name(record.name)
        return True


def unified_formatter() -> logging.Formatter:
    """Return the shared formatter used by all project loggers."""

    return logging.Formatter(UNIFIED_LOG_FORMAT)
