"""Shared runtime helpers for CLI command execution."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatasetCommandResult:
    """Completed dataset command payload and summary metadata."""

    command: str
    dataset_type: str
    payload: Mapping[str, object]
    summary_fields: Mapping[str, object]
    json_output: bool


def emit_json_output(enabled: bool, payload: Mapping[str, object]) -> None:
    """Print one JSON payload when output is enabled."""

    if enabled:
        print(json.dumps(payload, indent=2))


def log_job_event(
    logger: logging.Logger,
    level: int,
    command: str,
    event: str,
    **fields: object,
) -> None:
    """Write one structured command lifecycle event."""

    logger.log(level, "job_event command=%s event=%s %s", command, event, _format_log_fields(fields))


def log_dataset_event(
    logger: logging.Logger,
    level: int,
    command: str,
    event: str,
    *,
    dataset_type: str,
    exchange: str = "deribit",
    **fields: object,
) -> None:
    """Write one dataset-scoped lifecycle event with the shared log envelope."""

    log_job_event(
        logger,
        level,
        command,
        event,
        dataset_type=dataset_type,
        exchange=exchange,
        **fields,
    )


def log_dataset_debug_event(
    logger: logging.Logger,
    command: str,
    event: str,
    *,
    dataset_type: str,
    exchange: str = "deribit",
    **fields: object,
) -> None:
    """Write an expressive dataset debug event only when DEBUG logging is enabled."""

    if not logger.isEnabledFor(logging.DEBUG):
        return
    log_dataset_event(
        logger,
        logging.DEBUG,
        command,
        event,
        dataset_type=dataset_type,
        exchange=exchange,
        **fields,
    )


def emit_dataset_command_result(logger: logging.Logger, result: DatasetCommandResult) -> None:
    """Emit JSON output and the standard dataset run summary event."""

    emit_json_output(result.json_output, result.payload)
    log_job_event(
        logger,
        logging.INFO,
        result.command,
        "run_summary",
        dataset_type=result.dataset_type,
        exchange="deribit",
        **result.summary_fields,
    )


def _log_value(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, Mapping):
        return ",".join(f"{key}:{_log_value(item)}" for key, item in sorted(value.items()))
    if isinstance(value, list | tuple | set):
        return ",".join(str(item) for item in value)
    return str(value)


def _format_log_fields(fields: Mapping[str, object]) -> str:
    return " ".join(f"{key}={_log_value(value)}" for key, value in sorted(fields.items()))
