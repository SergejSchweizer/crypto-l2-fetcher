"""Runtime helpers for CLI logging settings."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from api.logging_common import ModuleScopeFilter, log_file_stem_for_module_name, unified_formatter
from ingestion.config import Config, load_config, typed_runtime_config

LOGGER_NAME = "crypto_live_loader"
DEFAULT_LOG_DIR = ".logs"
DEFAULT_LOG_ROTATION_DAYS = 7
DEFAULT_LOG_BACKUP_COUNT = 3


def _zip_archive_name(default_name: str) -> str:
    """Return the dated ZIP archive name used by the rotating handler."""

    return f"{default_name}.zip"


def _zip_rotated_log(source: str, destination: str) -> None:
    """Atomically compress a rotated logfile and remove its source.

    Args:
        source (str): Active logfile path selected for rotation.
        destination (str): Final dated ZIP archive path.

    Raises:
        OSError: If the source cannot be read or the archive cannot be written.
    """

    source_path = Path(source)
    destination_path = Path(destination)
    temporary_path = destination_path.with_suffix(f"{destination_path.suffix}.tmp")
    try:
        with ZipFile(
            temporary_path,
            mode="w",
            compression=ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            archive.write(source_path, arcname=source_path.name)
        temporary_path.replace(destination_path)
        source_path.unlink()
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise


def _safe_log_module_name(module_name: str) -> str:
    """Return a filesystem-safe log module name."""

    normalized = module_name.strip().replace("/", "-").replace("\\", "-")
    return normalized or "crypto-live-loader"


def configure_logging(module_name: str = "crypto-live-loader", config: Config | None = None) -> logging.Logger:
    """Configure runtime logging with rotation to one file per module."""

    safe_module_name = _safe_log_module_name(module_name)
    safe_logfile_stem = _safe_log_module_name(log_file_stem_for_module_name(module_name))
    logger = logging.getLogger(f"{LOGGER_NAME}.{safe_module_name}")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = unified_formatter()
    scope_filter = ModuleScopeFilter()
    resolved_config = config or load_config()
    runtime_config = typed_runtime_config(resolved_config)
    log_dir = runtime_config.log_dir
    logfile = log_dir / f"{safe_logfile_stem}.log"
    rotation_days = max(1, runtime_config.log_rotation_days)
    backup_count = max(0, runtime_config.log_backup_count)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=logfile,
            when="D",
            interval=rotation_days,
            backupCount=backup_count,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.namer = _zip_archive_name
        file_handler.rotator = _zip_rotated_log
        file_handler.setFormatter(formatter)
        file_handler.addFilter(scope_filter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Falling back to stderr logging; cannot create logfile '%s'", logfile)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(scope_filter)
    logger.addHandler(stream_handler)

    return logger
