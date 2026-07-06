# Architecture

Last updated: 2026-07-06

`crypto-live-loader` is a Bronze-only Deribit market-data ingestion system. It fetches public REST
market data, normalizes source payloads into typed rows, and writes deterministic parquet upserts to
the local Bronze lake.

This document is part of the repository contract. Keep it up to date in the same change set whenever
module boundaries, dataset contracts, storage layout, runtime orchestration, logging conventions, or
quality gates change.

## System Shape

```text
Deribit public REST APIs
  -> sources/ Deribit fetchers
  -> ingestion/ dataset normalizers
  -> ingestion/*_lake.py parquet writers
  -> ingestion/parquet_repository.py atomic upserts
  -> lake/bronze Hive-style parquet partitions

main.py
  -> api/cli.py
  -> api/commands/bronze.py
  -> dataset orchestration, logging, JSON command output
```

The repository intentionally stops at Bronze. Silver and Gold feature engineering, option-surface
construction, research joins, and forecasting features are downstream responsibilities.

## Layer Responsibilities

| Layer | Responsibility | Must Not Own |
|---|---|---|
| `domain/` | Shared dataset contracts, source contracts, and typed market-data models | CLI behavior, Deribit HTTP calls, parquet writes, filesystem state |
| `sources/` | Source-specific Deribit REST adapters and exchange registry | CLI parsing, parquet persistence, application orchestration |
| `ingestion/` | Dataset normalization, runtime config loading, partition path construction, idempotent lake writes | CLI presentation, command-line parsing |
| `api/` | CLI parser, command dispatch, command runtime output, logging setup | Low-level parquet repository internals and file-lock mechanics |
| `scripts/` | Operational maintenance helpers and migrations | Runtime command contracts |
| `tests/` | Unit, CLI, lake, migration, contract, and architecture regression tests | Production behavior |

Architecture tests in `tests/test_architecture.py` enforce the most important dependency rules:

- `domain/` does not import `api`, `ingestion`, or `sources`.
- `ingestion/` and `sources/` do not import `api`.
- `api/` does not import low-level parquet repository or file-lock internals.
- Shared helpers stay free of dataset-specific dependencies.
- Project imports remain acyclic.

## Dataset Contract Model

Bronze datasets are registered in `domain/datasets.py` as `DatasetContract` values. The contract is
the canonical source for:

- `dataset_type` partition value.
- Lake layer ownership.
- Dataset-scoped logfile stem.
- Ordered semantic partition columns.
- Canonical source identifier.
- Short operational description.

The current Bronze datasets are:

| Dataset Type | Source | Partition Columns |
|---|---|---|
| `perps_l2_snapshot_1m` | `rest_order_book` | `exchange`, `instrument_type`, `symbol`, `depth`, `source`, `event_time` |
| `options_ticker_snapshot_1m` | `rest_get_book_summary_by_currency` | `exchange`, `currency`, `instrument_name`, `source`, `snapshot_time` |
| `options_instrument_ticker_snapshot_1m` | `rest_ticker` | `exchange`, `instrument_name`, `source`, `snapshot_time` |
| `options_l2_snapshot_1m` | `rest_order_book` | `exchange`, `symbol`, `source`, `depth`, `event_time` |
| `instrument_metadata_snapshot_daily` | `rest_get_instruments` | `exchange`, `instrument_name`, `snapshot_date` |
| `futures_instrument_metadata_snapshot_daily` | `rest_get_instruments` | `exchange`, `instrument_name`, `snapshot_date` |
| `index_price_snapshot_1m` | `rest_get_index_price` | `exchange`, `index_name`, `event_time` |
| `volatility_index_snapshot_1m` | `rest_get_volatility_index_data` | `exchange`, `currency`, `source`, `event_time` |
| `futures_summary_snapshot_1m` | `rest_get_book_summary_by_currency` | `exchange`, `instrument_name`, `source`, `snapshot_time` |
| `recent_trade_snapshot_1m` | `rest_get_last_trades_by_currency` | `exchange`, `instrument_name`, `trade_id` |

When a dataset contract changes, update `README.md`, this file, relevant tests, and migration notes
in the same commit. If the change renames persisted data, migrate both the Hive partition path and
the in-file `dataset_type` column.

## Runtime Flow

1. `main.py` calls the CLI entrypoint.
2. `api/cli.py` loads `config.yaml`, builds command parsers, and maps config defaults to flags.
3. `api/runtime.py` configures dataset-scoped logging under `.logs`.
4. `api/commands/bronze.py` dispatches the selected Bronze collector.
5. `sources/` fetchers call Deribit public REST endpoints sequentially for bounded request scopes.
6. `ingestion/` normalizers convert payload dictionaries into typed rows with run metadata.
7. `ingestion/*_lake.py` modules group rows by dataset-owned partition keys.
8. `ingestion/parquet_repository.py` performs deterministic upserts into `data.parquet` files.
9. Command runtime helpers emit structured logs and optional JSON command output.

## Storage and Idempotency

The Bronze lake uses Hive-style local paths below `lake/bronze`:

```text
lake/bronze/dataset_type=<dataset>/.../data.parquet
```

Dataset-specific lake modules own partition order. Shared lake helpers only assemble ordered
partition parts and delegate persistence to the parquet repository.

Writes are designed to be restart-safe:

- Rows are grouped by partition before writing.
- Existing parquet rows are merged with incoming rows by natural key.
- Output order is deterministic through dataset-specific sort keys.
- Staging filenames are deterministic enough to avoid cross-partition collisions.
- `data.parquet` is replaced atomically after a successful write.

## Configuration and Operations

`config.yaml` is the canonical runtime configuration file. CLI flags may override config defaults for
one run, but command behavior should remain reproducible from explicit config plus command arguments.

Operational state is local and intentionally excluded from git:

- `lake/` contains Bronze data.
- `.logs/` contains dataset-scoped logs.
- `.state/` contains runtime state where used.

Every dataset command accepts `--debug`. Debug logs should include source scope, row counts,
persistence paths, timing, and error counts without hiding failed external calls.

## Quality Gates

The repository uses strict local quality gates through pre-commit and test configuration:

- Ruff linting and formatting.
- Strict mypy and pyright checks.
- `ty` checks.
- `interrogate` and `pydoclint` documentation checks.
- Pytest with coverage.
- Architecture boundary tests.

For architecture-sensitive changes, run at least:

```bash
.venv/bin/python -m pytest -q tests/test_architecture.py
.venv/bin/python -m pytest -q
```

## Update Rules

Update `ARCHITECTURE.md` in the same commit when any of these change:

- A package gains or loses responsibility.
- A dependency direction or forbidden import rule changes.
- A dataset is added, removed, renamed, repartitioned, or re-sourced.
- A command orchestration path changes.
- Lake write semantics, natural keys, sort keys, or atomic write behavior change.
- Runtime config, logging, or local state conventions change.
- Quality gates or architecture tests change.

If the architecture impact is intentionally none, no update is required.