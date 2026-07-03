# CRYPTO-LIVE-LOADER

Production-grade Deribit live market-data ingestion framework for Bronze parquet snapshots used in
quantitative research, IV/RV monitoring, and operational analytics.

Author: Sergej Schweizer

---

# Table Of Contents

- [CRYPTO-LIVE-LOADER](#crypto-live-loader)
- [Table Of Contents](#table-of-contents)
- [1. System Overview](#1-system-overview)
  - [1.1 Core Design Principles](#11-core-design-principles)
  - [1.2 Bronze-Only Architecture](#12-bronze-only-architecture)
  - [1.3 Supported Live Data Domains](#13-supported-live-data-domains)
    - [Domain Groups](#domain-groups)
    - [CLI Contract](#cli-contract)
- [2. Repository Structure](#2-repository-structure)
- [3. Installation](#3-installation)
  - [3.1 System Prerequisites](#31-system-prerequisites)
  - [3.2 Python Environment Setup](#32-python-environment-setup)
  - [3.3 Quick Start](#33-quick-start)
- [4. Bronze Datasets](#4-bronze-datasets)
  - [4.1 Perpetual L2 Order Book (`dataset_type=perps_l2_snapshot_1m`)](#41-perpetual-l2-order-book-dataset_typeperps_l2_snapshot_1m)
  - [4.2 Options Summary (`dataset_type=options_ticker_snapshot_1m`)](#42-options-summary-dataset_typeoptions_ticker_snapshot_1m)
  - [4.3 Option Instrument Ticker (`dataset_type=option_instrument_ticker_snapshot_1m`)](#43-option-instrument-ticker-dataset_typeoption_instrument_ticker_snapshot_1m)
  - [4.4 Option L2 Order Book (`dataset_type=options_l2_snapshot_1m`)](#44-option-l2-order-book-dataset_typeoptions_l2_snapshot_1m)
  - [4.5 Instrument Metadata (`dataset_type=instrument_metadata_snapshot_daily`)](#45-instrument-metadata-dataset_typeinstrument_metadata_snapshot_daily)
  - [4.6 Index Price (`dataset_type=index_price_snapshot_1m`)](#46-index-price-dataset_typeindex_price_snapshot_1m)
  - [4.7 Volatility Index (`dataset_type=volatility_index_snapshot_1m`)](#47-volatility-index-dataset_typevolatility_index_snapshot_1m)
  - [4.8 Futures Summary (`dataset_type=futures_summary_snapshot_1m`)](#48-futures-summary-dataset_typefutures_summary_snapshot_1m)
  - [4.9 Recent Trade Tape (`dataset_type=recent_trade_snapshot_1m`)](#49-recent-trade-tape-dataset_typerecent_trade_snapshot_1m)
- [5. Storage Layout](#5-storage-layout)
- [6. Configuration Model](#6-configuration-model)
- [7. Example Commands](#7-example-commands)
  - [7.1 Bronze Collectors](#71-bronze-collectors)
  - [7.2 Validation Utility](#72-validation-utility)
  - [7.3 Production Cron](#73-production-cron)
  - [7.4 Quality Checks](#74-quality-checks)
- [8. Operations](#8-operations)
  - [8.1 Idempotency](#81-idempotency)
  - [8.2 Observability and Logging](#82-observability-and-logging)
  - [8.3 Bronze Disk Retention](#83-bronze-disk-retention)
  - [8.4 Data Quality](#84-data-quality)
- [9. Risk Notes and Limitations](#9-risk-notes-and-limitations)
- [10. Roadmap](#10-roadmap)

---

# 1. System Overview

`crypto-live-loader` ingests live Deribit public REST market data into a deterministic local Bronze
lake. The repository is intentionally focused on raw, restart-safe capture. It does not build Silver
or Gold feature layers.

Primary use cases:

- Maintain minute-level raw Deribit market snapshots for BTC, ETH, and SOL.
- Capture L2 order-book state, option summary rows, per-option Greeks/IV, instrument metadata, and
  index prices.
- Provide stable Bronze inputs for downstream IV/RV forecasting, research joins, and incident
  replay.

## 1.1 Core Design Principles

The repository follows the engineering principles defined in `AGENTS.md`:

- maintainability
- modularity
- reproducibility
- deterministic processing
- idempotent ingestion
- explicit interfaces
- restart-safe operational behavior

## 1.2 Bronze-Only Architecture

The system persists source-facing records directly into Bronze parquet partitions. Every collector
normalizes response payloads into typed rows, writes them with deterministic natural keys, and keeps
external side effects behind explicit source and lake adapters.

```text
Deribit public REST endpoints
  -> source fetchers
  -> typed Bronze normalizers
  -> idempotent parquet upserts
  -> lake/bronze
```

Silver and Gold functionality is intentionally out of scope for this repository.

## 1.3 Supported Live Data Domains

### Domain Groups

Perpetual Order Book:

| CLI Command | Bronze `dataset_type` | Instrument Type | Default Symbols | Source Endpoint | Description |
|---|---|---|---|---|---|
| `perp-l2-bronze-builder` | `perps_l2_snapshot_1m` | `perp` | `BTC ETH SOL` | `public/get_order_book` | Raw perpetual order-book snapshots |

Options:

| CLI Command | Bronze `dataset_type` | Instrument Type | Default Symbols | Source Endpoint | Description |
|---|---|---|---|---|---|
| `options-bronze-builder` | `options_ticker_snapshot_1m` | `option` | `BTC ETH SOL` | `public/get_book_summary_by_currency` | Broad option-chain summary rows |
| `option-instrument-ticker-bronze-builder` | `option_instrument_ticker_snapshot_1m` | `option` | `BTC ETH SOL` | `public/ticker` | Selected per-option IV, bid/ask IV, and Greeks |
| `options-l2-bronze-builder` | `options_l2_snapshot_1m` | `option` | `BTC ETH SOL` | `public/get_order_book` | Selected per-option bid/ask depth and top-of-book IV context |
| `futures-summary-bronze-builder` | `futures_summary_snapshot_1m` | `future`, `perp` | `BTC ETH SOL` | `public/get_book_summary_by_currency` | Dated futures and perpetual curve summary rows |
| `recent-trades-bronze-builder` | `recent_trade_snapshot_1m` | `option`, `future`, `perp` | `BTC ETH SOL` | `public/get_last_trades_by_currency` | Recent trade tape for options, futures, and perpetuals |
| `instrument-metadata-bronze-builder` | `instrument_metadata_snapshot_daily`, `future_instrument_metadata_snapshot_daily` | `option`, `future` | `BTC ETH SOL` | `public/get_instruments` | Active instrument metadata snapshots |

Index State:

| CLI Command | Bronze `dataset_type` | Instrument Type | Default Symbols | Source Endpoint | Description |
|---|---|---|---|---|---|
| `index-price-bronze-builder` | `index_price_snapshot_1m` | `index` | `btc_usd eth_usd sol_usdc` | `public/get_index_price` | Raw Deribit index-price observations |
| `volatility-index-bronze-builder` | `volatility_index_snapshot_1m` | `volatility_index` | `BTC ETH SOL` | `public/get_volatility_index_data` | Deribit volatility-index candles |

### CLI Contract

- `--symbols` and `--currencies` are aliases for option currency inputs.
- Logical `SOL` option requests map to Deribit `USDC` option summaries and filter `SOL_USDC-*`
  instruments.
- All Bronze writers default to `lake/bronze` unless overridden with `--lake-root`.
- Every command supports `--debug` for verbose operational logs.

Current exchange support:

- Deribit

Primary assets:

- BTC
- ETH
- SOL

---

# 2. Repository Structure

```text
api/
domain/
ingestion/
sources/
scripts/
tests/
lake/
config.yaml
pyproject.toml
main.py
README.md
AGENTS.md
```

| Path | Responsibility |
|---|---|
| `api/` | CLI parsing, command orchestration, runtime logging setup, and command constants |
| `domain/` | Shared source contracts and typed market-data models |
| `ingestion/` | Bronze normalizers, parquet lake writers, config loading, and file-locking utilities |
| `sources/` | Deribit public REST fetchers and source adapter registry |
| `scripts/` | Operational helpers, including Bronze layout migration and agent sync tooling |
| `tests/` | Unit, integration-style, CLI, storage, and architecture regression tests |
| `lake/` | Local runtime Bronze data lake; ignored by git |
| `.logs/` | Local runtime logs; ignored by git |
| `.state/` | Local runtime state, where used; ignored by git |
| `config.yaml` | Canonical runtime configuration file |
| `pyproject.toml` | Project metadata and Python quality-tool configuration |
| `main.py` | Python entrypoint wrapper for CLI execution |
| `AGENTS.md` | Generated repository operating policy |
| `RISKS.md` | Active risk register and mitigation status |
| `TIMELINE.md` | Chronological record of dataset, runtime, and operational milestones |
| `DECISIONS.md` | Durable architecture, naming, and operational decisions |

---

# 3. Installation

## 3.1 System Prerequisites

Install Python and common workflow tooling before running collectors or quality gates.

Linux (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y git gh python3 python3-venv
```

Verify installs:

```bash
git --version
gh --version
python3 --version
```

## 3.2 Python Environment Setup

Development setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

Runtime-only setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Runtime configuration uses:

```text
config.yaml
```

Recommended permissions:

```bash
chmod 600 config.yaml
```

## 3.3 Quick Start

For an IV/RV research feed, run the Bronze collectors in this order:

```bash
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind future
python main.py options-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py futures-summary-bronze-builder --debug --symbols BTC ETH SOL
python main.py option-instrument-ticker-bronze-builder --debug --symbols BTC ETH SOL --max-instruments-per-run 60
python main.py options-l2-bronze-builder --debug --symbols BTC ETH SOL --depth 20 --max-instruments-per-run 60
python main.py recent-trades-bronze-builder --debug --symbols BTC ETH SOL --kinds option future --count 1000
python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
python main.py volatility-index-bronze-builder --debug --symbols BTC ETH SOL --resolution 60
```

The metadata job captures the active option universe, the options summary job provides broad chain
liquidity context, the per-instrument ticker and option L2 jobs capture the selected IV/Greeks panel
and bid/ask depth, and the recent trade job captures signed flow and jump/volume-shock inputs used
by downstream surface builders.

---

# 4. Bronze Datasets

All datasets are Bronze raw snapshots. Shared metadata columns include source identifiers,
`run_id`, ingestion timestamps, dataset type, exchange, and schema version. Dataset-specific
timestamps are preserved as the natural event or snapshot time.

## 4.1 Perpetual L2 Order Book (`dataset_type=perps_l2_snapshot_1m`)

### 1. Bronze Layer

Market role: raw perpetual order-book depth for spread, imbalance, microstructure, and realized
volatility context.

Canonical dataset name: `perps_l2_snapshot_1m`. Historical Bronze directories named
`dataset_type=l2_snapshot` or `dataset_type=perp_l2_snapshot` are deprecated and must be migrated
into `dataset_type=perps_l2_snapshot_1m` before downstream reads.

Endpoint: `GET /api/v2/public/get_order_book`

Default command:

```bash
python main.py perp-l2-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
```

Key fields:

| Column | Market Meaning |
|---|---|
| `symbol` | Normalized Deribit perpetual instrument |
| `event_time` | Exchange snapshot timestamp |
| `bids`, `asks` | Raw book levels |
| `mark_price`, `index_price` | Venue mark and index context |
| `open_interest` | Perpetual positioning state |
| `funding_8h`, `current_funding` | Funding context for carry and dislocation diagnostics |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `source`, `depth` |
| Time/lineage | `event_time`, `ingested_at`, `run_id`, `fetch_duration_s` |
| Book | `bids`, `asks` |
| Market state | `mark_price`, `index_price`, `open_interest`, `funding_8h`, `current_funding` |

## 4.2 Options Summary (`dataset_type=options_ticker_snapshot_1m`)

### 1. Bronze Layer

Market role: broad option-chain context for IV surface state, liquidity screening, and downstream
selection of high-value per-instrument ticker requests.

Endpoint: `GET /api/v2/public/get_book_summary_by_currency`

Default command:

```bash
python main.py options-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
```

Key fields:

| Column | Market Meaning |
|---|---|
| `currency`, `requested_currency`, `source_currency` | Logical and Deribit endpoint currency mapping |
| `instrument_name` | Option contract |
| `mark_iv`, `mark_price` | Summary implied volatility and mark state |
| `underlying_price`, `underlying_index` | Underlying reference for moneyness and term structure |
| `open_interest`, `volume`, `volume_usd` | Liquidity and participation context |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `exchange`, `dataset_type`, `source`, `currency`, `requested_currency`, `source_currency`, `instrument_name`, `base_currency`, `quote_currency`, `instrument_type`, `schema_version` |
| Time/lineage | `snapshot_time`, `exchange_creation_time`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Price/IV | `bid_price`, `ask_price`, `mid_price`, `mark_price`, `mark_iv`, `last`, `price_change` |
| Underlying/carry | `underlying_price`, `underlying_index`, `interest_rate` |
| Liquidity/activity | `open_interest`, `volume`, `volume_usd`, `high`, `low` |

## 4.3 Option Instrument Ticker (`dataset_type=option_instrument_ticker_snapshot_1m`)

### 1. Bronze Layer

Market role: high-value IV/RV forecasting source with per-option `bid_iv`, `ask_iv`, `mark_iv`,
underlying price, interest rate, open interest, and Greeks.

Endpoint: `GET /api/v2/public/ticker?instrument_name=<option instrument>`

Default command:

```bash
python main.py option-instrument-ticker-bronze-builder --debug --symbols BTC ETH SOL --max-instruments-per-run 60
```

Selection policy:

- Aligns with the active listed option universe captured by Bronze instrument metadata.
- Uses the current broad option summary as a liquidity and moneyness input.
- Selects a bounded liquid prediction universe per currency rather than sparse cursor rotation.
- Targets tenor buckets `1D`, `2D`, `7D`, `14D`, `30D`, and `60D`.
- Targets moneyness buckets `0.90`, `0.95`, `1.00`, `1.05`, and `1.10`.
- Captures calls and puts where available.
- Uses the nearest listed expiry when exact 30D or 60D contracts are unavailable.
- Filters out stale rows without a usable quote or mark.
- Keeps live Deribit REST access in this repository; history loaders should consume this Bronze
  dataset and build Silver/Gold surfaces downstream.
- Join with `instrument_metadata_snapshot_daily` for `expiration_timestamp`, `strike`,
  `option_type`, contract sizing, tick size, settlement currency, and active-state metadata.

Coverage contract:

| Item | Contract |
|---|---|
| Target dataset | `option_instrument_ticker_snapshot_1m` |
| Live endpoint owner | `crypto-live-loader` only |
| Downstream owner | History/research repositories consume Bronze and build Silver/Gold surfaces |
| Assets | BTC, ETH, SOL |
| SOL endpoint mapping | Fetch Deribit `USDC` option summaries and keep `SOL_USDC-*` instruments |
| Fetch mode | Sequential, bounded per run, no parallel REST fan-out |
| Selection goal | Liquid calls and puts across target tenor and moneyness buckets |
| Operational default | `60` instruments per currency per minute to cover the 6 × 5 × 2 target grid when listings are available |

Raw ticker fields:

| Column | Market Meaning |
|---|---|
| `instrument_name`, `state`, `exchange_timestamp`, `snapshot_time` | Contract identity, exchange event time, and capture time |
| `ingested_at`, `run_id`, `source`, `raw_payload_hash` | Operational lineage, idempotency, and replay auditing |
| `best_bid_price`, `best_ask_price`, `best_bid_amount`, `best_ask_amount` | Top-of-book quote and size for stale or illiquid quote filtering |
| `bid_price`, `ask_price`, `mark_price`, `last_price` | Raw price observations from Deribit ticker payloads |
| `bid_iv`, `ask_iv`, `mark_iv` | Bid/ask/mark implied volatility |
| `delta`, `gamma`, `theta`, `vega`, `rho` | Option Greeks |
| `underlying_price`, `underlying_index`, `index_price` | IV/RV alignment inputs |
| `interest_rate`, `open_interest` | Carry and positioning context |
| `volume`, `volume_usd`, `high`, `low`, `price_change` | 24h liquidity and activity context |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `exchange`, `dataset_type`, `source`, `currency`, `instrument_name`, `instrument_type`, `schema_version`, `state` |
| Time/lineage | `snapshot_time`, `exchange_creation_time`, `exchange_timestamp`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Quote/price | `bid_price`, `ask_price`, `best_bid_price`, `best_ask_price`, `best_bid_amount`, `best_ask_amount`, `mark_price`, `last_price` |
| IV/Greeks | `bid_iv`, `ask_iv`, `mark_iv`, `delta`, `gamma`, `theta`, `vega`, `rho` |
| Underlying/carry | `underlying_price`, `underlying_index`, `index_price`, `interest_rate` |
| Liquidity/activity | `open_interest`, `volume`, `volume_usd`, `high`, `low`, `price_change` |

Metadata join fields:

| Column | Market Meaning |
|---|---|
| `base_currency`, `quote_currency`, `settlement_currency`, `counter_currency` | Correct cross-asset joins and settlement semantics |
| `expiration_timestamp`, `strike`, `option_type` | Surface coordinates and time-to-expiry |
| `contract_size`, `tick_size`, `min_trade_amount` | Tradability, notional interpretation, and liquidity filters |
| `price_index`, `is_active`, `state` | Underlying reference and stale-contract filtering |

## 4.4 Option L2 Order Book (`dataset_type=options_l2_snapshot_1m`)

### 1. Bronze Layer

Market role: selected option bid/ask depth for quote quality, spread, microstructure, and liquidity
features around the same IV/Greeks prediction universe used by the per-instrument ticker collector.

Endpoint: `GET /api/v2/public/get_order_book?instrument_name=<option instrument>&depth=<depth>`

Default command:

```bash
python main.py options-l2-bronze-builder --debug --symbols BTC ETH SOL --depth 20 --max-instruments-per-run 60
```

Selection policy:

- Reuses the option ticker prediction universe selection from current option summaries.
- Supports explicit `--instruments` for deterministic targeted captures.
- Fetches sequentially and records per-instrument `fetch_duration_s` for debugging slow REST calls.
- Persists the requested `depth` in both row data and partition layout.
- Keeps raw `bids` and `asks` as price/amount level arrays without reconstructing a surface.

Coverage contract:

| Item | Contract |
|---|---|
| Target dataset | `options_l2_snapshot_1m` |
| Live endpoint owner | `crypto-live-loader` only |
| Assets | BTC, ETH, SOL |
| SOL endpoint mapping | Fetch Deribit `USDC` option summaries and keep `SOL_USDC-*` instruments |
| Fetch mode | Sequential, bounded per run, no parallel REST fan-out |
| Operational default | `20` book levels for up to `60` instruments per currency per minute |

Key fields:

| Column | Market Meaning |
|---|---|
| `instrument_name`, `snapshot_time`, `exchange_timestamp` | Contract identity, capture time, and Deribit order-book timestamp |
| `depth`, `bids`, `asks`, `bid_levels`, `ask_levels` | Requested and observed book depth per side |
| `best_bid_price`, `best_ask_price`, `best_bid_amount`, `best_ask_amount` | Top-of-book quote state |
| `bid_iv`, `ask_iv`, `mark_iv`, `underlying_price`, `index_price` | IV/RV alignment context from the order-book payload |
| `open_interest`, `volume`, `volume_usd`, `high`, `low`, `price_change` | Liquidity and activity context |
| `fetch_duration_s`, `run_id`, `raw_payload_hash` | Runtime diagnosis, replay lineage, and idempotency support |

## 4.5 Instrument Metadata (`dataset_type=instrument_metadata_snapshot_daily`)

### 1. Bronze Layer

Market role: active instrument reference data for option universe reconstruction and contract
metadata auditing.

Endpoint: `GET /api/v2/public/get_instruments`

Default command:

```bash
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
```

Futures metadata command:

```bash
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind future
```

SOL metadata mapping:

- Logical `SOL` uses Deribit `currency=USDC`.
- Rows are filtered to instruments starting with `SOL_USDC-`.
- `kind=option` writes `dataset_type=instrument_metadata_snapshot_daily`.
- `kind=future` writes `dataset_type=future_instrument_metadata_snapshot_daily`.

Key fields:

| Column | Market Meaning |
|---|---|
| `instrument_name` | Deribit instrument identifier |
| `base_currency`, `quote_currency`, `settlement_currency` | Contract currency semantics |
| `strike`, `expiration_timestamp`, `option_type` | Option contract shape |
| `is_active` | Listing status |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `source`, `instrument_name`, `kind`, `instrument_type` |
| Time/lineage | `snapshot_date`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Currency semantics | `base_currency`, `quote_currency`, `counter_currency`, `settlement_currency` |
| Contract rules | `tick_size`, `contract_size`, `min_trade_amount`, `is_active`, `state` |
| Lifecycle/shape | `creation_timestamp`, `expiration_timestamp`, `option_type`, `strike`, `settlement_period`, `price_index` |

## 4.6 Index Price (`dataset_type=index_price_snapshot_1m`)

### 1. Bronze Layer

Market role: raw index reference price for realized volatility calculations, option moneyness, and
cross-dataset time alignment.

Endpoint: `GET /api/v2/public/get_index_price`

Default command:

```bash
python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
```

Key fields:

| Column | Market Meaning |
|---|---|
| `index_name` | Deribit index identifier |
| `index_price` | Raw index value |
| `event_time` | Exchange event timestamp when available |
| `snapshot_time` | Collector minute timestamp |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `source`, `index_name` |
| Time/lineage | `snapshot_time`, `event_time`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Price | `price` |

## 4.7 Volatility Index (`dataset_type=volatility_index_snapshot_1m`)

### 1. Bronze Layer

Market role: Deribit market-level implied-volatility regime candles for IV/RV forecasting.

Endpoint: `GET /api/v2/public/get_volatility_index_data`

Default command:

```bash
python main.py volatility-index-bronze-builder --debug --symbols BTC ETH SOL --resolution 60
```

Coverage policy:

- Fetches BTC and ETH volatility-index candles directly.
- Probes logical SOL with Deribit `currency=USDC`.
- If Deribit returns no SOL-specific volatility-index candles, downstream systems should derive SOL
  implied-volatility regimes from the SOL option surface instead of fabricating a Bronze value.

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `source`, `currency`, `source_currency`, `resolution` |
| Candle | `timestamp`, `open`, `high`, `low`, `close` |
| Time/lineage | `snapshot_time`, `ingested_at`, `run_id`, `raw_payload_hash` |

## 4.8 Futures Summary (`dataset_type=futures_summary_snapshot_1m`)

### 1. Bronze Layer

Market role: futures/perpetual curve state for forward assumptions, basis, carry, and expiry-specific
dislocations used by option IV/RV models.

Endpoint: `GET /api/v2/public/get_book_summary_by_currency`

Default command:

```bash
python main.py futures-summary-bronze-builder --debug --symbols BTC ETH SOL
```

Coverage policy:

- Fetches `kind=future` summary rows for BTC, ETH, and SOL.
- Logical `SOL` maps to Deribit `currency=USDC` and filters `SOL_USDC-*` instruments.
- Includes dated futures and perpetual instruments returned by Deribit’s future summary endpoint.

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `source`, `currency`, `requested_currency`, `source_currency`, `instrument_name`, `instrument_type` |
| Time/lineage | `snapshot_time`, `exchange_creation_time`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Price | `bid_price`, `ask_price`, `mid_price`, `mark_price`, `last`, `high`, `low`, `price_change` |
| Curve/carry | `underlying_price`, `estimated_delivery_price`, `interest_rate` |
| Liquidity/activity | `open_interest`, `volume`, `volume_usd` |

## 4.9 Recent Trade Tape (`dataset_type=recent_trade_snapshot_1m`)

### 1. Bronze Layer

Market role: raw public trade tape for realized volatility, jump detection, signed flow, trade
imbalance, option trade IV, liquidation context, and volume-shock features.

Endpoint: `GET /api/v2/public/get_last_trades_by_currency`

Default command:

```bash
python main.py recent-trades-bronze-builder --debug --symbols BTC ETH SOL --kinds option future --count 1000
```

Coverage policy:

- Fetches BTC, ETH, and SOL trade tape for `kind=option` and `kind=future`.
- Uses Deribit `future` kind for both dated futures and perpetual instruments.
- Maps logical SOL to Deribit `currency=USDC` and keeps only `SOL_USDC-*` instruments.
- Uses `sorting=asc` and a configurable overlap window from `snapshot_time - lookback_seconds`.
- Fetches sequentially by currency/kind; there is no parallel REST fan-out.
- Relies on parquet upsert by `exchange`, `instrument_name`, and `trade_id` to dedupe overlap
  windows and make minute cron retries restart-safe.

Key fields:

| Column | Market Meaning |
|---|---|
| `trade_id`, `trade_sequence`, `instrument_name` | Trade identity and exchange ordering |
| `requested_currency`, `source_currency`, `currency`, `kind`, `instrument_type` | Logical and Deribit source scope, including SOL/USDC mapping |
| `exchange_timestamp`, `snapshot_time`, `ingested_at`, `run_id` | Exchange event time and ingestion lineage |
| `price`, `amount`, `direction`, `tick_direction` | Raw trade price, size, taker direction, and tick movement |
| `signed_amount`, `notional` | Derived signed flow and price-times-amount notional |
| `mark_price`, `index_price`, `iv` | Model context for futures/perps and option trade IV |
| `liquidation`, `block_trade_id`, `raw_payload_hash` | Liquidation/block context and replay checksum |

Fetched columns:

| Group | Columns |
|---|---|
| Identity | `schema_version`, `dataset_type`, `exchange`, `source`, `requested_currency`, `source_currency`, `currency`, `instrument_name`, `instrument_type`, `kind`, `trade_id`, `trade_sequence` |
| Time/lineage | `exchange_timestamp`, `snapshot_time`, `ingested_at`, `run_id`, `raw_payload_hash` |
| Trade | `price`, `amount`, `direction`, `tick_direction`, `signed_amount`, `notional` |
| Market context | `mark_price`, `index_price`, `iv`, `liquidation`, `block_trade_id` |

---

# 5. Storage Layout

Bronze root:

```text
lake/bronze/
  dataset_type=perps_l2_snapshot_1m/
    exchange=<exchange>/instrument_type=perp/symbol=<symbol>/depth=<depth>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=options_ticker_snapshot_1m/
    exchange=<exchange>/instrument_type=option/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=option_instrument_ticker_snapshot_1m/
    exchange=<exchange>/instrument_type=option/currency=<currency>/instrument_name=<instrument_name>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=options_l2_snapshot_1m/
    exchange=<exchange>/instrument_type=option/currency=<currency>/instrument_name=<instrument_name>/depth=<depth>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=instrument_metadata_snapshot_daily/
    exchange=<exchange>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=future_instrument_metadata_snapshot_daily/
    exchange=<exchange>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=index_price_snapshot_1m/
    exchange=<exchange>/index_name=<index_name>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=volatility_index_snapshot_1m/
    exchange=<exchange>/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=futures_summary_snapshot_1m/
    exchange=<exchange>/instrument_type=<future|perp>/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=recent_trade_snapshot_1m/
    exchange=<exchange>/instrument_type=<option|future|perp>/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
```

Partitioning uses explicit `year=YYYY/month=MM/date=DD/hour=HH` directories. Writers upsert into one
`data.parquet` file per partition.

Perpetual L2 readers and jobs should only use `dataset_type=perps_l2_snapshot_1m`. Legacy
`dataset_type=l2_snapshot` and `dataset_type=perp_l2_snapshot` paths are historical aliases, not
valid active Bronze roots.

---

# 6. Configuration Model

Configuration source of truth:

- Runtime file: `config.yaml`
- Built-in defaults: `ingestion/config.py`

Key controls:

| Area | Settings |
|---|---|
| HTTP transport | `http.timeout_s`, `http.max_retries`, `http.retry_backoff_s` |
| Logging | `logfile`, `runtime.log_dir`, `runtime.log_rotation_days`, `runtime.log_backup_count` |
| L2 ingestion | `ingestion.symbols`, `ingestion.levels`, `ingestion.snapshot_count`, `ingestion.poll_interval_s`, `ingestion.max_runtime_s` |
| Options summary | `ingestion.options.*` |
| Option instrument ticker | `ingestion.option_instrument_ticker.*` |
| Option L2 order book | `ingestion.option_l2.*` |
| Futures summary | `ingestion.futures_summary.*` |
| Instrument metadata | `ingestion.instrument_metadata.*` |
| Index price | `ingestion.index_price.*` |
| Volatility index | `ingestion.volatility_index.*` |
| Recent trades | `ingestion.recent_trades.*` |

Important behavior:

- `max_runtime_s = 0` disables the L2 runtime budget.
- Options, option instrument ticker, option L2, futures summary, instrument metadata,
  volatility-index, and index-price builders default to saving parquet output.
- REST calls are sequential and bounded by HTTP timeout/retry settings.
- `ingestion.option_instrument_ticker.max_instruments_per_run` caps the selected universe per
  currency for each minute run.
- `ingestion.option_l2.depth` controls option book depth, and
  `ingestion.option_l2.max_instruments_per_run` caps the selected universe per currency for each
  minute run.
- `ingestion.recent_trades.lookback_seconds` controls the restart-safe overlap window for recent
  trade collection.
- Cron should use `flock` for per-instrument ticker, option L2, and recent-trade commands to avoid
  overlapping minute runs.

---

# 7. Example Commands

## 7.1 Bronze Collectors

```bash
python main.py perp-l2-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py options-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py futures-summary-bronze-builder --debug --symbols BTC ETH SOL
python main.py option-instrument-ticker-bronze-builder --debug --symbols BTC ETH SOL --max-instruments-per-run 60
python main.py options-l2-bronze-builder --debug --symbols BTC ETH SOL --depth 20 --max-instruments-per-run 60
python main.py recent-trades-bronze-builder --debug --symbols BTC ETH SOL --kinds option future --count 1000
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind future
python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
python main.py volatility-index-bronze-builder --debug --symbols BTC ETH SOL --resolution 60
```

## 7.2 Validation Utility

```bash
python main.py validate-symbols --debug --symbols BTC ETH SOL
```

## 7.3 Production Cron

```cron
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py perp-l2-bronze-builder --debug --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py options-bronze-builder --debug --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py futures-summary-bronze-builder --debug --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && flock -n .logs/option-instrument-ticker-bronze-builder.cron.lock .venv/bin/python main.py option-instrument-ticker-bronze-builder --debug --symbols BTC ETH SOL --max-instruments-per-run 60
* * * * * cd /home/vcs/git/crypto-live-loader && flock -n .logs/options-l2-bronze-builder.cron.lock .venv/bin/python main.py options-l2-bronze-builder --debug --symbols BTC ETH SOL --depth 20 --max-instruments-per-run 60
* * * * * cd /home/vcs/git/crypto-live-loader && flock -n .logs/recent-trades-bronze-builder.cron.lock .venv/bin/python main.py recent-trades-bronze-builder --debug --symbols BTC ETH SOL --kinds option future --count 1000
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py volatility-index-bronze-builder --debug --symbols BTC ETH SOL --resolution 60
15 3 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
20 3 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind future
*/15 * * * * cd /home/vcs/git/crypto-live-loader && flock -n .logs/bronze-retention.cron.lock .venv/bin/python -m scripts.prune_bronze_by_disk_usage --bronze-lake-root lake/bronze --trigger-percent 93 --target-percent 90
```

## 7.4 Quality Checks

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy .
.venv/bin/pyright --level error
.venv/bin/ty check .
.venv/bin/interrogate .
.venv/bin/pydoclint api ingestion sources domain tests
.venv/bin/pytest -q
.venv/bin/pytest --cov --cov-report=term-missing
```

Serial execution is the default because it is faster on low-core ingestion hosts. Larger development
or CI machines can distribute independent test files across a bounded worker pool:

```bash
make test-parallel PYTEST_WORKERS=4
```

File-level scheduling keeps tests from the same module in one process. Override `PYTEST_ARGS` to run
only the files under active development.

Convenience target:

```bash
make check
```

For a short test-authoring feedback loop, stop on the first failure and run failed tests first:

```bash
make test-fast PYTEST_ARGS=tests/test_target_module.py
```

---

# 8. Operations

## 8.1 Idempotency

Upsert-based datasets merge by natural keys and deterministic sort order:

| Dataset | Natural Key |
|---|---|
| `perps_l2_snapshot_1m` | `exchange`, `instrument_type`, `symbol`, `depth`, `source`, `event_time` |
| `options_ticker_snapshot_1m` | `exchange`, `currency`, `instrument_name`, `source`, `snapshot_time` |
| `option_instrument_ticker_snapshot_1m` | `exchange`, `instrument_name`, `source`, `snapshot_time` |
| `options_l2_snapshot_1m` | `exchange`, `instrument_name`, `source`, `depth`, `exchange_timestamp` |
| `instrument_metadata_snapshot_daily` | `exchange`, `instrument_name`, `snapshot_date` |
| `future_instrument_metadata_snapshot_daily` | `exchange`, `instrument_name`, `snapshot_date` |
| `index_price_snapshot_1m` | `exchange`, `index_name`, `event_time`, `source` |
| `volatility_index_snapshot_1m` | `exchange`, `currency`, `timestamp`, `resolution` |
| `futures_summary_snapshot_1m` | `exchange`, `instrument_name`, `source`, `snapshot_time` |
| `recent_trade_snapshot_1m` | `exchange`, `instrument_name`, `trade_id` |

## 8.2 Observability and Logging

- Every CLI command accepts `--debug`.
- Dataset commands write to dataset-named log files under the configured `.logs` directory.
- Runtime logs use one formatter: `timestamp level module_scope logger message`.
- Current dataset log files:
  - `perps_l2_snapshot_1m.log`
  - `options_ticker_snapshot_1m.log`
  - `option_instrument_ticker_snapshot_1m.log`
  - `options_l2_snapshot_1m.log`
  - `instrument_metadata_snapshot_daily.log`
  - `future_instrument_metadata_snapshot_daily.log`
  - `index_price_snapshot_1m.log`
  - `volatility_index_snapshot_1m.log`
  - `futures_summary_snapshot_1m.log`
  - `recent_trade_snapshot_1m.log`
- Dataset lifecycle messages use a stable key-value envelope with `dataset_type` and `exchange`
  on every dataset event:
  - `job_event command=<command> event=dispatch ...`
  - `job_event command=<command> event=run_start dataset_type=<dataset> exchange=<exchange> ...`
  - `job_event command=<command> event=fetch_complete dataset_type=<dataset> exchange=<exchange> ...`
  - `job_event command=<command> event=normalization_complete dataset_type=<dataset> exchange=<exchange> ...`
  - `job_event command=<command> event=persistence_complete dataset_type=<dataset> exchange=<exchange> ...`
  - `job_event command=<command> event=run_summary ...`
- `--debug` emits request scope, source mappings, raw row counts, normalized row counts, persistence
  paths, and collector-specific timing or window parameters where applicable.
- Log rotation is controlled by `runtime.log_rotation_days` and `runtime.log_backup_count`.

## 8.3 Bronze Disk Retention

`scripts/prune_bronze_by_disk_usage.py` protects the VM from Bronze lake disk pressure. It checks the
filesystem containing `lake/bronze`; when usage reaches `93%`, it deletes the oldest safe Bronze
partition directories until usage is at or below `90%`. The script only considers Hive-style
`lake/bronze/dataset_type=*/.../data.parquet` partitions, supports `--dry-run`, and writes JSON
evidence under `.logs/bronze-retention-evidence`.

Recommended cron:

```cron
*/15 * * * * cd /home/vcs/git/crypto-live-loader && flock -n .logs/bronze-retention.cron.lock .venv/bin/python -m scripts.prune_bronze_by_disk_usage --bronze-lake-root lake/bronze --trigger-percent 93 --target-percent 90
```

## 8.4 Data Quality

Built-in validation and normalization behavior:

- `validate-symbols` checks symbol normalization and top-of-book sanity.
- Option normalizers reject malformed option instrument names.
- SOL option ingestion filters Deribit `USDC` option responses to `SOL_USDC-*`.
- SOL instrument metadata and futures summary ingestion use Deribit `USDC` and filter `SOL_USDC-*`.
- SOL trade ingestion filters Deribit `USDC` trade responses to `SOL_USDC-*`.
- Per-instrument ticker selection requires usable quote, mark, or liquidity information before a
  contract enters the IV/RV prediction universe.
- Option L2 rows preserve the selected universe book depth and observed bid/ask level counts for
  quote-quality filtering.
- Recent trade overlap windows are deduped by trade id during parquet upsert.
- Per-instrument ticker rows preserve raw payload hashes for audit and replay.
- External calls are bounded by configured timeout and retry settings.

---

# 9. Risk Notes and Limitations

The active risk register lives in [`RISKS.md`](RISKS.md). Update it in the same change set as any
dataset coverage, storage, scheduling, data-quality, or observability change that materially changes
operational exposure.

- Deribit is the only supported exchange.
- The repository polls public REST endpoints only; websocket microburst capture is out of scope.
- Local parquet storage is the only lake backend.
- Local file locks protect partition writes but do not provide distributed coordination.
- Per-instrument option ticker and option L2 selection are optimized for IV/RV forecasting, not
  exhaustive option archival of every listed contract every minute.

---

# 10. Roadmap

Durable architecture and operational choices are tracked in [`DECISIONS.md`](DECISIONS.md). Delivered
dataset and runtime milestones are tracked in [`TIMELINE.md`](TIMELINE.md).

- Extract CLI command handlers into focused modules.
- Add websocket collectors for higher-frequency order-book and ticker state.
- Add schema migration regression tooling.
- Add replay utilities for incident analysis and backtest audits.
- Add multi-exchange adapters behind the current source contracts.
