# Spot Market Data Ingestion Baseline for BTC/ETH (Binance Public API)

## Abstract
This report documents the first implementation stage of a modular crypto data ingestion framework designed for research and production workflows. The immediate objective is to establish a reproducible baseline pipeline to fetch BTC/ETH spot market candles, including prices and traded volume, through public REST endpoints. We implement a typed ingestion adapter for Binance spot klines and expose the workflow through a CLI command that supports configurable symbols, interval, and request depth. The current dataset source is Binance spot market data queried on demand; each run returns recent OHLCV observations for specified trading pairs. Findings at this stage are engineering-focused: the pipeline successfully normalizes exchange payloads into typed records with deterministic schema and validates key edge conditions (for example request limits). The contribution of this stage is a maintainable ingestion foundation that can be extended to Deribit L2 order books, perpetual contracts, funding rates, and database-backed full/backfill update modes in later iterations.

## Introduction
Crypto market research systems require reliable and repeatable ingestion layers before advanced modeling can be trusted.

Many prototype systems fail because they tightly couple data pulls, transformation logic, and ad hoc scripts.

We propose a modular ingestion baseline with strict typing, explicit interfaces, and command-line reproducibility.

This stage contributes: (1) a production-oriented spot ingestion module, (2) a reusable CLI workflow, and (3) test coverage for parsing and input validation.

## Literature Review
Initial stage. Full quantitative literature synthesis will be added once modeling and evaluation components are implemented.

## Dataset
- Source: Binance public REST API (`/api/v3/klines`).
- Sample period: rolling recent candles requested at runtime.
- Number of observations: user-defined via `--limit`.
- Variables: open, high, low, close, base volume, quote volume, trade count.
- Cleaning methodology: typed parsing and schema normalization.
- Train/test split: not applicable in baseline ingestion-only stage.

## Methodology
- Retrieve raw kline arrays from public endpoint.
- Map ordered fields into strongly typed `SpotCandle` records.
- Convert timestamps from epoch milliseconds to UTC datetime.
- Enforce request constraints (`1 <= limit <= 1000`).

## Results
Results tables and figures are not yet included because this stage is ingestion infrastructure only.

## Discussion
The baseline demonstrates that exchange data can be ingested through a deterministic, testable interface. Current limitations include single-source spot focus and lack of persistence.

## Conclusion
Step 1 establishes a maintainable ingestion foundation for BTC/ETH spot price-volume data and enables immediate extension toward L2, perpetual, funding, and database synchronization workflows.

## Appendix
None for this stage.
