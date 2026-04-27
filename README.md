# L2 Synchronizer

## 1. Project Overview

This repository provides a modular framework for ingesting crypto market data with emphasis on reproducibility and production quality.

Current implemented scope (Step 1):
- Pull BTC/ETH spot OHLCV data from Binance public API.
- Expose a CLI command for repeatable fetch runs.

## 2. Architecture Diagram

```text
CLI -> Ingestion Adapter -> HTTP Client -> Exchange REST API -> (next step) Database
```

## 3. Installation Guide

### 3.1 Prerequisites

- Python 3.11+

### 3.2 Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 4. Dependency Setup

No runtime external dependencies are required for Step 1.

## 5. Module Explanations

- `ingestion/http_client.py`: lightweight JSON HTTP utilities.
- `ingestion/spot.py`: spot candle schema and Binance spot fetch logic.
- `api/cli.py`: CLI command registration and output formatting.
- `infra/`: shared domain and time window utilities for upcoming steps.

## 6. Execution Workflow

Fetch latest BTC/ETH spot candles:

```bash
python3 main.py fetch-spot --symbols BTCUSDT ETHUSDT --timeframe H1 --limit 5
```

List all currently supported spot timeframes:

```bash
python3 main.py list-spot-timeframes
```

## 7. Testing Instructions

```bash
pytest
ruff check .
mypy .
```

## 8. Deployment Instructions

- For now this is a local CLI tool.
- Next stage will add scheduled runs and database persistence.

## 9. Known Limitations

- Step 1 currently supports spot candles only.
- No database persistence yet.
- No exchange failover yet.

## 10. Future Improvements

- Add perpetual and funding endpoints.
- Add Deribit adapter for L2 order book snapshots.
- Add full-update vs last-N-days database ingestion mode.
