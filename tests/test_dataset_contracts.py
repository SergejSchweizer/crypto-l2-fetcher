"""Dataset contract registry tests."""

from __future__ import annotations

from pathlib import Path

from api.logging_common import DATASET_LOG_SCOPES
from domain.datasets import (
    BRONZE_DATASET_CONTRACTS,
    BRONZE_DATASET_TYPES,
    bronze_contract_by_dataset_type,
)
from ingestion.futures_summary import FUTURES_SUMMARY_DATASET_TYPE
from ingestion.index_price import INDEX_PRICE_DATASET_TYPE
from ingestion.instrument_metadata import (
    FUTURE_INSTRUMENT_METADATA_DATASET_TYPE,
    INSTRUMENT_METADATA_DATASET_TYPE,
)
from ingestion.l2 import PERPS_L2_DATASET_TYPE
from ingestion.option_instrument_ticker import OPTION_INSTRUMENT_TICKER_DATASET_TYPE
from ingestion.option_l2 import OPTION_L2_DATASET_TYPE
from ingestion.options import OPTION_TICKER_DATASET_TYPE
from ingestion.recent_trades import RECENT_TRADE_DATASET_TYPE
from ingestion.volatility_index import VOLATILITY_INDEX_DATASET_TYPE

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_bronze_dataset_contracts_are_unique() -> None:
    """Keep canonical dataset names and log stems one-to-one."""

    dataset_types = [contract.dataset_type for contract in BRONZE_DATASET_CONTRACTS]
    log_stems = [contract.log_stem for contract in BRONZE_DATASET_CONTRACTS]

    assert len(dataset_types) == len(set(dataset_types))
    assert len(log_stems) == len(set(log_stems))
    assert BRONZE_DATASET_TYPES == frozenset(dataset_types)


def test_ingestion_dataset_constants_are_registered() -> None:
    """Keep dataset constants used by writers and commands in the registry."""

    assert {
        PERPS_L2_DATASET_TYPE,
        OPTION_TICKER_DATASET_TYPE,
        OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        OPTION_L2_DATASET_TYPE,
        INSTRUMENT_METADATA_DATASET_TYPE,
        FUTURE_INSTRUMENT_METADATA_DATASET_TYPE,
        INDEX_PRICE_DATASET_TYPE,
        VOLATILITY_INDEX_DATASET_TYPE,
        FUTURES_SUMMARY_DATASET_TYPE,
        RECENT_TRADE_DATASET_TYPE,
    } == BRONZE_DATASET_TYPES


def test_logging_scopes_match_dataset_contract_log_stems() -> None:
    """Keep module logfiles aligned with canonical dataset contracts."""

    assert DATASET_LOG_SCOPES == frozenset(contract.log_stem for contract in BRONZE_DATASET_CONTRACTS)


def test_readme_mentions_every_bronze_dataset_contract() -> None:
    """Keep operational docs aligned with the canonical Bronze registry."""

    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    for contract in BRONZE_DATASET_CONTRACTS:
        assert f"dataset_type={contract.dataset_type}" in readme or f"`{contract.dataset_type}`" in readme
        assert f"`{contract.log_stem}.log`" in readme


def test_contract_lookup_rejects_unknown_dataset_type() -> None:
    """Fail fast when a caller asks for an unregistered dataset contract."""

    assert bronze_contract_by_dataset_type(PERPS_L2_DATASET_TYPE).dataset_type == PERPS_L2_DATASET_TYPE

    try:
        bronze_contract_by_dataset_type("unknown_snapshot_1m")
    except KeyError as exc:
        assert "unknown_snapshot_1m" in str(exc)
    else:
        raise AssertionError("expected unknown dataset lookup to fail")
