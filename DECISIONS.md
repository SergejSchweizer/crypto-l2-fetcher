# Decisions

Last updated: 2026-07-02

This file records durable technical and operational decisions. Update it when a pull request changes
dataset naming, contracts, architecture boundaries, scheduling assumptions, storage semantics, or
observability conventions.

## Decision Log

| ID | Date | Decision | Status | Consequences |
|---|---|---|---|---|
| D-001 | 2026-07-02 | Keep this repository Bronze-only. | Active | Silver and Gold feature engineering remain downstream concerns; this repo focuses on raw, replayable, restart-safe capture. |
| D-002 | 2026-07-02 | Use Deribit public REST APIs as the live ingestion interface. | Active | Collectors are simpler and cron-friendly, but websocket-level microstructure bursts are out of scope. |
| D-003 | 2026-07-02 | Use canonical dataset names as storage, log, and documentation anchors. | Active | Dataset commands write dataset-named logs, Bronze partitions use `dataset_type=<dataset>`, and docs should avoid command-name aliases when discussing stored data. |
| D-004 | 2026-07-02 | Name the perpetual L2 dataset `perps_l2_snapshot_1m`. | Active | Plural `perps` distinguishes the stored perpetual dataset from option L2 and prevents generic `l2_snapshot` ambiguity. |
| D-005 | 2026-07-02 | Track option L2 as `options_l2_snapshot_1m` using selected instruments, not exhaustive full-chain archival coverage. | Active | The dataset is suitable for IV/RV feature context and quote-quality filters, while run caps remain explicit. |
| D-006 | 2026-07-02 | Preserve logical SOL requests while sourcing Deribit `USDC` endpoints where required. | Active | Rows carry requested/source currency metadata so downstream joins can audit SOL/USDC mapping. |
| D-007 | 2026-07-02 | Use local parquet Bronze storage with deterministic upserts. | Active | Local operation stays inspectable and reproducible; distributed coordination and remote durability are separate future work. |
| D-008 | 2026-07-02 | Keep debug logging expressive and dataset-scoped. | Active | `--debug` should expose request scope, source mappings, row counts, persistence paths, and collector timing or window parameters. |

## Decision Update Rules

- Add a new decision when a change creates a lasting convention or contract.
- Supersede, rather than delete, decisions that are no longer valid.
- Link related risks in `RISKS.md` when a decision intentionally accepts operational exposure.
- Reflect delivered decision outcomes in `TIMELINE.md`.
