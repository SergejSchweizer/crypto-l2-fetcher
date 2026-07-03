# Risks

Last updated: 2026-07-02

This file tracks active product, data, and operational risks for `crypto-live-loader`.
Update it in the same pull request whenever a change affects dataset coverage, data correctness,
runtime reliability, storage layout, or operational observability.

## Update Rules

- Add new risks before merging features that introduce new data contracts, cron jobs, storage paths,
  external API use, or operational assumptions.
- Close or downgrade risks only when the mitigation is implemented and validated.
- Keep each risk concrete enough to test, monitor, or document.
- Link material architecture or naming decisions in `DECISIONS.md`.
- Reflect delivered mitigation work in `TIMELINE.md`.

## Active Risks

| ID | Severity | Area | Risk | Current Mitigation | Status |
|---|---|---|---|---|---|
| R-001 | Medium | Exchange coverage | Deribit is the only supported exchange, so downstream models inherit single-venue liquidity and outage exposure. | Source adapters isolate exchange-specific behavior for future multi-exchange support. | Open |
| R-002 | Medium | Capture fidelity | Public REST polling can miss websocket-level microstructure bursts and intra-minute queue changes. | Bronze datasets preserve raw REST snapshots, trade tape, and L2 depth with explicit timestamps. | Accepted |
| R-003 | Medium | Coordination | Local file locks protect writes on one host but do not provide distributed writer coordination. | Cron uses `flock` for long-running collectors and parquet writers perform deterministic upserts. | Open |
| R-004 | Medium | Option universe sampling | Per-instrument option ticker and option L2 collectors cap instruments per run, so they are IV/RV feature feeds rather than exhaustive full-chain archival feeds. | Selection keeps contracts with usable quote, mark, or liquidity context and documents run limits. | Accepted |
| R-005 | Medium | SOL source mapping | Logical SOL option, futures, metadata, and trade requests use Deribit `USDC` source data and filter `SOL_USDC-*`; mapping mistakes can mix currencies. | Normalizers preserve `requested_currency` and `source_currency`; tests cover SOL/USDC filtering. | Open |
| R-006 | Low | Local storage | Local parquet is easy to inspect but has no built-in remote backup, lifecycle management, or object-store durability. | Lake root is explicit and ignored by git; storage layout is documented and deterministic. | Open |
| R-007 | Low | Dataset naming drift | Renames can leave stale directories, docs, cron references, or log names behind. | Canonical dataset names are documented, tests cover critical paths, and logs use dataset-named files. | Monitoring |

## Recently Mitigated

| Date | Risk | Mitigation |
|---|---|---|
| 2026-07-02 | Perpetual L2 naming ambiguity | Renamed canonical dataset surface to `perps_l2_snapshot_1m` and aligned code, docs, tests, migration tooling, and logs. |
| 2026-07-02 | Dataset log ambiguity | Runtime logs for dataset commands now write to dataset-named files under `.logs`. |
| 2026-07-02 | Option L2 coverage gap | Added `options_l2_snapshot_1m` collector and cron coverage for selected option book depth. |
