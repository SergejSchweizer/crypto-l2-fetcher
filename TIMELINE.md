# Timeline

Last updated: 2026-07-02

This file records notable repository milestones that affect data coverage, runtime behavior,
operational contracts, or downstream compatibility. Update it in the same pull request as the
change it describes.

## 2026-07-02

| Change | Impact |
|---|---|
| Aligned runtime log files with dataset names. | Dataset command logs now use files such as `.logs/perps_l2_snapshot_1m.log` and `.logs/options_l2_snapshot_1m.log`, making operations and incident review easier. |
| Renamed perpetual L2 canonical dataset to `perps_l2_snapshot_1m`. | Code, docs, tests, migration tooling, local Bronze directory naming, and log naming now use the plural `perps` dataset schema consistently. |
| Added and scheduled `options_l2_snapshot_1m`. | The live Bronze set now captures selected option L2 order-book depth for BTC, ETH, and SOL via REST. |
| Shared Deribit public response handling. | REST source modules now use common response parsing behavior for more consistent failure handling. |
| Shared CLI parser option helpers. | CLI command definitions reuse parser helpers for common flags and dataset controls. |
| Shared Bronze lake writer plumbing. | Parquet upsert behavior is more consistent across Bronze writers. |

## Earlier Milestones

| Change | Impact |
|---|---|
| Added IV/RV reliability feeds. | Expanded Bronze coverage with option ticker, futures summary, volatility index, index price, metadata, and recent trade context. |
| Unified dataset debug logging. | `--debug` logs now emit expressive dataset envelopes for diagnosis. |
| Documented Bronze dataset columns. | README became the canonical data-contract overview for raw Bronze outputs. |
| Added recent trades Bronze builder. | Trade tape became available for option, future, and perpetual flow features. |

## Maintenance Expectations

- Add a row for every merged feature or refactor that changes dataset coverage, schema, naming,
  storage behavior, cron behavior, or operational diagnostics.
- Keep entries outcome-oriented; implementation detail belongs in PRs and code comments.
- Cross-link lasting tradeoffs in `DECISIONS.md`.
- Cross-link open operational exposure in `RISKS.md`.
