# PR 09a — Full rerun from a pinned snapshot

Base: `stack/08-recovery`. Branch: `stack/09-full-rerun`.

A failed or completed run can be rerun without changing the resolved trade date, ISIN, job URLs, filenames, or admitted configuration. `POST /v1/runs/{run_id}/reruns` creates a linked child with a fresh request identity, accepts an optional `force` value, and uses the normal conditional admission and pull dispatch protocol. An idempotency-key replay returns the same child; a different request cannot start another run while that execution key is active. The API rejects extra body fields and oversized input.

Validation: `make check` passes with 2847/2993 unit statements covered (95.12%), race tests, mocked HTTP-to-state integration, lint, native/Linux arm64 builds, configuration validation, docs, and OpenAPI parity. The integration test confirms the child retains the original BSE URL and logical date even when admission occurs later. Delivery-only retry and its artifact-expiry/stale-baseline checks follow in PR 09b. No remote PR or AWS resources were created.
