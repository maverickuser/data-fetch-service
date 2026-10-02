# PR 09b — Retry delivery from retained artifacts

Base: `stack/09-full-rerun`. Branch: `stack/09-delivery-retry-api`.

When a run fails during processor delivery, `POST /v1/runs/{run_id}/delivery-retries` creates a linked child run with a new manifest pointing to the original complete files. The Pull worker verifies those files and dispatches delivery without making source requests. The request is idempotent; an active run, a non-delivery failure, or a newer different accepted dataset produces a conflict. Missing retained files produce HTTP 410 so the caller can choose a full rerun. An explicit `use_current_processor_config` choice changes only the pinned processor profile and records both destinations and revisions.

The coordinator checks the observed acceptance baseline at admission, preventing a stale retry from racing another acceptance. Another delivery retry can follow a failed retry; each child manifest keeps the original raw-file provenance, and reusing the same key resolves to that child. The reconciler does not automatically admit an unreserved manual retry intent, does not turn an interrupted delivery-only handoff into an unchanged-data skip, and fails a terminated delivery-only worker without starting an automatic source retry. Runtime wiring grants the API and Pull Lambdas bounded manifest reads and retained-file HEAD checks; PR 10 must include the corresponding scoped S3 permissions.

Validation: `make check` passes, including race/unit coverage 3128/3292 statements (95.02%), mocked HTTP-to-state-to-worker integration with retry-of-retry, interrupted-handoff recovery, lint, native and Linux/arm64 builds, configuration, documentation links, and OpenAPI parity. No AWS resources were deployed and no live endpoints were called.
