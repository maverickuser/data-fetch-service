# PR 03: state storage and coordination

Base: `stack/02-contracts`. Branch: `stack/03-state`. Implemented locally; exact-head review evidence is retained under `.reports/`.

The core implementation creates immutable records conditionally and verifies exact bytes on collisions or ambiguous write errors. Mutable coordination uses opaque ETags; conflicts and ambiguous CAS responses require callers to reread and reevaluate. It never blindly retries an update.

Phase ownership uses run ID, generation, invocation token and a hard deadline plus grace supplied by the caller. Expired owners cannot be stolen directly. Transitions reserve their complete payload before immutable history creation, then finalize conditionally. Explicit recovery completes pending intent; ordinary reads do not mutate state. Completion persists acceptance evidence before history and advances the retained baseline during finalization. Delivery-pending releases invocation ownership while preserving the active run.

The S3 SDK adapter bounds metadata reads, closes bodies, preserves provider diagnostics and uses conditional request headers. Unit tests cover concurrent immutable writers, stale CAS, retention, pagination and interruption before/after transition writes. Separate mocked integration exercises the actual SDK serializer/error decoder with a fake HTTP transport. No cloud resources are used.

Admission pins the first request candidate, reserves a serialized join/new-run decision, and persists snapshot/history/UTC listing/resolution before clearing the admission intent. A second receipt read under the coordination ETag closes stale-reader races. Terminal release waits for pending admission decisions.

Durable dispatch precedes queue publication. Consumers atomically claim the correct phase, reject late pull messages during delivery, and recover a lost claim response using the same invocation token/deadline. Read-only projections derive state from immutable history and enforce retention and bounded page sizes.

Validation: `make check` passed, including 785/813 unit statements (96.56%), race tests, separate mocked SDK and BSE/NSDL composition integration, native/arm64 builds, configuration/docs checks and OpenAPI parity. Tests inject failures before/after every admission and transition write and cover concurrent joins/terminal release and ambiguous queue outcomes.

PR 04 adds handlers and queue adapters. Automatic retry transfer belongs to the later recovery increment and must retain the claim across parent/child creation; ordinary failed-transition release cannot substitute for it. State primitives alone do not constitute a working ingestion service.
