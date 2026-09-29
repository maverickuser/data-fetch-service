# PR 03: state storage and coordination

Base: `stack/02-contracts`. Branch: `stack/03-state`. Increment remains in progress.

The core implementation creates immutable records conditionally and verifies exact bytes on collisions or ambiguous write errors. Mutable coordination uses opaque ETags; conflicts and ambiguous CAS responses require callers to reread and reevaluate. It never blindly retries an update.

Phase ownership uses run ID, generation, invocation token and a hard deadline plus grace supplied by the caller. Expired owners cannot be stolen directly. Transitions reserve their complete payload before immutable history creation, then finalize conditionally. Explicit recovery completes pending intent; ordinary reads do not mutate state. Completion persists acceptance evidence before history and advances the retained baseline during finalization. Delivery-pending releases invocation ownership while preserving the active run.

The S3 SDK adapter bounds metadata reads, closes bodies, preserves provider diagnostics and uses conditional request headers. Unit tests cover concurrent immutable writers, stale CAS, retention, pagination and interruption before/after transition writes. Separate mocked integration exercises the actual SDK serializer/error decoder with a fake HTTP transport. No cloud resources are used.

Remaining in this increment: recoverable admission/request resolution and dispatch intents, run projection, and composition tests. Automatic retry transfer belongs to the later recovery increment and must retain the claim across parent/child creation; ordinary failed-transition release cannot substitute for it. State primitives alone do not constitute a working ingestion service.
