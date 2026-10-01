# Implementation status

Branch: `stack/06-pull`. No remote PR or AWS resources created. Foundation committed as `e4bbe13`; its separate agent review covered staged content, not the committed head. PR 02 is approved through `cd31c10`, PR 03 through `8763839`, PR 04 through `eefd677`, and PR 05 through `0d8cd94`; exact-head local review records are under `.reports/`.

| Increment | Status |
|---|---|
| 01 Foundation | Implemented locally; separate agent reviewed staged content with no blockers |
| 02 Configuration and event contracts | Implemented locally; current committed-head review verdict is recorded under `.reports/` |
| 03 State and coordination | Implemented locally; committed-head review evidence under `.reports/` |
| 04 Admission and initial REST reads | Implemented locally; committed-head review evidence under `.reports/` |
| 05 Acquisition and artifact storage | Implemented locally; independent committed-head review approved under `.reports/` |
| 06 Complete-event pull orchestration | Implemented locally; independent review pending |
| 07–12 | Planned; not implemented |

Verified locally: gofmt/vet/staticcheck/errcheck; race/unit coverage greater than 95%; weighted-coverage regression checks; native and Linux/arm64 builds; configuration validation; documentation links; OpenAPI JSON/YAML parity. Current reports are in `.reports/`. CI YAML is prepared but has not run on GitHub.

PR 02 includes strict YAML overlays, pinned configuration revisions, safe template resolution, CloudEvents/SQS/native EventBridge normalization, resolved immutable JSON snapshots, and request/execution identities. Production BSE and six NSDL jobs are configured. PR 04 loads approved native mappings from `config/native-events.yaml`; its empty default enables no native producers. Full AWS schedule-expression validation belongs to Terraform in PR 10. The disabled processor target is an explicit placeholder until the endpoint is provided.

PR 03 core includes a pinned S3 SDK adapter, bounded metadata reads, conditional creates/CAS, exact immutable collision checks, retention-aware reads and paginated scans. Fenced leases and reserve/history/finalize transitions survive interruptions; acceptance evidence is persisted before completed history and baseline release. Tests inject failures before and after each write. `make test-integration` uses the real SDK over a mocked HTTP transport; its coverage is separate from the unit gate.

PR 03 now includes serialized admission/request resolution, durable pull/delivery dispatch, idempotent phase claims, read-only history/run projections and UTC listing records. Unit tests cover all admission write boundaries, concurrent joins, terminal-release races, and lost publish/claim responses. Separate BSE/NSDL composition tests exercise configuration, normalization, snapshots, admission and dispatch together. Verified unit coverage: 785/813 statements (96.56%).

PR 04 adds API/admission Lambda entry points, SDK SQS publishing, partial batch failures with durable invalid-event receipts, manual force/idempotency, and the initial read routes. First-request races rebind to the winning request's configuration, preserving the original manual logical date or external producer time. HTTP integration tests cover races across midnight/deployments, queue-reply loss, request replay/conflicts, read routes and listing pagination. Latest unit coverage: 1242/1284 statements (96.73%); integration coverage is excluded.

PR 05 adds bounded HTTPS downloads, streaming CSV/JSON validation, guarded ZIP selection, temporary-disk reservations, per-job attempt recording/retries, and conditional S3 multipart artifact storage. Integration tests exercise BSE/NSDL through the real SDK with mocked HTTP and verify invalid JSON aborts without completing an object. Latest unit coverage: 1849/1937 statements (95.46%); all `make check` gates pass. See [the local PR description](acquisition-pr.md). Independent review fixes include ZIP allocation bounds, cancellation, local-write classification, and cleanup accounting. Independent worktree review passed; the committed-head verdict is tracked separately under `.reports/`.

PR 06 adds a Pull Lambda, complete-event group coordination, per-job result records, CloudEvents manifests, accepted-baseline fingerprint comparison, skip/force decisions, and durable delivery dispatch. `make check` currently passes 2091/2197 unit statements (95.17%) and the existing mocked integration/build/config/contract gates. See [the local PR description](pull-pr.md). Independent review is pending.

Next: PR 07 processor delivery and acceptance after PR 06 review. Automatic retry orchestration, manual recovery routes, Terraform and deployment remain later work. Automatic retry transfer must preserve the active claim using its own durable intent; ordinary failed transitions release it and must not be used as the first step of automatic retry. Live AWS/source tests are deferred until release.
