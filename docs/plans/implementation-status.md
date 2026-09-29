# Implementation status

Branch: `stack/04-admission`. No remote PR or AWS resources created. Foundation committed as `e4bbe13`; its separate agent review covered staged content, not the committed head. PR 02 is approved through `cd31c10` and PR 03 through `8763839`; exact-head local review records are under `.reports/`.

| Increment | Status |
|---|---|
| 01 Foundation | Implemented locally; separate agent reviewed staged content with no blockers |
| 02 Configuration and event contracts | Implemented locally; current committed-head review verdict is recorded under `.reports/` |
| 03 State and coordination | Implemented locally; committed-head review evidence under `.reports/` |
| 04 Admission and initial REST reads | Implemented locally; committed-head review evidence under `.reports/` |
| 05–12 | Planned; not implemented |

Verified locally: gofmt/vet/staticcheck/errcheck; race/unit coverage greater than 95%; weighted-coverage regression checks; native and Linux/arm64 builds; configuration validation; documentation links; OpenAPI JSON/YAML parity. Current reports are in `.reports/`. CI YAML is prepared but has not run on GitHub.

PR 02 includes strict YAML overlays, pinned configuration revisions, safe template resolution, CloudEvents/SQS/native EventBridge normalization, resolved immutable JSON snapshots, and request/execution identities. Production BSE and six NSDL jobs are configured. PR 04 loads approved native mappings from `config/native-events.yaml`; its empty default enables no native producers. Full AWS schedule-expression validation belongs to Terraform in PR 10. The disabled processor target is an explicit placeholder until the endpoint is provided.

PR 03 core includes a pinned S3 SDK adapter, bounded metadata reads, conditional creates/CAS, exact immutable collision checks, retention-aware reads and paginated scans. Fenced leases and reserve/history/finalize transitions survive interruptions; acceptance evidence is persisted before completed history and baseline release. Tests inject failures before and after each write. `make test-integration` uses the real SDK over a mocked HTTP transport; its coverage is separate from the unit gate.

PR 03 now includes serialized admission/request resolution, durable pull/delivery dispatch, idempotent phase claims, read-only history/run projections and UTC listing records. Unit tests cover all admission write boundaries, concurrent joins, terminal-release races, and lost publish/claim responses. Separate BSE/NSDL composition tests exercise configuration, normalization, snapshots, admission and dispatch together. Verified unit coverage: 785/813 statements (96.56%).

PR 04 adds API/admission Lambda entry points, SDK SQS publishing, partial batch failures with durable invalid-event receipts, manual force/idempotency, and the initial read routes. First-request races rebind to the winning request's configuration, preserving the original manual logical date or external producer time. HTTP integration tests cover races across midnight/deployments, queue-reply loss, request replay/conflicts, read routes and listing pagination. Latest unit coverage: 1242/1284 statements (96.73%); integration coverage is excluded.

Next: PR 05 acquisition and artifact storage. Pull/delivery workers, automatic retry orchestration, manual recovery routes, Terraform and deployment remain later work. Automatic retry transfer must preserve the active claim using its own durable intent; ordinary failed transitions release it and must not be used as the first step of automatic retry. Live AWS/source tests are deferred until release.
