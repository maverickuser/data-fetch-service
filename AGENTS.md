# Agent instructions — data-fetch-service

Active repository guidance. Local implementation includes admission/read APIs, per-job acquisition, complete-event pull orchestration, and processor delivery; see the status file for review status. Do not edit the processing-service LLD; its copy is a read-only external reference.

## Purpose and reading order

This Go/AWS Lambda service acquires complete BSE and NSDL datasets, stores artifacts in S3, and hands a manifest location to a downstream processor. It does not implement downstream financial-data processing.

Read:

1. `docs/README.md` for the documentation map and authority labels.
2. `docs/specs/data-puller-service-lld.md` for current behaviour and architecture.
3. `docs/plans/data-fetch-service-implementation-plan.md` and `implementation-status.md` in the same directory for the stack and verified progress.
4. `docs/specs/data-puller-service-spec.md` for original requirements; the LLD supersedes conflicting historical decisions.
5. `docs/specs/structured-file-processing-spec.md` when working on the processor integration boundary.

Keep current instructions and user-approved changes reflected in the LLD. Do not silently resolve contradictory contracts by inventing behaviour; identify a material ambiguity and ask a focused question when available evidence cannot resolve it.

## Repository map

- `cmd/`: thin Lambda entry points and configuration-check command.
- `internal/domain`, `internal/config`: identities, states, contracts, YAML validation, and template resolution.
- `internal/admission`, `internal/orchestration`, `internal/acquisition`: admission, group execution, source downloads/validation/extraction.
- `internal/state`, `internal/storage`, `internal/delivery`: S3 coordination/artifacts and downstream handoff.
- `internal/api`, `internal/telemetry`: REST adapters, logs, and metrics.
- `config/events.yaml`, `config/environments/prod.yaml`: version-controlled effective configuration.
- `infra/bootstrap`, `infra/service`: separate Terraform backend bootstrap and production runtime resources.
- `internal/awsverify`: `aws`-tagged tests run only by the AWS integration workflow against disposable resources.
- `internal/smoke`, `cmd/smoke`: real-endpoint smoke runner, tested locally against a fake deployment.
- `.github/workflows`: CI checks (`quality.yml`), the manual smoke workflow (`smoke.yml`), the manual shared-network plan/apply (`network.yml`, calling `cloud-platform-network`), the manual staged release (`release.yml`), and the manual disposable-resource AWS integration tests (`aws-integration.yml`); the load workflow is planned, not present.

Only claim a package or feature exists after inspecting the checkout; this map describes the target architecture during incremental implementation.

## Local command contract

Implemented commands:

- `make fmt`: format authored code.
- `make tools`: install pinned staticcheck and errcheck (requires network).
- `make lint`: check-only gofmt, go vet, staticcheck and errcheck.
- `make test-unit`: isolated unit tests with race detection and unit-only coverage profile.
- `make coverage-check`: enforce coverage strictly greater than 95% from that profile.
- `make test-integration`: exercise the real S3 SDK with mocked HTTP, BSE/NSDL event-to-state and source-to-artifact composition, HTTP admission/read routes, and processor delivery with an in-memory HTTP handler. Integration coverage is separate from the unit gate.
- `make build`: build implemented production entry points/packages, including Pull, Delivery, and Reconciler Lambdas.
- `make configcheck`: validate the effective event configuration and print its revision.
- `make check-docs`: validate repository documentation links.
- `make check-contract`: verify supplied OpenAPI JSON/YAML equivalence (Ruby standard library).
- `make check-infra`: Terraform `fmt -check`, `validate`, and mocked `terraform test` for `infra/bootstrap` and `infra/service`; pass `TERRAFORM=` when the pinned 1.16.4 binary is not on `PATH`. Needs network for the provider, no AWS credentials.
- `make package`: build one reproducible `bootstrap`+config ZIP per Lambda under `dist/` and write `dist/release.json` with the configuration revision and base64 SHA-256 of each ZIP.
- `make check`: run all implemented deterministic gates.

Add configuration/Terraform/live-test commands as their implementations land and update this file in the same PR. A documented command must perform its advertised checks; never use success-returning placeholders. Local deterministic checks require no AWS credentials. Real AWS integration and production smoke tests run through the documented GitHub workflows using configured inputs.

## Implementation invariants

- Prioritize readable code: cohesive functions, descriptive names, and straightforward control flow. Add a brief comment immediately above each authored production function/method, starting with its name and explaining its purpose/result. Include caller-relevant side effects or failure/concurrency guarantees when needed. Keep comments current; avoid paraphrasing the code or adding boilerplate. Generated code is excluded; test names/scenario comments and obvious inline callbacks need not repeat themselves. Size/complexity metrics prompt review, not mechanical splitting.
- Normalize CloudEvents/native AWS triggers before domain logic. Use business event identity rather than SQS message IDs for request deduplication.
- Resolve dates once from logical scheduled time in Asia/Kolkata; preserve snapshot dates and inputs through retries.
- BSE selects exactly the configured fgroup CSV and names it `{exchangeName}_fgroup{ddMMyyyy}.csv`; the exchange comes from event inputs. NSDL produces six separate ISIN-prefixed JSON files.
- Never hand off a partial event. Only validated completed S3 objects belong in the manifest.
- Preserve complete-event deduplication against the latest HTTP-202-accepted baseline. Only 202 means accepted; no callback/polling for processor business completion.
- Keep source attempts, delivery attempts, automatic execution retries, and AWS SDK retries distinct. Pull crash/timeout allows three retries after the initial execution, four executions total; BSE 404 and other business failures do not enter that retry chain.
- Preserve conditional ownership/generation fencing, immutable history, durable transition/dispatch intents, and recoverability across every write boundary. Never overwrite coordination blindly or allow overlapping authoritative owners.
- Keep bounded streaming validation, temporary-storage limits, cancellation, and cleanup. Do not replace these with unbounded buffering.
- Retain the accepted fingerprint beyond artifact expiry. No success or live-readiness claims without evidence.

## Test and review expectations

Read `docs/engineering/go-standards.md` and `docs/engineering/pr-review.md` once introduced in foundation PR 01. Apply the explicitly adopted language rules, provide evidence for review findings, and distinguish required fixes from nonblocking preferences. The agreed workflow uses separate implementer-agent and reviewer-agent passes; no human approval is required for local PR review. Do not claim a GitHub approval by a human or fabricate review identity.

Add meaningful unit and mocked integration tests with every behavioural change. Unit statement coverage must be strictly greater than 95% over all authored production packages, including adapters and packages without tests. Do not round up, exclude difficult code, or mix integration coverage into this result. Preserve only explicit generated/vendor/test exclusions.

Test externally observable outcomes and interruption/retry paths. Use injected clocks, IDs, and boundary fakes for deterministic recovery tests. Use actual handlers and business logic in API integration tests. Real-endpoint smoke tests must use the actual configured sources and processor; mocks or missing configuration cannot produce a successful smoke result.

Inspect the current branch, worktree, applicable nested guidance, and implementation status before editing. Preserve unrelated changes. Run checks appropriate to the PR and report what ran, what passed, and what could not run.

## Stacked PR workflow

Follow the dependency order in the implementation plan. Each child PR targets its parent branch until that parent merges. Keep the diff focused on the increment, identify its dependency, and include test evidence and any migration/operational effect in the PR description.

Merge bottom-up and rebase/retarget children carefully. Rerun required checks on rewritten heads. Do not replay squashed parent commits. Keep implementation-status entries accurate and link actual PRs; do not invent links or mark planned work complete.

Production deployment activates only after the full implementation and release workflow is ready. Perform implementation, agent review/fixes, and deterministic tests locally. Do not apply Terraform or require live AWS resources for intermediate PRs. After the complete stack is locally verified, GitHub Actions CD can run from `main` and assume the dedicated AWS role through OIDC using the `AWS_ROLE_TO_ASSUME` secret. Role ARN, processor integration, and fixtures are deployment inputs. Queue ARN/URL come from this service's Terraform outputs. Keep secrets out of source and logs, and keep destructive test fixtures separate from production data.

## Keeping guidance current

Update canonical specs, examples, commands, and status when behaviour changes. Store substantial new decisions in `docs/decisions/` and link them from the LLD. Keep this file concise by linking detailed designs rather than copying them. Add scoped guidance only for real directory-specific requirements.
