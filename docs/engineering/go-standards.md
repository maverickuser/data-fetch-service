# Reusable Go Engineering Standards

Version: 1.0. Date: 2026-09-27.

## Adoption and authority

This repository-independent specification defines readable Go code, package structure, tests, agent review, and incremental delivery. Copy a versioned copy to `docs/engineering/go-engineering-standards-spec.md` in each adopting repository. Root `AGENTS.md` links it, the repository's LLD, and a short repository-specific review policy. Record adopted version and any explicit exceptions in that policy; update copies through reviewed PRs rather than silently following a moving external document.

The LLD owns business behaviour and architecture. This spec owns common engineering practice. Repository-specific instructions resolve explicit differences and must not silently weaken agreed test or correctness requirements. Current user instructions take precedence. Links to Google/Uber/Apache explain influences; this document makes no claim of organizational certification.

## Readability and documentation

- Make code clear to read: descriptive names, cohesive functions, straightforward control flow, and explicit state changes. Prefer early returns when they reduce nesting.
- Put a short Go-style comment immediately above every authored production function/method, including unexported helpers. Start with its name and describe purpose/result. Include caller-relevant side effects, failure behaviour, idempotency, or concurrency guarantees when necessary.
- Keep comments accurate and explain contracts or non-obvious reasons, not each line. Generated code is excluded; descriptive test names/scenario comments and obvious inline callbacks do not need redundant boilerplate.
- Function length, file size, parameter count, complexity, and nesting are review signals. No universal hard size limit is imposed: split responsibilities when it improves comprehension, not merely to satisfy a metric. A repository may adopt explicit advisory thresholds.
- Public API documentation includes inputs, outputs, error/status semantics, examples, and compatibility expectations. Avoid vague comments such as “Handle handles input.”

## Code structure and design patterns

- Keep command/transport entry points thin; application services coordinate use cases; domain code expresses business rules; adapters implement external storage, queues, and HTTP calls.
- Dependencies should point toward application/domain contracts. Domain code must not depend on Lambda events, HTTP handlers, or an SDK implementation unless the domain genuinely requires that type.
- Use small interfaces at the consuming boundary where multiple implementations or deterministic tests require them. Inject collaborators through constructors; avoid global service locators and hidden I/O in initialization.
- Prefer cohesive packages named for responsibilities. Avoid circular dependencies, generic `utils` dumping grounds, duplicate implementations, and oversized packages spanning unrelated responsibilities.
- Use adapters, explicit state machines, strategies, or storage interfaces only when they solve a concrete problem. Do not introduce a framework, factory hierarchy, interface per struct, or abstraction solely to claim pattern compliance.
- Prefer the standard library when adequate. A dependency needs a clear purpose, a pinned version, and a maintained compatibility/security path.

## Go correctness and resource handling

- Use gofmt and consistent imports; adopt documented Go naming, receiver, and error conventions.
- Return meaningful errors, preserve causes when wrapping, and use typed/sentinel comparisons where callers must classify failures. Handle/log errors at deliberate boundaries; normal input failures must not panic or terminate a process.
- Pass context explicitly into cancellable operations. Propagate deadlines, cancel work, and wait for owned goroutines to stop. Bound worker counts, buffers, retries, memory, disk, and parsing.
- Define ownership of mutable data. Protect shared state appropriately and avoid leaking mutable internal slices/maps across boundaries unintentionally.
- Close response bodies/files and release locks; abort unfinished uploads. Test cleanup paths and cancellation. Avoid defers inside long-lived loops when that retains resources unnecessarily.
- Keep retry layers and budgets explicit. Do not add hidden retries, assume exactly-once delivery, or use sleeps as correctness synchronization.
- Measure performance claims. Optimise cost/latency against the repository's objective; do not assume lower memory or concurrency always reduces cost.

## Test strategy and gates

1. **Unit:** deterministic, isolated behaviour tests with injected clocks/IDs/external interfaces. Run race checks where applicable. Require aggregate statement coverage strictly greater than 95% from unit tests alone, instrumenting all authored production packages including adapters and packages with no tests. Calculate weighted covered/total statements without rounding. Only explicit generated/vendor/test exclusions are allowed.
2. **Mocked integration/API:** run actual routing/handlers and application/domain logic together; mock external boundaries. Assert API schemas/statuses, persisted effects, invalid inputs, conflicts, retries, cancellation, duplicate handling, and partial failures. Do not replace domain logic with mocks.
3. **Real infrastructure:** validate provider-specific semantics, permissions, conditional writes, queues, and failure recovery in isolated disposable resources. Local emulation is useful evidence but not proof of actual cloud behaviour.
4. **Live smoke:** exercise deployed code against real configured dependencies and assert stable contracts and meaningful success. Do not compare changing business values with static fixtures. Missing dependencies or configuration must be reported as deferred/blocked before deployment, or failed when the release gate is run—not passed.
5. **Load/resource:** validate representative sizes, bursts, deadlines, and limits before production tuning. Keep disruptive tests away from production resources.

Tests accompany every behavioural PR. Test externally meaningful behaviour and difficult branches rather than mirroring implementation. Coverage is a floor, not proof of correctness. Store reports on success and failure and use the same deterministic commands locally and in CI. Repository-specific fixture choice, fallback rules, API contracts, and cloud integrations belong in its LLD.

## Local agent review and correction loop

Default adoption profile: agent-only approval, with an implementer and a separate reviewer session/context. A repository may explicitly adopt another approval model. No human approval is required by this profile.

1. Implement the next scoped branch, self-review its diff, and run applicable local format/lint/build/unit/coverage/integration checks.
2. Prepare or raise the PR against its parent branch. Record base and head commit IDs. If no GitHub PR is authorized yet, prepare the equivalent local PR description and diff; do not invent a PR URL.
3. A reviewer agent reads the applicable standards, relevant design sections, actual incremental diff, integration points, and test evidence. Review code/tests independently; do not rely only on the implementer's summary. The reviewer reports findings without editing during that review pass.
4. Record each finding with an ID, severity, file/line, concrete scenario/impact, and verification expectation. Distinguish correctness/security/reliability/contract blockers, adopted-rule violations, and nonblocking suggestions.
5. The implementer fixes valid findings, adds regression tests where meaningful, and reruns affected checks plus required local gates. Track findings as open, fixed awaiting verification, verified, or dismissed with evidence. Do not dismiss valid blockers to complete a PR.
6. The reviewer verifies fixes, reviews new changes, and repeats until no blocking findings remain. An evidence-backed disagreement is resolved explicitly; unresolved material ambiguity is reported, not converted into approval.
7. Record approval for the exact reviewed base/head and passing-check evidence. New code, a rebase, or changed inherited assumptions invalidates the relevant approval; rerun checks and re-review before merge.

Use a machine-readable record such as the following under a gitignored local report directory. Attach/export evidence later if the repository uses GitHub. Avoid committing a record containing the very commit hash that would change when committing the record.

```json
{
  "base_sha": "<reviewed-base>",
  "head_sha": "<reviewed-head>",
  "reviewer_session": "<actual-session-identifier>",
  "decision": "approved",
  "findings": [],
  "checks": [
    {"name": "unit-and-coverage", "result": "passed", "report": "unit-report.json"},
    {"name": "mocked-integration", "result": "passed", "report": "integration-report.json"}
  ],
  "deferred_checks": ["real-infrastructure", "live-smoke"]
}
```

The example is a schema illustration, not fabricated execution evidence. A local checker verifies required fields, current base/head match, no unresolved blockers, and test results. It cannot prove review quality merely by reading `approved`. Do not represent a local agent verdict as a GitHub approval made by an independent account. If GitHub later requires an approval/check, wire it to a valid automation identity/integration; do not invent credentials or self-approve through the PR author's identity.

## PR structure, CI, and deployment separation

- Keep PRs incremental with one coherent outcome, parent dependency, design references, test evidence, and material operational effects.
- Review stacked PRs against their parent. Merge bottom-up, rebase/retarget children carefully, and rerun checks against rewritten heads. Do not replay commits already squashed into the target branch.
- For a local-first implementation phase, perform all implementation/review/fixes and deterministic tests locally. Prepare GitHub CI workflows using the same commands; do not require cloud deployment to finish a code PR.
- Keep cloud provisioning, credentialed plans, real infrastructure tests, and live smoke tests in a later release phase after all implementation increments are complete. Mark them deferred until run. Code-ready and production-verified are different milestones.
- A push/merge during local development must not provision resources. Use an explicitly activated release workflow targeting a completed commit when deployment is ready. After activation, require all real-dependency gates before reporting release success.
- Pin tool/action/provider versions. Automate formatting, vet/static analysis, checked errors, builds, coverage, mocked integration, config/docs checks, and applicable infrastructure validation. Use scoped justified suppressions only.
- CI/branch protections enforce configured gates; prose alone does not. Avoid enabling an approval requirement the chosen identities cannot satisfy.

## Agent context and repository adoption checklist

Root `AGENTS.md` must point to this adopted spec and record repository purpose, reading order, actual package map, verified build/test commands, domain invariants, current PR/status location, and local/release boundaries. Keep secrets out of instructions and logs. Preserve unrelated changes.

Before implementation, provide an LLD, representative contracts/fixtures, local tool versions and commands, and a scoped PR plan. Cloud roles, external endpoints, infrastructure state, and live credentials may remain release inputs when mocked boundaries support implementation. Never fabricate their values.

Document commands only when they exist and perform their advertised work; no success-returning stubs. Update documentation/examples with code changes. Keep progress factual, link actual PRs, and distinguish passed, failed, and deferred verification.

## References

References reviewed in this design discussion; adopted rules are the explicit text above, not every rule from each external guide:

- [Effective Go](https://go.dev/doc/effective_go)
- [Google Go Style](https://google.github.io/styleguide/go/)
- [Uber Go Style](https://github.com/uber-go/guide/blob/master/style.md)
- [Google code review guidance](https://google.github.io/eng-practices/review/reviewer/looking-for.html)
- [Apache governance and collaboration](https://www.apache.org/foundation/how-it-works/)
- [AGENTS.md convention](https://github.com/agentsmd/agents.md)
