# Data Fetch Service — Proposed PR Review Policy

Date: 2026-09-27. Add to the repository as `docs/engineering/pr-review.md` in foundation PR 01. This is a project review policy, not a claim of compliance or endorsement by Google, Uber, Netflix, or Apache. Reviewer identities and GitHub protection configuration must be set during repository setup.

## Standards and precedence

Use the Go language/toolchain rules and gofmt as the baseline. Adopt Google's Go guidance for readability and idioms, with selected Uber guidance for explicit dependencies, error handling, cleanup, and managed goroutine lifetimes. Document the concrete adopted rules in `docs/engineering/go-standards.md`; do not require every rule from several potentially conflicting company guides.

Repository-specific documented decisions settle stylistic differences; the LLD determines domain behaviour. Record reference URLs and an accessed date or pinned revision so changes to an external guide do not silently change required checks. A company-specific library or naming convention is not mandatory just because it appears in a guide. Prefer current standard-library facilities when sufficient.

References:

- https://go.dev/doc/effective_go
- https://google.github.io/styleguide/go/
- https://github.com/uber-go/guide/blob/master/style.md
- https://google.github.io/eng-practices/review/reviewer/looking-for.html
- https://www.apache.org/foundation/how-it-works/

Google's review guidance provides a useful scope: design, behaviour, complexity, tests, names, comments, style, and documentation. Apache's public decision-making/review practices inform traceability and reasoned discussion; Apache is not one universal Go coding standard. Do not invent a Netflix-wide Go policy: adopt specific published project practices only if relevant and explicitly selected.

## Review sequence

1. Author prepares a focused incremental diff with LLD references, tests, known limits, and parent PR link. Self-review the actual diff.
2. Deterministic CI checks validate formatting, analysis, compilation, tests, coverage, and applicable infrastructure/docs checks.
3. A separate reviewer-agent session examines code and tests against the LLD and language rules, including failure paths rather than only the happy path. The review agent can assist itself with available tools, but its positive verdict must be supported by check and test evidence.
4. Author resolves blocking findings and links fixes/tests. Reviewer checks the updated diff and affected behaviour.
5. Merge only the reviewed current head with required checks passing. Rebase/retarget stacked children after parent merge and revalidate changed heads.

Agreed implementation profile: agent-only approval. Each PR receives an implementer-agent pass and a separate reviewer-agent pass. The reviewer records findings against the exact base/head commit, the implementer fixes valid findings, and the reviewer rechecks until there are no blocking findings and all local gates pass. Human approval and CODEOWNERS are not implementation prerequisites. GitHub branch protections may be added later, but must not claim a human approval occurred when review was agent-only. CI checks remain evidence and do not replace review reasoning.

## Automated checks

Pin tool versions and enable a deliberate set of checks rather than every available linter:

- gofmt/import formatting, go vet, Staticcheck, and checked-error analysis through a curated linter configuration.
- Unit tests with race detection and statement coverage strictly greater than 95%, measured as specified in the LLD.
- Mocked API integration/contract tests and build verification.
- Dependency vulnerability checks and secret scanning using selected pinned tooling; triage findings with evidence. Exceptions need a recorded reason and scope, not blanket suppression.
- Terraform format/validate and trusted plans, plus docs/link/config validation as those files exist.

Enforce these through GitHub required checks, not only prose in AGENTS.md. All suppressions must be narrow and explain why the check does not apply. CI findings cannot be hidden by dropping difficult code from coverage or silently disabling checks.

## Go review checklist

- Guiding principle: code must be clear to read and understand. Prefer cohesive functions, descriptive names, and straightforward control flow over satisfying arbitrary size limits. Function/file length and complexity are review signals, not reasons to fragment coherent code mechanically.
- Every authored production function and method has a brief Go-style comment immediately above its declaration explaining what it does. Start with its name, including for unexported helpers. State the purpose/result rather than narrating implementation steps; add side effects, failure behaviour, idempotency, or concurrency guarantees when relevant to callers. Keep comments accurate when behaviour changes. Generated code is excluded; test functions can use descriptive names and scenario comments without redundant boilerplate. Inline anonymous callbacks need a comment only when their purpose is not apparent from context.
- Treat these comments like concise API documentation: explain the contract, not each line. Avoid boilerplate such as “Process processes data.” Non-obvious internal reasoning belongs near the relevant code.
- Clear package boundaries and names; small consumer-focused interfaces; no abstractions without an actual use.
- Explicit dependencies, limited mutable shared state, and no hidden production I/O during initialization.
- Errors preserve useful context and are handled intentionally; normal request failures do not panic or terminate the process.
- Context/deadlines propagate to blocking work; every goroutine has a bounded lifetime and an owner responsible for cancellation/joining.
- Shared data has a clear synchronization/ownership policy. Cleanup closes bodies/files and aborts incomplete uploads on all relevant paths.
- Memory, temporary disk, concurrency, retries, and input parsing are bounded. Performance changes include measurement where claims depend on it.
- Tests assert meaningful observable outcomes and failure behaviour. A coverage percentage alone does not establish correctness.

## Service-specific review checklist

- No partial dataset delivery; selected ZIP file and event-derived output names match the LLD.
- Event identity and original logical date survive redelivery and retries.
- Three source attempts, three delivery attempts, and three automatic execution retries remain separate budgets.
- Conditional S3 ownership, generations, and durable intents survive interruption without overlapping owners or duplicate children.
- Only HTTP 202 advances the accepted baseline; retries reuse the appropriate idempotency key.
- IAM, public API behaviour, producer permissions, private artifact access, and deployment changes match the agreed contract.
- Tests cover stale ownership, conflicts, timeout/unknown outcomes, expiry, and backward-compatible stored snapshots where relevant.
- Documentation and examples reflect the final implementation; release smoke tests use actual dependencies.

## Findings and PR evidence

Label findings as blocking correctness/reliability/security/contract issues, required violations of an adopted rule, or nonblocking suggestions. Provide file/line, concrete failure scenario, impact, and a suggested fix or verification. Personal style preference is not a blocker when the code meets the documented rules.

Every PR description records: problem/result, parent/base branch, relevant LLD sections, validation results, and material operational effects. Keep review decisions in the PR or a linked decision record. Describe the final diff, not abandoned approaches.

For stacked PRs, review the child's incremental diff against its parent and assess inherited assumptions. After rebasing, review changed integration points and rerun checks on the new head; do not silently reuse stale approval evidence.
