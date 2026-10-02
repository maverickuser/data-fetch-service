# PR 02: configuration and event contracts

Base: `stack/01-foundation`. Branch: `stack/02-contracts`.

Resolve acquisition jobs from validated configuration and the original event time. BSE produces an exchange-prefixed fgroup CSV; NSDL produces six ISIN-prefixed JSON files. Strict YAML overlays reject unknown fields and additional documents. URL substitutions preserve path/query boundaries; filenames reject traversal, control characters and incompatible extensions.

CloudEvents and explicitly mapped native EventBridge events normalize independently of SQS delivery identity. Snapshots preserve resolved dates, jobs and configuration; equivalent normalized inputs share execution identities while changed payloads can be detected. Configuration revisions include the deployment commit.

Validation: `make check` runs formatting, vet, staticcheck, errcheck, race/unit tests, weighted coverage strictly greater than 95%, native/Linux arm64 builds, config validation, documentation links and external OpenAPI parity. Fixtures cover scheduled BSE output and tests cover six NSDL filenames, identity equivalence/conflicts, independent SQS record failures, malformed configuration and unsafe templates.

No deployment or live dependency calls. Native mapping persistence and Lambda wiring are PR 04 work; full AWS schedule grammar is checked with infrastructure in PR 10. Optional filenames resolve from usable URL basenames or ZIP members; a blank direct filename means acquisition must choose the response-header/job-ID fallback in PR 05. ISIN JSON jobs require explicit ISIN templates. Processor remains disabled with a clearly invalid example endpoint until supplied. Independent agent review evidence is retained under the ignored `.reports/` directory for the exact reviewed commit.
