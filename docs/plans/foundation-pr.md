# Foundation: local quality gates and acquisition domain primitives

Base: `main`. Branch: `stack/01-foundation`. Local PR preparation only; no remote PR URL yet.

The repository now contains the authoritative fetch design and processor admission contract, agent instructions, and reproducible Go quality checks. Domain primitives establish immutable terminal-state classification and source/event request identity without AWS dependencies.

Validation: race tests; 100% unit statement coverage over the current small domain package; native and Linux arm64 builds; vet, staticcheck, errcheck; coverage threshold regression checks; relative documentation links; OpenAPI JSON/YAML parity.

Mocked API integration begins with the first implemented application boundary. Cloud tests and deployment remain deferred. The processor LLD is copied unchanged as an external reference; its upstream-relative links are excluded from local link checks.
