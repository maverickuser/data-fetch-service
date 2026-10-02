# Documentation map

- [Fetch LLD](specs/data-puller-service-lld.md): authoritative service design.
- [Processor OpenAPI JSON](specs/data-processing-service-openapi.json) and [YAML](specs/data-processing-service-openapi.yaml): authoritative admission contract, API 1.0.0.
- [Implementation plan](plans/data-fetch-service-implementation-plan.md) and [status](plans/implementation-status.md): sequence and evidence.
- [Implementation context](plans/data-fetch-service-implementation-context-spec.md): supplied and pending inputs.
- [Go standards](engineering/go-standards.md) and [review policy](engineering/pr-review.md): engineering gates.
- [Admission runtime and API](runtime.md): implemented entry points, deployment inputs and API limits.
- [Pull PR description](plans/pull-pr.md): complete-event orchestration, manifest and fingerprint behavior.
- [Delivery PR description](plans/delivery-pr.md): processor admission, durable attempts, and accepted-baseline recovery.
- [Recovery PR description](plans/recovery-pr.md): reconciler, linked Pull retries, and delivery resume.
- [Full-rerun PR description](plans/full-rerun-pr.md): pinned manual rerun and API contract.
- [Infrastructure guide](../infra/README.md) and [infrastructure PR description](plans/infrastructure-pr.md): Terraform modules, inputs, apply order, and known gaps.
- [Live verification PR description](plans/live-verification-pr.md): smoke runner behaviour, inputs, and what remains open.
- [Original fetch specification](specs/data-puller-service-spec.md): historical; LLD supersedes conflicts.
- [Processor LLD](specs/structured-file-processing-lld.md) and [processor specification](specs/structured-file-processing-spec.md): external references. Processor LLD is preserved unchanged at user request; authoritative admission details are in OpenAPI.

The shared-network stack owns VPC resources; application stacks consume outputs. Network ownership prose predating that decision does not authorize duplicate Terraform ownership. Exact processor listener outputs belong to its application stack, not the network stack.
