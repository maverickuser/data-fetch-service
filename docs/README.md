# Documentation map

- [Fetch LLD](specs/data-puller-service-lld.md): authoritative service design.
- [Processor OpenAPI JSON](specs/data-processing-service-openapi.json) and [YAML](specs/data-processing-service-openapi.yaml): authoritative admission contract, API 1.0.0, copied from data-processing-service (see [processing-service copies](specs/processing-service-copies.md)).
- [Status](plans/implementation-status.md): what is built, verified, and deployed.
- [Active plans](plans/active/README.md): plans for changes that span several PRs. [Completed plans](plans/completed/data-fetch-service-implementation-plan.md): the original build plan and its per-PR descriptions, kept as history.
- [Tech debt](plans/tech-debt.md): known gaps no current PR fixes.
- [Implementation context](plans/data-fetch-service-implementation-context-spec.md): supplied and pending inputs.
- [Go standards](engineering/go-standards.md) and [review policy](engineering/pr-review.md): engineering gates. The package layering is enforced by `internal/architecture`.
- [Context management](engineering/context-management.md): when agents compact or clear their context, and what to keep.
- [API guide](api.md) and [OpenAPI document](../internal/api/openapi.yaml): public REST API for clients, served with Swagger UI at `/docs`.
- [Admission runtime and API](runtime.md): implemented entry points, deployment inputs and API limits.
- [Pull PR description](plans/completed/pull-pr.md): complete-event orchestration, manifest and fingerprint behavior.
- [Delivery PR description](plans/completed/delivery-pr.md): processor admission, durable attempts, and accepted-baseline recovery.
- [Recovery PR description](plans/completed/recovery-pr.md): reconciler, linked Pull retries, and delivery resume.
- [Full-rerun PR description](plans/completed/full-rerun-pr.md): pinned manual rerun and API contract.
- [Infrastructure guide](../infra/README.md) and [infrastructure PR description](plans/completed/infrastructure-pr.md): Terraform modules, inputs, apply order, and known gaps.
- [Live verification PR description](plans/completed/live-verification-pr.md): smoke runner behaviour, inputs, and what remains open.
- [Runbook](runbook.md): releasing, rolling back, and operating the deployed service.
- [Original fetch specification](specs/data-puller-service-spec.md): historical; LLD supersedes conflicts.
- [Processor LLD](specs/structured-file-processing-lld.md) and [processor specification](specs/structured-file-processing-spec.md): external references, copied byte-for-byte from data-processing-service and never edited here; [processing-service copies](specs/processing-service-copies.md) records the source commit and how to re-sync. Authoritative admission details are in OpenAPI.

The shared-network stack owns VPC resources. This service's Lambdas run outside any VPC (2026-10-06), so it consumes no network outputs; VPC and NAT prose predating that decision is superseded. Exact processor listener outputs belong to its application stack, not the network stack.
