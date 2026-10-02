# PR 10 — Terraform runtime infrastructure

Base: `stack/09-api-telemetry`. Branch: `stack/10-infrastructure`.

Adds `infra/bootstrap` (Terraform backend) and `infra/service` (production runtime); see the [infrastructure guide](../../infra/README.md). The service stack provisions the artifact and state buckets, ingress/pull/delivery queues with DLQs, five VPC-attached `provided.al2023` arm64 Lambdas, the HTTP API with its `fetch.kagent.app` custom domain, the YAML-derived BSE schedule and five-minute reconciler schedule, per-role IAM, log groups, and alarms. It consumes the shared-network and processor Terraform states and creates no VPC.

Processor change agreed 2026-10-02: the processor is reached through its IAM-protected API Gateway route rather than an internal load balancer. Delivery signs the unchanged `POST /v1/event-ingestions` request with SigV4 for `execute-api`; a gateway `403` is terminal and a signing failure is retried. The Delivery and Reconciler roles (the Reconciler resumes interrupted deliveries) may invoke only that route. `config/environments/prod.yaml` now sets `processor.url` to `https://processing.kagent.app/v1/event-ingestions`; `processor.enabled` stays `false` until release, so the configuration revision changes but no handoff is attempted.

Behaviour worth reviewing:

- State lifecycle expires only `runs/`, `requests/`, and `listings/` after 30 days. `coordination/` and `acceptance/` are retained so the accepted fingerprint outlives its artifacts. Abandoned multipart uploads are removed after one day.
- Every role can read and write the five state prefixes including `acceptance/`. Artifact read: API, Pull, Delivery, Reconciler. Artifact write: Pull and API (delivery-retry child manifests). Admission has no artifact access. API and Admission may publish to both internal queues because dispatch repair for a joined run can target either. `s3:ListBucket` is unconditioned so a missing key reads as 404, which admission and expiry handling depend on.
- Lambda does not set the reserved `AWS_REGION` variable; the runtime supplies it.
- Ingress consumption and the BSE schedule default to disabled for a staged first rollout.

Known gaps, not implemented here:

- Alarms cover queue age, DLQ depth, handler retry outcomes, and Lambda invocation errors. The LLD's state-derived failed-run and expired-ownership metrics are not emitted by the Go handlers, so no alarm exists for them yet.
- A failing `terraform test` assertion on an IAM policy crashes Terraform 1.16.4 while printing the diagnostic; the run still fails.
- Alarms have no notification target, the Scheduler trust policy has no source-account condition, and noncurrent object versions persist for a further 30 days after expiry.
- No trusted plan has run: it needs AWS access and the real network and processor states.

Validation: `make check` passes, including race/unit coverage 3257/3425 statements (95.09%), mocked integration, lint, native and Linux/arm64 builds, configuration, documentation links, OpenAPI parity, and `check-infra` (`fmt -check`, `validate` on both modules, three mocked `terraform test` runs). No Terraform was applied and no AWS or live endpoint was called.
