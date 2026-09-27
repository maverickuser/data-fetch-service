# Data Fetch Service — Implementation Context Specification

Status: input checklist for implementation; values are collected incrementally.
Date: 2026-09-27.

This document records the repository, tooling, contracts, fixtures, credentials, and operational facts that coding agents and reviewers need to implement and validate the LLD. It is complementary to the LLD: the LLD defines service behaviour; this document defines the implementation context and evidence needed to build it safely.

## How to use this document

- Keep one authoritative value for each input. Link the source when the value comes from a repository, issue, contract, or AWS resource.
- Mark each item `known`, `pending`, `assumed`, or `not applicable`.
- Do not put secrets, long-lived tokens, private keys, or production response bodies in this file. Record secret names/locations and access requirements only.
- Update this document in the same PR when an implementation input changes. Material behaviour changes also update the LLD and a decision record.
- Ask for pending values one at a time. A pending deployment value must not block local implementation unless the relevant test or integration requires it.

## Already agreed

| Context | Value | Status |
|---|---|---|
| Repository | `https://github.com/maverickuser/data-fetch-service`; default branch `main` | known |
| Language/runtime | Go; AWS SDK for Go v2; standard AWS Lambda | known |
| Deployment | Terraform through GitHub Actions; `prod` only | known |
| CI/CD split | CI runs quality checks on pull requests; CD builds and deploys from `main` using Terraform, Docker, and AWS | known |
| GitHub ↔ AWS authentication | GitHub Actions assumes a dedicated AWS IAM role through OIDC; provider URL `https://token.actions.githubusercontent.com`; audience `sts.amazonaws.com` | known |
| Deployment role secret | GitHub repository Actions secret `AWS_ROLE_TO_ASSUME` contains the role ARN | known |
| AWS region | `AWS_REGION`, default `ap-south-1` | known |
| Resource prefix | `data-fetch-service`; no account/region/environment suffix | known |
| Ingress queue | Created and owned by this service; dedicated consumer queue with DLQ | known |
| BSE trigger | Monday–Friday at 20:00 Asia/Kolkata, including holidays | known |
| BSE event input | `exchangeName: BSE`; no ISIN | known |
| BSE source | `DEBTBHAVCOPY{run_date:ddMMyyyy}.zip` | known |
| BSE selected member | `fgroup{run_date:ddMMyyyy}.csv` | known |
| NSDL trigger | CloudEvents 1.0 message through ingress SQS | known |
| NSDL input | `data.inputs.isin_code` | known |
| NSDL jobs | Six configured JSON API calls, one file per job | known |
| Pull crash/timeout | Three automatic full-event retries after the initial execution | known |
| Test gate | Unit statement coverage strictly greater than 95%; mocked integration tests; real-endpoint smoke tests | known |

## Required implementation context

### Repository and workflow

| Input | Needed for | Status |
|---|---|---|
| Repository access and default branch | PR 01 and stacked branches | known: default branch is `main`; access still needed for implementation |
| GitHub Actions runner/permissions policy | CI and deployment workflows | known: PR CI and `main` CD; deployment workflow uses `id-token: write` and least-privilege role assumption |
| Required checks and branch protection administrators | Merge enforcement | agent-only review is agreed; GitHub branch protection/human administrators are not required for local implementation. Configure automated checks when repository CI is enabled |
| CODEOWNERS/maintainer identities | Independent PR approval | not required for the agreed agent-only approval workflow; add human ownership later if governance requires it |
| Stack/PR naming convention, if different from the plan | Stacked PR workflow | assumed: plan defaults |

### Toolchain

Compatibility note: AWS Go Lambda uses the `provided.al2023` OS-only runtime and executes the compiled binary, so the Lambda runtime does not constrain the project to a managed Go release. AWS SDK for Go v2 currently requires at least Go 1.23 in its developer guidance; the current SDK module declares Go 1.24. Pin exact SDK module versions in `go.mod` because SDK service modules are independently versioned. Go 1.27.1 is compatible with these minimums, subject to the final project pin and CI image availability.

| Input | Needed for | Status |
|---|---|---|
| Go version/toolchain pin | Reproducible builds and CI | known: Go 1.27.1; CI must test JSON/timer compatibility, race detection, and `linux/arm64` cross-compilation |
| Terraform version pin | Reproducible plans | known: Terraform 1.16.4; use `required_version = "~> 1.16.4"` |
| AWS provider version | AWS resource provisioning | known: `hashicorp/aws` 6.61.0 with committed `.terraform.lock.hcl` |
| Linter/static-analysis versions and policy | CI quality gates | assumed: choose and pin in PR 01 |
| Package/build commands | AGENTS.md and local development | assumed: define in PR 01 and verify |

### Contracts and fixtures

| Input | Needed for | Status |
|---|---|---|
| Complete processor API/OpenAPI contract | Delivery implementation and mocked tests | supplied: [JSON](../specs/data-processing-service-openapi.json) and [YAML](../specs/data-processing-service-openapi.yaml), equivalent representations of OpenAPI 3.1.0, API version 1.0.0; authoritative for processor admission |
| Processor durable-202 and idempotency behaviour | Delivery correctness | specified by supplied OpenAPI: stable submission CloudEvent and run-ID key across retries; durable receipt/job/dispatch before 202; live behaviour remains to be verified |
| Processor S3 read role/bucket policy integration | End-to-end smoke test | pending |
| External NSDL producer identity and event source | Queue IAM and integration | publisher service owns its send permissions; its role ARN is not an implementation prerequisite. Producer must use the agreed CloudEvents contract and exported queue URL/ARN |
| Native EventBridge rule/source mappings, if any | Event adapter configuration | pending |
| BSE date for smoke tests | Real BSE smoke test | agreed: current IST date, then three earlier weekdays on HTTP 404; configurable fallback count with default/minimum 3. Record selected date and failures; this is smoke-only |
| Valid NSDL ISIN for smoke tests | Real NSDL smoke test | supplied: `SMOKE_NSDL_ISIN=INE121A07QY9`; verify all six real API responses during integration |
| Sanitised BSE/NSDL fixtures and expected outputs | Deterministic tests | assumed: create in PR 02/05 |

### AWS and deployment

| Input | Needed for | Status |
|---|---|---|
| GitHub OIDC deployment role ARN | Trusted Terraform/deployment workflows | supplied: (ARN kept out of the repository), to be configured in GitHub secret `AWS_ROLE_TO_ASSUME`; trust/permissions and secret presence not yet verified |
| Terraform backend location/bootstrap ownership | State management | known: create `data-fetch-service-terraform-state` through the separate bootstrap configuration; export/use its location for service Terraform state. Bucket not yet created |
| AWS account/organization policy constraints | IAM and resource creation | agreed: standard least-privilege IAM; no additional organization-specific requirements supplied. Scope deployment permissions to required resources; verify effective role permissions during integration |
| KMS/encryption requirements | S3/SQS configuration | agreed: standard AWS-managed/service-managed encryption; use S3 SSE-S3 and SQS SSE-SQS, including state buckets and DLQs. No customer-managed KMS key required |
| External producer cross-account permissions | Ingress queue policy | conditional integration input only if publisher is in another account; confirm then and configure the required queue policy |
| Processor network/connectivity requirements | Lambda deployment and smoke tests | agreed: processor runs in the same VPC as the Lambdas and is reached through private security-group/DNS connectivity |
| VPC ID, private subnet IDs, and Availability Zone mapping | Terraform runtime infrastructure | pending deployment input; use private subnets in at least two AZs |
| VPC endpoint and NAT ownership/sizing | Terraform runtime infrastructure | agreed topology: S3 gateway endpoint, SQS/CloudWatch Logs interface endpoints, NAT for public BSE/NSDL calls; exact existing-network ownership and NAT sizing remain deployment inputs |
| Shared VPC Terraform state and outputs | Processor/fetch deployment ordering | required: one network state consumed by both services; expose `vpc_id`, private subnets by AZ, processor/fetch security groups, endpoint IDs, and internal processor DNS/listener |

### Operational sizing

| Input | Needed for | Status |
|---|---|---|
| Typical and maximum BSE ZIP/CSV size | Memory, `/tmp`, timeout | pending |
| Typical and maximum NSDL response size | Streaming and limits | pending |
| Expected daily BSE and NSDL volume | Concurrency and cost | supplied: initially approximately 10,000 NSDL events/day, or 60,000 source calls/day before retries and smoke tests; BSE once per scheduled weekday. NSDL arrivals are bursty after bhavcopy processing: the system checks whether each ISIN is already present and requests missing ISINs. Initial volume is high and expected to decrease as coverage grows |
| Batch completion latency | Queue concurrency and capacity | agreed: no strict latency target; cost is the primary tuning objective, subject to reliable processing, upstream limits, and draining backlog within retention. Monitor queue age |
| Cost optimisation | Resource sizing and operations | agreed: measure cost per completed event across Lambda, S3 requests/storage, queues, logs, and recovery. Use on-demand execution, conservative concurrency, measured memory sizing, bounded recovery scans, and limited status polling; retain the agreed correctness/retry/test gates |
| Processor latency and availability target | Delivery timeout/retry tuning | pending |
| Alert owners and on-call route | Production operations | pending |

## Agent operating rules

Agents should read this document, `AGENTS.md`, the LLD, the functional spec, and the current implementation-status file before editing. They should inspect the actual branch and CI state, preserve unrelated changes, and report missing context rather than inventing production values.

Agents may continue local implementation with mocked boundaries while deployment inputs are pending. They must not claim live readiness, create production resources, use guessed credentials, or silently skip required smoke tests. Every supplied value should be reflected in the relevant config, workflow, test fixture, or decision record and linked from the implementation-status file.

## Collection order

Collect pending inputs in this order unless a dependent task requires another value first:

1. Repository access/default branch and GitHub workflow permissions.
2. Go and Terraform versions.
3. External NSDL producer identity and EventBridge mappings.
4. Deployment role and Terraform backend.
5. Smoke-test fixtures.
6. Sizing and operational ownership.
7. Processor contract and integration identity (deferred until last at the user's request).
