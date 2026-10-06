# Infrastructure

Two Terraform root modules, both pinned to Terraform `~> 1.16.4` and AWS provider `6.61.0`. Nothing here has been applied; see [implementation status](../docs/plans/implementation-status.md).

- `bootstrap/`: the bucket `data-fetch-service-packages` that holds immutable Lambda release packages. The Terraform state bucket `data-fetch-service-terraform-state` is created by the release workflow before any Terraform runs, and state is locked with an S3 lock file, so there is no lock table.
- `service/`: production runtime — buckets, three queues with DLQs, five Lambdas outside any VPC, the HTTP API at `fetch.kagent.app`, schedules, IAM, and alarms. It uses no VPC.

## Local checks

`make check-infra TERRAFORM=/path/to/terraform` runs `fmt -check`, `validate`, and the mocked `terraform test` suites for both modules. It needs network access to download the provider, but no AWS credentials. `service/tests/runtime.tftest.hcl` asserts queue visibility/retention/redrive, lifecycle prefixes, per-role queue publish/consume scope, artifact access, processor-route access, the schedule payload, staged activation defaults, and output names. It also asserts that no Lambda has a VPC configuration and no role has an `ec2:` action. Its failing-case run is a processor endpoint that differs from the bundled configuration.

## Apply order

1. `data-processing-service` infrastructure (it applies and uses the shared network; this service uses no VPC).
2. `bootstrap/`, then `service/`.

The manual `Release` workflow does all of this; see the [runbook](../docs/runbook.md). To initialise the service backend by hand:

```sh
terraform -chdir=infra/service init \
  -backend-config=bucket=data-fetch-service-terraform-state \
  -backend-config=key=service/terraform.tfstate \
  -backend-config=region=ap-south-1 \
  -backend-config=use_lockfile=true
```

## Required remote-state outputs

| State | Outputs |
|---|---|
| Processor | `processor_api_endpoint`, `processor_submission_route_arn` |

Preconditions fail the plan when `processor_api_endpoint` is not `https://processing.kagent.app/v1/event-ingestions`, the route ARN is not one `POST /v1/event-ingestions` execute-api route in this region, or the endpoint differs from the bundled `processor.url`.

## Service inputs

| Variable | Purpose |
|---|---|
| `hosted_zone_id` | Existing public `kagent.app` zone; this stack manages only the `fetch.kagent.app` alias and certificate-validation record |
| `processor_state_*` | Location of the processor remote state |
| `deployment_commit`, `config_revision`, `package_bucket`, `package_sha256` | Exact release: packages are read from `releases/{commit}/{config revision}/{kind}.zip` |
| `external_producer_role_arns`, `native_eventbridge_rule_arns` | Approved senders to the ingress queue |
| `processor_reader_role_arns` | Processor roles allowed to read `runs/*` in the artifact bucket |
| `enable_ingress_consumption`, `enable_bse_schedule` | Staged activation; both default to `false` |

Outputs: `ingress_queue_url`, `ingress_queue_arn`, `api_url`, `artifact_bucket`, `state_bucket`, `lambda_function_names`, `deployment_commit`, `config_revision`.
