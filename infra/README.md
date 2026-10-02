# Infrastructure

Two Terraform root modules, both pinned to Terraform `~> 1.16.4` and AWS provider `6.61.0`. Nothing here has been applied; see [implementation status](../docs/plans/implementation-status.md).

- `bootstrap/`: the Terraform backend only — bucket `data-fetch-service-terraform-state` and lock table `data-fetch-service-terraform-lock`. Applied once with local state, outside the 30-day runtime lifecycle.
- `service/`: production runtime — buckets, three queues with DLQs, five VPC-attached Lambdas, the HTTP API at `fetch.kagent.app`, schedules, IAM, and alarms. It creates no VPC.

## Local checks

`make check-infra TERRAFORM=/path/to/terraform` runs `fmt -check` and `validate` for both modules and the mocked `terraform test` suite for `service/` (`bootstrap/` has no tests). It needs network access to download the provider, but no AWS credentials. `service/tests/runtime.tftest.hcl` asserts queue visibility/retention/redrive, lifecycle prefixes, per-role queue publish/consume scope, artifact access, processor-route access, the schedule payload, staged activation defaults, and output names. Its only failing-case runs are a processor in another VPC and a processor endpoint that differs from the bundled configuration; the subnet, NAT-route, and endpoint preconditions are not exercised by a test, and the S3 endpoint-policy check is a substring match evaluated at apply.

## Apply order

1. Shared-network state, owned by `cloud-platform-network`. The manual `Shared network` workflow in this repository calls its reusable workflow; it only plans unless `apply` is ticked.
2. `data-processing-service` infrastructure.
3. `bootstrap/`, then `service/`.

Initialise the service backend with the bootstrap outputs:

```sh
terraform -chdir=infra/service init \
  -backend-config=bucket=data-fetch-service-terraform-state \
  -backend-config=key=service/terraform.tfstate \
  -backend-config=region=ap-south-1 \
  -backend-config=dynamodb_table=data-fetch-service-terraform-lock
```

## Required remote-state outputs

| State | Outputs |
|---|---|
| Shared network | `vpc_id`, `private_subnet_ids_by_az`, `private_route_table_ids_by_az`, `nat_gateway_ids_by_az`, `s3_endpoint_id`, `sqs_endpoint_id`, `logs_endpoint_id`, `fetch_lambda_security_group_id` |
| Processor | `vpc_id`, `processor_api_endpoint`, `processor_submission_route_arn` |

Preconditions fail the plan when a Lambda subnet is public or outside the shared VPC, a private route table lacks its NAT default route, an endpoint is in another VPC or lacks private DNS, the S3 endpoint policy allows neither this account's buckets nor both service buckets, the processor is in another VPC, or `processor_api_endpoint` differs from the bundled `processor.url`.

## Service inputs

| Variable | Purpose |
|---|---|
| `hosted_zone_id` | Existing public `kagent.app` zone; this stack manages only the `fetch.kagent.app` alias and certificate-validation record |
| `network_state_*`, `processor_state_*` | Location of the two remote states |
| `deployment_commit`, `config_revision`, `package_bucket`, `package_sha256` | Exact release: packages are read from `releases/{commit}/{config revision}/{kind}.zip` |
| `external_producer_role_arns`, `native_eventbridge_rule_arns` | Approved senders to the ingress queue |
| `processor_reader_role_arns` | Processor roles allowed to read `runs/*` in the artifact bucket |
| `enable_ingress_consumption`, `enable_bse_schedule` | Staged activation; both default to `false` |

Outputs: `ingress_queue_url`, `ingress_queue_arn`, `api_url`, `artifact_bucket`, `state_bucket`, `lambda_function_names`, `shared_vpc_id`, `deployment_commit`, `config_revision`.
