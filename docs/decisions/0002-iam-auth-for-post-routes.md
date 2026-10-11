# 0002 — IAM authorization for routes that start work

Date: 2026-10-11. Status: accepted.

## Context

The API had no authentication. Anyone who could reach `https://fetch.kagent.app` could start runs, which download from BSE and NSDL and deliver to the processor. Callers that start runs are the owner and AWS-hosted services, including the release smoke suite, all of which hold AWS credentials.

## Decision

The three POST routes (`/v1/events/{event_type}/runs`, `/v1/runs/{run_id}/reruns`, `/v1/runs/{run_id}/delivery-retries`) use API Gateway `AWS_IAM` authorization. Callers sign with SigV4 for `execute-api` and need `execute-api:Invoke` on the `api_post_invoke_arn` output. Read routes and `/docs` stay public. The smoke runner signs its requests with the same signer delivery uses for the processor.

## Consequences

- Unsigned or unauthorized POSTs get `403` from API Gateway before reaching the Lambda.
- Swagger UI "Try it out" works only for reads; POSTs need curl `--aws-sigv4`, awscurl, Postman, or an SDK.
- The release deployment role, which runs the smoke suite, needs `execute-api:Invoke` on `api_post_invoke_arn`.
- Run history remains publicly readable. Protecting reads, or callers without AWS credentials, would need a JWT authorizer instead.
