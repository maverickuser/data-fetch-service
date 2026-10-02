# PR 11 — Release verification harness (smoke runner)

Base: `stack/10-infrastructure`. Branch: `stack/11-live-verification`.

Adds the real-endpoint smoke runner (`internal/smoke`, `cmd/smoke`) and a manually triggered workflow (`.github/workflows/smoke.yml`). The runner has only been exercised against a local fake of the deployed service; it has never run against AWS, BSE, NSDL, or the processor.

What one suite does, in order, stopping at the first failure:

1. Preflight: an `https` API origin, a 12-character ISIN, at least three fallback weekdays, a suite ID, a commit, and all four clients. A missing input fails before any call.
2. Schedule: reads `data-fetch-service-daily-bhavcopy` and requires `cron(0 20 ? * MON-FRI *)`, `Asia/Kolkata`, and `ENABLED`.
3. BSE: captures today's date in `Asia/Kolkata` once, submits a manual run for it, and on a `SOURCE_NOT_FOUND` failure tries the preceding weekdays (three by default), each as a fresh run with its own idempotency key. Any other failure stops the suite. It then requires one stored `BSE_fgroup{ddMMyyyy}.csv` and the run manifest in the artifact bucket.
4. NSDL: sends a CloudEvent to the ingress queue, resolves the run through request lookup, and requires the six `{ISIN}_{job}.json` files and the manifest.
5. Forced run: submits a manual NSDL run with `force: true` and requires phase `completed` and a recorded processor HTTP 202 with a receipt.

Polling follows automatic retry children through `latest_run_id` and is bounded (75 minutes by default, `SMOKE_TIMEOUT_SECONDS` to change). The report (`smoke-report.json`) is written on success and failure and lists every attempted BSE date.

Service change: a source HTTP 404 is now recorded as `SOURCE_NOT_FOUND` instead of `SOURCE_HTTP`, so an unpublished file can be told apart from other source errors. Retry behaviour is unchanged (still not retried).

Workflow inputs: secret `AWS_ROLE_TO_ASSUME`; variables `SMOKE_API_URL`, `SMOKE_NSDL_ISIN`, `SMOKE_INGRESS_QUEUE_URL`, `SMOKE_ARTIFACT_BUCKET`, optional `AWS_REGION`. The workflow is `workflow_dispatch` only and does not deploy.

Not in this increment (still open from the PR 11 plan): the disposable-resource AWS integration workflow, the load/resource workflow, and local fakes for IAM, queue redelivery, and lifecycle assertions. The smoke suite also does not verify the processor's own S3 read access; it checks that artifacts exist using the workflow role.

Validation: see the implementation status for the `make check` result on this branch. No AWS resource was created and no live endpoint was called.
