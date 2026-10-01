# Admission, initial API, and pull runtime

Local commands are `make check` and `make test-integration`. Tests use fakes at AWS/queue boundaries; no credentials or cloud resources are needed. The entry points are `cmd/api`, `cmd/admission`, and `cmd/pull`, compiled for Linux arm64 during checks. Artifact packaging and deployment arrive in the release increments.

At cold start all three functions read `config/events.yaml`, `config/environments/prod.yaml` and `config/native-events.yaml` from the deployment artifact. Configuration and native mappings must validate. Required environment variables are `DEPLOYMENT_COMMIT`, `STATE_BUCKET`, `PULL_QUEUE_URL` and `DELIVERY_QUEUE_URL`; `AWS_REGION` defaults to the configured region, then `ap-south-1`. Credentials come from the Lambda execution role through the SDK default provider. SDK attempts are capped at three, separate from source/delivery/execution retry budgets.

Pull also requires `ARTIFACT_BUCKET`. It constructs one guarded HTTPS client and one temporary-byte budget per warm Lambda environment. Pull SQS messages carry only `{"run_id":"..."}`. A record whose owner has already claimed or completed the phase is acknowledged as stale; an infrastructure failure is returned as an SQS partial batch failure. Invalid internal messages receive a durable rejection receipt.

Native mappings are repository managed and packaged with the pinned deployment commit. `mappings: []` accepts no native EventBridge signatures. Add an approved scheduled rule using `rule_arn`, `event_type` and fixed `inputs`; a generic native mapping uses `account`, `region`, `source`, `detail_type`, `event_type` and `detail_fields` (input name to top-level detail field). Inputs must be declared by the event configuration. CloudEvents producers require no native mapping.

The initial HTTP API implements the PR04 routes listed in the LLD. Manual submission is:

```http
POST /v1/events/daily-bhavcopy/runs
Content-Type: application/json
Idempotency-Key: daily-bse-2026-09-21

{"inputs":{"exchangeName":"BSE","run_date":"2026-09-21"},"force":false}
```

New/joined admissions return 202 with `run_id`, `status`, `reused` and `status_url`, plus `Location`. A retained terminal replay returns 200. Changed payloads under the same key return 409; malformed inputs return 400, oversized bodies 413, unknown resources 404, expired retained records 410 and transient infrastructure failures 503. Force is manual-only and does not replace an existing active run's choice.

Run listings use UTC `from`/`to` dates, at most 30 days and 100 candidate records per page (default 25). Preserve the original date filters when following `next_cursor`. Individual run detail includes its immutable snapshot; listings omit snapshots. Pull/delivery metadata pages cap aggregate content at 1 MiB; a larger result returns 422 requesting a lower limit. Raw source response objects are never exposed by these routes.

API bodies are limited to 64 KiB. Gateway responses are checked after JSON envelope encoding; an oversized response returns 502. Ingress bodies over 256 KiB are permanently rejected with a durable receipt. Source artifact limits are independent of these transport/metadata budgets.

No AWS resources have been provisioned. Delivery, manual recovery endpoints, reconciler, and deployment wiring are supplied by the remaining implementation increments.
