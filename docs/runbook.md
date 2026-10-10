# Runbook

How to release and operate the deployed service. As of the last update to the [implementation status](plans/implementation-status.md), only the `package` stage has run and no service has been applied; steps below that depend on a deployment are written from the design and have not been exercised.

## Releasing

Everything runs from the manual `Release` workflow (Actions → Release → Run workflow) on `main`. It never starts on push or pull request. Pick a stage:

| Stage | What it does | What it creates in AWS |
|---|---|---|
| `package` | Runs all CI gates, builds the five Lambda ZIPs, uploads them | State bucket `data-fetch-service-terraform-state`, package bucket `data-fetch-service-packages`, five objects under `releases/{commit}/{config revision}/` |
| `plan` | The above, then plans the service stack | Nothing more |
| `apply` | The above, then applies the plan and runs the smoke suite | The whole service |

A package is never overwritten: re-running a commit re-uses its objects, and a differing ZIP under the same key fails the run.

Before any stage mutates AWS, a read-only preflight checks the deployment role and ownership/availability of the fixed state and package bucket names. For `plan` and `apply`, it also requires the hosted-zone input, checks the hosted-zone name, and confirms that the processor endpoint is reachable. Both stages must pass the disposable-resource AWS integration workflow before the service stack is planned. A failed preflight prevents package-bucket changes. If AWS integration fails, the package stage may already have run, but the service stack is not planned or applied.

Required repository configuration:

- Secret `AWS_ROLE_TO_ASSUME`.
- Variable `HOSTED_ZONE_ID` (needed from `plan` onward), `SMOKE_NSDL_ISIN` (needed for `apply`).
- Optional variables: `AWS_REGION` (default `ap-south-1`), `ENABLE_INGRESS_CONSUMPTION` and `ENABLE_BSE_SCHEDULE` (default `false`), `EXTERNAL_PRODUCER_ROLE_ARNS` and `PROCESSOR_READER_ROLE_ARNS` (JSON lists, default `[]`), `SMOKE_BSE_FALLBACK_WEEKDAYS`.

First rollout order: the processing service must be deployed first, because delivery submits to its `POST /v1/event-ingestions` route in the same account. `config/environments/prod.yaml` sets `processor.enabled: true`, so delivery submits to the processor as soon as runs complete. Release with both activation switches `false`, then set `ENABLE_INGRESS_CONSUMPTION` and `ENABLE_BSE_SCHEDULE` to `true` and release again. The smoke suite's schedule check fails while `ENABLE_BSE_SCHEDULE` is `false`.

A failed smoke suite fails the workflow. It does not undo the applied infrastructure.

## Verifying AWS behaviour

The manual `AWS integration` workflow creates a disposable bucket and queue, runs the `aws`-tagged tests in `internal/awsverify` (conditional writes, immutable-record collisions, missing-key 404s, SQS redelivery), and deletes the resources even when tests fail. A warning in its last step means a resource was left behind and should be deleted by hand.
`Release` calls the same workflow for `plan` and `apply`. The deployment role needs permission to create and delete the disposable `data-fetch-service-verify-*` bucket and queue, in addition to its release permissions.

The manual `Load and resource` workflow uses isolated in-memory source responses and temporary files. It measures a 64-MiB streamed CSV, a 32-MiB ZIP member, and a source that stalls until its request deadline. Its artifact contains test timing/allocation logs and `/usr/bin/time` peak process memory. Run it before raising source-size or concurrency limits; it does not call AWS or public source endpoints and does not prove deployed Lambda capacity.

## Rolling back

Code and configuration roll back together. Run `Release` at stage `apply` from the earlier commit (Run workflow → pick the tag or branch at that commit). Its packages are still in the package bucket, so the Lambdas return to exactly that build and configuration revision. Check the plan output before the apply step for anything other than Lambda code and environment changes.

## Operating

- **Failed run:** `GET /v1/runs/{run_id}` for the phase, `/history` for the failure stage and code, `/pulls` for per-job results.
- **File not published (`SOURCE_NOT_FOUND`), for example an exchange holiday:** no automatic recovery. Submit a manual run for the date you want with `POST /v1/events/daily-bhavcopy/runs`.
- **Pull failed for another reason:** `POST /v1/runs/{run_id}/reruns` repeats the whole event from its original snapshot and date.
- **Delivery failed but files are stored:** `POST /v1/runs/{run_id}/delivery-retries` resubmits without downloading again. It returns `410` once the artifacts have expired (30 days); use a full rerun then.
- **Automatic retries:** a crashed or timed-out pull is retried up to three times as linked child runs. Follow `latest_run_id` in the run detail.
- **Dead-letter queues:** `data-fetch-service-{ingress,pull,delivery}-dlq`. A message there has failed five receives. Read its body for the run ID, check that run's state, and redrive to the source queue only after the cause is fixed; the handlers treat an already-finished run as stale and acknowledge it.
- **Alarms:** queue oldest-message age, DLQ depth, handler retry outcomes, and Lambda errors. None has a notification target yet, so they must be watched in CloudWatch.
