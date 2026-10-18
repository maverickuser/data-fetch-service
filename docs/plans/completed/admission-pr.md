# PR 04: admission and initial REST reads

Base: `stack/03-state` (`8763839`). Branch: `stack/04-admission`.

Manual HTTP and ingress SQS requests now share durable admission. Identical requests retain their first configuration and logical date, including concurrent first arrivals across midnight or deployments. Conflicting inputs/force remain conflicts; equivalent active work joins its original run. Queue publication uses the persisted dispatch intent. SQS acknowledges permanent invalid events only after writing a rejection receipt and retries transient record failures independently.

The API includes manual creation, run/snapshot detail, history, pull/delivery metadata, event definition/revision, active-run lookup, request lookup and date-bounded listing. Reads do not perform recovery. Listing scans cap partitions and candidate records, bind cursors to filters and return compact summaries. Bodies, metadata aggregation and encoded Lambda responses have explicit budgets.

`cmd/api` and `cmd/admission` wire the tested handlers to Lambda, SDK S3/SQS and validated repository configuration. `config/native-events.yaml` permits explicit legacy/native producer mappings; the empty default grants no producer mapping. New schedules continue to use CloudEvents.

Validation: `make check` passes formatting/vet/staticcheck/errcheck, race/unit tests, 1242/1284 unit statements (96.73%), separate mocked SDK/composition/HTTP integration, native/arm64 builds, config/native-mapping validation, documentation links and processor OpenAPI parity. Independent review evidence is stored under `.reports/` for the committed head.

No AWS deployment or live source calls. Source acquisition, pull/delivery execution, recovery operations and Terraform remain later increments. Manual rerun/delivery-retry endpoints are introduced in PR 09.
