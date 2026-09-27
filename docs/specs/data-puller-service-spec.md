# Data Puller Service Specification

## 1. Purpose

The Data Puller Service retrieves structured data from public HTTP endpoints that return CSV, JSON, or ZIP archives containing a CSV or JSON file. It stores every retrieved response unchanged, extracts one configured file from ZIP responses, detects duplicate raw data, and automatically hands new raw data to the structured-file processor.

The service is responsible for data acquisition, not business-field validation or domain mapping.

```text
HTTP endpoint
  -> Data Puller Service
  -> If ZIP: extract one configured CSV or JSON file
  -> Raw CSV or JSON artifact
  -> Queue event: raw-file-ready
  -> Structured File Processor
  -> Canonical records -> validated internal records
```

The Structured File Processor is specified separately in `structured-file-processing-spec.md`.

## 2. Initial scope

The first version supports:

- Public, unauthenticated HTTP APIs
- HTTP `GET` requests
- CSV and JSON responses
- ZIP responses with exactly one selected CSV or JSON member for processing
- Full-response retrieval only
- Manual, scheduled, and queue-event triggers
- Controlled URL templates with runtime variables
- One queue event faning out to multiple independent API pulls
- Retry handling for transient failures
- Content-hash duplicate detection
- Asynchronous handoff to the structured-file processor

The first version does not support:

- HTML scraping or browser automation
- Authentication, API keys, OAuth, cookies, or client certificates
- Incremental synchronization, cursors, pagination, or `updated_since` requests
- Arbitrary executable expressions in URL templates
- Dependent workflows in which one API response determines calls to another API
- Encrypted ZIP archives, nested archive extraction, or processing multiple members from one archive

## 3. Responsibilities and boundary

| Data Puller Service | Structured File Processor |
|---|---|
| Resolve configured URL templates | Parse CSV or JSON content |
| Make HTTP requests | Build canonical source records |
| Retry transient request failures | Validate source headers or JSON paths |
| Confirm expected response format | Validate types and source values |
| Extract one configured file from ZIP responses | Parse the extracted CSV or JSON using its contract |
| Store raw response artifacts | Quarantine invalid records |
| Calculate content hashes | Map source fields to internal fields |
| Skip unchanged response processing | Validate internal business rules |
| Publish `raw-file-ready` events | Persist validated internal records |

## 4. Pull jobs

A pull job is a configuration for one HTTP endpoint. Every pull job must declare an explicit, versioned processing contract. The service does not infer business meaning from the endpoint URL, CSV headers, JSON shape, or HTTP content type.

```yaml
pull_job:
  id: bse-debt-bhavcopy
  triggers:
    - manual
    - schedule
    - queue_event

  request:
    method: GET
    url_template: "https://www.bseindia.com/download/BhavCopy/Debt/{file_name}"

  response:
    format: csv

  processing_contract: bse-debt-bhavcopy-csv-v1
```

The response format identifies the downloaded payload. For direct CSV or JSON responses, it also specifies the raw data format. For ZIP responses, the extraction configuration specifies the selected member and its CSV or JSON format. The processing contract specifies the expected source schema, validation rules, and mapping to the internal model of the raw data file.

### ZIP responses

A ZIP pull job downloads and preserves the original archive, then extracts exactly one configured member as the raw data artifact for downstream processing.

```yaml
pull_job:
  id: daily-prices-zip
  triggers:
    - manual
    - schedule
  request:
    method: GET
    url_template: "https://example.com/downloads/daily-prices.zip"
  response:
    format: zip
    extraction:
      member_path: "reports/prices.csv"
      format: csv
  processing_contract: daily-prices-csv-v1
```

- `member_path` selects an exact, case-sensitive path inside the archive. It may use the same declared variables and controlled date formatting as URL templates; member-path values are not URL-encoded.
- Exactly one regular file must match. A missing member or duplicate entries matching the configured path cause the pull to fail. Other members are ignored; the service never selects the first file implicitly.
- The extracted member must match the declared `csv` or `json` format. Its bytes are stored unchanged as the raw artifact.
- Reject corrupt or encrypted archives, unsafe member paths (including absolute paths and `..` traversal), and selected members that are symbolic links. Extraction must not write outside its designated workspace.
- Enforce configured limits on download size, archive entry count, and selected member uncompressed size and compression ratio. Check limits while extracting, not solely against archive metadata.
- Downstream processing receives the extracted file and its processing contract, not the ZIP archive.

## 5. Pull triggers

Pull jobs may run from any of the following triggers:

```text
Manual request --+
Schedule --------+--> Pull run
Queue event -----+
```

All triggers create the same pull-run record. A manual or queue-triggered request may additionally provide variables used by a URL template.

## 6. URL templates and variables

The request URL may include declared variable placeholders.

```text
https://www.bseindia.com/download/BhavCopy/Debt/{file_name}
https://www.indiabondinfo.nsdl.com/bds-service/v1/public/isins?isin={isin_code}
```

Variable values can originate from:

- The current run date or time
- A manual trigger request
- A queue event

Example queue event:

```json
{
  "event_type": "bond-data-requested",
  "event_id": "evt_001",
  "variables": {
    "isin_code": "INE831R08076"
  }
}
```

The service URL-encodes values before insertion. Templates only permit declared placeholders; they do not permit arbitrary code execution.

Example BSE configuration with a date-derived filename:

```yaml
pull_job:
  id: bse-debt-bhavcopy
  request:
    url_template: "https://www.bseindia.com/download/BhavCopy/Debt/{file_name}"
    variables:
      file_name:
        source: derived
        template: "DebtBhavCopy_{run_date:yyyyMMdd}.csv"
  response:
    format: csv
  processing_contract: bse-debt-bhavcopy-csv-v1
```

## 7. Pull groups and multi-API fan-out

A pull group is a set of independent pull jobs triggered by one event. Every job gets its own run, status, raw artifact, hash, and processing contract. All jobs share a correlation ID from the triggering event.

The initial version supports independent fan-out; it does not require one API response to calculate a subsequent API request.

### Event completion and reruns

All pulls belonging to a specific event must succeed before any of that event's raw files are handed to downstream processing. Jobs execute independently, but share this completion gate.

- Create a group-run record for each execution of the event, listing every required job. All child pull runs reference its `group_run_id` as well as the original event correlation ID.
- Keep the group `running` until all required jobs finish. Mark it `succeeded` only when every pull succeeds, including successful unchanged pulls. If any job fails after its applicable retries, mark the group `failed`; there is no `partially_succeeded` outcome.
- Stage successful artifacts while other jobs run. A failed group publishes no `raw-file-ready` events, even for its successful jobs.
- Rerunning an event reruns the entire group with the same event variables and resolved date inputs, under a new group-run ID. Every required pull must succeed in that execution; do not combine partial results from different executions to satisfy the gate.
- Pulls and downstream writes are idempotent. A successful rerun may replace existing results for the same logical data keys rather than create duplicate records. Preserve execution history for diagnosis.
- This gate ensures that all source pulls succeed before handoff. It does not make downstream processing of multiple files a single atomic transaction.

```text
bond-data-requested (isin_code = INE831R08076)
  |
  +--> ISIN details       -> isins-json-v1
  +--> Instrument details -> instruments-json-v1
  +--> Coupon details     -> coupons-json-v1
  +--> Redemption details -> redemptions-json-v1
  +--> Credit ratings     -> credit-ratings-json-v1
  +--> Listings           -> listings-json-v1
```

Example NSDL group:

```yaml
pull_group:
  id: nsdl-bond-data
  trigger:
    event_type: bond-data-requested
  required_variables:
    - isin_code

  jobs:
    - id: nsdl-isin-details
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/isins?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-isin-details-json-v1

    - id: nsdl-instrument-details
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/instruments?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-instruments-json-v1

    - id: nsdl-coupon-details
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-coupons-json-v1

    - id: nsdl-redemptions
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/redemptions?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-redemptions-json-v1

    - id: nsdl-credit-ratings
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/credit-ratings?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-credit-ratings-json-v1

    - id: nsdl-listings
      request:
        url_template: "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/listings?isin={isin_code}"
      response:
        format: json
      processing_contract: nsdl-listings-json-v1
```

Each endpoint has a different contract because it returns different data:

| Endpoint | Contract responsibility |
|---|---|
| `isins` | Core security identity fields |
| `instruments` | Instrument attributes |
| `coupondetail` | Coupon schedules and rate data |
| `redemptions` | Maturity and redemption data |
| `credit-ratings` | Rating agency and rating history data |
| `listings` | Exchange-listing data |

## 8. Pull-run process

For every pull job execution, the service performs the following process:

```text
Create pull run
  -> Resolve and validate URL template
  -> Fetch HTTP response
  -> Retry transient failures when needed
  -> Confirm successful HTTP response and expected CSV/JSON/ZIP format
  -> Store downloaded response unchanged
  -> If ZIP: select and extract one member, validate its CSV/JSON format,
             and store it unchanged as the raw data artifact
  -> Calculate content hash of the raw CSV/JSON artifact
  -> Compare against last successfully processed response for this job
       -> unchanged: record success and skip processing
       -> new: mark artifact ready for handoff
  -> Standalone job: publish raw-file-ready for new data
  -> Group job: wait until every required pull in this group run succeeds,
                then publish raw-file-ready for its new data
```

The service must reject a response that is not in the expected format, even when its HTTP status is successful. For example, an HTML login or error page returned with HTTP 200 must not be passed to the JSON or CSV processor or treated as a ZIP archive. For ZIP pulls, both the archive and the selected member must pass their respective format checks.

## 9. Raw artifact storage and lineage

Each response is stored independently and unchanged. An NSDL pull group may create artifacts such as:

```text
raw/nsdl/INE831R08076/isin-details.json
raw/nsdl/INE831R08076/instruments.json
raw/nsdl/INE831R08076/coupons.json
raw/nsdl/INE831R08076/redemptions.json
raw/nsdl/INE831R08076/credit-ratings.json
raw/nsdl/INE831R08076/listings.json
```

Reruns may overwrite or replace the current logical artifacts and downstream records. Queue events must nevertheless reference a stable artifact version, such as a pull-run-specific key or object-storage version, so a later rerun cannot change the bytes consumed by an earlier event. Stage each group execution separately and promote its results only after all required pulls succeed; a failed rerun must not replace the previous successful group's current artifacts. Retain pull-run history and hashes.

For ZIP pulls, preserve both the original archive and the extracted raw file, including when the raw file is unchanged. The pull-run record additionally includes `downloaded_artifact_location`, `selected_member_path`, and `raw_format`; `response_format` is `zip`, while `raw_artifact_location` points to the extracted CSV or JSON file. The processing contract applies to that extracted file.

Each pull run records its source lineage:

```json
{
  "pull_run_id": "pull_001",
  "correlation_id": "evt_001",
  "pull_job_id": "nsdl-coupon-details",
  "resolved_url": "https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin=INE831R08076",
  "response_format": "json",
  "processing_contract": "nsdl-coupons-json-v1",
  "raw_artifact_location": "raw/nsdl/INE831R08076/coupons.json",
  "content_hash": "sha256:...",
  "status": "succeeded"
}
```

## 10. Duplicate handling

The service calculates a content hash for every successfully retrieved raw CSV or JSON artifact. For ZIP pulls, it hashes the extracted member bytes, not the archive bytes, so changes to ZIP metadata or unrelated members do not trigger processing. It compares the hash against the last successfully processed raw artifact from the same pull job.

- If the hash matches, the service records a successful but unchanged pull and does not publish a processing event.
- If the hash differs, the service marks the stored raw artifact for processing. For grouped pulls, publication waits until every required pull in the same group run succeeds.

Scope comparisons to the pull job, resolved request variables, and processing contract version so different inputs are tracked independently. Artifacts staged by a failed group do not advance the successfully processed baseline. Unchanged content that was never successfully processed remains eligible for handoff after a successful rerun.

This avoids repeating downstream processing for identical full responses while retaining pull history.

## 11. Queue handoff to structured-file processing

For each new, non-duplicate artifact, the Data Puller publishes a `raw-file-ready` event. For event-triggered groups, no events are published until all required pulls in that group execution succeed. Include `group_run_id` on grouped events to identify the successful execution.

Persist pending handoffs durably when the group succeeds and retry interrupted publication until all pending events are delivered. Duplicate delivery and reruns must be safe: the processor uses the contract's logical record keys to upsert or replace existing data. Group success describes pull completion, not completion of downstream validation or persistence.

Example event:

```json
{
  "event_type": "raw-file-ready",
  "correlation_id": "evt_001",
  "pull_run_id": "pull_001",
  "artifact_location": "raw/nsdl/INE831R08076/coupons.json",
  "format": "json",
  "processing_contract": "nsdl-coupons-json-v1",
  "content_hash": "sha256:..."
}
```

For ZIP pulls, `artifact_location` refers to the extracted member, `format` is `csv` or `json`, and `content_hash` is the hash of that member. The original archive and selected member path remain available through the pull-run lineage. The event schema does not require the structured-file processor to support ZIP archives.

The structured-file processor consumes this event and applies the referenced contract to construct canonical records, validate source data, quarantine invalid records, map accepted values to the internal model, and validate internal business rules.

## 12. Reliability

- Retry transient network and server failures for up to three total attempts.
- Record every request attempt, response status, and failure reason.
- Do not retry deterministic failures such as invalid URL-template variables, malformed CSV/JSON, or an unexpected response content type.
- ZIP extraction failures such as corrupt or encrypted archives, missing or ambiguous member selection, unsafe paths, or exceeded extraction limits are deterministic failures. Record the reason and do not publish a `raw-file-ready` event.
- Treat each fan-out job as an independent execution with separate retry and status tracking.
- Require all pulls for an event to succeed before releasing any artifacts for processing. Failed groups can be rerun in full; idempotent writes allow existing logical results to be replaced safely.
- Preserve a correlation ID across the triggering event, pull runs, raw artifacts, and downstream processing events.

## 13. Endpoint policy

The initial version does not require a hostname allowlist. Authorized users may configure URL templates for any public HTTPS endpoint without a separate hostname approval step.

The service continues to validate URL templates and enforce the public-endpoint scope, including for redirect destinations. A hostname allowlist may be introduced later if needed.
