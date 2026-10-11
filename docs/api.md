# Data Fetch Service API

How clients find, read, and call the public REST API. The OpenAPI document is the contract; this page explains how to use it.

## Where the API description lives

| What | Where |
|---|---|
| OpenAPI 3.1 document (source of truth) | [`internal/api/openapi.yaml`](../internal/api/openapi.yaml) |
| Same document from the deployed API | `GET https://fetch.kagent.app/docs/openapi.yaml` |
| Interactive Swagger UI | `https://fetch.kagent.app/docs` |

The API is not deployed yet (see [implementation status](plans/implementation-status.md)). Until it is, open the file from the repository in one of the viewers below.

The document is compiled into the API Lambda, so the deployed `/docs/openapi.yaml` always matches the deployed code.

## Viewing the document

- **Deployed Swagger UI:** open `https://fetch.kagent.app/docs`. Use **Try it out** to call the live API from the browser. POST routes start real runs.
- **Swagger Editor:** open <https://editor.swagger.io>, then File → Import file → `internal/api/openapi.yaml`.
- **Locally with Docker:**

  ```sh
  docker run --rm -p 8080:8080 -e SWAGGER_JSON=/spec/openapi.yaml \
    -v "$PWD/internal/api:/spec" swaggerapi/swagger-ui
  ```

  Then open <http://localhost:8080>.
- **Postman or Insomnia:** import `internal/api/openapi.yaml` (or the deployed URL) to get a ready-made collection.
- **Client code:** generate a typed client with any OpenAPI 3.1 generator, for example `npx @openapitools/openapi-generator-cli generate -i internal/api/openapi.yaml -g typescript-fetch -o client`.

The Swagger UI page loads Swagger UI 5.17.14 from `cdn.jsdelivr.net` with subresource-integrity hashes. The browser needs access to that CDN.

## Routes at a glance

| Method and path | Purpose |
|---|---|
| `POST /v1/events/{event_type}/runs` | Start a manual run |
| `POST /v1/runs/{run_id}/reruns` | Rerun a finished run from its original snapshot |
| `POST /v1/runs/{run_id}/delivery-retries` | Resubmit stored files to the processor without downloading again |
| `GET /v1/runs/{run_id}` | Run phase, timing, links, and snapshot |
| `GET /v1/runs/{run_id}/history` | Phase transitions |
| `GET /v1/runs/{run_id}/pulls` | Per-job source attempts and stored files |
| `GET /v1/runs/{run_id}/delivery` | Processor submission, attempts, and acceptance |
| `GET /v1/events/{event_type}` | Configured event definition |
| `GET /v1/events/{event_type}/runs` | Runs by UTC creation date |
| `GET /v1/events/{event_type}/active` | Active run for given inputs |
| `GET /v1/requests/{producer}/{event_id}` | Run created for an external queue trigger |
| `GET /docs`, `GET /docs/openapi.yaml` | This description |

Event types are `daily-bhavcopy` (BSE debt bhavcopy; inputs `exchangeName`, optional `run_date`) and `nsdl-bond-data` (six NSDL files for one ISIN; input `isin_code`).

## Common tasks

Start a BSE run for one trade date, with a key so a retry after a timeout is safe:

```sh
curl -sS -X POST https://fetch.kagent.app/v1/events/daily-bhavcopy/runs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: bse-2026-10-09' \
  -d '{"inputs": {"exchangeName": "BSE", "run_date": "2026-10-09"}}'
```

The response is `202` with `run_id` and `status_url`. Poll the run until its phase is terminal (`completed`, `skipped_unchanged`, or `failed`):

```sh
curl -sS https://fetch.kagent.app/v1/runs/<run_id>
```

If an automatic retry happened, `latest_run_id` and `latest_phase` point to the newest attempt. When a run fails, read `/history` for the failure code and `/pulls` or `/delivery` for the details.

Fetch NSDL data for an ISIN even if it is unchanged since the last delivery:

```sh
curl -sS -X POST https://fetch.kagent.app/v1/events/nsdl-bond-data/runs \
  -H 'Content-Type: application/json' \
  -d '{"inputs": {"isin_code": "INE121A07QY9"}, "force": true}'
```

List today's BSE runs, then the next page:

```sh
curl -sS 'https://fetch.kagent.app/v1/events/daily-bhavcopy/runs?limit=50'
curl -sS 'https://fetch.kagent.app/v1/events/daily-bhavcopy/runs?limit=50&cursor=<next_cursor>'
```

Recover a failed run: use `/delivery-retries` when downloads succeeded but delivery failed (within 30 days), otherwise `/reruns`. The [runbook](runbook.md#operating) covers when to use which.

## Behaviour clients should rely on

- **No authentication.** Anyone who can reach the endpoint can start runs. Bodies only choose configured events and inputs.
- **Throttling.** 10 requests per second, bursts of 20. Back off on `429`.
- **Status codes.** `202` admitted or joined an active run (`reused: true`). `200` replays a finished request. `400` invalid request. `404` unknown resource. `409` conflicting key or a state that forbids the action. `410` expired (30-day retention). `413` body over 64 KiB. `503` retry later with the same `Idempotency-Key`.
- **Errors** are `{"error": "<CODE>"}`, apart from API Gateway's `429` body. Codes are listed in the `Error` schema.
- **Unknown fields are rejected** with `400`, so send only documented fields.
- **Pagination.** Pass `next_cursor` back unchanged; an empty string means the last page. `limit` is 1–100, default 25.
- **Processor acceptance** is HTTP 202 from the processor only, recorded as an `acceptance` transition. This API has no callback for processing completion downstream.

## Keeping the document current

`make check` runs `internal/api/openapi_test.go`, which fails when:

- the document's routes differ from the Go router or the API Gateway routes in `infra/service/api.tf`;
- the `event_type` enum differs from the enabled events in `config/events.yaml`;
- a `$ref` does not resolve.

When changing a route, request, or response, update `internal/api/openapi.yaml` in the same change. Response schemas are not generated from the code, so review them by hand. For a full lint, run `npx @redocly/cli@1.25.11 lint internal/api/openapi.yaml`.
