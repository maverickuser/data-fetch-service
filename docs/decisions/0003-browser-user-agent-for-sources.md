# 0003 — Browser User-Agent for source requests

Date: 2026-10-11. Status: accepted.

## Context

The first release `plan` (run 38139010371) failed its source preflight: NSDL reset the connection for curl's default User-Agent. Manual probes on 2026-10-11 against a real BSE file (`DEBTBHAVCOPY09102026.zip`) and the NSDL `isins` endpoint showed that both sources filter on User-Agent:

| User-Agent | BSE file | NSDL |
|---|---|---|
| `Go-http-client/1.1` (the Go default the Pull Lambda sent) | 403 | connection reset |
| curl default | 200 (folder) | connection reset |
| `data-fetch-service/1.0 (+https://fetch.kagent.app)` | 403 | 200 |
| `Mozilla/5.0 (compatible; data-fetch-service/1.0)` | 403 | 200 |
| Full Chrome User-Agent | 200 | 200 |

Unit and mocked integration tests use fake sources, so they could not detect this. Without a change, every production BSE and NSDL acquisition would fail.

## Decision

`defaults.source_user_agent` in `config/events.yaml` is required, 1–256 printable ASCII characters, and set to a full Chrome User-Agent. `DownloadSource` sends it on every source request. The owner chose this over an identifying service User-Agent, which BSE rejects. The release preflight probes both sources with the same value and checks a real NSDL ISIN from stage `plan` onward.

## Consequences

- The value is pinned in each run's snapshot, so retries of a run use the User-Agent it was admitted with.
- Requests no longer identify this service to the sources. If BSE or NSDL publish an access policy or offer an identified-client route, revisit this decision.
- The sources may change their filtering. A source rejection then shows as `SOURCE_TRANSPORT` or a 403 attempt failure, and the release preflight fails before AWS changes.
