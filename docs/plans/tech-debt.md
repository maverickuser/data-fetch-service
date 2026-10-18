# Tech debt

Known shortcuts, gaps, and stale material that no current PR fixes. Add an entry when a review or a PR leaves something knowingly unfinished; remove it in the PR that fixes it. Each entry links to where the gap is recorded; confirm it is still open before acting on it.

| Added | Item | Why it matters | Source |
|---|---|---|---|
| 2026-10-18 | The smoke suite does not verify the processor's own S3 read access to artifacts, and a BSE run that ends `skipped_unchanged` passes without a processor handoff being observed | A broken processor read grant or handoff can pass smoke | [Live verification PR](completed/live-verification-pr.md); [implementation context](data-fetch-service-implementation-context-spec.md), "Processor S3 read role" |
| 2026-10-18 | Recorded as pending: typical and maximum BSE ZIP/CSV and NSDL response sizes, and the processor latency and availability target | Memory, `/tmp`, timeout, and delivery retry settings are not tuned to measured values | [Implementation context](data-fetch-service-implementation-context-spec.md) |
| 2026-10-18 | Recorded as pending: alert owners and on-call route | Alarms have no named owner | [Implementation context](data-fetch-service-implementation-context-spec.md) |
