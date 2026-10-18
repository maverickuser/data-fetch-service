# PR 05: acquisition and artifact storage

Base: `stack/04-admission` (`eefd677`). Branch: `stack/05-acquisition`.

Adds per-job acquisition with immutable started/finished records, three transient source attempts, bounded downloads, and a shared temporary-disk budget. BSE preserves its original archive and uploads only the configured fgroup member with the event-derived exchange prefix. Direct JSON keeps its configured ISIN filename. Source dates and names come from the admitted snapshot.

HTTPS transport validates DNS addresses before each connection, pins the checked IP, and validates redirects. Complete response, encoding, content type, full-stream CSV/JSON syntax, ZIP paths, exact member selection, directory metadata, decompression limits, and selected-member checksum are enforced. Temporary files, validation goroutines, and multipart uploads have owned cleanup paths. Invalid content cannot complete an upload.

Artifact storage uses serial 5 MiB multipart buffers, conditional completion, and SHA-256 over unchanged bytes. Existing objects and uncertain completion replies are reconciled by hashing their full bounded contents. Failed uploads are aborted with a separate cleanup deadline. Disk/S3 failures remain separate from source retries.

New configuration: `max_json_depth: 128`, `max_zip_metadata_bytes: 8388608`, and `max_temp_bytes: 3221225472`. These fields are required in effective configuration; this is a pre-deployment configuration change. No existing production snapshots require migration.

Validation: `make check` passes lint, race/unit tests (1849/1937 statements, 95.46%), separate BSE/NSDL acquisition through the real S3 SDK with mocked HTTP, existing API/state integration, native/arm64 builds, config/docs checks, and processor OpenAPI parity. Independent review status is recorded in the implementation status and local reports.

Pull Lambda/group orchestration and manifest publication belong to PR 06. No AWS provisioning, deployment, or live source calls are performed in this increment.
