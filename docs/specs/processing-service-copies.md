# Copies from data-processing-service

These files are byte-for-byte copies from [data-processing-service](https://github.com/maverickuser/data-processing-service), kept here so an agent working in this repository can read the processor's design and admission contract without leaving the checkout. Never edit them here: change them in data-processing-service, merge there, then re-sync.

Source commit: `f5c122e29a72` (origin/main, committed 2026-10-18).

| Copy | Role here |
|---|---|
| [structured-file-processing-lld.md](structured-file-processing-lld.md) | Processor LLD; external reference. Its relative links point into the processing repository and are not checked here |
| [structured-file-processing-spec.md](structured-file-processing-spec.md) | Processor functional specification; external reference |
| [data-processing-service-openapi.yaml](data-processing-service-openapi.yaml) and [.json](data-processing-service-openapi.json) | Authoritative admission contract that Delivery calls; `make check-contract` checks the two forms agree |

Re-sync after any merged change to these files in data-processing-service, in a `docs/` PR here:

```sh
make sync-processing-docs                       # copies from ../data-processing-service at origin/main
make sync-processing-docs PROCESSING_DIR=/path  # another checkout
```

`python3 scripts/sync_processing_docs.py --check ../data-processing-service` reports stale copies without changing anything.
