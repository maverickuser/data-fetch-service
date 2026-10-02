# data-fetch-service

Go service for BSE and NSDL acquisition and immutable S3 manifest delivery.
Implementation is incremental; AWS deployment is deferred until the complete stack passes local review.

Use Go **1.27.1** Python 3, and Ruby (standard-library YAML/JSON only). Run `make tools` once (network access required), add Go’s binary installation directory to PATH, then run `make check` for local formatting/vet/staticcheck/errcheck, race tests, unit coverage strictly greater than 95%, native/Linux arm64 builds, and documentation checks.

Put the Go toolchain bin directory and the directory containing staticcheck/errcheck on PATH before running checks. For this workspace:

```sh
export PATH="$HOME/.local/share/data-fetch-toolchain/go/bin:$HOME/.local/share/data-fetch-toolchain/bin:$PATH"
make check
```

Read [agent guidance](AGENTS.md), [documentation](docs/README.md), and [implementation status](docs/plans/implementation-status.md).
