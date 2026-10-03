#!/usr/bin/env bash
# Build one reproducible Lambda ZIP per handler and a manifest of their hashes.
# Each ZIP holds the arm64 `bootstrap` binary and the version-controlled config.
set -euo pipefail

GO="${GO:-go}"
out="${1:-dist}"
kinds=(api admission pull delivery reconciler)

rm -rf "$out"
mkdir -p "$out"
revision=$("$GO" run ./cmd/configcheck -config config/events.yaml | sed -n 's/.*revision=\(sha256:[0-9a-f]\{64\}\).*/\1/p')
if [ -z "$revision" ]; then
  echo "configuration revision could not be determined" >&2
  exit 1
fi

hashes=""
for kind in "${kinds[@]}"; do
  stage="$out/stage/$kind"
  mkdir -p "$stage/config/environments"
  GOOS=linux GOARCH=arm64 CGO_ENABLED=0 "$GO" build -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o "$stage/bootstrap" "./cmd/$kind"
  cp config/events.yaml config/native-events.yaml "$stage/config/"
  cp config/environments/prod.yaml "$stage/config/environments/"
  # Fixed timestamps and sorted entries keep the archive hash stable for identical inputs.
  find "$stage" -exec touch -t 198001010000 {} +
  (cd "$stage" && find . -type f | LC_ALL=C sort | zip -X -q "../../$kind.zip" -@)
  digest=$(openssl dgst -sha256 -binary "$out/$kind.zip" | openssl base64 -A)
  hashes="$hashes\"$kind\":\"$digest\","
done
rm -rf "$out/stage"

printf '{"config_revision":"%s","package_sha256":{%s}}\n' "$revision" "${hashes%,}" > "$out/release.json"
cat "$out/release.json"
