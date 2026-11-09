#!/usr/bin/env bash
# Runs the kind end-to-end suite and writes e2e/RESULTS.md.
#
#   ./e2e/run.sh                         run every scenario
#   ./e2e/run.sh 'TestE2E/split_brain'   run the scenarios matching a go test -run pattern
#
# Environment:
#   E2E_KEEP_CLUSTER=1   keep the kind cluster afterwards, and reuse it next time
#   E2E_TIMEOUT=120m     overall time limit
set -euo pipefail

cd "$(dirname "$0")/.."

KIND_VERSION=v0.27.0
BIN_DIR="$PWD/e2e/.bin"
mkdir -p "$BIN_DIR"
export PATH="$BIN_DIR:$PATH"

for tool in go docker kubectl; do
  command -v "$tool" >/dev/null || { echo "e2e: $tool is required" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "e2e: the Docker daemon is not running" >&2; exit 1; }

if ! command -v kind >/dev/null; then
  echo "e2e: installing kind $KIND_VERSION into e2e/.bin"
  (cd "$BIN_DIR" && GOBIN="$BIN_DIR" go install "sigs.k8s.io/kind@$KIND_VERSION")
fi

exec go test -tags e2e -v -count=1 -timeout "${E2E_TIMEOUT:-120m}" ./e2e/ -run "${1:-TestE2E}"
