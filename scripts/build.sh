#!/usr/bin/env bash
# Build the Kafka plugin frontend and package a .dbxp for the current platform.
# Frontend three-step (typecheck/test/build) is owned by this path; the Go
# backend + manifest.json are owned by the backend path — packaging (CLI
# resolution, sidecar identity injection, old-artifact cleanup) lives in
# scripts/package.sh and runs only when manifest.json exists.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v pnpm >/dev/null; then
  export PATH="$HOME/Library/pnpm:$HOME/.nvm/versions/node/v22.21.0/bin:$PATH"
fi

# Release CI builds the target-independent UI once per plugin and stages ui/
# here as an artifact; DBX_PREBUILT_UI=1 packages it as-is instead of rerunning
# the frontend three-step on every platform job.
if [ "${DBX_PREBUILT_UI:-0}" = "1" ]; then
  if [ ! -f ui/index.html ]; then
    echo "DBX_PREBUILT_UI=1 but ui/index.html is missing; stage the CI frontend artifact first" >&2
    exit 1
  fi
  echo "==> frontend: skipped (prebuilt ui/ staged by CI)"
else
  echo "==> frontend: install + typecheck + test + build"
  if [ ! -d frontend/node_modules ]; then
    pnpm --dir frontend install
  fi
  pnpm --dir frontend typecheck
  pnpm --dir frontend test
  pnpm --dir frontend build
fi

if [ ! -f manifest.json ]; then
  echo "SKIP: manifest.json not present yet (backend path owns it); frontend artifacts are in ui/"
  exit 0
fi

# Identity version comes from manifest.json — the single source of truth — so
# the sidecar's reported identity always matches the manifest (the host rejects
# mismatches at init: "backend identity ... does not match manifest").
PLUGIN_VERSION="$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' manifest.json | head -1)"
PLUGIN_VERSION="${PLUGIN_VERSION:-0.0.0-dev}"

# Build the Go sidecar first when the backend workspace is present.
# backend/bin is for local smoke/debug; scripts/package.sh lets the packaging
# CLI rebuild the sidecar itself with the same version injected via GOFLAGS,
# keeping the local output and the packaged sidecar on one version. The local
# output keeps a Windows .exe suffix so the offline smoke can execute it on
# every CI platform.
unset DBX_PLUGIN_SDK_ROOT
SIDECAR_BIN="bin/dbx-plugin-kafka"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) SIDECAR_BIN="bin/dbx-plugin-kafka.exe" ;;
esac
if [ -f backend/go.mod ] && command -v go >/dev/null 2>&1; then
  echo "==> backend: go build (owned by backend path; failures are recorded in docs/PROGRESS-B-KAFKA.zh-CN.md)"
  (cd backend && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${PLUGIN_VERSION}" -o "$SIDECAR_BIN" .) || {
    echo "WARN: go build failed (backend under parallel development); packaging will fail until it is green"
  }
fi

scripts/package.sh
