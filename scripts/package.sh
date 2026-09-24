#!/usr/bin/env bash
# Shared packaging step for build.sh and test.sh:
#   1. resolve the packaging CLI — native binary first, then the npm wrapper,
#      then a cargo build of the CLI from the DBX host worktree;
#   2. run `dbx-plugin package .` with the manifest version injected into the
#      CLI's own sidecar rebuild (plain `go build`, no ldflags support -> via
#      GOFLAGS) so the packaged identity matches manifest.json (the host
#      rejects mismatches at init: "backend identity ... does not match
#      manifest");
#   3. clean dist/ of .dbxp/.artifact.json files from older manifest versions
#      so only the current release line survives (current-version artifacts of
#      other targets are kept for the CI per-target upload).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f manifest.json ]; then
  echo "SKIP: manifest.json not present yet (backend path owns it); frontend artifacts are in ui/"
  exit 0
fi

# Identity version comes from manifest.json — the single source of truth.
PLUGIN_VERSION="$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' manifest.json | head -1)"
PLUGIN_VERSION="${PLUGIN_VERSION:-0.0.0-dev}"

# The npm CLI wrapper re-injects DBX_PLUGIN_SDK_ROOT whenever it is unset, and
# its bundled SDK builds the Go sidecar through a go.work pinned to go 1.22,
# which conflicts with backend go.mod requiring >= 1.26 — so the native binary
# is always preferred. The platform package suffix is resolved per-machine
# (linux uses a -gnu suffix), so cross-platform CI never falls back silently.
. scripts/cli-platform.sh
NATIVE_CLI=""
if ! NATIVE_CLI="$(resolve_native_plugin_cli)" && ! command -v dbx-plugin >/dev/null 2>&1; then
  HOST="${DBX_HOST_WORKTREE:-$PWD/../dbx-plugin-host-worktree}"
  echo "==> building plugin-cli from host worktree: $HOST"
  (cd "$HOST/plugins/sdk/cli" && cargo build --release)
  export PATH="$HOST/plugins/sdk/cli/target/release:$PATH"
fi

echo "==> package .dbxp (v${PLUGIN_VERSION})"
if [ -n "$NATIVE_CLI" ]; then
  env -u DBX_PLUGIN_SDK_ROOT NO_COLOR=1 GOFLAGS="-ldflags=-X=main.version=${PLUGIN_VERSION}" "$NATIVE_CLI" package .
else
  echo "WARN: native plugin-cli for $(uname -s)/$(uname -m) not found; falling back to the npm wrapper (its bundled SDK may conflict with backend go.mod)" >&2
  env -u DBX_PLUGIN_SDK_ROOT NO_COLOR=1 GOFLAGS="-ldflags=-X=main.version=${PLUGIN_VERSION}" dbx-plugin package .
fi

echo "==> clean dist/ artifacts older than v${PLUGIN_VERSION}"
python3 - "$PLUGIN_VERSION" <<'PY'
import pathlib
import re
import sys

current = sys.argv[1]
# Plain SemVer core only: an unparseable or prerelease filename is kept rather
# than risk deleting a current-version artifact on a bad guess.
version_in_name = re.compile(r"-(\d+\.\d+\.\d+)-")
removed = []
dist = pathlib.Path("dist")
if dist.is_dir():
    for path in sorted(dist.iterdir()):
        if not path.name.endswith((".dbxp", ".artifact.json")):
            continue
        m = version_in_name.search(path.name)
        if m and m.group(1) != current:
            path.unlink()
            removed.append(path.name)
print("\n".join(f"  removed {name}" for name in removed) if removed else "  none")
PY

echo
echo "Artifacts:"
ls -la dist/*.dbxp dist/*.artifact.json 2>/dev/null || true
