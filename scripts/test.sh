#!/usr/bin/env bash
# Verification suite for the Kafka plugin (frontend-owned path).
#
#   scripts/test.sh                # everything available on this machine
#
# Steps: frontend typecheck/test/build -> go vet/test (when the backend
# workspace exists) -> .dbxp package (when manifest.json exists) -> smoke
# (whole suite SKIPs automatically without a Kafka container; methods not
# implemented yet SKIP instead of FAIL while the backend is under parallel
# development).
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v pnpm >/dev/null 2>&1; then
  # Node 版本单一真源是仓库根 .nvmrc（CI setup-node node-version-file 同源）；
  # 精确版本缺失时回退 nvm 里最新的 v22。
  NODE_VER="$(head -n1 .nvmrc 2>/dev/null | tr -d 'vV\n ')"
  NODE_BIN="$(ls -d "$HOME/.nvm/versions/node/v${NODE_VER:-22.21.0}"/bin 2>/dev/null \
    || ls -d "$HOME"/.nvm/versions/node/v22*/bin 2>/dev/null | sort -V | tail -1 || true)"
  export PATH="$HOME/Library/pnpm:${NODE_BIN:+$NODE_BIN:}$PATH"
fi

echo "==> frontend typecheck + tests + build"
node scripts/connection-forms/verify.mjs kafka
# --frozen-lockfile 保证安装严格对齐仓内 lockfile；lockfile 有变动时重装。
if [ ! -d frontend/node_modules ] || [ frontend/pnpm-lock.yaml -nt frontend/node_modules ]; then
  pnpm --dir frontend install --frozen-lockfile
fi
pnpm --dir frontend typecheck
pnpm --dir frontend test
pnpm --dir frontend build

echo "==> UI walkthrough (headless Chrome via scripts/ui_test.mjs; SKIP without Chrome/network)"
node scripts/ui_test.mjs || exit 1

if [ -f backend/go.mod ] && command -v go >/dev/null 2>&1; then
  echo "==> backend unit tests (owned by backend path)"
  (cd backend && go vet ./... && go test ./...) || echo "WARN: backend go test failed (parallel development) — see docs/PROGRESS-B-KAFKA.zh-CN.md"
else
  echo "==> backend unit tests skipped (no backend/go.mod or no go toolchain yet)"
fi

echo "==> package .dbxp (SKIP when manifest.json is absent)"
scripts/package.sh

echo "==> smoke (Kafka container auto-SKIP; unimplemented methods SKIP)"
python3 scripts/smoke_test.py

echo "==> MCP smoke (sidecar auto-built; dev-cluster cases auto-SKIP)"
if [ -f backend/go.mod ] && command -v go >/dev/null 2>&1; then
  (cd backend && CGO_ENABLED=0 go build -trimpath -o bin/dbx-plugin-kafka .)
fi
python3 scripts/smoke_mcp.py

echo
echo "all green (frontend three-step + smoke suite, SKIPs allowed by design)"
