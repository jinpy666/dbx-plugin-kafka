# Kafka Plugin Standalone Repository Migration Notes

## Origin and history

This repository was split out of `/Users/Jinpy/btroot/dbx-plugins/kafka`. The
migration baseline is monorepo commit
`ce19beb4fa75cc79048e3ba8852c298d674fd9b3`. The initial import used
`git subtree split --prefix=kafka`, so the Kafka-directory commit history is
preserved.

The source repository's `host` checkout (including its unrelated "M host"
submodule state) was not updated, reset, cleaned, or written during the
migration. Monorepo-ignored working-tree artifacts (screenshots, runtime
state) do not travel with a subtree split.

## Standalone repository contents

- The repository root directly contains `manifest.json`, `dbx-plugin.toml`,
  `frontend/`, `backend/`, `assets/`, `ui/`, `scripts/`, `docs/`, and
  `.github/`.
- `shared/frontend/` vendors only the modules Kafka actually imports
  (`binaryEvent`, `editorTheme`, `themeSync`, `uiIntent`) plus their README.
  Import depth in `frontend/src` changed from the monorepo's three/four levels
  to two/three levels. No LDAP, Files, or SSH code was carried over.
- `shared/sdk/go/dbx-plugin-sdk/` is the minimal Go sidecar SDK vendoring
  matching the split baseline (pure stdlib). The `backend/go.mod` replace now
  points at `../shared/sdk/go/dbx-plugin-sdk` instead of the host worktree, so
  `go vet` / `go test` no longer need `../host`. During packaging the plugin
  CLI still injects its own resolution path via DBX_PLUGIN_SDK_ROOT + go.work;
  this replace does not affect packaging.
- `scripts/connection-forms/verify.mjs` is a standalone connection-form
  contract verifier with host-compatible visibility/required cascade checks
  and a 960-combination Kafka scenario matrix (broker discovery x security
  protocol x SASL mechanism incl. Kerberos/OAuth sub-forms x Schema Registry
  x read-only/delete gate). Usage:
  `node scripts/connection-forms/verify.mjs kafka`.
- `scripts/cli-platform.sh` resolves the plugin-cli platform package suffix
  (linux packages carry a `-gnu` suffix). `scripts/build.sh` and
  `scripts/test.sh` call the native CLI through `resolve_native_plugin_cli`,
  avoiding the npm wrapper's injected SDK go.work (pinned to go 1.22)
  conflicting with backend go.mod (go 1.26); no platform falls back to the
  wrapper.

## CI and release

- `.github/workflows/ci.yml`: a validate job checks the manifest,
  dbx-plugin.toml, Go identity, and connection forms; the frontend job runs
  typecheck/test/build plus a freshness check on the committed `ui/` output;
  the backend job runs `go vet ./... && go test ./...` (go 1.26.x, module
  versions pinned by go.sum); the candidate job packages `.dbxp` on five
  targets (linux-x64/linux-arm64/darwin-arm64/darwin-x64/windows-x64) via
  `DBX_PLUGIN_TARGET=<t> bash scripts/build.sh`, runs the offline MCP stdio
  smoke against the locally built sidecar, and finishes with
  `scripts/check_candidates.py` cross-target consistency checks.
- `.github/workflows/release.yml` is a self-contained build+publish pipeline
  (no t8y2/dbx reusable workflow): it triggers only on GitHub Releases tagged
  `kafka-v<manifest.version>`, builds all five targets, and uploads the
  `.dbxp` packages plus `release-candidates.json` back to the release. It
  embeds no signing key, remote registry, or secrets.
- `manifest.json` `source`/`homepage` point at
  `https://github.com/jinpy666/dbx-plugin-kafka`. The version stays at the
  release baseline (0.1.41) and matches the sidecar-injected `main.version`.

## Bug write-back and release boundary

Bugs found in this repository are fixed here first and must pass CI. If the
DBX Host API, host installer, or the shared SDK needs a change, open a
separate DBX host/SDK change and record the affected versions in the
issue/PR. Never point this repository back at the monorepo's `../shared`;
when the monorepo `shared/frontend` layer evolves, port the changes into this
repository's vendored copy and re-verify on the split baseline. For
cross-plugin reuse, publish a public package or sync explicit versions into
each standalone repository.

Real Kafka clusters (`docker-compose.kafka-test.yml` test containers,
`scripts/dev-cluster.sh` dev cluster, Schema Registry) and the DBX.app host
installation pipeline are not offline CI pass conditions; without those
environments the related live/e2e/smoke cases must print `SKIP`
(`scripts/smoke_mcp.py` and `scripts/smoke_test.py` container cases follow
this semantics) and must never fake a `PASS`.
