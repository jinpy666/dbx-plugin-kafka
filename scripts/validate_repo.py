#!/usr/bin/env python3
"""Validate standalone Kafka plugin identity and package-relative paths."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")


def fail(message: str) -> None:
    raise SystemExit(f"FAIL: {message}")


def main() -> int:
    manifest_path = ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("manifest_version") != 1:
        fail("manifest_version must be 1")
    if manifest.get("id") != "io.dbx.kafka":
        fail(f"manifest id is {manifest.get('id')!r}, expected io.dbx.kafka")
    version = manifest.get("version")
    if not isinstance(version, str) or not SEMVER.fullmatch(version):
        fail(f"invalid manifest version: {version!r}")
    for field in ("source", "homepage"):
        value = manifest.get(field, "")
        if not isinstance(value, str) or not value.startswith(("http://", "https://")):
            fail(f"{field} must be an HTTP(S) URL")

    entrypoint = manifest.get("entrypoints", {}).get("backend", {})
    if entrypoint.get("executable") != "bin/dbx-plugin-kafka":
        fail("backend executable must be bin/dbx-plugin-kafka")
    if manifest.get("entrypoints", {}).get("ui", {}).get("entry") != "ui/index.html":
        fail("UI entry must be ui/index.html")

    toml = (ROOT / "dbx-plugin.toml").read_text(encoding="utf-8")
    if 'directory = "backend"' not in toml or 'binary = "dbx-plugin-kafka"' not in toml:
        fail("dbx-plugin.toml backend identity/path is stale")

    go_mod = (ROOT / "backend/go.mod").read_text(encoding="utf-8")
    if not re.search(r'(?m)^module\s+io\.dbx\.kafka\.plugin\s*$', go_mod):
        fail("backend go.mod module does not match io.dbx.kafka.plugin")
    # Floor tracks the CI toolchain (ci.yml/release.yml `go-version`): x/net and
    # franz-go both declare `go 1.26.0` upstream, so a lower directive would let
    # a dependency bump pass this check and still fail `go vet` on the runner.
    go_version = re.search(r'(?m)^go\s+(\d+)\.(\d+)(?:\.\d+)?\s*$', go_mod)
    if not go_version or (int(go_version.group(1)), int(go_version.group(2))) < (1, 26):
        fail("backend go.mod must declare go >= 1.26")
    replace = re.search(r"(?m)^replace\s+github\.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk\s+=>\s+(\S+)\s*$", go_mod)
    if not replace or not replace.group(1).startswith("../shared/sdk/go/dbx-plugin-sdk"):
        fail("backend go.mod SDK replace must point at the vendored shared/sdk/go copy")
    main_go = (ROOT / "backend/main.go").read_text(encoding="utf-8")
    if not re.search(r'(?m)^var version = "0\.0\.0-dev"$', main_go):
        fail("backend main.go must declare the injectable `var version` identity")
    build_sh = (ROOT / "scripts/build.sh").read_text(encoding="utf-8")
    if "-X main.version=" not in build_sh and "-X=main.version=" not in build_sh:
        fail("scripts/build.sh must inject main.version from the manifest at build/package time")

    required = [
        ".dbx-store.json",
        "assets/plugin.svg",
        "frontend/package.json",
        "backend/go.mod",
        "scripts/test.sh",
        "scripts/build.sh",
        "scripts/package.sh",
        "scripts/install.sh",
        "scripts/cli-platform.sh",
        "scripts/smoke_mcp.py",
        "scripts/check_candidates.py",
        "scripts/connection-forms/verify.mjs",
        "shared/frontend/binaryEvent.ts",
        "shared/frontend/editorTheme.ts",
        "shared/frontend/pluginStorage.ts",
        "shared/frontend/themeSync.ts",
        "shared/frontend/uiIntent.ts",
        "shared/sdk/go/dbx-plugin-sdk/go.mod",
        "shared/sdk/go/dbx-plugin-sdk/sdk.go",
    ]
    for relative in required:
        if not (ROOT / relative).exists():
            fail(f"missing required path: {relative}")

    # vendored shared/frontend 副本身份（评审 WATCH：拆分后每个插件仓库一份
    # 手抄副本，无来源戳就无法判断两个仓库的副本谁新谁旧——标记行即检查点）。
    for name in ("binaryEvent.ts", "editorTheme.ts", "pluginStorage.ts", "themeSync.ts", "uiIntent.ts"):
        vendored = (ROOT / "shared/frontend" / name).read_text(encoding="utf-8")
        if not vendored.lstrip().startswith("// vendored:") or "vendored-sync:" not in vendored[:800]:
            fail(f"shared/frontend/{name} is missing the vendored provenance header")

    store = json.loads((ROOT / ".dbx-store.json").read_text(encoding="utf-8"))
    if store.get("permissions") != manifest.get("permissions"):
        fail(".dbx-store.json permissions must match manifest.json permissions")
    icon = store.get("icon", "")
    if not icon or not (ROOT / icon).exists():
        fail(f".dbx-store.json icon must point at an existing asset: {icon!r}")
    if store.get("homepage") != manifest.get("homepage") or store.get("source") != manifest.get("source"):
        fail(".dbx-store.json source/homepage must match manifest.json")

    print(f"PASS repository identity: {manifest['id']} {version}; standalone paths, store entry, and vendored SDK present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
