#!/usr/bin/env bash
# Platform resolution for the native @dbx-app/plugin-cli binary (sourced by
# build.sh and test.sh).
#
# The npm plugin-cli wrapper ships per-platform binaries as optional
# dependencies named @dbx-app/plugin-cli-<suffix>. Both scripts call the
# native binary directly: the wrapper re-injects DBX_PLUGIN_SDK_ROOT whenever
# it is unset, and the bundled SDK builds the Go backend through a go.work
# pinned to go 1.22, which conflicts with backend go.mod requiring >= 1.26.
#
# The suffix is NOT plain "<os>-<arch>": linux packages carry a -gnu suffix
# (linux-x64-gnu / linux-arm64-gnu), so the mapping lives in one place here
# instead of being duplicated inline.
#
# The pure mapping is testable without a real install:
#   source scripts/cli-platform.sh
#   cli_platform Darwin x86_64   # -> darwin-x64
#   cli_platform Linux x86_64    # -> linux-x64-gnu

# Map uname-style OS/arch to the plugin-cli platform package suffix.
# Returns non-zero for unsupported platforms (caller decides fallback).
cli_platform() {
  local os="$1" arch="$2"
  case "$os" in
    Darwin) os=darwin ;;
    Linux) os=linux ;;
    *) return 1 ;;
  esac
  case "$arch" in
    arm64|aarch64) arch=arm64 ;;
    x86_64|amd64) arch=x64 ;;
    *) return 1 ;;
  esac
  if [ "$os" = linux ]; then
    printf 'linux-%s-gnu' "$arch"
  else
    printf '%s-%s' "$os" "$arch"
  fi
}

# Print the path of the native plugin-cli binary for the running platform.
# Returns non-zero when the platform is unsupported or the package is not
# installed (caller prints a clear warning and falls back to the wrapper).
resolve_native_plugin_cli() {
  local suffix native
  suffix="$(cli_platform "$(uname -s)" "$(uname -m)")" || return 1
  native="$(npm root -g 2>/dev/null)/@dbx-app/plugin-cli/node_modules/@dbx-app/plugin-cli-${suffix}/bin/dbx-plugin"
  [ -x "$native" ] && { printf '%s\n' "$native"; return 0; }
  return 1
}
