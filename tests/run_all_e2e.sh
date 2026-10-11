#!/usr/bin/env bash
# Runs every example against a real Firefox and checks its output (`make e2e`).
# Not a quality gate: it needs Firefox, a display and network access.
set -uo pipefail

cd "$(dirname "$0")/.."

FIREFOX=tests/firefox.sh
started_here=0

cleanup() {
  # Only stop the Firefox this script started, never one that was already open.
  if [ "$started_here" -eq 1 ]; then
    "$FIREFOX" stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! "$FIREFOX" status >/dev/null 2>&1; then
  "$FIREFOX" start || exit 1
  started_here=1
fi

echo "Building the examples..."
cargo build --quiet --examples || exit 1

# example name | pattern its output must contain
EXAMPLES=(
  "simple|rust-lang.org/"
  "filter_domains|event: browsingContext.load"
  "runtime_usage|hi from bidi-lite"
  "multi_tab|docs.rs loaded"
  "multi_tab_parallel|all 4 pages"
  "breakpoints_usage|Breakpoint was resolved"
  "proxy_usage|Seen from the proxy"
)

pass=0
fail=0
skip=0
summary=()

for entry in "${EXAMPLES[@]}"; do
  name="${entry%%|*}"
  pattern="${entry#*|}"

  if [ "$name" = "proxy_usage" ] && [ -z "${PROXY_HOST:-}${PROXY_PORT:-}${PROXY_USER:-}${PROXY_PASS:-}" ]; then
    summary+=("SKIP  $name (PROXY_* not set)")
    skip=$((skip + 1))
    continue
  fi

  echo "Running $name..."
  output=$(RUST_LOG=info timeout 120 cargo run --quiet --example "$name" 2>&1 | sed 's/\x1b\[[0-9;]*m//g')
  status=${PIPESTATUS[0]}

  if [ "$status" -eq 0 ] && grep -qF "$pattern" <<<"$output"; then
    summary+=("PASS  $name")
    pass=$((pass + 1))
  else
    summary+=("FAIL  $name (exit $status, expected \"$pattern\")")
    fail=$((fail + 1))
    echo "$output" | tail -20
  fi
done

echo
printf '%s\n' "${summary[@]}"
echo "PASS $pass, FAIL $fail, SKIP $skip"
[ "$fail" -eq 0 ]
