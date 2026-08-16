#!/usr/bin/env bash
# Run the wasm test lanes. One script, both lanes, so local == CI.
#
#   ci/wasm-test.sh --plain   --firefox [-- <extra cargo args>]
#   ci/wasm-test.sh --atomics --chrome  [-- <extra cargo args>]
#
# Two traps this script exists to prevent:
#
#  1. Exported RUSTFLAGS clobbering the lane's rustflags (see guard-rustflags.sh).
#  2. A lane that exits 0 having run ZERO tests. wasm-bindgen-test-runner
#     defaults to Node when a test binary has no `wasm_bindgen_test_configure!`
#     section, and combined with WASM_BINDGEN_TEST_ONLY_WEB it prints
#     "only configured to run in node.js ... skipping" and returns success.
#     That is how a browser suite can sit in a repo for months, green, having
#     never executed. We assert a nonzero passing count instead.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "${HERE}/.." && pwd -P)"

LANE=""
BROWSER=""
while [ $# -gt 0 ]; do
    case "$1" in
        --plain)   LANE=plain;   shift ;;
        --atomics) LANE=atomics; shift ;;
        --chrome)  BROWSER=chrome;  shift ;;
        --firefox) BROWSER=firefox; shift ;;
        --) shift; break ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[ -n "${LANE}" ]    || { echo "need --plain or --atomics" >&2; exit 2; }
[ -n "${BROWSER}" ] || { echo "need --chrome or --firefox" >&2; exit 2; }

"${HERE}/guard-rustflags.sh"

cd "${ROOT}"

# The runner must match the resolved wasm-bindgen version exactly, or it
# aborts with a schema-version error. CI derives this from Cargo.lock; locally
# CROSSBANK_WBG_RUNNER can point at a matching binary.
RUNNER="${CROSSBANK_WBG_RUNNER:-$(command -v wasm-bindgen-test-runner || true)}"
[ -n "${RUNNER}" ] || { echo "no wasm-bindgen-test-runner found" >&2; exit 2; }
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="${RUNNER}"

export WASM_BINDGEN_USE_BROWSER=1
export WASM_BINDGEN_TEST_TIMEOUT="${WASM_BINDGEN_TEST_TIMEOUT:-180}"
# NEVER set WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION — the runner sets
# COOP/COEP by default, which is what makes SharedArrayBuffer reachable.
# NEVER set WASM_BINDGEN_TEST_ONLY_WEB — see trap 2 above.
unset WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION WASM_BINDGEN_TEST_ONLY_WEB || true

TOOLCHAIN=()
CONFIG=()
if [ "${LANE}" = atomics ]; then
    TOOLCHAIN=("+$(cat "${HERE}/wasm-toolchain.txt")")
    CONFIG=(--config "${HERE}/wasm-atomics.toml")
    export CARGO_UNSTABLE_BUILD_STD=std,panic_abort
    # Read at compile time by tests/spike_atomics.rs so a silently-plain
    # "atomics" lane fails instead of passing.
    export CROSSBANK_EXPECT_ATOMICS=1
else
    unset CROSSBANK_EXPECT_ATOMICS || true
fi

OUT="$(mktemp)"
trap 'rm -f "${OUT}"' EXIT

echo "==> lane=${LANE} browser=${BROWSER}"
set +e
cargo "${TOOLCHAIN[@]}" "${CONFIG[@]}" test \
    --target wasm32-unknown-unknown "$@" 2>&1 | tee "${OUT}"
status="${PIPESTATUS[0]}"
set -e

if [ "${status}" -ne 0 ]; then
    echo "FAIL: cargo test exited ${status}" >&2
    exit "${status}"
fi

"${HERE}/assert-tests-ran.sh" "${OUT}"
