#!/usr/bin/env bash
# Run the wasm test lanes. One script, both lanes, so local == CI.
#
#   ci/wasm-test.sh --plain   --firefox [-- <extra cargo args>]
#   ci/wasm-test.sh --atomics --chrome  [-- <extra cargo args>]
#   ci/wasm-test.sh --plain   --safari  [-- <extra cargo args>]   (macOS only)
#
# Two traps this script exists to prevent:
#
#  1. Exported RUSTFLAGS clobbering the lane's rustflags (see guard-rustflags.sh).
#  2. A lane that exits 0 having run ZERO tests. wasm-bindgen-test-runner
#     defaults to Node when a test binary has no `wasm_bindgen_test_configure!`
#     section, and combined with WASM_BINDGEN_TEST_ONLY_WEB it prints
#     "only configured to run in node.js ... skipping" and returns success.
#     That is how a browser suite can sit in a repo for months, green, having
#     never executed. We assert a per-lane MINIMUM passing count instead, read
#     from ci/expected-tests.txt.
#
#  3. A wasm-bindgen-test-runner whose version does not match the wasm-bindgen
#     resolved in Cargo.lock. It aborts with a schema-version error, so we
#     check up front and say exactly how to fix it.
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
        --safari)  BROWSER=safari;  shift ;;
        --) shift; break ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[ -n "${LANE}" ]    || { echo "need --plain or --atomics" >&2; exit 2; }
[ -n "${BROWSER}" ] || { echo "need --chrome, --firefox or --safari" >&2; exit 2; }

# Safari is the PLAIN lane only. There is no headless Safari, and
# SharedArrayBuffer under WebDriver-driven Safari is unreliable, so the
# atomics lane would be flaky rather than informative.
if [ "${BROWSER}" = safari ] && [ "${LANE}" != plain ]; then
    echo "safari supports the --plain lane only (no headless Safari; SAB under WebDriver is unreliable)" >&2
    exit 2
fi

"${HERE}/guard-rustflags.sh"

cd "${ROOT}"

# The runner must match the resolved wasm-bindgen version exactly, or it
# aborts with a schema-version error. CI derives this from Cargo.lock; locally
# CROSSBANK_WBG_RUNNER can point at a matching binary.
RUNNER="${CROSSBANK_WBG_RUNNER:-$(command -v wasm-bindgen-test-runner || true)}"
[ -n "${RUNNER}" ] || { echo "no wasm-bindgen-test-runner found" >&2; exit 2; }
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="${RUNNER}"

# Fail fast on a mismatched runner rather than mid-run on a schema error.
WANT_WBG="$(awk '/^name = "wasm-bindgen"$/{f=1;next} f&&/^version = /{gsub(/[",]/,"",$3);print $3;exit}' Cargo.lock)"
HAVE_WBG="$("${RUNNER}" --version 2>/dev/null | awk '{print $NF}')"
if [ -z "${WANT_WBG}" ]; then
    echo "FAIL: could not read the wasm-bindgen version from Cargo.lock" >&2
    exit 2
fi
if [ "${HAVE_WBG}" != "${WANT_WBG}" ]; then
    echo "FAIL: wasm-bindgen-test-runner is ${HAVE_WBG:-unknown}, Cargo.lock resolves wasm-bindgen ${WANT_WBG}." >&2
    echo "      The runner must match EXACTLY or it aborts with a schema-version error." >&2
    echo "      Fix with:  cargo install --locked wasm-bindgen-cli --version ${WANT_WBG}" >&2
    echo "      Or point CROSSBANK_WBG_RUNNER at a matching binary." >&2
    exit 2
fi

export WASM_BINDGEN_USE_BROWSER=1
# wasm-bindgen-test-runner prefers the first of GECKODRIVER / CHROMEDRIVER
# that is set. Pin exactly one so `--chrome` cannot silently run Firefox.
if [ "${BROWSER}" = chrome ]; then
    unset GECKODRIVER GECKODRIVER_REMOTE SAFARIDRIVER SAFARIDRIVER_REMOTE || true
    : "${CHROMEDRIVER:=$(command -v chromedriver || true)}"
    [ -n "${CHROMEDRIVER}" ] || { echo "no chromedriver found" >&2; exit 2; }
    export CHROMEDRIVER
elif [ "${BROWSER}" = safari ]; then
    # The runner probes geckodriver BEFORE safaridriver, so a stray
    # GECKODRIVER (or a geckodriver on PATH) would silently run Firefox here.
    unset GECKODRIVER GECKODRIVER_REMOTE CHROMEDRIVER CHROMEDRIVER_REMOTE || true
    : "${SAFARIDRIVER:=$(command -v safaridriver || true)}"
    [ -n "${SAFARIDRIVER}" ] || { echo "no safaridriver found (macOS only; run: sudo safaridriver --enable)" >&2; exit 2; }
    export SAFARIDRIVER
else
    unset CHROMEDRIVER CHROMEDRIVER_REMOTE SAFARIDRIVER SAFARIDRIVER_REMOTE || true
    : "${GECKODRIVER:=$(command -v geckodriver || true)}"
    [ -n "${GECKODRIVER}" ] || { echo "no geckodriver found" >&2; exit 2; }
    export GECKODRIVER
fi
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
# macOS runners ship bash 3.2, where expanding an EMPTY array as "${arr[@]}"
# under `set -u` is an unbound-variable error (bash >= 4.4 tolerates it). Both
# arrays below are empty on the plain lane, so every empty-able array in this
# repo's ci scripts must use the ${arr[@]+"${arr[@]}"} form instead.
cargo ${TOOLCHAIN[@]+"${TOOLCHAIN[@]}"} ${CONFIG[@]+"${CONFIG[@]}"} test \
    --target wasm32-unknown-unknown "$@" 2>&1 | tee "${OUT}"
status="${PIPESTATUS[0]}"
set -e

if [ "${status}" -ne 0 ]; then
    echo "FAIL: cargo test exited ${status}" >&2
    exit "${status}"
fi

# Per-lane expected minimum. Fewer than this is a failure; more is fine.
EXPECTED_FILE="${HERE}/expected-tests.txt"
MIN="$(awk -v k="${LANE}-${BROWSER}" '$1==k {print $2; exit}' "${EXPECTED_FILE}")"
if [ -z "${MIN}" ]; then
    echo "FAIL: no expected test count for lane '${LANE}-${BROWSER}' in ${EXPECTED_FILE}" >&2
    exit 2
fi

"${HERE}/assert-tests-ran.sh" "${OUT}" "${MIN}"
