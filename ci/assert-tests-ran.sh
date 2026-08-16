#!/usr/bin/env bash
# Assert that a wasm test run actually executed tests.
#
# This exists because a misconfigured wasm lane exits 0 having run nothing.
# wasm-bindgen-test-runner defaults to Node when a test binary carries no
# `wasm_bindgen_test_configure!` section; paired with WASM_BINDGEN_TEST_ONLY_WEB
# it prints "only configured to run in node.js ... skipping" and returns
# success. A green check that proved nothing is worse than a red one.
#
#   ci/assert-tests-ran.sh <output-file> [minimum-passed]
set -euo pipefail

OUT="${1:?usage: assert-tests-ran.sh <output-file> [minimum-passed]}"
MIN="${2:-1}"

if grep -qi "only configured to run in node.js" "${OUT}"; then
    echo "FAIL: the runner skipped this suite entirely (Node/browser mode mismatch)." >&2
    echo "      A test binary is missing wasm_bindgen_test_configure!." >&2
    exit 1
fi

passed="$(grep -oE 'test result: ok\. [0-9]+ passed' "${OUT}" \
          | grep -oE '[0-9]+' | awk '{s+=$1} END {print s+0}')"

if [ "${passed}" -lt "${MIN}" ]; then
    echo "FAIL: only ${passed} test(s) passed, expected at least ${MIN}." >&2
    echo "      A lane that runs zero tests must never report success." >&2
    exit 1
fi

echo "OK: ${passed} test(s) actually ran and passed."
