#!/usr/bin/env bash
# Build the crossbank web e2e page and drive it in a real browser.
#
#   ci/web-e2e.sh                              # chromium
#   ci/web-e2e.sh --browser firefox
#   ci/web-e2e.sh --browser webkit --keys 2000 --headed
#
# This is the lane that covers what wasm_bindgen_test structurally cannot: a
# real page reload (a fresh wasm instance reading an earlier one's bytes) and
# two real tabs exchanging a coherence message. See examples/web_e2e_page.rs.
#
# Nightly, not per PR: it needs Playwright browsers, which the wasm lanes do
# not.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "${HERE}/.." && pwd -P)"

BROWSER=chromium
PASS_THROUGH=()
while [ $# -gt 0 ]; do
    case "$1" in
        --browser) BROWSER="${2:?--browser needs a value}"; shift 2 ;;
        *) PASS_THROUGH+=("$1"); shift ;;
    esac
done

"${HERE}/guard-rustflags.sh"

cd "${ROOT}"

OUT="${ROOT}/target/web-e2e"
WASM="${ROOT}/target/wasm32-unknown-unknown/release/examples/web_e2e_page.wasm"

echo "==> building the page"
cargo build --example web_e2e_page --target wasm32-unknown-unknown --release

# The CLI must match the wasm-bindgen in Cargo.lock exactly, exactly as the
# test lanes require of the test runner — a mismatch is a schema error, not a
# graceful degradation.
WANT_WBG="$(awk '/^name = "wasm-bindgen"$/{f=1;next} f&&/^version = /{gsub(/[",]/,"",$3);print $3;exit}' Cargo.lock)"
BINDGEN="${CROSSBANK_WBG_CLI:-$(command -v wasm-bindgen || true)}"
if [ -z "${BINDGEN}" ]; then
    echo "FAIL: no wasm-bindgen CLI found." >&2
    echo "      Fix with:  cargo install --locked wasm-bindgen-cli --version ${WANT_WBG}" >&2
    exit 2
fi
HAVE_WBG="$("${BINDGEN}" --version 2>/dev/null | awk '{print $NF}')"
if [ "${HAVE_WBG}" != "${WANT_WBG}" ]; then
    echo "FAIL: wasm-bindgen CLI is ${HAVE_WBG:-unknown}, Cargo.lock resolves ${WANT_WBG}." >&2
    echo "      Fix with:  cargo install --locked wasm-bindgen-cli --version ${WANT_WBG}" >&2
    exit 2
fi

rm -rf "${OUT}"
mkdir -p "${OUT}"
"${BINDGEN}" --target web --no-typescript --out-dir "${OUT}" "${WASM}"
cp "${HERE}/web-e2e/index.html" "${OUT}/index.html"

echo "==> driving it in ${BROWSER}"
node "${HERE}/web-e2e/run.mjs" --browser "${BROWSER}" --dir "${OUT}" ${PASS_THROUGH[@]+"${PASS_THROUGH[@]}"}
