#!/usr/bin/env bash
# Opt-in benches. Not a CI gate.
#
#   ci/bench.sh                        # native Criterion
#   ci/bench.sh --hive                 # + Hive CE, native file backend
#   ci/bench.sh --hive --web           # + Hive CE on IndexedDB in a real browser
#   ci/bench.sh --web                  # + crossbank's tests/bench_web.rs timings
#   ci/bench.sh --hive --web --no-native   # web lanes only, skip Criterion
#   ci/bench.sh --web --debug-wasm     # wasm bench unoptimised (default: --release)
#
# Browser selection applies to BOTH web lanes so a pair is measured in the same
# browser: --chrome uses /usr/bin/google-chrome (chromedriver for the wasm lane,
# Playwright `executablePath` for the Hive lane), --firefox uses geckodriver +
# Playwright's Firefox. Default is --chrome, because that is the one browser
# both lanes can drive out of the box on this machine.
#
# Web rows land in bench/results/<date>-web.json, merged by (workload, backend).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "${HERE}/.." && pwd -P)"
cd "${ROOT}"

BROWSER=chrome
NATIVE=1
# The wasm bench lane defaults to --release. A debug wasm build against an
# -O2 dart2js build is not a comparison, it is a compiler-flag report.
WASM_PROFILE=(--release)
CARGO_ARGS=()
BENCH_ARGS=()
HIVE_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --web)       WEB=1; shift ;;
        --hive)      HIVE=1; shift ;;
        --no-native) NATIVE=0; shift ;;
        --firefox)   BROWSER=firefox; shift ;;
        --chrome)    BROWSER=chrome; shift ;;
        --plain|--atomics) shift ;;
        --debug-wasm) WASM_PROFILE=(); shift ;;
        --date|--machine) BENCH_ARGS+=("$1" "${2:?$1 needs a value}"); shift 2 ;;
        --iters) HIVE_ARGS+=("$1" "${2:?$1 needs a value}"); shift 2 ;;
        --) shift; CARGO_ARGS+=("$@"); break ;;
        *) CARGO_ARGS+=("$1"); shift ;;
    esac
done

# Empty arrays are expanded with the ${arr[@]+"${arr[@]}"} form throughout:
# macOS bash 3.2 treats a bare "${arr[@]}" on an empty array under `set -u` as
# an unbound variable. See RESUME_HERE.md trap 40.
if [ "${NATIVE}" = 1 ]; then
    cargo bench --bench kv ${CARGO_ARGS[@]+"${CARGO_ARGS[@]}"}
fi

if [ "${HIVE:-}" = 1 ] || [ "${WEB:-}" = 1 ]; then
    NODE="${NODE:-$(command -v node || true)}"
fi

DART="${DART:-$(command -v dart || echo "${HOME}/development/flutter/bin/dart")}"

# ---- Hive CE, native file backend ------------------------------------------
if [ "${HIVE:-}" = 1 ] && [ "${WEB:-}" != 1 ]; then
    (cd bench/hive_ce && "${DART}" pub get >/dev/null && "${DART}" run bin/hive_ce_bench.dart)
fi

# ---- Hive CE on IndexedDB, in a browser -------------------------------------
if [ "${HIVE:-}" = 1 ] && [ "${WEB:-}" = 1 ]; then
    [ -n "${NODE}" ] || { echo "need node for the web bench driver" >&2; exit 2; }
    echo "==> compiling bench/hive_ce/web"
    (cd bench/hive_ce \
        && "${DART}" pub get >/dev/null \
        && "${DART}" compile js -O2 -o web/main.dart.js web/main.dart >/dev/null)
    PW_BROWSER=chromium
    PW_ARGS=()
    if [ "${BROWSER}" = firefox ]; then
        PW_BROWSER=firefox
    else
        PW_ARGS+=(--chrome-path "${CROSSBANK_CHROME:-/usr/bin/google-chrome}")
    fi
    "${NODE}" "${HERE}/web-bench/run.mjs" \
        --browser "${PW_BROWSER}" ${PW_ARGS[@]+"${PW_ARGS[@]}"} ${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"} ${HIVE_ARGS[@]+"${HIVE_ARGS[@]}"}
fi

# ---- crossbank's own web timings (tests/bench_web.rs) -----------------------
if [ "${WEB:-}" = 1 ]; then
    [ -n "${NODE}" ] || { echo "need node for the web bench driver" >&2; exit 2; }
    # NOTE: we do NOT go through ci/wasm-test.sh here. That script asserts the
    # full per-lane test count (110+) and this run is a single #[ignore]d test,
    # so it would always fail the shrink detector. We set up the same runner
    # environment ourselves and skip the count assertion, which is the one
    # thing that does not apply to a one-test bench run.
    "${HERE}/guard-rustflags.sh"
    RUNNER="${CROSSBANK_WBG_RUNNER:-$(command -v wasm-bindgen-test-runner || true)}"
    [ -n "${RUNNER}" ] || { echo "no wasm-bindgen-test-runner found" >&2; exit 2; }
    export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="${RUNNER}"
    export WASM_BINDGEN_USE_BROWSER=1
    # The bench runs every workload ITERATIONS times, including 2 000-put and
    # 8 MiB rounds, so it needs far more headroom than a unit test.
    export WASM_BINDGEN_TEST_TIMEOUT="${WASM_BINDGEN_TEST_TIMEOUT:-1800}"
    unset WASM_BINDGEN_TEST_NO_ORIGIN_ISOLATION WASM_BINDGEN_TEST_ONLY_WEB || true
    if [ "${BROWSER}" = chrome ]; then
        unset GECKODRIVER GECKODRIVER_REMOTE SAFARIDRIVER SAFARIDRIVER_REMOTE || true
        : "${CHROMEDRIVER:=$(command -v chromedriver || true)}"
        [ -n "${CHROMEDRIVER}" ] || { echo "no chromedriver found" >&2; exit 2; }
        export CHROMEDRIVER
    else
        unset CHROMEDRIVER CHROMEDRIVER_REMOTE SAFARIDRIVER SAFARIDRIVER_REMOTE || true
        : "${GECKODRIVER:=$(command -v geckodriver || true)}"
        [ -n "${GECKODRIVER}" ] || { echo "no geckodriver found" >&2; exit 2; }
        export GECKODRIVER
    fi
    echo "==> tests/bench_web.rs in ${BROWSER} ${WASM_PROFILE[*]:---debug}"
    OUT="$(mktemp)"
    trap 'rm -f "${OUT}"' EXIT
    set +e
    cargo test ${WASM_PROFILE[@]+"${WASM_PROFILE[@]}"} --target wasm32-unknown-unknown --test bench_web -- --include-ignored --nocapture \
        2>&1 | tee "${OUT}"
    status="${PIPESTATUS[0]}"
    set -e
    # bench_web.rs now tears its database down BEFORE printing, so a clean run
    # exits 0 and the exit code means what it says. The tolerance below stays
    # as a belt: a bench is not a gate, and rows that reached the log are worth
    # recording even if the runner lost the browser on the way out.
    if ! "${NODE}" "${HERE}/web-bench/merge-crossbank.mjs" \
        --browser "${BROWSER}" --profile "${WASM_PROFILE[*]:-debug}" \
        ${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"} < "${OUT}"; then
        echo "FAIL: no bench_web JSON in the run (cargo test exited ${status})" >&2
        exit 1
    fi
    if [ "${status}" -ne 0 ]; then
        echo "WARN: cargo test exited ${status} AFTER printing its numbers; rows recorded anyway." >&2
    fi
fi
