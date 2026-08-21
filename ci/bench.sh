#!/usr/bin/env bash
# Opt-in benches. Not a CI gate.
#
#   ci/bench.sh                 # native Criterion
#   ci/bench.sh --web --firefox # also the ignored wasm timings
#   ci/bench.sh --hive          # also the Hive CE comparison (needs `dart`)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "${HERE}/.." && pwd -P)"
cd "${ROOT}"

CARGO_ARGS=()
for a in "$@"; do
    case "$a" in --web|--hive|--firefox|--chrome|--plain|--atomics) ;; *) CARGO_ARGS+=("$a") ;; esac
done
cargo bench --bench kv "${CARGO_ARGS[@]}"

while [ $# -gt 0 ]; do
    case "$1" in
        --web) WEB=1; shift ;;
        --hive) HIVE=1; shift ;;
        --firefox|--chrome|--plain|--atomics) shift ;;
        --) shift; break ;;
        *) shift ;;
    esac
done

if [ "${WEB:-}" = 1 ]; then
    "${HERE}/wasm-test.sh" --plain --firefox -- --test bench_web --include-ignored
fi

if [ "${HIVE:-}" = 1 ]; then
    DART="${DART:-$(command -v dart || echo "$HOME/development/flutter/bin/dart")}"
    (cd bench/hive_ce && "${DART}" pub get >/dev/null && "${DART}" run bin/hive_ce_bench.dart)
fi
