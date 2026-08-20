#!/usr/bin/env bash
# Opt-in benches. Not a CI gate.
#
#   ci/bench.sh                 # native Criterion
#   ci/bench.sh --web --firefox # also the ignored wasm timings
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(cd "${HERE}/.." && pwd -P)"
cd "${ROOT}"

cargo bench --bench kv "$@"

while [ $# -gt 0 ]; do
    case "$1" in
        --web) WEB=1; shift ;;
        --firefox|--chrome|--plain|--atomics) shift ;;
        --) shift; break ;;
        *) shift ;;
    esac
done

if [ "${WEB:-}" = 1 ]; then
    "${HERE}/wasm-test.sh" --plain --firefox -- --test bench_web --include-ignored
fi
