#!/usr/bin/env bash
# Fail loudly if anything has exported rustflags into the environment.
#
# A RUSTFLAGS-style environment variable *replaces* a .cargo/config.toml
# `target.*.rustflags` array rather than appending to it. For a shared-memory
# wasm build that means the atomics/TLS link args silently vanish and the
# module links against a non-atomics std. The failure is not a clean error —
# it is a subtly wrong artifact.
#
# The realistic offender is coverage tooling: cargo-llvm-cov and
# cargo-tarpaulin both export RUSTFLAGS. Coverage must stay native-only.
set -euo pipefail

fatal=0
for v in RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS \
         CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS; do
    if [ -n "${!v:-}" ]; then
        echo "FATAL: ${v} is set (='${!v}')." >&2
        echo "       It REPLACES .cargo config rustflags and will break shared-memory linking." >&2
        fatal=1
    fi
done

exit "${fatal}"
