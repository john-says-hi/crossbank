# Bench results

Dated snapshots, not CI gates. One file per recorded run, `YYYY-MM-DD.json`.
We commit a representative file when PLAN.md's Performance section changes,
not every local run.

Schema (one document, `samples` is a list):

```json
{
  "tool": "bench/hive_ce | benches/kv.rs",
  "date_utc": "ISO-8601",
  "machine": "free text",
  "samples": [
    {
      "workload": "settings_eager",
      "backend": "hive_ce_file | crossbank_redb | crossbank_memory | raw_redb",
      "n": 1000,
      "bytes": 1024,
      "p50_ms": 4.0,
      "p99_ms": 6.8,
      "ops_per_s": 248000
    }
  ]
}
```

`n` is the number of logical operations in one timed iteration (so `ops_per_s = n / p50`).
Criterion reports its own estimate as `p50_ms`; the Dart tool computes a true median over
`iterations` runs.

Reproduce:

```sh
cargo bench --bench kv                # crossbank + raw redb (Criterion)
ci/bench.sh --hive                    # also runs bench/hive_ce (needs a Dart SDK)
```
