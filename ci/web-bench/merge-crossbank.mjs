#!/usr/bin/env node
// Convert the flat JSON that tests/bench_web.rs logs to the console into
// bench/results schema rows and merge them into <date>-web.json.
//
//   ci/wasm-test.sh ... | node ci/web-bench/merge-crossbank.mjs --browser firefox
//
// tests/bench_web.rs belongs to another lane and is deliberately NOT edited
// here; this reads its output as-is:
//
//   {"backend":"indexeddb","settings_get_ms":..,"bulk_put_200_ms":..,"bulk_get_ms":..}
//
// Its shapes are the *small* ones (50 settings keys, 200 bulk), which is why
// the Hive web tool also emits `*_web_small` rows. Unifying bench_web.rs onto
// the large shapes is the remaining Phase 5 item.
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mergeInto, printTable, resultsPath } from './merge.mjs';

const ROOT = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const chunks = [];
for await (const c of process.stdin) chunks.push(c);
const text = Buffer.concat(chunks).toString('utf8');

const line = text
  .split(/\r?\n/)
  .map((l) => l.trim())
  .reverse()
  .find((l) => l.includes('"settings_get_ms"'));
if (!line) {
  console.error('no bench_web JSON found on stdin (looked for "settings_get_ms")');
  console.error('did the test run? it is #[ignore], so pass --include-ignored');
  process.exit(1);
}
const raw = JSON.parse(line.slice(line.indexOf('{'), line.lastIndexOf('}') + 1));

// bench_web.rs times with js_sys::Date::now(), i.e. whole milliseconds, so a
// fast per-op loop can round to 0. Report that as "faster than the clock"
// (null) rather than as Infinity ops/s.
const perOpRate = (ms) => (ms > 0 ? 1000 / ms : null);

const browser = arg('browser', 'unknown');
const backend = 'crossbank_indexeddb_web';
// bench_web.rs reports per-op for the two gets and a total for the 200 puts.
// The schema wants a total per timed iteration plus n, so scale the per-op
// numbers back up by their op count.
const samples = [
  {
    workload: 'settings_eager_web_small',
    backend,
    n: 200,
    bytes: 1024,
    p50_ms: raw.settings_get_ms * 200,
    p99_ms: null,
    ops_per_s: perOpRate(raw.settings_get_ms),
  },
  {
    workload: 'bulk_lazy_put_web_small',
    backend,
    n: 200,
    bytes: 200 * 256,
    p50_ms: raw.bulk_put_200_ms,
    p99_ms: null,
    ops_per_s: raw.bulk_put_200_ms > 0 ? 200 / (raw.bulk_put_200_ms / 1000) : null,
  },
  {
    workload: 'bulk_lazy_get_web_small',
    backend,
    n: 200,
    bytes: 256,
    p50_ms: raw.bulk_get_ms * 200,
    p99_ms: null,
    ops_per_s: perOpRate(raw.bulk_get_ms),
  },
];

const file = resultsPath(ROOT, arg('date', null));
const base = mergeInto(
  file,
  { samples },
  {
    machine: arg('machine', null),
    note: `crossbank_indexeddb_web rows: tests/bench_web.rs in ${browser}, cargo profile ${arg('profile', 'release')}, ONE un-warmed shot (Criterion does not run in wasm), so p99 is null and these are noisier than the Hive rows. It times with Date.now() (1 ms resolution), so a sub-millisecond per-op loop reads as 0 / null ops_per_s.`,
  },
);
console.log(`wrote ${file} (${base.samples.length} rows)`);
printTable(base);
