#!/usr/bin/env node
// Merge the rows that tests/bench_web.rs logs to the browser console into
// bench/results/<date>-web.json.
//
//   ci/bench.sh --web --chrome        (which pipes the run through this)
//
// Since Phase 5, bench_web.rs emits the SAME document schema as
// bench/hive_ce/web — one console line `BENCH_JSON {...}` carrying
// {tool, date_utc, iterations, samples:[{workload, backend, n, bytes,
// p50_ms, p99_ms, ops_per_s}]} — so there is nothing to convert here: the rows
// are already in bench/results schema, on the same workload names, computed
// with the same median/p99 maths. This script only locates the line and hands
// it to the shared merger.
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
  .find((l) => l.includes('BENCH_JSON'));
if (!line) {
  console.error('no bench_web JSON found on stdin (looked for "BENCH_JSON")');
  console.error('did the test run? it is #[ignore], so pass --include-ignored');
  process.exit(1);
}
const doc = JSON.parse(line.slice(line.indexOf('{'), line.lastIndexOf('}') + 1));
if (!Array.isArray(doc.samples) || doc.samples.length === 0) {
  console.error('BENCH_JSON carried no samples');
  process.exit(1);
}

const browser = arg('browser', 'unknown');
// The schema row carries the browser so Chrome and Firefox rows coexist in one
// file instead of overwriting each other.
for (const s of doc.samples) s.browser = browser;
const file = resultsPath(ROOT, arg('date', null));
const base = mergeInto(file, doc, {
  machine: arg('machine', null),
  note:
    `crossbank_indexeddb_web rows: tests/bench_web.rs in ${browser}, cargo profile ` +
    `${arg('profile', 'release')}, ${doc.iterations} iterations after one un-timed warm-up, ` +
    `median/p99, timed with performance.now() — the same sampling and maths as the ` +
    `hive_ce_web rows.`,
});
console.log(`wrote ${file} (${base.samples.length} rows)`);
printTable(base);
