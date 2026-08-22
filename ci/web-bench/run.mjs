#!/usr/bin/env node
// Drive bench/hive_ce/web in a real headless browser and record the rows.
//
//   node ci/web-bench/run.mjs [--browser chromium|firefox|webkit]
//                             [--chrome-path /usr/bin/google-chrome]
//                             [--iters N] [--headed] [--timeout-ms N]
//                             [--date YYYY-MM-DD] [--machine "..."]
//
// Serves bench/hive_ce/web over plain http from one origin (so IndexedDB is
// same-origin and there is no CSP in play), opens index.html, waits for the
// page to publish its result, and merges the rows into
// bench/results/<date>-web.json.
//
// The result is taken from the console line `BENCH_JSON <json>`; the
// `globalThis.__benchResult` global is the fallback in case a console message
// is dropped.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadPlaywright } from './resolve-playwright.mjs';
import { mergeInto, printTable, resultsPath } from './merge.mjs';

const HERE = fileURLToPath(new URL('.', import.meta.url));
const ROOT = resolve(HERE, '..', '..');
const WEB = join(ROOT, 'bench', 'hive_ce', 'web');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
};

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}
const flag = (name) => process.argv.includes(`--${name}`);

async function serve(dir) {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    let p = normalize(decodeURIComponent(url.pathname));
    if (p === '/' || p === '\\') p = '/index.html';
    const file = join(dir, p);
    if (!file.startsWith(dir)) {
      res.writeHead(403).end('forbidden');
      return;
    }
    try {
      const body = await readFile(file);
      // No CSP header on purpose: the page must be able to run its own script
      // and open IndexedDB on this origin.
      res.writeHead(200, {
        'content-type': MIME[extname(file)] ?? 'application/octet-stream',
        'cache-control': 'no-store',
      });
      res.end(body);
    } catch {
      res.writeHead(404).end('not found');
    }
  });
  await new Promise((ok) => server.listen(0, '127.0.0.1', ok));
  return { server, port: server.address().port };
}

async function main() {
  const browserName = arg('browser', 'chromium');
  const timeoutMs = Number(arg('timeout-ms', 30 * 60 * 1000));
  const iters = arg('iters', null);

  const pw = await loadPlaywright();
  const type = pw[browserName];
  if (!type) throw new Error(`unknown browser: ${browserName}`);

  const launch = { headless: !flag('headed') };
  // Prefer the SAME binary the crossbank wasm lane drives through
  // chromedriver, so "same browser" is literally true rather than
  // "same engine, different build".
  const chromePath = arg('chrome-path', process.env.CROSSBANK_CHROME ?? null);
  if (browserName === 'chromium' && chromePath) launch.executablePath = chromePath;

  const { server, port } = await serve(WEB);
  const browser = await type.launch(launch);
  let doc = null;
  let failure = null;
  const version = browser.version();
  try {
    const page = await browser.newPage();
    page.on('console', (m) => {
      const t = m.text();
      if (t.startsWith('BENCH_JSON ')) {
        doc = JSON.parse(t.slice('BENCH_JSON '.length));
      } else if (t.startsWith('running ') || t.startsWith('BENCH_ERROR')) {
        // Hive CE logs a line per object store; only echo our own progress.
        console.log(`[page] ${t}`);
      }
    });
    page.on('pageerror', (e) => {
      failure ??= String(e);
      console.error(`[pageerror] ${e}`);
    });

    const qs = iters ? `?iters=${encodeURIComponent(iters)}` : '';
    const url = `http://127.0.0.1:${port}/index.html${qs}`;
    console.log(`==> ${browserName} ${chromePath ?? '(bundled)'} -> ${url}`);
    await page.goto(url);
    await page.waitForFunction('globalThis.__benchDone === true', null, {
      timeout: timeoutMs,
      polling: 500,
    });
    if (!doc) {
      const raw = await page.evaluate('globalThis.__benchResult ?? null');
      if (raw) doc = JSON.parse(raw);
    }
    const err = await page.evaluate('globalThis.__benchError ?? null');
    if (err) throw new Error(`page reported: ${err}`);
  } finally {
    await browser.close();
    server.close();
  }

  if (!doc) throw new Error(failure ?? 'the page never produced a BENCH_JSON document');

  // Stamp the browser on every row so Chrome and Firefox rows coexist.
  const short = browserName === 'chromium' ? 'chrome' : browserName;
  for (const s of doc.samples ?? []) s.browser = short;

  const file = resultsPath(ROOT, arg('date', null));
  const base = mergeInto(file, doc, {
    machine: arg('machine', null),
    note: `hive_ce_web rows: Hive CE ${browserName} ${version}${chromePath ? ` (${chromePath})` : ''}, IndexedDB, ${doc.iterations} iterations, median/p99.`,
  });
  console.log(`wrote ${file} (${base.samples.length} rows)`);
  printTable(base);
}

main().catch((e) => {
  console.error(String(e.stack ?? e));
  process.exit(1);
});
