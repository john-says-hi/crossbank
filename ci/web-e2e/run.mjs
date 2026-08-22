#!/usr/bin/env node
// Drive the crossbank web e2e page through a real browser.
//
//   node ci/web-e2e/run.mjs [--browser chromium|firefox|webkit] [--dir DIR]
//                           [--keys N] [--headed] [--timeout-ms N]
//
// `ci/web-e2e.sh` builds DIR first; this script only serves and drives it.
//
// What it proves that the wasm test lanes cannot:
//
//  1. A REAL reload. `page.reload()` throws away the wasm instance and its
//     heap, so the second read is a fresh module reading bytes some earlier
//     module wrote — which is what an application restart actually is.
//  2. TWO REAL TABS. Coherence rides a BroadcastChannel between documents;
//     tests/web_coherence.rs puts both Banks in one document, so the browser
//     never has to deliver anything between pages.
//
// The static server is deliberately dependency-free and modelled on the one
// in ci/web-bench/run.mjs. No CSP header: the page must run its own module
// and open IndexedDB on this origin.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadPlaywright } from '../web-bench/resolve-playwright.mjs';

const HERE = fileURLToPath(new URL('.', import.meta.url));
const ROOT = resolve(HERE, '..', '..');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
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

const checks = [];
function check(name, ok, detail = '') {
  checks.push({ name, ok, detail });
  console.log(`  ${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

async function ready(page, url, timeoutMs) {
  await page.goto(url, { waitUntil: 'load', timeout: timeoutMs });
  await page.waitForFunction(
    () => window.crossbankE2EReady === true || window.crossbankE2EError,
    null,
    { timeout: timeoutMs },
  );
  const failure = await page.evaluate(() => window.crossbankE2EError ?? null);
  if (failure) throw new Error(`the page failed to start: ${failure}`);
}

async function main() {
  const browserName = arg('browser', 'chromium');
  const dir = resolve(arg('dir', join(ROOT, 'target', 'web-e2e')));
  const keys = Number(arg('keys', 10000));
  const timeoutMs = Number(arg('timeout-ms', 180000));

  const pw = await loadPlaywright();
  const type = pw[browserName];
  if (!type) throw new Error(`unknown browser: ${browserName}`);

  const { server, port } = await serve(dir);
  const url = `http://127.0.0.1:${port}/index.html`;

  // The launch is INSIDE the try. A launch that throws — a browser that was
  // never installed, or one missing a host library — used to leave the http
  // server listening, and a listening server keeps node's event loop alive:
  // the script hung until its outer timeout instead of reporting the error.
  let browser;
  let context;
  try {
    browser = await type.launch({ headless: !flag('headed'), timeout: 60000 });
    // One context, so both pages share an origin AND its storage — which is
    // what makes them two tabs rather than two browsers.
    context = await browser.newContext();

    const page = await context.newPage();
    page.on('pageerror', (e) => console.log(`  [page error] ${e.message}`));
    await ready(page, url, timeoutMs);

    // A run starts from nothing, or a rerun would read the last run's data
    // and the reload check would pass without writing anything.
    await page.evaluate(async () => {
      await window.crossbankE2E.destroy();
      await window.crossbankE2E.open();
    });

    const wrote = Date.now();
    await page.evaluate(async (n) => window.crossbankE2E.writeKeys(n), keys);
    const writeMs = Date.now() - wrote;
    const before = await page.evaluate(async () => window.crossbankE2E.readAll());
    const countBefore = await page.evaluate(() => window.crossbankE2E.count());
    check('wrote every key', countBefore === keys, `${countBefore} in ${writeMs} ms`);

    // ---- a real reload: new wasm instance, new heap, same database --------
    await page.reload({ waitUntil: 'load', timeout: timeoutMs });
    await page.waitForFunction(() => window.crossbankE2EReady === true, null, {
      timeout: timeoutMs,
    });
    const opened = Date.now();
    await page.evaluate(async () => window.crossbankE2E.open());
    const openMs = Date.now() - opened;

    const countAfter = await page.evaluate(() => window.crossbankE2E.count());
    check('count survives a reload', countAfter === keys, `${countAfter} in ${openMs} ms`);

    const after = await page.evaluate(async () => window.crossbankE2E.readAll());
    check('reads are byte-identical after a reload', after === before, `${before} vs ${after}`);

    const verified = await page.evaluate(async (n) => window.crossbankE2E.verify(n), keys);
    check('every value is exactly what was written', verified === 'ok', verified);

    // ---- two real tabs ----------------------------------------------------
    const second = await context.newPage();
    second.on('pageerror', (e) => console.log(`  [tab 2 error] ${e.message}`));
    await ready(second, url, timeoutMs);
    await second.evaluate(async () => window.crossbankE2E.open());

    const shared = `from-tab-2-${Date.now()}`;
    // Tab 1 must not know it yet, or the poll below would prove nothing.
    const knewEarly = await page.evaluate(async (k) => window.crossbankE2E.indexHas(k), shared);
    check('tab 1 does not know the key before it is written', knewEarly === false);

    await second.evaluate(
      async ([k, v]) => window.crossbankE2E.writeOne(k, v),
      [shared, 'hello from the other tab'],
    );

    let sawIt = false;
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      sawIt = await page.evaluate(async (k) => window.crossbankE2E.indexHas(k), shared);
      if (sawIt) break;
      await page.waitForTimeout(50);
    }
    check("tab 2's write reaches tab 1's lazy index", sawIt);

    if (sawIt) {
      const value = await page.evaluate(async (k) => window.crossbankE2E.readKey(k), shared);
      check('and tab 1 reads its value', value === 'hello from the other tab', String(value));
    }

    await second.evaluate(async () => window.crossbankE2E.close());
    await page.evaluate(async () => window.crossbankE2E.destroy());
  } finally {
    if (context) await context.close();
    if (browser) await browser.close();
    server.close();
  }

  const failed = checks.filter((c) => !c.ok);
  console.log(
    `${browserName}: ${checks.length - failed.length}/${checks.length} checks passed`,
  );
  if (failed.length) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
