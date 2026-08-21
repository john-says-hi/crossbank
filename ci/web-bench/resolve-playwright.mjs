// Locate a Playwright install without adding a package.json to this repo.
//
// Playwright is not a dependency of crossbank; it is a local tool. We try, in
// order: a normal resolve, $PLAYWRIGHT_DIR, the global npm root, and the npx
// cache (`npx playwright ...` leaves a full install there). If none hit, we
// say exactly how to fix it rather than failing with a bare resolve error.
import { createRequire } from 'node:module';
import { existsSync, readdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

function candidates() {
  const out = [];
  if (process.env.PLAYWRIGHT_DIR) out.push(process.env.PLAYWRIGHT_DIR);
  const globalRoot = process.env.NPM_GLOBAL_ROOT;
  if (globalRoot) out.push(join(globalRoot, 'playwright'));
  const npx = join(homedir(), '.npm', '_npx');
  if (existsSync(npx)) {
    for (const d of readdirSync(npx)) {
      out.push(join(npx, d, 'node_modules', 'playwright'));
    }
  }
  return out;
}

// A CJS entry imported by file URL can hand back a namespace whose named
// exports are the bundle's internals rather than the API, so always check that
// `chromium` is actually there before accepting a module.
function usable(mod) {
  if (!mod) return null;
  if (mod.chromium) return mod;
  if (mod.default?.chromium) return mod.default;
  return null;
}

export async function loadPlaywright() {
  try {
    const m = usable(await import('playwright'));
    if (m) return m;
  } catch {
    /* fall through */
  }
  for (const dir of candidates()) {
    if (!existsSync(join(dir, 'package.json'))) continue;
    const entries = [join(dir, 'index.mjs')];
    try {
      const require = createRequire(join(dir, 'noop.js'));
      entries.push(require.resolve('playwright'));
    } catch {
      /* no resolvable main; index.mjs is enough */
    }
    for (const entry of entries) {
      if (!existsSync(entry)) continue;
      try {
        const m = usable(await import(pathToFileURL(entry).href));
        if (m) return m;
      } catch {
        /* keep looking */
      }
    }
  }
  throw new Error(
    'playwright not found. Fix with one of:\n' +
      '  npx playwright --version        # populates the npx cache\n' +
      '  npm i -g playwright             # then set NPM_GLOBAL_ROOT="$(npm root -g)"\n' +
      '  PLAYWRIGHT_DIR=/path/to/node_modules/playwright ci/web-bench/run.mjs',
  );
}
