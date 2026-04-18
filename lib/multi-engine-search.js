/**
 * Multi-Engine Search — rotates across 4 free engines to defeat rate limits.
 *
 * Engines:
 *   1. Brave Search (own 20B-page index) — highest quality, strictest rate limit
 *   2. Startpage (Google-proxied) — best coverage, moderate rate limit
 *   3. Mojeek (own index, 6B pages) — lenient rate limit, narrower coverage
 *   4. DuckDuckGo HTML (Bing-backed) — fallback
 *
 * Strategy:
 *   - Round-robin across engines per query → 4x effective throughput
 *   - On rate limit (429/503), mark engine as backing-off and skip it for N minutes
 *   - Aggregate results across engines for higher recall (different indexes)
 *   - Falls back gracefully if an engine is blocked
 *
 * SearxNG (self-hosted) can be added as engine #5 if SEARXNG_URL env is set.
 *
 * Usage:
 *   const multi = require('./lib/multi-engine-search');
 *   const results = await multi.search('immigration consultant miami', { max: 30 });
 *   // [{ title, url, domain, snippet, source: 'brave'|'startpage'|..., is_directory }, ...]
 */

const brave = require('./brave-search');
const startpage = require('./startpage-search');
const ddg = require('./duckduckgo-scraper');

let mojeek;
try { mojeek = require('./mojeek-search'); } catch { /* optional */ }
let searxng;
try { searxng = require('./searxng-scraper'); } catch { /* optional */ }

// Per-engine backoff state
const BACKOFF = {
  brave: { until: 0, count: 0 },
  startpage: { until: 0, count: 0 },
  mojeek: { until: 0, count: 0 },
  ddg: { until: 0, count: 0 },
  searxng: { until: 0, count: 0 },
};

function isBackedOff(engine) {
  return BACKOFF[engine].until > Date.now();
}

function markBackoff(engine, ms) {
  BACKOFF[engine].until = Date.now() + ms;
  BACKOFF[engine].count++;
}

function markSuccess(engine) {
  // Gradual reset
  BACKOFF[engine].count = Math.max(0, BACKOFF[engine].count - 1);
}

const engines = [
  {
    name: 'brave',
    enabled: true,
    fn: async (query, opts) => brave.search(query, opts),
    cooldownMs: 60000, // 1 min after 429
  },
  {
    name: 'startpage',
    enabled: true,
    fn: async (query, opts) => startpage.search(query, opts),
    cooldownMs: 120000, // 2 min
  },
  {
    name: 'mojeek',
    enabled: !!mojeek,
    fn: async (query, opts) => mojeek ? mojeek.search(query, opts) : [],
    cooldownMs: 60000,
  },
  {
    name: 'ddg',
    enabled: true,
    fn: async (query, opts) => {
      // DDG scraper has different interface (niche+location)
      // Use its raw searchQuery for single-shot
      const raw = await ddg.searchQuery(query);
      return (raw || []).map(r => ({
        title: r.title || '',
        url: r.url || '',
        domain: r.domain || '',
        snippet: r.snippet || '',
        is_directory: false,
        source: 'ddg',
      }));
    },
    cooldownMs: 180000, // 3 min — DDG is most aggressive
  },
];

// Add SearxNG if configured
if (searxng && process.env.SEARXNG_URL) {
  engines.push({
    name: 'searxng',
    enabled: true,
    fn: async (query) => searxng.search(query, '', { maxResults: 30 }),
    cooldownMs: 30000,
  });
}

// Round-robin state (rotates per process invocation)
let rrIndex = Math.floor(Math.random() * engines.length);

function nextEngine() {
  for (let i = 0; i < engines.length; i++) {
    const e = engines[rrIndex];
    rrIndex = (rrIndex + 1) % engines.length;
    if (e.enabled && !isBackedOff(e.name)) return e;
  }
  return null;
}

/**
 * Single-engine search with retry + backoff tracking.
 */
async function searchOne(engine, query, opts = {}) {
  try {
    const results = await engine.fn(query, opts);
    markSuccess(engine.name);
    return results || [];
  } catch (err) {
    if (err.statusCode === 429 || err.statusCode === 503 || /rate|too many|429/i.test(err.message)) {
      console.error(`[multi-search] ${engine.name} rate-limited — backing off ${engine.cooldownMs / 1000}s`);
      markBackoff(engine.name, engine.cooldownMs);
    }
    throw err;
  }
}

/**
 * Main search — tries engines round-robin, accumulates unique results.
 *
 * @param {string} query
 * @param {object} [opts]
 * @param {number} [opts.max=25] - Max unique results to return
 * @param {number} [opts.maxEngines=2] - How many engines to aggregate (higher = slower but more results)
 * @param {boolean} [opts.verbose=false] - Log progress
 */
async function search(query, opts = {}) {
  const max = opts.max || 25;
  const maxEngines = opts.maxEngines || 2;
  const verbose = !!opts.verbose;

  const out = [];
  const seenDomains = new Set();
  let enginesUsed = 0;
  let enginesTried = 0;
  const maxTries = engines.length * 2;

  while (out.length < max && enginesUsed < maxEngines && enginesTried < maxTries) {
    enginesTried++;
    const engine = nextEngine();
    if (!engine) {
      if (verbose) console.log('[multi-search] All engines backed off, waiting 30s');
      await new Promise(r => setTimeout(r, 30000));
      continue;
    }

    try {
      if (verbose) console.log(`[multi-search] Trying ${engine.name}...`);
      const results = await searchOne(engine, query, opts);
      enginesUsed++;

      let newCount = 0;
      for (const r of results) {
        if (!r.domain || seenDomains.has(r.domain)) continue;
        seenDomains.add(r.domain);
        out.push(r);
        newCount++;
        if (out.length >= max) break;
      }
      if (verbose) console.log(`  ${engine.name}: ${results.length} results (${newCount} new, ${out.length} total)`);
    } catch (err) {
      if (verbose) console.log(`  ${engine.name} failed: ${err.message}`);
      continue;
    }
  }

  return out;
}

/**
 * Diagnostic — report which engines are currently healthy.
 */
function status() {
  const now = Date.now();
  return engines.map(e => ({
    name: e.name,
    enabled: e.enabled,
    backed_off: isBackedOff(e.name),
    remaining_ms: Math.max(0, BACKOFF[e.name].until - now),
    backoff_count: BACKOFF[e.name].count,
  }));
}

module.exports = {
  search,
  status,
  engines: engines.map(e => e.name),
};
