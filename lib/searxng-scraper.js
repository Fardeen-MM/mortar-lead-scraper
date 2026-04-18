/**
 * SearxNG Multi-Engine Search Scraper
 *
 * Per audit 2026-04-16: DDG HTML endpoint is degrading (documented HTTP 202
 * rate-limit errors across CrewAI / open-webui / gpt-researcher). SearxNG is
 * the recommended rewrite — it aggregates 70+ engines behind YOUR own IP,
 * rotating automatically with no single-engine rate limit.
 *
 * USAGE:
 *   1. Self-host SearxNG:
 *      docker run -p 8080:8080 searxng/searxng
 *      export SEARXNG_URL=http://localhost:8080
 *   2. OR use a public instance from https://searx.space (less reliable):
 *      export SEARXNG_URL=https://searx.be
 *
 * API: GET {base}/search?q={q}&format=json&engines=google,bing,duckduckgo,brave
 *
 * Exports same interface as duckduckgo-scraper.js for drop-in replacement.
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');
const { extractDomain } = require('./normalizer');

const DEFAULT_URL = process.env.SEARXNG_URL || 'http://localhost:8080';
const DEFAULT_ENGINES = (process.env.SEARXNG_ENGINES || 'google,bing,duckduckgo,brave').split(',');

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
];

const SKIP_DOMAINS = new Set([
  'duckduckgo.com', 'google.com', 'bing.com', 'yahoo.com', 'wikipedia.org',
  'facebook.com', 'twitter.com', 'x.com', 'instagram.com', 'linkedin.com',
  'youtube.com', 'reddit.com', 'pinterest.com', 'tiktok.com', 'amazon.com',
  'indeed.com', 'glassdoor.com',
  'forbes.com', 'expertise.com', 'cleverly.com', 'bark.com',
  'nextdoor.com', 'chamberofcommerce.com', 'superpages.com', 'mapquest.com',
  'toprated.com', 'trustpilot.com', 'nerdwallet.com', 'bankrate.com', 'usnews.com',
]);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * Query SearxNG JSON API.
 * Returns parsed results array or throws.
 */
async function querySearxNG(baseUrl, query, options = {}) {
  const engines = options.engines || DEFAULT_ENGINES;
  const pageNum = options.page || 1;
  const categories = options.categories || 'general';

  const params = new URLSearchParams({
    q: query,
    format: 'json',
    engines: engines.join(','),
    pageno: String(pageNum),
    categories,
    language: options.language || 'en',
    safesearch: '0',
  });

  const fullUrl = `${baseUrl}/search?${params.toString()}`;
  const parsed = new URL(fullUrl);
  const mod = parsed.protocol === 'https:' ? https : http;
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

  return new Promise((resolve, reject) => {
    const req = mod.get({
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      headers: {
        'User-Agent': ua,
        'Accept': 'application/json,text/html;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 30000,
    }, (res) => {
      let body = '';
      res.on('data', (c) => body += c);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          return reject(new Error(`SearxNG HTTP ${res.statusCode} — is SEARXNG_URL=${baseUrl} reachable?`));
        }
        try {
          const j = JSON.parse(body);
          resolve(j);
        } catch (err) {
          reject(new Error(`SearxNG non-JSON response: ${err.message}`));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('SearxNG timeout')); });
  });
}

/**
 * Extract business name from result title/URL.
 */
function extractBusinessName(title, domain) {
  if (!title) return domain || '';
  // Remove trailing " | Location" or " - SiteName" patterns
  let name = title.split(/\s+[|•·\-–—]\s+/)[0].trim();
  if (name.length > 80) name = name.slice(0, 80).trim();
  return name;
}

/**
 * Build search queries for a niche + location.
 */
function buildQueries(niche, location) {
  const loc = location ? ` ${location}` : '';
  return [
    `${niche}${loc}`,
    `"${niche}"${loc ? ` "${location}"` : ''}`,
    `${niche}${loc} contact email`,
  ];
}

/**
 * Main search function — niche + location → array of businesses.
 * Compatible with duckduckgo-scraper.js interface.
 */
async function search(niche, location, options = {}) {
  const maxResults = options.maxResults || 100;
  const baseUrl = options.searxngUrl || DEFAULT_URL;
  const onProgress = options.onProgress || (() => {});

  const queries = buildQueries(niche, location);
  const seenDomains = new Set();
  const results = [];

  for (let i = 0; i < queries.length; i++) {
    const q = queries[i];
    onProgress(i + 1, queries.length, q);

    let json;
    try {
      json = await querySearxNG(baseUrl, q, options);
    } catch (err) {
      // SearxNG unreachable — caller can fall back to DDG
      throw new Error(`SearxNG query failed: ${err.message}`);
    }

    const items = json.results || [];
    for (const r of items) {
      if (!r.url) continue;
      let domain;
      try { domain = new URL(r.url).hostname.replace(/^www\./, ''); }
      catch { continue; }

      if (SKIP_DOMAINS.has(domain)) continue;
      if (seenDomains.has(domain)) continue;

      // Skip listicle/directory pages
      const lowerTitle = (r.title || '').toLowerCase();
      if (/\b(best|top)\s+\d+\b/i.test(lowerTitle) ||
          /\bthe best\s+\d*/i.test(lowerTitle) ||
          /\b(ranking|ranked|list|directory|guide|comparison|review)\b/i.test(lowerTitle)) {
        continue;
      }

      seenDomains.add(domain);
      results.push({
        name: extractBusinessName(r.title, domain),
        url: r.url,
        domain,
        snippet: r.content || r.snippet || '',
        phone: '',
        source: 'searxng',
        engine: r.engine || 'unknown',
      });

      if (results.length >= maxResults) return results;
    }

    // Polite delay between queries
    await sleep(1000);
  }

  return results;
}

/**
 * Quick connectivity check — returns true if SearxNG is reachable.
 */
async function isReachable(baseUrl = DEFAULT_URL) {
  try {
    const j = await querySearxNG(baseUrl, 'test', { engines: ['duckduckgo'] });
    return Array.isArray(j.results);
  } catch {
    return false;
  }
}

module.exports = {
  search,
  querySearxNG,
  isReachable,
  DEFAULT_URL,
};
