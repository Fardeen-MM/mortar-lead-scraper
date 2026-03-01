/**
 * DuckDuckGo Web Search Scraper — discover businesses via web search
 *
 * Why DuckDuckGo: No CAPTCHAs, no bot detection, server-side HTML rendering.
 * No API key needed. Plain HTTP requests with Cheerio parsing.
 *
 * Search strategies:
 *   1. Direct: "{niche} in {location}"
 *   2. Yelp X-ray: site:yelp.com "{niche}" "{location}"
 *   3. BBB X-ray: site:bbb.org "{niche}" "{location}"
 *   4. Contact discovery: "{niche}" "{location}" phone email
 *
 * Usage:
 *   const ddg = require('./lib/duckduckgo-scraper');
 *   const results = await ddg.search('dentists', 'Miami, FL');
 *   // [{ name, url, domain, snippet, source }, ...]
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const { log } = require('./logger');
const { normalizePhone, extractDomain } = require('./normalizer');

// Polite delay between requests (3-5s)
const MIN_DELAY = 3000;
const MAX_DELAY = 5000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function randomDelay() {
  return MIN_DELAY + Math.random() * (MAX_DELAY - MIN_DELAY);
}

// User agents for rotation
const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];

// Domains to skip (not business websites)
const SKIP_DOMAINS = new Set([
  'duckduckgo.com', 'google.com', 'wikipedia.org', 'facebook.com',
  'twitter.com', 'x.com', 'instagram.com', 'linkedin.com', 'youtube.com',
  'reddit.com', 'pinterest.com', 'tiktok.com', 'amazon.com',
  'indeed.com', 'glassdoor.com', 'craigslist.org',
]);

// Directory domains — extract business info from these differently
const DIRECTORY_DOMAINS = new Set([
  'yelp.com', 'bbb.org', 'yellowpages.com', 'manta.com',
  'healthgrades.com', 'zocdoc.com', 'angi.com', 'thumbtack.com',
  'homeadvisor.com', 'houzz.com',
]);

/**
 * POST to DuckDuckGo HTML endpoint (DDG requires POST for search).
 */
function httpPost(url, postData) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    const parsed = new URL(url);
    const body = typeof postData === 'string' ? postData : new URLSearchParams(postData).toString();

    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 15000,
    };

    const req = mod.request(options, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          redirectUrl = `${parsed.protocol}//${parsed.host}${redirectUrl}`;
        }
        res.resume();
        return httpPost(redirectUrl, postData).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      let responseBody = '';
      res.on('data', chunk => responseBody += chunk);
      res.on('end', () => resolve(responseBody));
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(body);
    req.end();
  });
}

/**
 * Parse DuckDuckGo HTML results page.
 * DDG serves server-rendered HTML — no JS rendering needed.
 */
function parseDDGResults(html) {
  const $ = cheerio.load(html);
  const results = [];

  // DuckDuckGo organic results
  $('article[data-testid="result"], .result, .results_links, .result__body').each((i, el) => {
    const $el = $(el);

    // Try multiple selector patterns (DDG changes layout periodically)
    let title = $el.find('h2 a, a[data-testid="result-title-a"], .result__a, .result__title a').first().text().trim();
    let url = $el.find('h2 a, a[data-testid="result-title-a"], .result__a, .result__title a').first().attr('href') || '';
    let snippet = $el.find('[data-result="snippet"], .result__snippet, .result__body .result__snippet').first().text().trim();

    // Clean up DDG redirect URLs
    if (url.includes('//duckduckgo.com/l/?uddg=')) {
      try {
        const decoded = new URL(url);
        url = decodeURIComponent(decoded.searchParams.get('uddg') || url);
      } catch {}
    }

    if (!title || !url || url.startsWith('javascript:')) return;

    // Parse domain
    let domain = '';
    try {
      domain = new URL(url).hostname.replace(/^www\./, '');
    } catch {
      return;
    }

    // Skip non-business domains
    if (SKIP_DOMAINS.has(domain)) return;

    results.push({ title, url, domain, snippet });
  });

  return results;
}

/**
 * Extract business name from a search result title.
 * Removes common suffixes like "- Yelp", "| BBB", "- Home", etc.
 */
function extractBusinessName(title, domain) {
  let name = title;

  // Remove common suffixes
  name = name
    .replace(/\s*[-|–—]\s*(Yelp|BBB|Yellow\s*Pages|Manta|Reviews?|Home|About|Contact).*$/i, '')
    .replace(/\s*[-|–—]\s*\d+ Reviews?.*$/i, '')
    .replace(/\s*\(\d+ Reviews?\).*$/i, '')
    .replace(/\s*[-|–—]\s*Updated \d{4}.*$/i, '')
    .trim();

  return name || title;
}

/**
 * Extract phone numbers from snippet text.
 */
function extractPhoneFromSnippet(snippet) {
  const phoneMatch = snippet.match(/(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
  if (phoneMatch) {
    return normalizePhone(phoneMatch[0]);
  }
  // UK phone
  const ukMatch = snippet.match(/0\d{2,4}[\s-]?\d{3,4}[\s-]?\d{3,4}/);
  if (ukMatch) return normalizePhone(ukMatch[0]);
  // AU phone
  const auMatch = snippet.match(/0[2-9]\s?\d{4}\s?\d{4}/);
  if (auMatch) return normalizePhone(auMatch[0]);
  return '';
}

/**
 * Build search queries for a niche + location.
 */
function buildQueries(niche, location) {
  return [
    `${niche} in ${location}`,
    `site:yelp.com "${niche}" "${location}"`,
    `site:bbb.org "${niche}" "${location}"`,
    `"${niche}" "${location}" phone email`,
  ];
}

/**
 * Search DuckDuckGo for a single query.
 * DDG HTML endpoint requires POST with form data.
 * Returns array of parsed results.
 */
async function searchQuery(query) {
  const url = 'https://html.duckduckgo.com/html/';

  try {
    const html = await httpPost(url, { q: query });
    return parseDDGResults(html);
  } catch (err) {
    log.warn(`[DDG] Search failed for "${query}": ${err.message}`);
    return [];
  }
}

/**
 * Search DuckDuckGo for businesses matching a niche + location.
 *
 * @param {string} niche - Business type (e.g., "dentists", "plumbers")
 * @param {string} location - Location (e.g., "Miami, FL", "London, UK")
 * @param {object} [options]
 * @param {number} [options.maxResults=100] - Maximum results to return
 * @param {function} [options.onProgress] - Progress callback (current, total, query)
 * @returns {Promise<object[]>} Array of { name, url, domain, phone, snippet, source }
 */
async function search(niche, location, options = {}) {
  const maxResults = options.maxResults || 100;
  const onProgress = options.onProgress || (() => {});

  const queries = buildQueries(niche, location);
  const seenDomains = new Set();
  const allResults = [];

  for (let i = 0; i < queries.length; i++) {
    const query = queries[i];
    onProgress(i + 1, queries.length, query);
    log.info(`[DDG] Searching: "${query}"`);

    const results = await searchQuery(query);

    for (const result of results) {
      // Dedup by domain
      if (seenDomains.has(result.domain)) continue;
      seenDomains.add(result.domain);

      const isDirectory = DIRECTORY_DOMAINS.has(result.domain);
      const businessName = extractBusinessName(result.title, result.domain);
      const phone = extractPhoneFromSnippet(result.snippet);

      allResults.push({
        name: businessName,
        url: result.url,
        domain: result.domain,
        website: isDirectory ? '' : result.url,
        phone,
        snippet: result.snippet,
        source: isDirectory ? `ddg_${result.domain.split('.')[0]}` : 'ddg_organic',
      });

      if (allResults.length >= maxResults) break;
    }

    if (allResults.length >= maxResults) break;

    // Polite delay between queries
    if (i < queries.length - 1) {
      await sleep(randomDelay());
    }
  }

  log.info(`[DDG] Found ${allResults.length} businesses for "${niche}" in "${location}"`);
  return allResults;
}

module.exports = { search, searchQuery, parseDDGResults, extractBusinessName, extractPhoneFromSnippet };
