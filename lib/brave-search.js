/**
 * Brave Search HTML Scraper — FREE, no API key, no rate limit seen
 *
 * Brave has its own 20B+ page index (not Google-derived). HTTP GET to
 * `search.brave.com/search?q=X` returns ~20 results in clean Svelte HTML.
 *
 * Tested 2026-04-17: 5 rapid queries, all HTTP 200, no CAPTCHA, 15-20
 * unique external URLs per query. Much more reliable than DDG for 2026.
 *
 * HTML structure:
 *   - <div data-type="web"> — wraps each organic result (20 per query)
 *   - <a href="{external_url}"> — result link
 *   - <div class="title search-snippet-title ...">{title}</div>
 *   - <div class="generic-snippet ...">{snippet}</div>
 *
 * Usage:
 *   const brave = require('./lib/brave-search');
 *   const results = await brave.search('immigration consultant miami');
 *   // [{ title, url, domain, snippet, source: 'brave' }, ...]
 */

const https = require('https');
const cheerio = require('cheerio');

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

const SKIP_DOMAINS = new Set([
  'brave.com', 'search.brave.com', 'brave.app', 'status.brave.app',
  'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
  'wikipedia.org', 'wikidata.org',
  'facebook.com', 'twitter.com', 'x.com', 'instagram.com',
  'linkedin.com', 'youtube.com', 'reddit.com',
  'hackerone.com',
  'creativecommons.org',
]);

const DIRECTORY_DOMAINS = new Set([
  'yelp.com', 'yelp.ca', 'bbb.org', 'yellowpages.com', 'yellowpages.ca',
  'manta.com', 'healthgrades.com', 'zocdoc.com', 'angi.com',
  'mapquest.com', 'trustanalytica.org',
]);

function httpGetOnce(url, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    const req = https.get(url, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
      },
      timeout: timeoutMs,
    }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        const err = new Error(`Brave HTTP ${res.statusCode}`);
        err.statusCode = res.statusCode;
        return reject(err);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const enc = res.headers['content-encoding'];
        if (enc === 'gzip') {
          require('zlib').gunzip(buf, (err, out) => err ? reject(err) : resolve(out.toString('utf-8')));
        } else if (enc === 'br') {
          require('zlib').brotliDecompress(buf, (err, out) => err ? reject(err) : resolve(out.toString('utf-8')));
        } else {
          resolve(buf.toString('utf-8'));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Brave timeout')); });
  });
}

/**
 * HTTP GET — NO internal retry. Fail fast on 429 so the multi-engine
 * orchestrator can move on to Startpage/Mojeek/DDG. Caller (multi-engine-search)
 * tracks backoff per engine.
 */
async function httpGet(url, timeoutMs = 20000) {
  return httpGetOnce(url, timeoutMs);
}

function extractDomain(url) {
  if (!url) return '';
  try {
    let u = url.trim();
    if (!u.startsWith('http')) u = 'https://' + u;
    return new URL(u).hostname.replace(/^www\./, '').toLowerCase();
  } catch { return ''; }
}

/**
 * Parse Brave SERP HTML into structured results.
 */
function parseBraveHtml(html) {
  const $ = cheerio.load(html);
  const results = [];
  const seenDomains = new Set();

  // Brave wraps each web result in [data-type="web"]
  $('[data-type="web"]').each((_, el) => {
    const $el = $(el);

    // Find the outermost link (result URL)
    const $link = $el.find('a[href^="http"]').first();
    const url = $link.attr('href') || '';
    if (!url) return;

    const domain = extractDomain(url);
    if (!domain || SKIP_DOMAINS.has(domain)) return;
    if (seenDomains.has(domain)) return;
    seenDomains.add(domain);

    // Title in <div class="title search-snippet-title ...">
    const title = $el.find('.title, [class*="snippet-title"]').first().text().trim()
      || $link.text().trim();

    // Snippet in <div class="generic-snippet ...">
    const snippet = $el.find('.generic-snippet, [class*="snippet"]').filter((_, s) => {
      return !$(s).hasClass('snippet-title');
    }).first().text().trim();

    results.push({
      title: title.slice(0, 200),
      url,
      domain,
      snippet: snippet.slice(0, 400),
      is_directory: DIRECTORY_DOMAINS.has(domain),
      source: 'brave',
    });
  });

  // Fallback: regex-extract all external links if structured parsing yielded few
  if (results.length < 5) {
    const urlRe = /href="(https?:\/\/[^"]+)"/g;
    let m;
    while ((m = urlRe.exec(html)) !== null) {
      const url = m[1];
      const domain = extractDomain(url);
      if (!domain || SKIP_DOMAINS.has(domain) || seenDomains.has(domain)) continue;
      seenDomains.add(domain);
      results.push({
        title: '',
        url,
        domain,
        snippet: '',
        is_directory: DIRECTORY_DOMAINS.has(domain),
        source: 'brave-fallback',
      });
      if (results.length >= 25) break;
    }
  }

  return results;
}

/**
 * Search Brave and return parsed results.
 *
 * @param {string} query - The search query
 * @param {object} [opts]
 * @param {number} [opts.page=0] - Page number (0-indexed; Brave uses offset=0,20,40...)
 * @param {string} [opts.country='us'] - Country localization
 * @returns {Promise<Array>} Array of { title, url, domain, snippet, is_directory, source }
 */
async function search(query, opts = {}) {
  const q = encodeURIComponent(query);
  const page = opts.page || 0;
  const country = opts.country || 'us';
  // offset is 0, 20, 40 ... for Brave pagination
  const offset = page * 20;
  const url = `https://search.brave.com/search?q=${q}&offset=${offset}&country=${country}&source=web`;

  const html = await httpGet(url, opts.timeoutMs || 20000);
  return parseBraveHtml(html);
}

/**
 * Multi-page search — paginate up to maxResults.
 */
async function searchPaginated(query, maxResults = 50, opts = {}) {
  const all = [];
  const seen = new Set();
  const maxPages = Math.ceil(maxResults / 20);

  for (let page = 0; page < maxPages; page++) {
    try {
      const results = await search(query, { ...opts, page });
      let newCount = 0;
      for (const r of results) {
        if (seen.has(r.domain)) continue;
        seen.add(r.domain);
        all.push(r);
        newCount++;
        if (all.length >= maxResults) return all;
      }
      // If page returned no new results, stop
      if (newCount === 0) break;
      // Polite delay between pages
      if (page < maxPages - 1) {
        await new Promise(r => setTimeout(r, 500 + Math.random() * 500));
      }
    } catch (err) {
      console.error(`[Brave] page ${page} error: ${err.message}`);
      break;
    }
  }
  return all;
}

module.exports = {
  search,
  searchPaginated,
  parseBraveHtml,
};
