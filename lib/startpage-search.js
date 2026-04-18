/**
 * Startpage Search HTML Scraper — FREE, Google-proxied results.
 *
 * Startpage serves Google search results via their own proxy. No API key needed,
 * no direct rate limit seen (within reason). Results look 1:1 with Google.
 *
 * URL: https://www.startpage.com/do/search?query=X&cat=web
 *
 * HTML structure:
 *   - <a class="result-title result-link css-1bggj8v" href="{external_url}">
 *   - <div class="result css-o7i03b"> — wraps each result
 *
 * Exports same interface as brave-search for drop-in use in find-leads.js.
 */

const https = require('https');
const cheerio = require('cheerio');

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
];

const SKIP_DOMAINS = new Set([
  'startpage.com', 's8.sx',
  'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com',
  'wikipedia.org', 'wikidata.org',
  'facebook.com', 'twitter.com', 'x.com', 'instagram.com',
  'linkedin.com', 'youtube.com', 'reddit.com',
  'mastodon.social',
]);

const DIRECTORY_DOMAINS = new Set([
  'yelp.com', 'yelp.ca', 'bbb.org', 'yellowpages.com', 'yellowpages.ca',
  'manta.com', 'healthgrades.com', 'zocdoc.com', 'angi.com',
]);

function httpGetOnce(url, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    const req = https.get(url, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
      },
      timeout: timeoutMs,
    }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        const err = new Error(`Startpage HTTP ${res.statusCode}`);
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
    req.on('timeout', () => { req.destroy(); reject(new Error('Startpage timeout')); });
  });
}

// Fail fast — multi-engine orchestrator handles backoff + rotation.
async function httpGet(url, timeoutMs = 25000) {
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

function parseStartpageHtml(html) {
  const $ = cheerio.load(html);
  const results = [];
  const seenDomains = new Set();

  // Primary selector: .result-title.result-link
  $('a.result-title, a.result-link, a[class*="result-link"]').each((_, el) => {
    const $el = $(el);
    const url = $el.attr('href') || '';
    if (!url || !url.startsWith('http')) return;

    const domain = extractDomain(url);
    if (!domain || SKIP_DOMAINS.has(domain)) return;
    if (seenDomains.has(domain)) return;
    seenDomains.add(domain);

    const title = $el.text().trim() || $el.attr('aria-label') || '';

    // Find nearby snippet
    const $parent = $el.closest('[class*="result"]');
    let snippet = $parent.find('[class*="snippet"], [class*="description"], p').first().text().trim();

    results.push({
      title: title.slice(0, 200),
      url,
      domain,
      snippet: snippet.slice(0, 400),
      is_directory: DIRECTORY_DOMAINS.has(domain),
      source: 'startpage',
    });
  });

  // Fallback: regex-extract all external URLs if structured yielded few
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
        source: 'startpage-fallback',
      });
      if (results.length >= 25) break;
    }
  }

  return results;
}

async function search(query, opts = {}) {
  const q = encodeURIComponent(query);
  const cat = opts.category || 'web';
  const url = `https://www.startpage.com/do/search?query=${q}&cat=${cat}`;

  const html = await httpGet(url, opts.timeoutMs || 25000);
  return parseStartpageHtml(html);
}

module.exports = {
  search,
  parseStartpageHtml,
};
