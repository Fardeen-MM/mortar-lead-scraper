/**
 * Website Finder — discovers law firm websites from firm names
 *
 * For leads that have a firm_name but no website, this module tries to find
 * the firm's website. This is crucial because having a website domain enables:
 *   1. Email pattern generation (first.last@domain)
 *   2. SMTP verification
 *   3. Website email crawling
 *
 * Methods (waterfall — stops at first success):
 *   1. Google search scraping: "{firm_name} {city} law firm"
 *   2. Domain guessing: common patterns like firmname.com, firmnamelegal.com
 *   3. DNS verification: check if guessed domain has MX records
 *
 * Zero dependencies — uses built-in https and dns modules.
 */

const https = require('https');
const dns = require('dns');
const { log } = require('./logger');
const { extractDomain, normalizeFirmName } = require('./normalizer');

// Domains that are NOT firm websites (directories, social media, etc.)
const EXCLUDED_DOMAINS = new Set([
  'martindale.com', 'lawyers.com', 'avvo.com', 'justia.com', 'findlaw.com',
  'nolo.com', 'superlawyers.com', 'hg.org', 'lawinfo.com',
  'yelp.com', 'bbb.org', 'manta.com', 'yellowpages.com',
  'linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com',
  'youtube.com', 'tiktok.com', 'reddit.com',
  'google.com', 'bing.com', 'yahoo.com',
  'wikipedia.org', 'wikidata.org',
  'floridabar.org', 'calbar.ca.gov', 'nycourts.gov',
  'courts.state', 'state.gov', 'uscourts.gov',
]);

/**
 * Check if a domain resolves (has DNS A/AAAA records).
 */
function domainExists(domain) {
  return new Promise(resolve => {
    dns.resolve4(domain, (err) => {
      resolve(!err);
    });
  });
}

/**
 * Check if a domain has MX records (can receive email).
 */
function hasMxRecords(domain) {
  return new Promise(resolve => {
    dns.resolveMx(domain, (err, records) => {
      resolve(!err && records && records.length > 0);
    });
  });
}

/**
 * Generate likely domain names from a firm name.
 * "Smith & Jones LLP" → ['smithjones.com', 'smithjoneslaw.com', 'sjlaw.com', ...]
 */
function generateDomainGuesses(firmName, city) {
  if (!firmName) return [];

  // Clean firm name: remove legal suffixes, punctuation
  const cleaned = normalizeFirmName(firmName)
    .replace(/[^a-z0-9\s]/g, '')
    .trim();

  if (!cleaned || cleaned.length < 2) return [];

  const words = cleaned.split(/\s+/).filter(w => w.length > 1);
  if (words.length === 0) return [];

  const guesses = [];
  const base = words.join('');           // smithjones
  const dashed = words.join('-');        // smith-jones
  const firstWord = words[0];           // smith

  // Most common patterns for law firms
  guesses.push(`${base}.com`);
  guesses.push(`${base}law.com`);
  guesses.push(`${base}legal.com`);
  guesses.push(`${dashed}.com`);
  guesses.push(`${firstWord}law.com`);
  guesses.push(`${firstWord}legal.com`);

  // Initials
  if (words.length >= 2) {
    const initials = words.map(w => w[0]).join('');
    guesses.push(`${initials}law.com`);
    guesses.push(`${initials}legal.com`);
  }

  // With "the"
  guesses.push(`the${base}.com`);

  // ".law" TLD (newer, some firms use it)
  guesses.push(`${base}.law`);
  guesses.push(`${firstWord}.law`);

  return [...new Set(guesses)];
}

/**
 * Try to find a firm's website by checking common domain patterns.
 * Fast — only DNS lookups, no HTTP requests.
 *
 * @param {string} firmName - Firm name
 * @param {string} [city] - City for disambiguation
 * @returns {string} Found website URL or ''
 */
async function findByDomainGuessing(firmName, city) {
  const guesses = generateDomainGuesses(firmName, city);
  if (guesses.length === 0) return '';

  // Check domains in parallel (fast — just DNS)
  const results = await Promise.all(
    guesses.map(async domain => {
      const exists = await domainExists(domain);
      if (!exists) return null;
      // Bonus: check MX records (if it has email, it's more likely a real firm site)
      const hasMx = await hasMxRecords(domain);
      return { domain, hasMx };
    })
  );

  // Prefer domains with MX records
  const withMx = results.filter(r => r && r.hasMx);
  if (withMx.length > 0) return `https://${withMx[0].domain}`;

  // Fallback to any resolved domain
  const resolved = results.filter(r => r);
  if (resolved.length > 0) return `https://${resolved[0].domain}`;

  return '';
}

/**
 * Extract firm website from Google search results.
 * Scrapes Google search HTML for organic result URLs.
 *
 * @param {string} query - Search query
 * @returns {string[]} Array of result URLs
 */
function googleSearch(query) {
  return new Promise((resolve) => {
    const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=5`;

    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 10000,
    };

    const req = https.get(searchUrl, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        // Extract URLs from Google search results
        const urls = [];
        // Pattern 1: /url?q=https://...&
        const urlPattern = /\/url\?q=(https?:\/\/[^&"]+)/g;
        let match;
        while ((match = urlPattern.exec(data)) !== null) {
          try {
            const url = decodeURIComponent(match[1]);
            const domain = new URL(url).hostname.replace(/^www\./, '');
            if (!EXCLUDED_DOMAINS.has(domain) && !domain.endsWith('.gov')) {
              urls.push(url);
            }
          } catch {}
        }

        // Pattern 2: Direct href links in result cards
        const hrefPattern = /href="(https?:\/\/(?!www\.google)[^"]+)"/g;
        while ((match = hrefPattern.exec(data)) !== null) {
          try {
            const url = decodeURIComponent(match[1]);
            const domain = new URL(url).hostname.replace(/^www\./, '');
            if (!EXCLUDED_DOMAINS.has(domain) && !domain.endsWith('.gov') && !urls.includes(url)) {
              urls.push(url);
            }
          } catch {}
        }

        resolve(urls.slice(0, 5));
      });
    });

    req.on('error', () => resolve([]));
    req.on('timeout', () => { req.destroy(); resolve([]); });
  });
}

/**
 * Find a firm's website using Google search.
 *
 * @param {string} firmName
 * @param {string} city
 * @param {string} state
 * @returns {string} Website URL or ''
 */
async function findByGoogleSearch(firmName, city, state) {
  if (!firmName) return '';

  const query = `"${firmName}" ${city || ''} ${state || ''} law firm website`;
  const results = await googleSearch(query);

  if (results.length === 0) return '';

  // Return the first non-directory result
  return results[0] || '';
}

/**
 * Find website for a single lead. Tries domain guessing first (fast),
 * then Google search (slower but more accurate).
 *
 * @param {object} lead - Lead with firm_name, city, state
 * @returns {string} Website URL or ''
 */
async function findWebsite(lead) {
  if (!lead.firm_name) return '';

  // Method 1: Domain guessing (fast — DNS only)
  const guessed = await findByDomainGuessing(lead.firm_name, lead.city);
  if (guessed) return guessed;

  // Method 2: Google search (slower — HTTP request)
  const googled = await findByGoogleSearch(lead.firm_name, lead.city, lead.state);
  if (googled) return googled;

  return '';
}

/**
 * Batch find websites for leads missing them.
 *
 * @param {object[]} leads - Array of leads
 * @param {object} options
 * @param {function} [options.onProgress] - Callback(current, total, detail)
 * @param {function} [options.isCancelled] - Returns true to stop
 * @param {boolean} [options.googleSearch=true] - Enable Google search fallback
 * @returns {{ found, skipped, failed }}
 */
async function batchFindWebsites(leads, options = {}) {
  const { onProgress, isCancelled = () => false, googleSearch: useGoogle = true } = options;
  const stats = { found: 0, skipped: 0, failed: 0 };

  // Filter to leads that need websites
  const needsWebsite = leads.filter(l => !l.website && l.firm_name);
  const total = needsWebsite.length;

  // Group by firm name to avoid duplicate lookups
  const firmCache = new Map();

  let processed = 0;
  for (const lead of needsWebsite) {
    if (isCancelled()) break;

    const firmKey = normalizeFirmName(lead.firm_name);
    if (firmCache.has(firmKey)) {
      const cached = firmCache.get(firmKey);
      if (cached) {
        lead.website = cached;
        lead.website_source = 'domain-guess';
        stats.found++;
      } else {
        stats.skipped++;
      }
      processed++;
      if (onProgress) onProgress(processed, total, `${lead.first_name} ${lead.last_name}`);
      continue;
    }

    try {
      // Method 1: Domain guessing (fast)
      let website = await findByDomainGuessing(lead.firm_name, lead.city);

      // Method 2: Google search (if enabled and guessing failed)
      if (!website && useGoogle) {
        website = await findByGoogleSearch(lead.firm_name, lead.city, lead.state);
        // Rate limit Google searches
        await new Promise(r => setTimeout(r, 2000 + Math.random() * 3000));
      }

      firmCache.set(firmKey, website || '');

      if (website) {
        lead.website = website;
        lead.website_source = 'domain-guess';
        stats.found++;
      } else {
        stats.skipped++;
      }
    } catch (err) {
      stats.failed++;
      firmCache.set(firmKey, '');
    }

    processed++;
    if (onProgress) onProgress(processed, total, `${lead.first_name} ${lead.last_name}`);
  }

  return stats;
}

module.exports = {
  findWebsite,
  findByDomainGuessing,
  findByGoogleSearch,
  batchFindWebsites,
  generateDomainGuesses,
};
