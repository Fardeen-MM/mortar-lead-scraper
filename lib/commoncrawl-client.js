/**
 * Common Crawl CDX Client — find historical emails from the web archive
 *
 * Free API, no key needed. Petabytes of archived web pages.
 * Uses the CDX API to search for archived pages, then extracts emails
 * from the archived HTML content.
 *
 * Rate limit: 1 req/sec (Common Crawl asks for politeness).
 *
 * Usage:
 *   const cc = require('./lib/commoncrawl-client');
 *   const emails = await cc.findEmails('example.com');
 *   // ['john@example.com', 'jane@example.com']
 */

const https = require('https');
const http = require('http');
const { createGunzip } = require('zlib');
const { log } = require('./logger');

// Pages likely to contain contact info
const CONTACT_PATHS = ['/contact', '/about', '/team', '/staff', '/our-team', '/people', '/about-us'];

// Email regex (same as email-finder.js)
const EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

// Domains to ignore in extracted emails
const IGNORE_DOMAINS = new Set([
  'example.com', 'sentry.io', 'wixpress.com', 'googleapis.com',
  'w3.org', 'schema.org', 'wordpress.org', 'gravatar.com',
  'creativecommons.org', 'facebook.com', 'twitter.com',
  'jquery.com', 'cloudflare.com', 'google.com', 'gstatic.com',
]);

// Free email providers — not business emails
const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me',
]);

// Common Crawl indexes — check recent ones first
const CRAWL_INDEXES = [
  'CC-MAIN-2025-08',
  'CC-MAIN-2025-05',
  'CC-MAIN-2024-51',
  'CC-MAIN-2024-46',
  'CC-MAIN-2024-42',
];

/**
 * HTTP GET with timeout and gzip handling.
 */
function httpGet(url, timeout = 15000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;

    const req = mod.get(url, {
      headers: {
        'User-Agent': 'MortarLeadScraper/1.0 (contact@mortarmetrics.com)',
        'Accept': '*/*',
      },
      timeout,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return httpGet(res.headers.location, timeout).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve(body));
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

/**
 * Fetch WARC record content from Common Crawl S3.
 */
function fetchWarcContent(filename, offset, length) {
  return new Promise((resolve, reject) => {
    const url = `https://data.commoncrawl.org/${filename}`;
    const endByte = parseInt(offset) + parseInt(length) - 1;

    const req = https.get(url, {
      headers: {
        'User-Agent': 'MortarLeadScraper/1.0',
        'Range': `bytes=${offset}-${endByte}`,
      },
      timeout: 20000,
    }, (res) => {
      if (res.statusCode !== 206 && res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      const chunks = [];
      const decompressor = createGunzip();

      let output = '';
      decompressor.on('data', chunk => output += chunk.toString('utf-8'));
      decompressor.on('end', () => resolve(output));
      decompressor.on('error', (err) => {
        // If decompression fails, try raw content
        resolve(Buffer.concat(chunks).toString('utf-8'));
      });

      res.on('data', chunk => {
        chunks.push(chunk);
        decompressor.write(chunk);
      });
      res.on('end', () => decompressor.end());
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

/**
 * Search Common Crawl CDX API for archived pages of a domain.
 *
 * @param {string} domain - Domain to search (e.g., "smithdental.com")
 * @param {object} [options]
 * @param {string[]} [options.paths] - Specific paths to look for
 * @returns {Promise<object[]>} Array of CDX records
 */
async function searchDomain(domain, options = {}) {
  const paths = options.paths || CONTACT_PATHS;
  const records = [];

  for (const index of CRAWL_INDEXES) {
    try {
      // Search for the domain in this crawl index
      const url = `https://index.commoncrawl.org/${index}-index?url=*.${domain}&output=json&limit=50`;
      const body = await httpGet(url, 20000);

      // Parse NDJSON (one JSON per line)
      const lines = body.trim().split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const record = JSON.parse(line);
          // Filter to contact/about pages
          const urlPath = record.url ? new URL(record.url).pathname.toLowerCase() : '';
          const isContactPage = paths.some(p => urlPath.includes(p)) || urlPath === '/';

          if (isContactPage && record.status === '200') {
            records.push({
              url: record.url,
              filename: record.filename,
              offset: record.offset,
              length: record.length,
              timestamp: record.timestamp,
              index,
            });
          }
        } catch {}
      }

      // Found enough records — no need to check older indexes
      if (records.length >= 10) break;

      // Rate limit: 1 req/sec
      await new Promise(r => setTimeout(r, 1100));
    } catch (err) {
      // 404 means domain not in this index — normal
      if (!err.message.includes('404')) {
        log.warn(`[CommonCrawl] CDX error for ${domain} in ${index}: ${err.message}`);
      }
    }
  }

  return records;
}

/**
 * Extract valid business emails from HTML content.
 */
function extractEmails(html, domain) {
  const matches = html.match(EMAIL_REGEX) || [];
  const emails = new Set();

  for (const email of matches) {
    const lower = email.toLowerCase();
    const emailDomain = lower.split('@')[1];

    // Skip non-business emails
    if (!emailDomain) continue;
    if (IGNORE_DOMAINS.has(emailDomain)) continue;
    if (FREE_PROVIDERS.has(emailDomain)) continue;

    // Prefer emails from the same domain
    if (emailDomain === domain || emailDomain.endsWith('.' + domain)) {
      emails.add(lower);
    }
  }

  return [...emails];
}

/**
 * Find emails for a domain by searching Common Crawl archives.
 *
 * @param {string} domain - Domain to search (e.g., "smithdental.com")
 * @param {object} [options]
 * @param {number} [options.maxPages=5] - Max archived pages to fetch
 * @returns {Promise<string[]>} Array of unique emails found
 */
async function findEmails(domain, options = {}) {
  const maxPages = options.maxPages || 5;

  try {
    const records = await searchDomain(domain);

    if (records.length === 0) {
      return [];
    }

    log.info(`[CommonCrawl] Found ${records.length} archived pages for ${domain}`);

    const allEmails = new Set();
    const fetched = Math.min(records.length, maxPages);

    for (let i = 0; i < fetched; i++) {
      const record = records[i];
      try {
        const content = await fetchWarcContent(record.filename, record.offset, record.length);
        const emails = extractEmails(content, domain);
        emails.forEach(e => allEmails.add(e));

        // Rate limit
        if (i < fetched - 1) {
          await new Promise(r => setTimeout(r, 1100));
        }
      } catch (err) {
        log.warn(`[CommonCrawl] Failed to fetch WARC for ${record.url}: ${err.message}`);
      }
    }

    if (allEmails.size > 0) {
      log.info(`[CommonCrawl] Found ${allEmails.size} email(s) for ${domain}`);
    }

    return [...allEmails];
  } catch (err) {
    log.warn(`[CommonCrawl] Error searching ${domain}: ${err.message}`);
    return [];
  }
}

/**
 * Batch search for emails across multiple domains.
 *
 * @param {string[]} domains - Array of domains to search
 * @param {object} [options]
 * @param {function} [options.onProgress] - Progress callback (current, total, domain)
 * @returns {Promise<Map<string, string[]>>} Map of domain → emails
 */
async function batchFindEmails(domains, options = {}) {
  const onProgress = options.onProgress || (() => {});
  const results = new Map();

  for (let i = 0; i < domains.length; i++) {
    const domain = domains[i];
    onProgress(i + 1, domains.length, domain);

    const emails = await findEmails(domain);
    if (emails.length > 0) {
      results.set(domain, emails);
    }

    // Rate limit between domains
    if (i < domains.length - 1) {
      await new Promise(r => setTimeout(r, 1100));
    }
  }

  return results;
}

module.exports = { search: searchDomain, findEmails, batchFindEmails, extractEmails };
