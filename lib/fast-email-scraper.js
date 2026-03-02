/**
 * Fast HTTP-based email scraper — no Puppeteer needed.
 *
 * Instead of launching a headless browser per site (~30s each),
 * does plain HTTP GETs and regex extraction (~1-2s each).
 * Can scrape 20+ sites in parallel in under 10 seconds.
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');

// Pages most likely to have contact emails
const CONTACT_PATHS = ['/', '/contact', '/contact-us', '/about', '/about-us', '/team', '/our-team', '/attorneys', '/lawyers', '/staff', '/people'];

// Email regex — matches most valid emails, skips image/css files
const EMAIL_RE = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;

// Junk email patterns to filter out
const JUNK_EMAILS = new Set([
  'example@example.com', 'email@example.com', 'name@example.com',
  'info@example.com', 'your@email.com', 'user@example.com',
  'test@test.com', 'noreply@', 'no-reply@', 'donotreply@',
]);
const JUNK_DOMAINS = new Set([
  'example.com', 'test.com', 'sentry.io', 'wixpress.com', 'wix.com',
  'squarespace.com', 'wordpress.com', 'googleapis.com', 'google.com',
  'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
  'gravatar.com', 'w3.org', 'schema.org', 'cloudflare.com',
  'jquery.com', 'jsdelivr.net', 'cdnjs.cloudflare.com', 'gstatic.com',
  'googletagmanager.com', 'google-analytics.com', 'doubleclick.net',
  'fontawesome.com', 'bootstrapcdn.com', 'unpkg.com',
]);
const JUNK_EXTENSIONS = new Set(['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'css', 'js', 'woff', 'woff2', 'ttf', 'eot', 'ico', 'map']);

function isJunkEmail(email) {
  const lower = email.toLowerCase();
  if (JUNK_EMAILS.has(lower)) return true;
  const domain = lower.split('@')[1];
  if (!domain) return true;
  if (JUNK_DOMAINS.has(domain)) return true;
  // Skip if email has file extension (image@2x.png etc)
  const ext = lower.split('.').pop();
  if (JUNK_EXTENSIONS.has(ext)) return true;
  // Skip very long emails (usually CSS/JS artifacts)
  if (email.length > 60) return true;
  // Skip emails starting with common non-person patterns
  if (/^(noreply|no-reply|donotreply|mailer-daemon|postmaster|webmaster|hostmaster|abuse|support@)/.test(lower)) return true;
  return false;
}

/**
 * Fetch a URL with timeout, following redirects.
 */
function fetchPage(url, timeout = 8000, maxRedirects = 3) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(''), timeout);

    try {
      const parsedUrl = new URL(url);
      const client = parsedUrl.protocol === 'https:' ? https : http;

      const req = client.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml',
          'Accept-Language': 'en-US,en;q=0.9',
        },
        timeout: timeout,
        rejectUnauthorized: false,
      }, (res) => {
        // Follow redirects
        if ((res.statusCode === 301 || res.statusCode === 302 || res.statusCode === 307 || res.statusCode === 308) && res.headers.location && maxRedirects > 0) {
          clearTimeout(timer);
          let redirectUrl = res.headers.location;
          if (redirectUrl.startsWith('/')) {
            redirectUrl = `${parsedUrl.protocol}//${parsedUrl.host}${redirectUrl}`;
          }
          res.resume();
          resolve(fetchPage(redirectUrl, timeout, maxRedirects - 1));
          return;
        }

        if (res.statusCode !== 200) {
          clearTimeout(timer);
          res.resume();
          resolve('');
          return;
        }

        let data = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          data += chunk;
          // Cap at 500KB to avoid memory issues
          if (data.length > 500000) {
            res.destroy();
          }
        });
        res.on('end', () => { clearTimeout(timer); resolve(data); });
        res.on('error', () => { clearTimeout(timer); resolve(''); });
      });

      req.on('error', () => { clearTimeout(timer); resolve(''); });
      req.on('timeout', () => { req.destroy(); clearTimeout(timer); resolve(''); });
    } catch {
      clearTimeout(timer);
      resolve('');
    }
  });
}

/**
 * Extract emails from HTML content.
 */
function extractEmails(html) {
  if (!html) return [];

  const emails = new Set();

  // Decode HTML entities first
  const decoded = html.replace(/&#(\d+);/g, (_, n) => String.fromCharCode(n))
                      .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)));

  // Find mailto: links (highest confidence)
  const mailtoRe = /mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi;
  let match;
  while ((match = mailtoRe.exec(decoded)) !== null) {
    const email = match[1].toLowerCase().replace(/\?.*$/, '');
    if (!isJunkEmail(email)) emails.add(email);
  }

  // Find emails in text
  const allEmails = decoded.match(EMAIL_RE) || [];
  for (const email of allEmails) {
    const lower = email.toLowerCase();
    if (!isJunkEmail(lower)) emails.add(lower);
  }

  return [...emails];
}

/**
 * Scrape emails from a single domain by checking multiple pages.
 * Returns array of unique emails found.
 */
async function scrapeEmails(domain, maxPages = 3) {
  const allEmails = new Set();
  const baseUrl = `https://${domain}`;

  // Try pages in order of likelihood to have emails
  for (let i = 0; i < Math.min(maxPages, CONTACT_PATHS.length); i++) {
    const url = `${baseUrl}${CONTACT_PATHS[i]}`;
    try {
      const html = await fetchPage(url);
      const found = extractEmails(html);
      for (const e of found) allEmails.add(e);

      // If we found emails on the homepage, probably don't need more pages
      if (found.length > 0 && i === 0 && found.length >= 2) break;
    } catch {
      // Skip failed pages
    }
  }

  // Filter: prefer emails matching the domain
  const domainEmails = [...allEmails].filter(e => e.endsWith('@' + domain));
  if (domainEmails.length > 0) return domainEmails;

  // Otherwise return all found (could be gmail, etc.)
  return [...allEmails];
}

/**
 * Scrape emails for multiple domains in parallel.
 * @param {Array<{domain: string, ...}>} leads - Array of lead objects with domain field
 * @param {number} concurrency - How many parallel HTTP requests
 * @param {number} maxPagesPerSite - Max pages to check per site
 * @returns {Map<string, string[]>} domain → emails map
 */
async function scrapeEmailsBatch(leads, concurrency = 10, maxPagesPerSite = 3) {
  const results = new Map();
  const domains = [...new Set(leads.filter(l => l.domain).map(l => l.domain))];

  let idx = 0;
  const workers = [];

  for (let w = 0; w < Math.min(concurrency, domains.length); w++) {
    workers.push((async () => {
      while (idx < domains.length) {
        const domain = domains[idx++];
        try {
          const emails = await scrapeEmails(domain, maxPagesPerSite);
          if (emails.length > 0) {
            results.set(domain, emails);
          }
        } catch {
          // Skip failed domains
        }
      }
    })());
  }

  await Promise.all(workers);
  return results;
}

module.exports = { scrapeEmails, scrapeEmailsBatch, extractEmails, fetchPage };
