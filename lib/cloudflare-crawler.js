/**
 * Cloudflare Browser Rendering /crawl API client
 *
 * Replaces/supplements Puppeteer-based email-finder.js with Cloudflare's
 * headless browser crawl API — faster, more reliable, no local browser needed.
 *
 * Env vars:
 *   CLOUDFLARE_ACCOUNT_ID — Cloudflare account ID
 *   CLOUDFLARE_API_TOKEN  — API token with "Browser Rendering - Edit" permission
 *
 * If env vars are not set, all exports return empty results (safe stub mode).
 *
 * Usage:
 *   const cf = require('./lib/cloudflare-crawler');
 *   const emails = await cf.findEmails('smithlaw.com');
 *   // ['john.smith@smithlaw.com', 'jane@smithlaw.com']
 *
 *   const enriched = await cf.crawlForEmails(leads);
 *   // leads now have .email and .email_source populated
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { log } = require('./logger');

// ─── Load .env if env vars not already set ───────────────────────────────────
if (!process.env.CLOUDFLARE_ACCOUNT_ID || !process.env.CLOUDFLARE_API_TOKEN) {
  try {
    const envPath = path.join(__dirname, '..', '.env');
    if (fs.existsSync(envPath)) {
      const envContent = fs.readFileSync(envPath, 'utf8');
      for (const line of envContent.split('\n')) {
        const match = line.match(/^([A-Z_]+)=(.+)$/);
        if (match && !process.env[match[1]]) {
          process.env[match[1]] = match[2].trim();
        }
      }
    }
  } catch { /* ignore */ }
}

// ─── Configuration ────────────────────────────────────────────────────────────

const CF_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID || '';
const CF_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN || '';
const CF_BASE_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/browser-rendering/crawl`;

const ENABLED = !!(CF_ACCOUNT_ID && CF_API_TOKEN);

// Polling config
const POLL_INTERVAL_MS = 2000;   // 2 seconds between status checks
const POLL_MAX_WAIT_MS = 120000; // 2 minutes max wait per crawl job
const RATE_LIMIT_MS = 1000;      // 1 second between crawl submissions

// ─── Email extraction config ──────────────────────────────────────────────────

const EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

// Domains to ignore in extracted emails (framework/CDN/social)
const IGNORE_DOMAINS = new Set([
  'example.com', 'test.com', 'email.com', 'yoursite.com', 'yourdomain.com',
  'sentry.io', 'wixpress.com', 'w3.org', 'schema.org', 'googleapis.com',
  'google.com', 'gstatic.com', 'facebook.com', 'twitter.com', 'instagram.com',
  'wordpress.com', 'wordpress.org', 'wp.com', 'gravatar.com', 'cloudflare.com',
  'jquery.com', 'bootstrapcdn.com', 'fontawesome.com', 'googletagmanager.com',
  'creativecommons.org', 'sentry-next.wixpress.com',
]);

// Free email providers — not business emails
const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'ymail.com', 'rocketmail.com',
]);

// Generic/role-based prefixes (scored lower than personal emails)
const GENERIC_PREFIXES = new Set([
  'info', 'office', 'admin', 'contact', 'support', 'hello', 'enquiries',
  'reception', 'mail', 'sales', 'billing', 'accounts', 'help', 'team',
  'general', 'enquiry', 'inquiry',
]);

// Automated/system prefixes (never return these)
const SKIP_PREFIXES = new Set([
  'noreply', 'no-reply', 'donotreply', 'mailer-daemon', 'postmaster',
  'webmaster', 'root', 'abuse', 'hostmaster', 'daemon',
]);

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

/**
 * Make an HTTPS request and return parsed JSON.
 */
function cfRequest(method, urlPath, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlPath);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method,
      headers: {
        'Authorization': `Bearer ${CF_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    };

    if (body) {
      const payload = JSON.stringify(body);
      options.headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 400) {
            const errMsg = json.errors?.[0]?.message || `HTTP ${res.statusCode}`;
            return reject(new Error(`Cloudflare API error: ${errMsg} (${res.statusCode})`));
          }
          resolve(json);
        } catch (e) {
          reject(new Error(`Invalid JSON from Cloudflare: ${data.slice(0, 200)}`));
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });

    if (body) {
      req.write(JSON.stringify(body));
    }
    req.end();
  });
}

// ─── Cloudflare Crawl API ─────────────────────────────────────────────────────

/**
 * Start a crawl job via POST.
 * Returns the job ID string.
 */
async function startCrawl(url, options = {}) {
  const payload = {
    url,
    limit: options.limit || 10,
    depth: options.depth || 2,
    formats: ['markdown'],
  };

  const response = await cfRequest('POST', CF_BASE_URL, payload);

  if (!response.success || !response.result) {
    throw new Error(`Failed to start crawl: ${JSON.stringify(response)}`);
  }

  return response.result; // Job ID string
}

/**
 * Poll a crawl job until it completes or times out.
 * Returns the full result object with records.
 */
async function pollCrawl(jobId) {
  const startTime = Date.now();
  const jobUrl = `${CF_BASE_URL}/${jobId}`;

  while (Date.now() - startTime < POLL_MAX_WAIT_MS) {
    const response = await cfRequest('GET', jobUrl + '?limit=1');

    if (!response.success) {
      throw new Error(`Poll failed: ${JSON.stringify(response)}`);
    }

    const status = response.result?.status;

    if (status === 'completed') {
      // Fetch full results (may need pagination)
      return fetchAllRecords(jobId);
    }

    if (status === 'running') {
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    // Terminal error states
    throw new Error(`Crawl job ${jobId} ended with status: ${status}`);
  }

  throw new Error(`Crawl job ${jobId} timed out after ${POLL_MAX_WAIT_MS / 1000}s`);
}

/**
 * Fetch all records from a completed crawl job, handling pagination.
 */
async function fetchAllRecords(jobId) {
  const allRecords = [];
  let cursor = undefined;
  const jobUrl = `${CF_BASE_URL}/${jobId}`;

  do {
    const params = cursor ? `?cursor=${cursor}` : '';
    const response = await cfRequest('GET', jobUrl + params);

    if (!response.success || !response.result) {
      break;
    }

    const records = response.result.records || [];
    allRecords.push(...records);
    cursor = response.result.cursor || null;
  } while (cursor);

  return allRecords;
}

/**
 * Run a complete crawl: start → poll → return records.
 */
async function crawl(url, options = {}) {
  const jobId = await startCrawl(url, options);
  log.info(`[CF-Crawl] Started job ${jobId} for ${url}`);

  const records = await pollCrawl(jobId);
  log.info(`[CF-Crawl] Job ${jobId} completed: ${records.length} page(s)`);

  return records;
}

// ─── Email extraction ─────────────────────────────────────────────────────────

/**
 * Clean an email string (strip mailto:, query params, decode, lowercase).
 */
function cleanEmail(email) {
  if (!email) return '';
  email = email.replace(/^mailto:/i, '');
  email = email.split('?')[0];
  try { email = decodeURIComponent(email); } catch {}
  return email.replace(/^\s+|\s+$/g, '').replace(/[\x00-\x1f]/g, '').toLowerCase();
}

/**
 * Check if an email is valid for our purposes.
 */
function isValidEmail(email) {
  if (!email || email.length < 5 || email.length > 100) return false;

  const parts = email.split('@');
  if (parts.length !== 2) return false;

  const [local, domain] = parts;
  if (!local || !domain) return false;
  if (IGNORE_DOMAINS.has(domain)) return false;
  if (SKIP_PREFIXES.has(local)) return false;

  // Skip file extensions masquerading as emails
  if (/\.(jpg|jpeg|png|gif|svg|pdf|css|js|ico|woff|ttf)$/i.test(email)) return false;

  // Basic domain validation
  if (!domain.includes('.') || domain.length < 4) return false;

  return true;
}

/**
 * Extract valid business emails from text/markdown content.
 *
 * @param {string} content - Page content (HTML or markdown)
 * @param {string} domain - Target domain to prefer (e.g., "smithlaw.com")
 * @returns {string[]} Array of cleaned, valid emails
 */
function extractEmails(content, domain) {
  if (!content) return [];

  const matches = content.match(EMAIL_REGEX) || [];
  const emails = new Set();

  for (const raw of matches) {
    const email = cleanEmail(raw);
    if (!isValidEmail(email)) continue;

    const emailDomain = email.split('@')[1];
    if (!emailDomain) continue;

    // Skip free providers
    if (FREE_PROVIDERS.has(emailDomain)) continue;

    emails.add(email);
  }

  return [...emails];
}

/**
 * Score an email for quality. Higher = better.
 *
 * Scoring factors:
 *  - On target domain: +10
 *  - Personal (first.last@): +8
 *  - Contains name parts: +5 to +20 (if first/last provided)
 *  - Generic prefix (info@, office@): -5
 *
 * @param {string} email
 * @param {string} targetDomain - Expected firm domain
 * @param {string} [firstName] - Lead first name for matching
 * @param {string} [lastName] - Lead last name for matching
 * @returns {number}
 */
function scoreEmail(email, targetDomain, firstName, lastName) {
  const local = email.split('@')[0];
  const domain = email.split('@')[1];
  let score = 0;

  // On target domain
  if (domain === targetDomain || domain === 'www.' + targetDomain) {
    score += 10;
  }

  // Generic prefix penalty
  if (GENERIC_PREFIXES.has(local)) {
    score -= 5;
  }

  // Personal email pattern (contains a dot or underscore — likely first.last)
  if (/^[a-z]+[._][a-z]+$/.test(local)) {
    score += 8;
  }

  // Name matching (if provided)
  const first = (firstName || '').toLowerCase().trim();
  const last = (lastName || '').toLowerCase().trim();

  if (first && last) {
    // Exact patterns: first.last@, first_last@, flast@, firstl@
    if (local === `${first}.${last}` || local === `${first}_${last}`) {
      score += 20;
    } else if (local === first[0] + last) {
      score += 15;
    } else if (local === first + last[0]) {
      score += 12;
    } else if (local.includes(first) && local.includes(last)) {
      score += 18;
    } else if (local.includes(last)) {
      score += 8;
    } else if (local.includes(first)) {
      score += 4;
    }
  } else if (last) {
    if (local.includes(last)) score += 8;
  }

  return score;
}

/**
 * Pick the best email from a list, considering target domain and optional name.
 *
 * @param {string[]} emails
 * @param {string} targetDomain
 * @param {string} [firstName]
 * @param {string} [lastName]
 * @returns {string} Best email or ''
 */
function pickBestEmail(emails, targetDomain, firstName, lastName) {
  if (!emails || emails.length === 0) return '';

  const scored = emails.map(email => ({
    email,
    score: scoreEmail(email, targetDomain, firstName, lastName),
  }));

  scored.sort((a, b) => b.score - a.score);

  return scored[0].email;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Find emails on a website domain using Cloudflare Browser Rendering /crawl.
 *
 * 1. POST to /crawl with the domain homepage
 * 2. Crawl contact/about/team pages (depth 2, max 10 pages)
 * 3. Extract emails from returned markdown content
 * 4. Filter out generic/framework emails
 * 5. Score and rank: personal > role-based > generic
 *
 * @param {string} domain - Domain to crawl (e.g., "smithlaw.com")
 * @param {object} [options]
 * @param {string} [options.firstName] - Person first name for scoring
 * @param {string} [options.lastName] - Person last name for scoring
 * @param {number} [options.maxPages=10] - Max pages to crawl
 * @returns {Promise<string[]>} Array of found emails, best first
 */
async function findEmails(domain, options = {}) {
  if (!ENABLED) {
    log.warn('[CF-Crawl] CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN not set — returning empty');
    return [];
  }

  if (!domain) return [];

  // Normalize domain
  domain = domain.replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '').toLowerCase();

  const targetUrl = `https://www.${domain}`;
  const maxPages = options.maxPages || 10;

  try {
    const records = await crawl(targetUrl, {
      limit: maxPages,
      depth: 2,
    });

    if (!records || records.length === 0) {
      log.info(`[CF-Crawl] No pages returned for ${domain}`);
      return [];
    }

    // Extract emails from all crawled pages
    const allEmails = new Set();

    for (const record of records) {
      if (record.status !== 'completed') continue;

      // Extract from markdown content
      const content = record.markdown || '';
      const emails = extractEmails(content, domain);
      emails.forEach(e => allEmails.add(e));
    }

    if (allEmails.size === 0) {
      log.info(`[CF-Crawl] No emails found on ${domain} (${records.length} pages crawled)`);
      return [];
    }

    // Score and sort
    const emailList = [...allEmails];
    const scored = emailList.map(email => ({
      email,
      score: scoreEmail(email, domain, options.firstName, options.lastName),
    }));
    scored.sort((a, b) => b.score - a.score);

    const sorted = scored.map(s => s.email);
    log.info(`[CF-Crawl] Found ${sorted.length} email(s) on ${domain}: ${sorted.slice(0, 3).join(', ')}`);

    return sorted;
  } catch (err) {
    log.warn(`[CF-Crawl] Error crawling ${domain}: ${err.message}`);
    return [];
  }
}

/**
 * Enrich an array of leads with emails from their websites.
 * Groups by domain to avoid crawling the same site twice.
 *
 * @param {object[]} leads - Array of lead objects with .website, .first_name, .last_name
 * @param {object} [options]
 * @param {function} [options.onProgress] - Callback(current, total, domain)
 * @param {function} [options.isCancelled] - Returns true to abort
 * @returns {Promise<{emailsFound: number, domainsVisited: number, skipped: number, errors: number}>}
 */
async function crawlForEmails(leads, options = {}) {
  const onProgress = options.onProgress || (() => {});
  const isCancelled = options.isCancelled || (() => false);

  const stats = { emailsFound: 0, domainsVisited: 0, skipped: 0, errors: 0 };

  if (!ENABLED) {
    log.warn('[CF-Crawl] Disabled — env vars not set. Skipping all leads.');
    stats.skipped = leads.length;
    return stats;
  }

  // Group leads by domain
  const domainGroups = new Map();

  for (const lead of leads) {
    // Skip leads that already have email or no website
    if (lead.email || !lead.website) {
      stats.skipped++;
      continue;
    }

    let domain;
    try {
      const url = lead.website.startsWith('http') ? lead.website : 'https://' + lead.website;
      domain = new URL(url).hostname.replace(/^www\./, '').toLowerCase();
    } catch {
      stats.skipped++;
      continue;
    }

    if (!domainGroups.has(domain)) {
      domainGroups.set(domain, []);
    }
    domainGroups.get(domain).push(lead);
  }

  const totalDomains = domainGroups.size;
  let processed = 0;

  for (const [domain, domainLeads] of domainGroups) {
    if (isCancelled()) {
      log.info('[CF-Crawl] Cancelled by caller');
      break;
    }

    processed++;
    onProgress(processed, totalDomains, domain);

    try {
      const emails = await findEmails(domain);
      stats.domainsVisited++;

      if (emails.length > 0) {
        // Assign best-match email to each lead at this domain
        for (const lead of domainLeads) {
          const best = pickBestEmail(
            emails,
            domain,
            lead.first_name,
            lead.last_name
          );

          if (best) {
            lead.email = best;
            lead.email_source = 'cloudflare-crawl';
            stats.emailsFound++;
          }
        }
      }
    } catch (err) {
      log.warn(`[CF-Crawl] Failed to crawl ${domain}: ${err.message}`);
      stats.errors++;
    }

    // Rate limit: 1 crawl per second
    if (processed < totalDomains) {
      await sleep(RATE_LIMIT_MS);
    }
  }

  log.info(
    `[CF-Crawl] Batch complete: ${stats.domainsVisited} domains visited, ` +
    `${stats.emailsFound} emails found, ${stats.skipped} skipped, ${stats.errors} errors`
  );

  return stats;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check if the Cloudflare crawler is configured and available.
 */
function isEnabled() {
  return ENABLED;
}

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  findEmails,
  crawlForEmails,
  extractEmails,
  scoreEmail,
  pickBestEmail,
  isEnabled,
  // Expose internals for testing
  _crawl: crawl,
  _startCrawl: startCrawl,
  _pollCrawl: pollCrawl,
};
