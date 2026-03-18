#!/usr/bin/env node
/**
 * Batch Email Enrichment for Small Home Service Contractors
 *
 * Problem: 230K+ contractor leads with business names + cities but no emails.
 * Apollo won't work for small local businesses (roofers, plumbers, painters 1-10 employees).
 * Solution: DuckDuckGo search to find websites, then extract/verify emails.
 *
 * Waterfall (per lead):
 *   1. DuckDuckGo search — "{firm_name}" {city} {state} → website URL
 *   2. Domain guessing   — DNS-based domain patterns from business name
 *   3. Common Crawl      — archived emails from web archive (free)
 *   4. Cloudflare Crawl  — live headless browser crawl (if env vars set)
 *   5. SMTP patterns     — info@, contact@, first@, first.last@, flast@ → verify
 *
 * Features:
 *   - Saves progress to JSON after every lead (resume on interrupt)
 *   - DuckDuckGo rate limiting (8-12s between searches)
 *   - Domain-level caching (don't re-crawl same firm twice)
 *   - Live progress output with status line
 *   - Proper CSV parsing (handles quoted fields with commas)
 *
 * Usage:
 *   node scripts/find-contractor-emails.js output/dbpr-florida-all.csv --limit 500 --concurrency 5
 *
 * Options:
 *   --limit <n>        Max leads to process (default: all)
 *   --concurrency <n>  Parallel lead processing (default: 3)
 *   --no-commoncrawl   Disable Common Crawl archive search
 *   --no-cloudflare    Disable Cloudflare headless crawl
 *   --no-smtp          Disable SMTP pattern verification
 *   --no-ddg           Disable DuckDuckGo search (domain guessing only)
 *   --resume           Force resume from progress file (auto-detected)
 *   --fresh            Ignore progress file and start fresh
 */

const fs = require('fs');
const path = require('path');
const { searchQuery } = require('../lib/duckduckgo-scraper');
const { findByDomainGuessing, generateDomainGuesses } = require('../lib/website-finder');
const commoncrawl = require('../lib/commoncrawl-client');
const cfCrawler = require('../lib/cloudflare-crawler');
const EmailVerifier = require('../lib/email-verifier');
const { log } = require('../lib/logger');

// ─── CLI Args ─────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

const INPUT_FILE = args.find(a => !a.startsWith('--'));
const LIMIT = parseInt(getArg('limit') || '0', 10) || 0;
const CONCURRENCY = parseInt(getArg('concurrency') || '3', 10) || 3;
const NO_DDG = hasFlag('no-ddg');
const NO_COMMONCRAWL = hasFlag('no-commoncrawl');
const NO_CLOUDFLARE = hasFlag('no-cloudflare');
const NO_SMTP = hasFlag('no-smtp');
const FORCE_FRESH = hasFlag('fresh');

if (!INPUT_FILE) {
  console.log('Usage: node scripts/find-contractor-emails.js <csv_file> [options]');
  console.log('');
  console.log('Options:');
  console.log('  --limit <n>        Max leads to process (default: all)');
  console.log('  --concurrency <n>  Parallel workers (default: 3)');
  console.log('  --no-ddg           Disable DuckDuckGo search (domain guessing only)');
  console.log('  --no-commoncrawl   Disable Common Crawl archive search');
  console.log('  --no-cloudflare    Disable Cloudflare headless crawl');
  console.log('  --no-smtp          Disable SMTP pattern verification');
  console.log('  --fresh            Start fresh, ignoring any progress file');
  console.log('');
  console.log('Example:');
  console.log('  node scripts/find-contractor-emails.js output/dbpr-florida-all.csv --limit 500 --concurrency 5');
  process.exit(1);
}

// ─── Resolve paths ──────────────────────────────────────────────────────────

const PROJECT_ROOT = path.join(__dirname, '..');
const inputPath = path.isAbsolute(INPUT_FILE)
  ? INPUT_FILE
  : path.resolve(process.cwd(), INPUT_FILE);

if (!fs.existsSync(inputPath)) {
  console.error(`File not found: ${inputPath}`);
  process.exit(1);
}

const inputBasename = path.basename(inputPath, path.extname(inputPath));
const outputDir = path.join(PROJECT_ROOT, 'output');
const outputPath = path.join(outputDir, `enriched-${inputBasename}.csv`);
const progressPath = path.join(outputDir, `.progress-${inputBasename}.json`);

// Ensure output dir exists
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

// ─── CSV Parser (handles quoted fields with commas) ──────────────────────────

/**
 * Parse a CSV file into an array of objects.
 * Handles: quoted fields with commas, escaped quotes (""), newlines in quotes.
 */
function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = [];
  let current = '';
  let inQuotes = false;

  // Split into logical lines (respecting quoted newlines)
  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      current += ch;
    } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (current.trim()) lines.push(current);
      current = '';
      // Skip \r\n
      if (ch === '\r' && content[i + 1] === '\n') i++;
    } else {
      current += ch;
    }
  }
  if (current.trim()) lines.push(current);

  if (lines.length === 0) return [];

  // Parse header
  const headers = parseCSVLine(lines[0]).map(h => h.toLowerCase().trim());

  // Parse rows
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = (values[j] || '').trim();
    }
    rows.push(normalizeColumns(row));
  }

  return rows;
}

/**
 * Parse a single CSV line into field values.
 * Handles quoted fields with commas and escaped quotes.
 */
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          // Escaped quote
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);

  return fields;
}

/**
 * Normalize CSV column names to our standard format.
 */
function normalizeColumns(row) {
  return {
    first_name: row['first_name'] || row['first name'] || row['firstname'] || row['first'] || '',
    last_name: row['last_name'] || row['last name'] || row['lastname'] || row['last'] || '',
    firm_name: row['firm_name'] || row['company'] || row['company_name'] || row['company name'] ||
               row['firm'] || row['organization'] || '',
    email: row['email'] || row['email_address'] || '',
    phone: row['phone'] || row['phone_number'] || '',
    city: row['city'] || row['location_city'] || '',
    state: row['state'] || row['location_state'] || row['province'] || '',
    trade_category: row['trade_category'] || row['trade'] || row['category'] || row['industry'] || '',
    license_type: row['license_type'] || row['license'] || '',
    bar_number: row['bar_number'] || row['license_number'] || '',
    source: row['source'] || '',
    website: row['website'] || row['url'] || row['domain'] || row['company_url'] || '',
    email_source: row['email_source'] || '',
    website_source: row['website_source'] || '',
  };
}

// ─── CSV Writer ──────────────────────────────────────────────────────────────

const OUTPUT_HEADERS = [
  'first_name', 'last_name', 'firm_name', 'email', 'phone', 'city', 'state',
  'trade_category', 'license_type', 'bar_number', 'source', 'website',
  'email_source', 'website_source',
];

function escapeCSVField(val) {
  if (!val) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(filePath, records) {
  const headerLine = OUTPUT_HEADERS.join(',');
  const lines = [headerLine];
  for (const rec of records) {
    const row = OUTPUT_HEADERS.map(h => escapeCSVField(rec[h] || ''));
    lines.push(row.join(','));
  }
  fs.writeFileSync(filePath, lines.join('\n') + '\n', 'utf-8');
}

// ─── Progress save/resume ───────────────────────────────────────────────────

/**
 * Progress state:
 *   - processedKeys: Set of "firmName|city|state" keys already processed
 *   - enriched: Map of key -> { email, website, email_source, website_source }
 *   - stats: running stats
 */

function makeKey(lead) {
  const firm = (lead.firm_name || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const city = (lead.city || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const state = (lead.state || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  return `${firm}|${city}|${state}`;
}

function saveProgress(processedKeys, enrichedMap, stats) {
  const data = {
    timestamp: new Date().toISOString(),
    processedKeys: [...processedKeys],
    enriched: Object.fromEntries(enrichedMap),
    stats,
  };
  fs.writeFileSync(progressPath, JSON.stringify(data), 'utf-8');
}

function loadProgress() {
  if (FORCE_FRESH) return null;
  if (!fs.existsSync(progressPath)) return null;

  try {
    const data = JSON.parse(fs.readFileSync(progressPath, 'utf-8'));
    log.info(`Resuming from progress file (${data.processedKeys.length} leads already processed)`);
    return {
      processedKeys: new Set(data.processedKeys),
      enrichedMap: new Map(Object.entries(data.enriched || {})),
      stats: data.stats || {},
    };
  } catch (err) {
    log.warn(`Could not read progress file: ${err.message} — starting fresh`);
    return null;
  }
}

// ─── Domain extraction ──────────────────────────────────────────────────────

function extractDomain(website) {
  if (!website) return '';
  try {
    const url = website.startsWith('http') ? website : 'https://' + website;
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

// ─── DuckDuckGo website discovery ───────────────────────────────────────────

// Domains that are directories, not actual business websites
const DIRECTORY_DOMAINS = new Set([
  'yelp.com', 'bbb.org', 'yellowpages.com', 'manta.com', 'angi.com',
  'thumbtack.com', 'homeadvisor.com', 'houzz.com', 'porch.com',
  'facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com',
  'youtube.com', 'pinterest.com', 'nextdoor.com', 'mapquest.com',
  'google.com', 'bing.com', 'yahoo.com', 'superpages.com',
  'chamberofcommerce.com', 'duckduckgo.com', 'wikipedia.org',
  'reddit.com', 'craigslist.org', 'indeed.com', 'glassdoor.com',
  'trustpilot.com', 'expertise.com', 'bark.com', 'angieslist.com',
  'buildzoom.com', 'guildquality.com',
]);

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Use DuckDuckGo to find a contractor's website.
 * Searches for "{firm_name}" {city} {state} and returns the first non-directory URL.
 */
async function findWebsiteDDG(firmName, city, state) {
  if (!firmName) return '';

  const query = `"${firmName}" ${city || ''} ${state || ''}`.trim();
  const results = await searchQuery(query);

  if (!results || results.length === 0) return '';

  // Return first result whose domain is not a directory
  for (const result of results) {
    const domain = (result.domain || '').toLowerCase();
    if (!domain) continue;
    if (DIRECTORY_DOMAINS.has(domain)) continue;

    // Skip if it's clearly a listicle/ranking page
    const title = (result.title || '').toLowerCase();
    if (/\b(best|top)\s+\d+\b/.test(title) || /\bthe best\b/.test(title)) continue;

    return result.url;
  }

  return '';
}

// ─── Email scoring ──────────────────────────────────────────────────────────

const GENERIC_PREFIXES = new Set([
  'info', 'office', 'admin', 'contact', 'support', 'hello',
  'enquiries', 'reception', 'mail', 'sales', 'billing', 'service',
]);

function pickBestEmail(emails, targetDomain, firstName, lastName) {
  if (!emails || emails.length === 0) return '';

  const first = (firstName || '').toLowerCase().replace(/[^a-z]/g, '');
  const last = (lastName || '').toLowerCase().replace(/[^a-z]/g, '');

  const scored = emails.map(email => {
    const local = email.split('@')[0].toLowerCase();
    const domain = email.split('@')[1]?.toLowerCase() || '';
    let score = 0;

    // On target domain
    if (domain === targetDomain) score += 10;

    // Generic prefix penalty
    if (GENERIC_PREFIXES.has(local)) score -= 5;

    // Name matching
    if (first && last) {
      if (local === `${first}.${last}` || local === `${first}_${last}`) score += 25;
      else if (local === `${first}${last}`) score += 20;
      else if (local === `${first[0]}${last}`) score += 18;
      else if (local === `${first}${last[0]}`) score += 15;
      else if (local.includes(first) && local.includes(last)) score += 20;
      else if (local.includes(last)) score += 10;
      else if (local.includes(first)) score += 5;
    }

    // Personal pattern bonus
    if (/^[a-z]+[._][a-z]+$/.test(local)) score += 3;

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0]?.email || '';
}

// ─── Process a single lead ──────────────────────────────────────────────────

const verifier = new EmailVerifier({ timeout: 8000, retries: 1 });

// Domain-level caches
const domainEmailCache = new Map();
const firmWebsiteCache = new Map();

// DuckDuckGo rate limiter — serialize all DDG requests with delays
let ddgLastRequestTime = 0;
const DDG_MIN_DELAY = 8000;
const DDG_MAX_DELAY = 12000;

async function ddgRateLimit() {
  const now = Date.now();
  const elapsed = now - ddgLastRequestTime;
  const delay = DDG_MIN_DELAY + Math.random() * (DDG_MAX_DELAY - DDG_MIN_DELAY);
  if (elapsed < delay) {
    await sleep(delay - elapsed);
  }
  ddgLastRequestTime = Date.now();
}

// Mutex for DuckDuckGo (only one search at a time to respect rate limits)
let ddgMutex = Promise.resolve();

function withDDGMutex(fn) {
  const prev = ddgMutex;
  let resolve;
  ddgMutex = new Promise(r => { resolve = r; });
  return prev.then(fn).finally(resolve);
}

async function processLead(lead, index, total, enrichedMap, processedKeys, stats) {
  const key = makeKey(lead);
  const firmDisplay = lead.firm_name || '(unknown)';

  // Already processed in a previous run
  if (processedKeys.has(key)) {
    const cached = enrichedMap.get(key);
    if (cached) {
      if (cached.website && !lead.website) lead.website = cached.website;
      if (cached.website_source && !lead.website_source) lead.website_source = cached.website_source;
      if (cached.email && !lead.email) lead.email = cached.email;
      if (cached.email_source && !lead.email_source) lead.email_source = cached.email_source;
    }
    return;
  }

  // ── Step 1: Find website ────────────────────────────────────────────────

  const searchableName = lead.firm_name || `${lead.first_name || ''} ${lead.last_name || ''} ${lead.trade_category || lead.license_type || 'contractor'}`.trim();
  if (!lead.website && searchableName) {
    const firmKey = searchableName.toLowerCase().replace(/[^a-z0-9]/g, '');

    if (firmWebsiteCache.has(firmKey)) {
      const cached = firmWebsiteCache.get(firmKey);
      if (cached) {
        lead.website = cached;
        lead.website_source = 'cache';
        stats.websiteFromCache++;
      }
    } else {
      // 1a. DuckDuckGo search (primary)
      if (!NO_DDG) {
        try {
          const website = await withDDGMutex(async () => {
            await ddgRateLimit();
            return findWebsiteDDG(searchableName, lead.city, lead.state);
          });
          if (website) {
            lead.website = website;
            lead.website_source = 'duckduckgo';
            stats.websiteByDDG++;
            firmWebsiteCache.set(firmKey, website);
          }
        } catch (err) {
          log.warn(`  DDG error for "${searchableName}": ${err.message}`);
          stats.errors++;
        }
      }

      // 1b. Domain guessing fallback (fast DNS)
      if (!lead.website) {
        try {
          const website = await findByDomainGuessing(searchableName, lead.city);
          if (website) {
            lead.website = website;
            lead.website_source = 'domain-guess';
            stats.websiteByDNS++;
            firmWebsiteCache.set(firmKey, website);
          }
        } catch (err) {
          log.warn(`  DNS guess error: ${err.message}`);
        }
      }

      // Cache miss (no website found)
      if (!lead.website) {
        firmWebsiteCache.set(firmKey, '');
      }
    }
  } else if (lead.website) {
    stats.websiteAlreadyHad++;
  }

  // If no website, we can't find emails
  const domain = extractDomain(lead.website);
  if (!domain) {
    const status = `[${index + 1}/${total}] ${firmDisplay} -- no website found`;
    process.stdout.write(`\r\x1b[K${status}`);
    markProcessed(key, lead, enrichedMap, processedKeys, stats);
    return;
  }

  // ── Step 2: Check domain email cache ─────────────────────────────────────

  if (domainEmailCache.has(domain)) {
    const cachedEmails = domainEmailCache.get(domain);
    if (cachedEmails && cachedEmails.length > 0) {
      const best = pickBestEmail(cachedEmails, domain, lead.first_name, lead.last_name);
      if (best) {
        lead.email = best;
        lead.email_source = 'cache';
        stats.emailsFound++;
        const status = `[${index + 1}/${total}] ${firmDisplay} -> ${domain} -> ${best} (cached)`;
        process.stdout.write(`\r\x1b[K${status}`);
        markProcessed(key, lead, enrichedMap, processedKeys, stats);
        return;
      }
    }
    // Domain already crawled, no emails — try SMTP only
    if (!NO_SMTP) {
      await trySmtpPatterns(lead, domain, stats);
    }
    const email = lead.email || '--';
    const status = `[${index + 1}/${total}] ${firmDisplay} -> ${domain} -> ${email}`;
    process.stdout.write(`\r\x1b[K${status}`);
    markProcessed(key, lead, enrichedMap, processedKeys, stats);
    return;
  }

  // ── Step 3: Common Crawl ─────────────────────────────────────────────────

  let allEmails = [];

  if (!NO_COMMONCRAWL) {
    try {
      const ccEmails = await commoncrawl.findEmails(domain);
      if (ccEmails.length > 0) {
        allEmails.push(...ccEmails);
        stats.emailByCommonCrawl += ccEmails.length;
      }
    } catch (err) {
      log.warn(`  CC error for ${domain}: ${err.message}`);
    }
  }

  // ── Step 4: Cloudflare Crawl (if CC found nothing) ────────────────────────

  if (allEmails.length === 0 && !NO_CLOUDFLARE && cfCrawler.isEnabled()) {
    try {
      const cfEmails = await cfCrawler.findEmails(domain, {
        firstName: lead.first_name,
        lastName: lead.last_name,
        maxPages: 8,
      });
      if (cfEmails.length > 0) {
        allEmails.push(...cfEmails);
        stats.emailByCloudflare += cfEmails.length;
      }
    } catch (err) {
      log.warn(`  CF crawl error for ${domain}: ${err.message}`);
    }
  }

  // Cache domain emails
  const uniqueEmails = [...new Set(allEmails.map(e => e.toLowerCase()))];
  domainEmailCache.set(domain, uniqueEmails);

  // Pick best from crawl results
  if (uniqueEmails.length > 0) {
    const best = pickBestEmail(uniqueEmails, domain, lead.first_name, lead.last_name);
    if (best) {
      lead.email = best;
      lead.email_source = 'crawl';
      stats.emailsFound++;
      const status = `[${index + 1}/${total}] ${firmDisplay} -> ${domain} -> ${best}`;
      process.stdout.write(`\r\x1b[K${status}`);
      markProcessed(key, lead, enrichedMap, processedKeys, stats);
      return;
    }
  }

  // ── Step 5: SMTP pattern verification ─────────────────────────────────────

  if (!NO_SMTP) {
    await trySmtpPatterns(lead, domain, stats);
  }

  // Also try generic role addresses via SMTP
  if (!lead.email && !NO_SMTP) {
    await tryGenericSmtp(lead, domain, stats);
  }

  const emailResult = lead.email || '--';
  const marker = lead.email ? '\x1b[32m\u2713\x1b[0m' : '\x1b[31m\u2717\x1b[0m';
  const status = `[${index + 1}/${total}] ${firmDisplay} -> ${domain} -> ${emailResult} ${marker}`;
  process.stdout.write(`\r\x1b[K${status}\n`);

  markProcessed(key, lead, enrichedMap, processedKeys, stats);
}

function markProcessed(key, lead, enrichedMap, processedKeys, stats) {
  processedKeys.add(key);
  enrichedMap.set(key, {
    email: lead.email || '',
    website: lead.website || '',
    email_source: lead.email_source || '',
    website_source: lead.website_source || '',
  });

  // Save progress every 10 leads
  if (processedKeys.size % 10 === 0) {
    saveProgress(processedKeys, enrichedMap, stats);
  }
}

/**
 * Try SMTP email patterns using first_name + last_name.
 */
async function trySmtpPatterns(lead, domain, stats) {
  if (!lead.first_name || !lead.last_name) return;

  try {
    const bestEmail = await verifier.findBestEmail(lead.first_name, lead.last_name, domain);
    if (bestEmail) {
      lead.email = bestEmail;
      lead.email_source = 'smtp-pattern';
      stats.emailsFound++;
      stats.emailBySmtp++;
    }
  } catch (err) {
    // SMTP errors are common, don't log each one
    stats.errors++;
  }
}

/**
 * Try generic role-based emails: info@, contact@, office@ via SMTP.
 * Only for contractors — these are often the only email a small business has.
 */
async function tryGenericSmtp(lead, domain, stats) {
  const genericPrefixes = ['info', 'contact', 'office', 'service'];

  try {
    const mxHost = await verifier.getMxHost(domain);
    if (!mxHost) return;

    // Check catch-all first
    const catchAll = await verifier.isCatchAll(domain, mxHost);
    if (catchAll) {
      // Can't distinguish on catch-all — use info@ as default
      lead.email = `info@${domain}`;
      lead.email_source = 'smtp-catchall';
      stats.emailsFound++;
      stats.emailBySmtp++;
      return;
    }

    for (const prefix of genericPrefixes) {
      const email = `${prefix}@${domain}`;
      const result = await verifier.smtpCheck(email, mxHost);
      if (result.valid) {
        lead.email = email;
        lead.email_source = 'smtp-generic';
        stats.emailsFound++;
        stats.emailBySmtp++;
        return;
      }
      await sleep(300);
    }
  } catch {
    // SMTP errors are expected
  }
}

// ─── Concurrency helper ──────────────────────────────────────────────────────

async function processWithConcurrency(items, concurrency, fn) {
  let index = 0;
  const total = items.length;

  async function worker() {
    while (index < total) {
      const i = index++;
      await fn(items[i], i, total);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(concurrency, items.length); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('');
  console.log('========================================');
  console.log('  Contractor Email Enrichment');
  console.log('========================================');
  console.log(`  Input:       ${inputPath}`);
  console.log(`  Output:      ${outputPath}`);
  console.log(`  Progress:    ${progressPath}`);
  console.log(`  Limit:       ${LIMIT || 'all'}`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Sources:     ${[
    !NO_DDG ? 'duckduckgo' : null,
    'domain-guess',
    !NO_COMMONCRAWL ? 'common-crawl' : null,
    !NO_CLOUDFLARE && cfCrawler.isEnabled() ? 'cloudflare-crawl' : null,
    !NO_SMTP ? 'smtp-patterns' : null,
  ].filter(Boolean).join(', ')}`);
  console.log('========================================');
  console.log('');

  // ── Read CSV ─────────────────────────────────────────────────────────────

  log.info(`Reading CSV: ${inputPath}`);
  const allLeads = parseCSV(inputPath);
  log.info(`Loaded ${allLeads.length} leads`);

  // ── Load progress (if resuming) ──────────────────────────────────────────

  const progress = loadProgress();
  const processedKeys = progress ? progress.processedKeys : new Set();
  const enrichedMap = progress ? progress.enrichedMap : new Map();
  const stats = progress?.stats || {
    totalLeads: 0,
    needsEnrichment: 0,
    alreadyHasEmail: 0,
    noFirmName: 0,
    websiteByDDG: 0,
    websiteByDNS: 0,
    websiteAlreadyHad: 0,
    websiteFromCache: 0,
    emailsFound: 0,
    emailByCommonCrawl: 0,
    emailByCloudflare: 0,
    emailBySmtp: 0,
    errors: 0,
    startTime: Date.now(),
  };

  if (!progress) {
    stats.startTime = Date.now();
  }

  // ── Filter leads ─────────────────────────────────────────────────────────

  const toEnrich = [];
  let skippedAlreadyHasEmail = 0;
  let skippedNoFirmName = 0;

  for (const lead of allLeads) {
    // Apply enriched data from progress to leads that already have data
    const key = makeKey(lead);
    const cached = enrichedMap.get(key);
    if (cached) {
      if (cached.website && !lead.website) lead.website = cached.website;
      if (cached.website_source && !lead.website_source) lead.website_source = cached.website_source;
      if (cached.email && !lead.email) lead.email = cached.email;
      if (cached.email_source && !lead.email_source) lead.email_source = cached.email_source;
    }

    if (lead.email) {
      skippedAlreadyHasEmail++;
      continue;
    }
    const hasSearchable = lead.firm_name || lead.website || (lead.first_name && lead.last_name);
    if (!hasSearchable) {
      skippedNoFirmName++;
      continue;
    }

    // Skip already-processed leads with no results
    if (processedKeys.has(key)) continue;

    toEnrich.push(lead);
  }

  stats.totalLeads = allLeads.length;
  stats.alreadyHasEmail = skippedAlreadyHasEmail;
  stats.noFirmName = skippedNoFirmName;

  // Apply limit
  const batch = LIMIT > 0 ? toEnrich.slice(0, LIMIT) : toEnrich;
  stats.needsEnrichment = batch.length;

  log.info(`${skippedAlreadyHasEmail} already have email, ${skippedNoFirmName} have no firm/website`);
  log.info(`${processedKeys.size} already processed (from progress file)`);
  log.info(`Processing ${batch.length} leads for email enrichment`);

  if (batch.length === 0) {
    log.warn('No leads to enrich — all processed, have emails, or lack firm names');
    // Still write output with any data from progress
    writeCSV(outputPath, allLeads);
    log.info(`Output written to ${outputPath}`);
    return;
  }

  // ── Handle interrupts ───────────────────────────────────────────────────

  let interrupted = false;

  function handleInterrupt() {
    if (interrupted) {
      console.log('\nForce exit.');
      process.exit(1);
    }
    interrupted = true;
    console.log('\n\nInterrupted! Saving progress...');
    saveProgress(processedKeys, enrichedMap, stats);
    writeCSV(outputPath, allLeads);
    console.log(`Progress saved to ${progressPath}`);
    console.log(`Partial output saved to ${outputPath}`);
    console.log('Run the same command to resume.\n');
    process.exit(0);
  }

  process.on('SIGINT', handleInterrupt);
  process.on('SIGTERM', handleInterrupt);

  // ── Process leads ────────────────────────────────────────────────────────

  await processWithConcurrency(batch, CONCURRENCY, (lead, i, total) =>
    processLead(lead, i, total, enrichedMap, processedKeys, stats)
  );

  // ── Write output ─────────────────────────────────────────────────────────

  process.stdout.write('\n');
  log.info(`Writing enriched CSV: ${outputPath}`);
  writeCSV(outputPath, allLeads);

  // Save final progress
  saveProgress(processedKeys, enrichedMap, stats);

  // ── Print summary ────────────────────────────────────────────────────────

  const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);

  console.log('');
  console.log('========================================');
  console.log('  ENRICHMENT SUMMARY');
  console.log('========================================');
  console.log(`  Total leads:          ${stats.totalLeads}`);
  console.log(`  Already had email:    ${stats.alreadyHasEmail}`);
  console.log(`  No firm/website:      ${stats.noFirmName}`);
  console.log(`  Processed:            ${stats.needsEnrichment + processedKeys.size}`);
  console.log('  ------------------------------------');
  console.log(`  Websites found:`);
  console.log(`    DuckDuckGo:         ${stats.websiteByDDG}`);
  console.log(`    DNS guess:          ${stats.websiteByDNS}`);
  console.log(`    Already had:        ${stats.websiteAlreadyHad}`);
  console.log('  ------------------------------------');
  console.log(`  Emails found:         ${stats.emailsFound}`);
  console.log(`    Common Crawl:       ${stats.emailByCommonCrawl}`);
  console.log(`    Cloudflare Crawl:   ${stats.emailByCloudflare}`);
  console.log(`    SMTP patterns:      ${stats.emailBySmtp}`);
  console.log('  ------------------------------------');
  console.log(`  Errors:               ${stats.errors}`);
  console.log(`  Time:                 ${elapsed}s`);
  console.log(`  Output:               ${outputPath}`);
  console.log('========================================');
  console.log('');

  // Clean up progress file on successful completion
  if (processedKeys.size >= toEnrich.length) {
    try {
      fs.unlinkSync(progressPath);
      log.info('Progress file cleaned up (all leads processed)');
    } catch {}
  }
}

main().catch(err => {
  console.error('\nFatal error:', err);
  // Save progress on fatal error too
  try {
    const progress = loadProgress();
    if (progress) {
      console.log(`Progress was saved to ${progressPath} — run again to resume`);
    }
  } catch {}
  process.exit(1);
});
