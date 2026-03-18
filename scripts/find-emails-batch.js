#!/usr/bin/env node
/**
 * Batch Email Enrichment — find emails for contractor leads
 *
 * Orchestrates existing modules in a waterfall:
 *   1. Website Finder   — guess/find domain from business name (DNS + Google)
 *   2. Common Crawl     — search archived pages for emails (free, no API key)
 *   3. Cloudflare Crawl — live headless browser crawl (if CLOUDFLARE_* env vars set)
 *   4. SMTP Patterns    — generate first.last@domain patterns + verify via SMTP
 *
 * Reads a CSV, enriches leads that have a firm_name but no email, writes output.
 *
 * Usage:
 *   node scripts/find-emails-batch.js output/dbpr-florida-all.csv
 *   node scripts/find-emails-batch.js output/leads.csv --limit 500 --concurrency 3
 *
 * Options:
 *   --limit <n>        Max leads to process (default: all)
 *   --concurrency <n>  Parallel lead processing (default: 3)
 *   --skip-website     Skip website discovery (only process leads that already have one)
 *   --no-commoncrawl   Disable Common Crawl lookup
 *   --no-cloudflare    Disable Cloudflare crawl even if configured
 *   --no-smtp          Disable SMTP pattern verification
 *   --no-google        Disable Google search for website discovery (DNS guessing only)
 *
 * Output:
 *   output/enriched-{original-filename}.csv
 */

const fs = require('fs');
const path = require('path');
const { readCSV, writeCSV } = require('../lib/csv-handler');
const { findWebsite, findByDomainGuessing } = require('../lib/website-finder');
const commoncrawl = require('../lib/commoncrawl-client');
const cfCrawler = require('../lib/cloudflare-crawler');
const EmailVerifier = require('../lib/email-verifier');
const { log } = require('../lib/logger');

// ─── CLI Args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  return idx === -1 ? undefined : args[idx + 1];
}
function hasFlag(name) { return args.includes(`--${name}`); }

// Find input file: first positional arg (not a flag and not a flag's value)
const INPUT_FILE = (() => {
  const flagsWithValues = new Set(['--limit', '--concurrency']);
  for (let i = 0; i < args.length; i++) {
    if (flagsWithValues.has(args[i])) { i++; continue; } // skip flag + its value
    if (args[i].startsWith('--')) continue;               // skip boolean flags
    return args[i];
  }
  return undefined;
})();
const LIMIT = parseInt(getArg('limit') || '0', 10) || 0;
const CONCURRENCY = parseInt(getArg('concurrency') || '3', 10) || 3;
const SKIP_WEBSITE = hasFlag('skip-website');
const NO_COMMONCRAWL = hasFlag('no-commoncrawl');
const NO_CLOUDFLARE = hasFlag('no-cloudflare');
const NO_SMTP = hasFlag('no-smtp');
const NO_GOOGLE = hasFlag('no-google');

if (!INPUT_FILE) {
  console.log('Usage: node scripts/find-emails-batch.js <csv_file> [options]');
  console.log('');
  console.log('Options:');
  console.log('  --limit <n>        Max leads to process (default: all)');
  console.log('  --concurrency <n>  Parallel workers (default: 3)');
  console.log('  --skip-website     Only enrich leads that already have a website');
  console.log('  --no-commoncrawl   Disable Common Crawl archive search');
  console.log('  --no-cloudflare    Disable Cloudflare headless crawl');
  console.log('  --no-smtp          Disable SMTP pattern verification');
  console.log('  --no-google        Disable Google search for websites (DNS only)');
  console.log('');
  console.log('Example:');
  console.log('  node scripts/find-emails-batch.js output/dbpr-florida-all.csv --limit 500 --concurrency 3');
  process.exit(1);
}

// ─── Resolve file path ──────────────────────────────────────────────────────

const inputPath = path.isAbsolute(INPUT_FILE)
  ? INPUT_FILE
  : path.resolve(process.cwd(), INPUT_FILE);

if (!fs.existsSync(inputPath)) {
  console.error(`File not found: ${inputPath}`);
  process.exit(1);
}

const inputBasename = path.basename(inputPath, path.extname(inputPath));
const outputPath = path.join(path.dirname(inputPath), `enriched-${inputBasename}.csv`);

// ─── Stats ───────────────────────────────────────────────────────────────────

const stats = {
  totalLeads: 0,
  needsEnrichment: 0,
  alreadyHasEmail: 0,
  noFirmName: 0,

  // Website discovery
  websitesFound: 0,
  websiteByDnsGuess: 0,
  websiteByGoogle: 0,
  websiteAlreadyHad: 0,

  // Email discovery by method
  emailsFound: 0,
  emailByCommonCrawl: 0,
  emailByCloudflare: 0,
  emailBySmtp: 0,

  // Errors
  errors: 0,

  startTime: Date.now(),
};

// ─── Domain extraction helper ────────────────────────────────────────────────

function extractDomain(website) {
  if (!website) return '';
  try {
    const url = website.startsWith('http') ? website : 'https://' + website;
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

// ─── Process a single lead ───────────────────────────────────────────────────

const verifier = new EmailVerifier({ timeout: 8000, retries: 1 });

// Domain-level caches to avoid re-crawling the same firm
const domainEmailCache = new Map();   // domain -> string[] emails
const domainWebsiteCache = new Map(); // normalized firm name -> website URL

async function processLead(lead, index, total) {
  const name = `${lead.first_name || ''} ${lead.last_name || ''}`.trim() || lead.firm_name || '(unknown)';
  log.info(`[${index + 1}/${total}] Processing: ${name}`);

  // ── Step 1: Find website if missing ──────────────────────────────────────

  const firmOrName = lead.firm_name || `${lead.first_name || ''} ${lead.last_name || ''} ${lead.trade_category || lead.license_type || 'contractor'}`.trim();
  if (!lead.website && !SKIP_WEBSITE && firmOrName) {
    // Check cache first
    const firmKey = (firmOrName || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    if (domainWebsiteCache.has(firmKey)) {
      const cached = domainWebsiteCache.get(firmKey);
      if (cached) {
        lead.website = cached;
        lead.website_source = 'cache';
      }
    } else {
      try {
        let website = '';
        // DNS guessing first (fast)
        website = await findByDomainGuessing(firmOrName, lead.city);
        if (website) {
          stats.websiteByDnsGuess++;
        }
        // Google search fallback
        if (!website && !NO_GOOGLE) {
          const { findByGoogleSearch } = require('../lib/website-finder');
          website = await findByGoogleSearch(firmOrName, lead.city, lead.state);
          if (website) {
            stats.websiteByGoogle++;
            // Rate limit Google
            await sleep(2000 + Math.random() * 3000);
          }
        }
        domainWebsiteCache.set(firmKey, website);
        if (website) {
          lead.website = website;
          lead.website_source = 'discovered';
          stats.websitesFound++;
          log.success(`  Website found: ${website}`);
        }
      } catch (err) {
        log.warn(`  Website discovery failed: ${err.message}`);
        domainWebsiteCache.set(firmKey, '');
        stats.errors++;
      }
    }
  } else if (lead.website) {
    stats.websiteAlreadyHad++;
  }

  // If no website at this point, we can't find emails
  const domain = extractDomain(lead.website);
  if (!domain) {
    log.skip(`  No domain — skipping email enrichment`);
    return;
  }

  // ── Step 2: Check domain cache ───────────────────────────────────────────

  if (domainEmailCache.has(domain)) {
    const cachedEmails = domainEmailCache.get(domain);
    if (cachedEmails && cachedEmails.length > 0) {
      const best = pickBestEmail(cachedEmails, domain, lead.first_name, lead.last_name);
      if (best) {
        lead.email = best;
        lead.email_source = 'cache';
        stats.emailsFound++;
        log.success(`  Email (cached): ${best}`);
        return;
      }
    }
    // Domain was already crawled but no emails found — still try SMTP patterns
    if (!NO_SMTP) {
      await trySmtpPatterns(lead, domain);
    }
    return;
  }

  // ── Step 3: Common Crawl ─────────────────────────────────────────────────

  const ccEmailSet = new Set();  // Track which emails came from Common Crawl
  const cfEmailSet = new Set();  // Track which emails came from Cloudflare
  let allEmails = [];

  if (!NO_COMMONCRAWL) {
    try {
      log.info(`  Common Crawl: searching ${domain}...`);
      const ccEmails = await commoncrawl.findEmails(domain);
      if (ccEmails.length > 0) {
        ccEmails.forEach(e => ccEmailSet.add(e.toLowerCase()));
        allEmails.push(...ccEmails);
        log.success(`  Common Crawl: found ${ccEmails.length} email(s)`);
      }
    } catch (err) {
      log.warn(`  Common Crawl error: ${err.message}`);
    }
  }

  // ── Step 4: Cloudflare Crawl (only if CC found nothing) ──────────────────

  if (allEmails.length === 0 && !NO_CLOUDFLARE && cfCrawler.isEnabled()) {
    try {
      log.info(`  Cloudflare Crawl: crawling ${domain}...`);
      const cfEmails = await cfCrawler.findEmails(domain, {
        firstName: lead.first_name,
        lastName: lead.last_name,
        maxPages: 8,
      });
      if (cfEmails.length > 0) {
        cfEmails.forEach(e => cfEmailSet.add(e.toLowerCase()));
        allEmails.push(...cfEmails);
        log.success(`  Cloudflare Crawl: found ${cfEmails.length} email(s)`);
      }
    } catch (err) {
      log.warn(`  Cloudflare Crawl error: ${err.message}`);
    }
  }

  // ── Cache domain emails ──────────────────────────────────────────────────

  const uniqueEmails = [...new Set(allEmails.map(e => e.toLowerCase()))];
  domainEmailCache.set(domain, uniqueEmails);

  // ── Pick best email from crawl results ───────────────────────────────────

  if (uniqueEmails.length > 0) {
    const best = pickBestEmail(uniqueEmails, domain, lead.first_name, lead.last_name);
    if (best) {
      lead.email = best;
      // Tag source based on which method actually found it
      const bestLower = best.toLowerCase();
      if (ccEmailSet.has(bestLower)) {
        lead.email_source = 'commoncrawl';
      } else if (cfEmailSet.has(bestLower)) {
        lead.email_source = 'cloudflare-crawl';
      } else {
        lead.email_source = 'crawl';
      }
      stats.emailsFound++;
      trackEmailSource(lead.email_source);
      log.success(`  Email found: ${best} (${lead.email_source})`);
      return;
    }
  }

  // ── Step 5: SMTP Pattern Verification ────────────────────────────────────

  if (!NO_SMTP) {
    await trySmtpPatterns(lead, domain);
  }
}

/**
 * Try SMTP email pattern generation + verification.
 */
async function trySmtpPatterns(lead, domain) {
  if (!lead.first_name || !lead.last_name) {
    log.skip(`  SMTP: no name — cannot generate patterns`);
    return;
  }

  try {
    log.info(`  SMTP: trying patterns for ${lead.first_name} ${lead.last_name}@${domain}...`);
    const bestEmail = await verifier.findBestEmail(lead.first_name, lead.last_name, domain);
    if (bestEmail) {
      lead.email = bestEmail;
      lead.email_source = 'smtp-pattern';
      stats.emailsFound++;
      stats.emailBySmtp++;
      log.success(`  SMTP verified: ${bestEmail}`);
    } else {
      log.skip(`  SMTP: no pattern verified for ${domain}`);
    }
  } catch (err) {
    log.warn(`  SMTP error: ${err.message}`);
    stats.errors++;
  }
}

/**
 * Pick the best email for a lead from a list of candidates.
 * Prefers emails that match the person's name on the target domain.
 */
function pickBestEmail(emails, targetDomain, firstName, lastName) {
  if (!emails || emails.length === 0) return '';

  const first = (firstName || '').toLowerCase().replace(/[^a-z]/g, '');
  const last = (lastName || '').toLowerCase().replace(/[^a-z]/g, '');

  const GENERIC_PREFIXES = new Set([
    'info', 'office', 'admin', 'contact', 'support', 'hello',
    'enquiries', 'reception', 'mail', 'sales', 'billing',
  ]);

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

    // Personal pattern bonus (has dot or underscore)
    if (/^[a-z]+[._][a-z]+$/.test(local)) score += 3;

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0]?.email || '';
}

function trackEmailSource(source) {
  if (source === 'commoncrawl') stats.emailByCommonCrawl++;
  else if (source === 'cloudflare-crawl') stats.emailByCloudflare++;
  else if (source === 'smtp-pattern') stats.emailBySmtp++;
}

// ─── Concurrency helper ──────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Process items with limited concurrency.
 */
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
  console.log('  Batch Email Enrichment');
  console.log('========================================');
  console.log(`  Input:       ${inputPath}`);
  console.log(`  Output:      ${outputPath}`);
  console.log(`  Limit:       ${LIMIT || 'all'}`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Sources:     ${[
    !SKIP_WEBSITE ? 'website-finder' : null,
    !NO_COMMONCRAWL ? 'common-crawl' : null,
    !NO_CLOUDFLARE && cfCrawler.isEnabled() ? 'cloudflare-crawl' : null,
    !NO_SMTP ? 'smtp-patterns' : null,
  ].filter(Boolean).join(', ')}`);
  console.log('========================================');
  console.log('');

  // ── Read CSV ─────────────────────────────────────────────────────────────

  log.info(`Reading CSV: ${inputPath}`);
  const allLeads = await readCSV(inputPath);
  stats.totalLeads = allLeads.length;
  log.info(`Loaded ${allLeads.length} leads`);

  // ── Filter to leads needing enrichment ───────────────────────────────────

  const toEnrich = [];
  for (const lead of allLeads) {
    if (lead.email) {
      stats.alreadyHasEmail++;
      continue;
    }
    const searchName = lead.firm_name || `${lead.first_name || ''} ${lead.last_name || ''} ${lead.trade_category || lead.license_type || 'contractor'}`.trim();
    if (!searchName && !lead.website) {
      stats.noFirmName++;
      continue;
    }
    toEnrich.push(lead);
  }

  // Apply limit
  const batch = LIMIT > 0 ? toEnrich.slice(0, LIMIT) : toEnrich;
  stats.needsEnrichment = batch.length;

  log.info(`${stats.alreadyHasEmail} already have email, ${stats.noFirmName} have no firm/website`);
  log.info(`Processing ${batch.length} leads for email enrichment`);

  if (batch.length === 0) {
    log.warn('No leads to enrich — all have emails or lack firm names/websites');
    return;
  }

  // ── Process leads ────────────────────────────────────────────────────────

  await processWithConcurrency(batch, CONCURRENCY, processLead);

  // ── Write output ─────────────────────────────────────────────────────────

  log.info(`Writing enriched CSV: ${outputPath}`);
  await writeCSV(outputPath, allLeads);

  // ── Print summary ────────────────────────────────────────────────────────

  const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);

  console.log('');
  console.log('========================================');
  console.log('  ENRICHMENT SUMMARY');
  console.log('========================================');
  console.log(`  Total leads:          ${stats.totalLeads}`);
  console.log(`  Already had email:    ${stats.alreadyHasEmail}`);
  console.log(`  No firm/website:      ${stats.noFirmName}`);
  console.log(`  Processed:            ${stats.needsEnrichment}`);
  console.log('  ──────────────────────────────────');
  console.log(`  Websites found:       ${stats.websitesFound}`);
  console.log(`    DNS guess:          ${stats.websiteByDnsGuess}`);
  console.log(`    Google search:      ${stats.websiteByGoogle}`);
  console.log(`    Already had:        ${stats.websiteAlreadyHad}`);
  console.log('  ──────────────────────────────────');
  console.log(`  Emails found:         ${stats.emailsFound}`);
  console.log(`    Common Crawl:       ${stats.emailByCommonCrawl}`);
  console.log(`    Cloudflare Crawl:   ${stats.emailByCloudflare}`);
  console.log(`    SMTP patterns:      ${stats.emailBySmtp}`);
  console.log('  ──────────────────────────────────');
  console.log(`  Errors:               ${stats.errors}`);
  console.log(`  Time:                 ${elapsed}s`);
  console.log(`  Output:               ${outputPath}`);
  console.log('========================================');
  console.log('');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
