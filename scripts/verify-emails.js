#!/usr/bin/env node
/**
 * Email Verification & Discovery Pipeline
 *
 * This is how Hunter.io / Apollo / Snov.io do it:
 *
 * PHASE 1: SMTP-verify all 29k generated pattern emails
 *   - Connect to mail server, RCPT TO → does this mailbox exist?
 *   - Valid = real email. Invalid = remove it.
 *   - Catch-all domains = can't verify, keep best-guess pattern.
 *
 * PHASE 2: Crawl firm websites for emails (1,300+ sites)
 *   - Visit homepage, /contact, /about, /team, /attorneys
 *   - Extract mailto: links, text emails, JSON-LD, meta tags
 *   - Score by name match (john.smith@ for John Smith)
 *
 * PHASE 3: Pattern learning across firms
 *   - When we find a verified email format at a domain (e.g., jsmith@firm.com)
 *   - Apply that same pattern to all other people at that firm
 *
 * Usage:
 *   node scripts/verify-emails.js                    # run all phases
 *   node scripts/verify-emails.js --phase 1          # SMTP only
 *   node scripts/verify-emails.js --phase 2          # website crawl only
 *   node scripts/verify-emails.js --phase 3          # pattern learning only
 *   node scripts/verify-emails.js --limit 1000       # limit per phase
 *   node scripts/verify-emails.js --concurrency 10   # SMTP connections
 */

const path = require('path');
const Database = require('better-sqlite3');
const EmailVerifier = require('../lib/email-verifier');

const DB_PATH = path.join(__dirname, '..', 'data', 'leads.db');

// Parse args
const args = process.argv.slice(2);
let phase = 0; // 0 = all
let limit = 0; // 0 = unlimited
let smtpConcurrency = 5;

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--phase' && args[i + 1]) { phase = parseInt(args[i + 1]); i++; }
  if (args[i] === '--limit' && args[i + 1]) { limit = parseInt(args[i + 1]); i++; }
  if (args[i] === '--concurrency' && args[i + 1]) { smtpConcurrency = parseInt(args[i + 1]); i++; }
}

function getDb(readonly = true) {
  return new Database(DB_PATH, { readonly });
}

function progress(current, total, detail = '') {
  const pct = ((current / total) * 100).toFixed(1);
  const bar = '█'.repeat(Math.floor(current / total * 20)) + '░'.repeat(20 - Math.floor(current / total * 20));
  process.stdout.write(`\r  ${bar} ${pct}% (${current}/${total}) ${detail.substring(0, 40).padEnd(40)}`);
}

// ============================================================
// PHASE 1: SMTP Verification of generated emails
// Uses concurrent worker pool — each domain is a different mail
// server so we can check many in parallel.
// ============================================================
async function phase1_smtpVerify() {
  console.log('\n' + '='.repeat(60));
  console.log('PHASE 1: SMTP Email Verification');
  console.log('='.repeat(60));

  const db = getDb(true);

  const leads = db.prepare(`
    SELECT rowid, first_name, last_name, email, firm_name, website
    FROM leads
    WHERE email_source = 'generated_pattern'
      AND email IS NOT NULL AND email != ''
    ORDER BY email
  `).all();

  db.close();

  console.log(`\nGenerated emails to verify: ${leads.length}`);

  if (leads.length === 0) {
    console.log('Nothing to verify.');
    return { verified: 0, invalid: 0, catchAll: 0, errors: 0 };
  }

  // Group by domain
  const domainMap = new Map();
  for (const lead of leads) {
    const domain = lead.email.split('@')[1]?.toLowerCase();
    if (!domain) continue;
    if (!domainMap.has(domain)) domainMap.set(domain, []);
    domainMap.get(domain).push(lead);
  }

  const domainList = [...domainMap.entries()];
  const totalToProcess = limit > 0 ? Math.min(domainList.length, limit) : domainList.length;
  console.log(`Unique domains: ${domainList.length} (processing: ${totalToProcess})`);
  console.log(`Concurrency: ${smtpConcurrency} parallel domains\n`);

  // Ensure email_verified column exists
  const setupDb = getDb(false);
  try { setupDb.exec('ALTER TABLE leads ADD COLUMN email_verified INTEGER DEFAULT 0'); } catch {}
  setupDb.close();

  const stats = { verified: 0, invalid: 0, catchAll: 0, noMx: 0, errors: 0, removed: 0 };
  let domainIdx = 0;
  let completedDomains = 0;

  // DB update queue — batch writes to avoid lock contention
  const updateQueue = [];
  function flushUpdates() {
    if (updateQueue.length === 0) return;
    const wdb = getDb(false);
    wdb.pragma('journal_mode = WAL');
    const txn = wdb.transaction((updates) => {
      for (const u of updates) {
        if (u.action === 'verify') {
          wdb.prepare('UPDATE leads SET email = ?, email_source = ?, email_verified = 1 WHERE rowid = ?')
            .run(u.email, 'smtp_verified', u.rowid);
        } else if (u.action === 'catchall') {
          wdb.prepare("UPDATE leads SET email_source = 'catch_all_unverified' WHERE rowid = ?")
            .run(u.rowid);
        } else if (u.action === 'remove') {
          wdb.prepare('UPDATE leads SET email = NULL, email_source = NULL WHERE rowid = ?')
            .run(u.rowid);
        }
      }
    });
    txn(updateQueue.splice(0));
    wdb.close();
  }

  // Worker function — processes one domain at a time
  async function processDomain(domain, domainLeads) {
    const verifier = new EmailVerifier({ timeout: 8000, retries: 0 });

    // Step 1: MX lookup
    const mxHost = await verifier.getMxHost(domain);
    if (!mxHost) {
      for (const lead of domainLeads) {
        updateQueue.push({ action: 'remove', rowid: lead.rowid });
        stats.noMx++;
        stats.removed++;
      }
      return;
    }

    // Step 2: Catch-all check
    const catchAll = await verifier.isCatchAll(domain, mxHost);
    if (catchAll) {
      for (const lead of domainLeads) {
        updateQueue.push({ action: 'catchall', rowid: lead.rowid });
        stats.catchAll++;
      }
      return;
    }

    // Step 3: Verify first lead's email to learn the pattern
    let verifiedPattern = null;
    const firstLead = domainLeads[0];

    // Try the generated email
    let result = await verifier.smtpCheck(firstLead.email, mxHost);
    if (result.valid) {
      updateQueue.push({ action: 'verify', rowid: firstLead.rowid, email: firstLead.email });
      stats.verified++;
      verifiedPattern = detectPattern(firstLead.email, firstLead.first_name, firstLead.last_name);
    } else {
      // Try top 4 alternative patterns
      const patterns = verifier.generatePatterns(firstLead.first_name, firstLead.last_name, domain);
      for (const altEmail of patterns.slice(0, 4)) {
        if (altEmail === firstLead.email) continue;
        const altResult = await verifier.smtpCheck(altEmail, mxHost);
        if (altResult.valid) {
          updateQueue.push({ action: 'verify', rowid: firstLead.rowid, email: altEmail });
          stats.verified++;
          verifiedPattern = detectPattern(altEmail, firstLead.first_name, firstLead.last_name);
          break;
        }
        await sleep(200);
      }
      if (!verifiedPattern) {
        updateQueue.push({ action: 'remove', rowid: firstLead.rowid });
        stats.invalid++;
        stats.removed++;
      }
    }

    // Step 4: Apply learned pattern to remaining leads at this domain
    for (let i = 1; i < domainLeads.length; i++) {
      const lead = domainLeads[i];
      if (verifiedPattern) {
        const newEmail = applyPattern(verifiedPattern, lead.first_name, lead.last_name, domain);
        if (newEmail) {
          // Quick verify this specific email
          const r = await verifier.smtpCheck(newEmail, mxHost);
          if (r.valid) {
            updateQueue.push({ action: 'verify', rowid: lead.rowid, email: newEmail });
            stats.verified++;
          } else {
            updateQueue.push({ action: 'remove', rowid: lead.rowid });
            stats.invalid++;
            stats.removed++;
          }
        } else {
          updateQueue.push({ action: 'remove', rowid: lead.rowid });
          stats.invalid++;
          stats.removed++;
        }
      } else {
        updateQueue.push({ action: 'remove', rowid: lead.rowid });
        stats.invalid++;
        stats.removed++;
      }
      await sleep(100);
    }
  }

  // Worker pool — process N domains concurrently
  async function worker() {
    while (domainIdx < totalToProcess) {
      const idx = domainIdx++;
      const [domain, domainLeads] = domainList[idx];

      try {
        await processDomain(domain, domainLeads);
      } catch (err) {
        stats.errors++;
      }

      completedDomains++;

      // Flush DB updates every 20 domains
      if (completedDomains % 20 === 0) {
        flushUpdates();
        const pct = ((completedDomains / totalToProcess) * 100).toFixed(1);
        console.log(`  [${completedDomains}/${totalToProcess}] ${pct}% — verified: ${stats.verified} | catch-all: ${stats.catchAll} | removed: ${stats.removed} | no-MX: ${stats.noMx}`);
      }
    }
  }

  // Launch workers
  const workers = [];
  for (let i = 0; i < smtpConcurrency; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  // Final flush
  flushUpdates();

  console.log('\n  SMTP Results:');
  console.log(`    Verified:       ${stats.verified}`);
  console.log(`    Catch-all:      ${stats.catchAll} (kept as best-guess)`);
  console.log(`    Invalid/removed: ${stats.removed}`);
  console.log(`    No MX records:  ${stats.noMx}`);
  console.log(`    Errors:         ${stats.errors}`);
  console.log(`    Done.`);

  return stats;
}


// ============================================================
// PHASE 2: Website Crawling for emails (no Puppeteer — pure HTTP)
// ============================================================
async function phase2_websiteCrawl() {
  console.log('\n' + '='.repeat(60));
  console.log('PHASE 2: Website Email Crawling');
  console.log('='.repeat(60));

  const db = getDb(true);

  // Leads with a website but no real email
  const leads = db.prepare(`
    SELECT rowid, first_name, last_name, email, email_source, firm_name, website
    FROM leads
    WHERE website IS NOT NULL AND website != ''
      AND (email IS NULL OR email = '' OR email_source = 'generated_pattern' OR email_source = 'catch_all_unverified')
    ORDER BY icp_score DESC
  `).all();

  // Get unique domains to avoid re-crawling
  const domainMap = new Map();
  for (const lead of leads) {
    const domain = extractDomain(lead.website);
    if (!domain) continue;
    if (!domainMap.has(domain)) domainMap.set(domain, { website: lead.website, leads: [] });
    domainMap.get(domain).leads.push(lead);
  }

  db.close();

  const totalDomains = limit > 0 ? Math.min(domainMap.size, limit) : domainMap.size;
  console.log(`\nWebsites to crawl: ${totalDomains} (covering ${leads.length} leads)`);

  if (totalDomains === 0) {
    console.log('Nothing to crawl.');
    return { found: 0, crawled: 0 };
  }

  const writeDb = getDb(false);
  writeDb.pragma('journal_mode = WAL');

  const stats = { crawled: 0, found: 0, failed: 0 };
  let domainIdx = 0;

  // HTTP-based email extraction (no Puppeteer needed for most sites)
  const https = require('https');
  const http = require('http');

  for (const [domain, data] of domainMap) {
    if (domainIdx >= totalDomains) break;
    domainIdx++;

    progress(domainIdx, totalDomains, domain);

    try {
      // Fetch homepage
      const homeHtml = await fetchPage(data.website);
      if (!homeHtml) { stats.failed++; continue; }

      stats.crawled++;
      const emails = new Set();

      // Extract emails from homepage
      extractEmailsFromHtml(homeHtml).forEach(e => emails.add(e));

      // Find and fetch contact/about pages
      const contactUrls = findContactUrls(homeHtml, domain);
      for (const contactUrl of contactUrls.slice(0, 2)) {
        await sleep(1000 + Math.random() * 1000);
        try {
          const contactHtml = await fetchPage(contactUrl);
          if (contactHtml) {
            extractEmailsFromHtml(contactHtml).forEach(e => emails.add(e));
          }
        } catch {}
      }

      // Also try common paths
      const commonPaths = ['/contact', '/about', '/attorneys', '/our-team', '/team'];
      for (const p of commonPaths) {
        if (emails.size >= 5) break;
        const tryUrl = `https://${domain}${p}`;
        if (contactUrls.includes(tryUrl)) continue;
        await sleep(500);
        try {
          const html = await fetchPage(tryUrl);
          if (html) extractEmailsFromHtml(html).forEach(e => emails.add(e));
        } catch {}
      }

      if (emails.size === 0) continue;

      const emailList = [...emails].filter(e => {
        const d = e.split('@')[1]?.toLowerCase();
        return d && !IGNORE_DOMAINS.has(d);
      });

      // Match each lead at this domain to the best email found
      for (const lead of data.leads) {
        const best = scoreEmailForPerson(emailList, lead.first_name, lead.last_name, domain);
        if (best && !isGenericEmail(best)) {
          writeDb.prepare('UPDATE leads SET email = ?, email_source = ?, email_verified = 1 WHERE rowid = ?')
            .run(best, 'website_crawl', lead.rowid);
          stats.found++;
        }
      }

    } catch (err) {
      stats.failed++;
    }

    await sleep(1500 + Math.random() * 1000);
  }

  writeDb.close();

  console.log('\n');
  console.log(`  Websites crawled: ${stats.crawled}`);
  console.log(`  Emails found:     ${stats.found}`);
  console.log(`  Failed:           ${stats.failed}`);
  console.log(`  Done.`);

  return stats;
}


// ============================================================
// PHASE 3: Pattern Learning — propagate verified emails to firm-mates
// ============================================================
async function phase3_patternLearning() {
  console.log('\n' + '='.repeat(60));
  console.log('PHASE 3: Pattern Learning (Firm-mate Propagation)');
  console.log('='.repeat(60));

  const db = getDb(true);

  // Find domains where we have at least one verified email
  // and other people at the same domain who need emails
  const verifiedEmails = db.prepare(`
    SELECT first_name, last_name, email, website,
           SUBSTR(email, INSTR(email, '@')+1) as domain
    FROM leads
    WHERE email_verified = 1
      AND email IS NOT NULL AND email != ''
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
  `).all();

  // Build domain → pattern map
  const domainPatterns = new Map();
  for (const lead of verifiedEmails) {
    const domain = lead.domain?.toLowerCase();
    if (!domain) continue;
    const pattern = detectPattern(lead.email, lead.first_name, lead.last_name);
    if (pattern && !domainPatterns.has(domain)) {
      domainPatterns.set(domain, pattern);
    }
  }

  console.log(`\nDomains with verified patterns: ${domainPatterns.size}`);

  // Also learn patterns from scraped emails (bar directory emails are real)
  const scrapedEmails = db.prepare(`
    SELECT first_name, last_name, email,
           SUBSTR(email, INSTR(email, '@')+1) as domain
    FROM leads
    WHERE (email_source IS NULL OR email_source NOT IN ('generated_pattern', 'catch_all_unverified'))
      AND email IS NOT NULL AND email != ''
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
      AND email NOT LIKE 'info@%' AND email NOT LIKE 'contact@%'
      AND email NOT LIKE 'office@%' AND email NOT LIKE 'admin@%'
  `).all();

  for (const lead of scrapedEmails) {
    const domain = lead.domain?.toLowerCase();
    if (!domain || FREE_PROVIDERS.has(domain)) continue;
    const pattern = detectPattern(lead.email, lead.first_name, lead.last_name);
    if (pattern && !domainPatterns.has(domain)) {
      domainPatterns.set(domain, pattern);
    }
  }

  console.log(`Total domains with known patterns (incl. scraped): ${domainPatterns.size}`);

  // Find leads at these domains who need emails
  const needEmail = db.prepare(`
    SELECT rowid, first_name, last_name, email, email_source, website, firm_name
    FROM leads
    WHERE (email IS NULL OR email = '' OR email_source = 'generated_pattern' OR email_source = 'catch_all_unverified')
      AND first_name IS NOT NULL AND first_name != ''
      AND last_name IS NOT NULL AND last_name != ''
  `).all();

  db.close();

  console.log(`Leads needing email: ${needEmail.length}`);

  const writeDb = getDb(false);
  writeDb.pragma('journal_mode = WAL');

  let applied = 0;
  let skipped = 0;

  // For each lead, check if we know the pattern for their email domain
  for (const lead of needEmail) {
    // Figure out what domain this person should be at
    let domain = null;

    // From existing generated email
    if (lead.email && lead.email.includes('@')) {
      domain = lead.email.split('@')[1].toLowerCase();
    }
    // From website
    if (!domain && lead.website) {
      domain = extractDomain(lead.website);
    }

    if (!domain || FREE_PROVIDERS.has(domain)) { skipped++; continue; }

    const pattern = domainPatterns.get(domain);
    if (!pattern) { skipped++; continue; }

    const newEmail = applyPattern(pattern, lead.first_name, lead.last_name, domain);
    if (newEmail) {
      writeDb.prepare('UPDATE leads SET email = ?, email_source = ? WHERE rowid = ?')
        .run(newEmail, 'pattern_learned', lead.rowid);
      applied++;
    } else {
      skipped++;
    }
  }

  writeDb.close();

  console.log(`\n  Patterns applied: ${applied}`);
  console.log(`  Skipped:          ${skipped}`);
  console.log(`  Done.`);

  return { applied, skipped };
}


// ============================================================
// Helper functions
// ============================================================

const IGNORE_DOMAINS = new Set([
  'example.com', 'test.com', 'email.com', 'yoursite.com', 'yourdomain.com',
  'sentry.io', 'wixpress.com', 'w3.org', 'schema.org', 'googleapis.com',
  'google.com', 'facebook.com', 'twitter.com', 'instagram.com',
  'wordpress.com', 'wp.com', 'gravatar.com', 'cloudflare.com',
  'jquery.com', 'bootstrapcdn.com', 'fontawesome.com', 'googletagmanager.com',
]);

const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com', 'comcast.net', 'bellsouth.net',
  'att.net', 'verizon.net', 'cox.net', 'sbcglobal.net', 'earthlink.net',
  'hawaii.rr.com', 'charter.net', 'roadrunner.com',
]);

const EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function extractDomain(website) {
  if (!website) return null;
  try {
    const url = website.startsWith('http') ? website : 'https://' + website;
    return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return null;
  }
}

function isGenericEmail(email) {
  const local = email.split('@')[0].toLowerCase();
  return ['info', 'contact', 'office', 'admin', 'support', 'hello', 'enquiries',
          'reception', 'billing', 'accounts', 'general', 'mail', 'team', 'hr', 'sales'].includes(local);
}

/**
 * Detect the email pattern used from a known email + name.
 * Returns pattern string like 'first.last', 'flast', 'firstlast', etc.
 */
function detectPattern(email, firstName, lastName) {
  if (!email || !firstName || !lastName) return null;

  const local = email.split('@')[0].toLowerCase();
  const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return null;

  const fi = f[0];

  if (local === `${f}.${l}`) return 'first.last';
  if (local === `${f}${l}`) return 'firstlast';
  if (local === `${fi}${l}`) return 'flast';
  if (local === `${fi}.${l}`) return 'f.last';
  if (local === `${f}`) return 'first';
  if (local === `${l}`) return 'last';
  if (local === `${f}_${l}`) return 'first_last';
  if (local === `${l}.${f}`) return 'last.first';
  if (local === `${l}${fi}`) return 'lastf';
  if (local === `${f}.${l[0]}`) return 'first.l';
  if (local === `${f}${l[0]}`) return 'firstl';

  return null;
}

/**
 * Apply a known pattern to generate an email for a different person.
 */
function applyPattern(pattern, firstName, lastName, domain) {
  if (!firstName || !lastName || !domain) return null;

  const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return null;

  const fi = f[0];

  switch (pattern) {
    case 'first.last': return `${f}.${l}@${domain}`;
    case 'firstlast': return `${f}${l}@${domain}`;
    case 'flast': return `${fi}${l}@${domain}`;
    case 'f.last': return `${fi}.${l}@${domain}`;
    case 'first': return `${f}@${domain}`;
    case 'last': return `${l}@${domain}`;
    case 'first_last': return `${f}_${l}@${domain}`;
    case 'last.first': return `${l}.${f}@${domain}`;
    case 'lastf': return `${l}${fi}@${domain}`;
    case 'first.l': return `${f}.${l[0]}@${domain}`;
    case 'firstl': return `${f}${l[0]}@${domain}`;
    default: return `${f}.${l}@${domain}`;
  }
}

/**
 * Fetch a webpage via HTTP/HTTPS (no Puppeteer needed).
 */
function fetchPage(url, timeout = 8000) {
  return new Promise((resolve) => {
    try {
      if (!url.startsWith('http')) url = 'https://' + url;
      const lib = url.startsWith('https') ? require('https') : require('http');

      const timer = setTimeout(() => resolve(null), timeout);

      const req = lib.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          'Accept': 'text/html,application/xhtml+xml',
        },
        timeout: timeout,
        rejectUnauthorized: false,
      }, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          clearTimeout(timer);
          const redirectUrl = res.headers.location.startsWith('http')
            ? res.headers.location
            : new URL(res.headers.location, url).href;
          resolve(fetchPage(redirectUrl, timeout));
          return;
        }

        if (res.statusCode !== 200) {
          clearTimeout(timer);
          resolve(null);
          return;
        }

        let data = '';
        res.on('data', chunk => {
          data += chunk;
          if (data.length > 500000) { res.destroy(); clearTimeout(timer); resolve(data); }
        });
        res.on('end', () => { clearTimeout(timer); resolve(data); });
        res.on('error', () => { clearTimeout(timer); resolve(null); });
      });

      req.on('error', () => { clearTimeout(timer); resolve(null); });
      req.on('timeout', () => { req.destroy(); clearTimeout(timer); resolve(null); });
    } catch {
      resolve(null);
    }
  });
}

/**
 * Extract emails from raw HTML.
 */
function extractEmailsFromHtml(html) {
  if (!html) return [];
  const emails = new Set();

  // mailto: links
  const mailtoRegex = /mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi;
  let match;
  while ((match = mailtoRegex.exec(html)) !== null) {
    emails.add(match[1].toLowerCase());
  }

  // Email patterns in text
  const textEmails = html.match(EMAIL_REGEX) || [];
  textEmails.forEach(e => {
    const lower = e.toLowerCase();
    const domain = lower.split('@')[1];
    if (!IGNORE_DOMAINS.has(domain)) {
      emails.add(lower);
    }
  });

  // JSON-LD structured data
  const ldRegex = /<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
  while ((match = ldRegex.exec(html)) !== null) {
    try {
      const data = JSON.parse(match[1]);
      if (data.email) emails.add(data.email.toLowerCase().replace('mailto:', ''));
      if (data.contactPoint?.email) emails.add(data.contactPoint.email.toLowerCase());
    } catch {}
  }

  return [...emails].filter(e => {
    if (e.length < 5 || e.length > 100) return false;
    const domain = e.split('@')[1];
    if (!domain || domain.split('.').length < 2) return false;
    const local = e.split('@')[0];
    if (['noreply', 'no-reply', 'donotreply', 'mailer-daemon', 'postmaster', 'webmaster'].includes(local)) return false;
    return true;
  });
}

/**
 * Find contact/about page URLs in HTML.
 */
function findContactUrls(html, baseDomain) {
  const urls = new Set();
  const linkRegex = /<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match;

  while ((match = linkRegex.exec(html)) !== null) {
    const href = match[1];
    const text = match[2].toLowerCase().replace(/<[^>]*>/g, '').trim();

    if (text.includes('contact') || text.includes('about') || text.includes('team') ||
        text.includes('attorneys') || text.includes('lawyers') || text.includes('staff') ||
        text.includes('our firm') || text.includes('people') ||
        href.includes('/contact') || href.includes('/about') || href.includes('/team') ||
        href.includes('/attorneys') || href.includes('/our-team') || href.includes('/our-firm') ||
        href.includes('/people') || href.includes('/professionals')) {

      try {
        let fullUrl;
        if (href.startsWith('http')) {
          fullUrl = href;
          const urlDomain = new URL(fullUrl).hostname.replace(/^www\./, '');
          if (urlDomain !== baseDomain.replace(/^www\./, '')) continue;
        } else if (href.startsWith('/')) {
          fullUrl = `https://${baseDomain}${href}`;
        } else {
          continue;
        }
        urls.add(fullUrl);
      } catch {}
    }
  }

  return [...urls].slice(0, 5);
}

/**
 * Score emails against a person's name and return the best match.
 */
function scoreEmailForPerson(emails, firstName, lastName, firmDomain) {
  if (!emails || emails.length === 0) return null;

  const f = (firstName || '').toLowerCase().replace(/[^a-z]/g, '');
  const l = (lastName || '').toLowerCase().replace(/[^a-z]/g, '');

  const scored = emails.map(email => {
    const local = email.split('@')[0].toLowerCase();
    const domain = email.split('@')[1]?.toLowerCase() || '';
    let score = 0;

    // On firm domain = good
    if (domain === firmDomain) score += 5;

    // Generic = bad
    if (isGenericEmail(email)) return { email, score: -10 };

    // Name matching
    if (f && l) {
      if (local === `${f}.${l}` || local === `${f}_${l}`) score += 25;
      else if (local === `${f}${l}`) score += 22;
      else if (local === `${f[0]}${l}`) score += 20;
      else if (local === `${f[0]}.${l}`) score += 20;
      else if (local.includes(f) && local.includes(l)) score += 18;
      else if (local.includes(l)) score += 10;
      else if (local.includes(f)) score += 5;
    }

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  const best = scored.find(s => s.score > 0);
  return best ? best.email : null;
}


// ============================================================
// Main
// ============================================================
async function main() {
  console.log('EMAIL VERIFICATION & DISCOVERY PIPELINE');
  console.log(`Phase: ${phase === 0 ? 'ALL' : phase} | Limit: ${limit || 'none'} | Concurrency: ${smtpConcurrency}`);

  const startStats = getDb(true);
  const totalBefore = startStats.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
  const verifiedBefore = startStats.prepare("SELECT COUNT(*) as c FROM leads WHERE email_verified = 1").get().c;
  const generatedBefore = startStats.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'generated_pattern'").get().c;
  startStats.close();

  console.log(`\nBefore: ${totalBefore} emails (${verifiedBefore} verified, ${generatedBefore} generated)`);

  if (phase === 0 || phase === 1) await phase1_smtpVerify();
  if (phase === 0 || phase === 2) await phase2_websiteCrawl();
  if (phase === 0 || phase === 3) await phase3_patternLearning();

  // Final stats
  const endDb = getDb(true);
  const totalAfter = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != ''").get().c;
  const verifiedAfter = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_verified = 1").get().c;
  const smtpVerified = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'smtp_verified'").get().c;
  const websiteCrawl = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'website_crawl'").get().c;
  const patternLearned = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'pattern_learned'").get().c;
  const catchAll = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'catch_all_unverified'").get().c;
  const generated = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email_source = 'generated_pattern'").get().c;
  const scraped = endDb.prepare("SELECT COUNT(*) as c FROM leads WHERE email IS NOT NULL AND email != '' AND (email_source IS NULL OR email_source NOT IN ('generated_pattern','smtp_verified','website_crawl','pattern_learned','catch_all_unverified'))").get().c;
  endDb.close();

  console.log('\n' + '='.repeat(60));
  console.log('FINAL EMAIL BREAKDOWN');
  console.log('='.repeat(60));
  console.log(`  Total emails:      ${totalAfter} (was ${totalBefore})`);
  console.log(`  SMTP verified:     ${smtpVerified}`);
  console.log(`  Website crawled:   ${websiteCrawl}`);
  console.log(`  Pattern learned:   ${patternLearned}`);
  console.log(`  Catch-all (kept):  ${catchAll}`);
  console.log(`  Scraped (bar dir): ${scraped}`);
  console.log(`  Unverified guess:  ${generated}`);
  console.log(`  Total verified:    ${verifiedAfter}`);
  console.log('='.repeat(60));
}

main().catch(err => {
  console.error('Pipeline error:', err);
  process.exit(1);
});
