#!/usr/bin/env node
/**
 * General-Purpose Immigration Lead Email Enricher
 *
 * Takes any CSV with standard columns and enriches emails using:
 *   1. MX record verification — confirm the domain accepts email
 *   2. Email pattern generation — firstname.lastname@domain
 *   3. DNS-based domain discovery — for leads with firm names but no domain
 *
 * Usage:
 *   node scripts/enrich-immigration-emails.js --input output/us-immigration-lawyers-justia.csv
 *   node scripts/enrich-immigration-emails.js --input output/file.csv --concurrency 30
 *   node scripts/enrich-immigration-emails.js --input output/file.csv --limit 100
 *
 * Flags:
 *   --input FILE       CSV file path (required)
 *   --limit N          Max leads to process (for testing)
 *   --concurrency N    Parallel DNS lookups (default: 20)
 */

const dns = require('dns');
const fs = require('fs');
const path = require('path');

// ─── CLI Argument Parsing ──────────────────────────────────────────

const cliArgs = process.argv.slice(2);
function getArg(name) {
  const idx = cliArgs.indexOf(`--${name}`);
  return idx === -1 ? undefined : cliArgs[idx + 1];
}

const INPUT = getArg('input');
const LIMIT = parseInt(getArg('limit') || '0') || 0;
const CONCURRENCY = parseInt(getArg('concurrency') || '20') || 20;

if (!INPUT) {
  console.error('Usage: node scripts/enrich-immigration-emails.js --input <csv-file>');
  console.error('');
  console.error('Example:');
  console.error('  node scripts/enrich-immigration-emails.js --input output/us-immigration-lawyers-justia.csv');
  process.exit(1);
}

// Resolve input path (relative to cwd or absolute)
const INPUT_PATH = path.resolve(INPUT);
const parsed = path.parse(INPUT_PATH);
const OUTPUT_PATH = path.join(parsed.dir, parsed.name + '-enriched' + parsed.ext);

// ─── Domain Blocklists ──────────────────────────────────────────────

const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com', 'comcast.net', 'att.net',
  'verizon.net', 'sbcglobal.net', 'cox.net', 'charter.net',
  'earthlink.net', 'optonline.net', 'bellsouth.net', 'mindspring.com',
  'yahoo.co.uk', 'yahoo.co.nz', 'hotmail.co.uk', 'hotmail.co.nz',
  'outlook.co.nz', 'xtra.co.nz', 'btinternet.com', 'sky.com',
]);

const SKIP_DOMAINS = new Set([
  'yelp.com', 'bbb.org', 'facebook.com', 'linkedin.com', 'instagram.com',
  'twitter.com', 'x.com', 'youtube.com', 'yellowpages.com', 'pinterest.com',
  'tiktok.com', 'reddit.com', 'crunchbase.com', 'glassdoor.com',
  'indeed.com', 'amazon.com', 'google.com', 'apple.com',
  'wikipedia.org', 'avvo.com', 'justia.com', 'findlaw.com',
  'martindale.com', 'lawyers.com', 'nolo.com', 'lawinfo.com',
  'superlawyers.com', 'bestlawyers.com', 'aila.org', 'ailalawyer.com',
  'sra.org.uk', 'lawsociety.org.uk', 'iaa.govt.nz',
  'mbieregisters.govt.nz', 'companiesoffice.govt.nz',
]);

// ─── CSV Parsing / Writing ──────────────────────────────────────────

function parseCSVLine(line) {
  const vals = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      vals.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  vals.push(current);
  return vals;
}

function parseCSV(content) {
  const lines = content.split('\n');
  if (lines.length > 0 && lines[0].charCodeAt(0) === 0xFEFF) {
    lines[0] = lines[0].substring(1);
  }
  let headerIdx = 0;
  while (headerIdx < lines.length && !lines[headerIdx].trim()) headerIdx++;
  if (headerIdx >= lines.length) return { headers: [], rows: [] };

  const headers = parseCSVLine(lines[headerIdx]).map(h => h.trim());
  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const vals = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => { row[h] = (vals[idx] || '').trim(); });
    rows.push(row);
  }
  return { headers, rows };
}

function escapeCSV(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function writeCSV(filePath, headers, rows) {
  const lines = [headers.map(escapeCSV).join(',')];
  for (const row of rows) {
    lines.push(headers.map(h => escapeCSV(row[h] || '')).join(','));
  }
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, lines.join('\n') + '\n', 'utf8');
}

// ─── Domain Helpers ──────────────────────────────────────────────────

function extractDomain(urlStr) {
  if (!urlStr) return '';
  try {
    const parsed = new URL(urlStr.startsWith('http') ? urlStr : 'https://' + urlStr);
    let host = parsed.hostname.toLowerCase();
    if (host.startsWith('www.')) host = host.substring(4);
    return host;
  } catch {
    return '';
  }
}

function isSkippedDomain(domain) {
  if (!domain) return true;
  if (domain.endsWith('.gov') || domain.endsWith('.edu') || domain.endsWith('.mil')) return true;
  if (domain.endsWith('.govt.nz') || domain.endsWith('.gov.uk') || domain.endsWith('.gov.au')) return true;
  for (const skip of SKIP_DOMAINS) {
    if (domain === skip || domain.endsWith('.' + skip)) return true;
  }
  return false;
}

// ─── MX Record Check ─────────────────────────────────────────────────

const mxCache = new Map();

function checkMX(domain) {
  return new Promise((resolve) => {
    if (mxCache.has(domain)) return resolve(mxCache.get(domain));
    dns.resolveMx(domain, (err, records) => {
      if (err || !records || records.length === 0) {
        mxCache.set(domain, false);
        return resolve(false);
      }
      mxCache.set(domain, true);
      resolve(true);
    });
  });
}

// ─── DNS A Record Check ──────────────────────────────────────────────

function checkDNS(domain) {
  return new Promise((resolve) => {
    dns.resolve(domain, (err) => {
      resolve(!err);
    });
  });
}

// ─── Domain Discovery from Firm Name ─────────────────────────────────

function slugifyFirm(firmName) {
  let cleaned = firmName.toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|plc|lp|llp|pllc|pc|pa|co|company|group|limited|incorporated|corporation|associates|assoc|professional|prof|pty)\b\.?/gi, '')
    .replace(/\b(law\s*(offices?|firm|group|center|centre|practice|studio)s?\s*(of|at)?)\b/gi, '')
    .replace(/\b(attorneys?\s*at\s*law)\b/gi, '')
    .replace(/\b(immigration|consultants?|advisory|advisers?|services?|solutions?|education)\b/gi, '')
    .replace(/\b(solicitors?|barristers?)\b/gi, '')
    .replace(/[&+]/g, 'and')
    .replace(/[''`]/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .trim();

  const slug = cleaned.replace(/\s+/g, '');
  const slugDash = cleaned.replace(/\s+/g, '-');
  return { slug, slugDash };
}

async function discoverDomain(firmName) {
  if (!firmName) return null;

  const { slug, slugDash } = slugifyFirm(firmName);
  if (!slug || slug.length < 3) return null;

  // Also try raw firm name slug (without stripping keywords)
  let rawCleaned = firmName.toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|plc|lp|llp|pllc|pc|pa|co|company|group|limited|incorporated|corporation|pty)\b\.?/gi, '')
    .replace(/[&+]/g, 'and')
    .replace(/[''`]/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .trim();
  const rawSlug = rawCleaned.replace(/\s+/g, '');

  const candidates = new Set([
    slug + '.com',
    slugDash + '.com',
    rawSlug + '.com',
    slug + '.co.nz',
    rawSlug + '.co.nz',
    slug + '.co.uk',
    rawSlug + '.co.uk',
    slug + '.com.au',
    rawSlug + '.com.au',
    slug + '.net',
    slug + '.law',
    slugDash + '.law',
    rawSlug + '.net',
    slug + '.org',
    slug + '.nz',
    rawSlug + '.nz',
  ]);

  for (const candidate of candidates) {
    if (candidate.length < 6) continue;
    if (isSkippedDomain(candidate) || FREE_PROVIDERS.has(candidate)) continue;

    const hasA = await checkDNS(candidate);
    if (hasA) {
      const hasMx = await checkMX(candidate);
      if (hasMx) return candidate;
    }
  }

  return null;
}

// ─── Email Pattern Generation ────────────────────────────────────────

function cleanFirstName(raw) {
  let name = raw.trim();
  // Remove trailing single-letter middle initials
  name = name.replace(/\s+[A-Z]\.?(\s+[A-Z]\.?)*\s*$/i, '');
  // Take only the first word
  const parts = name.trim().split(/\s+/);
  return parts[0] || name;
}

function generateEmail(firstName, lastName, domain) {
  if (!firstName || !lastName || !domain) return null;
  if (FREE_PROVIDERS.has(domain.toLowerCase())) return null;

  const cleanedFirst = cleanFirstName(firstName);
  const f = cleanedFirst.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return null;

  return `${f}.${l}@${domain.toLowerCase()}`;
}

// ─── Concurrency Control ─────────────────────────────────────────────

async function parallelForEach(items, fn, concurrency) {
  let idx = 0;
  async function worker() {
    while (idx < items.length) {
      const i = idx++;
      await fn(items[i], i);
    }
  }
  const workers = [];
  for (let w = 0; w < Math.min(concurrency, items.length); w++) {
    workers.push(worker());
  }
  await Promise.all(workers);
}

// ─── Utility ─────────────────────────────────────────────────────────

function formatTime(ms) {
  const secs = Math.floor(ms / 1000);
  const mins = Math.floor(secs / 60);
  const hrs = Math.floor(mins / 60);
  if (hrs > 0) return `${hrs}h ${mins % 60}m ${secs % 60}s`;
  if (mins > 0) return `${mins}m ${secs % 60}s`;
  return `${secs}s`;
}

// ─── Main Pipeline ───────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('==========================================================');
  console.log('  MORTAR -- Immigration Lead Email Enricher');
  console.log('==========================================================');
  console.log('');

  // ── Validate input ──

  if (!fs.existsSync(INPUT_PATH)) {
    console.error(`  File not found: ${INPUT_PATH}`);
    process.exit(1);
  }

  // ── Load and parse CSV ──

  const rawContent = fs.readFileSync(INPUT_PATH, 'utf8');
  const { headers, rows } = parseCSV(rawContent);

  if (rows.length === 0) {
    console.error('  No data rows found in CSV');
    process.exit(1);
  }

  console.log(`  Input:        ${path.basename(INPUT_PATH)}`);
  console.log(`  Output:       ${path.basename(OUTPUT_PATH)}`);
  console.log(`  Rows:         ${rows.length}`);
  console.log(`  Columns:      ${headers.join(', ')}`);
  console.log(`  Concurrency:  ${CONCURRENCY}`);
  console.log('');

  // ── Ensure output headers include enrichment columns ──

  const outputHeaders = [...headers];
  if (!outputHeaders.includes('email_source')) outputHeaders.push('email_source');

  // ── Apply limit ──

  let leads = rows;
  if (LIMIT > 0 && leads.length > LIMIT) {
    leads = leads.slice(0, LIMIT);
    console.log(`  Limit:        processing first ${LIMIT} of ${rows.length}`);
    console.log('');
  }

  // ── Pre-analysis ──

  const alreadyHaveEmail = leads.filter(l => l.email && l.email.trim()).length;
  const hasDomainCol = leads.filter(l => l.domain && l.domain.trim()).length;
  const hasWebsiteCol = leads.filter(l => l.website && l.website.trim()).length;
  const hasFirstName = leads.filter(l => l.first_name && l.first_name.trim()).length;
  const hasLastName = leads.filter(l => l.last_name && l.last_name.trim()).length;
  const hasFirmName = leads.filter(l => l.firm_name && l.firm_name.trim()).length;
  const needsProcessing = leads.filter(l => !l.email || !l.email.trim());

  console.log('  Data Coverage:');
  console.log(`    Already have email:  ${alreadyHaveEmail} (${(alreadyHaveEmail/leads.length*100).toFixed(1)}%)`);
  console.log(`    Has domain:          ${hasDomainCol} (${(hasDomainCol/leads.length*100).toFixed(1)}%)`);
  console.log(`    Has website:         ${hasWebsiteCol} (${(hasWebsiteCol/leads.length*100).toFixed(1)}%)`);
  console.log(`    Has first_name:      ${hasFirstName} (${(hasFirstName/leads.length*100).toFixed(1)}%)`);
  console.log(`    Has last_name:       ${hasLastName} (${(hasLastName/leads.length*100).toFixed(1)}%)`);
  console.log(`    Has firm_name:       ${hasFirmName} (${(hasFirmName/leads.length*100).toFixed(1)}%)`);
  console.log(`    Need enrichment:     ${needsProcessing.length}`);
  console.log('');

  if (needsProcessing.length === 0) {
    console.log('  All leads already have emails. Nothing to do.');
    writeCSV(OUTPUT_PATH, outputHeaders, leads);
    console.log(`  Output: ${OUTPUT_PATH}`);
    return;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 1: Resolve domains from website column, MX verify
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 1: Domain Resolution + MX Verification ──────────');
  console.log('');

  const stats = {
    mxVerified: 0,
    mxFailed: 0,
    domainsDiscovered: 0,
    domainDiscoveryFailed: 0,
    emailsGenerated: 0,
    skippedFreeProvider: 0,
    skippedNoName: 0,
    skippedNoDomain: 0,
  };

  // Build work items for leads needing enrichment
  const workItems = [];

  for (const lead of needsProcessing) {
    let domain = (lead.domain || '').trim().toLowerCase();
    if (!domain && lead.website) {
      domain = extractDomain(lead.website);
    }
    if (domain && domain.startsWith('www.')) domain = domain.substring(4);

    workItems.push({
      lead,
      domain: domain || null,
      firstName: (lead.first_name || '').trim(),
      lastName: (lead.last_name || '').trim(),
      firmName: (lead.firm_name || '').trim(),
    });
  }

  // Separate by domain availability
  const withDomain = workItems.filter(w => w.domain && !isSkippedDomain(w.domain) && !FREE_PROVIDERS.has(w.domain));
  const needsDomainDiscovery = workItems.filter(w => !w.domain || isSkippedDomain(w.domain) || FREE_PROVIDERS.has(w.domain));
  const freeProviderLeads = workItems.filter(w => w.domain && FREE_PROVIDERS.has(w.domain));

  stats.skippedFreeProvider = freeProviderLeads.length;

  console.log(`  With usable domain:    ${withDomain.length}`);
  console.log(`  Free provider domain:  ${freeProviderLeads.length} (skipped)`);
  console.log(`  No domain / skipped:   ${needsDomainDiscovery.length - freeProviderLeads.length}`);
  console.log('');

  // ── MX verify existing domains ──

  const mxVerifiedDomains = new Set();
  let mxChecked = 0;

  // Deduplicate domains to avoid redundant DNS lookups
  const uniqueDomains = [...new Set(withDomain.map(w => w.domain))];
  console.log(`  Checking MX records for ${uniqueDomains.length} unique domains...`);

  await parallelForEach(uniqueDomains, async (domain) => {
    const hasMx = await checkMX(domain);
    mxChecked++;
    if (hasMx) {
      stats.mxVerified++;
      mxVerifiedDomains.add(domain);
    } else {
      stats.mxFailed++;
    }
    if (mxChecked % 50 === 0 || mxChecked === uniqueDomains.length) {
      process.stdout.write(`\r  [${mxChecked}/${uniqueDomains.length}] MX verified: ${stats.mxVerified} | no MX: ${stats.mxFailed}    `);
    }
  }, CONCURRENCY);

  console.log('');
  console.log(`  MX check complete: ${stats.mxVerified} verified, ${stats.mxFailed} failed`);
  console.log('');

  // ═══════════════════════════════════════════════════════════════════
  // Step 2: Domain discovery for leads missing a domain
  // ═══════════════════════════════════════════════════════════════════

  const discoveryItems = needsDomainDiscovery.filter(w => w.firmName && !FREE_PROVIDERS.has(w.domain || ''));

  if (discoveryItems.length > 0) {
    console.log('--- Step 2: Domain Discovery from Firm Names ───────────');
    console.log('');

    // Deduplicate by firm name
    const firmDomainCache = new Map();
    const uniqueFirms = [...new Set(discoveryItems.map(w => w.firmName.toLowerCase().replace(/[^a-z0-9]/g, '')))];

    console.log(`  Attempting DNS discovery for ${uniqueFirms.length} unique firms...`);
    let discoveryChecked = 0;

    // Build a map of firmKey -> items for batch processing
    const firmKeyToItems = new Map();
    for (const item of discoveryItems) {
      const key = item.firmName.toLowerCase().replace(/[^a-z0-9]/g, '');
      if (!firmKeyToItems.has(key)) firmKeyToItems.set(key, []);
      firmKeyToItems.get(key).push(item);
    }

    await parallelForEach(uniqueFirms, async (firmKey) => {
      // Find the original firm name from the first item with this key
      const items = firmKeyToItems.get(firmKey);
      if (!items || items.length === 0) return;
      const firmName = items[0].firmName;

      const domain = await discoverDomain(firmName);
      discoveryChecked++;

      if (domain) {
        mxVerifiedDomains.add(domain);
        for (const item of items) {
          item.domain = domain;
          item.lead.domain = domain;
          stats.domainsDiscovered++;
        }
      } else {
        stats.domainDiscoveryFailed += items.length;
      }

      if (discoveryChecked % 50 === 0 || discoveryChecked === uniqueFirms.length) {
        process.stdout.write(`\r  [${discoveryChecked}/${uniqueFirms.length}] discovered: ${stats.domainsDiscovered} | failed: ${stats.domainDiscoveryFailed}    `);
      }
    }, CONCURRENCY);

    console.log('');
    console.log(`  Discovery complete: ${stats.domainsDiscovered} found, ${stats.domainDiscoveryFailed} failed`);
    console.log('');
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 3: Email pattern generation
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 3: Email Pattern Generation ────────────────────');
  console.log('');

  for (const item of workItems) {
    const { lead, firstName, lastName } = item;
    let domain = (item.domain || '').trim().toLowerCase();

    if (!domain || isSkippedDomain(domain) || FREE_PROVIDERS.has(domain)) {
      if (!domain) stats.skippedNoDomain++;
      continue;
    }

    if (!mxVerifiedDomains.has(domain)) continue;

    if (!firstName || !lastName) {
      stats.skippedNoName++;
      continue;
    }

    const email = generateEmail(firstName, lastName, domain);
    if (!email) continue;

    lead.email = email;
    lead.email_source = 'mx-pattern';
    if (!lead.domain) lead.domain = domain;

    stats.emailsGenerated++;
  }

  console.log(`  Emails generated: ${stats.emailsGenerated}`);
  console.log('');

  // ═══════════════════════════════════════════════════════════════════
  // Step 4: Write enriched CSV
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 4: Writing Enriched CSV ────────────────────────');
  console.log('');

  writeCSV(OUTPUT_PATH, outputHeaders, leads);

  const elapsed = Date.now() - startTime;
  const totalEmails = leads.filter(l => l.email && l.email.trim()).length;
  const newEmails = stats.emailsGenerated;
  const emailPct = (totalEmails / leads.length * 100).toFixed(1);

  console.log('==========================================================');
  console.log('  SUMMARY');
  console.log('==========================================================');
  console.log('');
  console.log(`  Total leads:             ${leads.length}`);
  console.log(`  Emails total:            ${totalEmails} (${emailPct}%)`);
  console.log(`    - Already had:         ${alreadyHaveEmail}`);
  console.log(`    - Newly generated:     ${newEmails}`);
  console.log('');
  console.log('  MX Verification:');
  console.log(`    Domains checked:       ${stats.mxVerified + stats.mxFailed}`);
  console.log(`    MX verified:           ${stats.mxVerified}`);
  console.log(`    No MX records:         ${stats.mxFailed}`);
  console.log(`    Unique MX domains:     ${mxVerifiedDomains.size}`);
  console.log('');
  if (stats.domainsDiscovered > 0 || stats.domainDiscoveryFailed > 0) {
    console.log('  Domain Discovery:');
    console.log(`    Discovered:            ${stats.domainsDiscovered}`);
    console.log(`    Failed:                ${stats.domainDiscoveryFailed}`);
    console.log('');
  }
  console.log('  Skipped:');
  console.log(`    Free provider domain:  ${stats.skippedFreeProvider}`);
  console.log(`    No domain available:   ${stats.skippedNoDomain}`);
  console.log(`    No first+last name:    ${stats.skippedNoName}`);
  console.log('');
  console.log(`  Output:  ${OUTPUT_PATH}`);
  console.log(`  Time:    ${formatTime(elapsed)}`);
  console.log('');
}

// ─── Run ─────────────────────────────────────────────────────────────

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
