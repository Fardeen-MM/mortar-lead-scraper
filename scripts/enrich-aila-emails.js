#!/usr/bin/env node
/**
 * AILA Immigration Lawyer Email Enricher
 *
 * Enriches the AILA lawyer CSV with email addresses using:
 *   1. MX record verification — confirm the domain accepts email
 *   2. Email pattern generation — firstname.lastname@domain (primary),
 *      finitial+lastname@domain, firstname@domain
 *   3. DNS-based domain discovery — for lawyers missing a domain but having a firm name,
 *      construct likely domains (firmname.com) and verify via DNS A/MX records
 *
 * No SMTP RCPT TO (port 25 blocked by ISP). MX verification proves the domain
 * is mail-capable; the most common pattern (firstname.lastname@) is used.
 *
 * Usage:
 *   node scripts/enrich-aila-emails.js
 *   node scripts/enrich-aila-emails.js --limit 100
 *   node scripts/enrich-aila-emails.js --concurrency 20
 *
 * Flags:
 *   --limit N          Max leads to process (for testing)
 *   --concurrency N    Parallel DNS lookups (default: 10)
 */

const dns = require('dns');
const fs = require('fs');
const path = require('path');

// ─── Configuration ───────────────────────────────────────────────────

const INPUT = path.join(__dirname, '..', 'output', 'us-aila-immigration-lawyers.csv');
const OUTPUT = path.join(__dirname, '..', 'output', 'us-aila-immigration-lawyers-enriched.csv');

const cliArgs = process.argv.slice(2);
function getArg(name) {
  const idx = cliArgs.indexOf(`--${name}`);
  return idx === -1 ? undefined : cliArgs[idx + 1];
}

const LIMIT = parseInt(getArg('limit') || '0') || 0;
const CONCURRENCY = parseInt(getArg('concurrency') || '10') || 10;
const SAVE_INTERVAL = 500;

// Free email providers — skip these domains for pattern generation
const FREE_PROVIDERS = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
  'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
  'protonmail.com', 'proton.me', 'zoho.com', 'yandex.com',
  'mail.com', 'gmx.com', 'fastmail.com', 'comcast.net', 'att.net',
  'verizon.net', 'sbcglobal.net', 'cox.net', 'charter.net',
  'earthlink.net', 'optonline.net', 'bellsouth.net', 'mindspring.com',
]);

// Domains to skip (directories, social media, gov, edu)
const SKIP_DOMAINS = new Set([
  'yelp.com', 'bbb.org', 'facebook.com', 'linkedin.com', 'instagram.com',
  'twitter.com', 'x.com', 'youtube.com', 'yellowpages.com', 'pinterest.com',
  'tiktok.com', 'reddit.com', 'crunchbase.com', 'glassdoor.com',
  'indeed.com', 'amazon.com', 'google.com', 'apple.com',
  'wikipedia.org', 'avvo.com', 'justia.com', 'findlaw.com',
  'martindale.com', 'lawyers.com', 'nolo.com', 'lawinfo.com',
  'superlawyers.com', 'bestlawyers.com', 'aila.org', 'ailalawyer.com',
]);

// ─── CSV Parsing / Writing ───────────────────────────────────────────

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
  for (const skip of SKIP_DOMAINS) {
    if (domain === skip || domain.endsWith('.' + skip)) return true;
  }
  return false;
}

// ─── MX Record Check ─────────────────────────────────────────────────

const mxCache = new Map(); // domain -> true/false

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

// ─── DNS A Record Check (for domain discovery) ──────────────────────

function checkDNS(domain) {
  return new Promise((resolve) => {
    dns.resolve(domain, (err) => {
      resolve(!err);
    });
  });
}

// ─── Domain Discovery from Firm Name ─────────────────────────────────

function slugifyFirm(firmName) {
  // Strip common legal suffixes
  let cleaned = firmName.toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|plc|lp|llp|pllc|pc|pa|co|company|group|limited|incorporated|corporation|associates|assoc|professional|prof)\b\.?/gi, '')
    .replace(/\b(law\s*(offices?|firm|group|center|centre|practice|studio)s?\s*(of|at)?)\b/gi, '')
    .replace(/\b(attorneys?\s*at\s*law)\b/gi, '')
    .replace(/\b(immigration)\b/gi, '')
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

  // Also try raw firm name slug (without stripping "law")
  let rawCleaned = firmName.toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|plc|lp|llp|pllc|pc|pa|co|company|group|limited|incorporated|corporation)\b\.?/gi, '')
    .replace(/[&+]/g, 'and')
    .replace(/[''`]/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .trim();
  const rawSlug = rawCleaned.replace(/\s+/g, '');

  // Build candidate list
  const candidates = new Set([
    slug + '.com',
    slugDash + '.com',
    rawSlug + '.com',
    slug + '.net',
    slug + '.law',
    slugDash + '.law',
    rawSlug + '.net',
    slug + '.org',
  ]);

  for (const candidate of candidates) {
    if (candidate.length < 6) continue; // skip tiny domains like "a.com"
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
  // Strip middle initials: "Jennifer L." -> "Jennifer", "Lloyd E." -> "Lloyd"
  // Also handles "Danielle L.C." -> "Danielle", "Cora Denise" -> "Cora"
  let name = raw.trim();
  // Remove trailing single-letter middle initials (with or without period)
  // e.g., "Jennifer L." -> "Jennifer", "Amy R." -> "Amy"
  name = name.replace(/\s+[A-Z]\.?(\s+[A-Z]\.?)*\s*$/i, '');
  // If the name has multiple words, take only the first (handles "Cora Denise", "Diane Elizabeth McHugh")
  const parts = name.trim().split(/\s+/);
  return parts[0] || name;
}

function generatePatterns(firstName, lastName, domain) {
  if (!firstName || !lastName || !domain) return [];
  if (FREE_PROVIDERS.has(domain.toLowerCase())) return [];

  // Clean name parts — strip middle initials, take first name only
  const cleanedFirst = cleanFirstName(firstName);
  const f = cleanedFirst.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return [];

  const fi = f[0]; // first initial
  const d = domain.toLowerCase();

  return [
    `${f}.${l}@${d}`,       // jennifer.brill@domain.com  (most common for law firms)
    `${fi}${l}@${d}`,       // jbrill@domain.com
    `${f}${l}@${d}`,        // jenniferbrill@domain.com
    `${f}@${d}`,            // jennifer@domain.com
    `${fi}.${l}@${d}`,      // j.brill@domain.com
    `${l}@${d}`,            // brill@domain.com
  ];
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

// ─── Resume Support ──────────────────────────────────────────────────

function loadExistingEnriched(filePath) {
  const enriched = new Map();
  if (!fs.existsSync(filePath)) return enriched;

  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const { rows } = parseCSV(content);
    for (const row of rows) {
      if (row.email) {
        const key = `${(row.first_name||'').toLowerCase()}|${(row.last_name||'').toLowerCase()}|${(row.firm_name||'').toLowerCase()}`;
        enriched.set(key, {
          email: row.email,
          email_source: row.email_source || '',
          email_pattern: row.email_pattern || '',
        });
      }
    }
    console.log(`  Resume: loaded ${enriched.size} previously enriched leads`);
  } catch (err) {
    console.log(`  Resume: could not load existing file (${err.message})`);
  }

  return enriched;
}

// ─── Main Pipeline ───────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('');
  console.log('==========================================================');
  console.log('  MORTAR -- AILA Immigration Lawyer Email Enricher');
  console.log('==========================================================');
  console.log('');

  // ── Validate input ──

  if (!fs.existsSync(INPUT)) {
    console.error(`File not found: ${INPUT}`);
    process.exit(1);
  }

  // ── Load and parse CSV ──

  const rawContent = fs.readFileSync(INPUT, 'utf8');
  const { headers, rows } = parseCSV(rawContent);

  if (rows.length === 0) {
    console.error('No data rows found in CSV');
    process.exit(1);
  }

  console.log(`  Input:        ${path.basename(INPUT)}`);
  console.log(`  Rows:         ${rows.length}`);
  console.log(`  Columns:      ${headers.join(', ')}`);
  console.log(`  Concurrency:  ${CONCURRENCY}`);
  console.log(`  Method:       MX verification + pattern generation (no SMTP)`);
  console.log('');

  // ── Ensure output headers include enrichment columns ──

  const outputHeaders = [...headers];
  if (!outputHeaders.includes('email_source')) outputHeaders.push('email_source');
  if (!outputHeaders.includes('email_pattern')) outputHeaders.push('email_pattern');

  // ── Apply limit ──

  let leads = rows;
  if (LIMIT > 0 && leads.length > LIMIT) {
    leads = leads.slice(0, LIMIT);
    console.log(`  Limit:        processing first ${LIMIT} of ${rows.length}`);
  }

  // ── Resume support ──

  const existingEnriched = loadExistingEnriched(OUTPUT);
  let skippedResume = 0;

  for (const lead of leads) {
    if (lead.email) continue; // already has email in source data
    const key = `${(lead.first_name||'').toLowerCase()}|${(lead.last_name||'').toLowerCase()}|${(lead.firm_name||'').toLowerCase()}`;
    const existing = existingEnriched.get(key);
    if (existing && existing.email) {
      lead.email = existing.email;
      lead.email_source = existing.email_source || 'resumed';
      lead.email_pattern = existing.email_pattern || '';
      skippedResume++;
    }
  }

  if (skippedResume > 0) {
    console.log(`  Resumed:      ${skippedResume} leads from previous run`);
  }

  // ── Pre-analysis ──

  const alreadyHaveEmail = leads.filter(l => l.email).length;
  const hasDomain = leads.filter(l => l.domain && l.domain.trim()).length;
  const hasWebsite = leads.filter(l => l.website && l.website.trim()).length;
  const needsProcessing = leads.filter(l => !l.email);

  console.log(`  Already have email: ${alreadyHaveEmail}`);
  console.log(`  Has domain:         ${hasDomain} (${(hasDomain/leads.length*100).toFixed(1)}%)`);
  console.log(`  Has website:        ${hasWebsite} (${(hasWebsite/leads.length*100).toFixed(1)}%)`);
  console.log(`  Need enrichment:    ${needsProcessing.length}`);
  console.log('');

  if (needsProcessing.length === 0) {
    console.log('  All leads already have emails. Nothing to do.');
    writeCSV(OUTPUT, outputHeaders, leads);
    console.log(`  Output: ${OUTPUT}`);
    return;
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 1: Domain resolution and MX verification for existing domains
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 1: MX Verification of Existing Domains ─────────');
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

  // Build work items: resolve domain for each lead, check MX
  const workItems = [];

  for (const lead of needsProcessing) {
    // Get domain — prefer existing domain column, fall back to extracting from website
    let domain = (lead.domain || '').trim().toLowerCase();
    if (!domain && lead.website) {
      domain = extractDomain(lead.website);
    }

    // Clean domain
    if (domain && domain.startsWith('www.')) domain = domain.substring(4);

    workItems.push({
      lead,
      domain: domain || null,
      firstName: (lead.first_name || '').trim(),
      lastName: (lead.last_name || '').trim(),
      firmName: (lead.firm_name || '').trim(),
    });
  }

  // Separate into: has domain vs needs discovery
  const withDomain = workItems.filter(w => w.domain && !isSkippedDomain(w.domain) && !FREE_PROVIDERS.has(w.domain));
  const needsDomainDiscovery = workItems.filter(w => !w.domain || isSkippedDomain(w.domain) || FREE_PROVIDERS.has(w.domain));
  const freeProviderLeads = workItems.filter(w => w.domain && FREE_PROVIDERS.has(w.domain));

  stats.skippedFreeProvider = freeProviderLeads.length;

  console.log(`  With usable domain:   ${withDomain.length}`);
  console.log(`  Free provider domain: ${freeProviderLeads.length} (skipped)`);
  console.log(`  Need domain discovery: ${needsDomainDiscovery.length - freeProviderLeads.length}`);
  console.log('');

  // ── Step 1a: MX verify existing domains ──

  let mxChecked = 0;
  const mxVerifiedDomains = new Set();

  console.log('  Checking MX records...');

  await parallelForEach(withDomain, async (item) => {
    const hasMx = await checkMX(item.domain);
    mxChecked++;

    if (hasMx) {
      stats.mxVerified++;
      mxVerifiedDomains.add(item.domain);
    } else {
      stats.mxFailed++;
    }

    if (mxChecked % 100 === 0) {
      console.log(`  [${mxChecked}/${withDomain.length}] MX verified: ${stats.mxVerified} | no MX: ${stats.mxFailed}`);
    }
  }, CONCURRENCY);

  console.log(`  MX check complete: ${stats.mxVerified} verified, ${stats.mxFailed} failed`);
  console.log(`  Unique domains with MX: ${mxVerifiedDomains.size}`);
  console.log('');

  // ═══════════════════════════════════════════════════════════════════
  // Step 2: Domain discovery for leads missing a domain
  // ═══════════════════════════════════════════════════════════════════

  const discoveryItems = needsDomainDiscovery.filter(w => w.firmName && !FREE_PROVIDERS.has(w.domain || ''));

  if (discoveryItems.length > 0) {
    console.log('--- Step 2: Domain Discovery for Missing Domains ──────────');
    console.log('');
    console.log(`  Attempting DNS-based domain discovery for ${discoveryItems.length} leads...`);
    console.log('');

    // Deduplicate by firm name to avoid redundant lookups
    const firmDomainCache = new Map(); // firmKey -> domain|null
    let discoveryChecked = 0;

    await parallelForEach(discoveryItems, async (item) => {
      const firmKey = item.firmName.toLowerCase().replace(/[^a-z0-9]/g, '');

      if (firmDomainCache.has(firmKey)) {
        const cached = firmDomainCache.get(firmKey);
        if (cached) {
          item.domain = cached;
          stats.domainsDiscovered++;
        }
      } else {
        const domain = await discoverDomain(item.firmName);
        firmDomainCache.set(firmKey, domain);
        if (domain) {
          item.domain = domain;
          item.lead.domain = domain;
          stats.domainsDiscovered++;
          mxVerifiedDomains.add(domain); // already MX-verified during discovery
        } else {
          stats.domainDiscoveryFailed++;
        }
      }

      discoveryChecked++;
      if (discoveryChecked % 50 === 0) {
        console.log(`  [${discoveryChecked}/${discoveryItems.length}] discovered: ${stats.domainsDiscovered} | failed: ${stats.domainDiscoveryFailed}`);
      }
    }, CONCURRENCY);

    console.log(`  Domain discovery complete: ${stats.domainsDiscovered} found, ${stats.domainDiscoveryFailed} failed`);
    console.log('');
  }

  // ═══════════════════════════════════════════════════════════════════
  // Step 3: Email pattern generation
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 3: Email Pattern Generation ────────────────────');
  console.log('');

  let generated = 0;
  let lastSave = 0;

  for (const item of workItems) {
    const { lead, firstName, lastName } = item;
    let domain = (item.domain || '').trim().toLowerCase();

    if (!domain || isSkippedDomain(domain) || FREE_PROVIDERS.has(domain)) {
      if (!domain) stats.skippedNoDomain++;
      continue;
    }

    // Only generate email if domain has MX records
    if (!mxVerifiedDomains.has(domain)) {
      // Might have been discovered in step 2 and already verified
      const hasMx = await checkMX(domain);
      if (!hasMx) continue;
      mxVerifiedDomains.add(domain);
    }

    if (!firstName || !lastName) {
      stats.skippedNoName++;
      continue;
    }

    const patterns = generatePatterns(firstName, lastName, domain);
    if (patterns.length === 0) continue;

    // Use the most common law firm pattern: firstname.lastname@domain
    lead.email = patterns[0];
    lead.email_source = 'mx-verified-pattern';
    lead.email_pattern = 'firstname.lastname';
    lead.domain = domain;

    stats.emailsGenerated++;
    generated++;

    if (generated % 100 === 0) {
      console.log(`  [${generated}] emails generated so far...`);
    }

    // Periodic save
    if (generated - lastSave >= SAVE_INTERVAL) {
      lastSave = generated;
      writeCSV(OUTPUT, outputHeaders, leads);
      console.log(`  >> Saved progress (${generated} emails) to ${path.basename(OUTPUT)}`);
    }
  }

  console.log(`  Pattern generation complete: ${stats.emailsGenerated} emails`);
  console.log('');

  // ═══════════════════════════════════════════════════════════════════
  // Step 4: Write Final CSV
  // ═══════════════════════════════════════════════════════════════════

  console.log('--- Step 4: Writing Enriched CSV ────────────────────────');
  console.log('');

  writeCSV(OUTPUT, outputHeaders, leads);

  const elapsed = Date.now() - startTime;
  const totalEmails = leads.filter(l => l.email).length;
  const totalDomains = leads.filter(l => l.domain && l.domain.trim()).length;

  console.log('==========================================================');
  console.log('  SUMMARY');
  console.log('==========================================================');
  console.log('');
  console.log(`  Total leads:             ${leads.length}`);
  console.log(`  Emails generated:        ${totalEmails} (${(totalEmails / leads.length * 100).toFixed(1)}%)`);
  console.log(`    - Previously had:      ${alreadyHaveEmail}`);
  console.log(`    - Resumed from prior:  ${skippedResume}`);
  console.log(`    - Newly generated:     ${stats.emailsGenerated}`);
  console.log('');
  console.log(`  Domains total:           ${totalDomains}`);
  console.log(`    - From CSV:            ${hasDomain}`);
  console.log(`    - Discovered via DNS:  ${stats.domainsDiscovered}`);
  console.log('');
  console.log('  MX Verification:');
  console.log(`    MX verified:           ${stats.mxVerified}`);
  console.log(`    No MX records:         ${stats.mxFailed}`);
  console.log(`    Unique MX domains:     ${mxVerifiedDomains.size}`);
  console.log('');
  console.log('  Skipped:');
  console.log(`    Free provider domain:  ${stats.skippedFreeProvider}`);
  console.log(`    No domain available:   ${stats.skippedNoDomain}`);
  console.log(`    No name available:     ${stats.skippedNoName}`);
  console.log('');
  console.log(`  Output:  ${OUTPUT}`);
  console.log(`  Time:    ${formatTime(elapsed)}`);
  console.log('');
}

// ─── Run ─────────────────────────────────────────────────────────────

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
