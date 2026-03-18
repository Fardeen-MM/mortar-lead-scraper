#!/usr/bin/env node
/**
 * FAST Email Enrichment — DNS domain guess + SMTP verify
 *
 * Skips DuckDuckGo (slow) and Common Crawl (unreliable).
 * Instead: guess domain from firm name → SMTP verify info@domain
 *
 * Speed: ~12 leads/min/worker → 3,600/hour with 5 workers
 *
 * Usage:
 *   node scripts/fast-email-enrich.js output/some-file.csv --limit 1000 --concurrency 5
 */

const fs = require('fs');
const path = require('path');
const dns = require('dns').promises;
const { log } = require('../lib/logger');

// ─── CLI Args ────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const INPUT_FILE = args.find(a => !a.startsWith('--'));
const LIMIT = parseInt((args[args.indexOf('--limit') + 1]) || '0', 10) || 0;
const CONCURRENCY = parseInt((args[args.indexOf('--concurrency') + 1]) || '5', 10) || 5;
const OUTPUT_FILE = INPUT_FILE ? path.join(path.dirname(INPUT_FILE), 'fast-enriched-' + path.basename(INPUT_FILE)) : '';

if (!INPUT_FILE) {
  console.log('Usage: node scripts/fast-email-enrich.js <csv> [--limit N] [--concurrency N]');
  process.exit(0);
}

// ─── Domain guessing ─────────────────────────────────────────────────────────

function guessDomains(firmName) {
  const clean = firmName.toLowerCase()
    .replace(/[''`]/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+(inc|llc|ltd|corp|pllc|pc|pa|dds|dmd|md|do|dc|od|associates|group|practice|clinic|office|center|centre)\s*$/i, '')
    .trim();
  if (!clean || clean.length < 3) return [];
  const words = clean.split(/\s+/).filter(Boolean);
  if (words.length === 0) return [];

  const joined = words.join('');
  const dashed = words.join('-');
  const initials = words.map(w => w[0]).join('');

  const guesses = [
    joined + '.com',
    dashed + '.com',
  ];

  if (words.length >= 2) {
    guesses.push(words[0] + words[words.length - 1] + '.com');
  }
  if (words.length >= 3) {
    guesses.push(words.slice(0, 2).join('') + '.com');
  }
  // Canadian
  guesses.push(joined + '.ca');

  return [...new Set(guesses)];
}

async function findDomain(firmName) {
  const domains = guessDomains(firmName);
  for (const d of domains) {
    try {
      await dns.resolve4(d);
      return d;
    } catch { /* not found */ }
  }
  return null;
}

// ─── SMTP email check ────────────────────────────────────────────────────────

const net = require('net');

async function getMX(domain) {
  try {
    const records = await dns.resolveMx(domain);
    if (!records || records.length === 0) return null;
    records.sort((a, b) => a.priority - b.priority);
    return records[0].exchange;
  } catch { return null; }
}

function smtpCheck(email, mxHost, timeout = 8000) {
  return new Promise((resolve) => {
    const socket = net.createConnection(25, mxHost);
    let step = 0, resolved = false;

    const done = (result) => {
      if (resolved) return;
      resolved = true;
      socket.destroy();
      resolve(result);
    };

    socket.setTimeout(timeout);
    socket.on('timeout', () => done(false));
    socket.on('error', () => done(false));

    socket.on('data', (data) => {
      const line = data.toString();
      if (step === 0 && line.startsWith('220')) {
        step = 1;
        socket.write('EHLO check.local\r\n');
      } else if (step === 1 && (line.includes('250') || line.includes('220'))) {
        step = 2;
        socket.write(`MAIL FROM:<check@check.local>\r\n`);
      } else if (step === 2 && line.startsWith('250')) {
        step = 3;
        socket.write(`RCPT TO:<${email}>\r\n`);
      } else if (step === 3) {
        done(line.startsWith('250'));
      }
    });
  });
}

async function verifyEmail(email) {
  const domain = email.split('@')[1];
  const mx = await getMX(domain);
  if (!mx) return false;
  try {
    return await smtpCheck(email, mx);
  } catch { return false; }
}

// ─── CSV parsing ─────────────────────────────────────────────────────────────

function parseCSVLine(line) {
  const cols = [];
  let current = '', inQ = false;
  for (const ch of line) {
    if (ch === '"') { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { cols.push(current); current = ''; continue; }
    current += ch;
  }
  cols.push(current);
  return cols;
}

// ─── Worker ──────────────────────────────────────────────────────────────────

const domainCache = new Map(); // firm → domain
const emailCache = new Map();  // domain → email or null

async function enrichLead(lead, idx, total) {
  const firm = lead.firm_name || `${lead.first_name || ''} ${lead.last_name || ''} ${lead.trade_category || ''}`.trim();
  if (!firm) return null;

  const firmKey = firm.toLowerCase().replace(/[^a-z0-9]/g, '');

  // Check domain cache
  let domain = domainCache.get(firmKey);
  if (domain === undefined) {
    domain = await findDomain(firm);
    domainCache.set(firmKey, domain);
  }
  if (!domain) {
    process.stdout.write(`\r[${idx}/${total}] ${firm.slice(0,30).padEnd(30)} → no domain`);
    return null;
  }

  // Check email cache
  if (emailCache.has(domain)) {
    const cached = emailCache.get(domain);
    if (cached) {
      process.stdout.write(`\r[${idx}/${total}] ${firm.slice(0,30).padEnd(30)} → ${domain} → ${cached} (cached)\n`);
      return cached;
    }
    return null;
  }

  // Try common email patterns
  const patterns = ['info@' + domain, 'contact@' + domain, 'office@' + domain, 'hello@' + domain];

  // Add name-based if we have first/last
  if (lead.first_name && lead.last_name) {
    const f = lead.first_name.toLowerCase();
    const l = lead.last_name.toLowerCase();
    patterns.unshift(f + '.' + l + '@' + domain);
    patterns.unshift(f + '@' + domain);
  }

  for (const email of patterns) {
    const valid = await verifyEmail(email);
    if (valid) {
      emailCache.set(domain, email);
      process.stdout.write(`\r[${idx}/${total}] ${firm.slice(0,30).padEnd(30)} → ${domain} → ${email} ✓\n`);
      return email;
    }
  }

  emailCache.set(domain, null);
  process.stdout.write(`\r[${idx}/${total}] ${firm.slice(0,30).padEnd(30)} → ${domain} → no valid email\n`);
  return null;
}

// ─── Main ────────────────────────────────────────────────────────────────────

(async () => {
  console.log('========================================');
  console.log('  FAST Email Enrichment');
  console.log('========================================');
  console.log(`  Input:       ${INPUT_FILE}`);
  console.log(`  Output:      ${OUTPUT_FILE}`);
  console.log(`  Limit:       ${LIMIT || 'all'}`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Method:      DNS guess → SMTP verify`);
  console.log('========================================\n');

  // Read CSV
  const content = fs.readFileSync(INPUT_FILE, 'utf8');
  const lines = content.split('\n');
  const headers = parseCSVLine(lines[0]);

  const allLeads = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = parseCSVLine(lines[i]);
    const lead = {};
    headers.forEach((h, idx) => { lead[h.trim()] = (cols[idx] || '').trim(); });
    allLeads.push(lead);
  }

  // Filter to leads without email
  const toEnrich = allLeads.filter(l => !l.email || !l.email.includes('@'));
  const batch = LIMIT > 0 ? toEnrich.slice(0, LIMIT) : toEnrich;

  console.log(`Loaded ${allLeads.length} leads, ${batch.length} need email enrichment\n`);

  const startTime = Date.now();
  let found = 0, processed = 0;

  // Process with concurrency
  const workers = [];
  let nextIdx = 0;

  async function worker() {
    while (nextIdx < batch.length) {
      const idx = nextIdx++;
      const lead = batch[idx];
      const email = await enrichLead(lead, idx + 1, batch.length);
      if (email) {
        lead.email = email;
        lead.email_source = 'smtp-fast';
        found++;
      }
      processed++;
    }
  }

  for (let i = 0; i < Math.min(CONCURRENCY, batch.length); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  // Write output
  const emailIdx = headers.indexOf('email');
  const outLines = [lines[0]];

  // Add email_source column if not present
  let headerLine = lines[0];
  if (!headers.includes('email_source')) {
    headerLine += ',email_source';
  }

  const outRows = [headerLine];
  for (let i = 0; i < allLeads.length; i++) {
    const lead = allLeads[i];
    const cols = parseCSVLine(lines[i + 1] || '');
    if (lead.email && lead.email_source === 'smtp-fast') {
      cols[emailIdx] = lead.email;
    }
    let row = cols.map(v => {
      const s = v.replace(/"/g, '""');
      return s.includes(',') ? '"' + s + '"' : s;
    }).join(',');
    if (!headers.includes('email_source')) {
      row += ',' + (lead.email_source || '');
    }
    outRows.push(row);
  }

  fs.writeFileSync(OUTPUT_FILE, outRows.join('\n'));

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const rate = (processed / (elapsed / 60)).toFixed(1);

  console.log('\n\n========================================');
  console.log('  RESULTS');
  console.log('========================================');
  console.log(`  Processed:  ${processed}`);
  console.log(`  Emails:     ${found} (${(found/Math.max(processed,1)*100).toFixed(1)}%)`);
  console.log(`  Domains:    ${domainCache.size} checked`);
  console.log(`  Time:       ${elapsed}s (${rate} leads/min)`);
  console.log(`  Output:     ${OUTPUT_FILE}`);
  console.log('========================================');
})().catch(e => console.error(e.stack));
