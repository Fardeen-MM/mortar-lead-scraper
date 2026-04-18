#!/usr/bin/env node
/**
 * Export master-immigration-leads.csv into format ready for cold email tools.
 *
 * Outputs:
 *   - output/cold-email-ready.csv — has email, passes basic validation
 *   - output/cold-email-by-country/{country}.csv — split by country
 *   - output/cold-email-stats.txt — summary stats
 *
 * Validation:
 *   - valid email format (RFC 5322-ish regex)
 *   - not a role email noreply/no-reply
 *   - deduped by email
 *
 * Columns (Instantly/Smartlead compatible):
 *   email, first_name, last_name, company, phone, website,
 *   city, state, country, niche, source, notes
 *
 * Usage:
 *   node scripts/export-for-cold-email.js
 *   node scripts/export-for-cold-email.js --in output/master-immigration-leads.csv
 */

const fs = require('fs');
const path = require('path');

const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const s = a.replace(/^--/, '');
  if (s.includes('=')) { const [k, v] = s.split('='); args[k] = v; }
  else { const n = argv[i + 1]; if (n && !n.startsWith('--')) { args[s] = n; i++; } else args[s] = true; }
}

const INPUT_FILE = args.in || path.join(__dirname, '..', 'output', 'master-immigration-leads.csv');
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const READY_FILE = path.join(OUTPUT_DIR, 'cold-email-ready.csv');
const COUNTRY_DIR = path.join(OUTPUT_DIR, 'cold-email-by-country');
const STATS_FILE = path.join(OUTPUT_DIR, 'cold-email-stats.txt');

const EMAIL_RE = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
const BAD_PREFIXES = new Set([
  'noreply', 'no-reply', 'donotreply', 'do-not-reply',
  'postmaster', 'mailer-daemon', 'webmaster', 'abuse',
  'bounce', 'spam',
]);
const BAD_DOMAINS = new Set([
  'example.com', 'example.org', 'test.com', 'domain.com',
  'wixpress.com', 'sentry.io', 'googleapis.com',
]);

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') inQ = false;
        else cur += c;
      } else {
        if (c === ',') { out.push(cur); cur = ''; }
        else if (c === '"' && cur === '') inQ = true;
        else cur += c;
      }
    }
    out.push(cur);
    return out;
  }
  if (!lines.length) return { headers: [], rows: [] };
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {};
    headers.forEach((h, idx) => { rec[h] = cols[idx] || ''; });
    rows.push(rec);
  }
  return { headers, rows };
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function validEmail(email) {
  if (!email || !EMAIL_RE.test(email)) return false;
  const [local, domain] = email.split('@');
  if (BAD_PREFIXES.has(local.toLowerCase())) return false;
  if (BAD_DOMAINS.has(domain.toLowerCase())) return false;
  if (email.length > 100) return false;
  return true;
}

function writeCsv(file, cols, rows) {
  const lines = [cols.join(',')];
  for (const r of rows) lines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(file, lines.join('\n') + '\n');
}

// Normalize country codes → full names for grouping
const COUNTRY_NORMALIZE = {
  'CA': 'Canada', 'US': 'United States', 'USA': 'United States',
  'GB': 'United Kingdom', 'UK': 'United Kingdom', 'AU': 'Australia',
  'NZ': 'New Zealand', 'IE': 'Ireland',
};

function normalizeCountry(c) {
  if (!c) return 'Unknown';
  const up = c.toUpperCase();
  if (COUNTRY_NORMALIZE[up]) return COUNTRY_NORMALIZE[up];
  return c;
}

function main() {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`Input not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  console.log('=== Cold Email Export ===');
  console.log(`Input:  ${INPUT_FILE}`);

  const text = fs.readFileSync(INPUT_FILE, 'utf-8');
  const { rows } = parseCsv(text);
  console.log(`Total rows: ${rows.length}`);

  // Filter + dedup + normalize
  const seenEmails = new Set();
  const ready = [];
  const rejected = { no_email: 0, invalid: 0, dupe: 0 };

  for (const r of rows) {
    const email = (r.email || '').toLowerCase().trim();
    if (!email) { rejected.no_email++; continue; }
    if (!validEmail(email)) { rejected.invalid++; continue; }
    if (seenEmails.has(email)) { rejected.dupe++; continue; }
    seenEmails.add(email);

    ready.push({
      email,
      first_name: r.first_name || '',
      last_name: r.last_name || '',
      company: r.firm_name || '',
      phone: r.phone || '',
      website: r.website || '',
      city: r.city || '',
      state: r.state || '',
      country: normalizeCountry(r.country),
      niche: r.niche || '',
      source: r.source || '',
      notes: (r.notes || '').slice(0, 150),
    });
  }

  // Write main ready file
  const cols = ['email', 'first_name', 'last_name', 'company', 'phone', 'website',
                'city', 'state', 'country', 'niche', 'source', 'notes'];
  writeCsv(READY_FILE, cols, ready);

  // Write by-country files
  if (!fs.existsSync(COUNTRY_DIR)) fs.mkdirSync(COUNTRY_DIR, { recursive: true });
  const byCountry = {};
  for (const r of ready) {
    const c = r.country || 'Unknown';
    if (!byCountry[c]) byCountry[c] = [];
    byCountry[c].push(r);
  }
  for (const [country, list] of Object.entries(byCountry)) {
    const slug = country.toLowerCase().replace(/[^a-z0-9]+/g, '-');
    writeCsv(path.join(COUNTRY_DIR, `${slug}.csv`), cols, list);
  }

  // Stats
  const byNiche = {};
  const bySource = {};
  for (const r of ready) {
    byNiche[r.niche] = (byNiche[r.niche] || 0) + 1;
    bySource[r.source] = (bySource[r.source] || 0) + 1;
  }

  const statLines = [];
  statLines.push(`=== COLD EMAIL EXPORT STATS (generated ${new Date().toISOString()}) ===`);
  statLines.push(`Source file: ${INPUT_FILE}`);
  statLines.push(`Total input rows: ${rows.length}`);
  statLines.push(`Ready (valid+unique): ${ready.length}`);
  statLines.push(`  No email: ${rejected.no_email}`);
  statLines.push(`  Invalid format: ${rejected.invalid}`);
  statLines.push(`  Duplicates: ${rejected.dupe}`);
  statLines.push('');
  statLines.push('By country (sorted):');
  for (const [c, n] of Object.entries(byCountry).map(([c,v]) => [c, v.length]).sort((a,b) => b[1]-a[1])) {
    statLines.push(`  ${c}: ${n}`);
  }
  statLines.push('');
  statLines.push('By source:');
  for (const [s, n] of Object.entries(bySource).sort((a,b) => b[1]-a[1])) {
    statLines.push(`  ${s}: ${n}`);
  }
  statLines.push('');
  statLines.push('By niche:');
  for (const [n, c] of Object.entries(byNiche).sort((a,b) => b[1]-a[1])) {
    statLines.push(`  ${n}: ${c}`);
  }
  statLines.push('');
  statLines.push('Files written:');
  statLines.push(`  Main:    ${READY_FILE}`);
  statLines.push(`  By-country: ${COUNTRY_DIR}/`);
  statLines.push(`  Stats:   ${STATS_FILE}`);

  fs.writeFileSync(STATS_FILE, statLines.join('\n') + '\n');
  console.log(statLines.join('\n'));
}

main();
