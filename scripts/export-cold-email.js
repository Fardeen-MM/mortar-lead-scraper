#!/usr/bin/env node
/**
 * export-cold-email.js — Export enriched leads from SQLite to CSV files
 *
 * Outputs multiple well-named CSV files into exports/ directory:
 *   - decision-makers-with-email.csv
 *   - decision-makers-all.csv
 *   - all-lawyers-us-east.csv
 *   - all-lawyers-us-west.csv
 *   - all-lawyers-us-central.csv
 *   - all-lawyers-canada.csv
 *   - all-lawyers-uk.csv
 *   - all-lawyers-australia.csv
 *   - all-lawyers-europe-asia.csv
 *   - all-with-email.csv
 *
 * Usage: node scripts/export-cold-email.js
 */

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DB_PATH = process.env.LEAD_DB_PATH || path.join(__dirname, '..', 'data', 'leads.db');
const EXPORTS_DIR = path.join(__dirname, '..', 'exports');

// ── Bad statuses to exclude ──────────────────────────────────────────
const BAD_STATUSES = [
  'deceased', 'suspended', 'disbarred', 'revoked', 'resigned',
  'inactive', 'retired', 'disabled', 'surrendered'
];

// ── Region mappings ──────────────────────────────────────────────────
const US_EAST = new Set([
  'NY', 'FL', 'GA', 'PA', 'NC', 'SC', 'VA', 'MD', 'DE', 'CT',
  'ME', 'MA', 'NH', 'VT', 'RI', 'NJ', 'DC', 'WV'
]);

const US_WEST = new Set([
  'CA', 'WA', 'OR', 'CO', 'AZ', 'HI', 'AK', 'ID', 'NM', 'NV',
  'UT', 'MT', 'WY'
]);

const US_CENTRAL = new Set([
  'TX', 'IL', 'OH', 'MN', 'MO', 'MI', 'KY', 'TN', 'IN', 'WI',
  'IA', 'AR', 'MS', 'AL', 'LA', 'NE', 'KS', 'ND', 'SD', 'OK'
]);

const EUROPE_ASIA = new Set(['FR', 'IE', 'IT', 'HK', 'NZ', 'SG']);

// ── CSV helpers ──────────────────────────────────────────────────────
const CSV_COLUMNS = [
  'first_name', 'last_name', 'email', 'firm_name', 'title', 'phone',
  'website', 'city', 'state', 'country', 'practice_area', 'bar_status',
  'admission_date', 'linkedin_url', 'decision_maker_score', 'lead_score',
  'primary_source', 'email_source'
];

const CSV_HEADER = CSV_COLUMNS.join(',');

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function leadToCsvRow(lead) {
  return CSV_COLUMNS.map(col => {
    if (col === 'decision_maker_score') return csvEscape(lead.icp_score);
    if (col === 'country') return csvEscape(deriveCountryName(lead.state));
    return csvEscape(lead[col]);
  }).join(',');
}

// ── Country name from state code ─────────────────────────────────────
function deriveCountryName(state) {
  if (!state) return 'United States';
  if (state.startsWith('CA-')) return 'Canada';
  if (state.startsWith('UK-')) return 'United Kingdom';
  if (state.startsWith('AU-')) return 'Australia';
  if (['FR', 'IE', 'IT'].includes(state)) return 'Europe';
  if (['HK', 'NZ', 'SG'].includes(state)) return 'Asia-Pacific';
  return 'United States';
}

// ── Max file size before splitting (50 MB) ───────────────────────────
const MAX_FILE_SIZE = 50 * 1024 * 1024;

// ── Main ─────────────────────────────────────────────────────────────
function main() {
  console.log('=== Cold Email Export ===\n');

  // Ensure exports dir exists
  if (!fs.existsSync(EXPORTS_DIR)) {
    fs.mkdirSync(EXPORTS_DIR, { recursive: true });
  }

  const db = new Database(DB_PATH, { readonly: true });

  // Build the WHERE clause to exclude bad statuses
  // We keep leads with NULL/blank bar_status, plus any status that does NOT
  // contain any of the bad keywords (case-insensitive)
  const badStatusClauses = BAD_STATUSES.map(s => `LOWER(bar_status) NOT LIKE '%${s}%'`).join(' AND ');
  const statusFilter = `(bar_status IS NULL OR bar_status = '' OR (${badStatusClauses}))`;

  // Fetch ALL valid leads once, then partition in JS
  const allLeads = db.prepare(`
    SELECT
      id, first_name, last_name, email, firm_name, title, phone,
      website, city, state, country, practice_area, bar_status,
      admission_date, linkedin_url, icp_score, lead_score,
      primary_source, email_source
    FROM leads
    WHERE ${statusFilter}
    ORDER BY icp_score DESC, lead_score DESC
  `).all();

  console.log(`Loaded ${allLeads.length} valid leads (after filtering bad statuses)\n`);

  // Track unique lead IDs across all files
  const allExportedIds = new Set();
  let totalRowsExported = 0;
  const fileSummaries = [];

  // ── Helper: write a CSV file (with optional splitting) ─────────
  function writeCsv(filename, leads, sortFn) {
    if (sortFn) leads.sort(sortFn);

    const rows = leads.map(leadToCsvRow);
    const content = CSV_HEADER + '\n' + rows.join('\n') + '\n';
    const filePath = path.join(EXPORTS_DIR, filename);

    // Check if we need to split
    const sizeBytes = Buffer.byteLength(content, 'utf8');
    if (sizeBytes > MAX_FILE_SIZE) {
      console.log(`  [!] ${filename} would be ${formatSize(sizeBytes)} — splitting...`);
      return splitAndWrite(filename, leads, sortFn);
    }

    fs.writeFileSync(filePath, content, 'utf8');

    const emailCount = leads.filter(l => l.email && l.email.trim()).length;
    const phoneCount = leads.filter(l => l.phone && l.phone.trim()).length;

    leads.forEach(l => allExportedIds.add(l.id));
    totalRowsExported += leads.length;

    const summary = {
      file: filename,
      rows: leads.length,
      size: sizeBytes,
      emails: emailCount,
      phones: phoneCount
    };
    fileSummaries.push(summary);

    console.log(`  ${filename}`);
    console.log(`    Rows: ${leads.length.toLocaleString()}`);
    console.log(`    Size: ${formatSize(sizeBytes)}`);
    console.log(`    Email: ${emailCount.toLocaleString()} | Phone: ${phoneCount.toLocaleString()}`);
    console.log('');

    return [summary];
  }

  // ── Helper: split a large file by state ────────────────────────
  function splitAndWrite(filename, leads, sortFn) {
    const base = filename.replace('.csv', '');
    const byState = {};
    for (const lead of leads) {
      const st = lead.state || 'unknown';
      if (!byState[st]) byState[st] = [];
      byState[st].push(lead);
    }

    const summaries = [];
    // Sort states by lead count descending
    const states = Object.keys(byState).sort((a, b) => byState[b].length - byState[a].length);

    for (const st of states) {
      const stateLeads = byState[st];
      if (sortFn) stateLeads.sort(sortFn);

      const splitFilename = `${base}-${st.toLowerCase()}.csv`;
      const rows = stateLeads.map(leadToCsvRow);
      const content = CSV_HEADER + '\n' + rows.join('\n') + '\n';
      const filePath = path.join(EXPORTS_DIR, splitFilename);
      const sizeBytes = Buffer.byteLength(content, 'utf8');

      fs.writeFileSync(filePath, content, 'utf8');

      const emailCount = stateLeads.filter(l => l.email && l.email.trim()).length;
      const phoneCount = stateLeads.filter(l => l.phone && l.phone.trim()).length;

      stateLeads.forEach(l => allExportedIds.add(l.id));
      totalRowsExported += stateLeads.length;

      const summary = {
        file: splitFilename,
        rows: stateLeads.length,
        size: sizeBytes,
        emails: emailCount,
        phones: phoneCount
      };
      fileSummaries.push(summary);
      summaries.push(summary);

      console.log(`  ${splitFilename}`);
      console.log(`    Rows: ${stateLeads.length.toLocaleString()}`);
      console.log(`    Size: ${formatSize(sizeBytes)}`);
      console.log(`    Email: ${emailCount.toLocaleString()} | Phone: ${phoneCount.toLocaleString()}`);
      console.log('');
    }

    return summaries;
  }

  function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  }

  // ── Sort helpers ───────────────────────────────────────────────
  const sortByIcpThenLead = (a, b) => (b.icp_score - a.icp_score) || (b.lead_score - a.lead_score);
  const sortByIcp = (a, b) => b.icp_score - a.icp_score;

  // ── 1. Decision makers with email ──────────────────────────────
  console.log('--- File 1: Decision Makers with Email ---');
  const dmWithEmail = allLeads.filter(l => l.icp_score >= 50 && l.email && l.email.trim());
  writeCsv('decision-makers-with-email.csv', dmWithEmail, sortByIcpThenLead);

  // ── 2. Decision makers all ─────────────────────────────────────
  console.log('--- File 2: Decision Makers All ---');
  const dmAll = allLeads.filter(l => l.icp_score >= 50);
  writeCsv('decision-makers-all.csv', dmAll, sortByIcp);

  // ── 3. US East ─────────────────────────────────────────────────
  console.log('--- File 3: US East ---');
  const usEast = allLeads.filter(l => US_EAST.has(l.state));
  writeCsv('all-lawyers-us-east.csv', usEast, sortByIcpThenLead);

  // ── 4. US West ─────────────────────────────────────────────────
  console.log('--- File 4: US West ---');
  const usWest = allLeads.filter(l => US_WEST.has(l.state));
  writeCsv('all-lawyers-us-west.csv', usWest, sortByIcpThenLead);

  // ── 5. US Central ──────────────────────────────────────────────
  console.log('--- File 5: US Central ---');
  const usCentral = allLeads.filter(l => US_CENTRAL.has(l.state));
  writeCsv('all-lawyers-us-central.csv', usCentral, sortByIcpThenLead);

  // ── 6. Canada ──────────────────────────────────────────────────
  console.log('--- File 6: Canada ---');
  const canada = allLeads.filter(l => l.state && l.state.startsWith('CA-'));
  writeCsv('all-lawyers-canada.csv', canada, sortByIcpThenLead);

  // ── 7. UK ──────────────────────────────────────────────────────
  console.log('--- File 7: UK ---');
  const uk = allLeads.filter(l => l.state && l.state.startsWith('UK-'));
  writeCsv('all-lawyers-uk.csv', uk, sortByIcpThenLead);

  // ── 8. Australia ───────────────────────────────────────────────
  console.log('--- File 8: Australia ---');
  const australia = allLeads.filter(l => l.state && l.state.startsWith('AU-'));
  writeCsv('all-lawyers-australia.csv', australia, sortByIcpThenLead);

  // ── 9. Europe + Asia-Pacific ───────────────────────────────────
  console.log('--- File 9: Europe + Asia-Pacific ---');
  const europeAsia = allLeads.filter(l => EUROPE_ASIA.has(l.state));
  writeCsv('all-lawyers-europe-asia.csv', europeAsia, sortByIcpThenLead);

  // ── 10. All with email ─────────────────────────────────────────
  console.log('--- File 10: All with Email ---');
  const allWithEmail = allLeads.filter(l => l.email && l.email.trim());
  writeCsv('all-with-email.csv', allWithEmail, sortByIcp);

  db.close();

  // ── Final summary ──────────────────────────────────────────────
  console.log('='.repeat(60));
  console.log('FINAL SUMMARY');
  console.log('='.repeat(60));
  console.log(`Total rows exported across all files: ${totalRowsExported.toLocaleString()}`);
  console.log(`Total unique leads: ${allExportedIds.size.toLocaleString()}`);
  console.log('');
  console.log('Files created:');
  for (const s of fileSummaries) {
    console.log(`  ${s.file.padEnd(42)} ${String(s.rows).padStart(6)} rows   ${formatSize(s.size).padStart(10)}   ${s.emails} email / ${s.phones} phone`);
  }
  console.log('');
  const totalSize = fileSummaries.reduce((acc, s) => acc + s.size, 0);
  console.log(`Total disk usage: ${formatSize(totalSize)}`);
  console.log(`Files: ${fileSummaries.length}`);
  console.log('\nDone.');
}

main();
