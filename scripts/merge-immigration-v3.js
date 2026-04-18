#!/usr/bin/env node
/**
 * v3 merge: add apac-immigration-agents, bulk-leads-immigration-crawled,
 * canada-immigration-lawyers-cila to master v2.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v2.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v3.csv');

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i+1] === '"') { cur += '"'; i++; }
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
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {}; headers.forEach((h, idx) => rec[h] = cols[idx] || '');
    rows.push(rec);
  }
  return { headers, rows };
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

const normEmail = e => (e || '').trim().toLowerCase();

function main() {
  console.log('=== Merging v3: +apac +cila +bulk-crawled ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v2: ${master.rows.length} rows`);

  const masterEmails = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) masterEmails.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  // apac-immigration-agents
  const apac = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'apac-immigration-agents.csv'), 'utf-8'));
  let apacAdded = 0;
  for (const r of apac.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@') || masterEmails.has(e)) continue;
    masterEmails.add(e);
    allNew.push({
      first_name: r.first_name, last_name: r.last_name, email: e,
      phone: r.phone, firm_name: r.company, website: '', city: r.city,
      state: r.state, country: r.country, niche: 'immigration agent',
      source: r.source || 'apac_immigration',
    });
    apacAdded++;
  }
  console.log(`APAC: +${apacAdded} new`);

  // bulk-leads-immigration-crawled
  const bulkCrawled = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'bulk-leads-immigration-crawled.csv'), 'utf-8'));
  let bulkAdded = 0;
  for (const r of bulkCrawled.rows) {
    const e = normEmail(r.primary_email || r.email);
    if (!e || !e.includes('@') || masterEmails.has(e)) continue;
    masterEmails.add(e);
    allNew.push({
      first_name: r.first_name, last_name: r.last_name, email: e,
      phone: r.phone, firm_name: r.firm_name, website: r.website, domain: r.domain,
      city: r.city, state: r.state, country: r.country || 'US',
      niche: 'immigration consultant', source: 'bulk_discovery_crawled',
    });
    bulkAdded++;
  }
  console.log(`Bulk crawled: +${bulkAdded} new`);

  // canada-immigration-lawyers-cila
  const cila = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'canada-immigration-lawyers-cila.csv'), 'utf-8'));
  let cilaAdded = 0;
  for (const r of cila.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@') || masterEmails.has(e)) continue;
    masterEmails.add(e);
    allNew.push({
      first_name: r.first_name, last_name: r.last_name, email: e,
      phone: r.phone, firm_name: r.firm_name, website: r.website,
      city: r.city, state: r.province || r.state, country: 'Canada',
      niche: 'immigration lawyer', source: 'cila_canada',
    });
    cilaAdded++;
  }
  console.log(`CILA: +${cilaAdded} new`);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v3: ${total} rows, ${masterEmails.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
