#!/usr/bin/env node
/**
 * v13: + LawyerLegion crawl (729 emails) + DOJ additional enrichment
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v12.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v13.csv');

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
  console.log('=== Merging v13 ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v12: ${master.rows.length} rows`);

  const seen = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) seen.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  // LawyerLegion crawl
  const llPath = path.join(OUT_DIR, 'lawyerlegion-crawled-emails.csv');
  const ll = parseCsv(fs.readFileSync(llPath, 'utf-8'));
  let llAdded = 0;
  for (const r of ll.rows) {
    const e = normEmail(r.primary_email);
    if (!e || !e.includes('@') || seen.has(e)) continue;
    seen.add(e);
    allNew.push({
      first_name: r.first_name || '',
      last_name: r.last_name || '',
      email: e,
      phone: '',
      firm_name: r.firm_name || '',
      website: r.website || '',
      domain: r.domain || '',
      city: r.city || '',
      state: r.state || '',
      country: 'US',
      niche: 'immigration attorney',
      source: 'lawyerlegion_crawled',
    });
    llAdded++;
    // Also add additional emails from crawled_emails column
    const extras = (r.crawled_emails || '').split(/[;,]\s*/);
    for (const ex of extras) {
      const e2 = normEmail(ex);
      if (!e2 || !e2.includes('@') || seen.has(e2)) continue;
      seen.add(e2);
      allNew.push({
        first_name: '', last_name: '', email: e2, phone: '',
        firm_name: r.firm_name || '', website: r.website || '', domain: r.domain || '',
        city: r.city || '', state: r.state || '', country: 'US',
        niche: 'immigration law firm', source: 'lawyerlegion_crawled_extra',
      });
      llAdded++;
    }
  }
  console.log(`  LawyerLegion crawl: +${llAdded} new`);

  // DOJ enriched (latest)
  const dojPath = path.join(OUT_DIR, 'doj-reps-enriched.csv');
  const doj = parseCsv(fs.readFileSync(dojPath, 'utf-8'));
  let dojAdded = 0;
  for (const r of doj.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@') || seen.has(e)) continue;
    seen.add(e);
    allNew.push({
      first_name: r.first_name || '',
      last_name: r.last_name || '',
      email: e,
      phone: r.phone || '',
      firm_name: r.firm_name || '',
      website: r.website || '',
      domain: r.domain || '',
      city: r.city || '',
      state: r.state || '',
      country: 'US',
      niche: 'immigration nonprofit',
      source: 'doj_enriched',
    });
    dojAdded++;
  }
  console.log(`  DOJ enriched: +${dojAdded} new`);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v13: ${total} rows, ${seen.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
