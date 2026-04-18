#!/usr/bin/env node
/**
 * Merge MyConsultant.ca + Martindale crawl + Martindale waterfall into master.
 * Outputs: IMMIGRATION-FINAL-CLEAN-v2.csv
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v2.csv');

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

function normEmail(e) { return (e || '').trim().toLowerCase(); }
function normKey(r) {
  const e = normEmail(r.email);
  if (e && e.includes('@')) return 'e:' + e;
  const fn = (r.first_name || '').trim().toLowerCase();
  const ln = (r.last_name || '').trim().toLowerCase();
  const dom = (r.domain || r.website || '').toLowerCase().replace(/https?:\/\//, '').replace(/^www\./, '').split('/')[0];
  if (fn && ln && dom) return 'n:' + fn + '|' + ln + '|' + dom;
  return 'x:' + Math.random();
}

function main() {
  console.log('=== Merging immigration sources into master v2 ===');

  console.log('Loading existing master...');
  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`  ${master.rows.length} existing rows`);

  // Add MyConsultant
  const mc = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'myconsultant-ca-rcic.csv'), 'utf-8'));
  console.log(`  MyConsultant: ${mc.rows.length} rows`);

  // Add Martindale crawl (only rows with primary_email)
  const mart = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'martindale-crawled-emails.csv'), 'utf-8'));
  const martWithEmail = mart.rows.filter(r => r.primary_email);
  console.log(`  Martindale crawl: ${martWithEmail.length} rows with email`);

  // Add Martindale waterfall (all rows — both verified and not_found have name+firm)
  const waterfall = parseCsv(fs.readFileSync(path.join(OUT_DIR, 'martindale-immigration-emails.csv'), 'utf-8'));
  const waterfallWithEmail = waterfall.rows.filter(r => r.email);
  console.log(`  Martindale waterfall: ${waterfallWithEmail.length} rows with email`);

  // Canonical column set (superset of all sources)
  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  // Build seen set from existing master
  const seen = new Set();
  for (const r of master.rows) seen.add(normKey(r));

  const allNew = [];
  let mcAdded = 0, martAdded = 0, waterfallAdded = 0;

  for (const r of mc.rows) {
    const row = {
      first_name: r.first_name, last_name: r.last_name, email: r.email, phone: r.phone,
      firm_name: r.firm_name, website: r.website, city: '', state: '',
      country: 'Canada', profile_url: r.profile_url, niche: 'immigration consultant',
      source: 'myconsultant_ca', cicc_id: r.cicc_id,
      region: r.location,
    };
    const k = normKey(row);
    if (seen.has(k)) continue;
    seen.add(k);
    allNew.push(row);
    mcAdded++;
  }

  for (const r of martWithEmail) {
    const row = {
      first_name: r.first_name, last_name: r.last_name, email: r.primary_email,
      phone: '', firm_name: r.firm_name, website: r.website, domain: r.domain,
      city: r.city, state: r.state, country: r.country || 'US',
      niche: 'immigration attorney', source: 'martindale_crawled',
    };
    const k = normKey(row);
    if (seen.has(k)) continue;
    seen.add(k);
    allNew.push(row);
    martAdded++;
  }

  for (const r of waterfallWithEmail) {
    const row = {
      first_name: r.first_name, last_name: r.last_name, email: r.email,
      phone: r.phone, firm_name: r.firm_name, website: r.website, domain: r.domain,
      city: r.city, state: r.state, country: r.country || 'US',
      profile_url: r.profile_url, niche: 'immigration attorney',
      source: 'martindale_waterfall',
    };
    const k = normKey(row);
    if (seen.has(k)) continue;
    seen.add(k);
    allNew.push(row);
    waterfallAdded++;
  }

  console.log(`\n  New rows added:`);
  console.log(`    MyConsultant: ${mcAdded}`);
  console.log(`    Martindale crawled: ${martAdded}`);
  console.log(`    Martindale waterfall: ${waterfallAdded}`);
  console.log(`    Total new: ${allNew.length}`);

  // Write new master with existing + new
  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const totalRows = master.rows.length + allNew.length;
  const withEmail = [...master.rows, ...allNew].filter(r => r.email).length;

  console.log(`\n=== MASTER v2 ===`);
  console.log(`  Total rows: ${totalRows}`);
  console.log(`  With email: ${withEmail}`);
  console.log(`  Output: ${OUT}`);
}

main();
