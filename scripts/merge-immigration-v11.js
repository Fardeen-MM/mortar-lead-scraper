#!/usr/bin/env node
/**
 * v11: + Ireland Law Society (12,663 solicitors / 11,716 emails) + DOJ enriched
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v10.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v11.csv');

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

function importFile(fpath, srcTag, defaultNiche, defaultCountry, seenEmails, allNew) {
  if (!fs.existsSync(fpath)) return 0;
  const data = parseCsv(fs.readFileSync(fpath, 'utf-8'));
  let added = 0, totalWithEmail = 0;
  for (const r of data.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@')) continue;
    totalWithEmail++;
    if (seenEmails.has(e)) continue;
    seenEmails.add(e);
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
      region: r.region || '',
      country: r.country || defaultCountry,
      profile_url: r.profile_url || '',
      niche: r.niche || defaultNiche,
      source: r.source || srcTag,
      cicc_id: r.cicc_id || '',
      bar_number: r.bar_number || '',
    });
    added++;
  }
  console.log(`  ${path.basename(fpath)}: ${totalWithEmail} with email → +${added} new`);
  return added;
}

function main() {
  console.log('=== Merging v11 ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v10: ${master.rows.length} rows`);

  const seen = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) seen.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  importFile(path.join(OUT_DIR, 'ireland-solicitors-all.csv'), 'law_society_ireland', 'solicitor', 'Ireland', seen, allNew);
  importFile(path.join(OUT_DIR, 'doj-reps-enriched.csv'), 'doj_eoir_enriched', 'immigration nonprofit', 'US', seen, allNew);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v11: ${total} rows, ${seen.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
