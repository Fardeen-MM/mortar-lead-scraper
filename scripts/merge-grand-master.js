#!/usr/bin/env node
/**
 * Grand master merger: combines IMMIGRATION master + MULTI-NICHE master.
 * Output: GRAND-MASTER.csv — everything we have.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const IMM_FILE = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v14.csv');
const MNI_FILE = path.join(OUT_DIR, 'MULTI-NICHE-MASTER.csv');
const OUT = path.join(OUT_DIR, 'GRAND-MASTER.csv');

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
  console.log('=== GRAND MASTER MERGER ===');

  const imm = parseCsv(fs.readFileSync(IMM_FILE, 'utf-8'));
  console.log(`Immigration master: ${imm.rows.length} rows`);

  const mni = fs.existsSync(MNI_FILE) ? parseCsv(fs.readFileSync(MNI_FILE, 'utf-8')) : { rows: [] };
  console.log(`Multi-niche master: ${mni.rows.length} rows`);

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'address', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'category'];

  const seen = new Set();
  const combined = [];

  for (const r of imm.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@') || seen.has(e)) continue;
    seen.add(e);
    combined.push({
      first_name: r.first_name || '',
      last_name: r.last_name || '',
      email: e,
      phone: r.phone || '',
      firm_name: r.firm_name || '',
      website: r.website || '',
      domain: r.domain || '',
      address: '',
      city: r.city || '',
      state: r.state || '',
      region: r.region || '',
      country: r.country || '',
      profile_url: r.profile_url || '',
      niche: r.niche || 'immigration',
      source: r.source || 'immigration',
      category: 'immigration',
    });
  }

  for (const r of mni.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@') || seen.has(e)) continue;
    seen.add(e);
    combined.push({
      first_name: r.first_name || '',
      last_name: r.last_name || '',
      email: e,
      phone: r.phone || '',
      firm_name: r.firm_name || '',
      website: r.website || '',
      domain: r.domain || '',
      address: r.address || '',
      city: r.city || '',
      state: r.state || '',
      region: r.region || '',
      country: r.country || '',
      profile_url: r.profile_url || '',
      niche: r.niche || '',
      source: r.source || '',
      category: r.category || 'multi-niche',
    });
  }

  const outLines = [cols.join(',')];
  for (const r of combined) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  console.log(`\n=== GRAND MASTER ===`);
  console.log(`Total rows: ${combined.length}, unique emails: ${seen.size}`);
  console.log(`Output: ${OUT}`);
}

main();
