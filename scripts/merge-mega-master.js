#!/usr/bin/env node
/**
 * MEGA MASTER: combines EVERY email across all output CSVs.
 * Handles any column named 'email' or 'primary_email'.
 * Categorizes by source file.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT = path.join(OUT_DIR, 'MEGA-MASTER-ALL.csv');

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

const norm = e => (e || '').trim().toLowerCase();

// Skip these files (backups/intermediate/etc.)
const SKIP_FILES = new Set([
  'MEGA-MASTER-ALL.csv',
  'IMMIGRATION-SEND-PART1.csv', 'IMMIGRATION-SEND-PART2.csv', 'IMMIGRATION-SEND-PART3.csv',
  'IMMIGRATION-SEND-PART4.csv', 'IMMIGRATION-SEND-PART5.csv',
  'IMMIGRATION-HIGH-QUALITY.csv', 'IMMIGRATION-ONLY-FILTERED.csv',
  'IMMIGRATION-FINAL-CLEAN.csv', 'IMMIGRATION-FINAL-CLEAN-v2.csv', 'IMMIGRATION-FINAL-CLEAN-v3.csv',
  'IMMIGRATION-FINAL-CLEAN-v4.csv', 'IMMIGRATION-FINAL-CLEAN-v5.csv', 'IMMIGRATION-FINAL-CLEAN-v6.csv',
  'IMMIGRATION-FINAL-CLEAN-v7.csv', 'IMMIGRATION-FINAL-CLEAN-v8.csv', 'IMMIGRATION-FINAL-CLEAN-v9.csv',
  'IMMIGRATION-FINAL-CLEAN-v10.csv', 'IMMIGRATION-FINAL-CLEAN-v11.csv', 'IMMIGRATION-FINAL-CLEAN-v12.csv',
  'IMMIGRATION-FINAL-CLEAN-v13.csv',
  'IMMIGRATION-CLEAN-FOR-LEADMAGIC.csv', 'IMMIGRATION-LEADMAGIC-PART1.csv', 'IMMIGRATION-LEADMAGIC-PART2.csv',
  'IMMIGRATION-NEW-LEADS-FOR-LEADMAGIC.csv',
  'IMMIGRATION-MEGA-ALL.csv', 'IMMIGRATION-ALL-MEGA.csv', 'IMMIGRATION-ALL-MEGA-DEDUPED.csv',
  'ALL-LAWYERS-DEDUPED-MASTER.csv', 'ALL-LAWYERS-MEGA-DEDUPED.csv', 'ALL-LAWFIRMS-MASTER.csv',
  'ALL-LAWFIRMS-AD-SCAN.csv', 'ALL-CONTRACTORS-WITH-EMAILS.csv',
  'MULTI-NICHE-MASTER.csv', 'GRAND-MASTER.csv',
  'US-LAWFIRMS-MASTER.csv', 'US-LAWFIRMS-MASTER-enriched.csv',
  'UK-LAWFIRMS-MASTER.csv', 'CANADA-LAWFIRMS-MASTER.csv', 'OTHER-LAWFIRMS-MASTER.csv',
  'AUSTRALIA-LAWFIRMS-MASTER.csv',
]);

function main() {
  console.log('=== MEGA MASTER ===');

  const seen = new Set();
  const allRows = [];

  const cols = ['email', 'first_name', 'last_name', 'phone', 'firm_name', 'website',
                'city', 'state', 'country', 'niche', 'source', 'source_file'];

  const files = fs.readdirSync(OUT_DIR).filter(f => f.endsWith('.csv') && !SKIP_FILES.has(f));
  console.log(`Scanning ${files.length} files...`);

  let fileStats = [];

  for (const fname of files) {
    const fpath = path.join(OUT_DIR, fname);
    if (fs.statSync(fpath).size < 100) continue;
    try {
      const data = parseCsv(fs.readFileSync(fpath, 'utf-8'));
      const emailFields = (data.headers || []).filter(h => /email/i.test(h) && !/verified|seen/i.test(h));
      if (!emailFields.length) continue;

      let fileAdded = 0;
      for (const r of data.rows) {
        let e = '';
        for (const ef of emailFields) {
          const v = norm(r[ef]);
          if (v && v.includes('@') && v.includes('.')) { e = v; break; }
        }
        if (!e || seen.has(e)) continue;
        seen.add(e);
        allRows.push({
          email: e,
          first_name: r.first_name || r.firstname || '',
          last_name: r.last_name || r.lastname || '',
          phone: r.phone || r.mobile || '',
          firm_name: r.firm_name || r.company || r.school_name || r.name || r.agency_name || '',
          website: r.website || r.school_url || '',
          city: r.city || r.city_searched || '',
          state: r.state || r.province || '',
          country: r.country || '',
          niche: r.niche || '',
          source: r.source || '',
          source_file: fname,
        });
        fileAdded++;
      }
      if (fileAdded > 0) fileStats.push({ fname, added: fileAdded });
    } catch (err) {}
  }

  const outLines = [cols.join(',')];
  for (const r of allRows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  console.log(`\nTop contributors:`);
  fileStats.sort((a, b) => b.added - a.added);
  for (const s of fileStats.slice(0, 20)) console.log(`  +${s.added.toString().padStart(7)}  ${s.fname}`);

  console.log(`\n=== MEGA MASTER ===`);
  console.log(`Total unique emails: ${seen.size}`);
  console.log(`Output: ${OUT}`);
}

main();
