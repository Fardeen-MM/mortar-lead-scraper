#!/usr/bin/env node
/**
 * v7: solicitors per-city UK + Ireland files + remaining US immigration files.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v6.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v7.csv');

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
      firm_name: r.firm_name || r.company || r.name || '',
      website: r.website || '',
      domain: r.domain || '',
      city: r.city || '',
      state: r.state || r.province || '',
      region: r.region || '',
      country: r.country || defaultCountry,
      profile_url: r.profile_url || r.source_url || '',
      niche: r.niche || defaultNiche,
      source: r.source || srcTag,
      cicc_id: r.cicc_id || '',
      bar_number: r.bar_number || '',
    });
    added++;
  }
  if (totalWithEmail > 0 || added > 0) {
    console.log(`  ${path.basename(fpath)}: ${totalWithEmail} with email → +${added} new`);
  }
  return added;
}

function main() {
  console.log('=== Merging v7 ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v6: ${master.rows.length} rows`);

  const seen = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) seen.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  // All solicitors_* per-city files
  const files = fs.readdirSync(OUT_DIR);
  for (const f of files) {
    if (f.startsWith('solicitors_') && f.endsWith('.csv')) {
      const country = f.includes('-uk') || f.includes('northern-ireland') ? 'UK' : (f.includes('ireland') ? 'Ireland' : '');
      importFile(path.join(OUT_DIR, f), 'solicitors_city', 'solicitor', country, seen, allNew);
    }
  }

  // Remaining US immigration
  const usImmFiles = [
    'us-immigration-advocates-network.csv',
    'us-immigration-clinic-affiliates.csv',
    'us-immigration-lawyers-avvo.csv',
    'us-immigration-lawyers-bestlawyers.csv',
    'us-immigration-lawyers-expertise.csv',
    'us-immigration-lawyers-findlaw.csv',
    'us-immigration-lawyers-findlaw-enriched.csv',
    'us-immigration-lawyers-justia.csv',
    'us-immigration-lawyers-lawfully.csv',
    'us-immigration-lawyers-lawinfo.csv',
    'us-immigration-lawyers-lawyerlegion.csv',
    'us-immigration-lawyers-lawyerscom.csv',
    'us-immigration-lawyers-nolo.csv',
    'us-immigration-lawyers-state-bar.csv',
    'us-immigration-lawyers-superlawyers.csv',
    'us-immigration-lawyers-thumbtack.csv',
    'us-immigration-lawyers-yellowpages.csv',
  ];
  for (const f of usImmFiles) {
    importFile(path.join(OUT_DIR, f), f.replace('.csv', '').replace('us-immigration-', ''), 'immigration lawyer', 'US', seen, allNew);
  }

  // UK immigration extras
  const ukImmFiles = [
    'uk-ilpa-immigration-practitioners.csv',
    'uk-immigration-barristers-bsb.csv',
    'uk-immigration-extra.csv',
    'uk-immigration-solicitors-lawsociety.csv',
    'uk-immigration-solicitors.csv',
  ];
  for (const f of ukImmFiles) {
    importFile(path.join(OUT_DIR, f), f.replace('.csv', '').replace('uk-immigration-', ''), 'immigration solicitor', 'UK', seen, allNew);
  }

  // US education consultants (study abroad)
  const usEduFiles = [
    'us-education-consultants-expertise.csv',
    'us-education-consultants-extra.csv',
    'us-education-consultants-ieca.csv',
  ];
  for (const f of usEduFiles) {
    importFile(path.join(OUT_DIR, f), f.replace('.csv', ''), 'education consultant', 'US', seen, allNew);
  }

  // APAC extras
  importFile(path.join(OUT_DIR, 'au-immigration-extra.csv'), 'au_immigration_extra', 'migration agent', 'Australia', seen, allNew);
  importFile(path.join(OUT_DIR, 'best-lawyers-immigration-global.csv'), 'best_lawyers_global', 'immigration attorney', '', seen, allNew);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v7: ${total} rows, ${seen.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
