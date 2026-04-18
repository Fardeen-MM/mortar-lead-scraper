#!/usr/bin/env node
/**
 * v6 merge: more immigration-specific files.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v5.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v6.csv');

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
  if (!fs.existsSync(fpath)) { console.log(`  ${path.basename(fpath)}: missing`); return 0; }
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
  console.log(`  ${path.basename(fpath)}: ${totalWithEmail} with email → +${added} new`);
  return added;
}

function main() {
  console.log('=== Merging v6: more immigration-specific files ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v5: ${master.rows.length} rows`);

  const seen = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) seen.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  // Immigration-specific
  importFile(path.join(OUT_DIR, 'nz-immigration-advisers-enriched.csv'), 'nz_iaa', 'immigration adviser', 'NZ', seen, allNew);
  importFile(path.join(OUT_DIR, 'uk-immigration-advisers-oisc.csv'), 'uk_oisc', 'immigration adviser', 'UK', seen, allNew);
  importFile(path.join(OUT_DIR, 'uk-immigration-solicitors-enriched.csv'), 'uk_solicitor_enriched', 'immigration solicitor', 'UK', seen, allNew);
  importFile(path.join(OUT_DIR, 'uk-immigration-solicitors-sra.csv'), 'uk_sra', 'immigration solicitor', 'UK', seen, allNew);
  importFile(path.join(OUT_DIR, 'netherlands-svma-lawyers.csv'), 'nl_svma', 'immigration lawyer', 'Netherlands', seen, allNew);
  importFile(path.join(OUT_DIR, 'belgium-immigration-lawyers.csv'), 'be_immigration', 'immigration lawyer', 'Belgium', seen, allNew);
  importFile(path.join(OUT_DIR, 'us-immigration-lawyers-justia-enriched.csv'), 'justia_enriched', 'immigration attorney', 'US', seen, allNew);
  importFile(path.join(OUT_DIR, 'eoir-crawled.csv'), 'eoir_crawled', 'immigration attorney', 'US', seen, allNew);
  importFile(path.join(OUT_DIR, 'canada-immigration-extra.csv'), 'canada_immigration_extra', 'immigration consultant', 'Canada', seen, allNew);
  importFile(path.join(OUT_DIR, 'canada-education-consultants.csv'), 'canada_education', 'education consultant', 'Canada', seen, allNew);
  importFile(path.join(OUT_DIR, 'australia-mara-agents-full.csv'), 'mara_full', 'migration agent', 'Australia', seen, allNew);
  importFile(path.join(OUT_DIR, 'au-mara-progress.csv'), 'mara_progress', 'migration agent', 'Australia', seen, allNew);
  importFile(path.join(OUT_DIR, 'australia-lawfirms-master.csv'), 'au_lawfirms', 'lawyer', 'Australia', seen, allNew);

  // Nordic + Eastern Europe lawyers
  importFile(path.join(OUT_DIR, 'nordic-lawyers-norway.csv'), 'norway_bar', 'lawyer', 'Norway', seen, allNew);
  importFile(path.join(OUT_DIR, 'eastern-europe-lawyers.csv'), 'eastern_europe_bar', 'lawyer', '', seen, allNew);

  // PropPublica crawled (already-email-verified nonprofits)
  importFile(path.join(OUT_DIR, 'propublica-crawled.csv'), 'propublica_crawled', 'immigration nonprofit', 'US', seen, allNew);

  // ICEF additional enriched
  importFile(path.join(OUT_DIR, 'icef-with-emails.csv'), 'icef_enriched', 'education agent', '', seen, allNew);

  // Apollo-turbo scrapes (attorney + solicitor focused)
  const apolloDir = OUT_DIR;
  const apolloFiles = fs.readdirSync(apolloDir).filter(f => f.startsWith('apollo-turbo_') && f.endsWith('.csv'));
  for (const af of apolloFiles) {
    const niche = af.includes('solicitor') ? 'immigration solicitor' : 'attorney';
    const country = af.includes('_uk_') ? 'UK' : 'US';
    importFile(path.join(apolloDir, af), 'apollo_turbo', niche, country, seen, allNew);
  }

  // Other law firm masters
  importFile(path.join(OUT_DIR, 'other-lawfirms-master.csv'), 'other_lawfirms', 'lawyer', '', seen, allNew);
  importFile(path.join(OUT_DIR, 'canada-lawfirms-master.csv'), 'canada_lawfirms', 'lawyer', 'Canada', seen, allNew);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v6: ${total} rows, ${seen.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
