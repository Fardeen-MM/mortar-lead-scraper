#!/usr/bin/env node
/**
 * v4 merge: add master-immigration-leads, cold-email-ready, cicc-registry,
 * british-council education agents, trustpilot education consultants,
 * professional-association-members.
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const MASTER = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v3.csv');
const OUT = path.join(OUT_DIR, 'IMMIGRATION-FINAL-CLEAN-v4.csv');

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

function importFile(fpath, srcTag, defaultNiche, seenEmails, allNew) {
  const data = parseCsv(fs.readFileSync(fpath, 'utf-8'));
  let added = 0, totalWithEmail = 0;
  for (const r of data.rows) {
    const e = normEmail(r.email);
    if (!e || !e.includes('@')) continue;
    totalWithEmail++;
    if (seenEmails.has(e)) continue;
    seenEmails.add(e);
    allNew.push({
      first_name: r.first_name || r.firstname || '',
      last_name: r.last_name || r.lastname || '',
      email: e,
      phone: r.phone || '',
      firm_name: r.firm_name || r.company || r.name || '',
      website: r.website || '',
      domain: r.domain || '',
      city: r.city || '',
      state: r.state || r.province || '',
      region: r.region || '',
      country: r.country || '',
      profile_url: r.profile_url || r.source_url || '',
      niche: r.niche || defaultNiche,
      source: r.source || srcTag,
      cicc_id: r.college_id || r.cicc_id || '',
      bar_number: r.bar_number || '',
    });
    added++;
  }
  console.log(`  ${path.basename(fpath)}: ${totalWithEmail} with email → +${added} new`);
  return added;
}

function main() {
  console.log('=== Merging v4 ===');

  const master = parseCsv(fs.readFileSync(MASTER, 'utf-8'));
  console.log(`Master v3: ${master.rows.length} rows`);

  const seen = new Set();
  for (const r of master.rows) {
    const e = normEmail(r.email);
    if (e && e.includes('@')) seen.add(e);
  }

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'city', 'state', 'region', 'country', 'profile_url',
                'niche', 'source', 'cicc_id', 'bar_number'];

  const allNew = [];

  // Immigration-focused
  importFile(path.join(OUT_DIR, 'master-immigration-leads.csv'), 'master_immigration', 'immigration attorney', seen, allNew);
  importFile(path.join(OUT_DIR, 'cold-email-ready.csv'), 'cold_email_ready', 'immigration attorney', seen, allNew);
  importFile(path.join(OUT_DIR, 'cicc-registry_2026-03-06T01-41-06.csv'), 'cicc_registry', 'immigration consultant', seen, allNew);

  // Education agents (student visa / study abroad — immigration-adjacent)
  importFile(path.join(OUT_DIR, 'uk-education-agents-british-council.csv'), 'british_council', 'education agent', seen, allNew);
  importFile(path.join(OUT_DIR, 'education-consultants-trustpilot.csv'), 'trustpilot_education', 'education consultant', seen, allNew);

  // Professional associations (various)
  importFile(path.join(OUT_DIR, 'professional-association-members.csv'), 'pro_assoc', 'professional', seen, allNew);

  console.log(`\nTotal new: ${allNew.length}`);

  const outLines = [cols.join(',')];
  for (const r of master.rows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  for (const r of allNew) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  const total = master.rows.length + allNew.length;
  console.log(`\nMaster v4: ${total} rows, ${seen.size} unique emails`);
  console.log(`Output: ${OUT}`);
}

main();
