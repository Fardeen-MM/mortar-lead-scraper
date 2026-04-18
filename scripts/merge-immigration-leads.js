#!/usr/bin/env node
/**
 * Master merger for immigration-focused lead CSVs.
 *
 * Combines:
 *   - CICC segments (Canadian immigration consultants)
 *   - EOIR US accredited reps
 *   - ICEF global education/migration agents (+ crawled emails)
 *   - Immigration podcast hosts
 *   - English UK ESL schools (adjacent — students coming to UK)
 *
 * Dedup strategy (in order of priority):
 *   1. email (exact match)
 *   2. domain (same-domain leads merged — use primary contact)
 *   3. firm_name + city (fuzzy normalize)
 *
 * Output: output/master-immigration-leads.csv
 *   - Unified schema: first_name, last_name, firm_name, email, phone, website,
 *     domain, city, state, country, niche, source, source_url, notes
 */

const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, '..', 'output', 'master-immigration-leads.csv');

const SOURCES = [
  // CICC segments (existing merged file)
  { file: 'immigration-consultants-canada-cicc.csv', source: 'cicc_registry', country: 'Canada', niche: 'immigration consultant' },
  { file: 'cicc-segment-D.csv', source: 'cicc_registry', country: 'Canada', niche: 'immigration consultant' },
  // EOIR — prefer crawled (has emails) → domains (has domains for future crawl) → raw
  { file: 'eoir-crawled.csv', source: 'eoir_accredited_reps', country: 'US', niche: 'immigration representative' },
  { file: 'eoir-with-domains.csv', source: 'eoir_accredited_reps', country: 'US', niche: 'immigration representative', fallback: true },
  { file: 'eoir-immigration-reps.csv', source: 'eoir_accredited_reps', country: 'US', niche: 'immigration representative', fallback: true },
  // ICEF (use enriched version if available)
  { file: 'icef-with-emails.csv', source: 'icef_ias', country: null, niche: 'study abroad / migration agent' },
  // Fallback: ICEF without emails
  { file: 'icef-ias-global-agents.csv', source: 'icef_ias', country: null, niche: 'study abroad / migration agent', fallback: true },
  // Immigration podcasts
  { file: 'immigration-podcast-hosts.csv', source: 'immigration_podcast', country: null, niche: 'immigration podcast host' },
  // English UK ESL schools (adjacent)
  { file: 'uk-esl-schools-english-uk.csv', source: 'english_uk_schools', country: 'UK', niche: 'ESL school (UK)' },
  // ProPublica US immigration nonprofits — prefer crawled version
  { file: 'propublica-crawled.csv', source: 'propublica', country: 'US', niche: 'immigration nonprofit' },
  { file: 'propublica-with-domains.csv', source: 'propublica', country: 'US', niche: 'immigration nonprofit', fallback: true },
  { file: 'propublica-immigration-nonprofits.csv', source: 'propublica', country: 'US', niche: 'immigration nonprofit', fallback: true },
  // Bulk find-leads output (53 immigration cities via multi-engine Google alt)
  { file: 'bulk-leads-immigration-crawled.csv', source: 'bulk_find_leads', country: null, niche: 'immigration consultant' },
  { file: 'bulk-leads-immigration-consultant.csv', source: 'bulk_find_leads', country: null, niche: 'immigration consultant', fallback: true },
  // MEGA sweeps (277 cities × multiple niches)
  { file: 'mega-immigration-sweep.csv', source: 'mega_sweep_a', country: null, niche: 'immigration (multi)' },
  { file: 'mega-immigration-sweep-B.csv', source: 'mega_sweep_b', country: null, niche: 'immigration (adjacent)' },
  { file: 'mega-immigration-sweep-C.csv', source: 'mega_sweep_c', country: null, niche: 'immigration (visa code)' },
  // IMMLAW consortium (US immigration attorney consortium) — 29 verified partners
  { file: 'immlaw-immigration-attorneys.csv', source: 'immlaw_consortium', country: 'US', niche: 'immigration attorney' },
  // Best Lawyers — elite US/CA/AU immigration attorneys
  { file: 'best-lawyers-immigration-global.csv', source: 'best_lawyers', country: null, niche: 'immigration attorney' },
  // NEAIMS Kenya — 1,309 overseas employment agencies (single curl win!)
  { file: 'neaims-kenya-employment-agencies.csv', source: 'neaims_kenya', country: 'Kenya', niche: 'overseas employment agent' },
  // Martindale US Immigration — 19,275 lawyers, 2,169 with name+domain for waterfall
  { file: 'martindale-immigration-emails.csv', source: 'martindale_immigration', country: 'US', niche: 'immigration attorney' },
  { file: 'us-immigration-lawyers-martindale.csv', source: 'martindale_immigration', country: 'US', niche: 'immigration attorney', fallback: true },
  // NY state attorney registrations — 618 immigration-firm attorneys (filtered from 429K total)
  { file: 'nys-immigration-attorneys.csv', source: 'nys_attorney_registration', country: 'US', niche: 'immigration attorney' },
];

const OUT_COLS = [
  'first_name', 'last_name', 'firm_name', 'email', 'phone', 'website',
  'domain', 'city', 'state', 'country', 'niche', 'title', 'source', 'source_url', 'notes',
];

function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  function splitRow(line) {
    const out = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (inQ) {
        if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; }
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
  if (!lines.length) return { headers: [], rows: [] };
  const headers = splitRow(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = splitRow(lines[i]);
    const rec = {};
    headers.forEach((h, idx) => { rec[h] = cols[idx] || ''; });
    rows.push(rec);
  }
  return { headers, rows };
}

function escapeCsv(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function normalizeDomain(w) {
  if (!w) return '';
  try {
    return w.toLowerCase().trim()
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0]
      .split('?')[0];
  } catch { return ''; }
}

function normalizeStr(s) {
  return (s || '').toLowerCase().replace(/[^a-z0-9]/g, '').trim();
}

function toStandard(row, sourceConfig) {
  // Map source-specific columns to the unified schema
  const domain = normalizeDomain(row.domain || row.website || '');

  // Priority: existing email → primary_email (from website crawl) → first of crawled_emails list
  let email = row.email || row.primary_email || '';
  if (!email && row.crawled_emails) {
    email = row.crawled_emails.split(';')[0]?.trim() || '';
  }
  if (email) email = email.toLowerCase().trim();

  return {
    first_name: row.first_name || '',
    last_name: row.last_name || '',
    firm_name: row.firm_name || row.company || row.organization || row.podcast_name || row.collectionName || '',
    email,
    phone: row.phone || '',
    website: row.website || '',
    domain,
    city: row.city || row.mailingCity || '',
    state: row.state || '',
    country: row.country || row.mailingCountry || sourceConfig.country || '',
    niche: row.niche || sourceConfig.niche || '',
    title: row.title || '',
    source: row.source || sourceConfig.source || '',
    source_url: row.profile_url || row.source_url || row.feed_url || row.ias_cert_url || '',
    notes: row.description ? row.description.slice(0, 200) : '',
  };
}

function main() {
  const dir = path.join(__dirname, '..', 'output');
  const all = [];
  const bySource = {};

  for (const conf of SOURCES) {
    const filePath = path.join(dir, conf.file);
    if (!fs.existsSync(filePath)) {
      console.log(`  SKIP ${conf.file} (not found)`);
      continue;
    }
    if (conf.fallback) {
      // If a non-fallback version for the same source already loaded, skip
      const already = all.some(r => r.source === conf.source);
      if (already) {
        console.log(`  SKIP ${conf.file} (fallback; non-fallback already loaded)`);
        continue;
      }
    }

    const text = fs.readFileSync(filePath, 'utf-8');
    const { rows } = parseCsv(text);
    const mapped = rows.map(r => toStandard(r, conf));
    console.log(`  ${conf.file}: ${rows.length} rows`);
    bySource[conf.source] = (bySource[conf.source] || 0) + rows.length;
    all.push(...mapped);
  }

  console.log(`\nTotal before dedup: ${all.length}`);

  // Dedup
  const seen = new Set();
  const deduped = [];
  let emailDupes = 0, domainDupes = 0, nameDupes = 0;

  // Pass 1: exact email match
  for (const r of all) {
    if (r.email && r.email.includes('@')) {
      const key = `email:${r.email}`;
      if (seen.has(key)) { emailDupes++; continue; }
      seen.add(key);
      deduped.push(r);
    }
  }

  // Pass 2: domain match (if no email), keep one per unique domain
  const noEmail = all.filter(r => !r.email || !r.email.includes('@'));
  const seenDomains = new Set(deduped.map(r => r.domain).filter(Boolean));
  for (const r of noEmail) {
    if (!r.domain) continue;
    const key = `domain:${r.domain}`;
    if (seen.has(key)) { domainDupes++; continue; }
    if (seenDomains.has(r.domain)) { domainDupes++; continue; }
    seen.add(key);
    seenDomains.add(r.domain);
    deduped.push(r);
  }

  // Pass 3: firm_name + city (for rows with neither email nor domain)
  const remaining = all.filter(r => !r.email && !r.domain);
  for (const r of remaining) {
    const norm = normalizeStr(r.firm_name) + '|' + normalizeStr(r.city);
    if (!r.firm_name) continue;
    const key = `name:${norm}`;
    if (seen.has(key)) { nameDupes++; continue; }
    seen.add(key);
    deduped.push(r);
  }

  // Write CSV
  const header = OUT_COLS.join(',');
  const body = deduped.map(r => OUT_COLS.map(c => escapeCsv(r[c] || '')).join(',')).join('\n');
  fs.writeFileSync(OUTPUT_FILE, header + '\n' + body + '\n');

  // Stats
  const withEmail = deduped.filter(r => r.email).length;
  const withPhone = deduped.filter(r => r.phone).length;
  const withDomain = deduped.filter(r => r.domain).length;
  const countryCounts = {};
  const nicheCounts = {};
  const sourceCounts = {};
  for (const r of deduped) {
    if (r.country) countryCounts[r.country] = (countryCounts[r.country] || 0) + 1;
    if (r.niche) nicheCounts[r.niche] = (nicheCounts[r.niche] || 0) + 1;
    if (r.source) sourceCounts[r.source] = (sourceCounts[r.source] || 0) + 1;
  }

  console.log(`\n=== MASTER MERGE RESULTS ===`);
  console.log(`Before dedup: ${all.length}`);
  console.log(`After dedup:  ${deduped.length}`);
  console.log(`  email dupes: ${emailDupes}`);
  console.log(`  domain dupes: ${domainDupes}`);
  console.log(`  name dupes:  ${nameDupes}`);
  console.log(``);
  console.log(`Coverage:`);
  console.log(`  email:  ${withEmail} (${(withEmail/deduped.length*100).toFixed(1)}%)`);
  console.log(`  phone:  ${withPhone} (${(withPhone/deduped.length*100).toFixed(1)}%)`);
  console.log(`  domain: ${withDomain} (${(withDomain/deduped.length*100).toFixed(1)}%)`);
  console.log(``);
  console.log('By source:');
  for (const [s, n] of Object.entries(sourceCounts).sort((a,b)=>b[1]-a[1])) {
    console.log(`  ${s}: ${n}`);
  }
  console.log('');
  console.log('Top 15 countries:');
  for (const [c, n] of Object.entries(countryCounts).sort((a,b)=>b[1]-a[1]).slice(0, 15)) {
    console.log(`  ${c}: ${n}`);
  }
  console.log(`\nOutput: ${OUTPUT_FILE}`);
}

main();
