#!/usr/bin/env node
/**
 * Merge all non-immigration niches into a single MULTI-NICHE-MASTER.csv
 */
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '..', 'output');
const OUT = path.join(OUT_DIR, 'MULTI-NICHE-MASTER.csv');

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

const FILES = [
  // Nursing
  { file: 'nursing-ccne-schools.csv', niche: 'nursing school', country: 'US', source: 'ccne', emailField: 'email' },
  { file: 'nursing-aacn-crawled.csv', niche: 'nursing school', country: 'US', source: 'aacn_crawled', emailField: 'primary_email' },
  { file: 'nursing-faculty-emails.csv', niche: 'nursing faculty', country: 'US', source: 'nursing_faculty_crawl', emailField: 'email' },

  // Coaches/therapists
  { file: 'noomii-coaches.csv', niche: 'coach', country: 'US', source: 'noomii', emailField: 'email' },
  { file: 'noomii-coaches-crawled.csv', niche: 'coach', country: 'US', source: 'noomii_crawled', emailField: 'primary_email' },
  { file: 'noomii-coaches-full-crawled.csv', niche: 'coach', country: 'US', source: 'noomii_full_crawled', emailField: 'primary_email' },
  { file: 'noomii-crawled-v2.csv', niche: 'coach', country: 'US', source: 'noomii_v2', emailField: 'primary_email' },
  { file: 'therapyden-therapists.csv', niche: 'therapist', country: 'US', source: 'therapyden', emailField: 'email' },

  // Financial/accounting
  { file: 'nasba-cpe-sponsors.csv', niche: 'CPE sponsor', country: 'US', source: 'nasba', emailField: 'email' },
  { file: 'nasba-sponsors-crawled.csv', niche: 'CPE sponsor', country: 'US', source: 'nasba_crawled', emailField: 'primary_email' },
  { file: 'finra-brokercheck.csv', niche: 'broker-dealer', country: 'US', source: 'finra', emailField: 'email' },

  // Overpass (multi-category businesses)
  { file: 'overpass-bulk-businesses.csv', niche: 'local business', country: '', source: 'osm_overpass', emailField: 'email' },
  { file: 'overpass-bulk-trades.csv', niche: 'local business', country: '', source: 'osm_overpass', emailField: 'email' },
  { file: 'overpass-bulk-pro.csv', niche: 'local business', country: '', source: 'osm_overpass', emailField: 'email' },

  // Legal
  { file: 'lsac-law-schools.csv', niche: 'law school', country: 'US', source: 'lsac', emailField: 'admissions_email' },

  // Medical
  { file: 'wdoms-medical-schools.csv', niche: 'medical school', country: '', source: 'wdoms', emailField: 'email' },
  { file: 'med-prep-tutors.csv', niche: 'medical tutor', country: 'US', source: 'med_prep', emailField: 'email' },

  // Other
  { file: 'sec-iapd-firms.csv', niche: 'RIA firm', country: 'US', source: 'sec_iapd', emailField: 'email' },
  { file: "crossfit-affiliates-partial.csv", niche: "gym owner", country: "", source: "crossfit_enriched", emailField: "email" },
  { file: "crossfit-affiliates.csv", niche: 'gym owner', country: '', source: 'crossfit', emailField: 'email' },
  { file: 'naifa-financial-pros.csv', niche: 'financial advisor', country: 'US', source: 'naifa', emailField: 'email' },
  { file: 'mtna-music-teachers.csv', niche: 'music teacher', country: 'US', source: 'mtna', emailField: 'email' },
  { file: 'dance-studios.csv', niche: 'dance chapter', country: 'US', source: 'usadance', emailField: 'email' },
  { file: 'ifa-franchisors.csv', niche: 'franchise', country: 'US', source: 'ifa', emailField: 'email' },
  { file: 'nursing-aacn-schools.csv', niche: 'nursing school', country: 'US', source: 'aacn', emailField: 'email' },
  { file: 'ancc-accredited-crawled.csv', niche: 'nursing CE provider', country: 'US', source: 'ancc_crawled', emailField: 'primary_email' },
  { file: 'martialarts-schools.csv', niche: 'martial arts school', country: 'US', source: 'martial_arts_directory', emailField: 'email' },
  { file: 'overpass-businesses-crawled.csv', niche: 'local business', country: '', source: 'overpass_crawled', emailField: 'primary_email' },
  { file: 'overpass-full-crawled.csv', niche: 'local business', country: '', source: 'overpass_full_crawled', emailField: 'primary_email' },
  { file: "overpass-retry-crawled.csv", niche: "local business", country: "", source: "overpass_retry", emailField: "primary_email" },
  { file: "overpass-extras-crawled.csv", niche: "local business", country: "", source: "overpass_extras_crawled", emailField: "primary_email" },
  { file: "overpass-bulk-extras.csv", niche: 'local business', country: '', source: 'osm_overpass_extras', emailField: 'email' },
];

function main() {
  console.log('=== Merging MULTI-NICHE-MASTER ===');

  const seen = new Set();
  const allRows = [];

  const cols = ['first_name', 'last_name', 'email', 'phone', 'firm_name', 'website',
                'domain', 'address', 'city', 'state', 'country', 'profile_url',
                'niche', 'source', 'category'];

  for (const f of FILES) {
    const fpath = path.join(OUT_DIR, f.file);
    if (!fs.existsSync(fpath)) { console.log(`  ${f.file}: SKIP (not found)`); continue; }
    const data = parseCsv(fs.readFileSync(fpath, 'utf-8'));
    let added = 0, withEmail = 0;
    for (const r of data.rows) {
      const e = normEmail(r[f.emailField] || r.email);
      if (!e || !e.includes('@')) continue;
      withEmail++;
      if (seen.has(e)) continue;
      seen.add(e);
      allRows.push({
        first_name: r.first_name || r.firstname || '',
        last_name: r.last_name || r.lastname || '',
        email: e,
        phone: r.phone || r.mobile || r.telephone_number || '',
        firm_name: r.firm_name || r.company || r.school_name || r.name || r.agency_name || r.sponsor_name || r.affiliate_name || '',
        website: r.website || r.school_url || '',
        domain: r.domain || '',
        address: [r.address, r.addr_street, r.address_1].filter(Boolean).join(' '),
        city: r.city || r.city_searched || r.osm_city || '',
        state: r.state || r.province || r.osm_state || '',
        country: r.country || r.country_searched || r.osm_country || f.country,
        profile_url: r.profile_url || '',
        niche: r.niche || f.niche,
        source: r.source || f.source,
        category: r.category || r.cip_code || r.specialty || '',
      });
      added++;
    }
    console.log(`  ${f.file}: ${withEmail} emails → +${added} unique new`);
  }

  const outLines = [cols.join(',')];
  for (const r of allRows) outLines.push(cols.map(c => escapeCsv(r[c] || '')).join(','));
  fs.writeFileSync(OUT, outLines.join('\n') + '\n');

  console.log(`\n=== MULTI-NICHE-MASTER ===`);
  console.log(`Total rows: ${allRows.length}, unique emails: ${seen.size}`);
  console.log(`Output: ${OUT}`);
}

main();
