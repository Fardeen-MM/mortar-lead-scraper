#!/usr/bin/env node
/**
 * Merge + deduplicate all law firm CSVs into a single master file.
 * Deduplicates by: email > phone+city > firm_name+city > website domain.
 * When merging duplicates, keeps the record with the most data.
 */

const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const MASTER_FILE = path.join(OUTPUT_DIR, 'ALL-LAWFIRMS-MASTER.csv');

function parseCSV(text) {
  const lines = text.split('\n');
  if (lines.length < 2) return { header: [], rows: [] };
  const header = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const fields = parseCSVLine(lines[i]);
    if (fields.length === header.length) {
      const row = {};
      header.forEach((h, idx) => row[h] = fields[idx] || '');
      rows.push(row);
    }
  }
  return { header, rows };
}

function parseCSVLine(line) {
  const fields = [];
  let field = '', inQuotes = false;
  for (const ch of line) {
    if (ch === '"') inQuotes = !inQuotes;
    else if (ch === ',' && !inQuotes) { fields.push(field); field = ''; }
    else field += ch;
  }
  fields.push(field);
  return fields;
}

function escapeCSV(val) {
  if (!val) return '';
  val = String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function domainOf(url) {
  if (!url) return '';
  try {
    return new URL(url.startsWith('http') ? url : `https://${url}`).hostname.replace(/^www\./, '').toLowerCase();
  } catch { return ''; }
}

function normalizePhone(ph) {
  if (!ph) return '';
  return ph.replace(/\D/g, '').replace(/^1(\d{10})$/, '$1');
}

function dataScore(row) {
  let score = 0;
  if (row.email) score += 10;
  if (row.phone) score += 5;
  if (row.website) score += 3;
  if (row.first_name) score += 4;
  if (row.last_name) score += 4;
  if (row.title) score += 3;
  if (row.linkedin_url) score += 3;
  if (row.bio) score += 2;
  if (row.education) score += 1;
  if (row.practice_specialties) score += 1;
  return score;
}

function mergeRows(existing, incoming) {
  // Keep the one with more data as base, fill gaps from the other
  const base = dataScore(existing) >= dataScore(incoming) ? { ...existing } : { ...incoming };
  const other = dataScore(existing) >= dataScore(incoming) ? incoming : existing;
  for (const key of Object.keys(base)) {
    if (!base[key] && other[key]) {
      base[key] = other[key];
    }
  }
  return base;
}

// Collect all CSV files
const files = fs.readdirSync(OUTPUT_DIR).filter(f =>
  f.endsWith('.csv') &&
  f !== 'ALL-LAWFIRMS-MASTER.csv' &&
  f !== 'US-LAWFIRMS-MASTER.csv' &&
  f !== 'CANADA-LAWFIRMS-MASTER.csv' &&
  f !== 'UK-LAWFIRMS-MASTER.csv' &&
  f !== 'AUSTRALIA-LAWFIRMS-MASTER.csv' &&
  f !== 'IRELAND-LAWFIRMS-MASTER.csv' &&
  !f.startsWith('.')
);

console.log(`Found ${files.length} CSV files to merge`);

// Parse all CSVs
let allRows = [];
let header = null;
for (const f of files) {
  const text = fs.readFileSync(path.join(OUTPUT_DIR, f), 'utf8');
  const { header: h, rows } = parseCSV(text);
  if (!header && h.length > 0) header = h;
  allRows.push(...rows);
  if (rows.length > 0) {
    console.log(`  ${f}: ${rows.length} leads`);
  }
}

console.log(`\nTotal raw leads: ${allRows.length}`);

// Dedup: email > phone+city > firm+city > domain
const byEmail = new Map();
const byPhoneCity = new Map();
const byFirmCity = new Map();
const byDomain = new Map();
const unique = [];

for (const row of allRows) {
  const email = (row.email || '').toLowerCase().trim();
  const phone = normalizePhone(row.phone);
  const city = (row.city || '').toLowerCase().trim();
  const firm = (row.firm_name || '').toLowerCase().trim().replace(/[^a-z0-9\s]/g, '');
  const domain = domainOf(row.website);

  let merged = false;

  // 1. Dedup by email
  if (email) {
    if (byEmail.has(email)) {
      const idx = byEmail.get(email);
      unique[idx] = mergeRows(unique[idx], row);
      merged = true;
    }
  }

  // 2. Dedup by phone + city
  if (!merged && phone && city) {
    const key = `${phone}__${city}`;
    if (byPhoneCity.has(key)) {
      const idx = byPhoneCity.get(key);
      unique[idx] = mergeRows(unique[idx], row);
      merged = true;
    }
  }

  // 3. Dedup by firm name + city (fuzzy — strip common suffixes)
  if (!merged && firm && city) {
    const normFirm = firm.replace(/\b(llc|llp|pllc|pc|pa|inc|corp|ltd|plc|psc|apc)\b/g, '').trim().replace(/\s+/g, ' ');
    const key = `${normFirm}__${city}`;
    if (byFirmCity.has(key)) {
      const idx = byFirmCity.get(key);
      // Only merge if same or no person name (don't merge different people at same firm)
      const existing = unique[idx];
      const samePerson = (!row.first_name && !existing.first_name) ||
        (row.first_name === existing.first_name && row.last_name === existing.last_name);
      if (samePerson) {
        unique[idx] = mergeRows(existing, row);
        merged = true;
      }
    }
  }

  // 4. Dedup by domain (only if no person name — avoids merging different people at same firm)
  if (!merged && domain && !row.first_name) {
    if (byDomain.has(domain)) {
      const idx = byDomain.get(domain);
      if (!unique[idx].first_name) {
        unique[idx] = mergeRows(unique[idx], row);
        merged = true;
      }
    }
  }

  if (!merged) {
    const idx = unique.length;
    unique.push(row);
    if (email) byEmail.set(email, idx);
    if (phone && city) byPhoneCity.set(`${phone}__${city}`, idx);
    if (firm && city) {
      const normFirm = firm.replace(/\b(llc|llp|pllc|pc|pa|inc|corp|ltd|plc|psc|apc)\b/g, '').trim().replace(/\s+/g, ' ');
      byFirmCity.set(`${normFirm}__${city}`, idx);
    }
    if (domain && !row.first_name) byDomain.set(domain, idx);
  }
}

console.log(`After dedup: ${unique.length} unique leads`);

// Stats
let withEmail = 0, withPhone = 0, withName = 0, withTitle = 0, withLinkedIn = 0;
for (const row of unique) {
  if (row.email) withEmail++;
  if (row.phone) withPhone++;
  if (row.first_name) withName++;
  if (row.title) withTitle++;
  if (row.linkedin_url) withLinkedIn++;
}

console.log(`  With email: ${withEmail} (${Math.round(withEmail * 100 / unique.length)}%)`);
console.log(`  With phone: ${withPhone} (${Math.round(withPhone * 100 / unique.length)}%)`);
console.log(`  With person name: ${withName} (${Math.round(withName * 100 / unique.length)}%)`);
console.log(`  With title: ${withTitle} (${Math.round(withTitle * 100 / unique.length)}%)`);
console.log(`  With LinkedIn: ${withLinkedIn} (${Math.round(withLinkedIn * 100 / unique.length)}%)`);

// Write master CSV
if (!header) {
  console.error('No header found — no CSVs to merge.');
  process.exit(1);
}

const lines = [header.join(',')];
for (const row of unique) {
  lines.push(header.map(h => escapeCSV(row[h])).join(','));
}

fs.writeFileSync(MASTER_FILE, lines.join('\n') + '\n');
console.log(`\nWritten: ${MASTER_FILE} (${unique.length} leads)`);

// Also write per-region masters by detecting country from state/city
const regions = { US: [], CANADA: [], UK: [], AUSTRALIA: [], IRELAND: [], OTHER: [] };
for (const row of unique) {
  const state = (row.state || '').toUpperCase();
  const city = (row.city || '').toLowerCase();
  const source = (row.source || '').toLowerCase();

  if (/canada|,\s*(on|bc|ab|qc|mb|sk|ns|nb|nl|pe|nt|yt),/i.test(city) || source.includes('canada')) {
    regions.CANADA.push(row);
  } else if (/uk|united kingdom|england|scotland|wales/i.test(city) || source.includes('solicitor') || source.includes('uk')) {
    regions.UK.push(row);
  } else if (/australia|,\s*(nsw|vic|qld|wa|sa|tas|act|nt),/i.test(city) || source.includes('australia')) {
    regions.AUSTRALIA.push(row);
  } else if (/ireland|northern ireland/i.test(city) || source.includes('ireland')) {
    regions.IRELAND.push(row);
  } else if (state.length === 2 && /^[A-Z]{2}$/.test(state)) {
    regions.US.push(row);
  } else {
    regions.OTHER.push(row);
  }
}

for (const [region, rows] of Object.entries(regions)) {
  if (rows.length === 0) continue;
  const file = path.join(OUTPUT_DIR, `${region}-LAWFIRMS-MASTER.csv`);
  const regionLines = [header.join(',')];
  for (const row of rows) {
    regionLines.push(header.map(h => escapeCSV(row[h])).join(','));
  }
  fs.writeFileSync(file, regionLines.join('\n') + '\n');
  console.log(`  ${region}: ${rows.length} leads → ${file}`);
}
