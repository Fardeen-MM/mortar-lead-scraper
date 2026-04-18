#!/usr/bin/env node
/**
 * CMS Healthcare Facility Downloader
 *
 * Downloads 5 free government datasets from CMS (Centers for Medicare & Medicaid Services):
 *   1. Nursing Home Provider Info (~15,000+)
 *   2. Home Health Agency Compare
 *   3. Hospital General Information (~6,000+)
 *   4. Hospice Provider Data
 *   5. Dialysis Facility Compare (~7,000+)
 *
 * Each dataset includes facility name, phone, address, city, state, zip.
 * Saves each as a separate CSV in output/
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const PAGE_SIZE = 500;

// ── Dataset definitions ──────────────────────────────────────────────
const DATASETS = [
  {
    name: 'nursing-homes',
    label: 'Nursing Home Provider Info',
    apiBase: 'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0',
    csvFallback: 'https://data.cms.gov/provider-data/sites/default/files/resources/a64b94ee7f224b1a9471e6fba5e5c3d6/NH_ProviderInfo_Apr2026.csv',
    fieldMap: {
      facility_name: ['provider_name', 'facility_name', 'provname', 'provider_legal_business_name'],
      phone: ['provider_phone_number', 'phone_number', 'telephone_number', 'phone', 'provider_telephone_number'],
      address: ['provider_address', 'address', 'address_line_1', 'provider_street_address'],
      city: ['provider_city', 'city', 'citytown'],
      state: ['provider_state', 'state', 'provider_state_code'],
      zip: ['provider_zip_code', 'zip', 'zip_code', 'provider_zip'],
    }
  },
  {
    name: 'home-health-agencies',
    label: 'Home Health Agency Compare',
    apiBase: 'https://data.cms.gov/provider-data/api/1/datastore/query/6jpm-sxkc/0',
    csvFallback: null,
    fieldMap: {
      facility_name: ['provider_name', 'facility_name', 'provname', 'provider_legal_business_name', 'agency_name'],
      phone: ['provider_phone_number', 'phone_number', 'telephone_number', 'phone', 'provider_telephone_number'],
      address: ['provider_address', 'address', 'address_line_1', 'provider_street_address'],
      city: ['provider_city', 'city', 'citytown'],
      state: ['provider_state', 'state', 'provider_state_code'],
      zip: ['provider_zip_code', 'zip', 'zip_code', 'provider_zip'],
    }
  },
  {
    name: 'hospitals',
    label: 'Hospital General Information',
    apiBase: 'https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0',
    csvFallback: null,
    fieldMap: {
      facility_name: ['hospital_name', 'facility_name', 'provider_name', 'provname'],
      phone: ['phone_number', 'provider_phone_number', 'telephone_number', 'phone'],
      address: ['address', 'provider_address', 'address_line_1', 'hospital_address'],
      city: ['city', 'provider_city', 'citytown', 'hospital_city'],
      state: ['state', 'provider_state', 'provider_state_code'],
      zip: ['zip_code', 'provider_zip_code', 'zip', 'provider_zip'],
    }
  },
  {
    name: 'hospices',
    label: 'Hospice Provider Data',
    apiBase: 'https://data.cms.gov/provider-data/api/1/datastore/query/252m-zfp9/0',
    csvFallback: null,
    highDupExpected: true,  // Multiple rows per facility (one per quality measure)
    fieldMap: {
      facility_name: ['facility_name', 'provider_name', 'provname', 'hospice_name'],
      phone: ['phone_number', 'provider_phone_number', 'telephone_number', 'phone'],
      address: ['address_line_1', 'address', 'provider_address', 'street_address'],
      city: ['city', 'provider_city', 'citytown'],
      state: ['state', 'provider_state', 'provider_state_code'],
      zip: ['zip_code', 'provider_zip_code', 'zip', 'provider_zip'],
    }
  },
  {
    name: 'dialysis-facilities',
    label: 'Dialysis Facility Compare',
    apiBase: 'https://data.cms.gov/provider-data/api/1/datastore/query/23ew-n7w9/0',
    csvFallback: null,
    fieldMap: {
      facility_name: ['facility_name', 'provider_name', 'provname'],
      phone: ['phone_number', 'provider_phone_number', 'telephone_number', 'phone'],
      address: ['address_line_1', 'address', 'provider_address', 'address_line_1_text'],
      city: ['city', 'provider_city', 'citytown', 'city_town'],
      state: ['state', 'provider_state', 'provider_state_code'],
      zip: ['zip_code', 'provider_zip_code', 'zip', 'provider_zip', 'zip5'],
    }
  },
];

// ── HTTP helpers ─────────────────────────────────────────────────────

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'Accept': 'application/json', 'User-Agent': 'MortarLeadScraper/1.0' } }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchJSON(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`JSON parse error: ${e.message}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function fetchCSV(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'User-Agent': 'MortarLeadScraper/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchCSV(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve(body));
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ── Field extraction ─────────────────────────────────────────────────

function extractField(row, candidates) {
  // Try each candidate key (case-insensitive)
  const keys = Object.keys(row);
  for (const candidate of candidates) {
    // Exact match first
    if (row[candidate] !== undefined && row[candidate] !== null) return String(row[candidate]).trim();
    // Case-insensitive match
    const lower = candidate.toLowerCase();
    for (const k of keys) {
      if (k.toLowerCase() === lower) return String(row[k]).trim();
    }
  }
  return '';
}

function normalizePhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits[0] === '1') return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7)}`;
  return raw.trim();
}

function mapRow(row, fieldMap) {
  return {
    facility_name: extractField(row, fieldMap.facility_name),
    phone: normalizePhone(extractField(row, fieldMap.phone)),
    address: extractField(row, fieldMap.address),
    city: extractField(row, fieldMap.city),
    state: extractField(row, fieldMap.state),
    zip: extractField(row, fieldMap.zip),
  };
}

// ── CSV writing ──────────────────────────────────────────────────────

function escapeCSV(val) {
  if (!val) return '';
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function writeCSV(filename, rows) {
  const header = 'facility_name,phone,address,city,state,zip';
  const lines = [header];
  for (const r of rows) {
    lines.push([
      escapeCSV(r.facility_name),
      escapeCSV(r.phone),
      escapeCSV(r.address),
      escapeCSV(r.city),
      escapeCSV(r.state),
      escapeCSV(r.zip),
    ].join(','));
  }
  const outPath = path.join(OUTPUT_DIR, filename);
  fs.writeFileSync(outPath, lines.join('\n'), 'utf8');
  return outPath;
}

// ── API pagination ───────────────────────────────────────────────────

function dedupKey(mapped) {
  // Dedup by facility_name + zip (handles multi-row-per-facility datasets like hospice)
  return `${(mapped.facility_name || '').toLowerCase().trim()}|${(mapped.zip || '').trim()}`;
}

async function downloadViaAPI(dataset) {
  const allRows = [];
  const seen = new Set();
  let offset = 0;
  let totalPages = 0;
  let schema = null;
  let dupsSkipped = 0;

  console.log(`  [API] Starting paginated download...`);

  while (true) {
    const url = `${dataset.apiBase}?limit=${PAGE_SIZE}&offset=${offset}`;
    let data;
    try {
      data = await fetchJSON(url);
    } catch (err) {
      if (offset === 0) throw err; // first page failed = API broken
      console.log(`  [API] Error at offset ${offset}: ${err.message} — stopping`);
      break;
    }

    // CMS API returns { results: [...], count: N } or { results: [...] }
    const results = data.results || data;
    if (!Array.isArray(results) || results.length === 0) break;

    // Log first row schema once
    if (!schema && results.length > 0) {
      schema = Object.keys(results[0]);
      console.log(`  [API] Fields: ${schema.join(', ')}`);
    }

    for (const row of results) {
      const mapped = mapRow(row, dataset.fieldMap);
      if (!mapped.facility_name) continue;
      const key = dedupKey(mapped);
      if (seen.has(key)) { dupsSkipped++; continue; }
      seen.add(key);
      allRows.push(mapped);
    }

    totalPages++;
    offset += PAGE_SIZE;
    const total = data.count || '?';
    process.stdout.write(`  [API] Page ${totalPages}: ${allRows.length} unique rows (${dupsSkipped} dups skipped) of ~${total} API rows\r`);

    // Stop if we got fewer than PAGE_SIZE results (last page)
    if (results.length < PAGE_SIZE) break;

    // For datasets with many rows per facility (like hospice measures),
    // track new-facility rate over recent pages. Once we go 50 pages
    // with zero new unique facilities, we've captured them all.
    if (dataset.highDupExpected && totalPages > 10) {
      if (!dataset._lastUniqueCount) dataset._lastUniqueCount = 0;
      if (!dataset._stalePages) dataset._stalePages = 0;

      if (allRows.length === dataset._lastUniqueCount) {
        dataset._stalePages++;
      } else {
        dataset._stalePages = 0;
        dataset._lastUniqueCount = allRows.length;
      }

      if (dataset._stalePages >= 50) {
        console.log(`\n  [API] No new unique facilities for 50 pages. All ${allRows.length} captured.`);
        break;
      }
    }

    // Safety: don't loop forever (1M rows max)
    if (offset > 1000000) {
      console.log(`\n  [API] Safety limit reached at offset ${offset}`);
      break;
    }

    // Small delay to be respectful
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`\n  [API] Downloaded ${allRows.length} unique rows in ${totalPages} pages (${dupsSkipped} duplicates skipped)`);
  return allRows;
}

// ── CSV fallback parsing ─────────────────────────────────────────────

function parseCSVString(csvText) {
  const lines = csvText.split('\n');
  if (lines.length < 2) return [];

  // Parse header
  const header = parseCSVLine(lines[0]);
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const vals = parseCSVLine(lines[i]);
    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = vals[j] || '';
    }
    rows.push(obj);
  }
  return rows;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current.trim());
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current.trim());
  return result;
}

async function downloadViaCSV(dataset) {
  if (!dataset.csvFallback) throw new Error('No CSV fallback URL');
  console.log(`  [CSV] Downloading from ${dataset.csvFallback}...`);
  const csvText = await fetchCSV(dataset.csvFallback);
  console.log(`  [CSV] Downloaded ${(csvText.length / 1024 / 1024).toFixed(1)} MB`);

  const rawRows = parseCSVString(csvText);
  console.log(`  [CSV] Parsed ${rawRows.length} raw rows`);

  if (rawRows.length > 0) {
    console.log(`  [CSV] Fields: ${Object.keys(rawRows[0]).join(', ')}`);
  }

  const mapped = [];
  for (const row of rawRows) {
    const m = mapRow(row, dataset.fieldMap);
    if (m.facility_name) mapped.push(m);
  }
  return mapped;
}

// ── Main ─────────────────────────────────────────────────────────────

async function main() {
  console.log('=== CMS Healthcare Facility Downloader ===\n');

  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const summary = [];

  for (const dataset of DATASETS) {
    console.log(`\n── ${dataset.label} ──`);
    let rows = [];

    // Try API first
    try {
      rows = await downloadViaAPI(dataset);
    } catch (apiErr) {
      console.log(`  [API] Failed: ${apiErr.message}`);
      // Try CSV fallback
      try {
        rows = await downloadViaCSV(dataset);
      } catch (csvErr) {
        console.log(`  [CSV] Failed: ${csvErr.message}`);
        summary.push({ name: dataset.label, count: 0, withPhone: 0, file: 'FAILED' });
        continue;
      }
    }

    // If API returned 0 rows, try CSV fallback
    if (rows.length === 0 && dataset.csvFallback) {
      console.log(`  [API] 0 rows — trying CSV fallback...`);
      try {
        rows = await downloadViaCSV(dataset);
      } catch (csvErr) {
        console.log(`  [CSV] Failed: ${csvErr.message}`);
      }
    }

    const withPhone = rows.filter(r => r.phone && r.phone.length >= 10).length;
    const filename = `cms-${dataset.name}.csv`;
    const outPath = writeCSV(filename, rows);

    console.log(`  Saved: ${outPath}`);
    console.log(`  Total: ${rows.length} facilities | With phone: ${withPhone}`);

    // Show sample
    if (rows.length > 0) {
      const sample = rows[0];
      console.log(`  Sample: ${sample.facility_name} | ${sample.phone} | ${sample.city}, ${sample.state} ${sample.zip}`);
    }

    summary.push({ name: dataset.label, count: rows.length, withPhone, file: filename });
  }

  // ── Summary ──
  console.log('\n\n========================================');
  console.log('           DOWNLOAD SUMMARY');
  console.log('========================================');
  let totalFacilities = 0;
  let totalWithPhone = 0;
  for (const s of summary) {
    const status = s.file === 'FAILED' ? 'FAILED' : `${s.count.toLocaleString()} facilities (${s.withPhone.toLocaleString()} with phone)`;
    console.log(`  ${s.name}: ${status}`);
    totalFacilities += s.count;
    totalWithPhone += s.withPhone;
  }
  console.log('----------------------------------------');
  console.log(`  TOTAL: ${totalFacilities.toLocaleString()} facilities | ${totalWithPhone.toLocaleString()} with phone`);
  console.log(`  Files saved in: ${OUTPUT_DIR}/`);
  console.log('========================================\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
