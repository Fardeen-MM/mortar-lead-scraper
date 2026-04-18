#!/usr/bin/env node
/**
 * Driving School Scraper — Multi-Source
 *
 * Scrapes driving school directories from government open data portals:
 *
 *   1. Ontario (Canada) — data.ontario.ca CSV (~1,500 schools)
 *      Fields: City, School, DigitalCurriculum, DriverImprovementCourse,
 *              DefensiveDrivingCourse, Address, Phone
 *      URL: https://data.ontario.ca/dataset/government-approved-driving-schools
 *
 *   2. New York (USA) — data.ny.gov SODA API (~613 schools)
 *      Fields: school_name, street, city, state, zip, phone_number, school_number,
 *              pre_licensing, auto, tractor_trailer, truck, motorcycle, bus
 *      URL: https://data.ny.gov/Transportation/DMV-Driving-Schools/3p6i-cv8y
 *
 *   3. Texas Driver Education (USA) — TDLR CSV (~650 providers)
 *      Fields: License Number, Profession, License Status, Licensee,
 *              License Expiration Date, Address 1, Address 2, City, State, County, Zip, Phone
 *      URL: https://www.tdlr.texas.gov/dbproduction2/vsDriverEduProvider.csv
 *
 *   4. Texas Driving Safety (USA) — TDLR CSV (~558 providers)
 *      Fields: Same as #3
 *      URL: https://www.tdlr.texas.gov/dbproduction2/vsDriverSafetyProvider.csv
 *
 * TAM context: ~40K ADIs in the UK alone, 10K+ schools in the US.
 * A "How to pass your driving test first time" webinar funnel would crush.
 *
 * Usage:
 *   node scripts/scrape-driving-schools.js                # All sources
 *   node scripts/scrape-driving-schools.js --source=ontario
 *   node scripts/scrape-driving-schools.js --source=ny
 *   node scripts/scrape-driving-schools.js --source=texas-edu
 *   node scripts/scrape-driving-schools.js --source=texas-safety
 *   node scripts/scrape-driving-schools.js --test          # First 20 per source
 *
 * Output: output/driving-schools.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

// === CLI Args ===
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const TEST_MODE = !!args.test;
const SOURCE_FILTER = args.source || 'all'; // ontario, ny, texas-edu, texas-safety, all
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'driving-schools.csv');
const DELAY_MS = parseInt(args.delay) || 500;

// === CSV Output Columns ===
const CSV_COLUMNS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
];

// === Helpers ===

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * HTTP GET with redirect following, timeout, and retry.
 */
function httpGet(url, retries = 3) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    }, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith('/')) {
          const parsed = new URL(url);
          redirectUrl = `${parsed.protocol}//${parsed.host}${redirectUrl}`;
        }
        httpGet(redirectUrl, retries).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        if (retries > 0) {
          log(`  HTTP ${res.statusCode} for ${url}, retrying (${retries} left)...`);
          sleep(2000).then(() => httpGet(url, retries - 1).then(resolve).catch(reject));
          return;
        }
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    });
    req.on('error', err => {
      if (retries > 0) {
        sleep(2000).then(() => httpGet(url, retries - 1).then(resolve).catch(reject));
      } else {
        reject(err);
      }
    });
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

/**
 * Parse CSV text into array of objects.
 * Handles quoted fields with commas and newlines inside them.
 */
function parseCSV(text) {
  const lines = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < text.length && text[i + 1] === '"') {
        current += '"';
        i++; // skip escaped quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === '\n' && !inQuotes) {
      if (current.trim() || lines.length > 0) {
        lines.push(current);
      }
      current = '';
    } else if (ch === '\r' && !inQuotes) {
      // skip carriage returns
    } else {
      current += ch;
    }
  }
  if (current.trim()) lines.push(current);

  if (lines.length < 2) return [];

  // Parse header
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    if (values.length === 0) continue;
    const obj = {};
    for (let j = 0; j < headers.length; j++) {
      obj[headers[j].trim()] = (values[j] || '').trim();
    }
    rows.push(obj);
  }
  return rows;
}

/**
 * Parse a single CSV line into fields, respecting quoted strings.
 */
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

/**
 * Normalize phone number: strip non-digits, format as (XXX) XXX-XXXX for 10-digit NA numbers.
 */
function normalizePhone(raw) {
  if (!raw) return '';
  let digits = raw.replace(/[^\d]/g, '');
  // Strip leading country code
  if (digits.length === 11 && digits.startsWith('1')) digits = digits.slice(1);
  if (digits.length < 7) return '';
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return raw.trim();
}

/**
 * Clean company name: trim whitespace, normalize casing for ALL-CAPS names.
 */
function cleanCompany(raw) {
  if (!raw) return '';
  let name = raw.trim();
  // If the name is ALL CAPS (more than 3 chars), title-case it
  if (name.length > 3 && name === name.toUpperCase()) {
    name = name.replace(/\b\w+/g, word => {
      // Keep common abbreviations uppercase
      const abbrevs = ['LLC', 'INC', 'LTD', 'ISD', 'CDL', 'DBA', 'USA', 'CDI'];
      if (abbrevs.includes(word.toUpperCase())) return word.toUpperCase();
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    });
  }
  return name;
}

/**
 * Write leads to CSV file.
 */
function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => {
      const val = (lead[col] || '').toString();
      return val.includes(',') || val.includes('"') || val.includes('\n')
        ? `"${val.replace(/"/g, '""')}"` : val;
    }).join(',')
  );
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, [header, ...rows].join('\n'));
}

// =========================================================================
// SOURCE 1: Ontario (Canada) — Government-Approved Driving Schools CSV
// =========================================================================

const ONTARIO_CSV_URL = 'https://data.ontario.ca/dataset/b94f3a8a-7af1-4414-87fe-9e9c226e134a/resource/f0e8d85b-3711-45c3-a787-c2227eaf09e4/download/driving_school_website_march_12_2026_english.csv';

async function scrapeOntario() {
  log('=== Ontario: Fetching government-approved driving schools CSV ===');
  const csvText = await httpGet(ONTARIO_CSV_URL);
  const rows = parseCSV(csvText);
  log(`  Parsed ${rows.length} rows from Ontario CSV`);

  const leads = [];
  for (const row of rows) {
    const company = cleanCompany(row['School'] || '');
    const city = (row['City'] || '').trim();
    const phone = normalizePhone(row['Phone'] || '');
    const address = (row['Address'] || '').trim();

    if (!company) continue;

    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company,
      phone,
      city,
      state: 'ON',
      country: 'CA',
      source: 'ontario_mto_driving_schools',
    });

    if (TEST_MODE && leads.length >= 20) break;
  }

  log(`  Ontario: ${leads.length} driving schools extracted`);
  return leads;
}

// =========================================================================
// SOURCE 2: New York (USA) — SODA API (data.ny.gov)
// =========================================================================

const NY_API_BASE = 'https://data.ny.gov/resource/3p6i-cv8y.json';
const NY_PAGE_SIZE = 1000;

async function scrapeNewYork() {
  log('=== New York: Fetching DMV driving schools via SODA API ===');
  const allLeads = [];
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const url = `${NY_API_BASE}?$limit=${NY_PAGE_SIZE}&$offset=${offset}&$order=school_name`;
    log(`  Fetching offset=${offset}...`);
    const text = await httpGet(url);
    let records;
    try {
      records = JSON.parse(text);
    } catch (e) {
      log(`  Error parsing JSON at offset=${offset}: ${e.message}`);
      break;
    }

    if (!Array.isArray(records) || records.length === 0) {
      hasMore = false;
      break;
    }

    for (const rec of records) {
      const company = cleanCompany(rec.school_name || '');
      const city = (rec.city || '').trim();
      const state = (rec.state || 'NY').trim();
      const phone = normalizePhone(rec.phone_number || '');
      const zip = (rec.zip || '').trim();

      if (!company) continue;

      allLeads.push({
        email: '',
        first_name: '',
        last_name: '',
        company,
        phone,
        city: city || '',
        state: state || 'NY',
        country: 'US',
        source: 'ny_dmv_driving_schools',
      });

      if (TEST_MODE && allLeads.length >= 20) break;
    }

    if (TEST_MODE && allLeads.length >= 20) break;

    offset += records.length;
    if (records.length < NY_PAGE_SIZE) hasMore = false;

    if (hasMore) await sleep(DELAY_MS);
  }

  log(`  New York: ${allLeads.length} driving schools extracted`);
  return allLeads;
}

// =========================================================================
// SOURCE 3: Texas Driver Education Providers — TDLR CSV
// =========================================================================

const TEXAS_EDU_CSV_URL = 'https://www.tdlr.texas.gov/dbproduction2/vsDriverEduProvider.csv';

async function scrapeTexasEdu() {
  log('=== Texas: Fetching TDLR driver education providers CSV ===');
  const csvText = await httpGet(TEXAS_EDU_CSV_URL);
  const rows = parseCSV(csvText);
  log(`  Parsed ${rows.length} rows from Texas Driver Education CSV`);

  const leads = [];
  for (const row of rows) {
    const status = (row['License Status'] || '').toUpperCase();
    // Only include CURRENT licenses
    if (status !== 'CURRENT') continue;

    const company = cleanCompany(row['Licensee'] || '');
    const city = (row['City'] || '').trim();
    const state = (row['State'] || 'TX').trim();
    const phone = normalizePhone(row['Phone'] || '');
    const county = (row['County'] || '').trim();

    if (!company) continue;

    // Skip ISDs (school districts) — they're public schools, not driving school businesses
    const companyUpper = company.toUpperCase();
    if (companyUpper.includes(' ISD') || companyUpper.endsWith(' ISD')) continue;

    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company,
      phone,
      city: cleanCompany(city), // title-case the city too
      state: state || 'TX',
      country: 'US',
      source: 'texas_tdlr_driver_education',
    });

    if (TEST_MODE && leads.length >= 20) break;
  }

  log(`  Texas Education: ${leads.length} driving schools extracted (CURRENT, non-ISD)`);
  return leads;
}

// =========================================================================
// SOURCE 4: Texas Driving Safety Providers — TDLR CSV
// =========================================================================

const TEXAS_SAFETY_CSV_URL = 'https://www.tdlr.texas.gov/dbproduction2/vsDriverSafetyProvider.csv';

async function scrapeTexasSafety() {
  log('=== Texas: Fetching TDLR driving safety providers CSV ===');
  const csvText = await httpGet(TEXAS_SAFETY_CSV_URL);
  const rows = parseCSV(csvText);
  log(`  Parsed ${rows.length} rows from Texas Driving Safety CSV`);

  const leads = [];
  for (const row of rows) {
    const status = (row['License Status'] || '').toUpperCase();
    if (status !== 'CURRENT') continue;

    const company = cleanCompany(row['Licensee'] || '');
    const city = (row['City'] || '').trim();
    const state = (row['State'] || 'TX').trim();
    const phone = normalizePhone(row['Phone'] || '');

    if (!company) continue;

    leads.push({
      email: '',
      first_name: '',
      last_name: '',
      company,
      phone,
      city: cleanCompany(city),
      state: state || 'TX',
      country: 'US',
      source: 'texas_tdlr_driving_safety',
    });

    if (TEST_MODE && leads.length >= 20) break;
  }

  log(`  Texas Safety: ${leads.length} driving safety providers extracted (CURRENT only)`);
  return leads;
}

// =========================================================================
// Dedup + Main
// =========================================================================

/**
 * Deduplicate leads by normalized company name + city + state.
 */
function dedup(leads) {
  const seen = new Set();
  const unique = [];
  for (const lead of leads) {
    const key = `${lead.company.toLowerCase().replace(/[^a-z0-9]/g, '')}|${lead.city.toLowerCase()}|${lead.state.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(lead);
  }
  return unique;
}

/**
 * Print stats summary.
 */
function printStats(leads) {
  const bySource = {};
  const byCountry = {};
  let withPhone = 0;
  let withEmail = 0;

  for (const lead of leads) {
    bySource[lead.source] = (bySource[lead.source] || 0) + 1;
    byCountry[lead.country] = (byCountry[lead.country] || 0) + 1;
    if (lead.phone) withPhone++;
    if (lead.email) withEmail++;
  }

  log('');
  log('=== RESULTS ===');
  log(`  Total leads:    ${leads.length}`);
  log(`  With phone:     ${withPhone}`);
  log(`  With email:     ${withEmail}`);
  log('');
  log('  By source:');
  for (const [source, count] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
    log(`    ${source}: ${count}`);
  }
  log('');
  log('  By country:');
  for (const [country, count] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
    log(`    ${country}: ${count}`);
  }
}

async function main() {
  const startTime = Date.now();
  log(`Driving School Scraper — ${TEST_MODE ? 'TEST MODE' : 'FULL SCRAPE'}`);
  log(`Source filter: ${SOURCE_FILTER}`);
  log('');

  const sourceMap = {
    'ontario': scrapeOntario,
    'ny': scrapeNewYork,
    'texas-edu': scrapeTexasEdu,
    'texas-safety': scrapeTexasSafety,
  };

  let allLeads = [];

  if (SOURCE_FILTER === 'all') {
    // Run all sources sequentially with delays between
    for (const [name, fn] of Object.entries(sourceMap)) {
      try {
        const leads = await fn();
        allLeads.push(...leads);
        log('');
      } catch (err) {
        log(`  ERROR in ${name}: ${err.message}`);
        log('  Continuing with next source...');
        log('');
      }
      await sleep(DELAY_MS);
    }
  } else {
    const fn = sourceMap[SOURCE_FILTER];
    if (!fn) {
      log(`Unknown source: ${SOURCE_FILTER}`);
      log(`Valid sources: ${Object.keys(sourceMap).join(', ')}, all`);
      process.exit(1);
    }
    const leads = await fn();
    allLeads.push(...leads);
  }

  // Dedup across all sources
  const before = allLeads.length;
  allLeads = dedup(allLeads);
  const dupes = before - allLeads.length;
  if (dupes > 0) log(`  Deduped: removed ${dupes} duplicates`);

  // Write output
  if (allLeads.length > 0) {
    writeCSV(OUTPUT_FILE, allLeads);
    log(`\n  Output written to: ${OUTPUT_FILE}`);
  } else {
    log('\n  No leads found.');
  }

  printStats(allLeads);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log(`\n  Done in ${elapsed}s`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
