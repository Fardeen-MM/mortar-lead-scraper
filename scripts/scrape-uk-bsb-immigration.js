#!/usr/bin/env node
/**
 * UK BSB Immigration Barristers Scraper
 *
 * Downloads the official BSB (Bar Standards Board) Barristers Register CSV
 * from https://www.barstandardsboard.org.uk and filters for immigration barristers.
 *
 * The BSB publishes a downloadable CSV of all ~4,600+ practising barristers in
 * England & Wales. This script:
 *   1. Downloads the full register CSV (or uses cached copy)
 *   2. Parses all records
 *   3. Filters for barristers with "Immigration" in their Areas of practice
 *   4. Optionally fetches BSB profile pages for each barrister (Puppeteer)
 *   5. Outputs filtered results in standard lead format
 *
 * Data fields from BSB CSV:
 *   - Title, Forenames, Surname, Honours
 *   - Primary status, Member status
 *   - Date of call, Inn of Court
 *   - Address Name (chambers), Address Floor Street, Town, County, Postcode, Country
 *   - Address Tel, Address Fax
 *   - Areas of practice (comma-separated list)
 *   - Public Access (Y/N)
 *   - Other Entitlements (includes "Immigration Work")
 *
 * Output: output/uk-immigration-barristers-bsb.csv
 *
 * Usage:
 *   node scripts/scrape-uk-bsb-immigration.js              # Full run
 *   node scripts/scrape-uk-bsb-immigration.js --test        # Test mode (first 10 records)
 *   node scripts/scrape-uk-bsb-immigration.js --no-cache    # Force re-download of BSB CSV
 *   node scripts/scrape-uk-bsb-immigration.js --profiles    # Also fetch BSB profile pages (slow)
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ── Config ───────────────────────────────────────────────────────────────────

const BSB_CSV_URL = 'https://www.barstandardsboard.org.uk/asset/55AEDEAE%2D4A33%2D4D31%2DB673038F6FBB473C/';
const BSB_REGISTER_URL = 'https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html';
const CACHE_DIR = path.join(__dirname, '..', 'output', '.cache');
const CACHE_FILE = path.join(CACHE_DIR, 'bsb-register-full.csv');
const CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'uk-immigration-barristers-bsb.csv');
const PROGRESS_FILE = OUTPUT_FILE.replace('.csv', '-progress.json');
const REQUEST_TIMEOUT = 60000;
const DELAY_MIN = 2000;
const DELAY_MAX = 3000;

const CSV_COLUMNS = [
  'first_name', 'last_name', 'firm_name', 'title', 'email', 'phone',
  'website', 'domain', 'city', 'state', 'country', 'niche',
  'source', 'profile_url',
];

// BSB CSV column indices (0-based)
const COL = {
  TITLE: 0,
  FORENAMES: 1,
  SURNAME: 2,
  HONOURS: 3,
  PRIMARY_STATUS: 4,
  MEMBER_STATUS: 5,
  DATE_OF_CALL: 6,
  INN_OF_COURT: 7,
  PRACTISING_CERT: 8,
  ADDRESS_NAME: 9,
  ADDRESS_STREET: 10,
  ADDRESS_TOWN: 11,
  ADDRESS_COUNTY: 12,
  ADDRESS_POSTCODE: 13,
  ADDRESS_COUNTRY: 14,
  ADDRESS_TEL: 15,
  ADDRESS_FAX: 16,
  RIGHTS_OF_AUDIENCE: 17,
  CONDUCT_LITIGATION: 18,
  AREAS_OF_PRACTICE: 19,
  PUBLIC_ACCESS: 20,
  YOUTH_COURTS: 21,
  OTHER_ENTITLEMENTS: 22,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomDelay() {
  return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN));
}

function log(msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

/**
 * Parse a single CSV line handling quoted fields with embedded commas and quotes.
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
      fields.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  fields.push(current.trim());
  return fields;
}

/**
 * Clean and normalize a UK phone number.
 * BSB format: "(+44) 020 74057311" → "+44 20 7405 7311"
 */
function cleanPhone(phone) {
  if (!phone) return '';
  let cleaned = phone.trim();
  if (!cleaned) return '';

  // Remove (+44) prefix and normalize
  cleaned = cleaned
    .replace(/^\(\+44\)\s*/, '+44 ')
    .replace(/\s+/g, ' ')
    .trim();

  // Remove leading 0 after +44 (e.g., "+44 020..." → "+44 20...")
  cleaned = cleaned.replace(/^\+44\s*0/, '+44 ');

  return cleaned;
}

/**
 * Normalize city name: uppercase → title case, trim.
 */
function normalizeCity(city) {
  if (!city) return '';
  return city.trim()
    .toLowerCase()
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/\bEc\d/gi, m => m.toUpperCase()) // EC1, EC2, etc.
    .replace(/\bWc\d/gi, m => m.toUpperCase()); // WC1, WC2, etc.
}

/**
 * Build a BSB register search URL for a barrister by name.
 * This isn't a guaranteed profile URL but allows direct lookup.
 */
function buildProfileUrl(forenames, surname) {
  const name = `${forenames} ${surname}`.trim();
  if (!name) return '';
  const encoded = encodeURIComponent(name);
  return `${BSB_REGISTER_URL}?view=practising&search_field=contact&q=${encoded}`;
}

function csvEscape(val) {
  if (!val) return '';
  const str = String(val).replace(/[\r\n]+/g, ' ').trim();
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function writeCSV(leads, outPath) {
  const header = CSV_COLUMNS.join(',');
  const rows = leads.map(lead =>
    CSV_COLUMNS.map(col => csvEscape(lead[col] || '')).join(',')
  );
  fs.writeFileSync(outPath, header + '\n' + rows.join('\n') + '\n');
}

/**
 * Load existing CSV for resume support.
 */
function loadExistingCSV(csvPath) {
  const result = { leads: [], seenKeys: new Set() };
  if (!fs.existsSync(csvPath)) return result;

  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length <= 1) return result;

  const headers = lines[0].split(',');
  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    const lead = {};
    for (let j = 0; j < headers.length && j < fields.length; j++) {
      lead[headers[j]] = fields[j];
    }

    // Dedup key: first_name + last_name + city (handles same-name barristers in different cities/chambers)
    const key = `${(lead.first_name || '').toLowerCase()}|${(lead.last_name || '').toLowerCase()}|${(lead.city || '').toLowerCase()}`;
    if (key !== '||') result.seenKeys.add(key);
    result.leads.push(lead);
  }

  return result;
}

/**
 * Load/save progress tracker.
 */
function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
  } catch {}
  return { downloadedAt: null, processedCount: 0 };
}

function saveProgress(data) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({
    ...data,
    updatedAt: new Date().toISOString(),
  }, null, 2));
}

// ── Download BSB CSV ─────────────────────────────────────────────────────────

/**
 * Download the BSB register CSV file via HTTPS.
 * Returns the file content as a string.
 */
function downloadBSBCSV() {
  return new Promise((resolve, reject) => {
    log('Downloading BSB Register CSV...');
    log(`  URL: ${BSB_CSV_URL}`);

    const makeRequest = (url, redirectCount = 0) => {
      if (redirectCount > 5) {
        return reject(new Error('Too many redirects'));
      }

      const mod = url.startsWith('https') ? https : require('http');
      const req = mod.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/csv,text/plain,*/*',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Referer': 'https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html',
        },
        timeout: REQUEST_TIMEOUT,
      }, (res) => {
        // Handle redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirectUrl = res.headers.location;
          if (redirectUrl.startsWith('/')) {
            redirectUrl = 'https://www.barstandardsboard.org.uk' + redirectUrl;
          }
          log(`  Redirect ${res.statusCode} → ${redirectUrl}`);
          return makeRequest(redirectUrl, redirectCount + 1);
        }

        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} downloading BSB CSV`));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          log(`  Downloaded ${(data.length / 1024).toFixed(0)} KB`);
          resolve(data);
        });
      });

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Download timed out'));
      });
    };

    makeRequest(BSB_CSV_URL);
  });
}

/**
 * Get BSB CSV content, using cache if available and fresh.
 */
async function getBSBCSV(noCache = false) {
  // Check cache
  if (!noCache && fs.existsSync(CACHE_FILE)) {
    const stat = fs.statSync(CACHE_FILE);
    const ageMs = Date.now() - stat.mtimeMs;
    if (ageMs < CACHE_MAX_AGE_MS) {
      const ageHours = (ageMs / 3600000).toFixed(1);
      log(`Using cached BSB CSV (${ageHours}h old, max ${CACHE_MAX_AGE_MS / 3600000}h)`);
      return fs.readFileSync(CACHE_FILE, 'utf8');
    } else {
      log('Cache expired, re-downloading...');
    }
  }

  // Download fresh copy
  const content = await downloadBSBCSV();

  // Cache it
  if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });
  fs.writeFileSync(CACHE_FILE, content);
  log(`  Cached to ${CACHE_FILE}`);

  return content;
}

// ── Parse & Filter ───────────────────────────────────────────────────────────

/**
 * Parse the BSB CSV and return all records as objects.
 */
function parseBSBCSV(content) {
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length <= 1) return [];

  const headers = parseCSVLine(lines[0]);
  const records = [];

  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    if (fields.length < 20) continue; // Skip malformed lines

    records.push({
      title: fields[COL.TITLE] || '',
      forenames: fields[COL.FORENAMES] || '',
      surname: fields[COL.SURNAME] || '',
      honours: fields[COL.HONOURS] || '',
      primary_status: fields[COL.PRIMARY_STATUS] || '',
      member_status: fields[COL.MEMBER_STATUS] || '',
      date_of_call: fields[COL.DATE_OF_CALL] || '',
      inn_of_court: fields[COL.INN_OF_COURT] || '',
      practising_cert: fields[COL.PRACTISING_CERT] || '',
      address_name: fields[COL.ADDRESS_NAME] || '',
      address_street: fields[COL.ADDRESS_STREET] || '',
      address_town: fields[COL.ADDRESS_TOWN] || '',
      address_county: fields[COL.ADDRESS_COUNTY] || '',
      address_postcode: fields[COL.ADDRESS_POSTCODE] || '',
      address_country: fields[COL.ADDRESS_COUNTRY] || '',
      address_tel: fields[COL.ADDRESS_TEL] || '',
      address_fax: fields[COL.ADDRESS_FAX] || '',
      areas_of_practice: fields[COL.AREAS_OF_PRACTICE] || '',
      public_access: fields[COL.PUBLIC_ACCESS] || '',
      other_entitlements: fields[COL.OTHER_ENTITLEMENTS] || '',
    });
  }

  return records;
}

/**
 * Filter BSB records for immigration barristers.
 * Matches "Immigration" in the "Areas of practice" field.
 */
function filterImmigration(records) {
  return records.filter(r => {
    const areas = r.areas_of_practice.toLowerCase();
    return areas.includes('immigration');
  });
}

/**
 * Transform a BSB record into a standard lead format.
 */
function transformToLead(record) {
  // Extract first name from forenames (may have multiple names like "John William")
  const forenames = record.forenames.trim();
  const forenameParts = forenames.split(/\s+/).filter(Boolean);
  const first_name = forenameParts[0] || '';
  const last_name = record.surname.trim();

  // Build title from BSB title + honours
  let title = '';
  const bsbTitle = record.title.trim();
  const honours = record.honours.trim();
  if (honours) {
    title = honours; // KC, QC, etc.
  } else if (bsbTitle && !['Mr', 'Mrs', 'Ms', 'Miss'].includes(bsbTitle)) {
    title = bsbTitle; // Dr, Professor, etc. — skip generic Mr/Mrs/Ms
  }

  // Firm = chambers name (Address Name)
  const firm_name = record.address_name.trim();

  // Phone
  const phone = cleanPhone(record.address_tel);

  // City — normalize from uppercase
  const city = normalizeCity(record.address_town);

  // Profile URL — BSB register search link
  const profile_url = buildProfileUrl(forenames, last_name);

  return {
    first_name,
    last_name,
    firm_name,
    title,
    email: '',          // BSB CSV doesn't include email
    phone,
    website: '',        // BSB CSV doesn't include website
    domain: '',
    city,
    state: 'UK-EW',
    country: 'UK',
    niche: 'immigration',
    source: 'bsb_barristers',
    profile_url,
  };
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const noCache = args.includes('--no-cache');
  const noResume = args.includes('--no-resume');
  const withProfiles = args.includes('--profiles');

  log('========================================');
  log('  BSB Immigration Barristers Scraper');
  log('========================================');
  log(`Mode: ${isTest ? 'TEST (first 10)' : 'FULL'}`);
  log(`Cache: ${noCache ? 'DISABLED' : 'ENABLED (24h)'}`);
  log(`Resume: ${noResume ? 'DISABLED' : 'ENABLED'}`);
  log('');
  log('Source: BSB Barristers Register (official CSV download)');
  log(`URL: ${BSB_CSV_URL}`);
  log('');

  // Ensure output directory exists
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Resume support — load existing output CSV
  let existingLeads = [];
  let seenKeys = new Set();
  if (!noResume) {
    const existing = loadExistingCSV(OUTPUT_FILE);
    if (existing.leads.length > 0) {
      existingLeads = existing.leads;
      seenKeys = existing.seenKeys;
      log(`RESUME: loaded ${existingLeads.length} existing leads from CSV`);
    }
  }

  // Step 1: Download or use cached BSB CSV
  log('Step 1: Fetching BSB Register CSV...');
  let csvContent;
  try {
    csvContent = await getBSBCSV(noCache);
  } catch (err) {
    log(`FATAL: Failed to download BSB CSV: ${err.message}`);
    process.exit(1);
  }

  // Step 2: Parse all records
  log('Step 2: Parsing BSB records...');
  const allRecords = parseBSBCSV(csvContent);
  log(`  Total barristers in register: ${allRecords.length}`);

  // Step 3: Filter for immigration
  log('Step 3: Filtering for immigration barristers...');
  let immigrationRecords = filterImmigration(allRecords);
  log(`  Immigration barristers found: ${immigrationRecords.length}`);

  // Test mode — limit to first 10
  if (isTest) {
    immigrationRecords = immigrationRecords.slice(0, 10);
    log(`  TEST MODE: limited to ${immigrationRecords.length} records`);
  }

  // Step 4: Transform to standard lead format
  log('Step 4: Transforming to lead format...');
  const newLeads = [];
  let skippedDup = 0;

  for (const record of immigrationRecords) {
    const lead = transformToLead(record);

    // Dedup check: first_name + last_name + city (handles same-name barristers)
    const key = `${lead.first_name.toLowerCase()}|${lead.last_name.toLowerCase()}|${lead.city.toLowerCase()}`;
    if (key !== '||' && seenKeys.has(key)) {
      skippedDup++;
      continue;
    }
    if (key !== '||') seenKeys.add(key);

    newLeads.push(lead);
  }

  log(`  New leads: ${newLeads.length}`);
  if (skippedDup > 0) log(`  Skipped (already in CSV): ${skippedDup}`);

  // Merge with existing leads
  const allLeads = [...existingLeads, ...newLeads];

  // Step 5: Write output CSV
  log('Step 5: Writing output CSV...');
  writeCSV(allLeads, OUTPUT_FILE);
  log(`  Saved to: ${OUTPUT_FILE}`);

  // Save progress
  saveProgress({
    downloadedAt: new Date().toISOString(),
    processedCount: immigrationRecords.length,
    totalInRegister: allRecords.length,
  });

  // ── Stats ──────────────────────────────────────────────────────────────────

  const withPhone = allLeads.filter(l => l.phone).length;
  const withFirm = allLeads.filter(l => l.firm_name).length;
  const withTitle = allLeads.filter(l => l.title).length;
  const withCity = allLeads.filter(l => l.city).length;
  const withName = allLeads.filter(l => l.first_name && l.last_name).length;

  // Practice area breakdown for immigration barristers
  const practiceAreaCounts = {};
  for (const record of immigrationRecords) {
    const areas = record.areas_of_practice.split(',').map(a => a.trim()).filter(Boolean);
    for (const area of areas) {
      practiceAreaCounts[area] = (practiceAreaCounts[area] || 0) + 1;
    }
  }

  // City breakdown
  const cityCounts = {};
  for (const lead of allLeads) {
    const city = lead.city || 'Unknown';
    cityCounts[city] = (cityCounts[city] || 0) + 1;
  }
  const topCities = Object.entries(cityCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  // Member status breakdown
  const statusCounts = {};
  for (const record of immigrationRecords) {
    const status = record.member_status || 'Unknown';
    statusCounts[status] = (statusCounts[status] || 0) + 1;
  }

  // Public access breakdown
  const publicAccessCount = immigrationRecords.filter(r => r.public_access === 'Y').length;

  log('');
  log('='.repeat(60));
  log('RESULTS');
  log('='.repeat(60));
  log(`Total barristers in BSB register: ${allRecords.length}`);
  log(`Immigration barristers found: ${immigrationRecords.length}`);
  log(`New leads added: ${newLeads.length}`);
  log(`Total leads in output: ${allLeads.length}`);
  log('');
  log('--- Lead Quality ---');
  log(`  With full name: ${withName}`);
  log(`  With phone: ${withPhone}`);
  log(`  With chambers name: ${withFirm}`);
  log(`  With title/honours: ${withTitle}`);
  log(`  With city: ${withCity}`);
  log('');
  log('--- Public Access (can be instructed directly by public) ---');
  log(`  Yes: ${publicAccessCount} / ${immigrationRecords.length}`);
  log('');
  log('--- Top Cities ---');
  for (const [city, count] of topCities) {
    log(`  ${city}: ${count}`);
  }
  log('');
  log('--- Member Status ---');
  for (const [status, count] of Object.entries(statusCounts).sort((a, b) => b[1] - a[1])) {
    log(`  ${status}: ${count}`);
  }
  log('');
  log('--- Co-occurring Practice Areas ---');
  for (const [area, count] of Object.entries(practiceAreaCounts).sort((a, b) => b[1] - a[1]).slice(0, 10)) {
    log(`  ${area}: ${count}`);
  }
  log('');
  log(`Output: ${OUTPUT_FILE}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
