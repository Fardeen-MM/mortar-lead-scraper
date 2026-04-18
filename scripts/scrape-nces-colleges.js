#!/usr/bin/env node
/**
 * NCES IPEDS US Colleges & Universities Scraper
 *
 * Downloads and parses the IPEDS (Integrated Postsecondary Education Data System)
 * institutional characteristics data from the National Center for Education Statistics.
 *
 * Data source: https://nces.ed.gov/ipeds/use-the-data
 * File: HD{YEAR}.zip — "Directory information" (institutional characteristics header)
 *
 * This is a FREE US government dataset containing every college, university, and
 * postsecondary institution in the United States (~6,500+ active institutions).
 *
 * Key fields extracted:
 *   - INSTNM:   Institution name
 *   - GENTELE:  General information telephone number
 *   - WEBADDR:  Institution website URL
 *   - ADMINURL: Admissions office web address
 *   - FAIDURL:  Financial aid office web address
 *   - CHFNM:    Chief administrator name
 *   - CHFTITLE: Chief administrator title
 *   - CITY:     City
 *   - STABBR:   State abbreviation
 *   - ZIP:      ZIP code
 *   - ADDR:     Street address
 *   - SECTOR:   Sector (public 4yr, private 4yr, public 2yr, etc.)
 *   - CONTROL:  Control (1=Public, 2=Private nonprofit, 3=Private for-profit)
 *   - ICLEVEL:  Level (1=4yr+, 2=2-4yr, 3=<2yr)
 *   - DEGGRANT: Degree-granting status (1=yes, 2=no)
 *   - INSTCAT:  Institutional category
 *   - LONGITUD: Longitude
 *   - LATITUDE: Latitude
 *
 * Filters applied:
 *   - Active institutions only (CYACTIVE=1)
 *   - Not closed (CLOSEDAT is empty)
 *   - Not deleted (DEATHYR is empty)
 *   - Currently operating postsecondary (POSTSEC=1)
 *
 * Zero npm dependencies — uses only Node.js built-in modules (http, https, zlib, fs, path).
 *
 * Usage:
 *   node scripts/scrape-nces-colleges.js                    # latest year (HD2023)
 *   node scripts/scrape-nces-colleges.js --year=2022        # specific year
 *   node scripts/scrape-nces-colleges.js --test             # download + show first 20 rows
 *   node scripts/scrape-nces-colleges.js --degree-granting  # only degree-granting institutions
 *   node scripts/scrape-nces-colleges.js --min-level=1      # only 4yr+ institutions
 *   node scripts/scrape-nces-colleges.js --control=1        # only public institutions
 *   node scripts/scrape-nces-colleges.js --with-phone       # only institutions with phone
 *   node scripts/scrape-nces-colleges.js --with-website     # only institutions with website
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const zlib = require('zlib');

// ── CLI args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}

const YEAR = parseInt(args.year) || 2023;
const TEST_MODE = !!args.test;
const DEGREE_GRANTING_ONLY = !!args['degree-granting'];
const MIN_LEVEL = parseInt(args['min-level']) || 0;   // 1=4yr+, 2=2-4yr, 3=<2yr
const CONTROL_FILTER = parseInt(args.control) || 0;    // 1=public, 2=private NP, 3=private FP
const WITH_PHONE = !!args['with-phone'];
const WITH_WEBSITE = !!args['with-website'];

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'nces-us-colleges.csv');
const CSV_HEADER = 'email,first_name,last_name,company,phone,city,state,country,source';

// IPEDS data file URL pattern (confirmed from Urban Institute ipeds-scraper + paulgp/ipeds-database)
// Format: https://nces.ed.gov/ipeds/datacenter/data/{SURVEY}{YEAR}.zip
const ZIP_URL = `https://nces.ed.gov/ipeds/datacenter/data/HD${YEAR}.zip`;

// Fallback URLs in case primary fails (try previous years)
const FALLBACK_URLS = [
  `https://nces.ed.gov/ipeds/datacenter/data/HD${YEAR - 1}.zip`,
  `https://nces.ed.gov/ipeds/datacenter/data/HD${YEAR - 2}.zip`,
];

// ── Sector labels ────────────────────────────────────────────────────────
const SECTOR_LABELS = {
  0: 'Administrative Unit',
  1: 'Public, 4-year or above',
  2: 'Private not-for-profit, 4-year or above',
  3: 'Private for-profit, 4-year or above',
  4: 'Public, 2-year',
  5: 'Private not-for-profit, 2-year',
  6: 'Private for-profit, 2-year',
  7: 'Public, less-than 2-year',
  8: 'Private not-for-profit, less-than 2-year',
  9: 'Private for-profit, less-than 2-year',
  99: 'Sector unknown (not active)',
};

const CONTROL_LABELS = {
  1: 'Public',
  2: 'Private not-for-profit',
  3: 'Private for-profit',
};

const LEVEL_LABELS = {
  1: 'Four or more years',
  2: 'At least 2 but less than 4 years',
  3: 'Less than 2 years (below associate)',
  '-3': 'Not available',
};

// ── HTTP helpers ────────────────────────────────────────────────────────
function httpGetBuffer(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    if (maxRedirects <= 0) return reject(new Error(`Too many redirects for ${url}`));
    const mod = url.startsWith('https') ? https : http;
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      port: parsedUrl.port || undefined,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'identity',
      },
    };
    const req = mod.request(options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsedUrl.protocol}//${parsedUrl.hostname}${res.headers.location}`;
        res.resume();
        httpGetBuffer(redirectUrl, maxRedirects - 1).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

// ── ZIP extraction (minimal — handles the common IPEDS ZIP format) ──────
// IPEDS ZIPs contain a single CSV file. We use Node's built-in zlib for
// deflate decompression and manually parse the ZIP local file headers.
function extractCSVFromZip(zipBuffer) {
  // ZIP local file header signature: PK\x03\x04
  const LOCAL_FILE_HEADER_SIG = 0x04034b50;

  let offset = 0;
  const files = [];

  while (offset < zipBuffer.length - 4) {
    const sig = zipBuffer.readUInt32LE(offset);
    if (sig !== LOCAL_FILE_HEADER_SIG) break;

    const compressionMethod = zipBuffer.readUInt16LE(offset + 8);
    const compressedSize = zipBuffer.readUInt32LE(offset + 18);
    const uncompressedSize = zipBuffer.readUInt32LE(offset + 22);
    const fileNameLength = zipBuffer.readUInt16LE(offset + 26);
    const extraFieldLength = zipBuffer.readUInt16LE(offset + 28);
    const fileName = zipBuffer.toString('utf8', offset + 30, offset + 30 + fileNameLength);
    const dataOffset = offset + 30 + fileNameLength + extraFieldLength;

    const compressedData = zipBuffer.slice(dataOffset, dataOffset + compressedSize);

    files.push({
      fileName,
      compressionMethod,
      compressedData,
      compressedSize,
      uncompressedSize,
    });

    offset = dataOffset + compressedSize;

    // Skip optional data descriptor (ZIP bit 3)
    if (offset + 4 <= zipBuffer.length) {
      const possibleSig = zipBuffer.readUInt32LE(offset);
      // Data descriptor signature: PK\x07\x08
      if (possibleSig === 0x08074b50) {
        offset += 16; // sig(4) + crc(4) + compSize(4) + uncompSize(4)
      }
    }
  }

  // Find the CSV file
  const csvFile = files.find(f =>
    f.fileName.toLowerCase().endsWith('.csv') &&
    !f.fileName.toLowerCase().includes('_rv')  // Skip revised-values file
  ) || files.find(f => f.fileName.toLowerCase().endsWith('.csv'));

  if (!csvFile) {
    const names = files.map(f => f.fileName).join(', ');
    throw new Error(`No CSV file found in ZIP. Files: ${names}`);
  }

  console.log(`  Found CSV: ${csvFile.fileName} (${(csvFile.uncompressedSize / 1024 / 1024).toFixed(1)} MB uncompressed)`);

  // Decompress
  if (csvFile.compressionMethod === 0) {
    // Stored (no compression)
    return csvFile.compressedData.toString('utf8');
  } else if (csvFile.compressionMethod === 8) {
    // Deflated
    const inflated = zlib.inflateRawSync(csvFile.compressedData);
    return inflated.toString('utf8');
  } else {
    throw new Error(`Unsupported compression method: ${csvFile.compressionMethod}`);
  }
}

// ── CSV parsing (handles quoted fields with embedded commas/newlines) ───
function parseCSV(csvText) {
  const rows = [];
  let i = 0;
  const len = csvText.length;

  while (i < len) {
    const row = [];
    let fieldStart = true;

    while (i < len) {
      if (fieldStart && csvText[i] === '"') {
        // Quoted field
        i++; // skip opening quote
        let field = '';
        while (i < len) {
          if (csvText[i] === '"') {
            if (i + 1 < len && csvText[i + 1] === '"') {
              field += '"';
              i += 2;
            } else {
              i++; // skip closing quote
              break;
            }
          } else {
            field += csvText[i];
            i++;
          }
        }
        row.push(field);
        // Skip comma or newline after quoted field
        if (i < len && csvText[i] === ',') {
          i++;
          fieldStart = true;
        } else {
          // End of line
          if (i < len && csvText[i] === '\r') i++;
          if (i < len && csvText[i] === '\n') i++;
          break;
        }
      } else {
        // Unquoted field
        let field = '';
        while (i < len && csvText[i] !== ',' && csvText[i] !== '\n' && csvText[i] !== '\r') {
          field += csvText[i];
          i++;
        }
        row.push(field);
        if (i < len && csvText[i] === ',') {
          i++;
          fieldStart = true;
        } else {
          // End of line
          if (i < len && csvText[i] === '\r') i++;
          if (i < len && csvText[i] === '\n') i++;
          break;
        }
      }
    }

    if (row.length > 0 && !(row.length === 1 && row[0] === '')) {
      rows.push(row);
    }
  }

  return rows;
}

// ── Helper functions ────────────────────────────────────────────────────
function escapeCSV(val) {
  const s = (val || '').toString().trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function normalizePhone(phone) {
  if (!phone) return '';
  // IPEDS GENTELE format is typically 10 digits like "2125551234" or "(212) 555-1234"
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return phone.trim();
}

function normalizeWebsite(url) {
  if (!url) return '';
  url = url.trim();
  if (url === '-2' || url === '-1' || url === 'NA' || url === '.') return '';
  // Remove trailing slashes
  url = url.replace(/\/+$/, '');
  // Add https:// if no protocol
  if (url && !url.match(/^https?:\/\//i)) {
    url = 'https://' + url;
  }
  return url;
}

function splitName(fullName) {
  if (!fullName || fullName === '-2' || fullName === '-1') return { first: '', last: '' };
  fullName = fullName.trim();

  // Remove titles/prefixes
  fullName = fullName
    .replace(/^(Dr\.?|Mr\.?|Ms\.?|Mrs\.?|Prof\.?|Rev\.?|Hon\.?|Sir|Dame)\s+/i, '')
    .trim();

  // Remove suffixes
  fullName = fullName
    .replace(/,?\s*(Jr\.?|Sr\.?|III|II|IV|Ph\.?D\.?|Ed\.?D\.?|M\.?D\.?|J\.?D\.?|Esq\.?)$/i, '')
    .trim();

  const parts = fullName.split(/\s+/);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: '', last: parts[0] };

  // First name is the first part, last name is everything else
  const first = parts[0];
  const last = parts.slice(1).join(' ');
  return { first, last };
}

function cleanField(val) {
  if (!val) return '';
  val = val.trim();
  // IPEDS uses -1 for "Not reported" and -2 for "Not applicable"
  if (val === '-1' || val === '-2' || val === '-3') return '';
  return val;
}

function writeCSV(filePath, leads) {
  if (!leads.length) return;
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const rows = leads.map(l =>
    [l.email, l.first_name, l.last_name, l.company, l.phone, l.city, l.state, l.country, l.source]
      .map(escapeCSV)
      .join(',')
  );
  fs.writeFileSync(filePath, [CSV_HEADER, ...rows].join('\n'));
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  const startTime = Date.now();
  console.log('');
  console.log('=============================================================');
  console.log('  MORTAR -- NCES IPEDS US Colleges & Universities Scraper');
  console.log('=============================================================');
  console.log(`  Data year:        HD${YEAR}`);
  console.log(`  Source URL:       ${ZIP_URL}`);
  console.log(`  Mode:             ${TEST_MODE ? 'TEST (preview first 20 rows)' : 'FULL'}`);
  if (DEGREE_GRANTING_ONLY) console.log('  Filter:           Degree-granting only');
  if (MIN_LEVEL) console.log(`  Min level:        ${MIN_LEVEL} (${LEVEL_LABELS[MIN_LEVEL] || 'unknown'})`);
  if (CONTROL_FILTER) console.log(`  Control:          ${CONTROL_FILTER} (${CONTROL_LABELS[CONTROL_FILTER] || 'unknown'})`);
  if (WITH_PHONE) console.log('  Filter:           With phone only');
  if (WITH_WEBSITE) console.log('  Filter:           With website only');
  console.log(`  Output:           ${OUTPUT_FILE}`);
  console.log('');

  // Step 1: Download the ZIP file
  console.log('--- Step 1: Download IPEDS ZIP ---');
  let zipBuffer;
  let usedUrl = ZIP_URL;

  const urls = [ZIP_URL, ...FALLBACK_URLS];
  for (const url of urls) {
    try {
      console.log(`  Downloading ${url}...`);
      zipBuffer = await httpGetBuffer(url);
      usedUrl = url;
      console.log(`  Downloaded: ${(zipBuffer.length / 1024 / 1024).toFixed(1)} MB`);
      break;
    } catch (err) {
      console.log(`  [WARN] Failed: ${err.message}`);
      if (url === urls[urls.length - 1]) {
        throw new Error(`All download URLs failed. Last error: ${err.message}`);
      }
      console.log('  Trying fallback URL...');
    }
  }

  // Step 2: Extract CSV from ZIP
  console.log('\n--- Step 2: Extract CSV from ZIP ---');
  const csvText = extractCSVFromZip(zipBuffer);
  console.log(`  CSV size: ${(csvText.length / 1024 / 1024).toFixed(1)} MB`);

  // Step 3: Parse CSV
  console.log('\n--- Step 3: Parse CSV ---');
  const rows = parseCSV(csvText);
  if (rows.length < 2) {
    throw new Error('CSV has fewer than 2 rows — possibly corrupt or empty');
  }

  const headers = rows[0].map(h => h.trim().toUpperCase());
  const dataRows = rows.slice(1);
  console.log(`  Headers:  ${headers.length} columns`);
  console.log(`  Rows:     ${dataRows.length}`);

  // Map column names to indices
  const colIdx = {};
  for (let i = 0; i < headers.length; i++) {
    colIdx[headers[i]] = i;
  }

  // Verify essential columns exist
  const requiredCols = ['INSTNM', 'CITY', 'STABBR', 'ZIP'];
  const missingCols = requiredCols.filter(c => colIdx[c] === undefined);
  if (missingCols.length > 0) {
    throw new Error(`Missing required columns: ${missingCols.join(', ')}. Available: ${headers.slice(0, 30).join(', ')}...`);
  }

  // Show available contact columns for debugging
  const contactCols = ['INSTNM', 'ADDR', 'CITY', 'STABBR', 'ZIP', 'GENTELE', 'WEBADDR',
    'ADMINURL', 'FAIDURL', 'CHFNM', 'CHFTITLE', 'SECTOR', 'ICLEVEL', 'CONTROL',
    'DEGGRANT', 'CYACTIVE', 'CLOSEDAT', 'DEATHYR', 'POSTSEC', 'INSTCAT',
    'LONGITUD', 'LATITUDE', 'EIN', 'OPEID', 'HLOFFER', 'UGOFFER', 'GROFFER'];
  const foundCols = contactCols.filter(c => colIdx[c] !== undefined);
  const notFoundCols = contactCols.filter(c => colIdx[c] === undefined);
  console.log(`  Found:    ${foundCols.join(', ')}`);
  if (notFoundCols.length > 0) {
    console.log(`  Missing:  ${notFoundCols.join(', ')}`);
  }

  // Helper to get value by column name
  const getVal = (row, col) => {
    const idx = colIdx[col];
    if (idx === undefined || idx >= row.length) return '';
    return cleanField(row[idx]);
  };

  // Step 4: Filter and transform
  console.log('\n--- Step 4: Filter & Transform ---');
  const leads = [];
  let filtered = {
    closed: 0,
    dead: 0,
    inactive: 0,
    nonPostsec: 0,
    nondegree: 0,
    levelFiltered: 0,
    controlFiltered: 0,
    noPhone: 0,
    noWebsite: 0,
    empty: 0,
  };

  for (const row of dataRows) {
    // Filter: skip closed institutions
    const closedat = getVal(row, 'CLOSEDAT');
    if (closedat) { filtered.closed++; continue; }

    // Filter: skip deleted/dead institutions
    const deathyr = getVal(row, 'DEATHYR');
    if (deathyr) { filtered.dead++; continue; }

    // Filter: skip inactive institutions
    // CYACTIVE: 1 = Active, anything else = not active
    const cyactive = getVal(row, 'CYACTIVE');
    if (cyactive && cyactive !== '1') { filtered.inactive++; continue; }

    // Filter: must be postsecondary
    const postsec = getVal(row, 'POSTSEC');
    if (postsec && postsec !== '1') { filtered.nonPostsec++; continue; }

    // Filter: degree-granting only
    if (DEGREE_GRANTING_ONLY) {
      const deggrant = getVal(row, 'DEGGRANT');
      if (deggrant !== '1') { filtered.nondegree++; continue; }
    }

    // Filter: institution level
    if (MIN_LEVEL > 0) {
      const iclevel = parseInt(getVal(row, 'ICLEVEL')) || 0;
      if (iclevel === 0 || iclevel > MIN_LEVEL) { filtered.levelFiltered++; continue; }
    }

    // Filter: control type
    if (CONTROL_FILTER > 0) {
      const control = parseInt(getVal(row, 'CONTROL')) || 0;
      if (control !== CONTROL_FILTER) { filtered.controlFiltered++; continue; }
    }

    const instnm = getVal(row, 'INSTNM');
    if (!instnm) { filtered.empty++; continue; }

    const phone = normalizePhone(getVal(row, 'GENTELE'));
    const website = normalizeWebsite(getVal(row, 'WEBADDR'));

    // Filter: require phone/website if requested
    if (WITH_PHONE && !phone) { filtered.noPhone++; continue; }
    if (WITH_WEBSITE && !website) { filtered.noWebsite++; continue; }

    // Split chief administrator name into first/last
    const chfnm = getVal(row, 'CHFNM');
    const { first, last } = splitName(chfnm);

    const city = getVal(row, 'CITY');
    const stabbr = getVal(row, 'STABBR');
    const zip = getVal(row, 'ZIP');

    leads.push({
      email: '',   // IPEDS does not include email addresses
      first_name: first,
      last_name: last,
      company: instnm,
      phone: phone,
      city: city,
      state: stabbr,
      country: 'United States',
      source: 'nces_ipeds',
      // Extra fields stored for enrichment reference (not in CSV output)
      _website: website,
      _adminurl: normalizeWebsite(getVal(row, 'ADMINURL')),
      _faidurl: normalizeWebsite(getVal(row, 'FAIDURL')),
      _chftitle: getVal(row, 'CHFTITLE'),
      _zip: zip,
      _addr: getVal(row, 'ADDR'),
      _sector: parseInt(getVal(row, 'SECTOR')) || 0,
      _control: parseInt(getVal(row, 'CONTROL')) || 0,
      _iclevel: parseInt(getVal(row, 'ICLEVEL')) || 0,
      _deggrant: getVal(row, 'DEGGRANT'),
      _unitid: getVal(row, 'UNITID'),
      _lat: getVal(row, 'LATITUDE'),
      _lon: getVal(row, 'LONGITUD'),
    });
  }

  console.log(`  Filtered out:`);
  if (filtered.closed) console.log(`    Closed:          ${filtered.closed}`);
  if (filtered.dead) console.log(`    Dead/deleted:     ${filtered.dead}`);
  if (filtered.inactive) console.log(`    Inactive:        ${filtered.inactive}`);
  if (filtered.nonPostsec) console.log(`    Non-postsecondary: ${filtered.nonPostsec}`);
  if (filtered.nondegree) console.log(`    Non-degree:      ${filtered.nondegree}`);
  if (filtered.levelFiltered) console.log(`    Level filtered:  ${filtered.levelFiltered}`);
  if (filtered.controlFiltered) console.log(`    Control filtered: ${filtered.controlFiltered}`);
  if (filtered.noPhone) console.log(`    No phone:        ${filtered.noPhone}`);
  if (filtered.noWebsite) console.log(`    No website:      ${filtered.noWebsite}`);
  if (filtered.empty) console.log(`    Empty name:      ${filtered.empty}`);
  console.log(`  Active institutions: ${leads.length}`);

  // Test mode: show preview and exit
  if (TEST_MODE) {
    console.log('\n--- TEST MODE: Preview (first 20 rows) ---');
    for (const l of leads.slice(0, 20)) {
      console.log(`  ${l.company} | ${l.city}, ${l.state} | ${l.phone || 'no phone'} | ${l._website || 'no website'} | ${l.first_name} ${l.last_name} (${l._chftitle || 'no title'})`);
    }
    console.log(`\n  Total active: ${leads.length} (showing 20 of ${leads.length})`);
    console.log('  Run without --test to generate full CSV.');
    return;
  }

  // Step 5: Write main CSV (standard output format)
  console.log('\n--- Step 5: Write CSV ---');
  writeCSV(OUTPUT_FILE, leads);
  console.log(`  Wrote ${leads.length} leads to ${OUTPUT_FILE}`);

  // Step 6: Write enriched CSV with extra fields (for reference/enrichment)
  const enrichedFile = path.join(OUTPUT_DIR, 'nces-us-colleges-enriched.csv');
  const enrichedHeader = 'email,first_name,last_name,company,phone,city,state,zip,country,source,website,admin_url,financial_aid_url,title,address,sector,control,level,degree_granting,unitid,latitude,longitude';
  const enrichedRows = leads.map(l =>
    [
      l.email, l.first_name, l.last_name, l.company, l.phone, l.city, l.state, l._zip,
      l.country, l.source, l._website, l._adminurl, l._faidurl, l._chftitle, l._addr,
      SECTOR_LABELS[l._sector] || l._sector, CONTROL_LABELS[l._control] || l._control,
      LEVEL_LABELS[l._iclevel] || l._iclevel, l._deggrant === '1' ? 'Yes' : 'No',
      l._unitid, l._lat, l._lon,
    ].map(escapeCSV).join(',')
  );
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.writeFileSync(enrichedFile, [enrichedHeader, ...enrichedRows].join('\n'));
  console.log(`  Wrote enriched file: ${enrichedFile}`);

  // Timestamped copy
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const tsFile = path.join(OUTPUT_DIR, `nces-us-colleges_${ts}.csv`);
  writeCSV(tsFile, leads);
  console.log(`  Timestamped: ${tsFile}`);

  // Step 7: Stats
  const withPhone = leads.filter(l => l.phone).length;
  const withWebsite = leads.filter(l => l._website).length;
  const withAdmin = leads.filter(l => l.first_name || l.last_name).length;
  const withAdminUrl = leads.filter(l => l._adminurl).length;

  // Sector breakdown
  const sectorCounts = {};
  for (const l of leads) {
    const label = SECTOR_LABELS[l._sector] || `Sector ${l._sector}`;
    sectorCounts[label] = (sectorCounts[label] || 0) + 1;
  }

  // Control breakdown
  const controlCounts = {};
  for (const l of leads) {
    const label = CONTROL_LABELS[l._control] || `Control ${l._control}`;
    controlCounts[label] = (controlCounts[label] || 0) + 1;
  }

  // State breakdown (top 15)
  const stateCounts = {};
  for (const l of leads) {
    if (l.state) stateCounts[l.state] = (stateCounts[l.state] || 0) + 1;
  }

  // Degree-granting stats
  const degreeGranting = leads.filter(l => l._deggrant === '1').length;

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('');
  console.log('=============================================================');
  console.log('  NCES IPEDS SCRAPE COMPLETE');
  console.log('=============================================================');
  console.log(`  Data source:        HD${YEAR} (${usedUrl})`);
  console.log(`  Total institutions: ${leads.length}`);
  console.log(`  Degree-granting:    ${degreeGranting}`);
  console.log(`  With phone:         ${withPhone} (${(withPhone / leads.length * 100).toFixed(1)}%)`);
  console.log(`  With website:       ${withWebsite} (${(withWebsite / leads.length * 100).toFixed(1)}%)`);
  console.log(`  With admin name:    ${withAdmin} (${(withAdmin / leads.length * 100).toFixed(1)}%)`);
  console.log(`  With admissions URL: ${withAdminUrl}`);
  console.log(`  Time:               ${elapsed}s`);
  console.log('');
  console.log('  Sector breakdown:');
  Object.entries(sectorCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([sector, cnt]) => console.log(`    ${sector}: ${cnt}`));
  console.log('');
  console.log('  Control breakdown:');
  Object.entries(controlCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([ctrl, cnt]) => console.log(`    ${ctrl}: ${cnt}`));
  console.log('');
  console.log('  Top 15 states:');
  Object.entries(stateCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .forEach(([st, cnt]) => console.log(`    ${st}: ${cnt}`));
  console.log('');
  console.log('  Output files:');
  console.log(`    Standard:  ${OUTPUT_FILE}`);
  console.log(`    Enriched:  ${enrichedFile}`);
  console.log(`    Stamped:   ${tsFile}`);
  console.log('');
  console.log('  Notes:');
  console.log('  - IPEDS HD file does NOT contain email addresses');
  console.log('  - Enrich emails by crawling WEBADDR/ADMINURL websites');
  console.log('  - CHFNM = chief administrator (president/chancellor/director)');
  console.log('  - GENTELE = general information phone number');
  console.log('  - Run with --degree-granting for accredited colleges only');
  console.log('  - Run with --min-level=1 for 4-year institutions only');
  console.log('  - Enriched CSV includes website, admin URL, sector, coords, etc.');
  console.log('  - UNITID can be used to join with other IPEDS surveys (finance, enrollment)');
  console.log('=============================================================');
  console.log('');
}

main().catch(e => {
  console.error('Fatal error:', e.message);
  console.error(e.stack);
  process.exit(1);
});
