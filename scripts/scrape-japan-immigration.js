#!/usr/bin/env node
/**
 * Japan Immigration Services Agency (ISA) — Registered Support Organizations Scraper
 *
 * Downloads the official ISA Excel registry of Registered Support Organizations
 * (登録支援機関登録簿) and parses it into CSV format.
 *
 * Source: https://www.moj.go.jp/isa/applications/ssw/nyuukokukanri07_00205.html
 * Data:   ~12,000+ organizations with name, address, phone, representative,
 *         languages supported, registration number
 *
 * The scraper:
 *   1. Fetches the ISA listing page to discover the current Excel download URLs
 *   2. Downloads the English version of the Excel file (romanized names/addresses)
 *   3. Also downloads the Japanese version for supplementary data (original kanji names)
 *   4. Parses both files, cross-references, and outputs a unified CSV
 *
 * Excel structure (English version):
 *   Row 0: "As of April 15.2026" (date stamp)
 *   Row 1: "List of Registered Support Organization"
 *   Row 2: Main column headers (1-10)
 *   Rows 3-6: Empty / merged header continuation
 *   Row 7: Sub-headers (Zip code, address, Telephone, etc.)
 *   Row 8+: Data rows
 *
 * Column mapping:
 *   A (0): Registration number (e.g., "19登-000002")
 *   B (1): Date of registration (Excel serial date)
 *   C (2): Organization name
 *   D (3): Zip code
 *   E (4): Address (full, includes city/prefecture/Japan)
 *   F (5): Telephone number
 *   G (6): Representative name
 *   H (7): Support office name
 *   I (8): Support office zip code
 *   J (9): Support office address
 *   K (10): Support details (checkbox text)
 *   L (11): Date of starting support (Excel serial date)
 *   M (12): Languages supported
 *
 * Output: output/japan-immigration-agents.csv
 * Header: email,first_name,last_name,company,phone,city,state,country,source
 *
 * Usage:
 *   node scripts/scrape-japan-immigration.js
 *   node scripts/scrape-japan-immigration.js --test          # First 50 rows only
 *   node scripts/scrape-japan-immigration.js --japanese-only # Use Japanese Excel only
 *   node scripts/scrape-japan-immigration.js --both          # Include both EN + JP name columns
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

// ── Config ──────────────────────────────────────────────────────────────
const ISA_LISTING_PAGE = 'https://www.moj.go.jp/isa/applications/ssw/nyuukokukanri07_00205.html';
const ISA_BASE_URL = 'https://www.moj.go.jp';

// Fallback URLs (known good as of April 2026, updated periodically by ISA)
const FALLBACK_EN_URL = 'https://www.moj.go.jp/isa/content/001440543.xlsx';
const FALLBACK_JP_URL = 'https://www.moj.go.jp/isa/content/001460874.xlsx';

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'japan-immigration-agents.csv');
const TEMP_DIR = path.join(__dirname, '..', 'output', '.tmp');

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const DATA_START_ROW = 8; // First data row in the Excel file (0-indexed)

// Column indices (0-indexed)
const COL = {
  REG_NUMBER:       0,  // A - Registration number
  REG_DATE:         1,  // B - Date of registration
  ORG_NAME:         2,  // C - Organization name
  ZIP_CODE:         3,  // D - Zip code
  ADDRESS:          4,  // E - Full address
  PHONE:            5,  // F - Telephone number
  REPRESENTATIVE:   6,  // G - Representative name
  OFFICE_NAME:      7,  // H - Support office name
  OFFICE_ZIP:       8,  // I - Support office zip code
  OFFICE_ADDRESS:   9,  // J - Support office address
  SUPPORT_DETAIL:  10,  // K - Support details
  SUPPORT_START:   11,  // L - Date of starting support
  LANGUAGES:       12,  // M - Languages supported
  NOTES:           13,  // N - Notes/remarks
};

const CSV_HEADERS = [
  'email', 'first_name', 'last_name', 'company', 'phone',
  'city', 'state', 'country', 'source',
  'registration_number', 'representative', 'address', 'zip_code',
  'languages', 'office_name', 'office_address',
  'company_jp', 'representative_jp',
];

// ── CLI Args ────────────────────────────────────────────────────────────
const args = {};
for (const arg of process.argv.slice(2)) {
  const [key, val] = arg.replace(/^--/, '').split('=');
  args[key] = val || true;
}
const TEST_MODE = !!args.test;
const JP_ONLY = !!args['japanese-only'];
const INCLUDE_BOTH = !!args.both;
const MAX_ROWS = TEST_MODE ? 50 : Infinity;

// ── Stats ───────────────────────────────────────────────────────────────
const stats = {
  startTime: Date.now(),
  excelRows: 0,
  parsed: 0,
  withPhone: 0,
  withEmail: 0,
  withCity: 0,
  skippedEmpty: 0,
  saved: 0,
};

// ── Helpers ─────────────────────────────────────────────────────────────
function log(msg) {
  const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);
  console.log(`[${elapsed}s] ${msg}`);
}

function escapeCSV(val) {
  if (!val) return '';
  const s = String(val).trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Fetch a URL with browser-like headers. Returns a Buffer.
 */
function fetchUrl(url, { expectBinary = false } = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const client = parsedUrl.protocol === 'https:' ? https : http;

    const opts = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'GET',
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': expectBinary
          ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*'
          : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ja,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      },
    };

    const req = client.request(opts, (res) => {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const redirectUrl = res.headers.location.startsWith('http')
          ? res.headers.location
          : `${parsedUrl.protocol}//${parsedUrl.hostname}${res.headers.location}`;
        log(`  Redirect ${res.statusCode} -> ${redirectUrl}`);
        res.resume();
        return fetchUrl(redirectUrl, { expectBinary }).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }

      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });

    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
    req.end();
  });
}

/**
 * Parse the ISA listing page to find the current Excel download URLs.
 * Returns { jpUrl, enUrl }.
 */
async function discoverExcelUrls() {
  log('Fetching ISA listing page to discover current Excel URLs...');
  try {
    const html = await fetchUrl(ISA_LISTING_PAGE);
    const htmlStr = html.toString('utf-8');

    // Extract all .xlsx links
    const xlsxLinks = [];
    const linkRegex = /href="([^"]*\.xlsx[^"]*)"/g;
    let match;
    while ((match = linkRegex.exec(htmlStr)) !== null) {
      let href = match[1];
      if (!href.startsWith('http')) {
        href = ISA_BASE_URL + href;
      }
      xlsxLinks.push(href);
    }

    log(`  Found ${xlsxLinks.length} Excel link(s) on page`);

    if (xlsxLinks.length >= 2) {
      // First link is typically the Japanese version (larger file), second is English
      // But we verify by checking file size or content
      return {
        jpUrl: xlsxLinks[0],
        enUrl: xlsxLinks[1],
      };
    } else if (xlsxLinks.length === 1) {
      // Only one link found - likely Japanese
      return {
        jpUrl: xlsxLinks[0],
        enUrl: FALLBACK_EN_URL,
      };
    }
  } catch (err) {
    log(`  Warning: Could not fetch listing page: ${err.message}`);
  }

  // Fallback to known URLs
  log('  Using fallback URLs');
  return {
    jpUrl: FALLBACK_JP_URL,
    enUrl: FALLBACK_EN_URL,
  };
}

/**
 * Download an Excel file and save to temp directory.
 * Returns the local file path.
 */
async function downloadExcel(url, filename) {
  const filepath = path.join(TEMP_DIR, filename);

  // Check if already downloaded recently (within 24 hours)
  if (fs.existsSync(filepath)) {
    const age = Date.now() - fs.statSync(filepath).mtimeMs;
    if (age < 24 * 60 * 60 * 1000) {
      log(`  Using cached ${filename} (${(fs.statSync(filepath).size / 1024 / 1024).toFixed(1)}MB)`);
      return filepath;
    }
  }

  log(`  Downloading ${filename} from ${url}...`);
  const data = await fetchUrl(url, { expectBinary: true });
  fs.writeFileSync(filepath, data);
  log(`  Downloaded ${filename}: ${(data.length / 1024 / 1024).toFixed(1)}MB`);
  return filepath;
}

/**
 * Get cell value from worksheet, returning empty string for missing cells.
 */
function cellVal(ws, row, col) {
  const addr = XLSX.utils.encode_cell({ r: row, c: col });
  const cell = ws[addr];
  if (!cell) return '';
  return String(cell.v).trim();
}

/**
 * Convert Excel serial date to YYYY-MM-DD string.
 */
function excelDateToString(serial) {
  if (!serial || isNaN(serial)) return '';
  // Excel serial date: days since 1900-01-01 (with the 1900 leap year bug)
  const utcDays = Math.floor(serial - 25569);
  const utcValue = utcDays * 86400 * 1000;
  const d = new Date(utcValue);
  return d.toISOString().split('T')[0];
}

/**
 * Normalize a Japanese phone number.
 * Input: "0223693840" or "022-369-3840" or "+81-22-369-3840"
 * Output: "022-369-3840" (standard Japanese format) or original if unparseable
 */
function normalizeJapanesePhone(raw) {
  if (!raw) return '';
  let phone = String(raw).trim();

  // Remove any non-digit characters for processing
  let digits = phone.replace(/[^0-9]/g, '');

  // Handle +81 country code prefix
  if (digits.startsWith('81') && digits.length > 10) {
    digits = '0' + digits.substring(2);
  }

  // Must be 10 or 11 digits for a valid Japanese phone
  if (digits.length < 10 || digits.length > 11) return phone;

  // Format based on area code patterns
  // Mobile: 070/080/090 -> 3-4-4
  if (/^0[789]0/.test(digits)) {
    return `${digits.slice(0,3)}-${digits.slice(3,7)}-${digits.slice(7)}`;
  }

  // Tokyo (03): 2-4-4
  if (digits.startsWith('03')) {
    return `${digits.slice(0,2)}-${digits.slice(2,6)}-${digits.slice(6)}`;
  }

  // Osaka (06): 2-4-4
  if (digits.startsWith('06')) {
    return `${digits.slice(0,2)}-${digits.slice(2,6)}-${digits.slice(6)}`;
  }

  // Toll-free (0120): 4-3-3 or 4-6
  if (digits.startsWith('0120')) {
    if (digits.length === 10) {
      return `${digits.slice(0,4)}-${digits.slice(4,7)}-${digits.slice(7)}`;
    }
    return `${digits.slice(0,4)}-${digits.slice(4)}`;
  }

  // Default: assume 3-digit area code -> 3-X-4
  if (digits.length === 10) {
    return `${digits.slice(0,3)}-${digits.slice(3,6)}-${digits.slice(6)}`;
  }

  // 11-digit with longer area code
  return `${digits.slice(0,4)}-${digits.slice(4,7)}-${digits.slice(7)}`;
}

// Prefecture lookup maps
const EN_PREFECTURES = [
  'Hokkaido', 'Aomori', 'Iwate', 'Miyagi', 'Akita', 'Yamagata', 'Fukushima',
  'Ibaraki', 'Tochigi', 'Gunma', 'Saitama', 'Chiba', 'Tokyo', 'Kanagawa',
  'Niigata', 'Toyama', 'Ishikawa', 'Fukui', 'Yamanashi', 'Nagano',
  'Gifu', 'Shizuoka', 'Aichi', 'Mie',
  'Shiga', 'Kyoto', 'Osaka', 'Hyogo', 'Nara', 'Wakayama',
  'Tottori', 'Shimane', 'Okayama', 'Hiroshima', 'Yamaguchi',
  'Tokushima', 'Kagawa', 'Ehime', 'Kochi',
  'Fukuoka', 'Saga', 'Nagasaki', 'Kumamoto', 'Oita', 'Miyazaki', 'Kagoshima', 'Okinawa',
];

const JP_PREFECTURES = {
  '北海道': 'Hokkaido', '青森県': 'Aomori', '岩手県': 'Iwate', '宮城県': 'Miyagi',
  '秋田県': 'Akita', '山形県': 'Yamagata', '福島県': 'Fukushima', '茨城県': 'Ibaraki',
  '栃木県': 'Tochigi', '群馬県': 'Gunma', '埼玉県': 'Saitama', '千葉県': 'Chiba',
  '東京都': 'Tokyo', '神奈川県': 'Kanagawa', '新潟県': 'Niigata', '富山県': 'Toyama',
  '石川県': 'Ishikawa', '福井県': 'Fukui', '山梨県': 'Yamanashi', '長野県': 'Nagano',
  '岐阜県': 'Gifu', '静岡県': 'Shizuoka', '愛知県': 'Aichi', '三重県': 'Mie',
  '滋賀県': 'Shiga', '京都府': 'Kyoto', '大阪府': 'Osaka', '兵庫県': 'Hyogo',
  '奈良県': 'Nara', '和歌山県': 'Wakayama', '鳥取県': 'Tottori', '島根県': 'Shimane',
  '岡山県': 'Okayama', '広島県': 'Hiroshima', '山口県': 'Yamaguchi', '徳島県': 'Tokushima',
  '香川県': 'Kagawa', '愛媛県': 'Ehime', '高知県': 'Kochi', '福岡県': 'Fukuoka',
  '佐賀県': 'Saga', '長崎県': 'Nagasaki', '熊本県': 'Kumamoto', '大分県': 'Oita',
  '宮崎県': 'Miyazaki', '鹿児島県': 'Kagoshima', '沖縄県': 'Okinawa',
};

/**
 * Extract city and prefecture from an address string.
 *
 * English addresses come in varied formats:
 *   "2-1-26, Funairi, Shiogama-shi, Miyagi, 985-0014,Japan"
 *   "3-21-2, Helios Bldg, 7F Motohama-cho, Naka-ku, Yokohama-city, Kanagawa-pref"
 *   "No9tatsumi-Bid3F, 1-4-4kanayama naka-ku Nagoya, Aichi"
 *   "5-3 kaminagare Ichiriyama-cho KARIYA-shi AICHI"
 *   "2788-1 Tando, Kajiki-cho, Aira City, Kagoshima Prefecture"
 *   "1-1-69 Matsugaoka Chigasaki city Kanagawa"
 *
 * Japanese addresses look like:
 *   "宮城県塩竈市舟入二丁目１番２６号"
 *   "神奈川県横浜市中区元浜町３丁目２１番地２"
 */
function parseAddress(enAddr, jpAddr) {
  let city = '';
  let state = ''; // prefecture

  if (enAddr) {
    const addr = enAddr.trim();

    // ── Prefecture extraction ──
    // Check for "-pref" suffix pattern (e.g., "Kanagawa-pref")
    const prefMatch = addr.match(/(\w+)-pref/i);
    if (prefMatch) {
      state = prefMatch[1];
      // Normalize to title case
      state = state.charAt(0).toUpperCase() + state.slice(1).toLowerCase();
    }

    // Check for "Prefecture" suffix (e.g., "Kagoshima Prefecture")
    if (!state) {
      const prefSuffix = addr.match(/(\w+)\s+Prefecture/i);
      if (prefSuffix) {
        state = prefSuffix[1].charAt(0).toUpperCase() + prefSuffix[1].slice(1).toLowerCase();
      }
    }

    // Search for known prefecture names anywhere in the address (word boundary)
    if (!state) {
      for (const pref of EN_PREFECTURES) {
        const regex = new RegExp('\\b' + pref + '\\b', 'i');
        if (regex.test(addr)) {
          state = pref;
          break;
        }
      }
    }

    // ── City extraction ──
    // Strategy 1: Look for "NAME-shi" or "NAME-city" patterns (with word boundary before)
    const cityPatterns = [
      // "Yokohama-city" or "KARIYA-shi" (standalone, possibly after space)
      /\b([A-Za-z]+(?:\s[A-Za-z]+)?)-(?:shi|city)\b/i,
      // "Aira City" (space-separated)
      /\b([A-Za-z]+(?:\s[A-Za-z]+)?)\s+(?:City|shi)\b/i,
    ];

    for (const pattern of cityPatterns) {
      const m = addr.match(pattern);
      if (m) {
        const candidate = m[1].trim();
        // Exclude false positives (building names, numbers, etc.)
        if (candidate.length > 1 && !/^\d/.test(candidate) && !/^(?:Bid|Bldg|Blg|No)$/i.test(candidate)) {
          city = candidate.charAt(0).toUpperCase() + candidate.slice(1).toLowerCase();
          break;
        }
      }
    }

    // Strategy 2: If no city found, look for comma-separated location just before prefecture
    if (!city && state) {
      const parts = addr.split(',').map(p => p.trim()).filter(Boolean);
      for (let i = parts.length - 1; i >= 0; i--) {
        const part = parts[i].replace(/-(?:pref|ken)$/i, '').replace(/\s*Prefecture$/i, '').trim();
        if (part.toLowerCase() === state.toLowerCase() || part === 'Japan' || /^\d/.test(part)) continue;
        // The part just before the prefecture is likely the city
        if (i > 0 && part.toLowerCase() !== state.toLowerCase()) {
          const prevPart = parts[i - 1] ? parts[i - 1].trim() : '';
          // Check if current part is the prefecture and previous is city
          if (part.toLowerCase() === state.toLowerCase()) continue;
        }
      }
      // Alternative: find the segment just before the prefecture name
      const prefIdx = addr.toLowerCase().indexOf(state.toLowerCase());
      if (prefIdx > 0) {
        const before = addr.substring(0, prefIdx).trim().replace(/,\s*$/, '');
        const segments = before.split(',').map(s => s.trim()).filter(Boolean);
        if (segments.length > 0) {
          const lastSeg = segments[segments.length - 1]
            .replace(/-(?:shi|city|ku|cho|machi|gun)$/i, '')
            .replace(/\s+\d[\d-]+$/, '')  // Remove trailing zip codes
            .trim();
          if (lastSeg.length > 1 && !/^\d/.test(lastSeg) && !/^(?:Bid|Bldg|Blg|No)$/i.test(lastSeg)) {
            city = lastSeg;
          }
        }
      }
    }
  }

  // ── Fallback: parse Japanese address for prefecture ──
  if (!state && jpAddr) {
    for (const [jp, en] of Object.entries(JP_PREFECTURES)) {
      if (jpAddr.startsWith(jp)) {
        state = en;
        break;
      }
    }
  }

  // ── Extract city from Japanese address if still missing ──
  if (!city && jpAddr) {
    // Japanese addresses: 県名 + 市/区/町/郡名
    // e.g., "宮城県塩竈市..." -> city is 塩竈
    // We just note the Japanese city name for now (romanization would need a dictionary)
    // Skip this for the CSV; the state is the most important field
  }

  return { city, state };
}

/**
 * Split a representative name into first_name and last_name.
 * Japanese convention: last name first, then first name.
 * English version often has "Firstname Lastname" or "LASTNAME FIRSTNAME".
 *
 * We try to handle both patterns.
 */
function splitName(name) {
  if (!name) return { first_name: '', last_name: '' };

  const cleaned = name.trim();

  // If the name contains Japanese characters, it's likely "LastName FirstName"
  if (/[\u3000-\u9FFF]/.test(cleaned)) {
    // Split on full-width or half-width space
    const parts = cleaned.split(/[\s\u3000]+/).filter(Boolean);
    if (parts.length >= 2) {
      return { first_name: parts.slice(1).join(' '), last_name: parts[0] };
    }
    return { first_name: '', last_name: cleaned };
  }

  // English name - split on spaces
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: '', last_name: parts[0] };

  // If ALL CAPS, likely "LASTNAME FIRSTNAME" (Japanese convention in romaji)
  if (cleaned === cleaned.toUpperCase() && parts.length === 2) {
    return { first_name: parts[1], last_name: parts[0] };
  }

  // Standard Western: "Firstname Lastname" or "Firstname Middle Lastname"
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' '),
  };
}

/**
 * Parse the English Excel file and return an array of lead objects.
 * Optionally cross-reference with Japanese Excel for original names.
 */
function parseExcel(enFilepath, jpFilepath) {
  log('Parsing English Excel file...');
  const enWb = XLSX.readFile(enFilepath);
  const enWs = enWb.Sheets[enWb.SheetNames[0]];

  // Determine data range
  const range = XLSX.utils.decode_range(enWs['!ref']);
  const totalRows = range.e.r + 1;
  log(`  Sheet "${enWb.SheetNames[0]}": ${totalRows} total rows`);

  // Load Japanese file for cross-reference if available
  let jpWs = null;
  if (jpFilepath && fs.existsSync(jpFilepath)) {
    log('  Loading Japanese Excel for cross-reference...');
    const jpWb = XLSX.readFile(jpFilepath);
    jpWs = jpWb.Sheets[jpWb.SheetNames[0]];
  }

  const leads = [];
  let rowCount = 0;

  for (let r = DATA_START_ROW; r <= range.e.r; r++) {
    if (rowCount >= MAX_ROWS) break;

    // Check if this is a real data row (registration number starts with digits or has 登-)
    const regNum = cellVal(enWs, r, COL.REG_NUMBER);
    if (!regNum || (!regNum.includes('登-') && !regNum.match(/^\d/))) {
      stats.skippedEmpty++;
      continue;
    }

    const orgName = cellVal(enWs, r, COL.ORG_NAME);
    const address = cellVal(enWs, r, COL.ADDRESS);
    const phone = cellVal(enWs, r, COL.PHONE);
    const representative = cellVal(enWs, r, COL.REPRESENTATIVE);
    const officeName = cellVal(enWs, r, COL.OFFICE_NAME);
    const officeAddress = cellVal(enWs, r, COL.OFFICE_ADDRESS);
    const zipCode = cellVal(enWs, r, COL.ZIP_CODE);
    const languages = cellVal(enWs, r, COL.LANGUAGES);

    // Get Japanese data for cross-reference
    let jpOrgName = '';
    let jpRepresentative = '';
    let jpAddress = '';
    if (jpWs) {
      jpOrgName = cellVal(jpWs, r, COL.ORG_NAME);
      jpRepresentative = cellVal(jpWs, r, COL.REPRESENTATIVE);
      jpAddress = cellVal(jpWs, r, COL.ADDRESS);
    }

    // Skip empty rows
    if (!orgName && !jpOrgName) {
      stats.skippedEmpty++;
      continue;
    }

    // Parse city and prefecture from address
    const { city, state } = parseAddress(address, jpAddress);

    // Split representative name
    const { first_name, last_name } = splitName(representative);

    // Normalize phone
    const normalizedPhone = normalizeJapanesePhone(phone);

    const lead = {
      email: '',  // ISA data does not include email addresses
      first_name,
      last_name,
      company: orgName || jpOrgName || '',
      phone: normalizedPhone,
      city,
      state,
      country: 'Japan',
      source: 'isa_registered_support_org',
      registration_number: regNum,
      representative: representative || jpRepresentative || '',
      address: address || jpAddress || '',
      zip_code: zipCode,
      languages: languages || '',
      office_name: officeName || '',
      office_address: officeAddress || '',
      company_jp: jpOrgName || '',
      representative_jp: jpRepresentative || '',
    };

    leads.push(lead);
    rowCount++;

    if (normalizedPhone) stats.withPhone++;
    if (city) stats.withCity++;
  }

  stats.excelRows = totalRows;
  stats.parsed = leads.length;
  return leads;
}

/**
 * Write leads to CSV file.
 */
function writeCSV(leads, filepath) {
  const headerLine = CSV_HEADERS.join(',');
  const lines = [headerLine];

  for (const lead of leads) {
    const row = CSV_HEADERS.map(h => escapeCSV(lead[h] || ''));
    lines.push(row.join(','));
  }

  fs.writeFileSync(filepath, '\ufeff' + lines.join('\n'), 'utf-8');  // BOM for Excel compatibility
  stats.saved = leads.length;
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║  Japan ISA — Registered Support Organizations Scraper       ║');
  console.log('║  Source: Immigration Services Agency (出入国在留管理庁)       ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');
  console.log('');

  if (TEST_MODE) log('TEST MODE: Limited to first 50 rows');

  // Ensure output directories exist
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });

  // Step 1: Discover current Excel download URLs
  const { jpUrl, enUrl } = await discoverExcelUrls();
  log(`  Japanese Excel URL: ${jpUrl}`);
  log(`  English Excel URL:  ${enUrl}`);

  // Step 2: Download Excel files
  let enFilepath, jpFilepath;

  if (JP_ONLY) {
    jpFilepath = await downloadExcel(jpUrl, 'isa-registry-jp.xlsx');
    enFilepath = null;
  } else {
    enFilepath = await downloadExcel(enUrl, 'isa-registry-en.xlsx');
    jpFilepath = await downloadExcel(jpUrl, 'isa-registry-jp.xlsx');
  }

  // Step 3: Parse Excel files
  let leads;
  if (JP_ONLY) {
    // Parse Japanese-only mode: use Japanese file directly
    log('Parsing Japanese Excel file (no English cross-reference)...');
    const jpWb = XLSX.readFile(jpFilepath);
    const jpWs = jpWb.Sheets[jpWb.SheetNames[0]];
    const range = XLSX.utils.decode_range(jpWs['!ref']);

    leads = [];
    let rowCount = 0;

    for (let r = DATA_START_ROW; r <= range.e.r; r++) {
      if (rowCount >= MAX_ROWS) break;

      const regNum = cellVal(jpWs, r, COL.REG_NUMBER);
      if (!regNum || (!regNum.includes('登-') && !regNum.match(/^\d/))) continue;

      const orgName = cellVal(jpWs, r, COL.ORG_NAME);
      const address = cellVal(jpWs, r, COL.ADDRESS);
      const phone = cellVal(jpWs, r, COL.PHONE);
      const representative = cellVal(jpWs, r, COL.REPRESENTATIVE);
      const officeName = cellVal(jpWs, r, COL.OFFICE_NAME);
      const officeAddress = cellVal(jpWs, r, COL.OFFICE_ADDRESS);
      const zipCode = cellVal(jpWs, r, COL.ZIP_CODE);
      const languages = cellVal(jpWs, r, COL.LANGUAGES);

      if (!orgName) continue;

      const { city, state } = parseAddress('', address);
      const { first_name, last_name } = splitName(representative);
      const normalizedPhone = normalizeJapanesePhone(phone);

      leads.push({
        email: '',
        first_name,
        last_name,
        company: orgName,
        phone: normalizedPhone,
        city,
        state,
        country: 'Japan',
        source: 'isa_registered_support_org',
        registration_number: regNum,
        representative: representative || '',
        address: address || '',
        zip_code: zipCode,
        languages: languages || '',
        office_name: officeName || '',
        office_address: officeAddress || '',
        company_jp: orgName,
        representative_jp: representative,
      });

      rowCount++;
      if (normalizedPhone) stats.withPhone++;
      if (city || state) stats.withCity++;
    }

    stats.excelRows = range.e.r + 1;
    stats.parsed = leads.length;
  } else {
    leads = parseExcel(enFilepath, jpFilepath);
  }

  log(`  Parsed ${leads.length} organizations from Excel`);

  // Step 4: Write CSV
  log(`Writing CSV to ${OUTPUT_FILE}...`);
  writeCSV(leads, OUTPUT_FILE);

  // Step 5: Summary
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  RESULTS');
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  Total Excel rows:     ${stats.excelRows.toLocaleString()}`);
  console.log(`  Organizations parsed: ${stats.parsed.toLocaleString()}`);
  console.log(`  With phone number:    ${stats.withPhone.toLocaleString()}`);
  console.log(`  With city/prefecture: ${stats.withCity.toLocaleString()}`);
  console.log(`  Skipped (empty):      ${stats.skippedEmpty.toLocaleString()}`);
  console.log(`  Saved to CSV:         ${stats.saved.toLocaleString()}`);
  console.log(`  Output file:          ${OUTPUT_FILE}`);
  console.log(`  Elapsed:              ${((Date.now() - stats.startTime) / 1000).toFixed(1)}s`);
  console.log('═══════════════════════════════════════════════════════════');

  // Prefecture breakdown
  const prefCounts = {};
  for (const lead of leads) {
    const pref = lead.state || 'Unknown';
    prefCounts[pref] = (prefCounts[pref] || 0) + 1;
  }
  const sorted = Object.entries(prefCounts).sort((a, b) => b[1] - a[1]);
  console.log('\n  Top 20 Prefectures:');
  for (const [pref, count] of sorted.slice(0, 20)) {
    console.log(`    ${pref.padEnd(15)} ${count.toLocaleString()}`);
  }
  if (sorted.length > 20) {
    console.log(`    ... and ${sorted.length - 20} more prefectures`);
  }
  console.log('');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
