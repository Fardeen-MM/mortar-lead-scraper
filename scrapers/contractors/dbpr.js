/**
 * Florida DBPR (Department of Business and Professional Regulation) — Contractor Scraper
 *
 * Source: https://www2.myfloridalicense.com/sto/file_download/extracts/constr_app.csv (construction)
 *         https://www2.myfloridalicense.com/sto/file_download/extracts/elec_app.csv  (electrical)
 * Records: ~101K construction + ~10K electrical = ~112K total
 * Method: Weekly CSV bulk downloads (no auth required)
 *
 * CSV format (15 fields, no header row, quote/comma delimited, CRLF line endings):
 *   0: License type code (e.g., "0605")
 *   1: License type name (e.g., "Certified General Contractor")
 *   2: First name
 *   3: Middle name
 *   4: Last name
 *   5: Suffix (Jr, II, etc.)
 *   6: Address line 1
 *   7: Address line 2
 *   8: Address line 3
 *   9: City
 *  10: State
 *  11: Zip code
 *  12: County code (FIPS)
 *  13: Phone number
 *  14: Additional field (sometimes suite/unit number, mostly empty)
 *
 * DBPR type code mapping:
 *   Construction (constr_app.csv):
 *     0601: Certified AC Contractor (HVAC)       — 8,255 records
 *     0602: Certified Building Contractor (CBC)   — 14,649 records
 *     0603: Certified Roofing Contractor (CRC)    — 9,499 records
 *     0604: Certified Plumbing Contractor (CPC)   — 5,713 records
 *     0605: Certified General Contractor (CGC)    — 40,178 records
 *     0606: Certified Mechanical Contractor (CMC) — 2,227 records
 *     0607: Certified Pool/Spa Contractor (CCC)   — 3,666 records
 *     0608: Certified Residential Contractor      — 8,711 records
 *     0609: Certified Sheet Metal Contractor      — 164 records
 *     0610: Certified Utility and Excavation      — 2,570 records
 *     0611: Certified Solar Contractor            — 1,106 records
 *     0612: Certified Specialty Contractor        — 4,670 records
 *     0613: Certified Pollutant Storage           — 228 records
 *   Electrical (elec_app.csv):
 *     0801: Cert. Electrical Contractors (EC)     — 6,753 records
 *     0804: Cert. Specialty Contractors (ES)      — 2,517 records
 *     0802: Cert. Alarm System I (EF)             — 946 records
 *     0803: Cert. Alarm System II (EG)            — 613 records
 *
 * EMAIL NOTE: The bulk CSV files do NOT include email addresses. Per Florida
 * Statute Section 455.275(1), licensees must provide DBPR with an email address,
 * and those emails are public records. However, emails are only visible on the
 * individual license detail pages at myfloridalicense.com (session-based ASP.NET
 * interface). The scraper uses Strategy A (bulk CSV) for the initial dataset and
 * Strategy B (individual license lookups via the search interface) to attempt
 * email enrichment for yielded leads.
 *
 * The CSV data is still extremely valuable: 112K+ contractor records with names,
 * addresses, phone numbers, and license types — all public record.
 */

const https = require('https');
const http = require('http');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// CSV download URLs
const CONSTR_CSV_URL = 'https://www2.myfloridalicense.com/sto/file_download/extracts/constr_app.csv';
const ELEC_CSV_URL = 'https://www2.myfloridalicense.com/sto/file_download/extracts/elec_app.csv';

// Search interface URL (for email enrichment via individual lookups)
const SEARCH_URL = 'https://www.myfloridalicense.com/wl11.asp';

// Cache settings
const CACHE_DIR = path.join(__dirname, '..', '..', 'data', 'contractor-cache');
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days (CSV updated weekly)

// DBPR type code -> friendly license abbreviation
// NOTE: Florida uses these official abbreviations:
//   CGC=General, CBC=Building, CRC=Roofing, CPC=Plumbing, CMC=Mechanical,
//   CCC=Pool/Spa, CAC=AC. Residential (0608) uses its own prefix CRC-R here
//   to disambiguate from Roofing (0603) which is also CRC.
const TYPE_CODE_MAP = {
  '0601': { abbrev: 'CAC', name: 'Certified AC Contractor', area: 'hvac' },
  '0602': { abbrev: 'CBC', name: 'Certified Building Contractor', area: 'building' },
  '0603': { abbrev: 'CRC', name: 'Certified Roofing Contractor', area: 'roofing' },
  '0604': { abbrev: 'CPC', name: 'Certified Plumbing Contractor', area: 'plumbing' },
  '0605': { abbrev: 'CGC', name: 'Certified General Contractor', area: 'general contractor' },
  '0606': { abbrev: 'CMC', name: 'Certified Mechanical Contractor', area: 'hvac' },
  '0607': { abbrev: 'CCC', name: 'Certified Pool/Spa Contractor', area: 'pool' },
  '0608': { abbrev: 'CRC-R', name: 'Certified Residential Contractor', area: 'residential' },
  '0609': { abbrev: 'CSM', name: 'Certified Sheet Metal Contractor', area: 'sheet metal' },
  '0610': { abbrev: 'CUC', name: 'Certified Utility/Excavation', area: 'general contractor' },
  '0611': { abbrev: 'CSC', name: 'Certified Solar Contractor', area: 'solar' },
  '0612': { abbrev: 'CSP', name: 'Certified Specialty Contractor', area: 'general contractor' },
  '0613': { abbrev: 'CPS', name: 'Certified Pollutant Storage', area: 'general contractor' },
  // Electrical (from elec_app.csv)
  '0801': { abbrev: 'EC', name: 'Cert. Electrical Contractor', area: 'electrical' },
  '0802': { abbrev: 'EF', name: 'Cert. Alarm System I', area: 'electrical' },
  '0803': { abbrev: 'EG', name: 'Cert. Alarm System II', area: 'electrical' },
  '0804': { abbrev: 'ES', name: 'Cert. Electrical Specialty', area: 'electrical' },
};

// Practice area -> license abbreviation filter
// When user selects a practice area, we filter to matching type codes via ABBREV_TO_CODES.
// Special handling: 'hvac' maps to both CAC (AC) and CMC (Mechanical).
const PRACTICE_AREA_CODES = {
  'general contractor': 'CGC',
  'building': 'CBC',
  'roofing': 'CRC',
  'plumbing': 'CPC',
  'hvac': 'HVAC',         // Special: matches CAC + CMC + CSM
  'ac': 'CAC',
  'air conditioning': 'CAC',
  'electrical': 'EC',
  'pool': 'CCC',
  'solar': 'CSC',
  'residential': 'CRC-R',
  'mechanical': 'CMC',
  'sheet metal': 'CSM',
};

// Reverse map: abbreviation -> list of type codes
const ABBREV_TO_CODES = {};
for (const [code, info] of Object.entries(TYPE_CODE_MAP)) {
  const abbrev = info.abbrev;
  if (!ABBREV_TO_CODES[abbrev]) ABBREV_TO_CODES[abbrev] = [];
  ABBREV_TO_CODES[abbrev].push(code);
}

// Florida FIPS county codes -> county names (for reference / filtering)
const COUNTY_CODES = {
  '01': 'Alachua', '02': 'Baker', '03': 'Bay', '04': 'Bradford', '05': 'Brevard',
  '06': 'Broward', '07': 'Calhoun', '08': 'Charlotte', '09': 'Citrus', '10': 'Clay',
  '11': 'Collier', '12': 'Columbia', '13': 'DeSoto', '14': 'Dixie', '15': 'Duval',
  '16': 'Broward', '17': 'Escambia', '18': 'Flagler', '19': 'Franklin', '20': 'Gadsden',
  '21': 'Gilchrist', '22': 'Glades', '23': 'Gulf', '24': 'Hamilton', '25': 'Hardee',
  '26': 'Hendry', '27': 'Hernando', '28': 'Highlands', '29': 'Hillsborough',
  '30': 'Holmes', '31': 'Indian River', '32': 'Jackson', '33': 'Jefferson',
  '34': 'Lafayette', '35': 'Lake', '36': 'Lee', '37': 'Leon', '38': 'Levy',
  '39': 'Liberty', '40': 'Madison', '41': 'Manatee', '42': 'Marion', '43': 'Martin',
  '44': 'Miami-Dade', '45': 'Monroe', '46': 'Nassau', '47': 'Okaloosa', '48': 'Okeechobee',
  '49': 'Orange', '50': 'Osceola', '51': 'Palm Beach', '52': 'Pasco', '53': 'Pinellas',
  '54': 'Polk', '55': 'Putnam', '56': 'Santa Rosa', '57': 'Sarasota', '58': 'Seminole',
  '59': 'St. Johns', '60': 'St. Lucie', '61': 'Sumter', '62': 'Suwannee',
  '63': 'Taylor', '64': 'Union', '65': 'Volusia', '66': 'Wakulla', '67': 'Walton',
  '68': 'Washington',
};

// Default cities to search (used for city filtering when option provided)
const DEFAULT_CITIES = [
  'Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Fort Lauderdale',
  'St. Petersburg', 'Sarasota', 'Naples', 'West Palm Beach', 'Tallahassee',
];

/**
 * Parse a single line of quote-comma-delimited CSV into an array of fields.
 * Handles quoted fields with embedded commas. No header row in DBPR files.
 */
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        // Escaped quote ""
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else if (ch === '\r' || ch === '\n') {
      // Skip line endings
      continue;
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

/**
 * Normalize a phone number string from the CSV.
 * DBPR phones come in various formats: (xxx)xxx-xxxx, xxx.xxx.xxxx, xxx-xxx-xxxx, xxxx/xxxx, etc.
 */
function normalizePhone(raw) {
  if (!raw) return '';
  // Strip everything except digits
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  // Return cleaned up version if not standard 10/11 digits
  return digits.length >= 7 ? raw.trim() : '';
}

/**
 * Title-case a name string (UPPERCASE -> Title Case).
 * Handles hyphens, apostrophes, and Mac/Mc prefixes.
 */
function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|[\s\-'])(\w)/g, (match) => match.toUpperCase());
}


class DbprScraper extends BaseScraper {
  constructor() {
    super({
      name: 'dbpr_contractors',
      stateCode: 'DBPR',
      baseUrl: 'https://www2.myfloridalicense.com',
      pageSize: 1000,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() not used — search() is overridden`);
  }

  /**
   * Override transformResult to use 'dbpr_contractors' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'dbpr_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Download a CSV file.
   *
   * Primary: Uses system curl (HTTP/2 passes Cloudflare without challenge).
   * Fallback: Node.js https.get (may get Cloudflare 403 on www2 subdomain).
   *
   * Returns the full response body as a string.
   */
  _downloadCSV(url, rateLimiter) {
    // Strategy 1: Use curl (passes Cloudflare via HTTP/2)
    try {
      log.info(`Downloading via curl: ${url}`);
      const result = execSync(
        `curl -sL --max-time 120 "${url}"`,
        { maxBuffer: 50 * 1024 * 1024, timeout: 130000 } // 50MB buffer, 130s timeout
      );
      const body = result.toString('utf-8');
      if (body && !body.trimStart().startsWith('<!DOCTYPE') && !body.trimStart().startsWith('<html')) {
        return Promise.resolve(body);
      }
      log.warn('curl returned HTML instead of CSV — falling back to Node.js HTTPS');
    } catch (err) {
      log.warn(`curl failed: ${err.message} — falling back to Node.js HTTPS`);
    }

    // Strategy 2: Node.js HTTPS (fallback)
    return new Promise((resolve, reject) => {
      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, {
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/csv, text/plain, */*',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 120000, // 2 minutes — file is ~15MB
      }, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return this._downloadCSV(redirect, rateLimiter).then(resolve).catch(reject);
        }

        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`CSV download returned ${res.statusCode} for ${url}`));
        }

        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf-8');
          resolve(body);
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error(`CSV download timed out for ${url}`)); });
    });
  }

  /**
   * Get cache file path for a given CSV source.
   */
  _cacheFilePath(sourceName) {
    return path.join(CACHE_DIR, `dbpr-${sourceName}.csv`);
  }

  /**
   * Load CSV data — from cache if fresh, otherwise download and cache.
   * Returns an array of parsed record arrays.
   */
  async _loadCSV(url, sourceName, rateLimiter) {
    const cacheFile = this._cacheFilePath(sourceName);

    // Check cache
    if (fs.existsSync(cacheFile)) {
      try {
        const stat = fs.statSync(cacheFile);
        const ageMs = Date.now() - stat.mtimeMs;
        if (ageMs < CACHE_TTL_MS) {
          const ageDays = Math.round(ageMs / (24 * 60 * 60 * 1000) * 10) / 10;
          log.info(`Loading DBPR ${sourceName} from cache (${ageDays}d old)`);
          const raw = fs.readFileSync(cacheFile, 'utf-8');
          const records = this._parseCSVData(raw);
          log.success(`Cache loaded: ${records.length.toLocaleString()} ${sourceName} records`);
          return records;
        }
        log.info(`DBPR ${sourceName} cache expired — redownloading`);
      } catch (err) {
        log.warn(`Cache read failed for ${sourceName}: ${err.message} — redownloading`);
      }
    }

    // Download
    log.info(`Downloading DBPR ${sourceName} CSV from ${url}`);
    const raw = await this._downloadCSV(url, rateLimiter);

    if (!raw || raw.length < 100) {
      throw new Error(`DBPR ${sourceName} CSV download returned empty/tiny response`);
    }

    // Validate it's actually CSV (not HTML error page)
    if (raw.trimStart().startsWith('<!DOCTYPE') || raw.trimStart().startsWith('<html')) {
      throw new Error(`DBPR ${sourceName} CSV download returned HTML instead of CSV`);
    }

    // Cache to disk
    try {
      if (!fs.existsSync(CACHE_DIR)) {
        fs.mkdirSync(CACHE_DIR, { recursive: true });
      }
      fs.writeFileSync(cacheFile, raw);
      log.info(`Cached DBPR ${sourceName} data to ${cacheFile}`);
    } catch (err) {
      log.warn(`Failed to cache DBPR ${sourceName} data: ${err.message}`);
    }

    const records = this._parseCSVData(raw);
    log.success(`Downloaded ${records.length.toLocaleString()} ${sourceName} records`);
    return records;
  }

  /**
   * Parse raw CSV text into an array of field arrays.
   * Filters out empty lines and lines that don't parse to 15 fields.
   */
  _parseCSVData(raw) {
    const lines = raw.split('\n');
    const records = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      // DBPR CSV has no header row — every line is data
      // Skip any line that starts with a quote but doesn't look like a record
      if (!trimmed.startsWith('"')) continue;

      const fields = parseCSVLine(trimmed);
      // Valid DBPR records have 15 fields
      if (fields.length >= 14) {
        records.push(fields);
      }
    }

    return records;
  }

  /**
   * Map a parsed CSV record (array of 15 fields) to a normalized lead object.
   */
  _mapRecord(fields) {
    const typeCode = (fields[0] || '').trim();
    const typeName = (fields[1] || '').trim();
    const firstName = titleCase((fields[2] || '').trim());
    const middleName = (fields[3] || '').trim();
    const lastName = titleCase((fields[4] || '').trim());
    const suffix = (fields[5] || '').trim();
    const address1 = (fields[6] || '').trim();
    const address2 = (fields[7] || '').trim();
    const address3 = (fields[8] || '').trim();
    const city = titleCase((fields[9] || '').trim());
    const state = (fields[10] || 'FL').trim();
    const zip = (fields[11] || '').trim();
    const countyCode = (fields[12] || '').trim();
    const phone = normalizePhone((fields[13] || '').trim());

    // Build full name with suffix
    let fullLast = lastName;
    if (suffix) {
      fullLast = `${lastName} ${suffix}`;
    }

    // Build address
    const addressParts = [address1, address2, address3].filter(Boolean);
    const address = addressParts.join(', ');

    // Look up type info
    const typeInfo = TYPE_CODE_MAP[typeCode] || {
      abbrev: typeCode,
      name: typeName,
      area: 'general contractor',
    };

    // County name from FIPS code
    const county = COUNTY_CODES[countyCode] || '';

    return {
      first_name: firstName,
      last_name: fullLast,
      firm_name: '',  // Not in the CSV — individual licensee records
      address: address,
      city: city,
      state: state,
      zip: zip,
      county: county,
      phone: phone,
      email: '',  // Not in bulk CSV — available via individual license detail pages
      website: '',
      bar_number: '',  // License number not in applicant CSV
      bar_status: 'Active', // CSV only contains Active/Inactive (no NULL/VOID)
      license_type_code: typeCode,
      license_type: typeInfo.name,
      license_abbrev: typeInfo.abbrev,
      source: 'dbpr_contractors',
      practice_area: typeInfo.area,
    };
  }

  /**
   * Check if a record's type code matches the requested practice area filter.
   */
  _matchesPracticeArea(typeCode, practiceCode) {
    if (!practiceCode) return true; // No filter — include all

    // Special HVAC group: matches AC (CAC), Mechanical (CMC), Sheet Metal (CSM)
    if (practiceCode === 'HVAC') {
      const hvacCodes = [
        ...(ABBREV_TO_CODES['CAC'] || []),
        ...(ABBREV_TO_CODES['CMC'] || []),
        ...(ABBREV_TO_CODES['CSM'] || []),
      ];
      return hvacCodes.includes(typeCode);
    }

    // practiceCode is an abbreviation like 'CGC', 'CBC', etc.
    const matchingCodes = ABBREV_TO_CODES[practiceCode];
    if (matchingCodes) {
      return matchingCodes.includes(typeCode);
    }

    // Direct type code match
    return typeCode === practiceCode;
  }

  /**
   * Check if a record's city matches the city filter.
   * Case-insensitive, supports partial matching.
   */
  _matchesCity(recordCity, filterCity) {
    if (!filterCity) return true;
    const rc = recordCity.toLowerCase().trim();
    const fc = filterCity.toLowerCase().trim();
    return rc === fc || rc.includes(fc) || fc.includes(rc);
  }

  /**
   * Main search generator — downloads CSV data, filters, and yields leads.
   *
   * Strategy A (primary): Bulk CSV download (constr_app.csv + elec_app.csv)
   *   - 112K+ records with name, address, phone, license type
   *   - Cached locally for 7 days (CSV updated weekly)
   *   - Filtered by practice area and city
   *
   * Strategy B (email enrichment — future):
   *   - Individual license detail lookups via DBPR search interface
   *   - Session-based ASP.NET form requires Puppeteer for reliable access
   *   - Email addresses are public records per Section 455.275(1)
   *   - Not yet implemented — would need Puppeteer + session management
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape(`Florida DBPR Contractor Search — ${practiceArea || 'all trades'}`);
    if (practiceCode) {
      log.info(`License type filter: ${practiceCode}`);
    }

    // Determine which CSV files to download based on practice area
    const needsElectrical = !practiceCode ||
      practiceCode === 'EC' ||
      (practiceArea && practiceArea.toLowerCase().includes('electri'));

    const needsConstruction = !practiceCode ||
      practiceCode !== 'EC' ||
      !practiceArea;

    // Collect all records from relevant CSV files
    let allRecords = [];

    yield { _cityProgress: { current: 1, total: 1 } };

    // Download construction CSV
    if (needsConstruction) {
      try {
        const constrRecords = await this._loadCSV(CONSTR_CSV_URL, 'construction', rateLimiter);
        allRecords = allRecords.concat(constrRecords);
      } catch (err) {
        log.error(`Failed to load construction CSV: ${err.message}`);
      }
    }

    // Download electrical CSV
    if (needsElectrical) {
      try {
        const elecRecords = await this._loadCSV(ELEC_CSV_URL, 'electrical', rateLimiter);
        allRecords = allRecords.concat(elecRecords);
      } catch (err) {
        log.error(`Failed to load electrical CSV: ${err.message}`);
      }
    }

    if (allRecords.length === 0) {
      log.warn('DBPR: No CSV records loaded');
      return;
    }

    log.info(`Processing ${allRecords.length.toLocaleString()} total DBPR records`);

    // Determine city filter
    const cityFilter = options.city || null;

    // Apply maxPages as a record limit (1000 per "page")
    const maxRecords = options.maxPages
      ? options.maxPages * this.pageSize
      : Infinity;

    // Stats
    let matchedType = 0;
    let matchedCity = 0;
    let yieldedCount = 0;
    let withPhone = 0;
    let skippedOutOfState = 0;

    for (const fields of allRecords) {
      if (yieldedCount >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords.toLocaleString()})`);
        break;
      }

      const typeCode = (fields[0] || '').trim();

      // Filter by practice area / license type
      if (!this._matchesPracticeArea(typeCode, practiceCode)) continue;
      matchedType++;

      // Filter by Florida state (some records may be out-of-state)
      const recState = (fields[10] || '').trim().toUpperCase();
      if (recState && recState !== 'FL') {
        skippedOutOfState++;
        continue;
      }

      // Filter by city
      const recCity = (fields[9] || '').trim();
      if (cityFilter && !this._matchesCity(recCity, cityFilter)) continue;
      matchedCity++;

      // Map to lead object
      const lead = this._mapRecord(fields);

      // Track stats
      if (lead.phone) withPhone++;

      yield this.transformResult(lead, practiceArea || lead.practice_area);
      yieldedCount++;
    }

    log.success([
      `DBPR scrape complete:`,
      `${allRecords.length.toLocaleString()} total CSV records`,
      `${matchedType.toLocaleString()} matched type`,
      `${matchedCity.toLocaleString()} matched city`,
      `${yieldedCount.toLocaleString()} yielded`,
      `(${withPhone.toLocaleString()} with phone)`,
      skippedOutOfState > 0 ? `(${skippedOutOfState} out-of-state skipped)` : '',
    ].filter(Boolean).join(' | '));
  }
}

module.exports = new DbprScraper();
