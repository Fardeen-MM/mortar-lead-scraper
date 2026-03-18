/**
 * California CSLB (Contractors State License Board) — Bulk Data Scraper
 *
 * Source: https://www.cslb.ca.gov/onlineservices/dataportal/
 * Records: ~700K+ active contractor licenses
 * Method: ASP.NET postback to ListByClassification.aspx — downloads XLS per classification
 *
 * The CSLB Data Portal provides bulk downloads of licensed contractors by classification.
 * The "XLS" files are actually tab-delimited text (a common government pattern).
 *
 * Available fields:
 *   License Number, Business Name, Address, City, State, Zip, Phone,
 *   Classification(s), Issue Date, Expiration Date, License Status,
 *   Personnel (qualifier/RMO/RME names)
 *
 * NOTE: Per California B&P Code Section 27, email addresses are NOT provided.
 *
 * CSLB classification codes:
 *   A  — General Engineering Contractor
 *   B  — General Building Contractor
 *   B-2 — Residential Remodeling
 *   C-2 through C-61 — Specialty contractors (electrical, plumbing, HVAC, roofing, etc.)
 *   C-61/D-* — Sub-specialty codes (tree service, hydroseed, etc.)
 *   ASB — Asbestos Certification
 *   HAZ — Hazardous Substance Removal
 *
 * Cache strategy:
 *   Downloaded files are cached in data/contractor-cache/ with a 24-hour TTL.
 *   File naming: cslb-{classification}-{YYYY-MM-DD}.tsv
 *   On startup, stale files (>24h) are ignored and re-downloaded.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Cache directory for downloaded CSLB data files
const CACHE_DIR = path.join(__dirname, '..', '..', 'data', 'contractor-cache');
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

// CSLB Data Portal URLs
const LIST_BY_CLASSIFICATION_URL = 'https://www2.cslb.ca.gov/OnlineServices/DataPortal/ListByClassification.aspx';

// Map friendly names to CSLB classification codes
const PRACTICE_AREA_CODES = {
  'general contractor': 'B',
  'general building': 'B',
  'general engineering': 'A',
  'remodeling': 'B-2',
  'residential remodeling': 'B-2',
  'painting': 'C-33',
  'painting and decorating': 'C-33',
  'roofing': 'C-39',
  'plumbing': 'C-36',
  'hvac': 'C-20',
  'heating': 'C-20',
  'air conditioning': 'C-20',
  'landscaping': 'C-27',
  'electrical': 'C-10',
  'sheet metal': 'C-43',
  'solar': 'C-46',
  'tile': 'C-54',
  'tree service': 'C-61/D-49',
  'tree': 'C-61/D-49',
  'insulation': 'C-2',
  'boiler': 'C-4',
  'framing': 'C-5',
  'cabinet': 'C-6',
  'low voltage': 'C-7',
  'concrete': 'C-8',
  'drywall': 'C-9',
  'earthwork': 'C-12',
  'fencing': 'C-13',
  'flooring': 'C-15',
  'fire protection': 'C-16',
  'glazing': 'C-17',
  'steel': 'C-22',
  'masonry': 'C-29',
  'pipeline': 'C-34',
  'lathing': 'C-35',
  'refrigeration': 'C-38',
  'sanitation': 'C-42',
  'sign': 'C-45',
  'general': 'B',
};

// Full classification descriptions for display
const CLASSIFICATION_NAMES = {
  'A': 'General Engineering',
  'B': 'General Building',
  'B-2': 'Residential Remodeling',
  'C-2': 'Insulation & Acoustical',
  'C-4': 'Boiler, Hot Water Heating & Steam Fitting',
  'C-5': 'Framing & Rough Carpentry',
  'C-6': 'Cabinet, Millwork & Finish Carpentry',
  'C-7': 'Low Voltage Systems',
  'C-8': 'Concrete',
  'C-9': 'Drywall',
  'C-10': 'Electrical',
  'C-11': 'Elevator',
  'C-12': 'Earthwork & Paving',
  'C-13': 'Fencing',
  'C-15': 'Flooring & Floor Covering',
  'C-16': 'Fire Protection',
  'C-17': 'Glazing',
  'C-20': 'Warm-Air Heating, Ventilating & Air Conditioning',
  'C-21': 'Building Moving/Demolition',
  'C-22': 'Asbestos Abatement',
  'C-23': 'Ornamental Metal',
  'C-27': 'Landscaping',
  'C-28': 'Lock & Security Equipment',
  'C-29': 'Masonry',
  'C-31': 'Construction Zone Traffic Control',
  'C-32': 'Parking & Highway Improvement',
  'C-33': 'Painting & Decorating',
  'C-34': 'Pipeline',
  'C-35': 'Lathing & Plastering',
  'C-36': 'Plumbing',
  'C-38': 'Refrigeration',
  'C-39': 'Roofing',
  'C-42': 'Sanitation System',
  'C-43': 'Sheet Metal',
  'C-45': 'Signs',
  'C-46': 'Solar',
  'C-47': 'General Manufactured Housing',
  'C-50': 'Reinforcing Steel',
  'C-51': 'Structural Steel',
  'C-53': 'Swimming Pool',
  'C-54': 'Tile (Ceramic & Mosaic)',
  'C-55': 'Water Conditioning',
  'C-57': 'Well Drilling',
  'C-60': 'Welding',
  'C-61': 'Limited Specialty',
  'C-61/D-49': 'Tree Service',
};

// Top California cities for default search
const DEFAULT_CITIES = [
  'Los Angeles', 'San Diego', 'San Jose', 'San Francisco', 'Sacramento',
  'Fresno', 'Oakland', 'Long Beach', 'Bakersfield', 'Anaheim',
  'Santa Ana', 'Riverside', 'Stockton', 'Irvine', 'Chula Vista',
  'Santa Clarita', 'Fremont', 'San Bernardino', 'Modesto', 'Fontana',
];

class CslbScraper extends BaseScraper {
  constructor() {
    super({
      name: 'cslb-contractors',
      stateCode: 'CSLB',
      baseUrl: 'https://www.cslb.ca.gov',
      pageSize: 500,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });

    // In-memory cache of parsed records keyed by classification code
    this._recordCache = {};
  }

  // Not used — search() is fully overridden
  buildSearchUrl() {
    throw new Error('cslb: buildSearchUrl() is not used — search() is overridden');
  }
  parseResultsPage() {
    throw new Error('cslb: parseResultsPage() is not used — search() is overridden');
  }
  extractResultCount() {
    throw new Error('cslb: extractResultCount() is not used — search() is overridden');
  }

  /**
   * Ensure the cache directory exists.
   */
  _ensureCacheDir() {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  }

  /**
   * Get the cache file path for a given classification code and date.
   */
  _cacheFilePath(classificationCode) {
    const dateStr = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const safeCode = classificationCode.replace(/\//g, '-');
    return path.join(CACHE_DIR, `cslb-${safeCode}-${dateStr}.tsv`);
  }

  /**
   * Check if a cached file exists and is fresh (within TTL).
   */
  _getCachedFile(classificationCode) {
    this._ensureCacheDir();
    const filePath = this._cacheFilePath(classificationCode);

    if (!fs.existsSync(filePath)) return null;

    const stat = fs.statSync(filePath);
    const age = Date.now() - stat.mtimeMs;

    if (age > CACHE_TTL_MS) {
      log.info(`Cache expired for ${classificationCode} (${Math.round(age / 3600000)}h old)`);
      return null;
    }

    const content = fs.readFileSync(filePath, 'utf-8');
    if (content.length < 100) {
      log.warn(`Cache file for ${classificationCode} is too small — ignoring`);
      return null;
    }

    log.info(`Using cached data for ${classificationCode} (${Math.round(age / 60000)} min old)`);
    return content;
  }

  /**
   * Save data to the cache file.
   */
  _saveCacheFile(classificationCode, content) {
    this._ensureCacheDir();
    const filePath = this._cacheFilePath(classificationCode);
    fs.writeFileSync(filePath, content, 'utf-8');
    log.info(`Cached ${classificationCode} data to ${path.basename(filePath)}`);
  }

  /**
   * HTTP GET that returns the raw response body as a Buffer.
   */
  _httpGetRaw(url, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 60000,
      };

      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this._httpGetRaw(redirect, rateLimiter, redirectCount + 1));
        }

        const chunks = [];
        res.on('data', chunk => { chunks.push(chunk); });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: Buffer.concat(chunks),
          headers: res.headers,
          contentType: res.headers['content-type'] || '',
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * HTTP POST with form data. Returns raw Buffer response.
   */
  _httpPost(url, formData, cookies, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const body = formData;

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(body),
          'Connection': 'keep-alive',
          'Cookie': cookies || '',
          'Referer': url,
          'Origin': `${parsed.protocol}//${parsed.host}`,
        },
        timeout: 120000, // 2 min for large file downloads
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        // Follow redirects for POST
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          // Follow redirect with GET
          return resolve(this._httpGetRaw(redirect, rateLimiter));
        }

        const chunks = [];
        res.on('data', chunk => { chunks.push(chunk); });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: Buffer.concat(chunks),
          headers: res.headers,
          contentType: res.headers['content-type'] || '',
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('POST request timed out')); });
      req.write(body);
      req.end();
    });
  }

  /**
   * Extract ASP.NET form tokens (__VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION)
   * from the HTML body of a page.
   */
  _extractAspNetTokens(html) {
    const tokens = {};

    const viewStateMatch = html.match(/id="__VIEWSTATE"\s+value="([^"]*)"/);
    if (viewStateMatch) tokens.__VIEWSTATE = viewStateMatch[1];

    const viewStateGenMatch = html.match(/id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"/);
    if (viewStateGenMatch) tokens.__VIEWSTATEGENERATOR = viewStateGenMatch[1];

    const eventValidationMatch = html.match(/id="__EVENTVALIDATION"\s+value="([^"]*)"/);
    if (eventValidationMatch) tokens.__EVENTVALIDATION = eventValidationMatch[1];

    return tokens;
  }

  /**
   * Extract cookies from Set-Cookie response headers.
   */
  _extractCookies(headers) {
    const setCookies = headers['set-cookie'];
    if (!setCookies) return '';

    const cookieArr = Array.isArray(setCookies) ? setCookies : [setCookies];
    return cookieArr.map(c => c.split(';')[0]).join('; ');
  }

  /**
   * Download contractor data for a classification code from CSLB Data Portal.
   *
   * Process:
   * 1. GET the ListByClassification.aspx page to grab ASP.NET tokens + cookies
   * 2. POST with classification selection to trigger file download
   * 3. Parse the response (tab-delimited XLS)
   *
   * Returns the raw file content as a string, or null on failure.
   */
  async _downloadClassificationData(classificationCode, rateLimiter) {
    // Check cache first
    const cached = this._getCachedFile(classificationCode);
    if (cached) return cached;

    log.info(`Downloading CSLB data for classification ${classificationCode}...`);

    try {
      // Step 1: GET the form page to extract tokens
      await rateLimiter.wait();
      const pageResp = await this._httpGetRaw(LIST_BY_CLASSIFICATION_URL, rateLimiter);

      if (pageResp.statusCode !== 200) {
        log.warn(`CSLB ListByClassification page returned ${pageResp.statusCode}`);
        return null;
      }

      const pageHtml = pageResp.body.toString('utf-8');
      const tokens = this._extractAspNetTokens(pageHtml);
      const cookies = this._extractCookies(pageResp.headers);

      if (!tokens.__VIEWSTATE) {
        log.warn('Could not extract __VIEWSTATE from CSLB page — ASP.NET tokens missing');
        // Try alternative approach: direct URL probe
        return await this._tryAlternativeDownload(classificationCode, rateLimiter);
      }

      // Step 2: POST with classification selection
      const formParams = new URLSearchParams();
      formParams.append('__VIEWSTATE', tokens.__VIEWSTATE);
      if (tokens.__VIEWSTATEGENERATOR) {
        formParams.append('__VIEWSTATEGENERATOR', tokens.__VIEWSTATEGENERATOR);
      }
      if (tokens.__EVENTVALIDATION) {
        formParams.append('__EVENTVALIDATION', tokens.__EVENTVALIDATION);
      }
      formParams.append('lbClassification', classificationCode);
      formParams.append('btnSearch', 'Download');

      await rateLimiter.wait();
      const downloadResp = await this._httpPost(
        LIST_BY_CLASSIFICATION_URL,
        formParams.toString(),
        cookies,
        rateLimiter
      );

      if (downloadResp.statusCode !== 200) {
        log.warn(`CSLB download POST returned ${downloadResp.statusCode}`);
        return await this._tryAlternativeDownload(classificationCode, rateLimiter);
      }

      const contentType = (downloadResp.contentType || '').toLowerCase();
      const content = downloadResp.body.toString('utf-8');

      // Check if we got a file download (XLS/CSV/TSV) or an HTML error page
      if (contentType.includes('html') && content.includes('<html')) {
        // We got an HTML page back — the form submission may require additional fields
        // or the page uses JavaScript-driven download. Try alternative.
        log.warn('CSLB returned HTML instead of data file — trying alternative approach');
        return await this._tryAlternativeDownload(classificationCode, rateLimiter);
      }

      // Check if we got tab-delimited or comma-delimited data
      const lines = content.split('\n').filter(l => l.trim());
      if (lines.length < 2) {
        log.warn(`CSLB download for ${classificationCode} returned empty data`);
        return null;
      }

      // Cache the downloaded data
      this._saveCacheFile(classificationCode, content);
      log.success(`Downloaded CSLB ${classificationCode} data — ${lines.length} lines`);
      return content;

    } catch (err) {
      log.error(`CSLB download failed for ${classificationCode}: ${err.message}`);
      return await this._tryAlternativeDownload(classificationCode, rateLimiter);
    }
  }

  /**
   * Alternative download approach: try the ContractorList (master file) page
   * or probe known URL patterns for direct file access.
   */
  async _tryAlternativeDownload(classificationCode, rateLimiter) {
    // Try alternate URL patterns that some government sites use
    const altUrls = [
      `https://www2.cslb.ca.gov/OnlineServices/DataPortal/ListByClassification.aspx?Ession=0&classification=${classificationCode}`,
      `https://www.cslb.ca.gov/OnlineServices/DataPortal/ListByClassification.aspx?classification=${classificationCode}`,
    ];

    for (const url of altUrls) {
      try {
        await rateLimiter.wait();
        const resp = await this._httpGetRaw(url, rateLimiter);
        if (resp.statusCode === 200) {
          const content = resp.body.toString('utf-8');
          const contentType = (resp.contentType || '').toLowerCase();
          // Check if it's data (not HTML)
          if (!contentType.includes('html') || !content.includes('<html')) {
            const lines = content.split('\n').filter(l => l.trim());
            if (lines.length > 2) {
              this._saveCacheFile(classificationCode, content);
              log.success(`Alternative download for ${classificationCode} — ${lines.length} lines`);
              return content;
            }
          }
        }
      } catch (err) {
        log.info(`Alternative URL failed: ${err.message}`);
      }
    }

    log.warn(`All download methods failed for classification ${classificationCode}`);
    return null;
  }

  /**
   * Parse CSLB data file content into an array of contractor records.
   *
   * The CSLB "XLS" files are actually tab-delimited text with these columns:
   *   LicenseNumber, BusinessName, Address, City, State, Zip, Phone,
   *   IssueDate, ExpirationDate, LicenseStatus, Classifications
   *
   * Some files may have additional columns (bond info, personnel).
   * We detect the delimiter and header row automatically.
   */
  _parseDataFile(content, classificationCode) {
    const lines = content.split('\n').filter(l => l.trim());
    if (lines.length < 2) return [];

    // Detect delimiter: tab is most common for CSLB, but check for comma too
    const firstLine = lines[0];
    const tabCount = (firstLine.match(/\t/g) || []).length;
    const commaCount = (firstLine.match(/,/g) || []).length;
    const delimiter = tabCount >= commaCount ? '\t' : ',';

    // Parse header row to find column indices
    const headerFields = this._splitRow(lines[0], delimiter);
    const colMap = this._buildColumnMap(headerFields);

    const records = [];
    for (let i = 1; i < lines.length; i++) {
      const fields = this._splitRow(lines[i], delimiter);
      if (fields.length < 3) continue; // Skip short/broken lines

      const record = this._extractRecord(fields, colMap, classificationCode);
      if (record) records.push(record);
    }

    return records;
  }

  /**
   * Split a row by delimiter, handling quoted fields (CSV-style).
   */
  _splitRow(line, delimiter) {
    if (delimiter === ',') {
      // Handle CSV with possible quoted fields
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
        } else if (ch === delimiter && !inQuotes) {
          fields.push(current.trim());
          current = '';
        } else {
          current += ch;
        }
      }
      fields.push(current.trim());
      return fields;
    }

    // Tab-delimited — simple split
    return line.split(delimiter).map(f => f.trim());
  }

  /**
   * Build a column index map from header fields.
   * Handles various header naming conventions used by CSLB.
   */
  _buildColumnMap(headers) {
    const map = {};
    const normalized = headers.map(h => h.toLowerCase().replace(/[^a-z0-9]/g, ''));

    const patterns = {
      licenseNumber: ['licensenumber', 'licensenum', 'licno', 'license', 'licnum', 'lic'],
      businessName: ['businessname', 'business', 'name', 'contractorname', 'company', 'firmname', 'dba'],
      address: ['address', 'streetaddress', 'addr', 'street', 'mailingaddress'],
      city: ['city', 'businesscity', 'mailingcity'],
      state: ['state', 'businessstate', 'mailingstate', 'st'],
      zip: ['zip', 'zipcode', 'postalcode', 'businesszip', 'mailingzip'],
      phone: ['phone', 'telephone', 'phonenumber', 'businessphone', 'tel'],
      issueDate: ['issuedate', 'dateissued', 'issued', 'originalissuedate'],
      expirationDate: ['expirationdate', 'expdate', 'expires', 'expiration'],
      status: ['licensestatus', 'status', 'licstatus', 'licensetype'],
      classification: ['classification', 'classifications', 'class', 'classcode', 'licenseclassification'],
      firstName: ['firstname', 'first', 'fname', 'qualifierfirstname', 'personnelfirst'],
      lastName: ['lastname', 'last', 'lname', 'qualifierlastname', 'personnellast'],
      personnelName: ['personnelname', 'qualifier', 'qualifiername', 'rmo', 'rme', 'owner', 'ownername'],
    };

    for (const [field, aliases] of Object.entries(patterns)) {
      for (let i = 0; i < normalized.length; i++) {
        if (aliases.includes(normalized[i])) {
          map[field] = i;
          break;
        }
      }
    }

    // If we couldn't map by header names, use positional defaults
    // Common CSLB layout: LicNum, Name, Addr, City, State, Zip, Phone, IssueDate, ExpDate, Status, Class
    if (Object.keys(map).length < 3) {
      log.info('Using positional column mapping (headers not recognized)');
      if (headers.length >= 7) {
        map.licenseNumber = 0;
        map.businessName = 1;
        map.address = 2;
        map.city = 3;
        map.state = 4;
        map.zip = 5;
        map.phone = 6;
        if (headers.length >= 9) {
          map.issueDate = 7;
          map.expirationDate = 8;
        }
        if (headers.length >= 10) map.status = 9;
        if (headers.length >= 11) map.classification = 10;
      }
    }

    return map;
  }

  /**
   * Extract a contractor record from parsed fields using the column map.
   */
  _extractRecord(fields, colMap, classificationCode) {
    const get = (key) => {
      if (colMap[key] !== undefined && fields[colMap[key]]) {
        return fields[colMap[key]].trim().replace(/^"(.*)"$/, '$1');
      }
      return '';
    };

    const licenseNumber = get('licenseNumber');
    const businessName = get('businessName');
    const city = get('city');
    const state = get('state') || 'CA';
    const zip = get('zip');
    const phone = get('phone');
    const status = get('status');
    const classification = get('classification') || classificationCode;
    const issueDate = get('issueDate');
    const expirationDate = get('expirationDate');
    const firstName = get('firstName');
    const lastName = get('lastName');
    const personnelName = get('personnelName');

    // Skip if no license number and no business name
    if (!licenseNumber && !businessName) return null;

    // Parse personnel name if individual first/last not available
    let first = firstName;
    let last = lastName;
    if (!first && !last && personnelName) {
      const parts = personnelName.split(/\s+/);
      if (parts.length >= 2) {
        first = parts[0];
        last = parts[parts.length - 1];
      } else if (parts.length === 1) {
        last = parts[0];
      }
    }

    // Try to extract person name from business name (e.g., "JOHN SMITH CONSTRUCTION")
    if (!first && !last && businessName) {
      const nameFromBiz = this._extractPersonFromBizName(businessName);
      first = nameFromBiz.first;
      last = nameFromBiz.last;
    }

    return {
      first_name: this._titleCase(first),
      last_name: this._titleCase(last),
      firm_name: this._titleCase(businessName),
      city: this._titleCase(city),
      state: state.toUpperCase(),
      zip: zip,
      phone: this._normalizePhone(phone),
      email: '', // CSLB does not provide emails per B&P Code Section 27
      website: '',
      bar_number: licenseNumber,
      bar_status: this._normalizeStatus(status, expirationDate),
      admission_date: issueDate,
      practice_area: CLASSIFICATION_NAMES[classificationCode] || classification || classificationCode,
      source: 'cslb_contractors',
      profile_url: licenseNumber ? `https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum=${licenseNumber}` : '',
    };
  }

  /**
   * Try to extract a person's name from a business name.
   * Handles patterns like "JOHN SMITH CONSTRUCTION", "J & M PLUMBING", etc.
   */
  _extractPersonFromBizName(bizName) {
    if (!bizName) return { first: '', last: '' };

    const upper = bizName.toUpperCase();

    // Skip if it looks clearly non-personal
    const corpIndicators = ['INC', 'LLC', 'CORP', 'LTD', 'CO', 'COMPANY', 'GROUP', 'PARTNERS', 'SERVICES', 'ENTERPRISES'];
    const tradeWords = [
      'CONSTRUCTION', 'PLUMBING', 'ELECTRIC', 'ELECTRICAL', 'ROOFING', 'PAINTING',
      'LANDSCAPING', 'HEATING', 'COOLING', 'HVAC', 'SOLAR', 'TILE', 'CONCRETE',
      'FENCING', 'FLOORING', 'MASONRY', 'DRYWALL', 'REMODELING', 'BUILDERS',
      'BUILDING', 'CONTRACTING', 'CONTRACTOR', 'CONTRACTORS', 'HOMES', 'HOME',
      'MAINTENANCE', 'REPAIR', 'RESTORATION', 'RENOVATIONS', 'IMPROVEMENT',
    ];

    // Remove trade/corporate suffixes to isolate potential name
    let namePart = upper;
    for (const word of [...corpIndicators, ...tradeWords]) {
      namePart = namePart.replace(new RegExp(`\\b${word}\\b`, 'g'), '').trim();
    }
    // Remove trailing punctuation and connectors
    namePart = namePart.replace(/[,.\-&]+$/g, '').replace(/\s+&\s+/g, ' ').trim();

    const parts = namePart.split(/\s+/).filter(p => p.length > 1);
    if (parts.length === 2) {
      return { first: parts[0], last: parts[1] };
    }
    if (parts.length === 3) {
      // Could be first middle last
      return { first: parts[0], last: parts[2] };
    }

    return { first: '', last: '' };
  }

  /**
   * Title-case a string (e.g., "LOS ANGELES" -> "Los Angeles").
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  /**
   * Normalize phone number — strip non-digits, format.
   */
  _normalizePhone(phone) {
    if (!phone) return '';
    const digits = phone.replace(/\D/g, '');
    if (digits.length === 10) {
      return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
    }
    if (digits.length === 11 && digits[0] === '1') {
      return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
    }
    return phone.trim();
  }

  /**
   * Normalize license status. CSLB uses various codes.
   */
  _normalizeStatus(status, expirationDate) {
    if (!status) {
      // Infer from expiration date
      if (expirationDate) {
        const exp = new Date(expirationDate);
        return exp > new Date() ? 'Active' : 'Expired';
      }
      return 'Unknown';
    }

    const s = status.toUpperCase().trim();
    if (s === 'A' || s === 'ACT' || s.includes('ACTIVE')) return 'Active';
    if (s === 'I' || s === 'INA' || s.includes('INACTIVE')) return 'Inactive';
    if (s === 'E' || s === 'EXP' || s.includes('EXPIRED')) return 'Expired';
    if (s === 'S' || s === 'SUS' || s.includes('SUSPENDED')) return 'Suspended';
    if (s === 'R' || s === 'REV' || s.includes('REVOKED')) return 'Revoked';
    if (s === 'C' || s === 'CAN' || s.includes('CANCELLED')) return 'Cancelled';
    if (s === 'P' || s === 'PEN' || s.includes('PENDING')) return 'Pending';
    return status.trim();
  }

  /**
   * Filter records by city (case-insensitive partial match).
   */
  _filterByCity(records, city) {
    if (!city) return records;
    const target = city.toLowerCase().trim();
    return records.filter(r => {
      const rCity = (r.city || '').toLowerCase();
      return rCity === target || rCity.includes(target) || target.includes(rCity);
    });
  }

  /**
   * Filter records to only active licenses.
   */
  _filterActive(records) {
    return records.filter(r => {
      const status = (r.bar_status || '').toLowerCase();
      return status === 'active' || status === 'unknown';
    });
  }

  /**
   * Main search generator. Downloads CSLB data by classification, filters, and yields leads.
   *
   * @param {string} practiceArea - Trade type (e.g., "plumbing", "electrical", "general contractor")
   * @param {object} options - Search options
   * @param {string} options.city - Filter to a specific city
   * @param {number} options.maxPages - Limit results in test mode
   * @param {number} options.maxCities - Limit cities in test mode
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // Resolve practice area to classification code
    let classificationCode = this.resolvePracticeCode(practiceArea);
    if (!classificationCode && practiceArea) {
      // Check if it's already a valid code (e.g., "C-10", "B")
      const upper = practiceArea.toUpperCase().trim();
      if (CLASSIFICATION_NAMES[upper]) {
        classificationCode = upper;
      } else {
        log.warn(`Unknown practice area "${practiceArea}" for CSLB — defaulting to B (General Building)`);
        log.info(`Available areas: ${Object.keys(PRACTICE_AREA_CODES).join(', ')}`);
        classificationCode = 'B';
      }
    }
    if (!classificationCode) {
      classificationCode = 'B'; // Default to General Building
    }

    const classificationName = CLASSIFICATION_NAMES[classificationCode] || classificationCode;
    log.scrape(`CSLB: Searching ${classificationName} (${classificationCode}) contractors`);

    // Check in-memory cache first
    let records = this._recordCache[classificationCode];

    if (!records) {
      // Download the data file for this classification
      const content = await this._downloadClassificationData(classificationCode, rateLimiter);

      if (!content) {
        log.error(`Failed to download CSLB data for ${classificationCode} — no data source available`);
        yield { _captcha: true, city: 'all', reason: `CSLB data download failed for ${classificationCode}. The CSLB Data Portal may require browser-based access. Try visiting https://www.cslb.ca.gov/onlineservices/dataportal/ and downloading manually to data/contractor-cache/` };
        return;
      }

      // Parse the data file
      records = this._parseDataFile(content, classificationCode);
      log.info(`Parsed ${records.length} total records for ${classificationCode}`);

      // Filter to active licenses only
      records = this._filterActive(records);
      log.info(`${records.length} active ${classificationName} contractors after status filter`);

      // Cache parsed records in memory for subsequent city iterations
      this._recordCache[classificationCode] = records;
    }

    if (records.length === 0) {
      log.warn(`No active contractors found for ${classificationCode}`);
      return;
    }

    // Determine cities to iterate
    const cities = options.city
      ? [options.city]
      : (options.maxCities ? DEFAULT_CITIES.slice(0, options.maxCities) : DEFAULT_CITIES);

    let totalYielded = 0;
    const maxResults = options.maxPages ? options.maxPages * this.pageSize : Infinity;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Filtering ${classificationName} contractors in ${city}, CA`);

      // Filter by city
      const cityRecords = this._filterByCity(records, city);
      log.info(`Found ${cityRecords.length} active ${classificationName} contractors in ${city}`);

      if (cityRecords.length === 0) continue;

      for (const record of cityRecords) {
        if (totalYielded >= maxResults) {
          log.info(`Reached max results limit (${maxResults}) — stopping`);
          return;
        }

        yield this.transformResult(record, practiceArea || classificationName);
        totalYielded++;
      }
    }

    log.success(`CSLB: Yielded ${totalYielded} ${classificationName} contractors across ${cities.length} cities`);
  }

  /**
   * Override transformResult to set source properly.
   */
  transformResult(contractor, practiceArea) {
    contractor.source = 'cslb_contractors';
    if (practiceArea && !contractor.practice_area) {
      contractor.practice_area = practiceArea;
    }
    return contractor;
  }

  /**
   * Clean up old cache files (older than 7 days).
   * Called optionally to free disk space.
   */
  cleanCache() {
    this._ensureCacheDir();
    const maxAge = 7 * 24 * 60 * 60 * 1000; // 7 days
    let cleaned = 0;

    try {
      const files = fs.readdirSync(CACHE_DIR).filter(f => f.startsWith('cslb-'));
      for (const file of files) {
        const filePath = path.join(CACHE_DIR, file);
        const stat = fs.statSync(filePath);
        if (Date.now() - stat.mtimeMs > maxAge) {
          fs.unlinkSync(filePath);
          cleaned++;
        }
      }
      if (cleaned > 0) {
        log.info(`Cleaned ${cleaned} old CSLB cache files`);
      }
    } catch (err) {
      log.warn(`Cache cleanup failed: ${err.message}`);
    }
  }
}

module.exports = new CslbScraper();
