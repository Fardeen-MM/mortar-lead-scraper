/**
 * Quebec RBQ (Regie du batiment du Quebec) — Contractor Licence Scraper
 *
 * Source: https://open.canada.ca/data/en/dataset/755b45d6-7aee-46df-a216-748a0191c79f
 * Platform: Donnees Quebec open data — free CSV/ZIP download
 * Method: Download ZIP containing full CSV of all active licensed contractors
 *
 * Data dictionary (from official PDF):
 *   Numero de licence, Statut de la licence, Type de licence,
 *   Date de delivrance, Restriction, Date de debut de la restriction,
 *   Date de fin de la restriction, Association ou compagnie fournissant le cautionnement,
 *   Montant de la caution, Date du paiement annuel, Mandataire,
 *   Courriel (EMAIL!), Adresse, NEQ, Nom de l'intervenant,
 *   Numero de telephone (PHONE!), Municipalite, Statut juridique,
 *   Code de region administrative, Region administrative,
 *   Nombre de sous-categories autorisees, Categories et sous-categories,
 *   Autre nom (DBA)
 *
 * This is a GOLDMINE — every record has email and phone.
 * Licensed under Creative Commons 4.0 Attribution (CC-BY) — Quebec.
 *
 * Cache: Downloads once → data/contractor-cache/rbq-licences.csv
 * Re-downloads if cache is older than 7 days.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const ZIP_URL = 'https://www.donneesquebec.ca/recherche/dataset/755b45d6-7aee-46df-a216-748a0191c79f/resource/32f6ec46-85fd-45e9-945b-965d9235840a/download/rdl01_extractiondonneesouvertes.zip';

const CACHE_DIR = path.join(__dirname, '..', '..', 'data', 'contractor-cache');
const CACHE_FILE = path.join(CACHE_DIR, 'rbq-licences.csv');
const CACHE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// ---- French column name mapping ----
// The CSV header line uses French names. We map them to internal keys.
// We try multiple variations in case the file uses slightly different names.
const COLUMN_MAP = {
  'numero de licence':             'licence_number',
  'numéro de licence':             'licence_number',
  'statut de la licence':          'licence_status',
  'type de licence':               'licence_type',
  'date de delivrance':            'issue_date',
  'date de délivrance':            'issue_date',
  'restriction':                   'restriction',
  'date de debut de la restriction':   'restriction_start',
  'date de début de la restriction':   'restriction_start',
  'date de fin de la restriction':     'restriction_end',
  'association ou compagnie fournissant le cautionnement': 'surety',
  'montant de la caution':         'surety_amount',
  'date du paiement annuel':       'annual_payment_date',
  'mandataire':                    'mandatary',
  'courriel':                      'email',
  'adresse':                       'address',
  'neq':                           'neq',
  'nom de l\'intervenant':         'company_name',
  "nom de l'intervenant":          'company_name',
  'numero de telephone':           'phone',
  'numéro de téléphone':           'phone',
  'municipalite':                  'municipality',
  'municipalité':                  'municipality',
  'statut juridique':              'legal_status',
  'code de region administrative': 'admin_region_code',
  'code de région administrative': 'admin_region_code',
  'region administrative':         'admin_region',
  'région administrative':         'admin_region',
  'nombre de sous-categories autorisees':   'num_subcategories',
  'nombre de sous-catégories autorisées':   'num_subcategories',
  'categories et sous-categories': 'categories',
  'catégories et sous-catégories': 'categories',
  'autre nom':                     'dba',
};

// ---- RBQ sub-category codes → English-friendly trade names ----
// The CSV uses "Categorie" = "Generale" or "Specialisee" and sub-category codes.
// Sub-category codes from the RBQ classification system:
const SUBCAT_TO_TRADE = {
  'GPC': 'general contractor',  // General purpose construction
  'SEC': 'general contractor',  // Security/general
  'ADM': 'general contractor',  // Administrative
  '1.1.1': 'excavation', '1.1.2': 'excavation', '1.2': 'excavation',
  '1.3': 'concrete', '1.4': 'concrete', '1.5': 'concrete',
  '2.2': 'masonry', '2.4': 'carpentry', '2.5': 'carpentry', '2.7': 'roofing',
  '3.1': 'masonry', '3.2': 'painting',
  '4.2': 'flooring',
  '5.2': 'plumbing',
  '6.1': 'electrical', '6.2': 'electrical',
  '7': 'hvac',
  '8': 'insulation',
  '9': 'general contractor',
  '10': 'fire protection',
  '11.2': 'hvac',
  '12': 'general contractor',
  '13.2': 'excavation', '13.3': 'excavation', '13.4': 'concrete', '13.5': 'general contractor',
  '15.5': 'demolition', '15.7': 'demolition', '15.8': 'demolition',
  '16': 'general contractor',
  '17.1': 'general contractor', '17.2': 'general contractor',
};

// Category filter: which sub-category codes belong to each trade
const TRADE_SUBCATS = {
  'general contractor': ['GPC', 'SEC', 'ADM', '9', '12', '13.5', '16', '17.1', '17.2'],
  'electrical':         ['6.1', '6.2'],
  'plumbing':           ['5.2'],
  'hvac':               ['7', '11.2'],
  'roofing':            ['2.7'],
  'painting':           ['3.2'],
  'excavation':         ['1.1.1', '1.1.2', '1.2', '13.2', '13.3'],
  'masonry':            ['2.2', '3.1'],
  'carpentry':          ['2.4', '2.5'],
  'insulation':         ['8'],
  'concrete':           ['1.3', '1.4', '1.5', '13.4'],
  'fire protection':    ['10'],
  'demolition':         ['15.5', '15.7', '15.8'],
  'flooring':           ['4.2'],
};

// Default cities — major Quebec municipalities
const DEFAULT_CITIES = [
  'Montréal', 'Montreal',
  'Québec', 'Quebec',
  'Laval',
  'Gatineau',
  'Longueuil',
  'Sherbrooke',
  'Lévis', 'Levis',
  'Saguenay',
  'Trois-Rivières', 'Trois-Rivieres',
  'Terrebonne',
];

// Normalized city names for matching (lowercase, no accents)
function normalizeCity(city) {
  return (city || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents
    .replace(/[^a-z0-9 -]/g, '')
    .trim();
}

// Detect if a company name looks like a person's name (for first/last extraction)
function extractPersonName(companyName) {
  if (!companyName) return { first_name: '', last_name: '' };

  const name = companyName.trim();

  // Patterns that indicate it's a business, not a person
  const businessIndicators = [
    /\binc\.?\b/i, /\bltd\.?\b/i, /\bltée\.?\b/i, /\bltee\.?\b/i,
    /\bltée\b/i, /\benr\.?\b/i, /\bs\.e\.n\.c\.?/i, /\bsenc\b/i,
    /\bconstruction\b/i, /\bentreprise/i, /\bgroupe\b/i, /\bservice/i,
    /\bcompagnie\b/i, /\bcorp\.?\b/i, /\bplomberie\b/i, /\bélectrique\b/i,
    /\belectrique\b/i, /\btoiture\b/i, /\bcouverture\b/i, /\bexcavation\b/i,
    /\b(9|1)\d{8,9}\b/, // NEQ numbers sometimes in name
    /\d{4,}/, // long numbers = business
    /&/, /\bet\b/i, // partnerships
  ];

  for (const pattern of businessIndicators) {
    if (pattern.test(name)) return { first_name: '', last_name: '' };
  }

  // If the name has 2-3 words and looks like a person name (all alpha)
  const parts = name.split(/\s+/).filter(Boolean);
  if (parts.length >= 2 && parts.length <= 4) {
    const allAlpha = parts.every(p => /^[A-Za-zÀ-ÖØ-öø-ÿ'-]+$/.test(p));
    if (allAlpha) {
      return {
        first_name: parts[0],
        last_name: parts[parts.length - 1],
      };
    }
  }

  return { first_name: '', last_name: '' };
}

// ---- CSV parser (handles quoted fields, semicolons, etc.) ----

/**
 * Parse a single CSV line respecting quoted fields.
 * Handles: "field with ""escaped"" quotes", regular fields, empty fields.
 */
function parseCSVLine(line, delimiter) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const ch = line[i];

    if (inQuotes) {
      if (ch === '"') {
        // Check for escaped quote ""
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i += 2;
          continue;
        }
        // End of quoted field
        inQuotes = false;
        i++;
        continue;
      }
      current += ch;
      i++;
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
        continue;
      }
      if (ch === delimiter) {
        fields.push(current.trim());
        current = '';
        i++;
        continue;
      }
      current += ch;
      i++;
    }
  }

  // Last field
  fields.push(current.trim());
  return fields;
}

/**
 * Detect the delimiter used in the CSV (semicolon vs comma).
 * French CSVs commonly use semicolons.
 */
function detectDelimiter(headerLine) {
  const semicolons = (headerLine.match(/;/g) || []).length;
  const commas = (headerLine.match(/,/g) || []).length;
  // If semicolons significantly outnumber commas, use semicolons
  if (semicolons > commas * 0.5 && semicolons > 3) return ';';
  if (commas > semicolons) return ',';
  // Default to semicolons for French data
  return semicolons > 0 ? ';' : ',';
}

// ---- ZIP extraction (minimal, handles one file) ----

/**
 * Extract a single CSV file from a ZIP buffer.
 * Minimal ZIP parser — handles standard DEFLATE-compressed and STORED entries.
 * Only standard Node.js modules (zlib).
 */
function extractCSVFromZip(buffer) {
  // ZIP local file header signature: PK\x03\x04
  let offset = 0;
  const files = [];

  while (offset < buffer.length - 4) {
    const sig = buffer.readUInt32LE(offset);
    if (sig !== 0x04034b50) break; // Not a local file header

    const compressionMethod = buffer.readUInt16LE(offset + 8);
    const compressedSize = buffer.readUInt32LE(offset + 18);
    const uncompressedSize = buffer.readUInt32LE(offset + 22);
    const fileNameLen = buffer.readUInt16LE(offset + 26);
    const extraLen = buffer.readUInt16LE(offset + 28);
    const fileName = buffer.slice(offset + 30, offset + 30 + fileNameLen).toString('utf8');
    const dataStart = offset + 30 + fileNameLen + extraLen;
    const fileData = buffer.slice(dataStart, dataStart + compressedSize);

    files.push({ fileName, compressionMethod, fileData, compressedSize, uncompressedSize });
    offset = dataStart + compressedSize;
  }

  // Find the CSV file
  const csvEntry = files.find(f => f.fileName.toLowerCase().endsWith('.csv')) || files[0];
  if (!csvEntry) throw new Error('No files found in ZIP archive');

  log.info(`ZIP entry: ${csvEntry.fileName} (${csvEntry.compressedSize} bytes compressed, method=${csvEntry.compressionMethod})`);

  if (csvEntry.compressionMethod === 0) {
    // STORED — no compression
    return csvEntry.fileData;
  } else if (csvEntry.compressionMethod === 8) {
    // DEFLATE
    return zlib.inflateRawSync(csvEntry.fileData);
  } else {
    throw new Error(`Unsupported ZIP compression method: ${csvEntry.compressionMethod}`);
  }
}


class RbqScraper extends BaseScraper {
  constructor() {
    super({
      name: 'rbq_contractors',
      stateCode: 'RBQ',
      baseUrl: 'https://www.rbq.gouv.qc.ca',
      pageSize: 1000, // Not really paginated — bulk CSV
      practiceAreaCodes: {
        'general contractor': 'general contractor',
        'electrical':         'electrical',
        'plumbing':           'plumbing',
        'hvac':               'hvac',
        'roofing':            'roofing',
        'painting':           'painting',
        'excavation':         'excavation',
        'masonry':            'masonry',
        'carpentry':          'carpentry',
        'insulation':         'insulation',
        'glazing':            'glazing',
        'elevator':           'elevator',
        'fire protection':    'fire protection',
        'demolition':         'demolition',
        'landscaping':        'landscaping',
        'concrete':           'concrete',
        'welding':            'welding',
        'swimming pool':      'swimming pool',
      },
      defaultCities: [
        'Montreal', 'Quebec City', 'Laval', 'Gatineau', 'Longueuil',
        'Sherbrooke', 'Levis', 'Saguenay', 'Trois-Rivieres', 'Terrebonne',
      ],
    });
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  // ---- Download + cache logic ----

  /**
   * Check if the cache is fresh (less than 7 days old).
   */
  _isCacheFresh() {
    try {
      const stat = fs.statSync(CACHE_FILE);
      const age = Date.now() - stat.mtimeMs;
      return age < CACHE_MAX_AGE_MS;
    } catch {
      return false;
    }
  }

  /**
   * Download a URL and return the raw Buffer.
   * Follows redirects (up to 5).
   */
  _downloadBuffer(url, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': '*/*',
          'Accept-Encoding': 'identity',
        },
        timeout: 120000, // 2 minutes — it's a large file
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this._downloadBuffer(redirect, redirectCount + 1));
        }

        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
        }

        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks)));
        res.on('error', reject);
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Download timed out')); });
    });
  }

  /**
   * Download the ZIP, extract the CSV, and cache it.
   */
  async _downloadAndCache() {
    log.info('Downloading RBQ licence data ZIP...');
    const zipBuffer = await this._downloadBuffer(ZIP_URL);
    log.info(`Downloaded ${(zipBuffer.length / 1024 / 1024).toFixed(1)} MB ZIP`);

    // Extract CSV from ZIP
    const csvBuffer = extractCSVFromZip(zipBuffer);
    log.info(`Extracted CSV: ${(csvBuffer.length / 1024 / 1024).toFixed(1)} MB`);

    // Ensure cache directory exists
    fs.mkdirSync(CACHE_DIR, { recursive: true });

    // Write CSV to cache
    fs.writeFileSync(CACHE_FILE, csvBuffer);
    log.success(`Cached RBQ CSV to ${CACHE_FILE}`);

    return csvBuffer.toString('utf8');
  }

  /**
   * Get CSV content — from cache if fresh, otherwise download.
   */
  async _getCSVContent() {
    if (this._isCacheFresh()) {
      log.info(`Using cached RBQ data (${CACHE_FILE})`);
      const buffer = fs.readFileSync(CACHE_FILE);
      // Handle BOM
      let content = buffer.toString('utf8');
      if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
      return content;
    }

    let content = await this._downloadAndCache();
    // Handle BOM
    if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
    return content;
  }

  // ---- CSV parsing ----

  /**
   * Parse the full CSV into an array of record objects.
   * Maps French column names to internal keys.
   */
  _parseCSV(csvContent) {
    const lines = csvContent.split(/\r?\n/);
    if (lines.length < 2) return [];

    // Find delimiter from header line
    const headerLine = lines[0];
    const delimiter = detectDelimiter(headerLine);
    log.info(`Detected CSV delimiter: "${delimiter === ';' ? 'semicolon' : 'comma'}"`);

    // Parse header
    const rawHeaders = parseCSVLine(headerLine, delimiter);
    const headers = rawHeaders.map(h => {
      const lower = h.toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents for matching
        .trim();
      // Also try the original lowered (with accents) for exact matches
      const lowerOriginal = h.toLowerCase().trim();
      return COLUMN_MAP[lowerOriginal] || COLUMN_MAP[lower] || lower;
    });

    log.info(`CSV columns (${headers.length}): ${headers.join(', ')}`);

    // Parse data rows
    const records = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      const fields = parseCSVLine(line, delimiter);
      const record = {};
      for (let j = 0; j < headers.length && j < fields.length; j++) {
        record[headers[j]] = fields[j] || '';
      }
      records.push(record);
    }

    return records;
  }

  // ---- Category matching ----

  /**
   * Group raw CSV rows by licence number into unique contractor records.
   * Each licence has multiple rows (one per sub-category).
   * Returns Map of licence_number → { record, subcats[], category }.
   */
  _groupByLicence(records) {
    const grouped = new Map();
    for (const r of records) {
      const num = (r.licence_number || '').trim();
      if (!num) continue;
      const cat = (r.categorie || r.categories || '').trim();
      const subcat = (r['sous-categories'] || r.sous_categories || '').trim();

      if (!grouped.has(num)) {
        grouped.set(num, { record: r, subcats: [], category: '' });
      }
      const entry = grouped.get(num);
      // The category row has "Generale" or "Specialisee"
      if (cat && (cat === 'Generale' || cat === 'Specialisee')) {
        entry.category = cat;
      }
      // Sub-category rows have codes like "GPC", "6.2", etc.
      if (subcat && subcat !== cat) {
        entry.subcats.push(subcat);
      }
    }
    return grouped;
  }

  /**
   * Match a grouped licence against the requested practice area.
   */
  _matchesCategory(entry, practiceCode) {
    if (!practiceCode) return true;

    // "general contractor" also matches all "Generale" category licences
    if (practiceCode === 'general contractor' && entry.category === 'Generale') return true;

    const targetSubcats = TRADE_SUBCATS[practiceCode];
    if (!targetSubcats) return true; // Unknown code — return all

    return entry.subcats.some(sc => targetSubcats.includes(sc));
  }

  /**
   * Determine the primary trade from sub-category codes.
   */
  _detectTrade(entry) {
    if (entry.category === 'Generale') return 'general contractor';
    for (const sc of (entry.subcats || [])) {
      const trade = SUBCAT_TO_TRADE[sc];
      if (trade) return trade;
    }
    return 'general contractor';
  }

  // ---- City matching ----

  /**
   * Normalize city names for matching (strip accents, lowercase).
   */
  _matchesCity(record, cityFilter) {
    if (!cityFilter) return true;

    const recordCity = normalizeCity(record.municipality);
    const filterCity = normalizeCity(cityFilter);

    // Exact match
    if (recordCity === filterCity) return true;

    // Contains match (e.g., "Quebec" matches "Quebec City" or "Ville de Quebec")
    if (recordCity.includes(filterCity) || filterCity.includes(recordCity)) return true;

    return false;
  }

  // ---- Phone normalization ----

  /**
   * Normalize Quebec phone numbers to standard format.
   */
  _normalizePhone(raw) {
    if (!raw) return '';
    // Strip everything except digits
    const digits = raw.replace(/\D/g, '');
    // Quebec phones are 10 digits (area code + 7 digit local)
    if (digits.length === 10) {
      return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
    }
    // If 11 digits starting with 1 (country code)
    if (digits.length === 11 && digits[0] === '1') {
      return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
    }
    // Return cleaned if not standard length
    return raw.trim();
  }

  // ---- Email validation ----

  _isValidEmail(email) {
    if (!email) return false;
    const e = email.trim().toLowerCase();
    // Basic sanity check
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e) && e.length < 254;
  }

  // ---- Map record to lead ----

  _mapRecord(record, practiceArea) {
    const email = (record.email || '').trim().toLowerCase();
    const phone = this._normalizePhone(record.phone);
    const companyName = (record.company_name || '').trim();
    const dba = (record.dba || '').trim();
    const { first_name, last_name } = extractPersonName(companyName);

    // Use DBA as firm name if the company name looks like a person
    const firmName = first_name
      ? (dba || companyName)
      : (companyName || dba);

    const trade = practiceArea || 'general contractor';
    const municipality = (record.municipality || '').trim();

    return {
      first_name: first_name || '',
      last_name: last_name || '',
      firm_name: firmName,
      city: municipality,
      state: 'QC',
      phone: phone,
      website: '',
      email: this._isValidEmail(email) ? email : '',
      bar_number: (record.licence_number || '').trim(),
      bar_status: (record.licence_status || '').trim(),
      admission_date: (record.issue_date || '').trim(),
      source: 'rbq_contractors',
      practice_area: trade,
      // Extra fields for enrichment
      neq: (record.neq || '').trim(),
      address: (record.address || '').trim(),
      dba: dba,
      licence_type: (record.licence_type || '').trim(),
      admin_region: (record.admin_region || '').trim(),
      categories: (record.categories || '').trim(),
      legal_status: (record.legal_status || '').trim(),
    };
  }

  // ---- Main search generator ----

  async *search(practiceArea, options = {}) {
    const practiceCode = this.resolvePracticeCode(practiceArea);
    const maxPages = options.maxPages || Infinity;
    const cityFilter = options.city || null;
    const maxCities = options.maxCities || Infinity;

    log.scrape('Quebec RBQ Contractor Licence scraper starting...');
    log.info(`Practice area: ${practiceArea || 'all trades'}, City filter: ${cityFilter || 'none'}`);

    // Step 1: Get CSV content (download or cache)
    let csvContent;
    try {
      csvContent = await this._getCSVContent();
    } catch (err) {
      log.error(`Failed to get RBQ CSV data: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Download failed: ${err.message}` };
      return;
    }

    // Step 2: Parse CSV
    log.info('Parsing RBQ CSV data...');
    const rawRecords = this._parseCSV(csvContent);
    log.success(`Parsed ${rawRecords.length.toLocaleString()} total CSV rows`);

    if (rawRecords.length === 0) {
      log.warn('No records found in CSV');
      return;
    }

    // Step 2b: Group by licence number (CSV has multiple rows per licence)
    const grouped = this._groupByLicence(rawRecords);
    log.info(`Grouped into ${grouped.size.toLocaleString()} unique licences`);

    // Step 3: Filter grouped records
    const filtered = [];
    for (const [num, entry] of grouped) {
      const r = entry.record;
      // Filter by licence status — keep only active
      const status = (r.licence_status || '').toLowerCase().trim();
      if (status && !status.includes('active') && !status.includes('valide') && !status.includes('actif')) continue;

      // Filter by entrepreneur type (skip owner-builders)
      const type = (r.licence_type || '').toLowerCase();
      if (type.includes('constructeur-propri')) continue;

      // Filter by restriction
      const restriction = (r.restriction || '').toLowerCase().trim();
      if (restriction === 'oui' || restriction === 'yes') continue;

      // Filter by practice area / trade
      if (practiceCode && !this._matchesCategory(entry, practiceCode)) continue;

      filtered.push({ record: r, entry });
    }
    log.info(`After all filters: ${filtered.length.toLocaleString()} unique contractors`);

    // Step 4: City iteration or full dump
    let cities;
    if (cityFilter) {
      cities = [cityFilter];
    } else {
      // Get unique municipalities from filtered data, sorted by count descending
      const cityCounts = {};
      for (const { record: r } of filtered) {
        const city = (r.municipality || '').trim();
        if (city) cityCounts[city] = (cityCounts[city] || 0) + 1;
      }
      // Use default cities first, then fill with top cities from data
      const defaultNormalized = DEFAULT_CITIES.map(normalizeCity);
      const defaultMatched = [];
      const seen = new Set();
      for (const city of Object.keys(cityCounts)) {
        const norm = normalizeCity(city);
        if (defaultNormalized.includes(norm) && !seen.has(norm)) {
          defaultMatched.push(city);
          seen.add(norm);
        }
      }
      // If no city filter and not in test mode, yield ALL records without city grouping
      if (maxCities === Infinity && !options.maxPages) {
        cities = ['__ALL__'];
      } else {
        cities = defaultMatched.length > 0 ? defaultMatched : Object.keys(cityCounts).slice(0, 10);
      }
    }

    // Limit cities in test mode
    if (maxCities < Infinity && cities[0] !== '__ALL__') {
      cities = cities.slice(0, maxCities);
    }

    const isAll = cities.length === 1 && cities[0] === '__ALL__';
    let totalYielded = 0;
    let totalWithEmail = 0;
    let totalWithPhone = 0;

    // In test mode, limit total records
    const maxRecords = (options.maxPages && options.maxPages <= 2) ? 200 : Infinity;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      const cityLabel = isAll ? 'All Quebec' : city;

      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Processing: ${practiceArea || 'all trades'} contractors in ${cityLabel}`);

      // Filter by city
      let cityRecords;
      if (isAll) {
        cityRecords = filtered;
      } else {
        cityRecords = filtered.filter(({ record: r }) => this._matchesCity(r, city));
      }

      log.info(`${cityLabel}: ${cityRecords.length.toLocaleString()} records`);

      for (const { record, entry } of cityRecords) {
        if (totalYielded >= maxRecords) break;

        const trade = practiceArea || this._detectTrade(entry);
        const lead = this._mapRecord(record, trade);

        // Skip records with no useful contact info at all
        if (!lead.email && !lead.phone && !lead.firm_name) continue;

        yield this.transformResult(lead, practiceArea || lead.practice_area);
        totalYielded++;
        if (lead.email) totalWithEmail++;
        if (lead.phone) totalWithPhone++;
      }

      if (totalYielded >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords}) — stopping`);
        break;
      }
    }

    log.success(`RBQ scrape complete — ${totalYielded.toLocaleString()} contractors yielded`);
    log.success(`  With email: ${totalWithEmail.toLocaleString()} | With phone: ${totalWithPhone.toLocaleString()}`);
  }
}

module.exports = new RbqScraper();
