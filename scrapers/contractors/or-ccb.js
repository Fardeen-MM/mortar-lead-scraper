/**
 * Oregon Construction Contractors Board (OR CCB) — Socrata Open Data Scraper
 *
 * Source: https://data.oregon.gov/resource/g77e-6bhs.json
 * Portal: https://data.oregon.gov/business/CCB-Active-Licenses/g77e-6bhs
 * Platform: Socrata SODA API (free, no auth required)
 * Records: ~55K active licensed contractors
 * Method: JSON API with $limit/$offset pagination
 *
 * Fields from API:
 *   license_number    — CCB license number (e.g., "100215")
 *   license_type      — Short code (RGC, RSC, CGC1, CGC2, CSC1, CSC2, LBPR, RLC, etc.)
 *   related_key       — Related license key (optional)
 *   related_type      — Related license type (optional)
 *   county_code       — Numeric county code
 *   county_name       — County name (e.g., "Multnomah")
 *   lic_exp_date      — License expiration date (MM/DD/YYYY)
 *   orig_regis_date   — Original registration date (MM/DD/YYYY)
 *   bond_company      — Surety bond company
 *   bond_amount       — Bond amount (e.g., "25000")
 *   bond_exp_date     — Bond expiration date (MM/DD/YYYY)
 *   ins_company       — Insurance company
 *   ins_amount        — Insurance amount (e.g., "1000000")
 *   ins_exp_date      — Insurance expiration date (MM/DD/YYYY)
 *   full_name         — Licensee name (UPPERCASE, person or company)
 *   address           — Street address
 *   city              — City (UPPERCASE)
 *   state             — Two-letter state code (usually OR)
 *   zip_code          — Zip code (5-digit)
 *   phone_number      — Phone number (10 digits, no formatting)
 *   fax_number        — Fax number (optional)
 *   rmi_name          — Responsible Managing Individual name (optional)
 *   exempt_text       — Exempt / Nonexempt status
 *   endorsement_text  — Trade endorsement (e.g., "Residential General Contractor")
 *
 * License type breakdown (~55K total):
 *   RGC   — Residential General Contractor (30K)
 *   RSC   — Residential Specialty Contractor (7.5K)
 *   CGC2  — Commercial General Contractor Level 2 (5.5K)
 *   LBPR  — Lead Based Paint Renovation Contractor (4.4K)
 *   CSC2  — Commercial Specialty Contractor Level 2 (2.5K)
 *   CGC1  — Commercial General Contractor Level 1 (2.2K)
 *   CSC1  — Commercial Specialty Contractor Level 1 (970)
 *   RLC   — Residential Limited Contractor (930)
 *   OCHI  — Oregon Certified Home Inspector (605)
 *   OCLS  — Oregon Certified Locksmith (479)
 *   + several smaller categories (RHISC, RD, RLSC, CD, RHSC, CF, RHEPSC, RRC)
 *
 * Notes:
 *   - Phone numbers available on most records
 *   - Names are UPPERCASE — we title-case them
 *   - full_name can be a person name or business name
 *   - rmi_name (Responsible Managing Individual) is the person even for company licenses
 *   - No email in this dataset — enrichment pipeline handles that
 *   - County filter available server-side via $where clause
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_BASE = 'https://data.oregon.gov/resource/g77e-6bhs.json';
const PAGE_SIZE = 1000;

// Name suffixes to strip when splitting names
const NAME_SUFFIXES = new Set([
  'JR', 'SR', 'II', 'III', 'IV', 'V', 'JR.', 'SR.',
]);

// Words that indicate a business name rather than a person
const BUSINESS_INDICATORS = [
  'LLC', 'INC', 'CORP', 'CO', 'LTD', 'LP', 'COMPANY', 'CORPORATION',
  'ENTERPRISES', 'SERVICES', 'CONSTRUCTION', 'BUILDERS', 'CONTRACTING',
  'PLUMBING', 'ELECTRIC', 'ELECTRICAL', 'ROOFING', 'PAINTING',
  'HEATING', 'COOLING', 'HVAC', 'LANDSCAPING', 'REMODELING',
  'RESTORATION', 'SOLUTIONS', 'GROUP', 'PROPERTIES', 'DEVELOPMENT',
  'HOMES', 'INDUSTRIES', 'ASSOCIATES', 'MAINTENANCE', 'RENOVATIONS',
  'DRYWALL', 'FLOORING', 'SIDING', 'FRAMING', 'CONCRETE', 'MASONRY',
  'EXCAVATING', 'EXCAVATION', 'TRUCKING', 'FENCING', 'PAVING',
  'INSULATION', 'DEMOLITION', 'DESIGN', 'SYSTEMS', 'MECHANICAL',
];

// Endorsement text → friendly practice area mapping
const ENDORSEMENT_MAP = {
  'Residential General Contractor': 'General Contractor',
  'Residential Specialty Contractor': 'Specialty Contractor',
  'Commercial General Contractor Level 1': 'Commercial General',
  'Commercial General Contractor Level 2': 'Commercial General',
  'Commercial Specialty Contractor Level 1': 'Commercial Specialty',
  'Commercial Specialty Contractor Level 2': 'Commercial Specialty',
  'Lead Based Paint Renovation Contractor': 'Lead Paint Renovation',
  'Residential Limited Contractor': 'Limited Contractor',
  'Oregon Certified Home Inspector': 'Home Inspector',
  'Oregon Certified Locksmith': 'Locksmith',
  'Home Inspector Services Contractor': 'Home Inspector',
  'Residential Developer': 'Developer',
  'Residential Locksmith Services Contractor': 'Locksmith',
  'Commercial Developer': 'Commercial Developer',
  'Home Services Contractor': 'Home Services',
  'Construction Flagging Contractor': 'Flagging',
  'Home Energy Performance Score Contractor': 'Energy Performance',
  'Residential Restoration Contractor': 'Restoration',
};

// License type codes for filtering
const LICENSE_TYPE_CODES = {
  'general': 'RGC',
  'residential general': 'RGC',
  'rgc': 'RGC',
  'specialty': 'RSC',
  'residential specialty': 'RSC',
  'rsc': 'RSC',
  'commercial general': 'CGC2',
  'cgc1': 'CGC1',
  'cgc2': 'CGC2',
  'commercial specialty': 'CSC2',
  'csc1': 'CSC1',
  'csc2': 'CSC2',
  'lead paint': 'LBPR',
  'lbpr': 'LBPR',
  'limited': 'RLC',
  'rlc': 'RLC',
  'home inspector': 'OCHI',
  'ochi': 'OCHI',
  'locksmith': 'OCLS',
  'ocls': 'OCLS',
  'developer': 'RD',
  'rd': 'RD',
};

/**
 * Title-case a string (UPPERCASE -> Title Case).
 * Handles hyphens, apostrophes, and Mc/Mac prefixes.
 */
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|[\s\-'.])(\w)/g, (match) => match.toUpperCase())
    // Fix "Mcdonald" -> "McDonald", "Macarthur" -> "MacArthur"
    .replace(/\bMc(\w)/g, (_, c) => `Mc${c.toUpperCase()}`)
    .replace(/\bMac([a-z]{3,})/g, (_, rest) => `Mac${rest.charAt(0).toUpperCase()}${rest.slice(1)}`);
}

/**
 * Detect whether a name string is a business or a person.
 * Returns true if it looks like a business name.
 */
function isBusinessName(name) {
  if (!name) return false;
  const upper = name.toUpperCase();

  // Check for business-indicating words
  for (const word of BUSINESS_INDICATORS) {
    // Match as whole word (with optional trailing period)
    const re = new RegExp(`\\b${word}\\.?\\b`);
    if (re.test(upper)) return true;
  }

  // Check for common business patterns
  if (/\bD\.?B\.?A\.?\b/i.test(upper)) return true;       // DBA
  if (/\bL\.?L\.?C\.?\b/i.test(upper)) return true;        // L.L.C.
  if (/&/.test(upper)) return true;                         // "Smith & Sons"
  if (/\bTHE\s/i.test(upper)) return true;                  // "THE ..."
  if (/\d{2,}/.test(name)) return true;                     // Numbers (addresses in name)

  return false;
}

/**
 * Split a full person name into first/last, handling suffixes.
 * "BRUCE HUIE" -> { first: "Bruce", last: "Huie" }
 * "JERRY D PAINTER" -> { first: "Jerry", last: "Painter" }
 */
function splitPersonName(fullName) {
  if (!fullName) return { first: '', last: '' };

  const parts = fullName.trim().split(/\s+/).filter(Boolean);

  // Strip trailing suffixes
  while (parts.length > 1 && NAME_SUFFIXES.has(parts[parts.length - 1].toUpperCase())) {
    parts.pop();
  }

  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: '', last: titleCase(parts[0]) };

  // First token = first name, last token = last name (skip middle names/initials)
  return {
    first: titleCase(parts[0]),
    last: titleCase(parts[parts.length - 1]),
  };
}

/**
 * Format a 10-digit phone string to (XXX) XXX-XXXX.
 * Strips non-digits first. Handles 11-digit with leading 1.
 */
function formatPhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return raw.trim();
}

/**
 * Parse MM/DD/YYYY date to YYYY-MM-DD.
 */
function parseDate(raw) {
  if (!raw) return '';
  const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1]}-${m[2]}`;
  return raw.trim();
}


class OrCcbScraper extends BaseScraper {
  constructor() {
    super({
      name: 'or_ccb_contractors',
      stateCode: 'OR-CCB',
      baseUrl: API_BASE,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: LICENSE_TYPE_CODES,
      defaultCities: [
        'Portland', 'Salem', 'Eugene', 'Gresham', 'Hillsboro',
        'Bend', 'Beaverton', 'Medford', 'Springfield', 'Corvallis',
        'Albany', 'Tigard', 'Lake Oswego', 'Keizer', 'Grants Pass',
        'Oregon City', 'McMinnville', 'Redmond', 'Tualatin', 'West Linn',
      ],
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

  transformResult(lead, practiceArea) {
    lead.source = 'or_ccb_contractors';
    lead.practice_area = practiceArea || lead.practice_area || 'Construction';
    return lead;
  }

  /**
   * Fetch a page of results from the Socrata SODA API.
   * Returns parsed JSON array.
   */
  _fetchPage(offset, rateLimiter, whereClause) {
    const params = new URLSearchParams({
      '$limit': String(PAGE_SIZE),
      '$offset': String(offset),
      '$order': 'license_number',
    });
    if (whereClause) {
      params.set('$where', whereClause);
    }
    const url = `${API_BASE}?${params.toString()}`;

    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const req = https.request({
        hostname: parsed.hostname,
        path: `${parsed.pathname}?${parsed.searchParams.toString()}`,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Accept-Encoding': 'identity',
        },
        timeout: 30000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return reject(new Error(`Redirect ${res.statusCode} to ${res.headers.location}`));
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf-8');
          resolve({ statusCode: res.statusCode, body });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Socrata API request timed out')); });
      req.end();
    });
  }

  /**
   * Map a Socrata JSON record to a normalized lead object.
   */
  _mapRecord(rec) {
    const rawName = (rec.full_name || '').trim();
    const rmiName = (rec.rmi_name || '').trim();
    const isBusiness = isBusinessName(rawName);

    let firstName = '';
    let lastName = '';
    let firmName = '';

    if (isBusiness) {
      // Business: full_name is the firm, rmi_name is the person
      firmName = titleCase(rawName);
      if (rmiName && rmiName !== rawName) {
        const split = splitPersonName(rmiName);
        firstName = split.first;
        lastName = split.last;
      }
    } else {
      // Person: full_name is the person
      const split = splitPersonName(rawName);
      firstName = split.first;
      lastName = split.last;
      // If rmi_name differs from full_name, the full_name might be a DBA
      // but usually they match for sole proprietors
    }

    const endorsement = (rec.endorsement_text || '').trim();
    const practiceArea = ENDORSEMENT_MAP[endorsement] || endorsement || 'Construction';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      address: titleCase((rec.address || '').trim()),
      city: titleCase((rec.city || '').trim()),
      state: (rec.state || 'OR').trim().toUpperCase(),
      zip: (rec.zip_code || '').trim(),
      county: titleCase((rec.county_name || '').trim()),
      phone: formatPhone(rec.phone_number),
      fax: formatPhone(rec.fax_number),
      email: '',
      website: '',
      bar_number: (rec.license_number || '').trim(),
      bar_status: 'Active',
      license_type: (rec.license_type || '').trim(),
      endorsement: endorsement,
      entity_type: isBusiness ? 'Business' : 'Individual',
      rmi_name: rmiName ? titleCase(rmiName) : '',
      issue_date: parseDate(rec.orig_regis_date),
      expiration_date: parseDate(rec.lic_exp_date),
      bond_company: titleCase((rec.bond_company || '').trim()),
      bond_amount: (rec.bond_amount || '').trim(),
      insurance_company: titleCase((rec.ins_company || '').trim()),
      insurance_amount: (rec.ins_amount || '').trim(),
      exempt_status: (rec.exempt_text || '').trim(),
      source: 'or_ccb_contractors',
      practice_area: practiceArea,
      profile_url: `https://search.ccb.state.or.us/search/search_result.aspx?id=${encodeURIComponent(rec.license_number || '')}`,
    };
  }

  /**
   * Build the $where clause for Socrata filtering.
   * Supports city and license_type filters.
   */
  _buildWhereClause(options, practiceCode) {
    const clauses = [];

    // City filter (server-side for efficiency)
    if (options.city) {
      const city = options.city.toUpperCase().replace(/'/g, "''");
      clauses.push(`upper(city)='${city}'`);
    }

    // License type filter
    if (practiceCode) {
      clauses.push(`license_type='${practiceCode}'`);
    }

    return clauses.length > 0 ? clauses.join(' AND ') : null;
  }

  /**
   * Main search generator — paginates through Socrata API, yields leads.
   *
   * The Socrata SODA API returns JSON arrays with $limit/$offset pagination.
   * All records in this dataset are active (it's the "Active Licenses" dataset).
   * Optional city and license type filters applied server-side via $where.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    log.scrape('OR CCB — Oregon Construction Contractors Board (Socrata API, ~55K active licenses)');

    // Resolve practice area to license type code
    const practiceCode = this.resolvePracticeCode(practiceArea);
    if (practiceCode) {
      log.info(`License type filter: ${practiceCode}`);
    }

    // Build server-side filter
    const whereClause = this._buildWhereClause(options, practiceCode);
    if (whereClause) {
      log.info(`Socrata $where: ${whereClause}`);
    }
    if (options.city) {
      log.info(`City filter: ${options.city}`);
    }

    yield { _cityProgress: { current: 1, total: 1 } };

    // Apply maxPages as a record limit
    const maxRecords = options.maxPages
      ? options.maxPages * PAGE_SIZE
      : Infinity;

    let offset = 0;
    let yieldedCount = 0;
    let totalFetched = 0;
    let withPhone = 0;
    let individuals = 0;
    let businesses = 0;
    let pageNum = 0;

    while (true) {
      if (yieldedCount >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords.toLocaleString()})`);
        break;
      }

      pageNum++;
      log.info(`Page ${pageNum} — offset ${offset.toLocaleString()}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._fetchPage(offset, rateLimiter, whereClause);
      } catch (err) {
        log.error(`Socrata API request failed: ${err.message}`);
        break;
      }

      if (response.statusCode === 429) {
        log.warn('Socrata API rate limited (429) — waiting 5s and retrying');
        await new Promise(r => setTimeout(r, 5000));
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Socrata API returned ${response.statusCode}`);
        break;
      }

      let records;
      try {
        records = JSON.parse(response.body);
      } catch (err) {
        log.error(`Failed to parse Socrata JSON: ${err.message}`);
        break;
      }

      if (!Array.isArray(records) || records.length === 0) {
        log.info('No more records — pagination complete');
        break;
      }

      totalFetched += records.length;
      log.info(`Fetched ${records.length} records (${totalFetched.toLocaleString()} total)`);

      for (const rec of records) {
        if (yieldedCount >= maxRecords) break;

        const lead = this._mapRecord(rec);

        // Track stats
        if (lead.phone) withPhone++;
        if (lead.entity_type === 'Individual') {
          individuals++;
        } else {
          businesses++;
        }

        yield this.transformResult(lead, practiceArea);
        yieldedCount++;
      }

      // If we got fewer than PAGE_SIZE, we've reached the end
      if (records.length < PAGE_SIZE) {
        log.info('Last page reached (fewer records than page size)');
        break;
      }

      offset += PAGE_SIZE;
    }

    log.success([
      'OR CCB scrape complete:',
      `${totalFetched.toLocaleString()} fetched from API`,
      `${yieldedCount.toLocaleString()} yielded`,
      `(${individuals.toLocaleString()} individuals, ${businesses.toLocaleString()} businesses, ${withPhone.toLocaleString()} with phone)`,
    ].join(' | '));
  }
}

module.exports = new OrCcbScraper();
