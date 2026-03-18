/**
 * NYC Home Improvement Contractor (HIC) Scraper
 *
 * Source: NYC Open Data — Home Improvement Contractor Licenses
 * API: https://data.cityofnewyork.us/resource/acd4-wkax.json (Socrata SODA API)
 * Records: ~69K total, ~42K active home improvement contractor licenses
 * Method: JSON API with $limit/$offset pagination (free, no auth required)
 *
 * Available fields per record:
 *   license_nbr           — License number (e.g., "2054609-DCA")
 *   license_type          — "Premises" or "Individual"
 *   lic_expir_dd          — License expiration date (ISO 8601)
 *   license_status        — Status string ("Active", "Expired", etc.)
 *   license_creation_date — Date license was first issued (ISO 8601)
 *   business_name         — Business/DBA name (UPPERCASE)
 *   address_building      — Street number
 *   address_street_name   — Street name
 *   address_city          — City (UPPERCASE, e.g., "BROOKLYN", "NEW YORK", "FLUSHING")
 *   address_state         — State code (e.g., "NY")
 *   address_zip           — ZIP code
 *   address_borough       — Borough name ("Brooklyn", "Queens", "Manhattan", etc.)
 *   contact_phone         — Contact phone number (10 digits, no formatting)
 *   latitude              — GPS latitude
 *   longitude             — GPS longitude
 *
 * City/borough note:
 *   address_city uses neighborhood names (e.g., "FLUSHING", "JAMAICA", "ASTORIA"
 *   for Queens), not just the 5 boroughs. We search by address_borough for
 *   borough-level coverage, falling back to address_city when user specifies a
 *   neighborhood. Default search uses the 5 boroughs via address_borough.
 *
 * SODA API notes:
 *   - $limit max is 50,000 per request (we use 1,000 for safe pagination)
 *   - $offset for pagination (zero-indexed)
 *   - $where for filtering (e.g., license_status='Active')
 *   - $order for consistent pagination ordering
 *   - No API key needed for <1,000 req/hour; app tokens remove throttle
 *   - SoQL supports LIKE, AND, OR, NOT, IS NOT NULL, etc.
 *
 * Owner name extraction:
 *   The dataset has business_name only (no separate owner name field).
 *   We attempt to split business names that look like personal names
 *   (e.g., "JOHN SMITH" -> first=John, last=Smith) vs. business names
 *   (e.g., "ABC PLUMBING INC" -> firm_name only).
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_HOST = 'data.cityofnewyork.us';
const API_PATH = '/resource/acd4-wkax.json';
const PAGE_SIZE = 1000;

// The 5 NYC boroughs as they appear in the address_borough field
const NYC_BOROUGHS = ['Brooklyn', 'Queens', 'Manhattan', 'Bronx', 'Staten Island'];

// Business name keywords indicating a company (not a person's name)
const BUSINESS_KEYWORDS = [
  'inc', 'llc', 'corp', 'ltd', 'co', 'company', 'plumbing', 'electric',
  'electrical', 'heating', 'cooling', 'hvac', 'roofing', 'construction',
  'contracting', 'contractors', 'contractor', 'renovations', 'renovation',
  'remodeling', 'services', 'service', 'enterprises', 'enterprise',
  'builders', 'building', 'group', 'associates', 'solutions', 'systems',
  'maintenance', 'management', 'restoration', 'improvements', 'improvement',
  'home', 'general', 'sons', 'brothers', 'bros', 'industries', 'supply',
  'design', 'designs', 'interiors', 'exteriors', 'painting', 'floors',
  'flooring', 'kitchens', 'bathrooms', 'windows', 'doors', 'insulation',
  'mechanical', 'dba', 'd/b/a', 'pllc', 'pllp', 'lp', 'llp',
];

/**
 * Determine if a business_name looks like a person's name vs. a company name.
 * Returns { firstName, lastName, firmName } with best-guess split.
 *
 * Handles "LAST, FIRST" format (common in NYC data) as well as "FIRST LAST".
 */
function splitBusinessName(raw) {
  if (!raw) return { firstName: '', lastName: '', firmName: '' };

  const name = raw.trim();
  const lower = name.toLowerCase();

  // If it contains business keywords, treat entire thing as firm name
  for (const kw of BUSINESS_KEYWORDS) {
    // Match whole word only
    const pattern = new RegExp(`\\b${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    if (pattern.test(lower)) {
      return { firstName: '', lastName: '', firmName: titleCase(name) };
    }
  }

  // If it contains numbers, punctuation (beyond apostrophe/hyphen/comma), treat as business
  if (/\d/.test(name) || /[&@#!%]/.test(name)) {
    return { firstName: '', lastName: '', firmName: titleCase(name) };
  }

  // Check for "LAST, FIRST" or "LAST, FIRST MIDDLE" format
  const commaMatch = name.match(/^([A-Za-z'-]+)\s*,\s*(.+)$/);
  if (commaMatch) {
    const last = commaMatch[1].trim();
    const firstParts = commaMatch[2].trim().split(/\s+/);
    const first = firstParts[0] || '';
    // If there are too many parts after the comma, it might be a business
    if (firstParts.length <= 2 && first.length > 1) {
      return {
        firstName: titleCase(first),
        lastName: titleCase(last),
        firmName: '',
      };
    }
  }

  const words = name.split(/\s+/).filter(Boolean);

  // 2-3 words without business keywords — likely a person's name
  if (words.length >= 2 && words.length <= 3) {
    return {
      firstName: titleCase(words[0]),
      lastName: titleCase(words[words.length - 1]),
      firmName: '',
    };
  }

  // Single word or 4+ words — treat as business
  return { firstName: '', lastName: '', firmName: titleCase(name) };
}

/**
 * Title-case a string (UPPERCASE -> Title Case).
 * Handles hyphens and apostrophes.
 */
function titleCase(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|[\s\-'/(])(\w)/g, (match) => match.toUpperCase());
}

/**
 * Format a 10-digit phone number to (XXX) XXX-XXXX.
 */
function formatPhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return digits.length >= 7 ? raw.trim() : '';
}

/**
 * Format a date string to YYYY-MM-DD.
 */
function formatDate(isoStr) {
  if (!isoStr) return '';
  // Socrata returns ISO 8601 like "2024-01-15T00:00:00.000"
  const match = isoStr.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : '';
}

/**
 * Check if a string matches a known NYC borough name (case-insensitive).
 */
function isBorough(str) {
  if (!str) return false;
  const lower = str.toLowerCase().trim();
  return NYC_BOROUGHS.some(b => b.toLowerCase() === lower);
}


class NycHicScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nyc_hic_contractors',
      stateCode: 'NYC-HIC',
      baseUrl: `https://${API_HOST}`,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'home improvement': 'Home Improvement Contractor',
        'general': 'all',
      },
      defaultCities: ['Brooklyn', 'Queens', 'Manhattan', 'Bronx', 'Staten Island'],
    });
  }

  // --- Base class stubs (not used -- search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override transformResult to use our source name.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'nyc_hic_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTPS GET against the Socrata SODA API.
   * Returns { statusCode, body }.
   */
  _apiGet(queryString, rateLimiter) {
    return new Promise((resolve, reject) => {
      const fullPath = queryString ? `${API_PATH}?${queryString}` : API_PATH;

      const req = https.request({
        hostname: API_HOST,
        path: fullPath,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      }, (res) => {
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('NYC Open Data API request timed out')); });
      req.end();
    });
  }

  /**
   * Build the SODA API query string with filters, pagination, and ordering.
   *
   * When the location is a borough name (Brooklyn, Queens, Manhattan, Bronx,
   * Staten Island), we filter by address_borough for full coverage.
   * Otherwise, we filter by address_city to support neighborhood-level queries.
   *
   * @param {object} params
   * @param {string|null} params.location — borough or city/neighborhood name
   * @param {number} params.limit — max results per page
   * @param {number} params.offset — pagination offset (zero-indexed)
   * @returns {string} URL-encoded query string
   */
  _buildQueryString({ location, limit, offset }) {
    const params = new URLSearchParams();

    // Always filter to Active licenses
    let where = "license_status='Active'";

    if (location) {
      const escaped = location.replace(/'/g, "''");
      if (isBorough(location)) {
        // Use address_borough for borough-level filtering (covers all neighborhoods)
        where += ` AND address_borough='${escaped}'`;
      } else {
        // Use address_city for specific city/neighborhood filtering
        where += ` AND UPPER(address_city)='${escaped.toUpperCase()}'`;
      }
    }

    params.set('$where', where);
    params.set('$limit', String(limit));
    params.set('$offset', String(offset));
    params.set('$order', 'license_nbr ASC');

    return params.toString();
  }

  /**
   * Get total count for a given location filter using $select=count(*).
   */
  async _getCount(location, rateLimiter) {
    const params = new URLSearchParams();

    let where = "license_status='Active'";
    if (location) {
      const escaped = location.replace(/'/g, "''");
      if (isBorough(location)) {
        where += ` AND address_borough='${escaped}'`;
      } else {
        where += ` AND UPPER(address_city)='${escaped.toUpperCase()}'`;
      }
    }

    params.set('$select', 'count(*) as count');
    params.set('$where', where);

    const response = await this._apiGet(params.toString(), rateLimiter);
    if (response.statusCode !== 200) return 0;

    try {
      const data = JSON.parse(response.body);
      if (Array.isArray(data) && data.length > 0) {
        return parseInt(data[0].count, 10) || 0;
      }
    } catch (_) { /* ignore parse errors */ }
    return 0;
  }

  /**
   * Map a Socrata API record to a normalized lead object.
   */
  _mapRecord(rec) {
    const businessName = (rec.business_name || '').trim();
    const { firstName, lastName, firmName } = splitBusinessName(businessName);

    const building = (rec.address_building || '').trim();
    const street = (rec.address_street_name || '').trim();
    const address = [building, street].filter(Boolean).join(' ');

    const city = titleCase((rec.address_city || '').trim());
    const state = (rec.address_state || 'NY').trim();
    const zip = (rec.address_zip || '').trim();
    const borough = (rec.address_borough || '').trim();
    const phone = formatPhone(rec.contact_phone);

    const licenseNbr = (rec.license_nbr || '').trim();
    const licenseType = (rec.license_type || '').trim();
    const licenseStatus = (rec.license_status || '').trim();
    const expireDate = formatDate(rec.lic_expir_dd);
    const createdDate = formatDate(rec.license_creation_date);

    const lat = rec.latitude || '';
    const lng = rec.longitude || '';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      address,
      city: city || borough,
      state,
      zip,
      county: borough,  // Use borough as county equivalent for NYC
      phone,
      email: '',   // Not available in this dataset
      website: '', // Not available in this dataset
      bar_number: licenseNbr,
      bar_status: licenseStatus,
      license_type: licenseType,
      license_expiry: expireDate,
      license_created: createdDate,
      latitude: lat,
      longitude: lng,
      source: 'nyc_hic_contractors',
      practice_area: 'Home Improvement Contractor',
    };
  }

  /**
   * Async generator that yields contractor leads from NYC Open Data.
   *
   * Strategy:
   * 1. If options.city is specified, search that city/borough
   * 2. Otherwise, iterate through defaultCities (5 NYC boroughs via address_borough)
   * 3. Paginate via $offset until no more results returned
   * 4. Filter to license_status='Active' via SoQL $where clause
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Determine locations to search
    const locations = options.city
      ? [options.city]
      : this.defaultCities;
    const locationsToSearch = options.maxCities
      ? locations.slice(0, options.maxCities)
      : locations;

    log.scrape(`NYC Home Improvement Contractor search — ${practiceArea || 'all'}, locations=${locationsToSearch.length}`);

    // Track seen license numbers to deduplicate across locations
    const seenLicenses = new Set();
    let totalWithPhone = 0;
    let totalWithName = 0;

    for (let ci = 0; ci < locationsToSearch.length; ci++) {
      const location = locationsToSearch[ci];
      yield { _cityProgress: { current: ci + 1, total: locationsToSearch.length } };
      log.scrape(`Searching NYC HIC licenses in ${location}${isBorough(location) ? ' (borough)' : ''}`);

      // Get total count for this location
      let totalCount = 0;
      try {
        await rateLimiter.wait();
        totalCount = await this._getCount(location, rateLimiter);
      } catch (err) {
        log.warn(`Failed to get count for ${location}: ${err.message}`);
      }

      if (totalCount > 0) {
        const totalPages = Math.ceil(totalCount / PAGE_SIZE);
        log.success(`Found ${totalCount.toLocaleString()} active licenses in ${location} (${totalPages} pages)`);
      } else {
        log.info(`No active licenses found in ${location}`);
        continue;
      }

      let offset = 0;
      let pagesFetched = 0;
      let locationYielded = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${location}`);
          break;
        }

        const qs = this._buildQueryString({
          location,
          limit: PAGE_SIZE,
          offset,
        });

        log.info(`Page ${pagesFetched + 1} (offset ${offset}) — ${location}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiGet(qs, rateLimiter);
        } catch (err) {
          log.error(`NYC Open Data API request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting / throttling
        if (response.statusCode === 429) {
          log.warn('NYC Open Data API rate limit hit (429)');
          const shouldRetry = await rateLimiter.handleBlock(429);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 403) {
          log.warn('NYC Open Data API returned 403 — may need app token for high volume');
          const shouldRetry = await rateLimiter.handleBlock(403);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`NYC Open Data API returned ${response.statusCode} for ${location}`);
          // Log response body for debugging
          try {
            const errData = JSON.parse(response.body);
            log.warn(`API error: ${errData.message || JSON.stringify(errData)}`);
          } catch (_) { /* ignore */ }
          break;
        }

        rateLimiter.resetBackoff();

        let records;
        try {
          records = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse NYC Open Data API response: ${err.message}`);
          break;
        }

        // Validate response — SODA returns an array of objects
        if (!Array.isArray(records)) {
          log.error(`Unexpected NYC Open Data response shape for ${location}`);
          break;
        }

        if (records.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            log.info(`No more results for ${location}`);
            break;
          }
          offset += PAGE_SIZE;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const rec of records) {
          // Deduplicate by license number across locations
          const licNbr = (rec.license_nbr || '').trim();
          if (licNbr && seenLicenses.has(licNbr)) continue;
          if (licNbr) seenLicenses.add(licNbr);

          const lead = this._mapRecord(rec);

          // Skip records with no business name at all
          if (!lead.firm_name && !lead.first_name && !lead.last_name) continue;

          if (lead.phone) totalWithPhone++;
          if (lead.first_name && lead.last_name) totalWithName++;

          yield this.transformResult(lead, practiceArea);
          locationYielded++;
        }

        log.info(`Got ${records.length} records for ${location} (${locationYielded} leads from this location so far)`);

        // Fewer than a full page means we're done with this location
        if (records.length < PAGE_SIZE) {
          log.success(`Completed ${location}: ${locationYielded} leads`);
          break;
        }

        offset += PAGE_SIZE;
        pagesFetched++;
      }
    }

    log.success([
      `NYC HIC scrape complete:`,
      `${seenLicenses.size.toLocaleString()} unique licenses`,
      `${totalWithPhone.toLocaleString()} with phone`,
      `${totalWithName.toLocaleString()} with person name`,
    ].join(' | '));
  }
}

module.exports = new NycHicScraper();
