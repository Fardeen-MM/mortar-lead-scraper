/**
 * Washington State L&I (Labor & Industries) Contractor Scraper
 *
 * Source: https://data.wa.gov/Labor/Exposed-Worker-Contractors/m8qx-ubtq
 * Platform: Socrata Open Data API (free, no auth)
 * Method: GET https://data.wa.gov/resource/m8qx-ubtq.json
 *
 * Returns JSON with contractor fields: BusinessName, ContractorLicenseNumber,
 * LicenseType, Address1/2, City, State, Zip, PhoneNumber, LicenseEffective/
 * ExpirationDate, BusinessType, SpecialtyCode/Description, UBI,
 * PrimaryPrincipalName, Status.
 *
 * ~159,942 records, updated 3x daily. Filtered to ACTIVE status.
 * Paginated via $limit/$offset (1000 per page).
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_BASE = 'https://data.wa.gov/resource/m8qx-ubtq.json';
const PAGE_SIZE = 1000;

/**
 * Filter definitions for each practice area.
 * Some trades are a separate license type (EC=Electrical, PC=Plumbing),
 * while others are specialty codes within Construction Contractor (CC).
 *
 * Socrata field names:
 *   contractorlicensetypecode: CC, EC, PC, LC
 *   specialtycode1desc: GENERAL, ROOFING, PAINTING/WALLCOVERING, LANDSCAPING, HVAC/RFRG, etc.
 */
const FILTERS = {
  'general contractor':  { type: 'specialty', value: 'GENERAL' },
  'plumbing':            { type: 'license',   value: 'PC' },
  'electrical':          { type: 'license',   value: 'EC' },
  'hvac':                { type: 'specialty',  value: 'HVAC' },       // matches HVAC/RFRG, HVAC/RFRG-RESTRICTED
  'roofing':             { type: 'specialty',  value: 'ROOFING' },
  'painting':            { type: 'specialty',  value: 'PAINTING/WALLCOVERING' },
  'landscaping':         { type: 'specialty',  value: 'LANDSCAPING' },
};

class WaLniScraper extends BaseScraper {
  constructor() {
    super({
      name: 'wa_lni_contractors',
      stateCode: 'WA-LNI',
      baseUrl: API_BASE,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'general contractor': 'GENERAL',
        'plumbing': 'PC',
        'electrical': 'EC',
        'hvac': 'HVAC',
        'roofing': 'ROOFING',
        'painting': 'PAINTING',
        'landscaping': 'LANDSCAPING',
      },
      defaultCities: [
        'Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue',
        'Kent', 'Everett', 'Renton', 'Olympia', 'Federal Way',
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

  /**
   * Override transformResult to set source without the default '_bar' suffix.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'wa_lni_contractors';
    lead.practice_area = practiceArea || '';
    return lead;
  }

  /**
   * HTTP GET against the Socrata JSON API.
   */
  _apiGet(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);

      const req = https.request({
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
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
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * Resolve a practice area to its FILTERS entry.
   * Returns { type, value } or null.
   */
  _resolveFilter(practiceArea) {
    if (!practiceArea) return null;
    const key = practiceArea.toLowerCase().trim();
    // Direct match
    if (FILTERS[key]) return FILTERS[key];
    // Partial match
    for (const [name, filter] of Object.entries(FILTERS)) {
      if (name.includes(key) || key.includes(name)) return filter;
    }
    return null;
  }

  /**
   * Build the Socrata SoQL query URL.
   * Filters by contractorlicensestatus=ACTIVE, optionally by trade filter and city.
   *
   * Socrata field names (all lowercase):
   *   contractorlicensestatus, contractorlicensetypecode, specialtycode1desc,
   *   city, businessname, contractorlicensenumber, phonenumber, primaryprincipalname
   */
  _buildApiUrl({ filter, city, limit, offset }) {
    const conditions = ["contractorlicensestatus='ACTIVE'"];

    if (filter) {
      if (filter.type === 'license') {
        // Electrical (EC) or Plumbing (PC) — filter by license type
        conditions.push(`contractorlicensetypecode='${filter.value}'`);
      } else if (filter.type === 'specialty') {
        // HVAC uses starts_with to match HVAC/RFRG and HVAC/RFRG-RESTRICTED
        if (filter.value === 'HVAC') {
          conditions.push(`starts_with(upper(specialtycode1desc),'HVAC')`);
        } else {
          conditions.push(`specialtycode1desc='${filter.value}'`);
        }
      }
    }

    if (city) {
      conditions.push(`upper(city)='${city.toUpperCase()}'`);
    }

    const params = new URLSearchParams({
      '$where': conditions.join(' AND '),
      '$limit': String(limit),
      '$offset': String(offset),
      '$order': 'businessname ASC',
    });

    return `${API_BASE}?${params.toString()}`;
  }

  /**
   * Split PrimaryPrincipalName into first/last name.
   * Format is typically "LAST, FIRST" or "FIRST LAST" or just a single name.
   */
  _splitOwnerName(name) {
    if (!name) return { firstName: '', lastName: '' };

    const trimmed = name.trim();

    // Handle "LAST, FIRST MIDDLE" format
    if (trimmed.includes(',')) {
      const parts = trimmed.split(',').map(s => s.trim());
      const lastName = this._titleCase(parts[0]);
      const firstParts = (parts[1] || '').split(/\s+/).filter(Boolean);
      const firstName = this._titleCase(firstParts[0] || '');
      return { firstName, lastName };
    }

    // Handle "FIRST LAST" format
    const parts = trimmed.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      return {
        firstName: this._titleCase(parts[0]),
        lastName: this._titleCase(parts[parts.length - 1]),
      };
    }

    if (parts.length === 1) {
      return { firstName: '', lastName: this._titleCase(parts[0]) };
    }

    return { firstName: '', lastName: '' };
  }

  /**
   * Convert an ALL-CAPS or mixed-case string to Title Case.
   */
  _titleCase(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
  }

  /**
   * Format a phone number string. Strips non-digit chars, returns formatted or raw.
   */
  _formatPhone(phone) {
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
   * Map a Socrata API record to a normalized lead object.
   * Socrata JSON API returns all-lowercase field names.
   */
  _mapRecord(rec, practiceArea) {
    const { firstName, lastName } = this._splitOwnerName(
      rec.primaryprincipalname || ''
    );

    const businessName = (rec.businessname || '').trim();
    const city = (rec.city || '').trim();
    const state = (rec.state || 'WA').trim();
    const zip = (rec.zip || '').trim();
    const phone = this._formatPhone(rec.phonenumber || '');
    const licenseNumber = (rec.contractorlicensenumber || '').trim();
    const status = (rec.contractorlicensestatus || '').trim();

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: businessName,
      city,
      state,
      zip,
      phone,
      website: '',
      email: '',
      bar_number: licenseNumber,
      bar_status: status,
      source: 'wa_lni_contractors',
      practice_area: practiceArea || '',
    };
  }

  /**
   * Async generator that yields contractor leads from the WA L&I Socrata API.
   * Paginates through results with $limit/$offset.
   * Filters by specialty code (practice area) and/or city.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Resolve trade filter from practice area
    const filter = this._resolveFilter(practiceArea);

    if (practiceArea && !filter) {
      log.warn(`Unknown trade "${practiceArea}" for WA-LNI — searching all trades`);
      log.info(`Available trades: ${Object.keys(FILTERS).join(', ')}`);
    }

    // Determine cities to search
    const cities = options.city
      ? [options.city]
      : this.defaultCities;
    const citiesToSearch = options.maxCities
      ? cities.slice(0, options.maxCities)
      : cities;

    const filterDesc = filter ? `${filter.type}=${filter.value}` : 'all';
    log.scrape(`WA L&I contractor search: filter=${filterDesc}, cities=${citiesToSearch.length}`);

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const city = citiesToSearch[ci];
      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
      log.scrape(`Searching WA L&I contractors in ${city}${filter ? ` (${filter.value})` : ''}`);

      let offset = 0;
      let pagesFetched = 0;
      let totalYielded = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${city}`);
          break;
        }

        const url = this._buildApiUrl({
          filter,
          city,
          limit: PAGE_SIZE,
          offset,
        });

        log.info(`Page ${pagesFetched + 1} (offset ${offset}) — ${city}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiGet(url, rateLimiter);
        } catch (err) {
          log.error(`WA L&I API request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from WA L&I API`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`WA L&I API returned ${response.statusCode}`);
          break;
        }

        rateLimiter.resetBackoff();

        let records;
        try {
          records = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse WA L&I API response: ${err.message}`);
          break;
        }

        if (!Array.isArray(records) || records.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.info(`No more records for ${city} (offset ${offset})`);
            break;
          }
          offset += PAGE_SIZE;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const rec of records) {
          const lead = this._mapRecord(rec, practiceArea);
          // Skip records without a business name
          if (!lead.firm_name) continue;
          yield this.transformResult(lead, practiceArea);
          totalYielded++;
        }

        log.info(`Got ${records.length} records for ${city} (${totalYielded} total from this city)`);

        // If we got fewer than PAGE_SIZE, we've reached the end
        if (records.length < PAGE_SIZE) {
          log.success(`Completed ${city}: ${totalYielded} contractors`);
          break;
        }

        offset += PAGE_SIZE;
        pagesFetched++;
      }
    }

    log.success(`WA L&I scrape complete`);
  }
}

module.exports = new WaLniScraper();
