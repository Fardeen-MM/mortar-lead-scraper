/**
 * Texas TDLR (Department of Licensing and Regulation) — Contractor License Scraper
 *
 * Source: https://data.texas.gov/resource/7358-krk7.json (Socrata Open Data / SODA API)
 * Records: 949,000+ license records across 85+ license types
 * Method: JSON REST API with $limit/$offset pagination, no auth required
 *
 * Available fields (9 total):
 *   license_type, license_number, business_name, owner_name,
 *   license_subtype, license_expiration_date_mmddccyy,
 *   continuing_education_flag, business_county, mailing_address_county
 *
 * NOTE: The TDLR dataset does NOT include phone, email, address, city, website, or status.
 * "Active" is inferred from license_expiration_date_mmddccyy being in the future.
 * County is the best geographic filter available.
 *
 * Relevant contractor license types (filtering out apprentices, cosmetology, tow, etc.):
 *   - Master Electrician (19K), Journeyman Electrician (44K), Electrical Contractor (14K)
 *   - A/C Contractor (20K), A/C Technician (57K)
 *   - Appliance Installation Contractor (826), Appliance Installer (2.5K)
 *   - Residential Wireman (2.5K), Maintenance Electrician (691)
 *   - Elevator Contractor (358), Elevator Responsible Party (606)
 *   - Water Well Driller/Pump Installer (1.8K)
 *   - Electrical Sign Contractor (646), Master Sign Electrician (555)
 *   - Journeyman Sign Electrician (322), Journeyman Lineman Electrician (72)
 *   - Journeyman Industrial Electrician (757)
 *
 * Excluded (not contractors / business owners):
 *   - Apprentice Electrician (227K), Apprentice Sign Electrician (5K)
 *   - Cosmetology/Barber types, Property Tax types, Behavior Analyst, Tow operators
 *   - CE Providers, Judges, Referees, Promoters, Breeders
 *
 * Plumbing note: Texas plumbing is under TSBPE (Texas State Board of Plumbing Examiners),
 * separate from TDLR. The TSBPE free licensee list (https://tsbpe.texas.gov/free-licensee-list/)
 * is a downloadable CSV, not a queryable API — not integrated here.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

const API_BASE = 'https://data.texas.gov/resource/7358-krk7.json';
const PAGE_SIZE = 1000;

// License types we want (contractors / business owners — not apprentices or non-trade types)
const CONTRACTOR_LICENSE_TYPES = [
  'Master Electrician',
  'Journeyman Electrician',
  'Electrical Contractor',
  'Residential Wireman',
  'Maintenance Electrician',
  'Journeyman Industrial Electrician',
  'Journeyman Lineman Electrician',
  'A/C Contractor',
  'A/C Technician',
  'Appliance Installation Contractor',
  'Appliance Installer',
  'Elevator Contractor',
  'Elevator Responsible Party',
  'Water Well Driller/Pump Installer',
  'Electrical Sign Contractor',
  'Master Sign Electrician',
  'Journeyman Sign Electrician',
  'Service Technician',
];

// Map practice area keywords to license type filter groups
const PRACTICE_AREA_GROUPS = {
  'electrical': [
    'Master Electrician', 'Journeyman Electrician', 'Electrical Contractor',
    'Residential Wireman', 'Maintenance Electrician', 'Journeyman Industrial Electrician',
    'Journeyman Lineman Electrician',
  ],
  'hvac': [
    'A/C Contractor', 'A/C Technician',
  ],
  'appliance': [
    'Appliance Installation Contractor', 'Appliance Installer',
  ],
  'elevator': [
    'Elevator Contractor', 'Elevator Responsible Party',
  ],
  'sign': [
    'Electrical Sign Contractor', 'Master Sign Electrician', 'Journeyman Sign Electrician',
  ],
  'water well': [
    'Water Well Driller/Pump Installer',
  ],
  'general': null, // null = all contractor types
  'plumbing': [], // Empty — TSBPE is separate, not available via this API
};

// Texas counties mapped to major cities (county is the only geographic filter in the API)
const COUNTY_CITY_MAP = {
  'HARRIS':    'Houston',
  'DALLAS':    'Dallas',
  'BEXAR':     'San Antonio',
  'TRAVIS':    'Austin',
  'TARRANT':   'Fort Worth',
  'EL PASO':   'El Paso',
  'COLLIN':    'Plano',
  'LUBBOCK':   'Lubbock',
  'NUECES':    'Corpus Christi',
  'DENTON':    'Denton',
};

const DEFAULT_COUNTIES = Object.keys(COUNTY_CITY_MAP);

class TdlrScraper extends BaseScraper {
  constructor() {
    super({
      name: 'tdlr-contractors',
      stateCode: 'TDLR',
      baseUrl: API_BASE,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'electrical': 'Electrician',
        'hvac': 'A/C and Refrigeration',
        'general': 'all',
        'plumbing': 'Plumber',
        'appliance': 'Appliance',
        'elevator': 'Elevator',
        'sign': 'Sign',
        'water well': 'Water Well',
      },
      defaultCities: [
        'Houston', 'Dallas', 'San Antonio', 'Austin', 'Fort Worth',
        'El Paso', 'Arlington', 'Plano', 'Lubbock', 'Corpus Christi',
      ],
    });
  }

  // Not used — search() is fully overridden
  buildSearchUrl() {
    throw new Error('tdlr: buildSearchUrl() is not used — search() is overridden');
  }
  parseResultsPage() {
    throw new Error('tdlr: parseResultsPage() is not used — search() is overridden');
  }
  extractResultCount() {
    throw new Error('tdlr: extractResultCount() is not used — search() is overridden');
  }

  /**
   * Resolve a city name to its Texas county for API filtering.
   * Falls back to null if no mapping exists (will search all counties).
   */
  _cityToCounty(city) {
    if (!city) return null;
    const upper = city.toUpperCase().trim();

    // Check if the input IS a county name already
    if (DEFAULT_COUNTIES.includes(upper)) return upper;

    // Reverse lookup: city name → county
    for (const [county, cityName] of Object.entries(COUNTY_CITY_MAP)) {
      if (cityName.toUpperCase() === upper) return county;
    }

    // Arlington is in Tarrant county
    if (upper === 'ARLINGTON') return 'TARRANT';

    return null;
  }

  /**
   * Determine which license types to query based on practice area.
   */
  _getLicenseTypes(practiceArea) {
    if (!practiceArea) return CONTRACTOR_LICENSE_TYPES;

    const key = practiceArea.toLowerCase().trim();

    // Direct match in groups
    if (PRACTICE_AREA_GROUPS[key] !== undefined) {
      const group = PRACTICE_AREA_GROUPS[key];
      if (group === null) return CONTRACTOR_LICENSE_TYPES; // 'general'
      if (group.length === 0) return []; // 'plumbing' — not available
      return group;
    }

    // Partial match
    for (const [name, types] of Object.entries(PRACTICE_AREA_GROUPS)) {
      if (name.includes(key) || key.includes(name)) {
        if (types === null) return CONTRACTOR_LICENSE_TYPES;
        return types;
      }
    }

    // Default to all contractor types
    return CONTRACTOR_LICENSE_TYPES;
  }

  /**
   * Build the Socrata SODA API URL with $where, $limit, $offset, $order.
   */
  _buildApiUrl(licenseTypes, county, offset) {
    const conditions = [];

    // License type filter
    if (licenseTypes.length === 1) {
      conditions.push(`license_type='${licenseTypes[0]}'`);
    } else if (licenseTypes.length > 1) {
      const inList = licenseTypes.map(t => `'${t}'`).join(',');
      conditions.push(`license_type in(${inList})`);
    }

    // Active filter: expiration date in the future
    // The field is MM/DD/CCYY string — compare lexicographically after building today's date
    const now = new Date();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const yyyy = now.getFullYear();
    conditions.push(`license_expiration_date_mmddccyy>'${mm}/${dd}/${yyyy}'`);

    // County filter
    if (county) {
      conditions.push(`business_county='${county}'`);
    }

    const where = encodeURIComponent(conditions.join(' AND '));
    return `${API_BASE}?$where=${where}&$limit=${PAGE_SIZE}&$offset=${offset}&$order=license_number`;
  }

  /**
   * Fetch JSON from the Socrata API using Node.js https module.
   */
  _fetchJson(url) {
    return new Promise((resolve, reject) => {
      const req = https.get(url, {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'MortarLeadScraper/1.0',
        },
        timeout: 30000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return resolve(this._fetchJson(res.headers.location));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            return reject(new Error(`TDLR API returned ${res.statusCode}: ${data.slice(0, 200)}`));
          }
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(new Error(`TDLR API returned invalid JSON: ${err.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('TDLR API request timed out')); });
    });
  }

  /**
   * Split "LAST, FIRST MIDDLE" or "LAST, FIRST M" into first/last name.
   * TDLR owner_name is always "LASTNAME, FIRSTNAME [MIDDLE]" format.
   */
  _parseOwnerName(ownerName) {
    if (!ownerName) return { firstName: '', lastName: '' };

    const cleaned = ownerName.trim();

    // "LAST, FIRST MIDDLE" format
    if (cleaned.includes(',')) {
      const [lastPart, ...restParts] = cleaned.split(',');
      const lastName = this._titleCase(lastPart.trim());
      const firstParts = restParts.join(',').trim().split(/\s+/);
      const firstName = this._titleCase(firstParts[0] || '');
      return { firstName, lastName };
    }

    // Fallback: space-separated
    const parts = cleaned.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      return {
        firstName: this._titleCase(parts[0]),
        lastName: this._titleCase(parts[parts.length - 1]),
      };
    }

    return { firstName: '', lastName: this._titleCase(cleaned) };
  }

  /**
   * Convert "JOHN DOE" or "mcdonald" to "John Doe" / "Mcdonald".
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/(?:^|\s)\S/g, c => c.toUpperCase());
  }

  /**
   * Clean up business name: title case, strip trailing owner name duplicates.
   */
  _cleanBusinessName(businessName, ownerName) {
    if (!businessName) return '';
    let name = businessName.trim();

    // If business name is identical to owner name, it's a sole proprietor — return as-is
    if (name.toUpperCase() === (ownerName || '').toUpperCase()) {
      return ''; // No separate firm name for sole proprietors
    }

    // Title case the business name
    return this._titleCase(name);
  }

  /**
   * Determine the practice_area label from the license_type.
   */
  _classifyPracticeArea(licenseType) {
    if (!licenseType) return '';
    const lt = licenseType.toLowerCase();
    if (lt.includes('electrician') || lt.includes('electrical') || lt.includes('wireman')) return 'electrical';
    if (lt.includes('a/c') || lt.includes('refrigeration')) return 'hvac';
    if (lt.includes('appliance')) return 'appliance';
    if (lt.includes('elevator')) return 'elevator';
    if (lt.includes('sign')) return 'sign_electrical';
    if (lt.includes('water well')) return 'water_well';
    if (lt.includes('service technician')) return 'service';
    return 'general';
  }

  /**
   * Check if a license is expired based on MM/DD/CCYY date string.
   * Returns true if the date can be parsed and is in the past.
   */
  _isExpired(dateStr) {
    if (!dateStr) return true;
    const match = dateStr.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!match) return true;
    const expDate = new Date(parseInt(match[3]), parseInt(match[1]) - 1, parseInt(match[2]));
    return expDate < new Date();
  }

  /**
   * Main search — queries Socrata SODA API, paginates, yields contractor leads.
   *
   * Geographic filtering: Uses business_county since the API has no city/address/zip fields.
   * When options.city is set, it's mapped to a Texas county via _cityToCounty().
   * When no city is given, iterates over the 10 largest county markets.
   */
  async *search(practiceArea, options = {}) {
    const licenseTypes = this._getLicenseTypes(practiceArea);

    if (licenseTypes.length === 0) {
      log.warn('[TDLR] No license types matched. Texas plumbing is under TSBPE (separate from TDLR).');
      yield { _captcha: true, city: 'all', reason: 'Plumbing licenses are under TSBPE, not TDLR' };
      return;
    }

    log.info(`[TDLR] Searching ${licenseTypes.length} license types: ${licenseTypes.slice(0, 5).join(', ')}${licenseTypes.length > 5 ? '...' : ''}`);

    // Determine counties to search
    let counties;
    if (options.city) {
      const county = this._cityToCounty(options.city);
      if (county) {
        counties = [county];
      } else {
        // City not mapped — search without county filter (statewide for that city name)
        log.warn(`[TDLR] City "${options.city}" not mapped to a county — searching statewide`);
        counties = [null]; // null = no county filter
      }
    } else {
      counties = options.maxCities
        ? DEFAULT_COUNTIES.slice(0, options.maxCities)
        : DEFAULT_COUNTIES;
    }

    for (let ci = 0; ci < counties.length; ci++) {
      const county = counties[ci];
      const cityLabel = county ? (COUNTY_CITY_MAP[county] || county) : 'Statewide';

      yield { _cityProgress: { current: ci + 1, total: counties.length } };
      log.scrape(`[TDLR] Searching: ${practiceArea || 'all'} contractors in ${cityLabel}, TX (county: ${county || 'ALL'})`);

      let offset = 0;
      let pagesFetched = 0;
      let totalYielded = 0;

      while (true) {
        // Respect maxPages for test mode
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`[TDLR] Reached max pages limit (${options.maxPages}) for ${cityLabel}`);
          break;
        }

        const url = this._buildApiUrl(licenseTypes, county, offset);
        log.info(`[TDLR] Page ${pagesFetched + 1} — offset=${offset}, county=${county || 'ALL'}`);

        let records;
        try {
          records = await this._fetchJson(url);
        } catch (err) {
          log.error(`[TDLR] API request failed: ${err.message}`);
          break;
        }

        if (!Array.isArray(records) || records.length === 0) {
          if (pagesFetched === 0) {
            log.info(`[TDLR] No results for ${practiceArea || 'all'} in ${cityLabel}`);
          } else {
            log.success(`[TDLR] Completed ${pagesFetched} pages (${totalYielded} leads) for ${cityLabel}`);
          }
          break;
        }

        if (pagesFetched === 0) {
          log.success(`[TDLR] First page returned ${records.length} records for ${cityLabel}`);
        }

        for (const rec of records) {
          // Double-check expiration (API $where should handle this, but be safe)
          if (this._isExpired(rec.license_expiration_date_mmddccyy)) continue;

          const { firstName, lastName } = this._parseOwnerName(rec.owner_name);
          const firmName = this._cleanBusinessName(rec.business_name, rec.owner_name);
          const practiceAreaLabel = this._classifyPracticeArea(rec.license_type);

          // Determine city from county mapping (API has no city field)
          const city = COUNTY_CITY_MAP[rec.business_county] || '';

          const lead = {
            first_name: firstName,
            last_name: lastName,
            firm_name: firmName,
            city: city,
            state: 'TX',
            county: rec.business_county || '',
            phone: '',        // Not available in TDLR dataset
            email: '',        // Not available in TDLR dataset
            website: '',      // Not available in TDLR dataset
            bar_number: rec.license_number || '',
            bar_status: this._isExpired(rec.license_expiration_date_mmddccyy) ? 'Expired' : 'Active',
            license_type: rec.license_type || '',
            license_subtype: rec.license_subtype || '',
            license_expiration: rec.license_expiration_date_mmddccyy || '',
            practice_area: practiceAreaLabel,
            source: 'tdlr_contractors',
          };

          yield this.transformResult(lead, practiceArea || practiceAreaLabel);
          totalYielded++;
        }

        log.info(`[TDLR] Page ${pagesFetched + 1}: ${records.length} records → ${totalYielded} total leads for ${cityLabel}`);

        // If we got fewer than PAGE_SIZE, we've reached the end
        if (records.length < PAGE_SIZE) {
          log.success(`[TDLR] Completed all data (${totalYielded} leads) for ${cityLabel}`);
          break;
        }

        offset += PAGE_SIZE;
        pagesFetched++;
      }
    }
  }
}

module.exports = new TdlrScraper();
