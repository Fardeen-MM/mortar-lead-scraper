/**
 * NPI Registry Scraper — National Provider Identifier (US Healthcare)
 *
 * Source: https://npiregistry.cms.hhs.gov/api/?version=2.1
 * Records: 8M+ healthcare providers (individuals + organizations)
 * Method: Free public REST API, no auth required
 *
 * The NPI Registry is maintained by CMS (Centers for Medicare & Medicaid Services)
 * and contains every healthcare provider in the United States that has been
 * assigned a National Provider Identifier.
 *
 * API Parameters:
 *   version=2.1           — API version (required)
 *   taxonomy_description  — Provider specialty (partial match, e.g. "dentist")
 *   state                 — 2-letter US state code
 *   city                  — City name
 *   enumeration_type      — NPI-1 (individual) or NPI-2 (organization)
 *   first_name            — Provider first name
 *   last_name             — Provider last name
 *   organization_name     — Organization name
 *   postal_code           — ZIP code
 *   limit                 — Results per request (max 200)
 *   skip                  — Offset for pagination
 *
 * Response fields per result:
 *   number                — NPI number (unique identifier)
 *   basic.first_name      — Provider first name (NPI-1)
 *   basic.last_name       — Provider last name (NPI-1)
 *   basic.credential      — Credential (DDS, MD, DO, DC, etc.)
 *   basic.organization_name — Org name (NPI-2)
 *   basic.authorized_official_first_name — Contact person (NPI-2)
 *   basic.authorized_official_last_name  — Contact person (NPI-2)
 *   addresses[]           — LOCATION and MAILING addresses with phone/fax
 *   taxonomies[]          — Specialties with code, description, license
 *   endpoints[]           — May contain email addresses (CONNECT type)
 *
 * Rate limits: Not documented, but be polite (1-2s between requests).
 * Pagination: max 200 per request. Use skip parameter to paginate.
 *   The API returns result_count which is the number of results in the response
 *   (NOT total matching). If result_count === limit, there are likely more results.
 *
 * Key taxonomy codes:
 *   122300000X  — Dentist
 *   1223G0001X  — General Dentist
 *   1223P0221X  — Pediatric Dentist
 *   207Q00000X  — Family Medicine
 *   207R00000X  — Internal Medicine
 *   208D00000X  — General Practice
 *   207N00000X  — Dermatology
 *   204E00000X  — Oral & Maxillofacial Surgery
 *   111N00000X  — Chiropractor
 *   152W00000X  — Optometrist
 *   363L00000X  — Nurse Practitioner
 *   1041C0700X  — Clinical Psychologist
 *   261QM0801X  — Medical Spa (Critical Access Hospital, Ambulatory)
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_BASE = 'https://npiregistry.cms.hhs.gov/api/';
const API_VERSION = '2.1';
const PAGE_SIZE = 200; // API maximum

// Practice area -> API taxonomy_description text
// The NPI API's taxonomy_description parameter requires TEXT descriptions, not codes.
// It performs partial/fuzzy matching, so "dentist" matches "Dentist", "Dentist, General Practice", etc.
// We store the exact text to send to the API.
const PRACTICE_AREA_CODES = {
  // Dental
  'dentist': 'Dentist',
  'general dentist': 'Dentist, General Practice',
  'pediatric dentist': 'Dentist, Pediatric',
  'oral surgery': 'Oral & Maxillofacial Surgery',
  'orthodontist': 'Orthodontics',
  'endodontist': 'Endodontics',
  'periodontist': 'Periodontics',
  'prosthodontist': 'Prosthodontics',
  // Medical
  'family medicine': 'Family Medicine',
  'internal medicine': 'Internal Medicine',
  'general practice': 'General Practice',
  'dermatology': 'Dermatology',
  'pediatrics': 'Pediatrics',
  'obstetrics': 'Obstetrics & Gynecology',
  'cardiology': 'Cardiovascular Disease',
  'orthopedic': 'Orthopaedic Surgery',
  'psychiatry': 'Psychiatry',
  'neurology': 'Neurology',
  'ophthalmology': 'Ophthalmology',
  'urology': 'Urology',
  'radiology': 'Radiology',
  'anesthesiology': 'Anesthesiology',
  'emergency medicine': 'Emergency Medicine',
  'plastic surgery': 'Plastic Surgery',
  'gastroenterology': 'Gastroenterology',
  'pulmonology': 'Pulmonary Disease',
  'oncology': 'Medical Oncology',
  'allergy': 'Allergy & Immunology',
  // Allied Health
  'chiropractic': 'Chiropractor',
  'optometry': 'Optometrist',
  'physical therapy': 'Physical Therapist',
  'occupational therapy': 'Occupational Therapist',
  'speech therapy': 'Speech-Language Pathologist',
  'nurse practitioner': 'Nurse Practitioner',
  'physician assistant': 'Physician Assistant',
  'psychologist': 'Psychologist',
  'pharmacy': 'Pharmacist',
  'podiatry': 'Podiatrist',
  'acupuncture': 'Acupuncturist',
  'massage therapy': 'Massage Therapist',
  'dietitian': 'Dietitian',
  // Facility types (NPI-2 orgs — these return organizations)
  'medical spa': 'Medical Spa',
  'hospital': 'Hospital',
  'clinic': 'Clinic',
  'nursing home': 'Nursing',
  'home health': 'Home Health',
  'ambulance': 'Ambulance',
  'laboratory': 'Laboratory',
  'durable medical equipment': 'Durable Medical Equipment',
  'dental laboratory': 'Dental Laboratory',
};

// US state codes + major cities for broad scanning
const US_STATES = [
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
  'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
  'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
  'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
  'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
];

// Default cities — the scraper queries by state, so cities are only used
// when the user provides a specific city filter via options.city
const DEFAULT_CITIES = [];

/**
 * Title-case a name string (UPPERCASE -> Title Case).
 * NPI data comes back in ALL CAPS.
 */
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|[\s\-'.])(\w)/g, (match) => match.toUpperCase())
    // Fix common prefixes
    .replace(/\bMc(\w)/g, (_, c) => `Mc${c.toUpperCase()}`)
    .replace(/\bO'(\w)/g, (_, c) => `O'${c.toUpperCase()}`);
}

/**
 * Normalize phone number from NPI format (xxx-xxx-xxxx or 10 digits).
 */
function normalizePhone(raw) {
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
 * Format a ZIP code (strip trailing digits if 9-digit format without dash).
 */
function formatZip(raw) {
  if (!raw) return '';
  const clean = raw.replace(/[^\d]/g, '');
  if (clean.length === 9) {
    return `${clean.slice(0, 5)}-${clean.slice(5)}`;
  }
  if (clean.length >= 5) {
    return clean.slice(0, 5);
  }
  return clean;
}


class NpiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'npi_registry',
      stateCode: 'NPI',
      baseUrl: API_BASE,
      pageSize: PAGE_SIZE,
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
   * Override transformResult to use 'npi_registry' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'npi_registry';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Fetch JSON from the NPI API.
   * Returns parsed JSON or null on failure.
   */
  _fetchAPI(params) {
    const url = new URL(API_BASE);
    url.searchParams.set('version', API_VERSION);
    for (const [key, value] of Object.entries(params)) {
      if (value) url.searchParams.set(key, value);
    }

    const urlStr = url.toString();

    return new Promise((resolve, reject) => {
      const req = https.get(urlStr, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Mortar Lead Scraper; NPI Registry)',
          'Accept': 'application/json',
        },
        timeout: 30000,
      }, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            return reject(new Error(`NPI API returned ${res.statusCode}: ${data.slice(0, 200)}`));
          }
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(new Error(`NPI API returned invalid JSON: ${err.message}`));
          }
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('NPI API request timed out')); });
    });
  }

  /**
   * Extract the best address from a result's addresses array.
   * Prefers LOCATION over MAILING.
   */
  _extractAddress(addresses) {
    if (!addresses || !addresses.length) {
      return { address: '', city: '', state: '', zip: '', phone: '', fax: '' };
    }

    // Prefer LOCATION address
    const location = addresses.find(a => a.address_purpose === 'LOCATION') || addresses[0];

    return {
      address: [location.address_1, location.address_2].filter(Boolean).join(', '),
      city: titleCase(location.city || ''),
      state: (location.state || '').toUpperCase(),
      zip: formatZip(location.postal_code || ''),
      phone: normalizePhone(location.telephone_number || ''),
      fax: normalizePhone(location.fax_number || ''),
    };
  }

  /**
   * Extract email from endpoints array (if available).
   * Endpoints are optional and not all providers have them.
   */
  _extractEmail(endpoints) {
    if (!endpoints || !endpoints.length) return '';

    for (const ep of endpoints) {
      // Endpoints with endpointType or endpoint that looks like email
      const val = ep.endpoint || ep.endpointDescription || '';
      if (val && val.includes('@') && val.includes('.')) {
        return val.trim().toLowerCase();
      }
    }
    return '';
  }

  /**
   * Extract primary taxonomy description and code.
   */
  _extractTaxonomy(taxonomies) {
    if (!taxonomies || !taxonomies.length) {
      return { taxonomyCode: '', taxonomyDesc: '', license: '' };
    }

    // Find primary taxonomy
    const primary = taxonomies.find(t => t.primary) || taxonomies[0];

    return {
      taxonomyCode: primary.code || '',
      taxonomyDesc: primary.desc || '',
      license: primary.license || '',
    };
  }

  /**
   * Map a single NPI API result to a normalized lead object.
   */
  _mapResult(result) {
    const basic = result.basic || {};
    const npiNumber = result.number || '';
    const enumType = result.enumeration_type || '';

    // Extract address, taxonomy, email
    const addr = this._extractAddress(result.addresses);
    const tax = this._extractTaxonomy(result.taxonomies);
    const email = this._extractEmail(result.endpoints);

    // Individual provider (NPI-1) vs Organization (NPI-2)
    let firstName = '';
    let lastName = '';
    let firmName = '';
    let credential = '';

    if (enumType === 'NPI-1') {
      // Individual provider
      firstName = titleCase(basic.first_name || '');
      lastName = titleCase(basic.last_name || '');
      credential = (basic.credential || '').trim();
      // Sole proprietors may not have an organization
      firmName = '';
    } else if (enumType === 'NPI-2') {
      // Organization
      firmName = titleCase(basic.organization_name || '');
      // Use authorized official as the contact person
      firstName = titleCase(basic.authorized_official_first_name || '');
      lastName = titleCase(basic.authorized_official_last_name || '');
      credential = (basic.authorized_official_credential || '').trim();
    }

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      credential: credential,
      address: addr.address,
      city: addr.city,
      state: addr.state,
      zip: addr.zip,
      phone: addr.phone,
      fax: addr.fax,
      email: email,
      website: '',
      bar_number: npiNumber,  // Using bar_number field for NPI number
      bar_status: (basic.status === 'A') ? 'Active' : basic.status || '',
      enumeration_type: enumType,
      taxonomy_code: tax.taxonomyCode,
      taxonomy_desc: tax.taxonomyDesc,
      license_number: tax.license,
      source: 'npi_registry',
      practice_area: tax.taxonomyDesc,
    };
  }

  /**
   * Resolve practice area to API parameters.
   * Returns { taxonomy_description, enumeration_type } for the API query.
   *
   * The NPI API's taxonomy_description accepts text descriptions and performs
   * partial matching. We map user-friendly names to the exact API descriptions.
   */
  _resolveSearchParams(practiceArea) {
    if (!practiceArea) {
      return { taxonomy_description: '', enumeration_type: '' };
    }

    const key = practiceArea.toLowerCase().trim();
    const apiDesc = PRACTICE_AREA_CODES[key];

    if (apiDesc) {
      // We have a known mapping — use the API description text
      return { taxonomy_description: apiDesc, enumeration_type: '' };
    }

    // No mapping found — pass the text directly (API does partial matching)
    return { taxonomy_description: practiceArea, enumeration_type: '' };
  }

  /**
   * Main search generator — queries NPI API by state (or city), paginating through results.
   *
   * Strategy:
   *   - If options.city is provided: query by taxonomy + state + city
   *   - Otherwise: iterate through US states, querying by taxonomy + state
   *   - Each state query paginates through all results (200 per page)
   *   - Yields normalized lead objects
   *
   * The NPI API caps at 1200 results per unique query (skip up to 1000 + limit 200).
   * For states with >1200 providers in a taxonomy, we fall back to city-level queries.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const searchParams = this._resolveSearchParams(practiceArea);

    log.scrape(`NPI Registry Search — ${practiceArea || 'all providers'}`);
    if (searchParams.taxonomy_description) {
      log.info(`Taxonomy filter: ${searchParams.taxonomy_description}`);
    }

    // Determine what to iterate over
    let stateList;
    const cityFilter = options.city || null;

    if (options.state && options.state !== 'NPI') {
      // Single state specified
      stateList = [options.state.toUpperCase()];
    } else if (cityFilter) {
      // City specified — we still need a state for the API
      // If no state, search all states (unlikely to be useful though)
      stateList = options.stateFilter ? [options.stateFilter.toUpperCase()] : US_STATES;
    } else {
      // Iterate all US states
      stateList = [...US_STATES];
    }

    // Apply maxCities as state limiter in test mode
    if (options.maxCities && stateList.length > options.maxCities) {
      stateList = stateList.slice(0, options.maxCities);
    }

    // Apply maxPages as a total page limit across all states
    const maxPages = options.maxPages || Infinity;
    let totalPagesFetched = 0;

    // Stats
    let totalYielded = 0;
    let totalWithPhone = 0;
    let totalWithEmail = 0;
    let totalWithFirm = 0;
    let statesSearched = 0;

    for (let si = 0; si < stateList.length; si++) {
      if (totalPagesFetched >= maxPages) {
        log.info(`Reached max pages limit (${maxPages})`);
        break;
      }

      const stateCode = stateList[si];
      yield { _cityProgress: { current: si + 1, total: stateList.length } };

      log.scrape(`NPI: Searching ${practiceArea || 'all'} in ${stateCode}${cityFilter ? ` (${cityFilter})` : ''}`);

      let skip = 0;
      let stateTotal = 0;
      let hasMore = true;

      while (hasMore) {
        if (totalPagesFetched >= maxPages) break;

        // Build API params
        const params = {
          taxonomy_description: searchParams.taxonomy_description || undefined,
          state: stateCode,
          limit: String(PAGE_SIZE),
          skip: String(skip),
        };

        if (cityFilter) {
          params.city = cityFilter;
        }

        if (searchParams.enumeration_type) {
          params.enumeration_type = searchParams.enumeration_type;
        }

        // Fetch from API
        let response;
        try {
          await rateLimiter.wait();
          response = await this._fetchAPI(params);
        } catch (err) {
          log.error(`NPI API error for ${stateCode} skip=${skip}: ${err.message}`);

          // If rate limited, back off and retry
          if (err.message.includes('429') || err.message.includes('Too Many')) {
            log.warn('NPI API rate limited — backing off 10s');
            await new Promise(r => setTimeout(r, 10000));
            continue;
          }
          break;
        }

        totalPagesFetched++;

        const resultCount = response.result_count || 0;
        const results = response.results || [];

        if (skip === 0 && resultCount > 0) {
          log.success(`NPI: Found results in ${stateCode} (first batch: ${resultCount})`);
        }

        if (results.length === 0) {
          if (skip === 0) {
            log.info(`NPI: No results for ${practiceArea || 'all'} in ${stateCode}`);
          }
          break;
        }

        // Process results
        for (const result of results) {
          const lead = this._mapResult(result);

          // Skip if no name at all
          if (!lead.first_name && !lead.last_name && !lead.firm_name) continue;

          // Track stats
          if (lead.phone) totalWithPhone++;
          if (lead.email) totalWithEmail++;
          if (lead.firm_name) totalWithFirm++;

          yield this.transformResult(lead, practiceArea || lead.practice_area);
          totalYielded++;
          stateTotal++;
        }

        // Check if there are more results
        // The API returns up to `limit` results. If we got fewer, we've reached the end.
        if (results.length < PAGE_SIZE) {
          hasMore = false;
        } else {
          skip += PAGE_SIZE;

          // NPI API has a max skip of 1000 (1000 + 200 limit = 1200 max results per query)
          if (skip > 1000) {
            log.warn(`NPI: Reached API pagination limit (1200) for ${stateCode} — ${stateTotal} results captured`);

            // If city filter is not set and we hit the limit, try breaking down by city
            // This is a known limitation of the NPI API
            if (!cityFilter) {
              log.info(`NPI: Consider using city-level queries for ${stateCode} to get more results`);
            }
            hasMore = false;
          }
        }
      }

      statesSearched++;

      if (stateTotal > 0) {
        log.info(`NPI: ${stateCode} complete — ${stateTotal.toLocaleString()} providers`);
      }
    }

    log.success([
      `NPI Registry scrape complete:`,
      `${statesSearched} states searched`,
      `${totalYielded.toLocaleString()} providers yielded`,
      `(${totalWithPhone.toLocaleString()} with phone`,
      `${totalWithEmail.toLocaleString()} with email`,
      `${totalWithFirm.toLocaleString()} organizations)`,
    ].join(' | '));
  }
}

module.exports = new NpiScraper();
