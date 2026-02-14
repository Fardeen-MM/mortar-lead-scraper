/**
 * New South Wales (AU-NSW) Law Society -- Register of Solicitors Scraper
 *
 * Source: https://www.lawsociety.com.au/register-of-solicitors
 * Method: JSON REST API (POST search + GET detail)
 *
 * The Law Society of NSW Register of Solicitors is embedded as an iframe at
 * https://ros.lawsociety.com.au/ which uses a JSON API at:
 *
 *   Search:  POST https://ros-link.lawsociety.com.au/api/lawyer/
 *            Body: { suburb, region, accreditedSpecialist, lastName, otherName,
 *                    lastNameSearchOption, page, pageSize }
 *            Returns: { resultCount, results: [...] }
 *
 *   Detail:  GET  https://ros-link.lawsociety.com.au/api/lawyer/{id}
 *            Returns full solicitor record with address, phone, email,
 *            admission date, certificate type, specialist accreditations, etc.
 *
 *   Regions: GET  https://ros-link.lawsociety.com.au/api/region
 *   Specialisations: GET https://ros-link.lawsociety.com.au/api/accreditedSpecialist
 *
 * The search API accepts suburb names (e.g., "Sydney", "Parramatta") or
 * region IDs (e.g., "CITY OF SYDNEY", "PARRAMATTA DISTRICT").
 * Using suburb gives broader matching. Page size max is 25.
 *
 * The search endpoint returns basic listing data (name, practice, suburb).
 * The detail endpoint (GET /api/lawyer/{id}) returns the full record including
 * phone, email, address, admission date, certificate type, languages, and
 * specialist accreditations.
 *
 * ~38,000 solicitors are registered in NSW.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class NswScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-nsw',
      stateCode: 'AU-NSW',
      baseUrl: 'https://ros-link.lawsociety.com.au/api',
      pageSize: 25,
      practiceAreaCodes: {
        'advocacy':                'Advocacy',
        'business':                'Business Law',
        'business law':            'Business Law',
        'children':                "Children's Law",
        "children's law":          "Children's Law",
        'commercial litigation':   'Commercial Litigation',
        'criminal':                'Criminal Law',
        'criminal law':            'Criminal Law',
        'dispute resolution':      'Dispute Resolution',
        'elder':                   'Elder Law',
        'elder law':               'Elder Law',
        'employment':              'Employment & Industrial Law',
        'industrial law':          'Employment & Industrial Law',
        'family':                  'Family Law',
        'family law':              'Family Law',
        'government':              'Government & Administrative Law/Public Law',
        'administrative law':      'Government & Administrative Law/Public Law',
        'public law':              'Government & Administrative Law/Public Law',
        'immigration':             'Immigration Law',
        'immigration law':         'Immigration Law',
        'planning':                'Local Government & Planning/Planning & Environment',
        'local government':        'Local Government & Planning/Planning & Environment',
        'environment':             'Local Government & Planning/Planning & Environment',
        'mediation':               'Mediation',
        'personal injury':         'Personal Injury',
        'property':                'Property Law',
        'property law':            'Property Law',
        'conveyancing':            'Property Law',
        'taxation':                'Taxation Law',
        'tax':                     'Taxation Law',
        'tax law':                 'Taxation Law',
        'wills':                   'Wills & Estates Law',
        'estates':                 'Wills & Estates Law',
        'wills and estates':       'Wills & Estates Law',
        'wills & estates':         'Wills & Estates Law',
      },
      defaultCities: ['Sydney', 'Parramatta', 'Newcastle', 'Wollongong', 'Central Coast'],
      maxConsecutiveEmpty: 2,
    });

    this.searchUrl = `${this.baseUrl}/lawyer/`;
    this.detailUrl = `${this.baseUrl}/lawyer/`;

    // Map city names to regions for cases where suburb search is too broad
    // or too narrow. Cities like "Central Coast" map better as regions.
    this.cityToRegion = {
      'Central Coast': 'CENTRAL COAST',
    };
  }

  // --- HTTP helpers for the JSON API ---

  /**
   * POST JSON to the NSW Law Society API.
   * Returns parsed JSON response.
   */
  httpPostJson(url, body, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = JSON.stringify(body);
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postBody),
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': 'https://ros.lawsociety.com.au',
          'Referer': 'https://ros.lawsociety.com.au/?tab=lawyer',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * GET JSON from the NSW Law Society API.
   * Returns parsed JSON response.
   */
  httpGetJson(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': 'https://ros.lawsociety.com.au',
          'Referer': 'https://ros.lawsociety.com.au/?tab=lawyer',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      const req = https.get(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  // --- BaseScraper overrides (required but not used since search() is overridden) ---

  buildSearchUrl({ city, practiceCode, page }) {
    // Not used directly -- search is done via JSON POST in search() override.
    // Provided for BaseScraper interface compliance.
    const params = new URLSearchParams();
    params.set('suburb', city || '');
    params.set('page', String(page || 1));
    params.set('pageSize', String(this.pageSize));
    if (practiceCode) params.set('accreditedSpecialist', practiceCode);
    return `${this.searchUrl}?${params.toString()}`;
  }

  parseResultsPage() {
    // Not used -- results come as JSON, parsed directly in search().
    return [];
  }

  extractResultCount() {
    // Not used -- result count comes from JSON response field "resultCount".
    return 0;
  }

  // --- Name parsing helpers ---

  /**
   * Parse a full name from the API format into first/last components.
   *
   * The API returns names in "Last, First Middle (Preferred)" format in the
   * search results, and separate firstName/lastName fields in detail responses.
   *
   * @param {string} fullName - e.g. "Smith, John David (Jack)"
   * @returns {{ firstName: string, lastName: string, fullName: string }}
   */
  _parseApiName(fullName) {
    if (!fullName) return { firstName: '', lastName: '', fullName: '' };

    const cleaned = fullName.trim().replace(/\s+/g, ' ');

    // Remove trailing preferred name in parentheses for the clean full name
    const withoutPref = cleaned.replace(/\s*\([^)]*\)\s*$/, '').trim();

    // API returns "Last, First Middle" format
    const commaIdx = withoutPref.indexOf(',');
    if (commaIdx > 0) {
      const lastName = withoutPref.substring(0, commaIdx).trim();
      const rest = withoutPref.substring(commaIdx + 1).trim();
      // Take first word as first name
      const firstParts = rest.split(/\s+/);
      const firstName = firstParts[0] || '';
      return {
        firstName,
        lastName,
        fullName: `${firstName} ${lastName}`.trim(),
      };
    }

    // Fallback: use BaseScraper splitName
    const parts = this.splitName(withoutPref);
    return {
      firstName: parts.firstName,
      lastName: parts.lastName,
      fullName: withoutPref,
    };
  }

  /**
   * Convert an UPPERCASE string to Title Case.
   * e.g. "NORTH SYDNEY" => "North Sydney"
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields solicitor records from the NSW Register.
   *
   * Strategy:
   * 1. For each city/suburb, POST to the search API to get a list of solicitors.
   * 2. Paginate through all results (the API caps at page * pageSize).
   * 3. For each solicitor in the results, GET their detail record for full data.
   * 4. Yield the enriched record.
   *
   * The detail fetch is necessary because the search results only contain
   * name, practice name, and suburb. The detail endpoint provides phone,
   * email, address, admission date, certificate type, etc.
   *
   * To avoid excessive API calls, we batch detail fetches with small delays.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for AU-NSW -- searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    const seenIds = new Set();

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} solicitors in ${city}, AU-NSW`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build search payload
        const searchBody = {
          page: page,
          pageSize: this.pageSize,
        };

        // Use region mapping if available, otherwise use suburb
        if (this.cityToRegion[city]) {
          searchBody.region = this.cityToRegion[city];
        } else {
          searchBody.suburb = city;
        }

        // Add specialist accreditation filter if a practice area was resolved
        if (practiceCode) {
          searchBody.accreditedSpecialist = practiceCode;
        }

        log.info(`Page ${page} -- POST ${this.searchUrl} [${this.cityToRegion[city] ? 'region' : 'suburb'}=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPostJson(this.searchUrl, searchBody, rateLimiter);
        } catch (err) {
          log.error(`Search request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from NSW Law Society API`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} from search API -- skipping ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        // Parse JSON response
        let searchData;
        try {
          searchData = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse search JSON: ${err.message}`);
          break;
        }

        // Get total count on first page
        if (page === 1) {
          totalResults = searchData.resultCount || 0;
          if (totalResults === 0 || !searchData.results || searchData.results.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

          if (totalResults >= 500) {
            log.warn(`NSW API client caps at 500 results -- we will attempt to paginate all ${totalResults}`);
          }
        }

        const results = searchData.results || [];

        if (results.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages -- stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Process each solicitor: fetch detail for full record
        for (const result of results) {
          const solId = result.id;

          // Skip duplicates across cities
          if (seenIds.has(solId)) continue;
          seenIds.add(solId);

          // Fetch full detail record for this solicitor
          let detail = null;
          try {
            await sleep(1000 + Math.random() * 2000); // 1-3s delay between detail fetches
            const detailResp = await this.httpGetJson(`${this.detailUrl}${solId}`, rateLimiter);

            if (detailResp.statusCode === 200) {
              detail = JSON.parse(detailResp.body);
            } else if (detailResp.statusCode === 429 || detailResp.statusCode === 403) {
              log.warn(`Rate limited on detail fetch for ID ${solId} -- using search data only`);
              await rateLimiter.handleBlock(detailResp.statusCode);
            }
          } catch (err) {
            log.warn(`Detail fetch failed for ID ${solId}: ${err.message} -- using search data only`);
          }

          const attorney = this._buildAttorneyRecord(result, detail);

          // Apply admission year filter if specified
          if (options.minYear && attorney.admission_date) {
            const yearMatch = attorney.admission_date.match(/\d{4}/);
            if (yearMatch) {
              const year = parseInt(yearMatch[0], 10);
              if (year > 0 && year < options.minYear) continue;
            }
          }

          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }

  /**
   * Build a normalized attorney record from the search result and detail data.
   *
   * @param {object} searchResult - Basic record from the search listing
   * @param {object|null} detail - Full record from the detail endpoint (may be null)
   * @returns {object} Normalized attorney record
   */
  _buildAttorneyRecord(searchResult, detail) {
    // Parse name from search result (format: "Last, First Middle (Preferred)")
    const parsed = this._parseApiName(searchResult.fullName);

    // If we have detail data, prefer its separate name fields
    let firstName = parsed.firstName;
    let lastName = parsed.lastName;
    let fullName = parsed.fullName;

    if (detail) {
      if (detail.firstName) firstName = detail.firstName.trim();
      if (detail.lastName) lastName = detail.lastName.trim();
      if (firstName && lastName) {
        fullName = `${firstName} ${lastName}`;
      }
    }

    // Extract address from detail
    let street = '';
    let suburb = '';
    let state = 'NSW';
    let postcode = '';
    let city = '';

    if (detail && detail.streetAddress) {
      street = (detail.streetAddress.street || '').trim();
      suburb = (detail.streetAddress.suburb || '').trim();
      state = (detail.streetAddress.state || 'NSW').trim();
      postcode = (detail.streetAddress.postCode || '').trim();
      city = suburb ? this._titleCase(suburb) : '';
    } else if (searchResult.suburb) {
      city = this._titleCase(searchResult.suburb);
    }

    // Phone
    let phone = '';
    if (detail) {
      phone = (detail.phoneWithAreaCode || '').trim();
      if (!phone && detail.firmPhoneAreaCode && detail.firmPhone) {
        phone = `${detail.firmPhoneAreaCode} ${detail.firmPhone}`.trim();
      }
    }

    // Email
    let email = '';
    if (detail) {
      email = (detail.email || detail.firmEmail || '').trim();
    }

    // Firm name
    const firmName = (detail && detail.placeOfPractice)
      ? detail.placeOfPractice.trim()
      : (searchResult.practice || '').trim();

    // Certificate type / bar status
    let barStatus = '';
    if (detail) {
      barStatus = (detail.certificateType || '').trim();
      if (detail.pcType && detail.pcType !== barStatus) {
        barStatus = barStatus ? `${barStatus} (${detail.pcType})` : detail.pcType;
      }
    }

    // Admission date
    let admissionDate = '';
    if (detail && detail.admissionDate) {
      // Format: "2020-10-09T00:00:00" -> "2020-10-09"
      admissionDate = detail.admissionDate.split('T')[0] || detail.admissionDate;
    }

    // Specialist accreditations
    let specialisations = '';
    if (detail && detail.specialistAccreditation && detail.specialistAccreditation.length > 0) {
      specialisations = detail.specialistAccreditation.join(', ');
    }

    // Languages
    let languages = '';
    if (detail && detail.languages && detail.languages.length > 0) {
      languages = detail.languages.map(l => this._titleCase(l)).join(', ');
    }

    // Region
    const region = (detail && detail.region) ? detail.region.trim() : '';

    // Law Society membership
    const isMember = searchResult.isMember || false;

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: state,
      zip: postcode,
      country: 'Australia',
      phone: phone,
      email: email,
      website: '',
      bar_number: String(searchResult.id || ''),
      bar_status: barStatus,
      admission_date: admissionDate,
      profile_url: `https://ros.lawsociety.com.au/?tab=lawyer#id=${searchResult.id}`,
      practice_areas: specialisations,
      specialisations: specialisations,
      languages: languages,
      region: region,
      is_member: isMember,
      address: street ? this._titleCase(street) : '',
    };
  }
}

module.exports = new NswScraper();
