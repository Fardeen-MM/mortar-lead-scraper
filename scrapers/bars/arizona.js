/**
 * Arizona State Bar Scraper
 *
 * Source: https://www.azbar.org/find-a-lawyer/
 * API:    https://api-proxy.azbar.org/MemberSearch/Search
 * Method: HTTP POST with JSON body, paginated via query params
 *
 * The AZ Bar exposes a public REST API behind api-proxy.azbar.org.
 * Authentication is via static userid/password headers (public credentials
 * embedded in the Find-a-Lawyer SPA).
 *
 * Request: POST with JSON body containing search filters.
 * Pagination: ?PageSize=25&Page=N query parameters.
 * Response shape:
 *   {
 *     IsSuccess: true,
 *     Result: {
 *       TotalCount: 2559,
 *       Results: [ { EntityNumber, BarNumber, FirstName, LastName, Company,
 *                     Address: { City, State, Zip, County },
 *                     Email, PhoneNumbers[], PrimaryPhone, MemberStatus, ... } ]
 *     }
 *   }
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ArizonaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'arizona',
      stateCode: 'AZ',
      baseUrl: 'https://api-proxy.azbar.org/MemberSearch/Search',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Phoenix', 'Tucson', 'Mesa', 'Scottsdale', 'Chandler',
        'Tempe', 'Gilbert', 'Glendale', 'Peoria', 'Flagstaff',
        'Surprise', 'Yuma', 'Prescott', 'Lake Havasu City', 'Sedona',
      ],
    });
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for AZ Bar API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for AZ Bar API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for AZ Bar API`);
  }

  /**
   * POST to the AZ Bar Member Search API.
   *
   * @param {string} city - City to search
   * @param {number} page - 1-based page number
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {Promise<{statusCode: number, body: object|null, rawBody?: string}>}
   */
  _apiPost(city, page, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();

      const queryParams = new URLSearchParams();
      queryParams.set('PageSize', String(this.pageSize));
      queryParams.set('Page', String(page));

      const requestBody = JSON.stringify({
        Type: '',
        FirstName: '',
        LastName: '',
        Firm: '',
        City: city,
        State: '',
        Zip: '',
        County: '',
        LanguageCode: '',
        Section: '',
        LegalNeed: '',
        Specialization: '',
        JurisdictionCode: '',
        LawSchool: '',
        FuzzySearch: false,
        IncludeDeceased: false,
      });

      const options = {
        hostname: 'api-proxy.azbar.org',
        port: 443,
        path: `/MemberSearch/Search?${queryParams.toString()}`,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Origin': 'https://www.azbar.org',
          'Referer': 'https://www.azbar.org/',
          // Public auth credentials embedded in the Find-a-Lawyer SPA
          'userid': 'publictools',
          'password': '12B631CC-5922-4EF8-8978-23CF2F32EA8D',
          'Content-Length': Buffer.byteLength(requestBody),
        },
        timeout: 20000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(data) });
          } catch {
            resolve({ statusCode: res.statusCode, body: null, rawBody: data });
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(requestBody);
      req.end();
    });
  }

  /**
   * Extract the best phone number from a member record.
   * Checks PrimaryPhone first, then PhoneNumbers array (prefers Office type).
   *
   * @param {object} member - Raw API member object
   * @returns {string}
   */
  _extractPhone(member) {
    // PrimaryPhone is a top-level string field
    if (member.PrimaryPhone && member.PrimaryPhone.trim()) {
      return member.PrimaryPhone.trim();
    }

    const phoneNumbers = member.PhoneNumbers;
    if (!Array.isArray(phoneNumbers) || phoneNumbers.length === 0) return '';

    // Prefer Office phone
    const office = phoneNumbers.find(p =>
      p && p.PhoneType && p.PhoneType.toLowerCase().includes('office')
    );
    if (office && office.PhoneNumber) return office.PhoneNumber.trim();

    // Fall back to first phone with a number
    const first = phoneNumbers.find(p => p && p.PhoneNumber);
    return first ? first.PhoneNumber.trim() : '';
  }

  /**
   * Map an API member record to the standard attorney object format.
   *
   * API response fields per member:
   *   EntityNumber, BarNumber, FirstName, MiddleName, LastName, Company,
   *   Address: { Address1, Address2, City, State, Zip, County },
   *   Email, PhoneNumbers[], PrimaryPhone, ProfilePicUrl,
   *   MemberStatus, MemberType, IsProBonoCounsel, BillCode
   *
   * @param {object} member - Raw API member object
   * @returns {object} Normalized attorney record
   */
  _mapMember(member) {
    const address = member.Address || {};

    return {
      first_name: (member.FirstName || '').trim(),
      last_name: (member.LastName || '').trim(),
      firm_name: (member.Company || '').trim(),
      city: (address.City || '').trim(),
      state: (address.State || 'AZ').trim(),
      zip: (address.Zip || '').trim(),
      phone: this._extractPhone(member),
      email: (member.Email || '').trim().toLowerCase(),
      bar_number: (member.BarNumber || String(member.EntityNumber || '')).trim(),
      bar_status: (member.MemberStatus || '').trim(),
      source: `${this.name}_bar`,
    };
  }

  /**
   * Async generator that yields attorney records from the AZ Bar API.
   * Overrides BaseScraper.search() for the JSON POST API.
   *
   * Response envelope: { IsSuccess, Result: { TotalCount, Results: [...] } }
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let totalCount = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${page} for ${city} — POST ${this.baseUrl}?PageSize=${this.pageSize}&Page=${page}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiPost(city, page, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting / auth errors
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 401) {
          log.error(`Got 401 Unauthorized — AZ Bar API credentials may have changed`);
          yield { _captcha: true, city, page };
          break;
        }

        if (response.statusCode !== 200 || !response.body) {
          log.error(`Unexpected status ${response.statusCode} or empty body for ${city} — skipping`);
          if (response.rawBody) {
            log.warn(`Raw response: ${response.rawBody.substring(0, 200)}`);
          }
          break;
        }

        rateLimiter.resetBackoff();

        // Unwrap the API envelope: { IsSuccess, Result: { TotalCount, Results } }
        const envelope = response.body;

        if (envelope.IsSuccess === false) {
          log.error(`API returned IsSuccess=false for ${city}: ${envelope.Error || envelope.Message || 'unknown error'}`);
          break;
        }

        const result = envelope.Result || envelope.result || envelope;
        const members = result.Results || result.results || [];
        totalCount = result.TotalCount || result.totalCount || totalCount;

        if (page === 1) {
          if (members.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          if (totalCount > 0) {
            const totalPages = Math.ceil(totalCount / this.pageSize);
            log.success(`Found ${totalCount.toLocaleString()} results (${totalPages} pages) for ${city}`);
          } else {
            log.success(`Fetching results for ${city} (first page: ${members.length} records)`);
          }
        }

        if (members.length === 0) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        // Map and yield each member record
        for (const member of members) {
          const attorney = this._mapMember(member);

          // Apply min year filter if available
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        // Check if we've fetched all pages
        if (totalCount > 0) {
          const totalPages = Math.ceil(totalCount / this.pageSize);
          if (page >= totalPages) {
            log.success(`Completed all ${totalPages} pages for ${city}`);
            break;
          }
        } else {
          // No totalCount — stop when we get fewer results than page size
          if (members.length < this.pageSize) {
            log.success(`Completed all pages for ${city} (last page had ${members.length} records)`);
            break;
          }
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new ArizonaScraper();
