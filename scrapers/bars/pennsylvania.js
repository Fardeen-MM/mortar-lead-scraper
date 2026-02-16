/**
 * Pennsylvania Disciplinary Board Attorney Scraper
 *
 * Source: https://www.padisciplinaryboard.org/for-the-public/find-attorney
 * Method: REST API (JSON) at /api/attorneysearch
 *
 * The PA Disciplinary Board provides a REST API that accepts query params
 * (city, status, last, first, pageNumber, pageLength) and returns JSON
 * with paginated attorney records.
 *
 * Profile enrichment: A separate detail API at /api/attorney?id={attorneyId}
 * returns full attorney info including street address (line1/line2/line3),
 * state, postalCode, country, district, faxNumber, otherPhone, middleName,
 * title, and pli (professional liability insurance). The search API returns
 * most of these as null — the detail API is the only way to get them.
 *
 * Detail page: /for-the-public/find-attorney/attorney-detail/{attorneyId}
 * (server-rendered HTML, but we use the JSON API instead for speed)
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class PennsylvaniaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'pennsylvania',
      stateCode: 'PA',
      baseUrl: 'https://www.padisciplinaryboard.org/api/attorneysearch',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Philadelphia', 'Pittsburgh', 'Harrisburg', 'Allentown', 'Erie',
        'Scranton', 'Reading', 'Bethlehem', 'Lancaster', 'Norristown',
        'Media', 'Doylestown', 'West Chester', 'King of Prussia',
      ],
    });

    this.detailApiUrl = 'https://www.padisciplinaryboard.org/api/attorney';
    this.detailPageBaseUrl = 'https://www.padisciplinaryboard.org/for-the-public/find-attorney/attorney-detail';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for PA REST API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for PA REST API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for PA REST API`);
  }

  /**
   * HTTP GET to the PA attorney search REST API.
   */
  _apiGet(url, rateLimiter) {
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
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Referer': 'https://www.padisciplinaryboard.org/for-the-public/find-attorney',
        },
        timeout: 15000,
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
      req.end();
    });
  }

  /**
   * Fetch detailed attorney info from the PA detail API.
   *
   * GET /api/attorney?id={attorneyId} returns:
   *   { attorney: { firstName, lastName, middleName, employer, phone,
   *     faxNumber, otherPhone, email, line1, line2, line3, city, state,
   *     postalCode, country, county, district, dateOfAdmission, status,
   *     title, pli, hasDiscipline, ... }, error, message }
   *
   * @param {string|number} attorneyId - PA attorney ID
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {object|null} Attorney detail object or null on failure
   */
  async _getAttorneyDetail(attorneyId, rateLimiter) {
    if (!attorneyId) return null;

    const url = `${this.detailApiUrl}?id=${attorneyId}`;

    try {
      await rateLimiter.wait();
      const response = await this._apiGet(url, rateLimiter);

      if (response.statusCode !== 200 || !response.body) return null;
      if (response.body.error) return null;

      return response.body.attorney || null;
    } catch (err) {
      log.warn(`PA detail API failed for ${attorneyId}: ${err.message}`);
      return null;
    }
  }

  /**
   * Override hasProfileParser to return true since we implement enrichFromProfile.
   * PA uses a JSON detail API, not server-rendered profile pages, so we override
   * enrichFromProfile directly (same pattern as Ohio).
   */
  get hasProfileParser() {
    return true;
  }

  /**
   * Fetch and parse attorney detail from the PA detail API.
   * Overrides BaseScraper.enrichFromProfile because PA uses a JSON API,
   * not a server-rendered profile page.
   *
   * The detail API returns full address, employer, phone, fax, district,
   * title, and other fields that are null/missing in search results.
   *
   * @param {object} lead - The lead object (must have bar_number = attorneyId)
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {object} Additional fields from the detail API
   */
  async enrichFromProfile(lead, rateLimiter) {
    const attorneyId = lead.bar_number;
    if (!attorneyId) return {};

    const detail = await this._getAttorneyDetail(attorneyId, rateLimiter);
    if (!detail) return {};

    const result = {};

    // Phone (detail API often has phone when search API doesn't)
    if (detail.phone) {
      const phone = String(detail.phone).trim();
      if (phone.length > 5) {
        result.phone = phone;
      }
    }

    // Email
    if (detail.email) {
      const email = String(detail.email).trim().toLowerCase();
      if (email.includes('@')) {
        result.email = email;
      }
    }

    // Employer / firm name
    if (detail.employer) {
      const employer = String(detail.employer).trim();
      if (employer.length > 1 && employer.length < 200) {
        result.firm_name = employer;
      }
    }

    // Full street address from line1 + line2 + line3
    const addressParts = [detail.line1, detail.line2, detail.line3]
      .map(s => (s || '').trim())
      .filter(Boolean);
    if (addressParts.length > 0) {
      result.address = addressParts.join(', ');
    }

    // State (detail API returns full state name like "PENNSYLVANIA")
    if (detail.state) {
      const state = String(detail.state).trim();
      if (state) {
        result.state_full = state;
      }
    }

    // ZIP / postal code
    if (detail.postalCode) {
      const zip = String(detail.postalCode).trim();
      if (zip) {
        result.zip = zip;
      }
    }

    // City (detail API may have more accurate city than search param)
    if (detail.city) {
      const city = String(detail.city).trim();
      if (city) {
        result.city = city;
      }
    }

    // County
    if (detail.county) {
      const county = String(detail.county).trim();
      if (county) {
        result.county = county;
      }
    }

    // District
    if (detail.district) {
      const district = String(detail.district).trim();
      if (district) {
        result.district = district;
      }
    }

    // Fax number
    if (detail.faxNumber) {
      const fax = String(detail.faxNumber).trim();
      if (fax.length > 5) {
        result.fax = fax;
      }
    }

    // Title (job title)
    if (detail.title) {
      const title = String(detail.title).trim();
      if (title.length > 1) {
        result.title = title;
      }
    }

    // Middle name (supplement the first_name)
    if (detail.middleName) {
      const middle = String(detail.middleName).trim();
      if (middle) {
        result.middle_name = middle;
      }
    }

    // Date of admission (detail API returns ISO format: "1997-10-30T00:00:00")
    if (detail.dateOfAdmission) {
      const raw = String(detail.dateOfAdmission).trim();
      // Convert ISO format to MM/DD/YYYY for consistency
      const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (isoMatch) {
        result.admission_date = `${isoMatch[2]}/${isoMatch[3]}/${isoMatch[1]}`;
      } else if (raw) {
        result.admission_date = raw;
      }
    }

    // Remove empty/null/undefined values
    for (const key of Object.keys(result)) {
      if (result[key] === '' || result[key] === undefined || result[key] === null) {
        delete result[key];
      }
    }

    return result;
  }

  /**
   * Async generator that yields attorney records from the PA REST API.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`PA attorney search does not support practice area filtering — searching all attorneys`);
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let pageNumber = 1;
      let pagesFetched = 0;
      let totalRecords = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const params = new URLSearchParams();
        params.set('city', city);
        params.set('status', 'Active');
        params.set('pageNumber', String(pageNumber));
        params.set('pageLength', String(this.pageSize));

        const url = `${this.baseUrl}?${params.toString()}`;
        log.info(`Page ${pageNumber} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200 || !response.body) {
          log.error(`Unexpected status ${response.statusCode} or empty body for ${city} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        const result = response.body.result;
        if (!result || !result.items) {
          log.error(`Unexpected response structure for ${city} — skipping`);
          break;
        }

        const items = result.items;

        if (pagesFetched === 0) {
          totalRecords = result.totalRecords || 0;
          if (items.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalRecords / this.pageSize);
          log.success(`Found ${totalRecords.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        if (items.length === 0) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        for (const item of items) {
          const attyId = String(item.attorneyId ?? '').trim();
          const attorney = {
            first_name: (item.firstName || '').trim(),
            last_name: (item.lastName || '').trim(),
            firm_name: (item.employer || '').trim(),
            city: (item.city || '').trim(),
            state: 'PA',
            phone: (item.phone || '').trim(),
            email: (item.email || '').trim(),
            website: '',
            bar_number: attyId,
            admission_date: (item.dateOfAdmission || '').trim(),
            bar_status: (item.status || '').trim(),
            county: (item.county || '').trim(),
            profile_url: attyId ? `${this.detailPageBaseUrl}/${attyId}` : '',
            source: `${this.name}_bar`,
          };

          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        const totalPages = Math.ceil(totalRecords / this.pageSize);
        if (pageNumber >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        pageNumber++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new PennsylvaniaScraper();
