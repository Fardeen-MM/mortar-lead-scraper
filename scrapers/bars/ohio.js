/**
 * Ohio Supreme Court Attorney Search Scraper
 *
 * Source: https://www.supremecourt.ohio.gov/AttorneySearch/
 * Method: HTTP GET to JSON API (Angular SPA backend)
 *
 * The Ohio Attorney Search is an Angular SPA that makes backend API calls.
 * We hit the API directly at /AttorneySearch/api/Attorney/Search with query params.
 * The API returns JSON with attorney results and supports pagination via PageNumber.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class OhioScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ohio',
      stateCode: 'OH',
      baseUrl: 'https://www.supremecourt.ohio.gov/AttorneySearch/api/Attorney/Search',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Columbus', 'Cleveland', 'Cincinnati', 'Dayton', 'Toledo',
        'Akron', 'Canton', 'Youngstown', 'Springfield', 'Hamilton',
        'Elyria', 'Mansfield', 'Newark', 'Lima',
      ],
    });
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Ohio JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Ohio JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Ohio JSON API`);
  }

  /**
   * Build the API search URL for a city and page number.
   * @param {string} city
   * @param {number} page
   * @returns {string}
   */
  _buildApiUrl(city, page) {
    const params = new URLSearchParams();
    params.set('LastName', '*');
    params.set('FirstName', '');
    params.set('City', city);
    params.set('Status', 'Active');
    params.set('PageNumber', String(page));
    params.set('PageSize', String(this.pageSize));
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * HTTP GET with JSON-specific headers for the Ohio API.
   * The Angular SPA backend may require Accept: application/json and
   * X-Requested-With: XMLHttpRequest to return JSON instead of HTML.
   */
  _httpGetJson(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'X-Requested-With': 'XMLHttpRequest',
          'Referer': 'https://www.supremecourt.ohio.gov/AttorneySearch/',
        },
        timeout: 15000,
      };

      const req = https.get(url, options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          return resolve(this._httpGetJson(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Async generator that yields attorney records from the Ohio Supreme Court API.
   * Overrides BaseScraper.search() entirely since the data source is a JSON API.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`Ohio attorney search does not support practice area filtering — searching all attorneys`);
    }

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = null;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this._buildApiUrl(city, page);
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpGetJson(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Parse JSON response
        let data;
        try {
          data = JSON.parse(response.body);
        } catch (err) {
          // If we got HTML instead of JSON, log a warning
          if (response.body.includes('<html') || response.body.includes('<!DOCTYPE')) {
            log.warn(`Got HTML instead of JSON for ${city} — API may require browser session`);
          } else {
            log.error(`Failed to parse JSON response: ${err.message}`);
          }
          break;
        }

        // Extract records — the API may return:
        //   { results: [...], totalCount: N }
        //   or just an array of attorney objects
        let records;
        if (Array.isArray(data)) {
          records = data;
        } else if (data && Array.isArray(data.results)) {
          records = data.results;
          if (totalResults === null && typeof data.totalCount === 'number') {
            totalResults = data.totalCount;
          }
        } else if (data && Array.isArray(data.attorneys)) {
          records = data.attorneys;
          if (totalResults === null && typeof data.totalCount === 'number') {
            totalResults = data.totalCount;
          }
        } else if (data && Array.isArray(data.data)) {
          records = data.data;
          if (totalResults === null && typeof data.total === 'number') {
            totalResults = data.total;
          }
        } else {
          log.warn(`Unexpected API response structure for ${city} — keys: ${Object.keys(data || {}).join(', ')}`);
          break;
        }

        if (records.length === 0) {
          if (pagesFetched === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          } else {
            log.success(`Completed all pages for ${city}`);
          }
          break;
        }

        if (pagesFetched === 0) {
          const totalMsg = totalResults !== null
            ? `${totalResults.toLocaleString()} total results`
            : `first batch: ${records.length} records`;
          log.success(`Fetching results for ${city} (${totalMsg})`);
        }

        // Map and yield each attorney record
        for (const rec of records) {
          const attorney = {
            first_name: (rec.firstName || rec.first_name || rec.FirstName || '').trim(),
            last_name: (rec.lastName || rec.last_name || rec.LastName || '').trim(),
            firm_name: (rec.firmName || rec.firm_name || rec.FirmName || rec.lawFirmName || '').trim(),
            city: (rec.city || rec.City || '').trim(),
            state: (rec.state || rec.State || 'OH').trim(),
            phone: (rec.phone || rec.Phone || rec.phoneNumber || '').trim(),
            email: (rec.email || rec.Email || rec.emailAddress || '').trim(),
            website: '',
            bar_number: (rec.attorneyNumber || rec.attorney_number || rec.AttorneyNumber || rec.registrationNumber || '').toString().trim(),
            admission_date: (rec.admissionDate || rec.admission_date || rec.AdmissionDate || '').trim(),
            bar_status: (rec.status || rec.Status || '').trim(),
            source: `${this.name}_bar`,
          };

          // Apply min year filter
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        // Determine if there are more pages
        if (totalResults !== null) {
          const totalPages = Math.ceil(totalResults / this.pageSize);
          if (page >= totalPages) {
            log.success(`Completed all ${totalPages} pages for ${city}`);
            break;
          }
        } else if (records.length < this.pageSize) {
          // Fewer results than page size means we've reached the end
          log.success(`Completed all pages for ${city} (last page had ${records.length} records)`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new OhioScraper();
