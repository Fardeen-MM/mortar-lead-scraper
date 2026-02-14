/**
 * Pennsylvania Disciplinary Board Attorney Scraper
 *
 * Source: https://www.padisciplinaryboard.org/for-the-public/find-attorney
 * Method: REST API (JSON) at /api/attorneysearch
 *
 * The PA Disciplinary Board provides a REST API that accepts query params
 * (city, status, last, first, pageNumber, pageLength) and returns JSON
 * with paginated attorney records.
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
          const attorney = {
            first_name: (item.firstName || '').trim(),
            last_name: (item.lastName || '').trim(),
            firm_name: (item.employer || '').trim(),
            city: (item.city || '').trim(),
            state: 'PA',
            phone: (item.phone || '').trim(),
            email: (item.email || '').trim(),
            website: '',
            bar_number: String(item.attorneyId ?? '').trim(),
            admission_date: (item.dateOfAdmission || '').trim(),
            bar_status: (item.status || '').trim(),
            county: (item.county || '').trim(),
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
