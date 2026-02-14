/**
 * Ohio Supreme Court Attorney Search Scraper
 *
 * Source: https://www.supremecourt.ohio.gov/AttorneySearch/
 * Method: POST to Ajax.ashx endpoint (Ember.js SPA backend)
 *
 * The Ohio Attorney Search is an Ember.js SPA. We hit the backend API
 * at /AttorneySearch/Ajax.ashx with POST requests.
 *
 * Flow:
 * 1. GET the main page to obtain the CSRF token from <meta name="csrf-token">
 * 2. POST action=SearchAttorney with search params + X-CSRF-TOKEN header
 * 3. Response: { MySearchResults: [...], TooManyResults: bool, NoResults: bool }
 *
 * Search results contain: AttorneyNumber, FirstName, MiddleName, LastName,
 * Status, AdmittedBy, AdmissionDate. City/phone/email are NOT in search results.
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
      baseUrl: 'https://www.supremecourt.ohio.gov/AttorneySearch/',
      pageSize: 500, // API returns all results at once; this is just a nominal limit
      practiceAreaCodes: {},
      defaultCities: [
        'Columbus', 'Cleveland', 'Cincinnati', 'Dayton', 'Toledo',
        'Akron', 'Canton', 'Youngstown', 'Springfield', 'Hamilton',
        'Elyria', 'Mansfield', 'Newark', 'Lima',
      ],
    });

    this.ajaxUrl = 'https://www.supremecourt.ohio.gov/AttorneySearch/Ajax.ashx';
    this.csrfToken = null;
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Ohio Ajax API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Ohio Ajax API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Ohio Ajax API`);
  }

  /**
   * Fetch the main page and extract the CSRF token.
   */
  _fetchCsrfToken(rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const req = https.get(this.baseUrl, {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html',
        },
        timeout: 15000,
      }, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          const match = data.match(/csrf-token.*?content="([^"]+)"/);
          if (match) {
            resolve(match[1]);
          } else {
            reject(new Error('Could not extract CSRF token from Ohio Attorney Search page'));
          }
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * POST to the Ajax.ashx endpoint with search parameters.
   */
  _ajaxPost(formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = new URLSearchParams(formData).toString();
      const bodyBuffer = Buffer.from(postBody, 'utf8');

      const parsed = new URL(this.ajaxUrl);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json, text/javascript, */*; q=0.01',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': bodyBuffer.length,
          'X-CSRF-TOKEN': this.csrfToken,
          'X-Requested-With': 'XMLHttpRequest',
          'Referer': this.baseUrl,
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
      req.write(bodyBuffer);
      req.end();
    });
  }

  /**
   * Async generator that yields attorney records from the Ohio Supreme Court API.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`Ohio attorney search does not support practice area filtering — searching all attorneys`);
    }

    // Fetch CSRF token once
    try {
      await rateLimiter.wait();
      this.csrfToken = await this._fetchCsrfToken(rateLimiter);
      log.info(`Obtained CSRF token for Ohio Attorney Search`);
    } catch (err) {
      log.error(`Failed to get CSRF token: ${err.message}`);
      return;
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Check max pages limit (1 "page" per city since API returns all results)
      if (options.maxPages && options.maxPages < 1) {
        log.info(`Reached max pages limit for ${city}`);
        continue;
      }

      let response;
      try {
        await rateLimiter.wait();
        response = await this._ajaxPost({
          action: 'SearchAttorney',
          lastName: '',
          firstName: '',
          city: city,
          state: '',
          zipCode: '',
          regNumber: '',
          companyName: '',
          status: 'Active',
        }, rateLimiter);
      } catch (err) {
        log.error(`Request failed for ${city}: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode !== 200 || !response.body) {
        log.error(`Unexpected status ${response.statusCode} or empty body for ${city} — skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      const data = response.body;

      if (data.NoResults) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      if (data.TooManyResults) {
        log.warn(`Too many results for ${city} — server returned TooManyResults flag`);
      }

      const records = data.MySearchResults || [];

      if (records.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      log.success(`Found ${records.length} results for ${city}`);

      for (const rec of records) {
        const attorney = {
          first_name: (rec.FirstName || '').trim(),
          last_name: (rec.LastName || '').trim(),
          firm_name: '',
          city: city,
          state: 'OH',
          phone: '',
          email: '',
          website: '',
          bar_number: (rec.AttorneyNumber || '').toString().trim(),
          admission_date: (rec.AdmissionDate || '').trim(),
          bar_status: (rec.Status || '').trim(),
          source: `${this.name}_bar`,
        };

        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }

        attorney.practice_area = practiceArea || '';
        yield attorney;
      }
    }
  }
}

module.exports = new OhioScraper();
