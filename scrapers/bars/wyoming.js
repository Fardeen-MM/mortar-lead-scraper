/**
 * Wyoming State Bar — WordPress REST API
 *
 * Source: https://www.wyomingbar.org/for-the-public/hire-a-lawyer/
 * Platform: WordPress REST API (fully open, no auth)
 * Method: GET https://www.wyomingbar.org/wp-json/wsb/v1/members
 *
 * Returns JSON with all fields: name, email, phone, firm, address, bar number,
 * admission date, county, judicial district. ~3,055 active members.
 *
 * Privacy: Respects ShowPhone/ShowEmail/ShowOrganization boolean flags.
 * No profile page needed — all data comes from the search API.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_URL = 'https://www.wyomingbar.org/wp-json/wsb/v1/members';
const PAGE_SIZE = 25;

class WyomingScraper extends BaseScraper {
  constructor() {
    super({
      name: 'wyoming',
      stateCode: 'WY',
      baseUrl: API_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {},
      defaultCities: [
        'Cheyenne', 'Casper', 'Laramie', 'Gillette',
        'Rock Springs', 'Sheridan', 'Jackson', 'Riverton',
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
   * GET request to the WY REST API.
   */
  _apiGet(params, rateLimiter) {
    return new Promise((resolve, reject) => {
      const qs = new URLSearchParams(params).toString();
      const url = `${API_URL}?${qs}`;
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
        timeout: 20000,
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
   * Map a WY API record to a normalized attorney object.
   * Respects ShowPhone/ShowEmail/ShowOrganization privacy flags.
   */
  _mapRecord(rec) {
    return {
      first_name: (rec.FirstName || '').trim(),
      last_name: (rec.LastName || '').trim(),
      full_name: `${(rec.FirstName || '').trim()} ${(rec.MiddleName || '').trim()} ${(rec.LastName || '').trim()}${rec.NameSuffix ? ' ' + rec.NameSuffix : ''}`.replace(/\s+/g, ' ').trim(),
      firm_name: rec.ShowOrganization ? (rec.RecordOrganization || '').trim() : '',
      city: (rec.RecordCity || '').trim(),
      state: (rec.RecordState || 'WY').trim(),
      zip: (rec.RecordZip || '').trim(),
      county: (rec.RecordCounty || '').trim(),
      phone: rec.ShowPhone ? (rec.Phone || '').trim() : '',
      email: rec.ShowEmail ? (rec.Email || '').trim().toLowerCase() : '',
      website: (rec.Url || '').trim(),
      bar_number: (rec.AttorneyNumber || '').trim(),
      bar_status: (rec.MemberStatus || '').trim(),
      admission_date: (rec.AdmissionDate || '').trim(),
      source: `${this.name}_bar`,
    };
  }

  /**
   * Fetch all active members from the WY REST API.
   * No city iteration needed — paginate through the full list.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    log.scrape(`Searching Wyoming Bar REST API (${API_URL})`);
    yield { _cityProgress: { current: 1, total: 1 } };

    let page = 1;
    let lastPage = null;
    let totalYielded = 0;

    while (true) {
      if (page > maxPages) {
        log.info(`Reached max pages limit (${maxPages})`);
        break;
      }

      const params = {
        per_page: String(PAGE_SIZE),
        page: String(page),
        MemberStatus: 'Active',
      };

      let response;
      try {
        await rateLimiter.wait();
        response = await this._apiGet(params, rateLimiter);
      } catch (err) {
        log.error(`WY API request failed page ${page}: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) continue;
        break;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from WY API`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (shouldRetry) continue;
        break;
      }

      if (response.statusCode !== 200) {
        log.error(`WY API returned ${response.statusCode}`);
        break;
      }

      rateLimiter.resetBackoff();

      let data;
      try {
        data = JSON.parse(response.body);
      } catch (err) {
        log.error(`Failed to parse WY API response: ${err.message}`);
        break;
      }

      // Extract pagination metadata
      const total = data.total || 0;
      lastPage = data.lastPage || 1;

      if (page === 1) {
        log.success(`Found ${total.toLocaleString()} active members (${lastPage} pages)`);
        if (total === 0) break;
      }

      // Records are keyed by numeric index, skip metadata keys
      let recordCount = 0;
      for (const key of Object.keys(data)) {
        if (key === 'total' || key === 'page' || key === 'lastPage') continue;
        const rec = data[key];
        if (!rec || typeof rec !== 'object' || !rec.LastName) continue;

        const attorney = this._mapRecord(rec);
        attorney.practice_area = practiceArea || '';
        yield this.transformResult(attorney, practiceArea);
        totalYielded++;
        recordCount++;
      }

      if (recordCount === 0) {
        log.info(`No records on page ${page} — stopping`);
        break;
      }

      log.info(`Page ${page}/${lastPage}: ${recordCount} records (${totalYielded} total)`);

      if (page >= lastPage) {
        log.success(`Completed all ${lastPage} pages`);
        break;
      }

      page++;
    }

    log.success(`WY scrape complete — ${totalYielded} records yielded`);
  }
}

module.exports = new WyomingScraper();
