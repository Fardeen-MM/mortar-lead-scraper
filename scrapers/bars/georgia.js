/**
 * Georgia State Bar Association Scraper
 *
 * Source: https://www.gabar.org/memberdirectory/
 * API: https://api.gabar.org/webservices/membersearch
 * Method: HTTP POST to JSON API (Vue.js SPA backend)
 *
 * The GA Bar migrated from a ColdFusion site to a Vue.js SPA
 * with a REST API at api.gabar.org. The membersearch endpoint
 * accepts city/status filters via query params and returns JSON.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class GeorgiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'georgia',
      stateCode: 'GA',
      baseUrl: 'https://api.gabar.org/webservices/membersearch',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Atlanta', 'Savannah', 'Augusta', 'Columbus', 'Macon',
        'Athens', 'Roswell', 'Albany', 'Marietta', 'Decatur',
        'Lawrenceville', 'Kennesaw', 'Gainesville', 'Valdosta',
      ],
    });
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for GA Bar API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for GA Bar API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for GA Bar API`);
  }

  /**
   * POST to the GA Bar API and parse JSON response.
   */
  _apiPost(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Origin': 'https://www.gabar.org',
          'Referer': 'https://www.gabar.org/',
          'Content-Length': Buffer.byteLength('{}'),
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
      req.write('{}');
      req.end();
    });
  }

  /**
   * Async generator that yields attorney records from the GA Bar API.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let skip = 0;
      let pagesFetched = 0;
      let totalRows = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const params = new URLSearchParams();
        params.set('City', city);
        params.set('Status', 'Active Member in Good Standing');
        params.set('top', String(this.pageSize));
        if (skip > 0) params.set('skip', String(skip));

        const url = `${this.baseUrl}?${params.toString()}`;
        log.info(`Page ${pagesFetched + 1} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiPost(url, rateLimiter);
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

        const { members, totalRows: total } = response.body;

        if (pagesFetched === 0) {
          totalRows = total || 0;
          if (!members || members.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          log.success(`Found ${totalRows.toLocaleString()} results for ${city}`);
        }

        if (!members || members.length === 0) {
          log.info(`No more results for ${city}`);
          break;
        }

        for (const m of members) {
          if (options.minYear && m.admitDate) {
            const year = parseInt((m.admitDate.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          yield {
            first_name: (m.firstName || '').trim(),
            last_name: (m.lastName || '').trim(),
            firm_name: (m.company || '').trim(),
            city: (m.city || '').trim(),
            state: (m.state || 'GA').trim(),
            phone: (m.phone || '').trim(),
            email: m.hideEmail ? '' : (m.email || '').trim(),
            website: '',
            bar_number: String(m.barNumber || ''),
            admission_date: (m.admitDate || '').split('T')[0],
            bar_status: (m.status || '').trim(),
            practice_area: practiceArea || '',
            source: `${this.name}_bar`,
          };
        }

        skip += members.length;
        pagesFetched++;

        if (skip >= totalRows || members.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }
      }
    }
  }
}

module.exports = new GeorgiaScraper();
