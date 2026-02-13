/**
 * Pennsylvania Disciplinary Board Attorney Scraper
 *
 * Source: https://www.padisciplinaryboard.org/for-the-public/find-attorney
 * Method: Algolia REST API (JSON, no HTML parsing)
 *
 * Uses the Algolia search API with application credentials to query the
 * attorney index. The search() async generator is fully overridden since
 * this is a JSON API, not HTML.
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
      baseUrl: 'https://www.padisciplinaryboard.org/for-the-public/find-attorney/attorney-detail',
      pageSize: 100,
      practiceAreaCodes: {},
      defaultCities: [
        'Philadelphia', 'Pittsburgh', 'Harrisburg', 'Allentown', 'Erie',
        'Scranton', 'Reading', 'Bethlehem', 'Lancaster', 'Norristown',
        'Media', 'Doylestown', 'West Chester', 'King of Prussia',
      ],
    });

    this.algoliaAppId = 'N1H4MQXREP';
    this.algoliaApiKey = '658c08635772deca1fc71b90f429d08c';
    this.algoliaIndex = 'attorneys';
    this.algoliaHost = `${this.algoliaAppId}-dsn.algolia.net`;
    this.algoliaPath = `/1/indexes/${this.algoliaIndex}/query`;
  }

  /**
   * Not used — search() is fully overridden for the Algolia JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Algolia API`);
  }

  /**
   * Not used — search() is fully overridden for the Algolia JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Algolia API`);
  }

  /**
   * Not used — search() is fully overridden for the Algolia JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Algolia API`);
  }

  /**
   * Make an Algolia search request.
   *
   * @param {string} query     - The search query (city name)
   * @param {number} page      - 0-indexed page number
   * @param {RateLimiter} rateLimiter - Rate limiter instance for user agent
   * @returns {Promise<object>} Parsed JSON response with hits and nbPages
   */
  algoliaSearch(query, page, rateLimiter) {
    return new Promise((resolve, reject) => {
      const params = `query=${encodeURIComponent(query)}&hitsPerPage=${this.pageSize}&page=${page}`;
      const postBody = JSON.stringify({ params });

      const options = {
        hostname: this.algoliaHost,
        port: 443,
        path: this.algoliaPath,
        method: 'POST',
        headers: {
          'X-Algolia-Application-Id': this.algoliaAppId,
          'X-Algolia-API-Key': this.algoliaApiKey,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postBody),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
        },
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(data) });
          } catch (err) {
            reject(new Error(`Failed to parse Algolia response: ${err.message}`));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Algolia request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Async generator that yields attorney records from the PA Algolia API.
   * Overrides BaseScraper.search() entirely since the data source is JSON, not HTML.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 0;
      let pagesFetched = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${page + 1} (Algolia page ${page}) for ${city}`);

        let result;
        try {
          await rateLimiter.wait();
          result = await this.algoliaSearch(city, page, rateLimiter);
        } catch (err) {
          log.error(`Algolia request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (result.statusCode === 429 || result.statusCode === 403) {
          log.warn(`Got ${result.statusCode} from Algolia`);
          const shouldRetry = await rateLimiter.handleBlock(result.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (result.statusCode !== 200) {
          log.error(`Unexpected status ${result.statusCode} from Algolia — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        const data = result.body;
        const hits = data.hits || [];
        const nbPages = data.nbPages || 0;

        if (hits.length === 0) {
          if (pagesFetched === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          } else {
            log.success(`Completed all pages for ${city}`);
          }
          break;
        }

        if (pagesFetched === 0) {
          const totalHits = data.nbHits || hits.length;
          log.success(`Found ${totalHits.toLocaleString()} results (${nbPages} pages) for ${city}`);
        }

        // Map and yield each attorney record
        for (const hit of hits) {
          const attorney = {
            first_name: (hit.first_name || '').trim(),
            last_name: (hit.last_name || '').trim(),
            firm_name: '',
            city: (hit.city || '').trim(),
            state: 'PA',
            phone: '',
            email: '',
            website: '',
            bar_number: (hit.attorney_id || '').toString().trim(),
            admission_date: (hit.admission_date || '').trim(),
            bar_status: (hit.status || '').trim(),
            county: (hit.county || '').trim(),
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

        // Stop when we've reached the last page (0-indexed)
        if (page + 1 >= nbPages) {
          log.success(`Completed all ${nbPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new PennsylvaniaScraper();
