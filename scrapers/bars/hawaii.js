/**
 * Hawaii State Bar Association — Algolia Search API
 *
 * Source: https://hsba.org/member-directory
 * Platform: Algolia instant search (Sail AMX)
 * Method: POST to Algolia REST API returning JSON
 *
 * Index: production-hsba-insta-search
 * Filter: DataType:User (member directory users, not section memberships)
 * Fields: first_name, last_name, full_name, email, phone, address,
 *         organization, membership_status, permalink, licenses,
 *         county_of_practice, specializations,
 *         customFormField:0ef19433-578b-48bf-8186-b2ed919d0b7e (JD Number)
 *
 * Total records: ~10,277 (as of 2026-02)
 * Strategy: Iterate A–Z by last_name prefix, paginate within each letter.
 *           Algolia caps at 1000 results per query; letters exceeding that
 *           (currently only "S") are split into two-letter sub-prefixes.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const ALGOLIA_APP_ID = 'PE7QKUXU6Z';
const ALGOLIA_API_KEY = '85d207fc72186c8a29100d3da19b9519';
const ALGOLIA_INDEX = 'production-hsba-insta-search';
const ALGOLIA_URL = `https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/${ALGOLIA_INDEX}/query`;
const HITS_PER_PAGE = 150;
const ALGOLIA_MAX_RESULTS = 1000; // Algolia hard limit: page * hitsPerPage + hitsPerPage <= 1000
const JD_NUMBER_FIELD = 'customFormField:0ef19433-578b-48bf-8186-b2ed919d0b7e';

class HawaiiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'hawaii',
      stateCode: 'HI',
      baseUrl: 'https://hsba.org/member-directory',
      pageSize: HITS_PER_PAGE,
      practiceAreaCodes: {},
      defaultCities: [
        'Honolulu', 'Hilo', 'Kailua', 'Pearl City',
        'Waipahu', 'Kaneohe', 'Kapolei', 'Wailuku',
      ],
    });
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
   * POST a search query to the Algolia API.
   *
   * @param {object} body - Algolia query body
   * @param {RateLimiter} rateLimiter
   * @returns {object} Parsed JSON response
   */
  algoliaSearch(body, rateLimiter) {
    return new Promise((resolve, reject) => {
      const payload = JSON.stringify(body);
      const url = new URL(ALGOLIA_URL);
      const options = {
        hostname: url.hostname,
        path: url.pathname,
        method: 'POST',
        headers: {
          'x-algolia-api-key': ALGOLIA_API_KEY,
          'x-algolia-application-id': ALGOLIA_APP_ID,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
          'User-Agent': rateLimiter.getUserAgent(),
        },
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            const parsed = JSON.parse(data);
            if (parsed.status && parsed.status >= 400) {
              return reject(new Error(`Algolia ${parsed.status}: ${parsed.message}`));
            }
            resolve(parsed);
          } catch (err) {
            reject(new Error(`Failed to parse Algolia response: ${err.message}`));
          }
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Algolia request timed out')); });
      req.write(payload);
      req.end();
    });
  }

  /**
   * Parse Algolia address field into city, state, zip.
   * Format: "1001 Shaw Rd\nPuyallup, WA, 98371" or "235 Pennsylvania St\nDenver, CO, 80209"
   */
  parseAddress(address) {
    if (!address) return { city: '', state: '', zip: '' };

    // Take the last line (city/state/zip portion)
    const lines = address.split('\n');
    const lastLine = lines[lines.length - 1].trim();

    // Match "City, ST, ZIP" or "City, ST ZIP"
    const match = lastLine.match(/^(.+?),\s*([A-Z]{2}),?\s*([\d-]*)\s*$/);
    if (match) {
      return { city: match[1].trim(), state: match[2], zip: match[3] || '' };
    }

    // Fallback: try just "City, ST"
    const fallback = lastLine.match(/^(.+?),\s*([A-Z]{2})\s*$/);
    if (fallback) {
      return { city: fallback[1].trim(), state: fallback[2], zip: '' };
    }

    return { city: '', state: '', zip: '' };
  }

  /**
   * Map an Algolia hit to a normalized attorney record.
   */
  mapHit(hit) {
    const addr = this.parseAddress(hit.address);

    return {
      first_name: (hit.first_name || '').trim(),
      last_name: (hit.last_name || '').trim(),
      firm_name: (hit.organization || '').trim(),
      email: (hit.email || '').trim(),
      phone: (hit.phone || '').trim(),
      city: addr.city,
      state: addr.state || 'HI',
      zip: addr.zip,
      bar_number: (hit[JD_NUMBER_FIELD] || '').trim(),
      bar_status: (hit.membership_status || '').trim(),
      profile_url: (hit.permalink || '').trim(),
      county: (hit.county_of_practice || '').trim(),
      source: `${this.name}_bar`,
      practice_area: '',
    };
  }

  /**
   * Fetch all results for a given search prefix (letter or two-letter combo).
   * Handles pagination within the Algolia 1000-result limit.
   *
   * @param {string} prefix - Search prefix (e.g. "A", "Sm")
   * @param {RateLimiter} rateLimiter
   * @param {object} options
   * @yields {object} Attorney records
   */
  async *fetchPrefix(prefix, rateLimiter, options) {
    let page = 0;
    let totalHits = 0;

    while (true) {
      if (options.maxPages && page >= options.maxPages) {
        log.info(`Reached max pages limit (${options.maxPages}) for prefix "${prefix}"`);
        break;
      }

      const body = {
        query: prefix,
        hitsPerPage: HITS_PER_PAGE,
        page,
        filters: 'DataType:User',
        restrictSearchableAttributes: ['last_name'],
      };

      let result;
      try {
        await rateLimiter.wait();
        result = await this.algoliaSearch(body, rateLimiter);
      } catch (err) {
        log.error(`Algolia request failed for prefix "${prefix}" page ${page}: ${err.message}`);
        break;
      }

      if (page === 0) {
        totalHits = result.nbHits || 0;
        if (totalHits === 0) {
          break;
        }
        log.info(`  Prefix "${prefix}": ${totalHits} results (${result.nbPages} pages)`);
      }

      const hits = result.hits || [];
      if (hits.length === 0) break;

      for (const hit of hits) {
        yield this.mapHit(hit);
      }

      // Check if we've reached the last page
      if (page + 1 >= result.nbPages) break;

      // Check Algolia pagination limit
      if ((page + 1) * HITS_PER_PAGE >= ALGOLIA_MAX_RESULTS) {
        log.warn(`  Prefix "${prefix}": hit Algolia 1000-result cap at page ${page + 1} — some records may be missed`);
        break;
      }

      page++;
    }
  }

  /**
   * Async generator that yields attorney records from the HSBA Algolia API.
   * Overrides BaseScraper.search() entirely since the data source is JSON.
   *
   * Strategy: iterate A–Z by last_name prefix. If a letter has more than
   * ALGOLIA_MAX_RESULTS hits, split into two-letter sub-prefixes (Aa, Ab, ...).
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const allLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
    // maxCities/maxPrefixes limits how many letters we iterate in test mode
    const maxLetters = options.maxPrefixes || options.maxCities || allLetters.length;
    const letters = allLetters.slice(0, maxLetters);
    let totalYielded = 0;

    log.scrape(`Searching HSBA Algolia directory (index: ${ALGOLIA_INDEX}), ${letters.length} letters`);

    // Emit a single city progress since we don't iterate by city
    yield { _cityProgress: { current: 1, total: letters.length } };

    for (let li = 0; li < letters.length; li++) {
      const letter = letters[li];

      // Probe the letter to check result count
      let probe;
      try {
        await rateLimiter.wait();
        probe = await this.algoliaSearch({
          query: letter,
          hitsPerPage: 1,
          filters: 'DataType:User',
          restrictSearchableAttributes: ['last_name'],
        }, rateLimiter);
      } catch (err) {
        log.error(`Failed to probe letter "${letter}": ${err.message}`);
        continue;
      }

      const count = probe.nbHits || 0;
      if (count === 0) {
        log.info(`Letter "${letter}": 0 results — skipping`);
        continue;
      }

      log.scrape(`Letter "${letter}": ${count} total results`);

      if (count > ALGOLIA_MAX_RESULTS) {
        // Split into two-letter sub-prefixes
        log.info(`  Letter "${letter}" exceeds ${ALGOLIA_MAX_RESULTS} — splitting into sub-prefixes`);
        const subLetters = 'abcdefghijklmnopqrstuvwxyz'.split('');
        for (const sub of subLetters) {
          const prefix = letter + sub;
          for await (const attorney of this.fetchPrefix(prefix, rateLimiter, options)) {
            attorney.practice_area = practiceArea || '';
            yield attorney;
            totalYielded++;
          }
        }
      } else {
        // Single letter fits within the Algolia limit
        for await (const attorney of this.fetchPrefix(letter, rateLimiter, options)) {
          attorney.practice_area = practiceArea || '';
          yield attorney;
          totalYielded++;
        }
      }
    }

    log.success(`HSBA Algolia scrape complete — ${totalYielded} records yielded`);
  }
}

module.exports = new HawaiiScraper();
