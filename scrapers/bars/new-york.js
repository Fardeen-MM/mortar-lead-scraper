/**
 * New York Attorney Registration Scraper
 *
 * Source: https://data.ny.gov/resource/2ea2-qc7r.json (SODA / Socrata Open Data API)
 * Method: HTTP GET returning JSON (no HTML parsing)
 *
 * Uses SoQL query parameters ($limit, $offset, $where) for filtering and pagination.
 * The search() async generator is fully overridden since this is a JSON API, not HTML.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');
const { titleCase } = require('../../lib/normalizer');

class NewYorkScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-york',
      stateCode: 'NY',
      baseUrl: 'https://data.ny.gov/resource/2ea2-qc7r.json',
      pageSize: 1000,
      practiceAreaCodes: {},
      defaultCities: [
        'New York', 'Brooklyn', 'Buffalo', 'Rochester', 'Albany',
        'Syracuse', 'White Plains', 'Garden City', 'Mineola',
        'Staten Island', 'Bronx', 'Melville', 'Uniondale', 'Hauppauge',
      ],
    });
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SODA JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SODA JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SODA JSON API`);
  }

  /**
   * Look up a single attorney by name and city.
   * Used by waterfall Step 4 for cross-reference enrichment.
   *
   * @param {string} firstName
   * @param {string} lastName
   * @param {string} city
   * @param {RateLimiter} rateLimiter
   * @returns {object|null} { phone, firm_name } or null
   */
  async lookupByName(firstName, lastName, city, rateLimiter) {
    if (!firstName || !lastName) return null;

    // Build SoQL query — NY data is ALL CAPS so use upper() for case-insensitive match
    const escapedFirst = firstName.toUpperCase().replace(/'/g, "''");
    const escapedLast = lastName.toUpperCase().replace(/'/g, "''");
    let whereClause = `upper(first_name)='${escapedFirst}' AND upper(last_name)='${escapedLast}'`;
    if (city) {
      whereClause += ` AND upper(city)='${city.toUpperCase().replace(/'/g, "''")}'`;
    }

    const params = new URLSearchParams();
    params.set('$limit', '5');
    params.set('$where', whereClause);
    const url = `${this.baseUrl}?${params.toString()}`;

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(url, rateLimiter);
      if (response.statusCode !== 200) return null;

      const records = JSON.parse(response.body);
      if (!Array.isArray(records) || records.length === 0) return null;

      // Take the first match
      const rec = records[0];
      const result = {};
      if (rec.phone_number) result.phone = rec.phone_number.trim();
      if (rec.company_name) result.firm_name = rec.company_name.trim();
      // NY SODA doesn't have email or website

      return Object.keys(result).length > 0 ? result : null;
    } catch {
      return null;
    }
  }

  /**
   * Async generator that yields attorney records from the NY SODA API.
   * Overrides BaseScraper.search() entirely since the data source is JSON, not HTML.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const limit = this.pageSize;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let offset = 0;
      let pagesFetched = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build SODA query URL
        const whereClause = `city='${city.replace(/'/g, "''")}' AND status='Currently registered'`;
        const params = new URLSearchParams();
        params.set('$limit', String(limit));
        params.set('$offset', String(offset));
        params.set('$where', whereClause);

        const url = `${this.baseUrl}?${params.toString()}`;
        log.info(`Page ${pagesFetched + 1} (offset ${offset}) — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
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
        let records;
        try {
          records = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse JSON response: ${err.message}`);
          break;
        }

        if (!Array.isArray(records) || records.length === 0) {
          if (pagesFetched === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          } else {
            log.success(`Completed all pages for ${city} (${offset} records total)`);
          }
          break;
        }

        if (pagesFetched === 0) {
          log.success(`Fetching results for ${city} (first batch: ${records.length} records)`);
        }

        // Map and yield each attorney record
        for (const rec of records) {
          // NY Socrata data is ALL CAPS — smart title-case names and city

          const attorney = {
            first_name: titleCase(rec.first_name),
            last_name: titleCase(rec.last_name),
            firm_name: (rec.company_name || '').trim(),
            city: titleCase(rec.city),
            state: (rec.state || 'NY').trim(),
            phone: (rec.phone_number || '').trim(),
            email: '',
            bar_number: (rec.registration_number || '').trim(),
            admission_date: (rec.year_admitted || '').trim(),
            bar_status: (rec.status || '').trim(),
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

        // Stop when fewer results than $limit are returned (last page)
        if (records.length < limit) {
          log.success(`Completed all pages for ${city} (${offset + records.length} records total)`);
          break;
        }

        offset += limit;
        pagesFetched++;
      }
    }
  }
}

module.exports = new NewYorkScraper();
