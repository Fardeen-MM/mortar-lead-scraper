/**
 * Prince Edward Island Law Society Scraper
 *
 * Source: https://lawsocietypei.ca/find-a-lawyer
 * Method: PHP JSON API at find-a-lawyer.api.php — cleanest Canadian bar API
 * ~200 lawyers total
 *
 * Overrides search() to query the JSON API directly (no HTML parsing needed).
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class PEIScraper extends BaseScraper {
  constructor() {
    super({
      name: 'pei',
      stateCode: 'CA-PE',
      baseUrl: 'https://lawsocietypei.ca/find-a-lawyer',
      pageSize: 100,
      practiceAreaCodes: {
        'family':                'family',
        'family law':            'family',
        'criminal':              'criminal',
        'criminal defense':      'criminal',
        'real estate':           'real-estate',
        'corporate/commercial':  'corporate-commercial',
        'corporate':             'corporate-commercial',
        'commercial':            'corporate-commercial',
        'personal injury':       'personal-injury',
        'employment':            'employment',
        'labour':                'employment',
        'immigration':           'immigration',
        'estate planning/wills': 'wills-estates',
        'estate planning':       'wills-estates',
        'wills':                 'wills-estates',
        'intellectual property': 'intellectual-property',
        'civil litigation':      'civil-litigation',
        'litigation':            'civil-litigation',
        'tax':                   'tax',
        'administrative':        'administrative',
        'environmental':         'environmental',
      },
      defaultCities: [
        'Charlottetown', 'Summerside',
      ],
    });

    this.apiUrl = 'https://lawsocietypei.ca/find-a-lawyer.api.php';
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for JSON API`);
  }

  /**
   * Async generator that yields attorney records from the PEI JSON API.
   * The API returns all lawyers at once (~200), so we filter client-side
   * by city and practice area.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    // Fetch entire directory from the JSON API
    log.scrape(`Fetching PEI lawyer directory from JSON API`);

    let response;
    try {
      await rateLimiter.wait();
      response = await this.httpGet(this.apiUrl, rateLimiter);
    } catch (err) {
      log.error(`Request failed: ${err.message}`);
      return;
    }

    if (response.statusCode === 429 || response.statusCode === 403) {
      log.warn(`Got ${response.statusCode} from ${this.name}`);
      return;
    }

    if (response.statusCode !== 200) {
      log.error(`Unexpected status ${response.statusCode} — skipping`);
      return;
    }

    rateLimiter.resetBackoff();

    let records;
    try {
      records = JSON.parse(response.body);
    } catch (err) {
      log.error(`Failed to parse JSON response: ${err.message}`);
      return;
    }

    if (!Array.isArray(records)) {
      // Some APIs wrap results in an object
      records = records.lawyers || records.members || records.data || records.results || [];
    }

    log.success(`Fetched ${records.length} total PEI lawyers`);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Filtering: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      const cityLower = city.toLowerCase();
      let cityCount = 0;

      for (const rec of records) {
        // Filter by city (case-insensitive partial match)
        const recCity = (rec.city || rec.location || rec.town || '').toString().trim();
        if (recCity.toLowerCase() !== cityLower &&
            !recCity.toLowerCase().includes(cityLower)) {
          continue;
        }

        // Filter by practice area if specified
        if (practiceCode) {
          const areas = rec.practice_areas || rec.practiceAreas || rec.areas || '';
          const areasStr = Array.isArray(areas) ? areas.join(' ').toLowerCase() : areas.toString().toLowerCase();
          if (!areasStr.includes(practiceCode.toLowerCase())) continue;
        }

        const fullName = (rec.name || rec.full_name || `${rec.first_name || ''} ${rec.last_name || ''}`).trim();
        const { firstName, lastName } = this.splitName(fullName);

        const attorney = {
          first_name: rec.first_name || firstName,
          last_name: rec.last_name || lastName,
          full_name: fullName,
          firm_name: (rec.firm || rec.firm_name || rec.company || '').trim(),
          city: recCity || city,
          state: 'CA-PE',
          phone: (rec.phone || rec.telephone || '').trim(),
          email: (rec.email || '').trim(),
          website: (rec.website || rec.url || '').trim(),
          bar_number: (rec.member_number || rec.bar_number || rec.id || '').toString().trim(),
          bar_status: (rec.status || 'Active').trim(),
          profile_url: rec.profile_url || rec.link || '',
        };

        // Apply min year filter
        if (options.minYear && rec.admission_date) {
          const year = parseInt((rec.admission_date.toString().match(/\d{4}/) || ['0'])[0], 10);
          if (year > 0 && year < options.minYear) continue;
        }

        cityCount++;
        yield this.transformResult(attorney, practiceArea);

        // Respect maxPages (treat each batch of pageSize as a "page")
        if (options.maxPages && cityCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max results limit for ${city}`);
          break;
        }
      }

      if (cityCount === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      } else {
        log.success(`Found ${cityCount} lawyers in ${city}`);
      }
    }
  }
}

module.exports = new PEIScraper();
