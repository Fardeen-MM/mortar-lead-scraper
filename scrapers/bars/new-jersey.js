/**
 * New Jersey Attorney Search — BLOCKED (Incapsula WAF)
 *
 * NJ Courts uses Incapsula/Imperva WAF which blocks automated access.
 * The WAF requires JavaScript execution and browser fingerprinting that
 * cannot be satisfied by HTTP-only scrapers. This scraper is a placeholder.
 *
 * Manual search: https://portalattysearch-cloud.njcourts.gov/
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NewJerseyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-jersey',
      stateCode: 'NJ',
      baseUrl: 'https://portalattysearch-cloud.njcourts.gov/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Newark', 'Jersey City', 'Trenton', 'Hackensack', 'Morristown',
        'Cherry Hill', 'New Brunswick', 'Woodbridge', 'Paterson', 'Camden',
        'Toms River', 'Princeton', 'Freehold', 'Somerville',
      ],
    });
  }

  buildSearchUrl() {
    return this.baseUrl;
  }

  parseResultsPage() {
    return [];
  }

  extractResultCount() {
    return 0;
  }

  async *search() {
    log.warn('NJ Courts uses Incapsula WAF — automated access is blocked');
    log.info('To search NJ attorneys manually, visit: https://portalattysearch-cloud.njcourts.gov/');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new NewJerseyScraper();
