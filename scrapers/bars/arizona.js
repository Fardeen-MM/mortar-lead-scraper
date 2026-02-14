/**
 * Arizona State Bar Association — API REQUIRES AUTH (PLACEHOLDER)
 *
 * The AZ Bar API at https://api-proxy.azbar.org/MemberSearch/Search returns
 * 401 Unauthorized. Without an API key we cannot access the search endpoint.
 * The public-facing page is at https://www.azbar.org/find-a-lawyer/.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class ArizonaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'arizona',
      stateCode: 'AZ',
      baseUrl: 'https://www.azbar.org/find-a-lawyer/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Phoenix', 'Tucson', 'Mesa', 'Scottsdale', 'Chandler',
        'Tempe', 'Gilbert', 'Glendale', 'Peoria', 'Flagstaff',
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
    log.warn('AZ Bar API (api-proxy.azbar.org) returns 401 Unauthorized — no public API key available');
    log.info('Manual search: https://www.azbar.org/find-a-lawyer/');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new ArizonaScraper();
