/**
 * Washington State Bar Association — TOO MANY REDIRECTS (PLACEHOLDER)
 *
 * The WSBA Legal Directory at https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx
 * redirects excessively, preventing automated access.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class WashingtonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'washington',
      stateCode: 'WA',
      baseUrl: 'https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue',
        'Kent', 'Everett', 'Renton', 'Olympia', 'Kirkland',
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
    log.warn('WSBA mywsba.org has too many redirects — cannot scrape without a full browser session');
    log.info('Manual search: https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new WashingtonScraper();
