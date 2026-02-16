/**
 * Washington State Bar Association — PLACEHOLDER (requires login)
 *
 * The WSBA Legal Directory at https://www.mywsba.org/PersonifyEbusiness/LegalDirectory.aspx
 * requires Personify SSO authentication. Without valid login credentials,
 * the site redirects in an infinite loop between the directory page and the
 * SSO login page.
 *
 * There is no public API, open data portal, or alternative directory endpoint
 * for Washington State attorney data. The only link from wsba.org points to
 * the authenticated mywsba.org portal.
 *
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
    log.warn('WA: WSBA Legal Directory requires Personify SSO login — cannot scrape without credentials');
    log.info('WA: Manual search at: https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx');
    log.info('WA: No public API or open data alternative available');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new WashingtonScraper();
