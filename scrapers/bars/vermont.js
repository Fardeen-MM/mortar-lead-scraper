/**
 * Vermont Bar Association — REQUIRES MEMBER LOGIN
 *
 * The VBA Lawyer Directory is for VBA members only and requires login.
 * Source: https://www.vtbar.org/online-directory/
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class VermontScraper extends BaseScraper {
  constructor() {
    super({
      name: 'vermont',
      stateCode: 'VT',
      baseUrl: 'https://www.vtbar.org/online-directory/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Burlington', 'South Burlington', 'Montpelier', 'Rutland',
        'Barre', 'St. Albans', 'Winooski', 'Brattleboro',
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
    log.warn('VT Bar Association Lawyer Directory requires member login — no public directory available');
    log.info('The Lawyer Directory is for VBA members only');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new VermontScraper();
