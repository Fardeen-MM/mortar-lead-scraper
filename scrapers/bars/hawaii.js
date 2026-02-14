/**
 * Hawaii State Bar Association — ALGOLIA KEY NOT FOUND (PLACEHOLDER)
 *
 * The HSBA Find-a-Lawyer at https://hsba.org/find-a-lawyer is powered by Algolia
 * (App ID: PE7QKUXU6Z), but the search API key could not be discovered from the
 * page source or JS bundles. Without the key we cannot query the Algolia index.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class HawaiiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'hawaii',
      stateCode: 'HI',
      baseUrl: 'https://hsba.org/find-a-lawyer',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Honolulu', 'Hilo', 'Kailua', 'Pearl City',
        'Waipahu', 'Kaneohe', 'Kapolei', 'Wailuku',
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
    log.warn('HSBA Algolia search API key could not be discovered — cannot query directory');
    log.info('Manual search: https://hsba.org/find-a-lawyer');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new HawaiiScraper();
