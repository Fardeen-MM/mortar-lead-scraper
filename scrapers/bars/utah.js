/**
 * Utah State Bar — DNN IFRAME (PLACEHOLDER)
 *
 * The Utah Bar Member Directory at https://services.utahbar.org/Member-Directory
 * uses a DotNetNuke (DNN) portal with an iframe (dnn_ctr423_IFrame_htmIFrame).
 * The actual search form lives inside the iframe and cannot be accessed without
 * a full browser to render the DNN page and extract the iframe content.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class UtahScraper extends BaseScraper {
  constructor() {
    super({
      name: 'utah',
      stateCode: 'UT',
      baseUrl: 'https://services.utahbar.org/Member-Directory',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Salt Lake City', 'West Valley City', 'Provo', 'West Jordan',
        'Orem', 'Sandy', 'Ogden', 'St. George',
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
    log.warn('UT Bar uses a DNN portal with iframe (dnn_ctr423_IFrame_htmIFrame) — cannot scrape without a full browser');
    log.info('Manual search: https://services.utahbar.org/Member-Directory');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new UtahScraper();
