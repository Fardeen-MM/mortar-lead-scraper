/**
 * Kentucky Bar Association — IFRAME SEARCH (PLACEHOLDER)
 *
 * Kentucky's Find-A-Lawyer uses an iframe that embeds the actual search form.
 * Without the iframe source URL the search cannot be reliably automated.
 * Results are noted as "randomly picked by the system."
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class KentuckyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'kentucky',
      stateCode: 'KY',
      baseUrl: 'https://www.kybar.org/page/FindaLawyer',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Louisville', 'Lexington', 'Bowling Green', 'Owensboro',
        'Covington', 'Richmond', 'Georgetown', 'Florence',
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
    log.warn('KY Bar Find-A-Lawyer uses an iframe — cannot scrape without the iframe source URL or a full browser');
    log.info('Manual search: https://www.kybar.org/page/FindaLawyer');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new KentuckyScraper();
