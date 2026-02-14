/**
 * Northwest Territories Law Society -- THENTIA CLOUD JS APP
 *
 * The NWT Law Society register at lsnt.ca.thentiacloud.net redirects to
 * /webs/lsnt/register/# which is a client-side JavaScript application.
 * There is no server-rendered HTML or discoverable public API to scrape.
 * This scraper exists as a placeholder until a working API endpoint is found.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NorthwestTerritoriesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'northwest-territories',
      stateCode: 'CA-NT',
      baseUrl: 'https://lsnt.ca.thentiacloud.net/webs/lsnt/register/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: ['Yellowknife'],
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
    log.warn('NWT Law Society register is a Thentia Cloud JS app -- no scrapable endpoint found');
    log.info('The site redirects to /webs/lsnt/register/# which requires JavaScript execution');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new NorthwestTerritoriesScraper();
