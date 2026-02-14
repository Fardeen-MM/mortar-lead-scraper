const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class IrelandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ireland',
      stateCode: 'IE',
      baseUrl: 'https://www.lawsociety.ie/Find-a-Solicitor/',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'conveyancing/property': 'Conveyancing/Property',
        'corporate': 'Corporate',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'litigation': 'Litigation',
      },
      defaultCities: ['Dublin', 'Cork', 'Galway', 'Limerick', 'Waterford', 'Kilkenny'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('IE: Law Society of Ireland search requires form inspection');
    yield { _captcha: true, city: 'Dublin' };
  }
}
module.exports = new IrelandScraper();
