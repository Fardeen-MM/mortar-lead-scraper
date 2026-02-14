const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NewZealandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-zealand',
      stateCode: 'NZ',
      baseUrl: 'https://www.lawsociety.org.nz/for-the-community/find-a-lawyer/',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'litigation': 'Litigation',
      },
      defaultCities: ['Auckland', 'Wellington', 'Christchurch', 'Hamilton', 'Tauranga', 'Dunedin'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('NZ: Law Society of NZ search requires form inspection');
    yield { _captcha: true, city: 'Auckland' };
  }
}
module.exports = new NewZealandScraper();
