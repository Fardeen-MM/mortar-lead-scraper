const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class ActScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-act',
      stateCode: 'AU-ACT',
      baseUrl: 'https://www.actlawsociety.asn.au/find-a-lawyer',
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
      defaultCities: ['Canberra'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-ACT: ACT Law Society requires form inspection');
    yield { _captcha: true, city: 'Canberra' };
  }
}
module.exports = new ActScraper();
