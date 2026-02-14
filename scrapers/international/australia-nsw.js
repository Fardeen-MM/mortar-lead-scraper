const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NswScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-nsw',
      stateCode: 'AU-NSW',
      baseUrl: 'https://www.lawsociety.com.au/register-of-solicitors',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property/conveyancing': 'Property/Conveyancing',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'litigation': 'Litigation',
        'personal injury': 'Personal Injury',
      },
      defaultCities: ['Sydney', 'Newcastle', 'Wollongong', 'Parramatta'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-NSW: Register of Solicitors requires form inspection');
    yield { _captcha: true, city: 'Sydney' };
  }
}
module.exports = new NswScraper();
