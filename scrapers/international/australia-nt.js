const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NtScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-nt',
      stateCode: 'AU-NT',
      baseUrl: 'https://lawsocietynt.asn.au/for-the-community/find-a-lawyer.html',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
      },
      defaultCities: ['Darwin', 'Alice Springs', 'Katherine'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-NT: Law Society NT requires form inspection');
    yield { _captcha: true, city: 'Darwin' };
  }
}
module.exports = new NtScraper();
