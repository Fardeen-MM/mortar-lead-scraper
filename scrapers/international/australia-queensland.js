const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class QueenslandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-queensland',
      stateCode: 'AU-QLD',
      baseUrl: 'https://www.youandthelaw.com.au/directory',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'personal injury': 'Personal Injury',
      },
      defaultCities: ['Brisbane', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-QLD: Kentico CMS directory requires API discovery');
    yield { _captcha: true, city: 'Brisbane' };
  }
}
module.exports = new QueenslandScraper();
