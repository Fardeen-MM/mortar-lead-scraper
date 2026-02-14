const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class GermanyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'germany',
      stateCode: 'DE-BRAK',
      baseUrl: 'https://bravsearch.bea-brak.de/bravsearch/',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'corporate': 'Corporate',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'tax': 'Tax',
        'intellectual property': 'Intellectual Property',
        'real estate': 'Real Estate',
      },
      defaultCities: ['Berlin', 'München', 'Hamburg', 'Frankfurt', 'Köln', 'Düsseldorf', 'Stuttgart', 'Leipzig'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('DE-BRAK: German BRAK register requires API discovery');
    yield { _captcha: true, city: 'Berlin' };
  }
}
module.exports = new GermanyScraper();
