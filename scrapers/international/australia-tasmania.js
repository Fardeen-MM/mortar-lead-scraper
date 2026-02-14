const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class TasmaniaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-tasmania',
      stateCode: 'AU-TAS',
      baseUrl: 'https://www.lst.org.au/find-a-lawyer/',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
      },
      defaultCities: ['Hobart', 'Launceston', 'Devonport', 'Burnie'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-TAS: WordPress Search Filter requires AJAX field discovery');
    yield { _captcha: true, city: 'Hobart' };
  }
}
module.exports = new TasmaniaScraper();
