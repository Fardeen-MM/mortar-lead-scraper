const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class SaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-sa',
      stateCode: 'AU-SA',
      baseUrl: 'https://www.lawsocietysa.asn.au/Public/Community/Register_Practising_Certificates.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'litigation': 'Litigation',
      },
      defaultCities: ['Adelaide', 'Mount Gambier', 'Port Augusta'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('AU-SA: ASP.NET register requires ViewState inspection');
    yield { _captcha: true, city: 'Adelaide' };
  }
}
module.exports = new SaScraper();
