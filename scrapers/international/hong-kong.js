const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class HongKongScraper extends BaseScraper {
  constructor() {
    super({
      name: 'hong-kong',
      stateCode: 'HK',
      baseUrl: 'https://www.hklawsoc.org.hk/en/serve-the-public/find-a-lawyer',
      pageSize: 25,
      practiceAreaCodes: {
        'corporate': 'Corporate',
        'banking': 'Banking',
        'litigation': 'Litigation',
        'family': 'Family',
        'criminal': 'Criminal',
        'intellectual property': 'Intellectual Property',
        'real estate': 'Real Estate',
        'maritime': 'Maritime',
      },
      defaultCities: ['Hong Kong', 'Kowloon', 'New Territories'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('HK: Law Society of Hong Kong search requires form inspection');
    yield { _captcha: true, city: 'Hong Kong' };
  }
}
module.exports = new HongKongScraper();
