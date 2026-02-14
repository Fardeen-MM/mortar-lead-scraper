const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class SingaporeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'singapore',
      stateCode: 'SG',
      baseUrl: 'https://www.lawsociety.org.sg/our-members/find-a-lawyer/',
      pageSize: 25,
      practiceAreaCodes: {
        'corporate': 'Corporate',
        'banking': 'Banking',
        'dispute resolution': 'Dispute Resolution',
        'family': 'Family',
        'criminal': 'Criminal',
        'intellectual property': 'Intellectual Property',
        'maritime': 'Maritime',
        'real estate': 'Real Estate',
      },
      defaultCities: ['Singapore'],
    });
  }
  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }
  async *search() {
    log.warn('SG: Law Society of Singapore search requires form inspection');
    yield { _captcha: true, city: 'Singapore' };
  }
}
module.exports = new SingaporeScraper();
