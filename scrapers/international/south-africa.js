/**
 * South Africa Lawyer Directory Scraper
 *
 * Source: Law Society of South Africa (LSSA)
 * URL: https://www.lssa.org.za/
 * Method: Placeholder — the LSSA does not maintain a public online searchable
 *         directory of attorneys. They provide referral services instead.
 *
 * Alternative sources investigated:
 *   - Legal Practice Council (LPC): https://www.lpc.org.za/ — the regulator
 *     has an "Admit and Enrol" system but no public search directory
 *   - Cape Law Society: https://cls.org.za/ — referral service, no public directory
 *   - KwaZulu-Natal Law Society: similar to CLS
 *   - De Rebus (legal journal): https://www.derebus.org.za/ — articles, not directory
 *   - LEAD (Legal Education and Development): training portal, not directory
 *
 * South Africa has ~30,000 practising attorneys.
 *
 * Potential approaches:
 *   1. LPC may expose an API for verification of practitioners
 *   2. Commercial legal directories like Bowmans, ENS may have listings
 *   3. Court roll data could be scraped for attorney names
 *   4. The Yellow Pages (yp.co.za) legal category might have listings
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class SouthAfricaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'south-africa',
      stateCode: 'ZA',
      baseUrl: 'https://www.lssa.org.za/',
      pageSize: 25,
      practiceAreaCodes: {
        'civil':        'Civil Litigation',
        'criminal':     'Criminal',
        'family':       'Family',
        'corporate':    'Corporate & Commercial',
        'tax':          'Tax',
        'employment':   'Labour & Employment',
        'real estate':  'Property & Conveyancing',
        'immigration':  'Immigration',
        'ip':           'Intellectual Property',
        'mining':       'Mining & Resources',
        'environmental':'Environmental',
      },
      defaultCities: ['Johannesburg', 'Cape Town', 'Durban', 'Pretoria', 'Port Elizabeth'],
    });
  }

  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }

  async *search() {
    log.warn('ZA: Law Society of South Africa does not have a public online directory');
    log.info('ZA: To implement, investigate the Legal Practice Council (LPC) at lpc.org.za');
    log.info('ZA: Alternative: commercial directories or court roll data');
    yield { _captcha: true, city: 'Johannesburg' };
  }
}

module.exports = new SouthAfricaScraper();
