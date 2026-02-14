/**
 * Rhode Island Judiciary — PLACEHOLDER
 *
 * The RI Judiciary attorney search at rijrs.courts.ri.gov returns data
 * but the response HTML structure is unknown and the guessed selectors
 * parse 0 results. Until the actual response format is reverse-engineered,
 * this scraper is a placeholder.
 *
 * Manual search: https://rijrs.courts.ri.gov/rijrs/attorney.do
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class RhodeIslandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'rhode_island',
      stateCode: 'RI',
      baseUrl: 'https://rijrs.courts.ri.gov/rijrs/attorney.do',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'ADMIN',
        'bankruptcy':            'BANKR',
        'business':              'BUS',
        'civil litigation':      'CIVIL',
        'commercial':            'COMM',
        'corporate':             'CORP',
        'criminal':              'CRIM',
        'criminal defense':      'CRIM',
        'elder':                 'ELDER',
        'employment':            'EMPL',
        'labor':                 'EMPL',
        'environmental':         'ENVIR',
        'estate planning':       'ESTATE',
        'estate':                'ESTATE',
        'family':                'FAMILY',
        'family law':            'FAMILY',
        'immigration':           'IMMIG',
        'intellectual property': 'IP',
        'personal injury':       'PI',
        'real estate':           'REAL',
        'tax':                   'TAX',
        'tax law':               'TAX',
        'workers comp':          'WC',
      },
      defaultCities: [
        'Providence', 'Warwick', 'Cranston', 'Pawtucket',
        'East Providence', 'Woonsocket', 'Newport', 'Westerly',
      ],
    });
  }

  buildSearchUrl() {
    return this.baseUrl;
  }

  parseResultsPage() {
    return [];
  }

  extractResultCount() {
    return 0;
  }

  async *search() {
    log.warn('RI Judiciary attorney search response format is unknown — scraper is a placeholder');
    log.info('To search RI attorneys manually, visit: https://rijrs.courts.ri.gov/rijrs/attorney.do');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new RhodeIslandScraper();
