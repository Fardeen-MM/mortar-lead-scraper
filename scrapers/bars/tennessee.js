/**
 * Tennessee Board of Professional Responsibility — PLACEHOLDER
 *
 * The TBPR online attorney directory at tbpr.org returns 200 but the
 * response HTML structure is unknown and the guessed selectors
 * (table.directory-results tr) parse 0 results. Until the actual
 * response format is reverse-engineered, this scraper is a placeholder.
 *
 * Manual search: https://www.tbpr.org/for-the-public/online-attorney-directory
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class TennesseeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'tennessee',
      stateCode: 'TN',
      baseUrl: 'https://www.tbpr.org/for-the-public/online-attorney-directory',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'labor',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
        'adoption':             'adoption',
      },
      defaultCities: [
        'Nashville', 'Memphis', 'Knoxville', 'Chattanooga',
        'Clarksville', 'Murfreesboro', 'Franklin', 'Jackson',
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
    log.warn('TN TBPR directory response format is unknown — scraper is a placeholder');
    log.info('To search TN attorneys manually, visit: https://www.tbpr.org/for-the-public/online-attorney-directory');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new TennesseeScraper();
