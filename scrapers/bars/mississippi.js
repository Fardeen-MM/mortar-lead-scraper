/**
 * Mississippi Bar Association — BLOCKED (403 Forbidden)
 *
 * The MS Bar Roll search at courts.ms.gov/bar/barroll/brsearch.php
 * returns 403 Forbidden for automated requests. The site blocks
 * non-browser access. This scraper is a placeholder until a
 * workaround is found.
 *
 * Manual search: https://courts.ms.gov/bar/barroll/brsearch.php
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class MississippiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'mississippi',
      stateCode: 'MS',
      baseUrl: 'https://courts.ms.gov/bar/barroll/brsearch.php',
      pageSize: 100,
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
        'labor':                'employment',
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
      },
      defaultCities: [
        'Jackson', 'Gulfport', 'Hattiesburg', 'Tupelo',
        'Meridian', 'Biloxi', 'Oxford', 'Southaven',
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
    log.warn('MS Bar Roll returns 403 Forbidden — automated access is blocked');
    log.info('To search MS attorneys manually, visit: https://courts.ms.gov/bar/barroll/brsearch.php');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new MississippiScraper();
