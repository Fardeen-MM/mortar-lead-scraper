/**
 * Alaska Bar Association — PLACEHOLDER
 *
 * The Alaska Bar uses a CV5 member directory system. AJAX requests to
 * memberdll.dll/List return 200 but the response format differs from
 * what the parser expects, resulting in 0 parsed results. The CV5
 * system may also require a session cookie from visiting the search
 * page first. Until the actual response format is reverse-engineered,
 * this scraper is a placeholder.
 *
 * Manual search: https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class AlaskaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alaska',
      stateCode: 'AK',
      baseUrl: 'https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BKR',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'corporate':              'COR',
        'criminal':               'CRM',
        'criminal defense':       'CRM',
        'elder':                  'ELD',
        'employment':             'EMP',
        'environmental':          'ENV',
        'estate planning':        'EST',
        'family':                 'FAM',
        'family law':             'FAM',
        'general practice':       'GEN',
        'immigration':            'IMM',
        'intellectual property':  'IPR',
        'labor':                  'LAB',
        'medical malpractice':    'MED',
        'native law':             'NAT',
        'oil and gas':            'OIL',
        'personal injury':        'PIN',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Anchorage', 'Fairbanks', 'Juneau', 'Wasilla',
        'Sitka', 'Kenai', 'Palmer', 'Kodiak',
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
    log.warn('AK Bar CV5 directory response format is unknown — scraper is a placeholder');
    log.info('To search AK attorneys manually, visit: https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new AlaskaScraper();
