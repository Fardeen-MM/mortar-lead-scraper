/**
 * Oklahoma Bar Association Scraper
 *
 * Source: https://ams.okbar.org/eweb/startpage.aspx?site=FALWEB
 * Status: PLACEHOLDER — eWeb/iMIS platform, form field names require browser inspection
 */

const BaseScraper = require('../base-scraper');

class OklahomaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'oklahoma',
      stateCode: 'OK',
      baseUrl: 'https://ams.okbar.org/eweb/startpage.aspx?site=FALWEB',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business/Commercial',
        'civil litigation':      'Civil Litigation',
        'civil rights':          'Civil Rights',
        'collections':           'Collections',
        'corporate':             'Corporate',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'elder':                 'Elder Law',
        'employment':            'Employment/Labor',
        'labor':                 'Employment/Labor',
        'environmental':         'Environmental',
        'estate planning':       'Estate Planning/Probate',
        'estate':                'Estate Planning/Probate',
        'family':                'Family',
        'family law':            'Family',
        'government':            'Government',
        'health':                'Health Care',
        'immigration':           'Immigration',
        'insurance':             'Insurance',
        'intellectual property': 'Intellectual Property',
        'juvenile':              'Juvenile',
        'mediation':             'Mediation/ADR',
        'military':              'Military Law',
        'oil and gas':           'Oil & Gas/Energy',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'securities':            'Securities',
        'social security':       'Social Security',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'tribal':                'Tribal/Indian Law',
        'water law':             'Water Rights',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Oklahoma City', 'Tulsa', 'Norman', 'Edmond',
        'Moore', 'Midwest City', 'Broken Arrow', 'Lawton',
      ],
    });
  }

  async *search(practiceArea, options = {}) {
    const { log } = require('../../lib/logger');
    log.warn(`${this.stateCode}: eWeb/iMIS platform — form field names require browser inspection — placeholder scraper`);
    yield { _captcha: true };
  }
}

module.exports = new OklahomaScraper();
