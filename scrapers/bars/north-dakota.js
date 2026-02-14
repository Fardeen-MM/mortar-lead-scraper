/**
 * State Bar Association of North Dakota Scraper
 *
 * Source: https://www.sband.org/page/FindaLawyer
 * Status: PLACEHOLDER — CMS-based search, form field names unknown
 */

const BaseScraper = require('../base-scraper');

class NorthDakotaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'north-dakota',
      stateCode: 'ND',
      baseUrl: 'https://www.sband.org/page/FindaLawyer',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative',
        'agricultural':           'Agricultural',
        'appellate':              'Appellate',
        'banking':                'Banking & Finance',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate',
        'criminal':               'Criminal',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment',
        'energy':                 'Energy & Natural Resources',
        'environmental':          'Environmental',
        'estate planning':        'Estate Planning',
        'family':                 'Family',
        'family law':             'Family',
        'general practice':       'General Practice',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor',
        'personal injury':        'Personal Injury',
        'probate':                'Probate & Trust',
        'real estate':            'Real Estate',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'tribal law':             'Tribal Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Fargo', 'Bismarck', 'Grand Forks', 'Minot',
        'West Fargo', 'Williston', 'Dickinson', 'Mandan',
      ],
    });
  }

  async *search(practiceArea, options = {}) {
    const { log } = require('../../lib/logger');
    log.warn(`${this.stateCode}: CMS-based search — form field names unknown — placeholder scraper`);
    yield { _captcha: true };
  }
}

module.exports = new NorthDakotaScraper();
