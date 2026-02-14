/**
 * Wyoming Bar Association Scraper
 *
 * Source: https://www.wyomingbar.org/directory/
 * Status: PLACEHOLDER — WordPress directory plugin, no discoverable API endpoint
 */

const BaseScraper = require('../base-scraper');

class WyomingScraper extends BaseScraper {
  constructor() {
    super({
      name: 'wyoming',
      stateCode: 'WY',
      baseUrl: 'https://www.wyomingbar.org/directory/',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'agricultural':          'Agricultural Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'employment':            'Employment Law',
        'energy':                'Energy Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'mineral':               'Mineral Law',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'water':                 'Water Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Cheyenne', 'Casper', 'Laramie', 'Gillette',
        'Rock Springs', 'Sheridan', 'Green River', 'Jackson',
      ],
    });
  }

  async *search(practiceArea, options = {}) {
    const { log } = require('../../lib/logger');
    log.warn(`${this.stateCode}: WordPress directory plugin — no discoverable API endpoint — placeholder scraper`);
    yield { _captcha: true };
  }
}

module.exports = new WyomingScraper();
