/**
 * Iowa State Bar Association Scraper
 *
 * Source: https://www.iowabar.org/?pg=findalawyerdirectory
 * Status: PLACEHOLDER — Lucee CMS form, actual field names unknown without browser inspection
 */

const BaseScraper = require('../base-scraper');

class IowaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'iowa',
      stateCode: 'IA',
      baseUrl: 'https://www.iowabar.org/?pg=findalawyerdirectory',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'agricultural':           'Agricultural Law',
        'appellate':              'Appellate Practice',
        'banking':                'Banking Law',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'government':             'Government',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor Law',
        'mediation':              'Mediation/Arbitration',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax Law',
        'tax law':                'Tax Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Des Moines', 'Cedar Rapids', 'Davenport', 'Sioux City',
        'Iowa City', 'Waterloo', 'Ames', 'Dubuque',
      ],
    });
  }

  async *search(practiceArea, options = {}) {
    const { log } = require('../../lib/logger');
    log.warn(`${this.stateCode}: Lucee CMS form — actual field names unknown without browser inspection — placeholder scraper`);
    yield { _captcha: true };
  }
}

module.exports = new IowaScraper();
