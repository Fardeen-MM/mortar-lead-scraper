/**
 * State Bar of Nevada Scraper
 *
 * Source: https://members.nvbar.org/cvweb/cgi-bin/memberdll.dll/info?WRP=lrs_referralNew.htm
 * Status: PLACEHOLDER — CV5 memberdll system, tilde-delimited params not working
 */

const BaseScraper = require('../base-scraper');

class NevadaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nevada',
      stateCode: 'NV',
      baseUrl: 'https://members.nvbar.org/cvweb/cgi-bin/memberdll.dll/info?WRP=lrs_referralNew.htm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'appellate':              'Appellate',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'collections':            'Collections',
        'construction':           'Construction Law',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment Law',
        'entertainment':          'Entertainment & Gaming Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'gaming':                 'Gaming Law',
        'general practice':       'General Practice',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor Law',
        'mining':                 'Mining & Natural Resources',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'water law':              'Water Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Las Vegas', 'Henderson', 'Reno', 'North Las Vegas',
        'Sparks', 'Carson City', 'Elko', 'Mesquite',
      ],
    });
  }

  async *search(practiceArea, options = {}) {
    const { log } = require('../../lib/logger');
    log.warn(`${this.stateCode}: CV5 memberdll system — tilde-delimited params not working — placeholder scraper`);
    yield { _captcha: true };
  }
}

module.exports = new NevadaScraper();
