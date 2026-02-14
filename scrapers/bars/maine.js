/**
 * Maine Board of Overseers of the Bar — PLACEHOLDER
 *
 * The Maine attorney directory CGI script at apps.web.maine.gov returns
 * data but the response HTML structure is unknown and the guessed
 * selectors/field names parse 0 results. Until the actual response
 * format and form field names are reverse-engineered, this scraper
 * is a placeholder.
 *
 * Manual search: https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class MaineScraper extends BaseScraper {
  constructor() {
    super({
      name: 'maine',
      stateCode: 'ME',
      baseUrl: 'https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl',
      pageSize: 100,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BAN',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'corporate':              'COR',
        'criminal':               'CRI',
        'criminal defense':       'CRI',
        'elder':                  'ELD',
        'employment':             'EMP',
        'environmental':          'ENV',
        'estate planning':        'EST',
        'family':                 'FAM',
        'family law':             'FAM',
        'general practice':       'GEN',
        'immigration':            'IMM',
        'insurance':              'INS',
        'intellectual property':  'IPR',
        'labor':                  'LAB',
        'maritime':               'MAR',
        'medical malpractice':    'MED',
        'personal injury':        'PIN',
        'probate':                'PRO',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Portland', 'Lewiston', 'Bangor', 'South Portland',
        'Auburn', 'Augusta', 'Biddeford', 'Scarborough',
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
    log.warn('ME attorney directory response format is unknown — scraper is a placeholder');
    log.info('To search ME attorneys manually, visit: https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new MaineScraper();
