/**
 * Montana State Bar Association Scraper
 *
 * Source: https://www.licensedlawyer.org/mt
 * Status: PLACEHOLDER — LicensedLawyer.org requires JavaScript rendering
 *
 * Montana's public attorney search has moved to LicensedLawyer.org.
 * The old montanabar.org/cv5/cgi-bin/utilities.dll endpoint is for member
 * dashboards, not public search. The LicensedLawyer.org site is a JavaScript
 * SPA that cannot be scraped with simple HTTP requests.
 *
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class MontanaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'montana',
      stateCode: 'MT',
      baseUrl: 'https://www.licensedlawyer.org/mt',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Billings', 'Missoula', 'Great Falls', 'Bozeman',
        'Helena', 'Butte', 'Kalispell', 'Whitefish',
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
    log.warn('Montana attorney search has moved to LicensedLawyer.org which requires JavaScript rendering');
    log.info('Visit https://www.licensedlawyer.org/mt to search manually');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new MontanaScraper();
