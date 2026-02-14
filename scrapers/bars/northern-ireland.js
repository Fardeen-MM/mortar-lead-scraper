/**
 * Law Society of Northern Ireland -- NO WORKING API FOUND
 *
 * The NI Law Society solicitor search at lawsoc-ni.org/using-a-solicitor/find-a-solicitor
 * uses client-side rendering (Leaflet/Mapbox). Multiple attempted AJAX endpoints
 * all returned 404. The actual data source for the directory has not been identified.
 *
 * The solicitor search may also be available at lawsoc-ni.org/solicitors.aspx
 * but this has not been verified as a working scraping target.
 *
 * This scraper exists as a placeholder until a working endpoint is found.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NorthernIrelandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'northern-ireland',
      stateCode: 'UK-NI',
      baseUrl: 'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Belfast', 'Derry', 'Londonderry', 'Lisburn', 'Newry',
        'Bangor', 'Craigavon', 'Ballymena', 'Newtownabbey',
        'Omagh', 'Enniskillen',
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
    log.warn('NI Law Society directory uses client-side rendering -- no working API endpoint found');
    log.info('Attempted AJAX endpoints at lawsoc-ni.org all returned 404');
    log.info('Possible alternative: lawsoc-ni.org/solicitors.aspx (not yet verified)');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new NorthernIrelandScraper();
