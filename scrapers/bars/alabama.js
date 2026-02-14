/**
 * Alabama State Bar Association — COMPLEX ASP.NET AJAX (PLACEHOLDER)
 *
 * The Alabama Bar member search at
 * https://members.alabar.org/Member_Portal/Member_Portal/Member-Search.aspx?hkey=36376536-98e5-4861-b687-491bf902ab2d
 * uses ASP.NET AJAX UpdatePanels with Telerik controls (RadGrid).
 * The form has fields: First Name, Last Name, City, County, State, Practice Area.
 * However, the complex AJAX with UpdatePanels and Telerik controls does not
 * work with simple HTTP POST — it requires full browser-level JS execution.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class AlabamaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alabama',
      stateCode: 'AL',
      baseUrl: 'https://members.alabar.org/Member_Portal/Member_Portal/Member-Search.aspx?hkey=36376536-98e5-4861-b687-491bf902ab2d',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Birmingham', 'Montgomery', 'Huntsville', 'Mobile',
        'Tuscaloosa', 'Hoover', 'Dothan', 'Auburn',
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
    log.warn('AL Bar uses ASP.NET AJAX UpdatePanels with Telerik controls — cannot scrape without a full browser');
    log.info('Manual search: https://members.alabar.org/Member_Portal/Member_Portal/Member-Search.aspx?hkey=36376536-98e5-4861-b687-491bf902ab2d');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new AlabamaScraper();
