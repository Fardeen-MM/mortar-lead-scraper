/**
 * Nova Scotia Barristers' Society -- REQUIRES LOGIN
 *
 * The NSBS member search at members.nsbs.org requires authentication.
 * There is no public directory available without login credentials.
 * This scraper exists as a placeholder to show the province in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NovaScotiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nova-scotia',
      stateCode: 'CA-NS',
      baseUrl: 'https://members.nsbs.org/NSBSMemberSearch/Search_Page.aspx',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: ['Halifax', 'Dartmouth', 'Sydney', 'Truro', 'New Glasgow'],
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
    log.warn('NSBS member search requires login -- no public directory available');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new NovaScotiaScraper();
