/**
 * New Hampshire Bar — NO PUBLIC DIRECTORY
 *
 * The NH Bar Association requires login to search members.
 * Attorney status verification: call (603) 229-0002
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NewHampshireScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new_hampshire',
      stateCode: 'NH',
      baseUrl: 'https://member.nhbar.org/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: ['Concord', 'Manchester', 'Nashua', 'Portsmouth', 'Dover', 'Keene', 'Laconia', 'Lebanon'],
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
    log.warn('NH Bar Association requires login — no public directory available');
    log.info('To verify attorney status, call (603) 229-0002');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new NewHampshireScraper();
