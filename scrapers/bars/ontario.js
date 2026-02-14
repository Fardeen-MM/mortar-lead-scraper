/**
 * Ontario Law Society -- SITE CONNECTIVITY ISSUES
 *
 * The lawyerandparalegal.directory domain has SSL/TLS errors (EPROTO / ECONNREFUSED).
 * The site appears to be down or has certificate issues preventing connections.
 * This scraper exists as a placeholder until the site is accessible again.
 *
 * Last checked: 2025 -- SSL EPROTO errors on all connection attempts.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class OntarioScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ontario',
      stateCode: 'CA-ON',
      baseUrl: 'https://lawyerandparalegal.directory',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Toronto', 'Ottawa', 'Mississauga', 'Hamilton', 'Brampton',
        'London', 'Markham', 'Vaughan', 'Kitchener', 'Windsor',
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
    log.warn('LSO directory (lawyerandparalegal.directory) has SSL/connectivity issues -- site unreachable');
    log.info('The domain returns EPROTO / ECONNREFUSED errors on connection attempts');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new OntarioScraper();
