/**
 * New Jersey Attorney Search — BLOCKED (Imperva/Incapsula WAF)
 *
 * NJ Courts uses Imperva/Incapsula WAF (formerly Incapsula) which blocks
 * automated access with bot detection that requires JavaScript execution,
 * browser fingerprinting, and cookie challenges. This scraper is a placeholder.
 *
 * The NJ attorney search is a Pega-based web app behind WAF:
 *   https://portalattysearch-cloud.njcourts.gov/prweb/PRServletPublicAuth/app/Attorney/...
 *
 * All endpoints (search page, Pega REST API, etc.) return the Imperva
 * "Pardon Our Interruption" challenge page that requires a real browser.
 *
 * Alternatives investigated and blocked:
 *   - portalattysearch-cloud.njcourts.gov (Imperva WAF)
 *   - portal.njcourts.gov (login required + WAF)
 *   - tcms.njsba.com (NJSBA, 403 Forbidden)
 *
 * This would require Puppeteer/Playwright with stealth plugin to bypass.
 *
 * Manual search: https://portalattysearch-cloud.njcourts.gov/
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class NewJerseyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-jersey',
      stateCode: 'NJ',
      baseUrl: 'https://portalattysearch-cloud.njcourts.gov/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Newark', 'Jersey City', 'Trenton', 'Hackensack', 'Morristown',
        'Cherry Hill', 'New Brunswick', 'Woodbridge', 'Paterson', 'Camden',
        'Toms River', 'Princeton', 'Freehold', 'Somerville',
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
    log.warn('NJ Courts uses Imperva/Incapsula WAF — automated HTTP access is blocked');
    log.info('All NJ endpoints (search page, Pega REST API) return bot challenge pages');
    log.info('To search NJ attorneys manually, visit: https://portalattysearch-cloud.njcourts.gov/');
    yield { _captcha: true, city: 'N/A', page: 0, reason: 'Imperva/Incapsula WAF — requires headless browser with stealth' };
  }
}

module.exports = new NewJerseyScraper();
