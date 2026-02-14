/**
 * Arkansas Bar Association — DNS NOT FOUND (PLACEHOLDER)
 *
 * The AR AOC attorney search at https://attorneyinfo.aoc.arkansas.gov/info/attorney/attorneysearch.aspx
 * does not resolve (DNS failure). The site appears to be down or moved.
 * This scraper exists as a placeholder to show the state in the UI.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class ArkansasScraper extends BaseScraper {
  constructor() {
    super({
      name: 'arkansas',
      stateCode: 'AR',
      baseUrl: 'https://attorneyinfo.aoc.arkansas.gov/info/attorney/attorneysearch.aspx',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Little Rock', 'Fort Smith', 'Fayetteville', 'Springdale',
        'Jonesboro', 'Conway', 'Rogers', 'Pine Bluff',
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
    log.warn('AR AOC attorney search domain (attorneyinfo.aoc.arkansas.gov) does not resolve — site is down or moved');
    log.info('Previous URL: https://attorneyinfo.aoc.arkansas.gov/info/attorney/attorneysearch.aspx');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new ArkansasScraper();
