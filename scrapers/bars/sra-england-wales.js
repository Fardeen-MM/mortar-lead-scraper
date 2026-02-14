/**
 * Solicitors Regulation Authority (SRA) -- England & Wales -- WRONG API ENDPOINT
 *
 * The previously used endpoint /consumers/register/setfilter returns 404.
 * The SRA has a developer portal at https://sra-prod-apim.developer.azure-api.net/
 * which may require an API key for access.
 *
 * This scraper exists as a placeholder until the correct public API endpoint
 * is identified and tested.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class SRAEnglandWalesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'sra-england-wales',
      stateCode: 'UK-EW',
      baseUrl: 'https://www.sra.org.uk/consumers/register/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'London', 'Manchester', 'Birmingham', 'Leeds', 'Bristol',
        'Liverpool', 'Sheffield', 'Newcastle', 'Nottingham', 'Cambridge',
        'Oxford', 'Reading', 'Southampton',
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
    log.warn('SRA register API endpoint /consumers/register/setfilter returns 404');
    log.info('The SRA developer portal (Azure API Management) may require an API key');
    log.info('See: https://sra-prod-apim.developer.azure-api.net/');
    yield { _captcha: true, city: 'N/A', page: 0 };
  }
}

module.exports = new SRAEnglandWalesScraper();
