/**
 * Spain Lawyer Directory Scraper
 *
 * Source: Consejo General de la Abogacía Española (CGAE) — Census of Lawyers
 * URL: https://www.abogacia.es/encuentre-abogado/
 * Method: Placeholder — the CGAE directory uses a JavaScript-rendered search
 *         application (Liferay/Angular) that cannot be scraped with HTTP alone.
 *
 * Alternative sources investigated:
 *   - ICAM (Madrid bar): https://www.icam.es/ — Liferay SPA, requires JS
 *   - ICAB (Barcelona bar): https://www.icab.cat/ — requires member login
 *   - RedAbogacia: https://www.redabogacia.org/ — requires authentication
 *
 * To make this scraper work, one of these approaches is needed:
 *   1. Headless browser (Puppeteer/Playwright) to render the CGAE search app
 *   2. Reverse-engineer the CGAE API endpoints from browser network tab
 *   3. Find an alternative open data source for Spanish lawyers
 *
 * Spain has ~150,000 registered lawyers across 83 Colegios de Abogados.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');

class SpainScraper extends BaseScraper {
  constructor() {
    super({
      name: 'spain',
      stateCode: 'ES',
      baseUrl: 'https://www.abogacia.es/encuentre-abogado/',
      pageSize: 25,
      practiceAreaCodes: {
        'civil':          'Derecho Civil',
        'criminal':       'Derecho Penal',
        'family':         'Derecho de Familia',
        'employment':     'Derecho Laboral',
        'corporate':      'Derecho Mercantil',
        'administrative': 'Derecho Administrativo',
        'tax':            'Derecho Fiscal',
        'immigration':    'Derecho de Extranjería',
        'real estate':    'Derecho Inmobiliario',
        'ip':             'Propiedad Intelectual',
      },
      defaultCities: ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao', 'Málaga'],
    });
  }

  buildSearchUrl() { throw new Error('Not implemented'); }
  parseResultsPage() { return []; }
  extractResultCount() { return 0; }

  async *search() {
    log.warn('ES: CGAE "Encuentre Abogado" requires JavaScript rendering (Liferay/Angular SPA)');
    log.info('ES: To implement, use Puppeteer or reverse-engineer the CGAE search API');
    log.info('ES: Alternative: check if regional bars (ICAM Madrid, ICAB Barcelona) have public APIs');
    yield { _captcha: true, city: 'Madrid' };
  }
}

module.exports = new SpainScraper();
