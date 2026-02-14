/**
 * California State Bar Scraper
 *
 * Source: https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch
 * Method: HTTP GET + Cheerio (results are server-rendered HTML)
 *
 * Uses the Advanced Search endpoint with City/State/Status filters.
 * The search() async generator is overridden to handle:
 *   - Advanced Search URL parameters (different from standard BaseScraper URL builder)
 *   - HTML table (#tblAttorney) result format
 *   - 500-result cap per query
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class CaliforniaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'california',
      stateCode: 'CA',
      baseUrl: 'https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch',
      pageSize: 500,
      practiceAreaCodes: {
        'administrative':     '1',
        'admiralty':          '2',
        'appellate':          '6',
        'bankruptcy':         '9',
        'business':           '10',
        'civil rights':       '11',
        'construction':       '16',
        'corporate':          '18',
        'criminal':           '19',
        'criminal defense':   '19',
        'criminal law':       '19',
        'education':          '21',
        'elder law':          '22',
        'employment':         '42',
        'environmental':      '28',
        'estate planning':    '60',
        'estate':             '60',
        'family':             '29',
        'family law':         '29',
        'health care':        '33',
        'immigration':        '34',
        'insurance':          '36',
        'intellectual property': '37',
        'ip':                 '37',
        'labor':              '42',
        'litigation':         '44',
        'medical malpractice': '46',
        'personal injury':    '51',
        'injury':             '51',
        'real estate':        '54',
        'securities':         '55',
        'tax':                '56',
        'tax law':            '56',
        'trusts':             '60',
        'wills':              '61',
        'workers comp':       '63',
        'workers compensation': '63',
      },
      defaultCities: [
        'Los Angeles', 'San Francisco', 'San Diego', 'Sacramento',
        'San Jose', 'Oakland', 'Irvine', 'Pasadena',
        'Santa Monica', 'Beverly Hills', 'Newport Beach', 'Fresno',
        'Long Beach', 'Riverside', 'Santa Ana', 'Palo Alto',
      ],
    });
  }

  /**
   * Not used directly — search() is overridden to build Advanced Search URLs.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Advanced Search`);
  }

  /**
   * Not used directly — search() parses the HTML table inline.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Advanced Search`);
  }

  /**
   * Not used directly — search() handles result count detection inline.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Advanced Search`);
  }

  /**
   * Build the Advanced Search URL for a city, with optional practice area filter.
   *
   * The CalBar ASP.NET MVC controller only returns results when ALL form fields
   * are present in the query string (even empty ones). Without the full set of
   * fields, the server returns the blank search form with no results.
   *
   * @param {string} city
   * @param {string|null} practiceCode - Practice area name or null
   * @returns {string}
   */
  _buildAdvancedSearchUrl(city, practiceCode) {
    const params = new URLSearchParams();
    // All fields must be present for the server to return results
    params.set('LastNameOption', 'b');
    params.set('LastName', '');
    params.set('FirstNameOption', 'b');
    params.set('FirstName', '');
    params.set('MiddleNameOption', 'b');
    params.set('MiddleName', '');
    params.set('FirmNameOption', 'b');
    params.set('FirmName', '');
    params.set('CityOption', 'e'); // exact match
    params.set('City', city);
    params.set('State', 'CA');    // abbreviation, not full name
    params.set('Zip', '');
    params.set('District', '');
    params.set('County', '');
    params.set('LegalSpecialty', '');
    params.set('LanguageSpoken', '');
    params.set('PracticeArea', practiceCode || '');
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse the attorney results table from a Cheerio-loaded page.
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object[]} Array of attorney objects
   */
  _parseResultsTable($) {
    const attorneys = [];

    $('#tblAttorney tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 5) return;

      // Column 0: Name (linked, format "Last, First Middle")
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const rawName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();

      // Extract bar number from link href: /attorney/Licensee/Detail/{barNum}
      let barNumber = '';
      const href = nameLink.attr('href') || '';
      const detailMatch = href.match(/Detail\/(\d+)/);
      if (detailMatch) {
        barNumber = detailMatch[1];
      }

      // Column 1: Status
      const barStatus = $(cells[1]).text().trim();

      // Column 2: Number (fallback for bar number)
      const numberCell = $(cells[2]).text().trim();
      if (!barNumber && numberCell) {
        barNumber = numberCell;
      }

      // Column 3: City
      const city = $(cells[3]).text().trim();

      // Column 4: Admission Date
      const admissionDate = $(cells[4]).text().trim();

      // Parse name: "Last, First Middle" → first_name, last_name
      let firstName = '';
      let lastName = '';
      if (rawName.includes(',')) {
        const commaParts = rawName.split(',');
        lastName = commaParts[0].trim();
        const afterComma = (commaParts[1] || '').trim();
        // First word after comma is the first name
        const nameParts = afterComma.split(/\s+/).filter(Boolean);
        firstName = nameParts.length > 0 ? nameParts[0] : '';
      } else {
        // Fallback: use splitName from base
        const split = this.splitName(rawName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: 'CA',
        phone: '',
        email: '',
        bar_number: barNumber,
        admission_date: admissionDate,
        bar_status: barStatus,
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the California Bar.
   * Overrides BaseScraper.search() to use Advanced Search endpoint with HTML table parsing.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      const url = this._buildAdvancedSearchUrl(city, practiceCode);
      log.info(`Fetching — ${url}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpGet(url, rateLimiter);
      } catch (err) {
        log.error(`Request failed for ${city}: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) {
          // Retry the same city — decrement won't happen since we use for..of
          // Instead just try once more
          try {
            await rateLimiter.wait();
            response = await this.httpGet(url, rateLimiter);
          } catch (retryErr) {
            log.error(`Retry failed for ${city}: ${retryErr.message}`);
            continue;
          }
        } else {
          continue;
        }
      }

      // Handle rate limiting
      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name} for ${city}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (shouldRetry) {
          try {
            await rateLimiter.wait();
            response = await this.httpGet(url, rateLimiter);
          } catch (retryErr) {
            log.error(`Retry failed for ${city}: ${retryErr.message}`);
            continue;
          }
          if (response.statusCode !== 200) {
            log.error(`Retry got status ${response.statusCode} for ${city} — skipping`);
            continue;
          }
        } else {
          continue;
        }
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} for ${city} — skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA detected for ${city} — skipping`);
        yield { _captcha: true, city };
        continue;
      }

      const $ = cheerio.load(response.body);
      const attorneys = this._parseResultsTable($);

      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      // Detect if results are capped at 500
      if (attorneys.length >= 500) {
        log.warn(`Got ${attorneys.length} results for ${city} — may be capped at 500`);
      } else {
        log.success(`Found ${attorneys.length} results for ${city}`);
      }

      // Filter and yield
      for (const attorney of attorneys) {
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }

        attorney.practice_area = practiceArea || '';
        yield attorney;
      }
    }
  }
}

module.exports = new CaliforniaScraper();
