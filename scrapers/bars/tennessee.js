/**
 * Tennessee Board of Professional Responsibility — Attorney Search Scraper
 *
 * Source: https://www.tbpr.org/attorneys/search
 * Method: HTTP GET with query parameters (Ruby on Rails / Turbo)
 *
 * The search endpoint accepts last_name and first_name parameters.
 * Results are server-rendered HTML tables with 50 results per page.
 * Pagination via &page=N query parameter.
 *
 * Flow:
 * 1. Iterate last name prefixes (A-Z) — no city-based filtering available
 * 2. For each prefix, paginate through all pages
 * 3. Parse HTML table: BPR Number, Attorney Name, City, County, Status
 * 4. Extract bar number and profile URL from link hrefs
 *
 * Result count format:
 *   "1 - 50 of 462" (paginated)
 *   "Displaying all 11 attorneys" (single page)
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class TennesseeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'tennessee',
      stateCode: 'TN',
      baseUrl: 'https://www.tbpr.org/attorneys/search',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'labor',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
        'adoption':             'adoption',
      },
      defaultCities: [
        'Nashville', 'Memphis', 'Knoxville', 'Chattanooga',
        'Clarksville', 'Murfreesboro', 'Franklin', 'Jackson',
      ],
    });

    // Single-letter last name prefixes for broad coverage
    this.lastNamePrefixes = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
  }

  /**
   * Build the search URL for a given last name prefix and page number.
   * @param {string} lastNamePrefix - The last name search prefix
   * @param {number} [page=1] - Page number (1-based)
   * @returns {string} Full search URL
   */
  _buildSearchUrl(lastNamePrefix, page = 1) {
    const params = new URLSearchParams();
    params.set('attorney[last_name]', lastNamePrefix);
    params.set('attorney[first_name]', '');
    if (page > 1) {
      params.set('page', String(page));
    }
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Extract total result count from the page header.
   * Formats:
   *   "1 - 50 of 462"           → 462
   *   "Displaying all 11 attorneys"  → 11
   *   No match                   → 0
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {number} Total results
   */
  _extractResultCount($) {
    const countText = $('span.count').text().trim();

    // "X - Y of Z" format
    const rangeMatch = countText.match(/of\s+([\d,]+)/);
    if (rangeMatch) {
      return parseInt(rangeMatch[1].replace(/,/g, ''), 10);
    }

    // "Displaying all N attorneys" format
    const allMatch = countText.match(/all\s+([\d,]+)/);
    if (allMatch) {
      return parseInt(allMatch[1].replace(/,/g, ''), 10);
    }

    return 0;
  }

  /**
   * Parse the HTML table of attorney results.
   * Columns: BPR Number | Attorney | City | County | Status
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object[]} Array of attorney objects
   */
  _parseResultsTable($) {
    const attorneys = [];

    $('table.table tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 5) return;

      // Column 0: BPR Number (linked)
      const barNumberCell = $(cells[0]);
      const barNumber = barNumberCell.text().trim();

      // Extract profile URL from link href
      const barLink = barNumberCell.find('a');
      const href = barLink.attr('href') || '';
      const profileUrl = href ? `https://www.tbpr.org${href}` : '';

      // Column 1: Attorney name (linked, format "Last, First Middle")
      const nameCell = $(cells[1]);
      const rawName = nameCell.text().trim();

      // Column 2: City
      const city = $(cells[2]).text().trim();

      // Column 3: County
      const county = $(cells[3]).text().trim();

      // Column 4: Status
      const barStatus = $(cells[4]).text().trim();

      // Parse name: "Last, First Middle" → first_name, last_name
      let firstName = '';
      let lastName = '';
      if (rawName.includes(',')) {
        const commaParts = rawName.split(',');
        lastName = commaParts[0].trim();
        const afterComma = (commaParts.slice(1).join(',') || '').trim();
        // First word after comma is the first name
        const nameParts = afterComma.split(/\s+/).filter(Boolean);
        firstName = nameParts.length > 0 ? nameParts[0] : '';
      } else {
        const split = this.splitName(rawName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      // Skip empty rows
      if (!lastName && !firstName) return;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: this._titleCase(city),
        state: 'TN',
        county: this._titleCase(county),
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: barStatus,
        profile_url: profileUrl,
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Convert ALL CAPS or mixed case to Title Case.
   * @param {string} str
   * @returns {string}
   */
  _titleCase(str) {
    if (!str) return '';
    return str.replace(/\w\S*/g, (txt) =>
      txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    );
  }

  /**
   * Not used — search() is overridden for prefix-based iteration.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Async generator that yields attorney records from the TN Board of
   * Professional Responsibility.
   *
   * Iterates A-Z last name prefixes with pagination. The TN bar search
   * does not support city-based filtering, so defaultCities are used only
   * for the frontend display. The search covers all TN attorneys.
   *
   * Options:
   *   maxPages     - Max pages to fetch per prefix (default: unlimited)
   *   maxCities    - Treated as maxPrefixes for compatibility (limits prefix count)
   *   maxPrefixes  - Max number of A-Z prefixes to iterate
   *   city         - Ignored (TN search is statewide by last name)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    if (practiceArea) {
      log.warn(`TN bar search does not support practice area filtering — searching all attorneys`);
    }

    // Determine how many prefixes to iterate
    // maxCities is treated as maxPrefixes for compatibility with test harness
    const maxPrefixes = options.maxPrefixes || options.maxCities || this.lastNamePrefixes.length;
    const prefixes = this.lastNamePrefixes.slice(0, maxPrefixes);
    const maxPagesPerPrefix = options.maxPages || Infinity;

    log.info(`TN scraper: iterating ${prefixes.length} last name prefixes, max ${maxPagesPerPrefix} pages each`);

    for (let pi = 0; pi < prefixes.length; pi++) {
      const prefix = prefixes[pi];

      // Emit city progress (using prefix as "city" for UI compatibility)
      yield { _cityProgress: { current: pi + 1, total: prefixes.length } };
      log.scrape(`Searching: TN attorneys with last name starting "${prefix}" (${pi + 1}/${prefixes.length})`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;

      while (true) {
        if (pagesFetched >= maxPagesPerPrefix) {
          log.info(`Reached max pages limit (${maxPagesPerPrefix}) for prefix "${prefix}"`);
          break;
        }

        const url = this._buildSearchUrl(prefix, page);
        log.info(`Fetching — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for prefix "${prefix}" page ${page}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) {
            try {
              await rateLimiter.wait();
              response = await this.httpGet(url, rateLimiter);
            } catch (retryErr) {
              log.error(`Retry failed for prefix "${prefix}" page ${page}: ${retryErr.message}`);
              break;
            }
          } else {
            break;
          }
        }

        // Handle rate limiting (429/403)
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name} for prefix "${prefix}" page ${page}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) {
            try {
              await rateLimiter.wait();
              response = await this.httpGet(url, rateLimiter);
            } catch (retryErr) {
              log.error(`Retry failed for prefix "${prefix}" page ${page}: ${retryErr.message}`);
              break;
            }
            if (response.statusCode !== 200) {
              log.error(`Retry got status ${response.statusCode} — skipping prefix "${prefix}"`);
              break;
            }
          } else {
            break;
          }
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for prefix "${prefix}" page ${page} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for prefix "${prefix}" page ${page} — skipping`);
          yield { _captcha: true, city: prefix, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total result count on first page
        if (page === 1) {
          totalResults = this._extractResultCount($);
          if (totalResults === 0) {
            log.info(`No results for last name prefix "${prefix}"`);
            break;
          }
          log.info(`Found ${totalResults} total results for prefix "${prefix}"`);
        }

        const attorneys = this._parseResultsTable($);

        if (attorneys.length === 0) {
          log.info(`No results on page ${page} for prefix "${prefix}" — done with prefix`);
          break;
        }

        // Yield each attorney
        for (const attorney of attorneys) {
          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        pagesFetched++;

        // Check if there are more pages
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.info(`Reached last page (${page}/${totalPages}) for prefix "${prefix}"`);
          break;
        }

        // Check for next page link as secondary confirmation
        const hasNextPage = $('a.page.next').length > 0;
        if (!hasNextPage) {
          log.info(`No next page link found for prefix "${prefix}" — done`);
          break;
        }

        page++;
      }

      if (totalResults > 0) {
        log.success(`Completed prefix "${prefix}": ${totalResults} total attorneys`);
      }
    }
  }
}

module.exports = new TennesseeScraper();
