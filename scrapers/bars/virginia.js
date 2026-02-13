/**
 * Virginia State Bar (VSB) Scraper
 *
 * Source: https://www.vsb.org/vlrs/
 * Method: HTTP GET with query params + Cheerio for HTML parsing
 * Search URL: https://www.vsb.org/vlrs/results.asp
 * Params: LName, FName, City, Adm (A=active), Specialty
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class VirginiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'virginia',
      stateCode: 'VA',
      baseUrl: 'https://www.vsb.org/vlrs/results.asp',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':        'Administrative',
        'bankruptcy':            'Bankruptcy',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'employment':            'Employment/Labor',
        'labor':                 'Employment/Labor',
        'environmental':         'Environmental',
        'estate planning':       'Estate Planning',
        'estate':                'Estate Planning',
        'family':                'Family/Domestic',
        'family law':            'Family/Domestic',
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Virginia Beach', 'Norfolk', 'Richmond', 'Arlington', 'Alexandria',
        'Newport News', 'Chesapeake', 'Hampton', 'Roanoke', 'Fairfax',
        'Charlottesville', 'Lynchburg', 'McLean', 'Tysons',
      ],
    });
  }

  /**
   * Build the search URL for a specific city, page, and optional practice code.
   */
  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    params.set('LName', '*');
    params.set('FName', '');
    if (city) params.set('City', city);
    params.set('Adm', 'A'); // Active only
    if (practiceCode) {
      params.set('Specialty', practiceCode);
    }
    if (page && page > 1) {
      params.set('Page', String(page));
    }
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse search results page HTML.
   * VSB returns results in HTML tables/lists with: Name, VSB Number, City, Admission date, Status.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('table tr').each((_, row) => {
      const $row = $(row);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      // Skip header rows
      if ($row.find('th').length > 0) return;
      const firstCellText = cells.first().text().trim().toLowerCase();
      if (firstCellText === 'name' || firstCellText === 'attorney' || firstCellText === 'member name') return;

      // Look for a link containing the attorney name
      const nameLink = $row.find('a').first();
      let fullName = '';
      let profileUrl = '';

      if (nameLink.length) {
        fullName = nameLink.text().trim();
        profileUrl = nameLink.attr('href') || '';
        // Make absolute URL if relative
        if (profileUrl && !profileUrl.startsWith('http')) {
          profileUrl = `https://www.vsb.org/vlrs/${profileUrl}`;
        }
      } else {
        fullName = cells.first().text().trim();
      }

      if (!fullName) return;

      // Extract VSB number, city, admission date, and status from cells
      let barNumber = '';
      let city = '';
      let admissionDate = '';
      let barStatus = '';

      cells.each((i, cell) => {
        const text = $(cell).text().trim();
        if (i === 0) return; // Skip name cell

        // VSB numbers are typically numeric
        if (/^\d{5,8}$/.test(text)) {
          barNumber = text;
        } else if (/\d{1,2}\/\d{1,2}\/\d{2,4}/.test(text) || /\d{4}-\d{2}-\d{2}/.test(text)) {
          // Date pattern — likely admission date
          admissionDate = text;
        } else if (/active|inactive|retired|suspended|authorized|good standing|emeritus/i.test(text)) {
          barStatus = text;
        } else if (text && !city && /^[A-Za-z\s.'-]+$/.test(text) && text.length < 40) {
          city = text;
        }
      });

      // Split name — VSB typically formats as "Last, First Middle"
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: '',
        city: city,
        state: 'VA',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: admissionDate,
        bar_status: barStatus,
        profile_url: profileUrl,
        source: 'virginia_bar',
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the page.
   */
  extractResultCount($) {
    const text = $('body').text();

    // Look for patterns like "X records found" or "Results 1-50 of X"
    const matchOf = text.match(/of\s+([\d,]+)\s+(?:records?|results?|attorneys?|members?|matches)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:records?|results?|attorneys?|members?|matches)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total:?\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() since Virginia has a unique URL structure and result format.
   * Async generator that yields attorney records.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for VA — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, VA`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this.buildSearchUrl({ city, practiceCode, page });
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from VSB`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            // Try parsing results even if count is not found
            const testAttorneys = this.parseResultsPage($);
            if (testAttorneys.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testAttorneys.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Filter and yield
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for "next" page links
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === 'next >>' || text === 'next page';
        }).length > 0;

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages && !hasNext) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new VirginiaScraper();
