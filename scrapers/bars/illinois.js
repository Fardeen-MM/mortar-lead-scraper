/**
 * Illinois Bar (IARDC) Scraper
 *
 * Source: https://www.iardc.org/lawyersearch
 * Method: HTTP GET via gateway.asp endpoint + Cheerio for HTML parsing
 * Search URL: https://www.iardc.org/lrs/gateway.asp with query params
 * Note: IARDC does not support practice area filtering
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IllinoisScraper extends BaseScraper {
  constructor() {
    super({
      name: 'illinois',
      stateCode: 'IL',
      baseUrl: 'https://www.iardc.org/lrs/gateway.asp',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Chicago', 'Springfield', 'Rockford', 'Naperville', 'Peoria',
        'Joliet', 'Elgin', 'Aurora', 'Champaign', 'Bloomington',
        'Decatur', 'Schaumburg', 'Wheaton', 'Waukegan',
      ],
    });
  }

  /**
   * Build the search URL for a specific city and page.
   * IARDC gateway uses simple GET parameters.
   */
  buildSearchUrl({ city, page }) {
    const params = new URLSearchParams();
    params.set('LastName', '*');
    params.set('FirstName', '*');
    if (city) params.set('City', city);
    params.set('State', 'IL');
    params.set('Status', 'AU'); // Authorized to practice
    if (page && page > 1) {
      params.set('Page', String(page));
    }
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse search results page HTML.
   * IARDC returns results in HTML table rows with: Name (linked), ARDC Number, City, Status.
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
      if (firstCellText === 'name' || firstCellText === 'attorney name') return;

      // Look for a link containing the attorney name
      const nameLink = $row.find('a').first();
      let fullName = '';
      let profileUrl = '';

      if (nameLink.length) {
        fullName = nameLink.text().trim();
        profileUrl = nameLink.attr('href') || '';
        // Make absolute URL if relative
        if (profileUrl && !profileUrl.startsWith('http')) {
          profileUrl = `https://www.iardc.org/lrs/${profileUrl}`;
        }
      } else {
        // No link — try first cell text
        fullName = cells.first().text().trim();
      }

      if (!fullName) return;

      // Extract ARDC number, city, and status from remaining cells
      let barNumber = '';
      let city = '';
      let barStatus = '';

      cells.each((i, cell) => {
        const text = $(cell).text().trim();
        if (i === 0) return; // Skip name cell

        // ARDC numbers are typically 7-digit numbers
        if (/^\d{5,8}$/.test(text)) {
          barNumber = text;
        } else if (/authorized|active|inactive|retired|suspended|not authorized/i.test(text)) {
          barStatus = text;
        } else if (text && !city && /^[A-Za-z\s.'-]+$/.test(text) && text.length < 40) {
          city = text;
        }
      });

      // Split name — IARDC typically formats as "Last, First Middle"
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
        state: 'IL',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: barStatus,
        profile_url: profileUrl,
        source: 'illinois_bar',
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the page.
   */
  extractResultCount($) {
    const text = $('body').text();

    // Look for patterns like "X records found" or "Results: 1-50 of X"
    const matchOf = text.match(/of\s+([\d,]+)\s+(?:records?|results?|attorneys?|matches)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:records?|results?|attorneys?|matches)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    // Try total at bottom of page
    const matchTotal = text.match(/Total:?\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() since IARDC has a unique URL structure and no practice area support.
   * Async generator that yields attorney records.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // IARDC does not support practice area filtering
    if (practiceArea) {
      log.warn(`Illinois IARDC does not support practice area filtering — ignoring "${practiceArea}"`);
    }

    const cities = this.getCities(options);

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, IL`);

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

        const url = this.buildSearchUrl({ city, page });
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
          log.warn(`Got ${response.statusCode} from IARDC`);
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
              log.info(`No results for attorneys in ${city}`);
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

module.exports = new IllinoisScraper();
