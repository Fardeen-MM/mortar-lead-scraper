/**
 * North Carolina State Bar Scraper
 *
 * Source: https://www.ncbar.gov/member-directory/
 * Method: HTTP GET + Cheerio (server-rendered HTML)
 *
 * Searches the NC Bar member directory by city with active status filter.
 * Practice area filtering is not supported by the NC directory.
 * The search() async generator is fully overridden since the URL structure
 * and pagination differ from the base scraper.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NorthCarolinaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'north-carolina',
      stateCode: 'NC',
      baseUrl: 'https://www.ncbar.gov/member-directory/',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Charlotte', 'Raleigh', 'Durham', 'Greensboro', 'Winston-Salem',
        'Fayetteville', 'Wilmington', 'Asheville', 'Cary', 'High Point',
        'Chapel Hill', 'Gastonia', 'Concord', 'Greenville',
      ],
    });
  }

  /**
   * Not used — search() is fully overridden for the NC Bar directory.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for NC Bar directory`);
  }

  /**
   * Not used — search() is fully overridden for the NC Bar directory.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for NC Bar directory`);
  }

  /**
   * Parse the NC Bar member directory search results HTML.
   * Looks for attorney listing elements and extracts name, bar number,
   * city, and status from each entry.
   *
   * @param {CheerioStatic} $ - Cheerio-loaded HTML document
   * @returns {object[]} Array of attorney records
   */
  parseResultsPage($) {
    const attorneys = [];

    // Try table-based results first
    $('table.member-results tr, table.directory-results tr, .search-results table tr, .results-table tr').each((i, el) => {
      const $row = $(el);

      // Skip header rows
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameLink = $row.find('a').first();
      const fullName = nameLink.text().trim();
      if (!fullName) return;

      const profileHref = nameLink.attr('href') || '';

      // Extract text from each cell
      const cellTexts = [];
      cells.each((_, cell) => { cellTexts.push($(cell).text().trim()); });

      const barNumberMatch = profileHref.match(/(?:id|member|barNumber)=(\d+)/i);
      const barNumber = barNumberMatch ? barNumberMatch[1] : (cellTexts[1] || '').replace(/\D/g, '');
      const city = cellTexts[2] || '';
      const status = cellTexts[3] || '';

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: '',
        city: city,
        state: 'NC',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: status || 'Active',
        profile_url: profileHref.startsWith('http') ? profileHref : (profileHref ? `https://www.ncbar.gov${profileHref.startsWith('/') ? '' : '/'}${profileHref}` : ''),
        source: `${this.name}_bar`,
      });
    });

    // Fallback: look for div/list-based results
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .attorney-listing, .result-item, .directory-entry').each((_, el) => {
        const $el = $(el);

        const nameLink = $el.find('a').first();
        const fullName = nameLink.text().trim();
        if (!fullName) return;

        const profileHref = nameLink.attr('href') || '';
        const barNumberMatch = profileHref.match(/(?:id|member|barNumber)=(\d+)/i);

        const barText = $el.find('.bar-number, .barnum, .member-number').text().trim();
        const barNumber = barNumberMatch ? barNumberMatch[1] : (barText.match(/\d+/) || [''])[0];

        const cityText = $el.find('.city, .location, .member-city').text().trim();
        const statusText = $el.find('.status, .member-status').text().trim();
        const firmText = $el.find('.firm, .firm-name, .company').text().trim();
        const phoneText = $el.find('.phone, .telephone').text().trim();
        const emailEl = $el.find('a[href^="mailto:"]');
        const email = emailEl.length ? emailEl.attr('href').replace('mailto:', '') : '';

        const { firstName, lastName } = this.splitName(fullName);

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmText,
          city: cityText,
          state: 'NC',
          phone: phoneText,
          email: email,
          website: '',
          bar_number: barNumber,
          admission_date: '',
          bar_status: statusText || 'Active',
          profile_url: profileHref.startsWith('http') ? profileHref : (profileHref ? `https://www.ncbar.gov${profileHref.startsWith('/') ? '' : '/'}${profileHref}` : ''),
          source: `${this.name}_bar`,
        });
      });
    }

    return attorneys;
  }

  /**
   * Detect if there is a next page link in the results.
   *
   * @param {CheerioStatic} $ - Cheerio-loaded HTML document
   * @param {string} baseUrl  - Current page URL for resolving relative links
   * @returns {string|null} URL of the next page, or null if no more pages
   */
  findNextPageUrl($, baseUrl) {
    // Look for common pagination patterns
    const nextLink = $('a:contains("Next"), a:contains("next"), a.next, a.nextPage, a[rel="next"], .pagination a.next').first();
    if (nextLink.length) {
      const href = nextLink.attr('href');
      if (href) {
        if (href.startsWith('http')) return href;
        if (href.startsWith('/')) return `https://www.ncbar.gov${href}`;
        // Relative URL — resolve against base
        const base = new URL(baseUrl);
        return `${base.origin}${base.pathname}${href.startsWith('?') ? href : '/' + href}`;
      }
    }
    return null;
  }

  /**
   * Async generator that yields attorney records from the NC Bar directory.
   * Overrides BaseScraper.search() since the URL structure is unique.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`NC Bar directory does not support practice area filtering — searching all attorneys`);
    }

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      // Build the initial search URL
      const params = new URLSearchParams();
      params.set('city', city);
      params.set('state', 'NC');
      params.set('status', 'active');

      let url = `${this.baseUrl}?${params.toString()}`;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${pagesFetched + 1} — ${url}`);

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
          log.warn(`Got ${response.statusCode} from ${this.name}`);
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
          log.warn(`CAPTCHA detected on page ${pagesFetched + 1} for ${city} — skipping`);
          yield { _captcha: true, city, page: pagesFetched + 1 };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (pagesFetched === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
        } else {
          consecutiveEmpty = 0;

          if (pagesFetched === 0) {
            log.success(`Fetching results for ${city} (first page: ${attorneys.length} records)`);
          }

          // Filter and yield each attorney record
          for (const attorney of attorneys) {
            if (options.minYear && attorney.admission_date) {
              const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
              if (year > 0 && year < options.minYear) continue;
            }

            attorney.practice_area = practiceArea || '';
            yield attorney;
          }
        }

        // Check for next page
        const nextPageUrl = this.findNextPageUrl($, url);
        if (!nextPageUrl) {
          if (pagesFetched > 0) {
            log.success(`Completed all pages for ${city}`);
          }
          break;
        }

        url = nextPageUrl;
        pagesFetched++;
      }
    }
  }
}

module.exports = new NorthCarolinaScraper();
