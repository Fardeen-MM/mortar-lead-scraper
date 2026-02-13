/**
 * Georgia State Bar Association Scraper
 *
 * Source: https://www.gabar.org/membersearchresults.cfm
 * Method: HTTP GET + Cheerio (ColdFusion-based server-rendered HTML)
 *
 * Searches the Georgia Bar member directory by city, with optional
 * practice area (section) filtering. The search() async generator is
 * fully overridden since the URL structure and pagination differ from
 * the base scraper.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class GeorgiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'georgia',
      stateCode: 'GA',
      baseUrl: 'https://www.gabar.org/membersearchresults.cfm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative': '1',
        'bankruptcy': '2',
        'business': '3',
        'corporate': '3',
        'criminal': '4',
        'criminal defense': '4',
        'environmental': '5',
        'estate planning': '6',
        'estate': '6',
        'family': '7',
        'family law': '7',
        'immigration': '8',
        'intellectual property': '9',
        'labor': '10',
        'employment': '10',
        'personal injury': '11',
        'real estate': '12',
        'tax': '13',
        'tax law': '13',
      },
      defaultCities: [
        'Atlanta', 'Savannah', 'Augusta', 'Columbus', 'Macon',
        'Athens', 'Roswell', 'Albany', 'Marietta', 'Decatur',
        'Lawrenceville', 'Kennesaw', 'Gainesville', 'Valdosta',
      ],
    });

    this.detailBaseUrl = 'https://www.gabar.org/membersearchdetail.cfm';
  }

  /**
   * Not used — search() is fully overridden for the ColdFusion directory.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for GA Bar directory`);
  }

  /**
   * Not used — search() is fully overridden for the ColdFusion directory.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for GA Bar directory`);
  }

  /**
   * Parse the ColdFusion search results HTML page.
   * Looks for attorney listing elements (tables/lists) and extracts
   * name, bar number, city, and status from each entry.
   *
   * @param {CheerioStatic} $ - Cheerio-loaded HTML document
   * @returns {object[]} Array of attorney records
   */
  parseResultsPage($) {
    const attorneys = [];

    // GA Bar results are typically rendered in a table or repeated div/list structure.
    // Try table rows first, then fall back to common listing patterns.
    $('table.searchresults tr, table.memberResults tr, .search-results tr, .member-list tr').each((i, el) => {
      const $row = $(el);

      // Skip header rows
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      // Typical layout: Name (linked), Bar Number, City, Status
      const nameLink = $row.find('a').first();
      const fullName = nameLink.text().trim();
      if (!fullName) return;

      const profileHref = nameLink.attr('href') || '';
      const barNumberMatch = profileHref.match(/(?:BarNumber|id|member)=(\d+)/i);

      // Extract text from each cell
      const cellTexts = [];
      cells.each((_, cell) => { cellTexts.push($(cell).text().trim()); });

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
        state: 'GA',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: status || 'Active',
        profile_url: profileHref ? `https://www.gabar.org/${profileHref.replace(/^\//, '')}` : '',
        source: `${this.name}_bar`,
      });
    });

    // Fallback: look for div-based or list-based results if table parsing found nothing
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .attorney-result, .result-item').each((_, el) => {
        const $el = $(el);

        const nameLink = $el.find('a').first();
        const fullName = nameLink.text().trim();
        if (!fullName) return;

        const profileHref = nameLink.attr('href') || '';
        const barNumberMatch = profileHref.match(/(?:BarNumber|id|member)=(\d+)/i);

        const barText = $el.find('.bar-number, .barnum').text().trim();
        const barNumber = barNumberMatch ? barNumberMatch[1] : (barText.match(/\d+/) || [''])[0];

        const cityText = $el.find('.city, .location').text().trim();
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
          state: 'GA',
          phone: phoneText,
          email: email,
          website: '',
          bar_number: barNumber,
          admission_date: '',
          bar_status: statusText || 'Active',
          profile_url: profileHref ? `https://www.gabar.org/${profileHref.replace(/^\//, '')}` : '',
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
   * @returns {string|null} URL of the next page, or null if no more pages
   */
  findNextPageUrl($) {
    // Look for common pagination patterns
    const nextLink = $('a:contains("Next"), a:contains("next"), a.next, a.nextPage, a[rel="next"]').first();
    if (nextLink.length) {
      const href = nextLink.attr('href');
      if (href) {
        if (href.startsWith('http')) return href;
        return `https://www.gabar.org/${href.replace(/^\//, '')}`;
      }
    }
    return null;
  }

  /**
   * Async generator that yields attorney records from the GA Bar directory.
   * Overrides BaseScraper.search() since the URL structure is unique.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);
    const cities = this.getCities(options);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      // Build the initial search URL
      const params = new URLSearchParams();
      params.set('FindMember', '');
      params.set('BarNumber', '');
      params.set('City', city);
      params.set('Circuit', '0');
      params.set('Section', practiceCode || '0');
      params.set('County', '0');
      params.set('Zip', '');
      params.set('Status', 'A');
      params.set('Practice', practiceCode || '0');

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
        const nextPageUrl = this.findNextPageUrl($);
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

module.exports = new GeorgiaScraper();
