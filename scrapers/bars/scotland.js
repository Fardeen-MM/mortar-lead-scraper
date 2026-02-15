/**
 * Law Society of Scotland Scraper
 *
 * Source: https://www.lawscot.org.uk/find-a-solicitor/
 * Method: HTTP GET with query params + Cheerio HTML parsing
 * Search URL: /find-a-solicitor/?type=sol&lastname=X&city=Y
 *
 * The Law Society of Scotland maintains a Find a Solicitor directory.
 * Search by last name, city, area of work via GET query params.
 * Results are in <div class="find-a-solicitor-list-item"> containers with
 * h2.h4 name headings ("Last, First"), firm links with data-heading attributes,
 * and admission date paragraphs.
 *
 * Note: results are randomised by default -- systematic searching by
 * last name initial and city provides more complete coverage.
 *
 * Overrides search() to iterate A-Z last name initials per city.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ScotlandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'scotland',
      stateCode: 'UK-SC',
      baseUrl: 'https://www.lawscot.org.uk/find-a-solicitor/',
      pageSize: 20,
      practiceAreaCodes: {
        'commercial property':          'commercial property',
        'residential conveyancing':     'residential conveyancing',
        'conveyancing':                 'residential conveyancing',
        'family':                       'family',
        'family law':                   'family',
        'criminal':                     'criminal',
        'criminal law':                 'criminal',
        'personal injury':              'personal injury',
        'employment':                   'employment',
        'employment law':               'employment',
        'wills/executries':             'wills/executries',
        'wills':                        'wills/executries',
        'executries':                   'wills/executries',
        'commercial/corporate':         'commercial/corporate',
        'commercial':                   'commercial/corporate',
        'corporate':                    'commercial/corporate',
        'immigration':                  'immigration',
        'child law':                    'child law',
        'mental health':                'mental health',
        'licensing':                    'licensing',
        'planning':                     'planning',
      },
      defaultCities: [
        'Edinburgh', 'Glasgow', 'Aberdeen', 'Dundee',
        'Inverness', 'Stirling', 'Perth', 'Paisley',
      ],
    });

    // GET-based search — no separate endpoint needed
  }

  /**
   * Build search URL for the Law Society of Scotland.
   * Uses GET with query params: type=sol, lastname, city, etc.
   */
  buildSearchUrl({ city, practiceCode, page, lastNameInitial }) {
    const params = new URLSearchParams();
    params.set('type', 'sol');
    if (lastNameInitial) {
      params.set('lastname', lastNameInitial);
    }
    if (city) {
      params.set('city', city);
    }
    if (practiceCode) {
      params.set('areaofwork', practiceCode);
    }
    if (page && page > 1) {
      params.set('page', String(page));
    }
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse search results from the Law Society of Scotland HTML page.
   * Results are in <div class="find-a-solicitor-list-item"> containers.
   * Name in <h2 class="h4"> as "Last, First".
   * Firm in <a class="overlay-link"> with data-heading attribute.
   * Admission date in <p>Admission date: DD/MM/YYYY</p> inside .findASolSummary.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('.find-a-solicitor-list-item').each((_, el) => {
      const $el = $(el);

      // Extract solicitor name from h2.h4 heading
      const fullName = $el.find('h2.h4').text().trim();
      if (!fullName || fullName.length < 2) return;

      // Parse "Last, First Middle" name format
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = parts[1] || '';
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      // Extract firm name from overlay-link data-heading attribute
      const $firmLink = $el.find('a.overlay-link').first();
      const firmName = $firmLink.attr('data-heading') || $firmLink.text().trim() || '';

      // Extract firm address from data-address attribute
      const firmAddress = ($firmLink.attr('data-address') || '').replace(/\r/g, ', ').replace(/&#xD;/g, ', ');

      // Extract total solicitors count from data-partners attribute
      const totalSolicitors = $firmLink.attr('data-partners') || '';

      // Extract admission date
      let admissionDate = '';
      $el.find('.findASolSummary p').each((_, p) => {
        const pText = $(p).text().trim();
        const admMatch = pText.match(/Admission date:\s*(\d{2}\/\d{2}\/\d{4})/i);
        if (admMatch) admissionDate = admMatch[1];
      });

      // Extract postcode from address
      const postcodeMatch = firmAddress.match(/\b([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})\b/);
      const postcode = postcodeMatch ? postcodeMatch[1] : '';

      // Extract solicitor ID from the item's id attribute
      const solId = ($el.attr('id') || '').replace('fos-item-', '');

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: firmName,
        city: '',
        state: 'UK-SC',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Practising',
        profile_url: '',
        address: firmAddress,
        postcode: postcode,
        admission_date: admissionDate,
        solicitor_id: solId,
        total_solicitors_in_firm: totalSolicitors,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the search results page.
   * The page displays "N results found" as plain text.
   */
  extractResultCount($) {
    // Try dedicated results-count element first
    const countText = $('.results-count').text().trim();
    const match = countText.match(/([\d,]+)\s+results?\s+found/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);

    // Fallback: search all text on page for "N results found"
    const text = $('body').text();
    const fallback = text.match(/([\d,]+)\s+results?\s+found/i);
    if (fallback) return parseInt(fallback[1].replace(/,/g, ''), 10);

    // Fallback: count h2 elements that look like names (containing commas)
    let nameCount = 0;
    $('h2').each((_, el) => {
      const t = $(el).text().trim();
      if (t.includes(',') && t.length > 3 && t.length < 100) nameCount++;
    });
    if (nameCount > 0) return nameCount;

    return 0;
  }

  /**
   * Async generator that yields solicitor records from the Law Society of Scotland.
   *
   * Strategy:
   *  - Uses GET requests with query params: type=sol, lastname, city
   *  - Systematically search by city and last name initials (A-Z)
   *  - Parse server-rendered HTML results
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    const seen = new Set();

    // Letters for systematic searching to combat randomised results
    const lastNameInitials = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} solicitors in ${city}, ${this.stateCode}`);

      // Search by each last name initial to get more complete coverage
      for (const initial of lastNameInitials) {
        let page = 1;
        let totalResults = 0;
        let pagesFetched = 0;
        let consecutiveEmpty = 0;

        while (true) {
          if (options.maxPages && pagesFetched >= options.maxPages) {
            log.info(`Reached max pages limit (${options.maxPages}) for ${city}/${initial}`);
            break;
          }

          const url = this.buildSearchUrl({ city, practiceCode, page, lastNameInitial: initial });
          log.info(`Page ${page} — GET ${url}`);

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

          if (response.statusCode === 429 || response.statusCode === 403) {
            log.warn(`Got ${response.statusCode} from ${this.name}`);
            const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
            if (shouldRetry) continue;
            break;
          }

          if (response.statusCode !== 200) {
            log.error(`Unexpected status ${response.statusCode} — skipping ${city}/${initial}`);
            break;
          }

          rateLimiter.resetBackoff();

          if (this.detectCaptcha(response.body)) {
            log.warn(`CAPTCHA detected on page ${page} for ${city}/${initial} — skipping`);
            yield { _captcha: true, city, page };
            break;
          }

          const $ = cheerio.load(response.body);

          if (page === 1) {
            totalResults = this.extractResultCount($);
            if (totalResults === 0) {
              const testResults = this.parseResultsPage($);
              if (testResults.length === 0) {
                // No results for this initial — move to next
                break;
              }
              totalResults = testResults.length;
            }
            const totalPages = Math.ceil(totalResults / this.pageSize);
            log.info(`Found ${totalResults} results (${totalPages} pages) for ${city}/${initial}`);
          }

          const attorneys = this.parseResultsPage($);

          if (attorneys.length === 0) {
            consecutiveEmpty++;
            if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
              break;
            }
            page++;
            pagesFetched++;
            continue;
          }

          consecutiveEmpty = 0;

          for (const attorney of attorneys) {
            // Set city from the search parameter
            attorney.city = city;

            // Deduplicate by name+firm combination
            const key = `${attorney.full_name}|${attorney.firm_name}`.toLowerCase();
            if (seen.has(key)) continue;
            seen.add(key);

            yield this.transformResult(attorney, practiceArea);
          }

          // Check for next page
          const hasNext = $('a').filter((_, el) => {
            const text = $(el).text().trim().toLowerCase();
            return text === 'next' || text === 'next >' || text === '>>' || text.includes('next page');
          }).length > 0;

          const totalPages = Math.ceil(totalResults / this.pageSize);
          if (page >= totalPages && !hasNext) {
            break;
          }

          page++;
          pagesFetched++;
        }
      }

      log.success(`Completed searching ${city} (${seen.size} unique solicitors so far)`);
    }
  }
}

module.exports = new ScotlandScraper();
