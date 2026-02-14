/**
 * Law Society of Scotland Scraper
 *
 * Source: https://www.lawscot.org.uk/find-a-solicitor/
 * Search endpoint: https://www.lawscot.org.uk/find-a-solicitor/search/
 * Method: HTML form-based search, server-side rendered results
 *
 * The Law Society of Scotland maintains a Find a Solicitor directory.
 * Search by last name, first name, firm name, postcode, area of work, city.
 * Results include firm name, postal address with postcode, legal aid status,
 * and partner count. Detail pages may have phone and email.
 *
 * Note: results are randomised by default -- systematic searching by
 * last name initial and city provides more complete coverage.
 *
 * Overrides search() to POST form data to the search endpoint and parse
 * server-rendered HTML results.
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

    this.searchEndpoint = 'https://www.lawscot.org.uk/find-a-solicitor/search/';
  }

  /**
   * Not used -- search() is fully overridden for HTML form scraping.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for HTML form search`);
  }

  /**
   * Not used -- search() is fully overridden for HTML form scraping.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for HTML form search`);
  }

  /**
   * Not used -- search() is fully overridden for HTML form scraping.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for HTML form search`);
  }

  /**
   * HTTP POST for the Law Society of Scotland search form.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof data === 'string'
        ? data
        : new URLSearchParams(data).toString();
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.lawscot.org.uk',
          'Referer': 'https://www.lawscot.org.uk/find-a-solicitor/',
          'Connection': 'keep-alive',
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        // Follow redirects with GET
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(postData);
      req.end();
    });
  }

  /**
   * Parse search results from the Law Society of Scotland HTML page.
   * Results typically show firm name, address, postcode, legal aid, partners.
   */
  _parseSearchResults($, city) {
    const attorneys = [];

    // Try various selectors for solicitor result items
    const selectors = [
      '.search-result', '.result-item', '.solicitor-result',
      'table tbody tr', '.card', 'li.result', 'article',
      '.find-solicitor-result', '.listing', '.member-result',
    ];

    let $items = $([]);
    for (const sel of selectors) {
      $items = $(sel);
      if ($items.length > 0) break;
    }

    // Fallback: try to find result containers by structure
    if ($items.length === 0) {
      $items = $('div').filter((_, el) => {
        const $el = $(el);
        const text = $el.text();
        // Look for blocks that contain name-like and address-like content
        return text.includes(city) && (text.match(/\b[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}\b/) !== null);
      });
    }

    $items.each((_, el) => {
      const $el = $(el);

      // Extract solicitor/firm name
      const nameEl = $el.find('h2 a, h3 a, h4 a, .name a, a.solicitor-name').first();
      let fullName = nameEl.text().trim();
      if (!fullName) {
        fullName = $el.find('h2, h3, h4, .name, .solicitor-name, strong').first().text().trim();
      }
      if (!fullName || fullName.length < 2) return;

      // Profile URL
      let profileUrl = nameEl.attr('href') || '';
      if (profileUrl && profileUrl.startsWith('/')) {
        profileUrl = `https://www.lawscot.org.uk${profileUrl}`;
      }

      // Extract firm name (may differ from solicitor name)
      let firmName = ($el.find('.firm, .firm-name, .organisation, .company').text() || '').trim();
      if (!firmName) {
        // Check if there is a secondary heading/line that looks like a firm
        const secondaryText = $el.find('p, .details, .info').first().text().trim();
        if (secondaryText && secondaryText !== fullName) {
          firmName = secondaryText.split('\n')[0].trim();
        }
      }

      // Extract address and postcode
      const addressEl = $el.find('.address, address, .location, .postal-address');
      let address = addressEl.text().trim() || '';
      if (!address) {
        // Try to extract from the full text
        const fullText = $el.text();
        const postcodeMatch = fullText.match(/\b([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})\b/);
        if (postcodeMatch) {
          address = fullText.substring(
            Math.max(0, fullText.indexOf(postcodeMatch[0]) - 100),
            fullText.indexOf(postcodeMatch[0]) + postcodeMatch[0].length
          ).trim();
        }
      }

      // Extract postcode from address
      const postcodeMatch = address.match(/\b([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})\b/);
      const postcode = postcodeMatch ? postcodeMatch[1] : '';

      // Extract phone number
      let phone = ($el.find('a[href^="tel:"]').attr('href') || '').replace('tel:', '').trim();
      if (!phone) {
        const phoneMatch = $el.text().match(/(?:Tel|Phone|Telephone):\s*([\d\s+()-]+)/i) ||
                           $el.text().match(/\b(0\d{2,4}\s?\d{3,4}\s?\d{3,4})\b/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // Extract email
      let email = ($el.find('a[href^="mailto:"]').attr('href') || '').replace('mailto:', '').trim();

      // Extract website
      const website = ($el.find('a[href^="http"]').not('a[href*="lawscot.org"]').attr('href') || '').trim();

      // Legal aid status
      const legalAid = ($el.find('.legal-aid, .legal-aid-status').text() || '').trim() ||
                        ($el.text().match(/Legal\s+Aid:\s*(Yes|No|Available)/i) || ['', ''])[1];

      // Extract status
      const status = ($el.find('.status, .badge').text() || '').trim();

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName || fullName,
        city: city,
        state: 'UK-SC',
        phone,
        email,
        website,
        bar_number: '',
        bar_status: status || 'Practising',
        profile_url: profileUrl,
        address,
        postcode,
        legal_aid: legalAid,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the search results page.
   */
  _extractSearchResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:results?|records?|solicitors?|firms?)\s+found/i) ||
                  text.match(/(?:Showing|Found|Displaying)\s+(?:\d+\s*[-–]\s*\d+\s+of\s+)?([\d,]+)/i) ||
                  text.match(/Results:\s*([\d,]+)/i) ||
                  text.match(/(\d+)\s+matching\s+(?:solicitors?|firms?|results?)/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }

  /**
   * Fetch detail page for a solicitor to get additional contact info.
   * Returns enriched attorney object with phone/email if available.
   */
  async _enrichFromDetailPage(attorney, rateLimiter) {
    if (!attorney.profile_url) return attorney;

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(attorney.profile_url, rateLimiter);
      if (response.statusCode !== 200) return attorney;

      const $ = cheerio.load(response.body);

      // Extract phone if not already present
      if (!attorney.phone) {
        const phoneEl = $('a[href^="tel:"]').first();
        if (phoneEl.length) {
          attorney.phone = phoneEl.attr('href').replace('tel:', '').trim();
        } else {
          const phoneMatch = $('body').text().match(/(?:Tel|Phone|Telephone):\s*([\d\s+()-]+)/i);
          if (phoneMatch) attorney.phone = phoneMatch[1].trim();
        }
      }

      // Extract email if not already present
      if (!attorney.email) {
        const emailEl = $('a[href^="mailto:"]').first();
        if (emailEl.length) {
          attorney.email = emailEl.attr('href').replace('mailto:', '').trim();
        }
      }

      // Extract website if not already present
      if (!attorney.website) {
        const websiteEl = $('a[href^="http"]').not('a[href*="lawscot.org"]').first();
        if (websiteEl.length) {
          attorney.website = websiteEl.attr('href').trim();
        }
      }
    } catch (err) {
      log.info(`Could not fetch detail page for ${attorney.full_name}: ${err.message}`);
    }

    return attorney;
  }

  /**
   * Async generator that yields solicitor records from the Law Society of Scotland.
   *
   * Strategy:
   *  - Systematically search by city using the HTML form POST
   *  - To combat randomised results, also search by last name initials (A-Z)
   *  - Parse server-rendered HTML results
   *  - Optionally fetch detail pages for phone/email enrichment
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

          // Build search form data
          const formData = {
            last_name: initial,
            first_name: '',
            firm_name: '',
            postcode: '',
            city: city,
          };
          if (practiceCode) {
            formData.area_of_work = practiceCode;
          }
          if (page > 1) {
            formData.page = String(page);
          }

          log.info(`Page ${page} — POST ${this.searchEndpoint} [City=${city}, Last=${initial}]`);

          let response;
          try {
            await rateLimiter.wait();
            response = await this.httpPost(this.searchEndpoint, formData, rateLimiter);
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
            totalResults = this._extractSearchResultCount($);
            if (totalResults === 0) {
              const testResults = this._parseSearchResults($, city);
              if (testResults.length === 0) {
                // No results for this initial — move to next
                break;
              }
              totalResults = testResults.length;
            }
            const totalPages = Math.ceil(totalResults / this.pageSize);
            log.info(`Found ${totalResults} results (${totalPages} pages) for ${city}/${initial}`);
          }

          const attorneys = this._parseSearchResults($, city);

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
