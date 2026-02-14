/**
 * Tennessee Board of Professional Responsibility Scraper
 *
 * Source: https://www.tbpr.org/for-the-public/online-attorney-directory
 * Method: HTTP POST form with firstName, lastName, city, county, bprNumber, status
 * Results rendered as HTML that is parsed with Cheerio.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class TennesseeScraper extends BaseScraper {
  constructor() {
    super({
      name: 'tennessee',
      stateCode: 'TN',
      baseUrl: 'https://www.tbpr.org/for-the-public/online-attorney-directory',
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
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
          'Referer': this.baseUrl,
        },
        timeout: 15000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Not used directly — search() is overridden for POST-based requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST requests`);
  }

  parseResultsPage($) {
    const attorneys = [];

    // TBPR results are typically in a table or list
    $('table.directory-results tr, table.attorney-results tr, table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 3) return;

      const firstCellText = $(cells[0]).text().trim().toLowerCase();
      if (firstCellText === 'name' || firstCellText === 'bpr #' || firstCellText === 'attorney') return;

      // Typical layout: Name | BPR # | City | Status | Phone
      const nameCell = $(cells[0]);
      const fullName = nameCell.text().trim();
      const profileLink = nameCell.find('a').attr('href') || '';

      if (!fullName || fullName.length < 2) return;

      const bprNumber = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const status = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const phone = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const firmName = cells.length > 5 ? $(cells[5]).text().trim() : '';

      // Parse name — may be "Last, First" format
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

      let profileUrl = '';
      if (profileLink) {
        profileUrl = profileLink.startsWith('http')
          ? profileLink
          : `https://www.tbpr.org${profileLink}`;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: firmName,
        city: city,
        state: 'TN',
        phone: phone,
        email: '',
        website: '',
        bar_number: bprNumber.replace(/[^0-9]/g, ''),
        bar_status: status || 'Active',
        profile_url: profileUrl,
      });
    });

    // Fallback: div-based or list-based results
    if (attorneys.length === 0) {
      $('.attorney, .result-item, .attorney-listing').each((_, el) => {
        const $el = $(el);

        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .attorney-name, h3, h4').text().trim();
        const profileLink = nameEl.attr('href') || '';

        if (!fullName || fullName.length < 2) return;

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

        const bprNumber = ($el.find('.bpr, .bar-number').text().trim() || '').replace(/[^0-9]/g, '');
        const city = $el.find('.city, .location').text().trim();
        const phone = $el.find('.phone, .telephone').text().trim();
        const email = $el.find('a[href^="mailto:"]').text().trim();
        const firmName = $el.find('.firm, .firm-name').text().trim();
        const status = $el.find('.status').text().trim();

        let profileUrl = '';
        if (profileLink) {
          profileUrl = profileLink.startsWith('http')
            ? profileLink
            : `https://www.tbpr.org${profileLink}`;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: firmName,
          city: city,
          state: 'TN',
          phone: phone,
          email: email,
          website: '',
          bar_number: bprNumber,
          bar_status: status || 'Active',
          profile_url: profileUrl,
        });
      });
    }

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();

    const matchFound = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?|members?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total:\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for POST-based form submissions.
   * TBPR uses POST with firstName, lastName, city, county, bprNumber, status params.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`TN bar search does not filter by practice area — searching all attorneys`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const formData = {
          firstName: '',
          lastName: '',
          city: city,
          county: '',
          bprNumber: '',
          status: 'Active',
        };

        if (page > 1) {
          formData.page = String(page);
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
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
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
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

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we have reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (totalPages > 0 && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        // If we cannot determine total pages, stop if we got fewer than page size
        if (totalPages === 0 && attorneys.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new TennesseeScraper();
