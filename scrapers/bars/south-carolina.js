/**
 * South Carolina Bar Association Scraper
 *
 * Source: https://www.sccourts.org/attorneys/
 * Alt:    https://www.scbar.org/lawyers/directory/
 * Method: Returns 406 error (bot protection) — try with proper Accept headers and Referer
 *
 * The SC Courts attorney directory has bot protection that returns 406 Not Acceptable
 * for requests without proper browser-like headers. This scraper attempts to:
 *  1. Mimic a real browser with proper Accept, Referer, and security headers
 *  2. Parse the HTML search form and submit searches
 *  3. Extract attorney data from results tables
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class SouthCarolinaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'south-carolina',
      stateCode: 'SC',
      baseUrl: 'https://www.sccourts.org/attorneys/',
      altUrl: 'https://www.scbar.org/lawyers/directory/',
      pageSize: 50,
      practiceAreaCodes: {
        'bankruptcy':            'bankruptcy',
        'business':              'business',
        'civil litigation':      'civil litigation',
        'corporate':             'corporate',
        'criminal':              'criminal',
        'criminal defense':      'criminal defense',
        'elder':                 'elder law',
        'employment':            'employment',
        'environmental':         'environmental',
        'estate planning':       'estate planning',
        'family':                'family law',
        'family law':            'family law',
        'immigration':           'immigration',
        'intellectual property': 'intellectual property',
        'personal injury':       'personal injury',
        'real estate':           'real estate',
        'tax':                   'tax',
      },
      defaultCities: [
        'Charleston', 'Columbia', 'Greenville', 'Mount Pleasant',
        'Rock Hill', 'Summerville', 'North Charleston', 'Spartanburg',
      ],
    });

    this.origin = 'https://www.sccourts.org';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SC Courts`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SC Courts`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SC Courts`);
  }

  /**
   * Build full browser headers to bypass 406 bot protection.
   */
  _getBrowserHeaders(rateLimiter) {
    return {
      'User-Agent': rateLimiter.getUserAgent(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Cache-Control': 'max-age=0',
      'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"macOS"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'Connection': 'keep-alive',
      'Referer': this.baseUrl,
    };
  }

  /**
   * HTTP GET with full browser headers, capturing cookies.
   */
  _httpGetBrowser(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._getBrowserHeaders(rateLimiter);
      if (this._cookies) {
        headers['Cookie'] = this._cookies;
      }

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers,
        timeout: 15000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        // Capture cookies
        const setCookies = res.headers['set-cookie'] || [];
        if (setCookies.length > 0) {
          this._cookies = setCookies.map(c => c.split(';')[0]).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this._httpGetBrowser(redirect, rateLimiter));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with form data and browser headers.
   */
  httpPost(url, formData, rateLimiter, headers = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const browserHeaders = this._getBrowserHeaders(rateLimiter);
      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          ...browserHeaders,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Origin': this.origin,
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'same-origin',
          ...(this._cookies ? { 'Cookie': this._cookies } : {}),
          ...headers,
        },
        timeout: 20000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        const setCookies = res.headers['set-cookie'] || [];
        if (setCookies.length > 0) {
          this._cookies = setCookies.map(c => c.split(';')[0]).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this._httpGetBrowser(redirect, rateLimiter));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse attorney records from HTML search results.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Try table-based results
    $('table tr, .table tr, .results tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|result)/i.test(fullName)) return;

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

      const barNumber = cells.length > 1 ? $(cells[1]).text().trim().replace(/[^0-9]/g, '') : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const status = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const phone = cells.length > 4 ? $(cells[4]).text().trim().replace(/[^\d()-.\s+]/g, '') : '';

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: '',
        city: city,
        state: 'SC',
        phone: phone,
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
      });
    });

    // Fallback: div/list results
    if (attorneys.length === 0) {
      $('.attorney-result, .search-result, .result-item, .member-listing, .attorney-card').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name, .attorney-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

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
          full_name: fullName.includes(',') ? `${firstName} ${lastName}` : fullName,
          firm_name: $el.find('.firm, .firm-name, .company').text().trim(),
          city: $el.find('.city, .location').text().trim(),
          state: 'SC',
          phone: ($el.find('.phone').text().trim() || '').replace(/[^\d()-.\s+]/g, ''),
          email: $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '',
          website: '',
          bar_number: $el.find('.bar-number, .barnum').text().trim().replace(/[^0-9]/g, ''),
          bar_status: $el.find('.status').text().trim() || 'Active',
          profile_url: nameEl.attr('href') ? `${this.origin}${nameEl.attr('href')}` : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract result count from response.
   */
  _extractResultCount(body) {
    const text = cheerio.load(body)('body').text();
    const match = text.match(/([\d,]+)\s*(?:results?|records?|attorneys?|lawyers?)\s*(?:found|returned|total)/i) ||
                  text.match(/(?:of|total[:\s]*)\s*([\d,]+)/i) ||
                  text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Override search() for SC Courts with 406 bot protection.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    this._cookies = null;

    // Step 1: Establish session
    log.info('Establishing session with SC Courts directory...');

    let sessionOk = false;
    let formTokens = {};

    try {
      await rateLimiter.wait();
      const response = await this._httpGetBrowser(this.baseUrl, rateLimiter);

      if (response.statusCode === 200) {
        sessionOk = true;
        log.success('Session established with SC Courts');

        const $ = cheerio.load(response.body);
        $('input[type="hidden"]').each((_, el) => {
          const name = $(el).attr('name') || '';
          const value = $(el).attr('value') || '';
          if (name) formTokens[name] = value;
        });

        // Check for a search form action URL
        const formAction = $('form').attr('action') || '';
        if (formAction && formAction !== '#') {
          this._searchActionUrl = formAction.startsWith('http')
            ? formAction
            : `${this.origin}${formAction.startsWith('/') ? '' : '/'}${formAction}`;
        }
      } else if (response.statusCode === 406) {
        log.warn(`SC Courts returned 406 Not Acceptable — bot protection active`);
        log.warn(`SC: This directory blocks automated requests even with browser headers.`);
        yield { _captcha: true, city: 'all', reason: '406 Not Acceptable — bot protection cannot be bypassed' };
        return;
      } else {
        log.warn(`SC Courts returned status ${response.statusCode}`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode} from SC Courts` };
        return;
      }
    } catch (err) {
      log.error(`Failed to connect to SC Courts: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    const cities = this.getCities(options);
    const searchUrl = this._searchActionUrl || this.baseUrl;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const formData = {
          ...formTokens,
          'city': city,
          'state': 'SC',
          'status': 'Active',
          'submit': 'Search',
        };
        if (page > 1) formData['page'] = String(page);

        log.info(`Page ${page} — POST ${searchUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(searchUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403 || response.statusCode === 406) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        if (page === 1) {
          totalResults = this._extractResultCount(response.body);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        const attorneys = this._parseAttorneys(response.body);

        if (attorneys.length === 0) {
          if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          break;
        }

        if (page === 1 && totalResults === 0) {
          log.success(`Found ${attorneys.length} results for ${city}`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        if (attorneys.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        const totalPages = totalResults > 0 ? Math.ceil(totalResults / this.pageSize) : 0;
        if (totalPages > 0 && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new SouthCarolinaScraper();
