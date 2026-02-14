/**
 * West Virginia Bar Association Scraper
 *
 * Source: https://mywvbar.org/searchpage
 * Method: Returns 403 Forbidden — best-effort with browser headers
 *
 * The WV Bar association's member directory blocks automated requests
 * with a 403 Forbidden response. This scraper attempts to:
 *  1. Fetch the page with full browser headers
 *  2. Establish a session with cookies
 *  3. Submit the search form and parse results
 *  4. If still blocked, yield a captcha signal
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WestVirginiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'west-virginia',
      stateCode: 'WV',
      baseUrl: 'https://mywvbar.org/searchpage',
      pageSize: 25,
      practiceAreaCodes: {
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'elder':                 'Elder Law',
        'employment':            'Employment Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'insurance':             'Insurance Law',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Charleston', 'Huntington', 'Morgantown', 'Parkersburg',
        'Wheeling', 'Martinsburg', 'Beckley', 'Clarksburg',
      ],
    });

    this.origin = 'https://mywvbar.org';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for 403-blocked site`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for 403-blocked site`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for 403-blocked site`);
  }

  /**
   * Build full browser headers to bypass 403.
   */
  _getBrowserHeaders(rateLimiter) {
    return {
      'User-Agent': rateLimiter.getUserAgent(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache',
      'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'Connection': 'keep-alive',
    };
  }

  /**
   * HTTP GET with full browser headers and cookie persistence.
   */
  _httpGetBrowser(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._getBrowserHeaders(rateLimiter);
      headers['Referer'] = this.origin + '/';
      if (this._cookies) headers['Cookie'] = this._cookies;

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers,
        timeout: 15000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        const setCookies = res.headers['set-cookie'] || [];
        if (setCookies.length > 0) {
          const existing = this._cookies ? this._cookies.split('; ') : [];
          const newCookies = setCookies.map(c => c.split(';')[0]);
          const cookieMap = {};
          for (const c of [...existing, ...newCookies]) {
            const name = c.split('=')[0];
            cookieMap[name] = c;
          }
          this._cookies = Object.values(cookieMap).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
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
          'Referer': this.baseUrl,
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
          const existing = this._cookies ? this._cookies.split('; ') : [];
          const newCookies = setCookies.map(c => c.split(';')[0]);
          const cookieMap = {};
          for (const c of [...existing, ...newCookies]) {
            const name = c.split('=')[0];
            cookieMap[name] = c;
          }
          this._cookies = Object.values(cookieMap).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
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
   * Parse attorney records from results.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Try table-based results
    $('table tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|result)/i.test(fullName)) return;
      if (fullName.length < 2) return;

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
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
        city: cells.length > 2 ? $(cells[2]).text().trim() : '',
        state: 'WV',
        phone: cells.length > 3 ? $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '') : '',
        email: '',
        website: '',
        bar_number: cells.length > 4 ? $(cells[4]).text().trim().replace(/[^0-9]/g, '') : '',
        bar_status: 'Active',
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
      });
    });

    // Fallback: div-based results
    if (attorneys.length === 0) {
      $('.search-result, .member-result, .attorney-card, .result-item, .views-row').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name, .field-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 2) return;

        const { firstName, lastName } = this.splitName(fullName);

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $el.find('.firm, .firm-name, .field-firm').text().trim(),
          city: $el.find('.city, .location, .field-city').text().trim(),
          state: 'WV',
          phone: ($el.find('.phone, .field-phone').text().trim() || '').replace(/[^\d()-.\s+]/g, ''),
          email: $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: nameEl.attr('href') ? (nameEl.attr('href').startsWith('http') ? nameEl.attr('href') : `${this.origin}${nameEl.attr('href')}`) : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Override search() for West Virginia with 403 protection.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    this._cookies = null;

    log.scrape('Attempting to access WV Bar member directory...');
    log.warn('WV Bar returns 403 Forbidden — this is a best-effort scraper.');

    // Step 1: Try to establish a session
    let sessionOk = false;
    let formTokens = {};

    try {
      await rateLimiter.wait();
      const response = await this._httpGetBrowser(this.baseUrl, rateLimiter);

      if (response.statusCode === 200) {
        sessionOk = true;
        log.success('Session established with WV Bar directory');

        const $ = cheerio.load(response.body);
        $('input[type="hidden"]').each((_, el) => {
          const name = $(el).attr('name') || '';
          const value = $(el).attr('value') || '';
          if (name) formTokens[name] = value;
        });

        if (this.detectCaptcha(response.body)) {
          log.warn('CAPTCHA detected on WV Bar page');
          yield { _captcha: true, city: 'all', reason: 'CAPTCHA on search page' };
          return;
        }
      } else if (response.statusCode === 403) {
        log.warn('WV Bar returned 403 even with full browser headers');

        // Try the homepage first, then the search page
        log.info('Trying to establish session via homepage...');
        await rateLimiter.wait();
        const homeResp = await this._httpGetBrowser(`${this.origin}/`, rateLimiter);
        if (homeResp.statusCode === 200) {
          await rateLimiter.wait();
          const retryResp = await this._httpGetBrowser(this.baseUrl, rateLimiter);
          if (retryResp.statusCode === 200) {
            sessionOk = true;
            log.success('Session established via homepage redirect');
            const $ = cheerio.load(retryResp.body);
            $('input[type="hidden"]').each((_, el) => {
              const name = $(el).attr('name') || '';
              const value = $(el).attr('value') || '';
              if (name) formTokens[name] = value;
            });
          }
        }

        if (!sessionOk) {
          log.warn(`WV: The directory at ${this.baseUrl} blocks automated access with 403 Forbidden.`);
          log.warn(`WV: Even full browser headers cannot bypass the protection.`);
          log.warn(`WV: A headless browser or manual access is required.`);
          yield { _captcha: true, city: 'all', reason: '403 Forbidden — cannot bypass with browser headers' };
          return;
        }
      } else {
        log.error(`WV Bar returned status ${response.statusCode}`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode}` };
        return;
      }
    } catch (err) {
      log.error(`Failed to connect to WV Bar: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    // Step 2: Search by city
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const formData = {
          ...formTokens,
          'city': city,
          'state': 'WV',
          'status': 'Active',
          'op': 'Search',
        };

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const attorneys = this._parseAttorneys(response.body);

        if (attorneys.length === 0) {
          if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          break;
        }

        if (page === 1) {
          log.success(`Found ${attorneys.length} results for ${city}`);
        }

        for (const attorney of attorneys) {
          yield this.transformResult(attorney, practiceArea);
        }

        if (attorneys.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new WestVirginiaScraper();
