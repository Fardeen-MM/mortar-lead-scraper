/**
 * Louisiana State Bar Association Scraper
 *
 * Source: https://www.lsba.org/MD321654/MembershipDirectory.aspx
 * Method: Image CAPTCHA gate (Obout.Ajax.UI.Captcha) — best-effort scraper
 *
 * The LSBA member directory is gated by an image CAPTCHA using the
 * Obout.Ajax.UI.Captcha component. This scraper attempts to:
 *  1. Fetch the directory page
 *  2. Check if CAPTCHA is present
 *  3. If not gated, submit the ASP.NET form and parse results
 *  4. If CAPTCHA is detected, yield a captcha signal
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class LouisianaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'louisiana',
      stateCode: 'LA',
      baseUrl: 'https://www.lsba.org/MD321654/MembershipDirectory.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'admiralty':             'Admiralty/Maritime',
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
        'insurance':             'Insurance',
        'intellectual property': 'Intellectual Property',
        'oil and gas':           'Oil and Gas',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'New Orleans', 'Baton Rouge', 'Shreveport', 'Lafayette',
        'Lake Charles', 'Metairie', 'Kenner', 'Monroe',
      ],
    });

    this.origin = 'https://www.lsba.org';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CAPTCHA-gated ASP.NET form`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CAPTCHA-gated ASP.NET form`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CAPTCHA-gated ASP.NET form`);
  }

  /**
   * HTTP POST with form data for ASP.NET postback.
   */
  httpPost(url, formData, rateLimiter, headers = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Origin': this.origin,
          'Referer': this.baseUrl,
          'Connection': 'keep-alive',
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
          return resolve(this.httpGet(redirect, rateLimiter));
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
   * HTTP GET with cookie persistence.
   */
  _httpGetWithCookies(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Referer': this.baseUrl,
          ...(this._cookies ? { 'Cookie': this._cookies } : {}),
        },
        timeout: 15000,
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
          if (redirect.startsWith('/')) redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          return resolve(this._httpGetWithCookies(redirect, rateLimiter));
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
   * Detect Obout CAPTCHA component in the page.
   */
  _detectOboutCaptcha(body) {
    return body.includes('obout_Captcha') ||
           body.includes('Obout.Ajax.UI.Captcha') ||
           body.includes('CaptchaControl') ||
           body.includes('captcha_image') ||
           body.includes('captchaInput') ||
           this.detectCaptcha(body);
  }

  /**
   * Extract ASP.NET form state (__VIEWSTATE, __EVENTVALIDATION, etc.)
   */
  _extractAspNetState($) {
    const state = {};
    $('input[type="hidden"]').each((_, el) => {
      const name = $(el).attr('name') || '';
      const value = $(el).attr('value') || '';
      if (name.startsWith('__') || name.includes('ViewState') || name.includes('Event')) {
        state[name] = value;
      }
    });
    return state;
  }

  /**
   * Parse attorney records from ASP.NET GridView results.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // ASP.NET GridView renders as table with specific class patterns
    $('table.GridView tr, table[id*="GridView"] tr, .gvResults tr, table tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;
      if ($row.hasClass('pager') || $row.hasClass('header')) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstCellText = $(cells[0]).text().trim();
      if (!firstCellText || /^(name|search|page|first|last)/i.test(firstCellText)) return;

      // LSBA directory format varies: Name | Bar# | City | Status | Phone
      const nameLink = $(cells[0]).find('a');
      let fullName = (nameLink.length ? nameLink.text() : firstCellText).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || fullName.length < 2) return;

      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
        fullName = `${firstName} ${lastName}`.trim();
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
        full_name: fullName,
        firm_name: '',
        city: city,
        state: 'LA',
        phone: phone,
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl.startsWith('/') ? '' : '/'}${profileUrl}`) : '',
      });
    });

    return attorneys;
  }

  /**
   * Override search() for Louisiana with Obout CAPTCHA gate.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    this._cookies = null;

    log.scrape('Attempting to access LSBA member directory...');
    log.warn('LA Bar uses Obout image CAPTCHA — this is a best-effort scraper.');

    // Step 1: Fetch directory page
    let pageBody = '';
    let hasCaptcha = false;
    let aspState = {};

    try {
      await rateLimiter.wait();
      const response = await this._httpGetWithCookies(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.error(`LSBA directory returned status ${response.statusCode}`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode} from LSBA` };
        return;
      }

      pageBody = response.body;
      hasCaptcha = this._detectOboutCaptcha(pageBody);

      const $ = cheerio.load(pageBody);
      aspState = this._extractAspNetState($);
    } catch (err) {
      log.error(`Failed to fetch LSBA directory: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    if (hasCaptcha) {
      log.warn(`LSBA: Image CAPTCHA (Obout.Ajax.UI.Captcha) is blocking automated access.`);
      log.warn(`LA: The member directory at ${this.baseUrl} requires solving an image CAPTCHA.`);
      log.warn(`LA: A headless browser with OCR or manual CAPTCHA solving is required.`);

      // Check if there are any results already visible (some sites show results behind CAPTCHA)
      const preloadedAttorneys = this._parseAttorneys(pageBody);
      if (preloadedAttorneys.length > 0) {
        log.info(`Found ${preloadedAttorneys.length} pre-loaded attorneys before CAPTCHA gate`);
        yield { _cityProgress: { current: 1, total: 1 } };
        for (const attorney of preloadedAttorneys) {
          yield this.transformResult(attorney, practiceArea);
        }
      }

      yield { _captcha: true, city: 'all', reason: 'Obout image CAPTCHA blocks automated access' };
      return;
    }

    // Step 2: No CAPTCHA detected — try submitting the search form
    log.success('No CAPTCHA detected on initial load — attempting search');
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
          ...aspState,
          'txtCity': city,
          'ddlState': 'LA',
          'ddlStatus': 'Active',
          'btnSearch': 'Search',
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

        // Check if CAPTCHA appeared after form submission
        if (this._detectOboutCaptcha(response.body)) {
          log.warn(`CAPTCHA appeared after form submission for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);
        aspState = this._extractAspNetState($);

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

module.exports = new LouisianaScraper();
