/**
 * Wisconsin Bar Association Scraper
 *
 * Source: https://www.wisbar.org/Pages/BasicLawyerSearch.aspx
 * Method: SharePoint with reCAPTCHA — best-effort scraper
 *
 * The State Bar of Wisconsin uses a SharePoint-based directory with
 * reCAPTCHA protection. This scraper attempts to:
 *  1. Fetch the search page without triggering CAPTCHA
 *  2. Submit the ASP.NET/SharePoint form
 *  3. If reCAPTCHA blocks, yield a clear captcha signal
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WisconsinScraper extends BaseScraper {
  constructor() {
    super({
      name: 'wisconsin',
      stateCode: 'WI',
      baseUrl: 'https://www.wisbar.org/Pages/BasicLawyerSearch.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
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
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Milwaukee', 'Madison', 'Green Bay', 'Kenosha',
        'Racine', 'Appleton', 'Waukesha', 'Eau Claire',
      ],
    });

    this.origin = 'https://www.wisbar.org';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SharePoint + reCAPTCHA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SharePoint + reCAPTCHA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SharePoint + reCAPTCHA`);
  }

  /**
   * HTTP POST with form data for SharePoint/ASP.NET postback.
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
          const existing = this._cookies ? this._cookies.split('; ') : [];
          const newCookies = setCookies.map(c => c.split(';')[0]);
          const allCookies = [...existing, ...newCookies];
          // Deduplicate by cookie name
          const cookieMap = {};
          for (const c of allCookies) {
            const name = c.split('=')[0];
            cookieMap[name] = c;
          }
          this._cookies = Object.values(cookieMap).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
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
          const existing = this._cookies ? this._cookies.split('; ') : [];
          const newCookies = setCookies.map(c => c.split(';')[0]);
          const allCookies = [...existing, ...newCookies];
          const cookieMap = {};
          for (const c of allCookies) {
            const name = c.split('=')[0];
            cookieMap[name] = c;
          }
          this._cookies = Object.values(cookieMap).join('; ');
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
   * Detect reCAPTCHA in the page.
   */
  _detectRecaptcha(body) {
    return body.includes('recaptcha') ||
           body.includes('reCAPTCHA') ||
           body.includes('grecaptcha') ||
           body.includes('g-recaptcha') ||
           body.includes('recaptcha/api') ||
           this.detectCaptcha(body);
  }

  /**
   * Extract SharePoint/ASP.NET form state.
   */
  _extractFormState($) {
    const state = {};
    $('input[type="hidden"]').each((_, el) => {
      const name = $(el).attr('name') || '';
      const value = $(el).attr('value') || '';
      if (name) state[name] = value;
    });
    return state;
  }

  /**
   * Parse attorney records from SharePoint search results.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // WisBar may use various result formats
    $('table tr, .search-results tr, .ms-listviewtable tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|page|result)/i.test(fullName)) return;
      if (fullName.length < 2 || fullName.length > 100) return;

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
        state: 'WI',
        phone: phone,
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
      });
    });

    // Fallback: div-based results
    if (attorneys.length === 0) {
      $('.lawyer-result, .search-result, .result-item, .attorney-card').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name, .attorney-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 2) return;

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
          firm_name: $el.find('.firm, .firm-name').text().trim(),
          city: $el.find('.city, .location').text().trim(),
          state: 'WI',
          phone: ($el.find('.phone').text().trim() || '').replace(/[^\d()-.\s+]/g, ''),
          email: $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '',
          website: '',
          bar_number: $el.find('.bar-number').text().trim().replace(/[^0-9]/g, ''),
          bar_status: $el.find('.status').text().trim() || 'Active',
          profile_url: nameEl.attr('href') ? `${this.origin}${nameEl.attr('href')}` : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Wisconsin SharePoint + reCAPTCHA.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    this._cookies = null;

    log.scrape('Attempting to access WisBar lawyer search...');
    log.warn('WI Bar uses SharePoint + reCAPTCHA — this is a best-effort scraper.');

    // Step 1: Fetch the search page
    let pageBody = '';
    let hasRecaptcha = false;
    let formState = {};

    try {
      await rateLimiter.wait();
      const response = await this._httpGetWithCookies(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.error(`WisBar returned status ${response.statusCode}`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${response.statusCode} from WisBar` };
        return;
      }

      pageBody = response.body;
      hasRecaptcha = this._detectRecaptcha(pageBody);

      const $ = cheerio.load(pageBody);
      formState = this._extractFormState($);
    } catch (err) {
      log.error(`Failed to fetch WisBar: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    if (hasRecaptcha) {
      log.warn('WI Bar: reCAPTCHA detected on search page.');
      log.warn(`WI: The lawyer search at ${this.baseUrl} requires reCAPTCHA validation.`);
      log.warn('WI: Attempting search without CAPTCHA token — this may fail.');
    }

    // Step 2: Try submitting the search form (may work without CAPTCHA on some requests)
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

        // Build SharePoint form data
        const formData = {
          ...formState,
        };

        // Try common WisBar form field names
        const cityFieldNames = ['ctl00$PlaceHolderMain$txtCity', 'txtCity', 'City', 'city'];
        for (const name of cityFieldNames) {
          formData[name] = city;
        }

        const searchBtnNames = ['ctl00$PlaceHolderMain$btnSearch', 'btnSearch', 'Search'];
        for (const name of searchBtnNames) {
          formData[name] = 'Search';
        }

        if (page > 1) {
          formData['__EVENTTARGET'] = 'ctl00$PlaceHolderMain$gvResults';
          formData['__EVENTARGUMENT'] = `Page$${page}`;
        }

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

        // Check if reCAPTCHA stopped us
        if (this._detectRecaptcha(response.body) && !pageBody.includes('search-result')) {
          log.warn(`reCAPTCHA blocked search for ${city}`);
          yield { _captcha: true, city, page, reason: 'reCAPTCHA validation required' };
          break;
        }

        // Update form state for pagination
        const $ = cheerio.load(response.body);
        formState = this._extractFormState($);

        const attorneys = this._parseAttorneys(response.body);

        if (attorneys.length === 0) {
          if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          break;
        }

        if (page === 1) {
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

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new WisconsinScraper();
