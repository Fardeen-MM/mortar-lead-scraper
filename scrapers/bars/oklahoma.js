/**
 * Oklahoma Bar Association Scraper
 *
 * Source: https://ams.okbar.org/eweb/startpage.aspx?site=FALWEB
 * Method: HTML form POST with JavaScript and ASP.NET ViewState
 *
 * The OBA uses an eWeb/iMIS-based ASP.NET application for its Find-A-Lawyer
 * directory. This is opt-in only — only attorneys who have elected to be listed
 * will appear in results. The form supports 40+ practice areas.
 *
 * Flow:
 * 1. GET the search page to obtain __VIEWSTATE and session cookies
 * 2. POST search form with city, practice area, and status filters
 * 3. Parse HTML table results
 * 4. Paginate through result pages using postback events
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class OklahomaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'oklahoma',
      stateCode: 'OK',
      baseUrl: 'https://ams.okbar.org/eweb/startpage.aspx?site=FALWEB',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business/Commercial',
        'civil litigation':      'Civil Litigation',
        'civil rights':          'Civil Rights',
        'collections':           'Collections',
        'corporate':             'Corporate',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'elder':                 'Elder Law',
        'employment':            'Employment/Labor',
        'labor':                 'Employment/Labor',
        'environmental':         'Environmental',
        'estate planning':       'Estate Planning/Probate',
        'estate':                'Estate Planning/Probate',
        'family':                'Family',
        'family law':            'Family',
        'government':            'Government',
        'health':                'Health Care',
        'immigration':           'Immigration',
        'insurance':             'Insurance',
        'intellectual property': 'Intellectual Property',
        'juvenile':              'Juvenile',
        'mediation':             'Mediation/ADR',
        'military':              'Military Law',
        'oil and gas':           'Oil & Gas/Energy',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'securities':            'Securities',
        'social security':       'Social Security',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'tribal':                'Tribal/Indian Law',
        'water law':             'Water Rights',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Oklahoma City', 'Tulsa', 'Norman', 'Edmond',
        'Moore', 'Midwest City', 'Broken Arrow', 'Lawton',
      ],
    });
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for OK Bar eWeb form`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for OK Bar eWeb form`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for OK Bar eWeb form`);
  }

  /**
   * HTTP GET with cookie tracking.
   */
  _httpGet(url, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Encoding': 'identity',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 20000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          const mergedCookies = [cookies, setCookies].filter(Boolean).join('; ');
          return resolve(this._httpGet(redirect, rateLimiter, mergedCookies));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: [cookies, setCookies].filter(Boolean).join('; '),
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with URL-encoded form data and cookie tracking.
   */
  httpPost(url, formData, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Referer': this.baseUrl,
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 20000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: [cookies, setCookies].filter(Boolean).join('; '),
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Extract all hidden form fields from HTML.
   */
  _extractHiddenFields(html) {
    const fields = {};
    const regex = /<input[^>]*type="hidden"[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g;
    let match;
    while ((match = regex.exec(html)) !== null) {
      fields[match[1]] = match[2];
    }
    const regex2 = /<input[^>]*value="([^"]*)"[^>]*type="hidden"[^>]*name="([^"]*)"/g;
    while ((match = regex2.exec(html)) !== null) {
      if (!fields[match[2]]) fields[match[2]] = match[1];
    }
    return fields;
  }

  /**
   * Parse attorneys from OBA eWeb results HTML.
   */
  _parseAttorneys($) {
    const attorneys = [];

    // eWeb/iMIS typically renders results in HTML tables or div grids
    $('table tr, .search-result-item, .fal-result').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2 && !$row.hasClass('search-result-item') && !$row.hasClass('fal-result')) return;

      let fullName = '';
      let profileUrl = '';
      let firmName = '';
      let city = '';
      let phone = '';
      let email = '';
      let practiceAreas = '';

      if (cells.length >= 2) {
        // Table format: Name, Firm, City, Phone, Practice Areas
        const nameCell = $(cells[0]);
        const nameLink = nameCell.find('a');
        fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
        profileUrl = nameLink.attr('href') || '';

        if (cells.length >= 2) firmName = $(cells[1]).text().trim();
        if (cells.length >= 3) city = $(cells[2]).text().trim();
        if (cells.length >= 4) phone = $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '');
        if (cells.length >= 5) practiceAreas = $(cells[4]).text().trim();
      } else {
        // Div-based format
        const nameEl = $row.find('.name, .member-name, a').first();
        fullName = nameEl.text().trim();
        profileUrl = nameEl.attr('href') || '';
        firmName = $row.find('.firm, .company').text().trim();
        city = $row.find('.city, .location').text().trim();
        phone = ($row.find('.phone').text().trim() || '').replace(/[^\d()-.\s+]/g, '');
      }

      // Check for email
      const emailLink = $row.find('a[href^="mailto:"]');
      if (emailLink.length) {
        email = emailLink.attr('href').replace('mailto:', '').trim();
      }

      if (!fullName || /^(name|search|find|member)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'OK',
        phone: phone,
        email: email,
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://ams.okbar.org${profileUrl}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Extract result count from page.
   */
  _extractResultCountFromHtml($) {
    const text = $('body').text();
    const matchOf = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?|lawyers?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchCount = text.match(/(\d+)\s+match(?:es)?/i);
    if (matchCount) return parseInt(matchCount[1], 10);

    return 0;
  }

  /**
   * Async generator that yields attorney records from the OBA directory.
   * Note: This is opt-in only — only listed attorneys will appear.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for OK — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Step 1: GET the search page for ViewState and cookies
      let pageResponse;
      try {
        await rateLimiter.wait();
        pageResponse = await this._httpGet(this.baseUrl, rateLimiter);
      } catch (err) {
        log.error(`Failed to load search page: ${err.message}`);
        continue;
      }

      if (pageResponse.statusCode !== 200) {
        log.error(`Search page returned ${pageResponse.statusCode}`);
        continue;
      }

      const sessionCookies = pageResponse.cookies;
      const hiddenFields = this._extractHiddenFields(pageResponse.body);

      if (!hiddenFields.__VIEWSTATE) {
        log.warn(`No __VIEWSTATE found — attempting search without it`);
      }

      // Step 2: POST search form
      const formData = {
        ...hiddenFields,
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
      };

      // Set city filter (eWeb uses various naming conventions)
      const cityFieldNames = [
        'txtCity', 'City', 'ctl00$MainContent$txtCity',
        'ctl00$Content$txtCity', 'txtLocation', 'CityText',
      ];
      for (const fieldName of cityFieldNames) {
        formData[fieldName] = city;
      }

      // Set state
      formData['txtState'] = 'OK';
      formData['State'] = 'OK';

      // Set practice area if available
      if (practiceCode) {
        const paFieldNames = [
          'ddlPracticeArea', 'PracticeArea', 'ctl00$MainContent$ddlPracticeArea',
          'ctl00$Content$ddlPracticeArea', 'PracticeAreaDropDown',
        ];
        for (const fieldName of paFieldNames) {
          formData[fieldName] = practiceCode;
        }
      }

      // Submit button
      formData['btnSearch'] = 'Search';
      formData['Submit'] = 'Search';

      let searchResponse;
      try {
        await rateLimiter.wait();
        searchResponse = await this.httpPost(this.baseUrl, formData, rateLimiter, sessionCookies);
      } catch (err) {
        log.error(`Search POST failed for ${city}: ${err.message}`);
        continue;
      }

      if (searchResponse.statusCode === 429 || searchResponse.statusCode === 403) {
        log.warn(`Got ${searchResponse.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(searchResponse.statusCode);
        if (!shouldRetry) break;
        continue;
      }

      if (searchResponse.statusCode !== 200) {
        log.error(`Search returned ${searchResponse.statusCode} for ${city}`);
        continue;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(searchResponse.body)) {
        log.warn(`CAPTCHA detected for ${city} — skipping`);
        yield { _captcha: true, city };
        continue;
      }

      const $ = cheerio.load(searchResponse.body);
      const totalResults = this._extractResultCountFromHtml($);
      const attorneys = this._parseAttorneys($);

      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city} (opt-in directory)`);
        continue;
      }

      log.success(`Found ${totalResults || attorneys.length} results for ${city}`);

      for (const attorney of attorneys) {
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }
        yield this.transformResult(attorney, practiceArea);
      }

      // Step 3: Paginate
      let currentHtml = searchResponse.body;
      let currentCookies = searchResponse.cookies || sessionCookies;
      let pagesFetched = 1;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const $current = cheerio.load(currentHtml);

        // Look for next page postback link
        const nextLink = $current('a[href*="__doPostBack"]').filter((_, el) => {
          const text = $current(el).text().trim().toLowerCase();
          return text === 'next' || text === '>' || text === String(pagesFetched + 1);
        }).first();

        if (!nextLink.length) break;

        const postbackMatch = (nextLink.attr('href') || '').match(/__doPostBack\('([^']+)','([^']*)'\)/);
        if (!postbackMatch) break;

        const pageHidden = this._extractHiddenFields(currentHtml);
        const pageFormData = {
          ...pageHidden,
          '__EVENTTARGET': postbackMatch[1],
          '__EVENTARGUMENT': postbackMatch[2],
        };

        let pageResponse2;
        try {
          await rateLimiter.wait();
          pageResponse2 = await this.httpPost(this.baseUrl, pageFormData, rateLimiter, currentCookies);
        } catch (err) {
          log.error(`Pagination failed for ${city} page ${pagesFetched + 1}: ${err.message}`);
          break;
        }

        if (pageResponse2.statusCode !== 200) break;

        const $page = cheerio.load(pageResponse2.body);
        const pageAttorneys = this._parseAttorneys($page);

        if (pageAttorneys.length === 0) break;

        for (const attorney of pageAttorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        currentHtml = pageResponse2.body;
        currentCookies = pageResponse2.cookies || currentCookies;
        pagesFetched++;
      }

      log.success(`Completed ${pagesFetched} page(s) for ${city}`);
    }
  }
}

module.exports = new OklahomaScraper();
