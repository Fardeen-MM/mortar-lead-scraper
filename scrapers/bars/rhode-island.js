/**
 * Rhode Island Judiciary Scraper
 *
 * Source: https://rijrs.courts.ri.gov/rijrs/attorney.do
 * Method: Java Servlet HTML form (Spring .do endpoint)
 *
 * The RI Judiciary uses a Spring MVC web application with .do endpoints
 * for attorney search. The form POSTs to attorney.do and returns HTML
 * results. The system supports search by name, bar number, city, and
 * status.
 *
 * Flow:
 * 1. GET the search form page to obtain any session tokens/cookies
 * 2. POST search form with city and status filters
 * 3. Parse HTML table results
 * 4. Paginate through result pages
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class RhodeIslandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'rhode_island',
      stateCode: 'RI',
      baseUrl: 'https://rijrs.courts.ri.gov/rijrs/attorney.do',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'ADMIN',
        'bankruptcy':            'BANKR',
        'business':              'BUS',
        'civil litigation':      'CIVIL',
        'commercial':            'COMM',
        'corporate':             'CORP',
        'criminal':              'CRIM',
        'criminal defense':      'CRIM',
        'elder':                 'ELDER',
        'employment':            'EMPL',
        'labor':                 'EMPL',
        'environmental':         'ENVIR',
        'estate planning':       'ESTATE',
        'estate':                'ESTATE',
        'family':                'FAMILY',
        'family law':            'FAMILY',
        'immigration':           'IMMIG',
        'intellectual property': 'IP',
        'personal injury':       'PI',
        'real estate':           'REAL',
        'tax':                   'TAX',
        'tax law':               'TAX',
        'workers comp':          'WC',
      },
      defaultCities: [
        'Providence', 'Warwick', 'Cranston', 'Pawtucket',
        'East Providence', 'Woonsocket', 'Newport', 'Westerly',
      ],
    });

    this.searchUrl = 'https://rijrs.courts.ri.gov/rijrs/attorney.do';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for RI Judiciary Spring form`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for RI Judiciary Spring form`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for RI Judiciary Spring form`);
  }

  /**
   * HTTP GET with cookie tracking for session management.
   */
  _httpGet(url, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || 443,
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
   * HTTP POST with URL-encoded form data and session cookies.
   * Spring MVC .do endpoints require proper session handling.
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
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Referer': this.searchUrl,
          'Connection': 'keep-alive',
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
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Extract hidden form fields (CSRF tokens, etc.) from Spring MVC forms.
   */
  _extractFormFields(html) {
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
   * Parse attorneys from Spring MVC results HTML.
   */
  _parseAttorneys($) {
    const attorneys = [];

    // Spring MVC apps typically render results in HTML tables
    $('table.results tr, table.list tr, table.data tr, table[id*="result"] tr, table[class*="result"] tr, table tbody tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;
      if ($row.closest('table').find('th').length === 0 && $row.is(':first-child')) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      // RI Judiciary format: Attorney Name, Bar #, Status, City, Phone
      let fullName = '';
      let barNumber = '';
      let barStatus = '';
      let city = '';
      let phone = '';
      let email = '';
      let profileUrl = '';

      // First cell is typically the name (possibly linked)
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|attorney|search|no\s)/i.test(fullName)) return;

      // Parse remaining cells based on count
      if (cells.length >= 2) barNumber = $(cells[1]).text().trim();
      if (cells.length >= 3) barStatus = $(cells[2]).text().trim();
      if (cells.length >= 4) city = $(cells[3]).text().trim();
      if (cells.length >= 5) phone = $(cells[4]).text().trim().replace(/[^\d()-.\s+]/g, '');

      // If barNumber looks like a status, shift columns
      if (/active|inactive|suspended|retired/i.test(barNumber)) {
        barStatus = barNumber;
        barNumber = '';
        if (cells.length >= 3) city = $(cells[2]).text().trim();
        if (cells.length >= 4) phone = $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '');
      }

      // Check for email
      const emailLink = $row.find('a[href^="mailto:"]');
      if (emailLink.length) {
        email = emailLink.attr('href').replace('mailto:', '').trim();
      }

      // Handle "Last, First" name format common in judicial systems
      const { firstName, lastName } = fullName.includes(',')
        ? { firstName: fullName.split(',')[1]?.trim().split(/\s+/)[0] || '', lastName: fullName.split(',')[0]?.trim() || '' }
        : this.splitName(fullName);

      const normalizedName = fullName.includes(',')
        ? `${firstName} ${lastName}`.trim()
        : fullName;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: normalizedName,
        firm_name: '',
        city: city,
        state: 'RI',
        phone: phone,
        email: email,
        website: '',
        bar_number: barNumber.replace(/[^\d]/g, ''),
        bar_status: barStatus || 'Active',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://rijrs.courts.ri.gov${profileUrl}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Extract result count from page.
   */
  _extractResultCountFromHtml($) {
    const text = $('body').text();

    const matchOf = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchReturned = text.match(/returned\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchReturned) return parseInt(matchReturned[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total[:\s]+([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Async generator that yields attorney records from the RI Judiciary directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for RI — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Step 1: GET the search form page for session/CSRF tokens
      let pageResponse;
      try {
        await rateLimiter.wait();
        pageResponse = await this._httpGet(this.searchUrl, rateLimiter);
      } catch (err) {
        log.error(`Failed to load search page: ${err.message}`);
        continue;
      }

      if (pageResponse.statusCode !== 200) {
        log.error(`Search page returned ${pageResponse.statusCode}`);
        continue;
      }

      const sessionCookies = pageResponse.cookies;
      const formFields = this._extractFormFields(pageResponse.body);

      // Step 2: POST search form with city filter
      // Spring MVC forms typically use method=POST with action parameter
      const formData = {
        ...formFields,
        'lastName': '',
        'firstName': '',
        'barNumber': '',
        'city': city,
        'state': 'RI',
        'status': 'Active',
        'action': 'search',
        'method': 'search',
        'submit': 'Search',
      };

      // Spring MVC may use different field naming
      formData['attorney.lastName'] = '';
      formData['attorney.firstName'] = '';
      formData['attorney.city'] = city;
      formData['attorney.status'] = 'Active';

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        if (page > 1) {
          formData['page'] = String(page);
          formData['pageNumber'] = String(page);
          formData['startRow'] = String((page - 1) * this.pageSize);
        }

        log.info(`Page ${page} — POST ${this.searchUrl} [City=${city}]`);

        let searchResponse;
        try {
          await rateLimiter.wait();
          searchResponse = await this.httpPost(this.searchUrl, formData, rateLimiter, sessionCookies);
        } catch (err) {
          log.error(`Search POST failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (searchResponse.statusCode === 429 || searchResponse.statusCode === 403) {
          log.warn(`Got ${searchResponse.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(searchResponse.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (searchResponse.statusCode !== 200) {
          log.error(`Search returned ${searchResponse.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(searchResponse.body)) {
          log.warn(`CAPTCHA detected for ${city} — skipping`);
          yield { _captcha: true, city };
          break;
        }

        const $ = cheerio.load(searchResponse.body);

        if (page === 1) {
          totalResults = this._extractResultCountFromHtml($);
          if (totalResults > 0) {
            const totalPages = Math.ceil(totalResults / this.pageSize);
            log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
          }
        }

        const attorneys = this._parseAttorneys($);

        if (attorneys.length === 0) {
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          }
          break;
        }

        if (page === 1 && totalResults === 0) {
          totalResults = attorneys.length;
          log.success(`Found ${attorneys.length} results for ${city}`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === '>' || text === '>>';
        }).length > 0;

        const totalPages = totalResults > 0 ? Math.ceil(totalResults / this.pageSize) : 0;

        if (!hasNext && (totalPages === 0 || page >= totalPages)) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new RhodeIslandScraper();
