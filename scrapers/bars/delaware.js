/**
 * Delaware Bar Association Scraper
 *
 * Source: https://rp470541.doelegal.com/vwPublicSearch/Show-VwPublicSearch-Table.aspx
 * Method: ASP.NET form with auto-complete AJAX and ViewState pagination
 *
 * The DE Bar uses a DOE Legal ASP.NET WebForms application for its public
 * attorney search. The form supports auto-complete (AJAX) for name fields
 * and returns: Attorney name, Firm, Phone, Supreme Court ID, Admit Date, Status.
 * Paging supports 5-250 records per page.
 *
 * Flow:
 * 1. GET the search page to obtain __VIEWSTATE and hidden fields
 * 2. POST search form with name/city filters
 * 3. Parse the ASP.NET GridView table results
 * 4. Paginate via __doPostBack events with updated ViewState
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class DelawareScraper extends BaseScraper {
  constructor() {
    super({
      name: 'delaware',
      stateCode: 'DE',
      baseUrl: 'https://rp470541.doelegal.com/vwPublicSearch/Show-VwPublicSearch-Table.aspx',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':        'Administrative',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business/Corporate',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'elder':                 'Elder Law',
        'employment':            'Employment',
        'labor':                 'Labor/Employment',
        'environmental':         'Environmental',
        'estate planning':       'Estate Planning',
        'estate':                'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Wilmington', 'Dover', 'Newark', 'Middletown',
        'Georgetown', 'Milford', 'Smyrna', 'Lewes',
      ],
    });
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for DE DOE Legal ASP.NET form`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for DE DOE Legal ASP.NET form`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for DE DOE Legal ASP.NET form`);
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
   * Extract all hidden form fields from ASP.NET page.
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
   * Parse attorneys from the ASP.NET GridView table.
   * Expected columns: Attorney Name, Firm, Phone, Supreme Court ID, Admit Date, Status
   */
  _parseAttorneys($) {
    const attorneys = [];

    $('table.GridView tr, table[id*="GridView"] tr, table[id*="gv"] tr, table.rgMasterTable tr, table tbody tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 3) return;

      // DOE Legal GridView columns: Name, Firm, Phone, Supreme Court ID, Admit Date, Status
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      let fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|attorney|search|no\s|page)/i.test(fullName)) return;
      // Skip pagination row cells
      if (fullName.match(/^\d+$/) && fullName.length <= 3) return;

      const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const phone = cells.length > 2 ? $(cells[2]).text().trim().replace(/[^\d()-.\s+]/g, '') : '';
      const supremeCourtId = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const admitDate = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const barStatus = cells.length > 5 ? $(cells[5]).text().trim() : 'Active';

      // Handle "Last, First" name format
      let firstName, lastName;
      if (fullName.includes(',')) {
        const nameParts = fullName.split(',');
        lastName = nameParts[0].trim();
        firstName = (nameParts[1] || '').trim().split(/\s+/)[0];
        fullName = `${firstName} ${lastName}`.trim();
      } else {
        const parsed = this.splitName(fullName);
        firstName = parsed.firstName;
        lastName = parsed.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: '',
        state: 'DE',
        phone: phone,
        email: '',
        website: '',
        bar_number: supremeCourtId.replace(/[^\d]/g, ''),
        admission_date: admitDate,
        bar_status: barStatus || 'Active',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://rp470541.doelegal.com${profileUrl}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Extract result count from ASP.NET page.
   */
  _extractResultCountFromHtml($) {
    const text = $('body').text();

    const matchOf = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchRecords = text.match(/(\d+)\s+records?/i);
    if (matchRecords) return parseInt(matchRecords[1], 10);

    const matchItems = text.match(/Items?\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchItems) return parseInt(matchItems[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Async generator that yields attorney records from the DE Bar directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for DE — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Step 1: GET the search page to obtain ViewState
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
        log.warn(`No __VIEWSTATE found on DE Bar search page`);
      }

      // Step 2: POST search form
      // DOE Legal ASP.NET forms use ctl00$ prefixed control names
      const formData = {
        ...hiddenFields,
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
      };

      // Set search fields — DOE Legal uses various naming patterns
      const searchFieldPrefixes = [
        'ctl00$ContentPlaceHolder1$',
        'ctl00$MainContent$',
        'ctl00$cphContent$',
        '',
      ];

      for (const prefix of searchFieldPrefixes) {
        formData[`${prefix}txtLastName`] = '';
        formData[`${prefix}txtFirstName`] = '';
        formData[`${prefix}txtCity`] = city;
        formData[`${prefix}txtFirm`] = '';
        formData[`${prefix}ddlStatus`] = 'Active';
        formData[`${prefix}ddlPageSize`] = String(this.pageSize);
        formData[`${prefix}btnSearch`] = 'Search';
      }

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
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      log.success(`Found ${totalResults || attorneys.length} results for ${city}`);

      // Assign city to results (DOE Legal table may not include city column)
      for (const attorney of attorneys) {
        if (!attorney.city) attorney.city = city;
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }
        yield this.transformResult(attorney, practiceArea);
      }

      // Step 3: Paginate via ASP.NET GridView postback
      let currentHtml = searchResponse.body;
      let currentCookies = searchResponse.cookies || sessionCookies;
      let pagesFetched = 1;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const $current = cheerio.load(currentHtml);

        // ASP.NET GridView pagination: look for __doPostBack links with Page$ arguments
        const nextLink = $current('a[href*="__doPostBack"]').filter((_, el) => {
          const href = $current(el).attr('href') || '';
          const text = $current(el).text().trim();
          // Match "Next", ">", or the next page number
          return (text === '>' || text === 'Next' || text === '...' || text === String(pagesFetched + 1)) &&
                 href.includes('__doPostBack');
        }).first();

        if (!nextLink.length) break;

        const postbackMatch = (nextLink.attr('href') || '').match(/__doPostBack\('([^']+)','([^']*)'\)/);
        if (!postbackMatch) break;

        const pageHidden = this._extractHiddenFields(currentHtml);
        const pageFormData = {
          ...pageHidden,
          '__EVENTTARGET': postbackMatch[1].replace(/\\'/g, "'"),
          '__EVENTARGUMENT': postbackMatch[2].replace(/\\'/g, "'"),
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
          if (!attorney.city) attorney.city = city;
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

module.exports = new DelawareScraper();
