/**
 * Missouri Bar Association Scraper
 *
 * Source: https://mobar.org/public/LawyerSearch.aspx (opt-in search)
 *         https://mobar.org/public/LawyerDirectory.aspx (all lawyers in good standing)
 * Method: ASP.NET POST with ViewState (Telerik RadGrid controls)
 *
 * The MO Bar uses an ASP.NET WebForms application with Telerik controls.
 * LawyerSearch.aspx is opt-in only; LawyerDirectory.aspx lists all lawyers
 * in good standing. Both use __doPostBack for pagination.
 *
 * Flow:
 * 1. GET the directory page to obtain __VIEWSTATE and hidden fields
 * 2. POST with search/filter params (city, practice area)
 * 3. Parse the results table
 * 4. Paginate via async postback with updated ViewState
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MissouriScraper extends BaseScraper {
  constructor() {
    super({
      name: 'missouri',
      stateCode: 'MO',
      baseUrl: 'https://mobar.org/public/LawyerSearch.aspx',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':        'ADM',
        'bankruptcy':            'BKR',
        'business':              'BUS',
        'civil litigation':      'CVL',
        'commercial':            'COM',
        'corporate':             'COR',
        'criminal':              'CRM',
        'criminal defense':      'CRM',
        'elder':                 'ELD',
        'employment':            'EMP',
        'labor':                 'EMP',
        'environmental':         'ENV',
        'estate planning':       'EST',
        'estate':                'EST',
        'family':                'FAM',
        'family law':            'FAM',
        'immigration':           'IMM',
        'insurance':             'INS',
        'intellectual property': 'IPR',
        'personal injury':       'PIJ',
        'real estate':           'REA',
        'tax':                   'TAX',
        'tax law':               'TAX',
        'workers comp':          'WKC',
      },
      defaultCities: [
        'Kansas City', 'St. Louis', 'Springfield', 'Columbia',
        'Independence', "Lee's Summit", "O'Fallon", 'St. Joseph',
      ],
    });

    this.directoryUrl = 'https://mobar.org/public/LawyerDirectory.aspx';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for MO Bar ASP.NET postback`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for MO Bar ASP.NET postback`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for MO Bar ASP.NET postback`);
  }

  /**
   * HTTP POST with URL-encoded form data and cookie support.
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
          'Referer': url,
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
    // Also match value before name
    const regex2 = /<input[^>]*value="([^"]*)"[^>]*type="hidden"[^>]*name="([^"]*)"/g;
    while ((match = regex2.exec(html)) !== null) {
      if (!fields[match[2]]) fields[match[2]] = match[1];
    }
    return fields;
  }

  /**
   * Extract result count from page text.
   */
  _extractResultCountFromHtml($) {
    const text = $('body').text();
    const matchOf = text.match(/(\d[\d,]*)\s+(?:results?|records?|attorneys?|lawyers?|members?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total:\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Parse attorneys from search results HTML.
   */
  _parseAttorneys($) {
    const attorneys = [];

    // Try table-based results (RadGrid or standard HTML table)
    $('table.rgMasterTable tr, table.gridview tr, table[id*="Grid"] tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return; // skip header rows

      const cells = $row.find('td');
      if (cells.length < 3) return;

      // MO Bar typically shows: Name, City, Phone, Practice Areas
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || fullName.toLowerCase() === 'name') return;

      const { firstName, lastName } = this.splitName(fullName);

      const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const phone = cells.length > 3 ? $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '') : '';

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'MO',
        phone: phone,
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://mobar.org${profileUrl}` : ''),
      });
    });

    // Fallback: div/card-based results
    if (attorneys.length === 0) {
      $('.lawyer-result, .search-result, .member-card, .directory-listing').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name, .lawyer-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileUrl = nameEl.attr('href') || '';
        const firmName = $el.find('.firm, .firm-name, .company').text().trim();
        const city = $el.find('.city, .location').text().trim();
        const phone = ($el.find('.phone, .telephone').text().trim() || '').replace(/[^\d()-.\s+]/g, '');
        const email = $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmName,
          city: city,
          state: 'MO',
          phone: phone,
          email: email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://mobar.org${profileUrl}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the MO Bar directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for MO — searching without filter`);
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
        pageResponse = await this.httpGet(this.baseUrl, rateLimiter);
      } catch (err) {
        log.error(`Failed to load search page: ${err.message}`);
        continue;
      }

      if (pageResponse.statusCode !== 200) {
        log.error(`Search page returned ${pageResponse.statusCode}`);
        continue;
      }

      const hiddenFields = this._extractHiddenFields(pageResponse.body);
      if (!hiddenFields.__VIEWSTATE) {
        log.error(`Could not extract __VIEWSTATE from MO Bar search page`);
        continue;
      }

      // Step 2: POST search form with city and optional practice area
      const formData = {
        ...hiddenFields,
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
      };

      // Set city field (common ASP.NET naming patterns)
      const cityFieldNames = ['txtCity', 'City', 'ctl00$MainContent$txtCity', 'ctl00$ContentPlaceHolder1$txtCity'];
      for (const fieldName of cityFieldNames) {
        formData[fieldName] = city;
      }

      // Set practice area if available
      if (practiceCode) {
        const paFieldNames = ['ddlPracticeArea', 'PracticeArea', 'ctl00$MainContent$ddlPracticeArea', 'ctl00$ContentPlaceHolder1$ddlPracticeArea'];
        for (const fieldName of paFieldNames) {
          formData[fieldName] = practiceCode;
        }
      }

      // Submit button
      const submitNames = ['btnSearch', 'ctl00$MainContent$btnSearch', 'ctl00$ContentPlaceHolder1$btnSearch'];
      for (const fieldName of submitNames) {
        formData[fieldName] = 'Search';
      }

      let searchResponse;
      try {
        await rateLimiter.wait();
        searchResponse = await this.httpPost(this.baseUrl, formData, rateLimiter);
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

      if (totalResults > 0) {
        log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
      }

      const attorneys = this._parseAttorneys($);
      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      if (totalResults === 0) {
        log.success(`Found ${attorneys.length} results for ${city}`);
      }

      for (const attorney of attorneys) {
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }
        yield this.transformResult(attorney, practiceArea);
      }

      // Step 3: Paginate through remaining pages
      let currentCookies = searchResponse.cookies || '';
      let currentHtml = searchResponse.body;
      let pagesFetched = 1;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Look for next page link or postback
        const $current = cheerio.load(currentHtml);
        const nextLink = $current('a[href*="__doPostBack"][href*="Page$Next"], a[href*="__doPostBack"][href*="Page$' + (pagesFetched + 1) + '"]').first();

        if (!nextLink.length) {
          log.info(`No more pages for ${city}`);
          break;
        }

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

module.exports = new MissouriScraper();
