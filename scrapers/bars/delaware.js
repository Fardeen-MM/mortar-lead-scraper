/**
 * Delaware Bar Association Scraper
 *
 * Source: https://rp470541.doelegal.com/vwPublicSearch/Show-VwPublicSearch-Table.aspx
 * Method: ASP.NET form with auto-complete AJAX and ViewState pagination
 *
 * The DE Bar uses a DOE Legal ASP.NET WebForms application for its public
 * attorney search. The form supports auto-complete (AJAX) for name fields
 * and returns: Attorney name, Firm, Phone, Supreme Court ID, Admit Date, Status.
 * Paging supports 5-250 records per page (defaults to 250).
 *
 * The search is keyword-based (matches names, firms) — not city-based.
 * We iterate common last name prefixes to get broad coverage.
 *
 * Flow:
 * 1. GET the search page to obtain __VIEWSTATE and hidden fields
 * 2. POST search form with name keyword (SearchButton is an image button)
 * 3. Parse the #VwPublicSearchTableControlGrid table results
 * 4. Paginate via Next Page image button with updated ViewState
 *
 * Key field names (confirmed via live inspection):
 *   Search input:  ctl00$PageContent$SearchText
 *   Search button: ctl00$PageContent$SearchButton (image — send .x and .y)
 *   Page size:     ctl00$PageContent$Pagination$_PageSizeSelector
 *   Next page:     ctl00$PageContent$Pagination$_NextPage (image — send .x and .y)
 *   Current page:  ctl00$PageContent$Pagination$_CurrentPage
 *
 * Name format: "LAST FIRST M" (all caps, space-separated, NO comma)
 * Firm cell:   "FIRM NAME\n\nCITY \nST" (city/state embedded in firm cell)
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Common last name prefixes for broad coverage of the directory.
// The DOE Legal search is keyword-based; we use these to systematically
// sweep the directory. With page size 250, most prefixes fit on one page.
const SEARCH_TERMS = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Davis', 'Miller',
  'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White',
  'Harris', 'Martin', 'Thompson', 'Garcia', 'Martinez', 'Robinson',
  'Clark', 'Lewis', 'Lee', 'Walker', 'Hall', 'Allen', 'Young', 'King',
  'Wright', 'Green', 'Baker', 'Adams', 'Nelson', 'Hill', 'Campbell',
  'Mitchell', 'Roberts', 'Carter', 'Phillips', 'Evans', 'Turner',
  'Collins', 'Murphy', 'Kelly', 'Sullivan', 'Ryan', 'Cohen', 'Schwartz',
  'Ross', 'Stewart', 'Morgan', 'Bell', 'Murray', 'Fox', 'Gordon',
];

class DelawareScraper extends BaseScraper {
  constructor() {
    super({
      name: 'delaware',
      stateCode: 'DE',
      baseUrl: 'https://rp470541.doelegal.com/vwPublicSearch/Show-VwPublicSearch-Table.aspx',
      pageSize: 250,
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

    // Track yielded bar numbers to dedup across search terms
    this._seenBarNumbers = new Set();
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
  _httpPost(url, formData, rateLimiter, cookies = '') {
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
        timeout: 30000,
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
   * Handles both attribute orderings: type before name, and name before type.
   */
  _extractHiddenFields(html) {
    const fields = {};
    const regex = /<input[^>]+type\s*=\s*"hidden"[^>]*/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      const tag = match[0];
      const nameM = tag.match(/name\s*=\s*"([^"]*)"/);
      const valueM = tag.match(/value\s*=\s*"([^"]*)"/);
      if (nameM) {
        fields[nameM[1]] = valueM ? valueM[1] : '';
      }
    }
    return fields;
  }

  /**
   * Parse attorneys from the DOE Legal #VwPublicSearchTableControlGrid table.
   *
   * Columns: Attorney | Firm/Employer | Phone | Supreme Court ID | Admit Date | Current Status
   * Name format: "LAST FIRST M" (all caps, space-separated)
   * Firm cell: "FIRM NAME\n\nCITY \nST" (city/state embedded after <br>)
   */
  _parseAttorneys($) {
    const attorneys = [];

    // The data table has id="VwPublicSearchTableControlGrid"
    $('#VwPublicSearchTableControlGrid tr').each((_, row) => {
      const $row = $(row);
      // Skip header rows
      if ($row.hasClass('tch') || $row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 6) return;

      // Column 0: Attorney name (format: "LAST FIRST M")
      const rawName = $(cells[0]).text().trim();
      if (!rawName || rawName.length < 3) return;
      // Skip non-name entries
      if (/^(name|attorney|search|no\s|page|N\/A)/i.test(rawName)) return;
      // Names must contain at least one space (first + last)
      if (!rawName.includes(' ')) return;

      // Column 1: Firm/Employer — contains firm name, city, and state
      const firmHtml = $(cells[1]).html() || '';
      const firmText = $(cells[1]).text().trim();

      // Parse firm cell: "FIRM NAME<br>CITY &nbsp;\nST"
      let firmName = '';
      let city = '';
      let state = 'DE';

      // Split on <br> to separate firm from location
      const firmParts = firmHtml.split(/<br\s*\/?>/i);
      if (firmParts.length >= 1) {
        firmName = cheerio.load(firmParts[0]).text().trim();
      }
      if (firmParts.length >= 2) {
        // The location part: "CITY &nbsp;\nST" or "CITY  \nST"
        const locationText = cheerio.load(firmParts.slice(1).join(' ')).text().trim();
        // Parse "CITY  ST" or "CITY\nST"
        const locationMatch = locationText.match(/^(.+?)\s{2,}([A-Z]{2})$/) ||
                              locationText.match(/^(.+?)\s+([A-Z]{2})$/);
        if (locationMatch) {
          city = locationMatch[1].trim();
          state = locationMatch[2];
        } else if (locationText) {
          city = locationText.trim();
        }
      }

      // Column 2: Phone
      const phone = $(cells[2]).text().trim().replace(/[^\d()-.\s+]/g, '');

      // Column 3: Supreme Court ID (bar number)
      const supremeCourtId = $(cells[3]).text().trim();

      // Column 4: Admit Date
      const admitDate = $(cells[4]).text().trim();

      // Column 5: Current Status
      const barStatus = $(cells[5]).text().trim() || 'Active';

      // Parse name: format is "LAST FIRST M" (all caps, space-separated)
      // Convert to title case and split into first/last
      const nameParts = rawName.split(/\s+/);
      let lastName, firstName;

      if (nameParts.length >= 2) {
        // First token is last name, second is first name
        lastName = nameParts[0];
        firstName = nameParts[1];
        // Handle suffixes like JR., II, III that may be at end
      } else {
        lastName = nameParts[0] || '';
        firstName = '';
      }

      // Title-case the names
      const toTitleCase = (s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
      firstName = firstName ? toTitleCase(firstName) : '';
      lastName = lastName ? toTitleCase(lastName) : '';

      // Build full name as "First Last"
      const fullName = `${firstName} ${lastName}`.trim();

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName.replace(/\s+/g, ' ').trim(),
        city: city,
        state: state,
        phone: phone,
        email: '',
        website: '',
        bar_number: supremeCourtId.replace(/[^\d]/g, ''),
        admission_date: admitDate,
        bar_status: barStatus,
        profile_url: '',
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the DE Bar directory.
   *
   * The DOE Legal search is keyword-based, not city-based.
   * We use getCities() to provide search terms (common last names) OR
   * actual cities if the user specifies one.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);
    this._seenBarNumbers = new Set();

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for DE — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    // Determine search terms: if user specified a city, use it as a search term.
    // Otherwise, use common last names for broad coverage.
    let searchTerms;
    if (options.city) {
      searchTerms = [options.city];
    } else {
      searchTerms = SEARCH_TERMS;
      if (options.maxCities) {
        searchTerms = searchTerms.slice(0, options.maxCities);
      }
    }

    for (let ci = 0; ci < searchTerms.length; ci++) {
      const searchTerm = searchTerms[ci];
      yield { _cityProgress: { current: ci + 1, total: searchTerms.length } };
      log.scrape(`Searching DE Bar: "${searchTerm}" (${ci + 1}/${searchTerms.length})`);

      // Step 1: GET the search page to obtain ViewState + session cookies
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

      // Step 2: POST search form with image button click
      const formData = {
        ...hiddenFields,
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        'ctl00$PageContent$SearchText': searchTerm,
        // Image button requires .x and .y coordinates
        'ctl00$PageContent$SearchButton.x': '10',
        'ctl00$PageContent$SearchButton.y': '10',
      };

      let searchResponse;
      try {
        await rateLimiter.wait();
        searchResponse = await this._httpPost(this.baseUrl, formData, rateLimiter, sessionCookies);
      } catch (err) {
        log.error(`Search POST failed for "${searchTerm}": ${err.message}`);
        continue;
      }

      if (searchResponse.statusCode === 429 || searchResponse.statusCode === 403) {
        log.warn(`Got ${searchResponse.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(searchResponse.statusCode);
        if (!shouldRetry) break;
        continue;
      }

      if (searchResponse.statusCode !== 200) {
        log.error(`Search returned ${searchResponse.statusCode} for "${searchTerm}"`);
        continue;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(searchResponse.body)) {
        log.warn(`CAPTCHA detected for "${searchTerm}" — skipping`);
        yield { _captcha: true, city: searchTerm };
        continue;
      }

      const $ = cheerio.load(searchResponse.body);
      const attorneys = this._parseAttorneys($);

      if (attorneys.length === 0) {
        log.info(`No results for "${searchTerm}"`);
        continue;
      }

      log.success(`Found ${attorneys.length} results for "${searchTerm}"`);

      // Yield results, deduplicating by bar number
      let newCount = 0;
      for (const attorney of attorneys) {
        // Filter by city if user specified one
        if (options.city && attorney.city &&
            attorney.city.toLowerCase() !== options.city.toLowerCase()) {
          continue;
        }

        // Filter by admission year
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }

        // Dedup by bar number
        if (attorney.bar_number && this._seenBarNumbers.has(attorney.bar_number)) {
          continue;
        }
        if (attorney.bar_number) {
          this._seenBarNumbers.add(attorney.bar_number);
        }

        newCount++;
        yield this.transformResult(attorney, practiceArea);
      }

      log.info(`Yielded ${newCount} new attorneys from "${searchTerm}" (${this._seenBarNumbers.size} total unique)`);

      // Step 3: Paginate via Next Page image button
      let currentHtml = searchResponse.body;
      let currentCookies = searchResponse.cookies || sessionCookies;
      let pagesFetched = 1;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for "${searchTerm}"`);
          break;
        }

        // Check if the current page value indicates more pages
        const $current = cheerio.load(currentHtml);
        const currentPage = parseInt($current('input[name="ctl00$PageContent$Pagination$_CurrentPage"]').val() || '0', 10);

        // If we only got < pageSize results, there's no next page
        const currentAttorneys = this._parseAttorneys($current);
        if (currentAttorneys.length < this.pageSize) break;

        // POST with Next Page image button click
        const pageHidden = this._extractHiddenFields(currentHtml);
        const pageFormData = {
          ...pageHidden,
          '__EVENTTARGET': '',
          '__EVENTARGUMENT': '',
          'ctl00$PageContent$SearchText': searchTerm,
          'ctl00$PageContent$Pagination$_NextPage.x': '10',
          'ctl00$PageContent$Pagination$_NextPage.y': '10',
        };

        let pageResponse2;
        try {
          await rateLimiter.wait();
          pageResponse2 = await this._httpPost(this.baseUrl, pageFormData, rateLimiter, currentCookies);
        } catch (err) {
          log.error(`Pagination failed for "${searchTerm}" page ${pagesFetched + 1}: ${err.message}`);
          break;
        }

        if (pageResponse2.statusCode !== 200) break;

        const $page = cheerio.load(pageResponse2.body);
        const pageAttorneys = this._parseAttorneys($page);

        if (pageAttorneys.length === 0) break;

        // Check if we got the same results (page didn't actually advance)
        const newPage = parseInt($page('input[name="ctl00$PageContent$Pagination$_CurrentPage"]').val() || '0', 10);
        if (newPage <= currentPage) break;

        let pageNewCount = 0;
        for (const attorney of pageAttorneys) {
          if (options.city && attorney.city &&
              attorney.city.toLowerCase() !== options.city.toLowerCase()) {
            continue;
          }
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          if (attorney.bar_number && this._seenBarNumbers.has(attorney.bar_number)) {
            continue;
          }
          if (attorney.bar_number) {
            this._seenBarNumbers.add(attorney.bar_number);
          }
          pageNewCount++;
          yield this.transformResult(attorney, practiceArea);
        }

        log.info(`Page ${pagesFetched + 1}: ${pageNewCount} new attorneys`);

        currentHtml = pageResponse2.body;
        currentCookies = pageResponse2.cookies || currentCookies;
        pagesFetched++;
      }

      log.success(`Completed ${pagesFetched} page(s) for "${searchTerm}"`);
    }

    log.success(`DE scraper complete: ${this._seenBarNumbers.size} unique attorneys found`);
  }
}

module.exports = new DelawareScraper();
