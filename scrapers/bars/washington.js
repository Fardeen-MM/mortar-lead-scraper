/**
 * Washington State Bar Association (WSBA) Legal Directory Scraper
 *
 * Source: https://www.mywsba.org/PersonifyEbusiness/LegalDirectory.aspx
 * Method: GET-based search with URL query parameters
 *
 * The WSBA Legal Directory runs on DotNetNuke (DNN) with Personify eBusiness
 * and Telerik RadAjaxManager. Despite the SSO-gated landing page, the search
 * results are accessible via GET requests with query parameters. The flow is:
 *
 *   1. GET the LegalDirectory.aspx page (follows 302 -> SSO -> 302 -> page)
 *      This establishes cookies: .ASPXANONYMOUS, ASP.NET_SessionId, etc.
 *   2. GET LegalDirectory.aspx?ShowSearchResults=TRUE&LastName=X&City=Y
 *      Returns an HTML page with a <table class="search-results"> grid.
 *   3. Paginate via &Page=N (0-indexed). 21 results per page.
 *
 * Available URL query parameters:
 *   ShowSearchResults=TRUE   (required to trigger results display)
 *   LastName=...             (prefix match, e.g. "A" matches Adams, Allen...)
 *   FirstName=...
 *   City=...
 *   County=...               (e.g. "King", "Pierce")
 *   EligibleToPractice=Y     (filters to currently eligible attorneys)
 *   Page=N                   (0-indexed pagination)
 *
 * Note: PracticeArea URL parameter does NOT actually filter results (returns
 * same count regardless). Practice area filtering only works via POST form.
 *
 * Result columns: License Number, First Name, Last Name, City, Status, Phone
 * The license number cell wraps its value in <ItemTemplate> tags.
 *
 * Page size: 21 results per page.
 * The last row in the table is a pager row with "Next Page >" / "Last >>" links.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WashingtonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'washington',
      stateCode: 'WA',
      baseUrl: 'https://www.mywsba.org/PersonifyEbusiness/LegalDirectory.aspx',
      pageSize: 21,
      practiceAreaCodes: {
        'administrative':       'Administrative-Regulator',
        'agricultural':         'Agricultural',
        'animal law':           'Animal Law',
        'antitrust':            'Antitrust',
        'appellate':            'Appellate',
        'aviation':             'AVIATION',
        'banking':              'Banking',
        'bankruptcy':           'Bankruptcy',
        'business':             'Business-Commercial',
        'cannabis':             'CANNABIS',
        'civil rights':         'Civil Rights',
        'collections':          'Collections',
        'communications':       'Communications',
        'constitutional':       'Constitutional',
        'construction':         'Construction',
        'consumer':             'Consumer',
        'contracts':            'Contracts',
        'corporate':            'Corporate',
        'criminal':             'Criminal',
        'criminal defense':     'Criminal',
        'debtor':               'Debtor-Creditor',
        'disability':           'Disability',
        'dispute resolution':   'Dispute Resolution',
        'education':            'Education',
        'elder':                'Elder',
        'employment':           'Employment',
        'entertainment':        'Entertainment',
        'environmental':        'Environmental',
        'estate planning':      'Estate Planning-Probate',
        'estate':               'Estate Planning-Probate',
        'family':               'Family',
        'family law':           'Family',
        'foreclosure':          'Foreclosure',
        'general':              'General',
        'government':           'Government',
        'guardianships':        'Guardianships',
        'health':               'Health',
        'housing':              'Housing',
        'human rights':         'Human Rights',
        'immigration':          'Immigration-Naturaliza',
        'indian':               'Indian',
        'insurance':            'Insurance',
        'intellectual property':'Intellectual Property',
        'international':        'International',
        'juvenile':             'Juvenile',
        'labor':                'Labor',
        'land use':             'Land Use',
        'landlord':             'Landlord-Tenant',
        'legal ethics':         'Legal Ethics',
        'litigation':           'Litigation',
        'malpractice':          'Malpractice',
        'maritime':             'Maritime',
        'military':             'Military',
        'municipal':            'Municipal',
        'non-profit':           'Non-Profit-Tax Exempt',
        'patent':               'Patent-Trademark-Copyr',
        'personal injury':      'Personal Injury',
        'privacy':              'PRIVACY AND DATA SECURIT',
        'real property':        'Real Property',
        'real estate':          'Real Property-Land Use',
        'securities':           'Securities',
        'sports':               'Sports',
        'tax':                  'Tax',
        'torts':                'Torts',
        'traffic':              'Traffic Offenses',
        'workers comp':         'Workers Compensation',
      },
      defaultCities: [
        'Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue',
        'Kent', 'Everett', 'Renton', 'Olympia', 'Kirkland',
        'Redmond', 'Yakima', 'Federal Way', 'Bellingham', 'Kennewick',
      ],
    });
  }

  /**
   * HTTP GET with cookie jar support. The WSBA site requires cookies from the
   * initial SSO redirect flow. This method tracks Set-Cookie headers across
   * redirects and returns the accumulated cookies along with the response.
   */
  _httpGetWithCookies(url, rateLimiter, cookies = '', redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 10) {
        return reject(new Error(`Too many redirects (>10) for ${url}`));
      }

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
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 25000,
      };

      const req = https.request(options, (res) => {
        // Collect new cookies
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);

        // Merge with existing cookies (newer values override older ones)
        const cookieMap = {};
        if (cookies) {
          for (const pair of cookies.split('; ')) {
            const [k] = pair.split('=');
            if (k) cookieMap[k] = pair;
          }
        }
        for (const pair of newCookies) {
          const [k] = pair.split('=');
          if (k) cookieMap[k] = pair;
        }
        const mergedCookies = Object.values(cookieMap).join('; ');

        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          return resolve(this._httpGetWithCookies(redirect, rateLimiter, mergedCookies, redirectCount + 1));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: mergedCookies,
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * Establish session by fetching the Legal Directory landing page.
   * This triggers the SSO redirect flow that sets the required cookies.
   * Returns the session cookies needed for subsequent search requests.
   */
  async _establishSession(rateLimiter) {
    const response = await this._httpGetWithCookies(this.baseUrl, rateLimiter);

    if (response.statusCode !== 200) {
      throw new Error(`Session establishment failed: status ${response.statusCode}`);
    }

    if (!response.cookies) {
      throw new Error('No cookies received from session establishment');
    }

    // Verify we got the directory page (check for search form)
    if (!response.body.includes('pnlSearchScreen') && !response.body.includes('Legal Directory')) {
      throw new Error('Session page does not contain search form — possible SSO failure');
    }

    return response.cookies;
  }

  /**
   * Build the search URL with query parameters.
   *
   * @param {object} params
   * @param {string} [params.city] - City name to search
   * @param {string} [params.county] - County name to search
   * @param {string} [params.lastName] - Last name prefix
   * @param {number} [params.page] - Page number (0-indexed)
   * @param {boolean} [params.eligibleOnly] - Filter to eligible attorneys
   * @returns {string} The complete search URL
   */
  _buildSearchUrl({ city, county, lastName, page = 0, eligibleOnly = true } = {}) {
    const params = new URLSearchParams();
    params.set('ShowSearchResults', 'TRUE');

    if (lastName) params.set('LastName', lastName);
    if (city) params.set('City', city);
    if (county) params.set('County', county);
    if (eligibleOnly) params.set('EligibleToPractice', 'Y');
    if (page > 0) params.set('Page', String(page));

    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Not used directly -- search() is overridden for GET-based query.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Parse the search results HTML page.
   * The results table has class "search-results" with columns:
   *   License Number, First Name, Last Name, City, Status, Phone
   *
   * The license number cell wraps its text in <ItemTemplate> tags.
   * The last row may be a pager row with colspan (Next/Last links).
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object[]} Array of attorney objects
   */
  parseResultsPage($) {
    const attorneys = [];

    const table = $('table.search-results');
    if (!table.length) return attorneys;

    const rows = table.find('tr');

    rows.each((i, el) => {
      const $row = $(el);

      // Skip header row
      if ($row.find('th').length > 0) return;

      const cells = $row.find('> td');

      // Skip pager rows (they have colspan or contain "Page" / "Next" links)
      if (cells.length < 6) return;
      const firstCellText = cells.first().text().trim();
      if (firstCellText.includes('Page') || firstCellText.includes('Next') ||
          firstCellText.includes('Last') || firstCellText.includes('First') ||
          firstCellText.includes('Prev') || firstCellText === '') {
        // Check if this is a genuine pager row (first cell has link text)
        const hasPagerLinks = $row.find('a[href*="Page"]').length > 0;
        if (hasPagerLinks || cells.length !== 6) return;
        // If 6 cells but first is empty, this might be a pager row with colspan table
        if (firstCellText === '' && $row.find('table').length > 0) return;
      }

      // Extract cell text
      const licenseNumber = $(cells[0]).text().trim()
        .replace(/ItemTemplate/gi, '').replace(/<[^>]*>/g, '').trim();
      const firstName = $(cells[1]).text().trim();
      const lastName = $(cells[2]).text().trim();
      const city = $(cells[3]).text().trim();
      const status = $(cells[4]).text().trim();
      const phone = $(cells[5]).text().trim();

      // Validate: license number should be numeric
      if (!licenseNumber || !/^\d+$/.test(licenseNumber)) return;
      // Skip if no name
      if (!lastName && !firstName) return;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: 'WA',
        phone: phone,
        email: '',
        website: '',
        bar_number: licenseNumber,
        bar_status: status,
        admission_date: '',
        profile_url: '',
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the results page.
   * The page shows "N results" text in the results panel.
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {number} Total result count
   */
  extractResultCount($) {
    const text = $('body').text();

    // Pattern: "119 results" or "16,802 results"
    const match = text.match(/([\d,]+)\s+results?/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Async generator that yields attorney records from the WSBA Legal Directory.
   *
   * Search strategy:
   * - For each city, searches with EligibleToPractice=Y
   * - If a city returns > 10,000 results, subdivides by last name prefix (A-Z)
   *   to avoid missing data from the result cap
   * - Paginates through all result pages (21 per page)
   *
   * @param {string} practiceArea - Practice area filter (note: URL-based filtering
   *   does NOT work for practice areas on WSBA; this is stored but not used for filtering)
   * @param {object} options - Search options
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    if (practiceArea) {
      log.warn(`WA: WSBA Legal Directory URL search does not filter by practice area — searching all attorneys`);
      log.info(`WA: Practice area "${practiceArea}" will be recorded but not used for filtering`);
    }

    const cities = this.getCities(options);

    // Step 1: Establish session (triggers SSO cookie flow)
    let sessionCookies;
    try {
      log.info('WA: Establishing session with WSBA Legal Directory...');
      await rateLimiter.wait();
      sessionCookies = await this._establishSession(rateLimiter);
      log.success('WA: Session established successfully');
    } catch (err) {
      log.error(`WA: Failed to establish session: ${err.message}`);
      yield { _captcha: true, city: 'N/A', page: 0 };
      return;
    }

    let totalPagesFetched = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Check max pages limit across all cities
      if (options.maxPages && totalPagesFetched >= options.maxPages) {
        log.info(`WA: Reached global max pages limit (${options.maxPages})`);
        break;
      }

      // Step 2: Fetch first page to get total count
      const firstUrl = this._buildSearchUrl({ city, page: 0, eligibleOnly: true });
      log.info(`WA: ${firstUrl}`);

      let firstResponse;
      try {
        await rateLimiter.wait();
        firstResponse = await this._httpGetWithCookies(firstUrl, rateLimiter, sessionCookies);
      } catch (err) {
        log.error(`WA: Request failed for ${city}: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (!shouldRetry) break;
        continue;
      }

      if (firstResponse.statusCode === 429 || firstResponse.statusCode === 403) {
        log.warn(`WA: Got ${firstResponse.statusCode}`);
        const shouldRetry = await rateLimiter.handleBlock(firstResponse.statusCode);
        if (!shouldRetry) break;
        continue;
      }

      if (firstResponse.statusCode !== 200) {
        log.error(`WA: Unexpected status ${firstResponse.statusCode} for ${city}`);
        continue;
      }

      // Update session cookies from response
      if (firstResponse.cookies) {
        sessionCookies = firstResponse.cookies;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(firstResponse.body)) {
        log.warn(`WA: CAPTCHA detected for ${city}`);
        yield { _captcha: true, city, page: 0 };
        continue;
      }

      const $first = cheerio.load(firstResponse.body);
      const totalResults = this.extractResultCount($first);

      if (totalResults === 0) {
        log.info(`WA: No results for ${city}`);
        continue;
      }

      const totalPages = Math.ceil(totalResults / this.pageSize);
      log.success(`WA: Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

      // If results exceed 10,000, subdivide by last name prefix to avoid cap
      if (totalResults >= 10000) {
        log.info(`WA: ${city} has ${totalResults} results — subdividing by last name prefix A-Z`);
        yield* this._searchByLastNamePrefix(city, practiceArea, options, rateLimiter, sessionCookies, totalPagesFetched);
        continue;
      }

      // Parse first page
      const firstAttorneys = this.parseResultsPage($first);
      for (const attorney of firstAttorneys) {
        yield this.transformResult(attorney, practiceArea);
      }
      totalPagesFetched++;

      // Paginate through remaining pages
      for (let page = 1; page < totalPages; page++) {
        if (options.maxPages && totalPagesFetched >= options.maxPages) {
          log.info(`WA: Reached max pages limit (${options.maxPages})`);
          break;
        }

        const pageUrl = this._buildSearchUrl({ city, page, eligibleOnly: true });
        log.info(`WA: Page ${page + 1}/${totalPages} — ${city}`);

        let pageResponse;
        try {
          await rateLimiter.wait();
          pageResponse = await this._httpGetWithCookies(pageUrl, rateLimiter, sessionCookies);
        } catch (err) {
          log.error(`WA: Pagination failed for ${city} page ${page}: ${err.message}`);
          break;
        }

        if (pageResponse.statusCode !== 200) {
          log.error(`WA: Page ${page} returned ${pageResponse.statusCode}`);
          break;
        }

        if (pageResponse.cookies) {
          sessionCookies = pageResponse.cookies;
        }

        if (this.detectCaptcha(pageResponse.body)) {
          log.warn(`WA: CAPTCHA on page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $page = cheerio.load(pageResponse.body);
        const pageAttorneys = this.parseResultsPage($page);

        if (pageAttorneys.length === 0) {
          log.info(`WA: No more results on page ${page} for ${city}`);
          break;
        }

        for (const attorney of pageAttorneys) {
          yield this.transformResult(attorney, practiceArea);
        }
        totalPagesFetched++;
      }

      log.success(`WA: Completed ${city} (${Math.min(totalPages, options.maxPages || Infinity)} pages)`);
    }
  }

  /**
   * Subdivide a city search by last name prefix (A-Z) when the result count
   * exceeds 10,000 (the apparent soft cap). Each prefix search typically
   * yields a manageable number of results that can be fully paginated.
   *
   * @param {string} city - City name
   * @param {string} practiceArea - Practice area (stored, not filtered)
   * @param {object} options - Search options
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @param {string} cookies - Session cookies
   * @param {number} pagesFetchedSoFar - Pages already fetched (for maxPages check)
   */
  async *_searchByLastNamePrefix(city, practiceArea, options, rateLimiter, cookies, pagesFetchedSoFar) {
    const prefixes = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
    let totalPagesFetched = pagesFetchedSoFar;

    for (const prefix of prefixes) {
      if (options.maxPages && totalPagesFetched >= options.maxPages) {
        log.info(`WA: Reached max pages limit in prefix search`);
        break;
      }

      // Fetch first page for this prefix
      const firstUrl = this._buildSearchUrl({ city, lastName: prefix, page: 0, eligibleOnly: true });
      log.info(`WA: Prefix "${prefix}" in ${city} — ${firstUrl}`);

      let firstResponse;
      try {
        await rateLimiter.wait();
        firstResponse = await this._httpGetWithCookies(firstUrl, rateLimiter, cookies);
      } catch (err) {
        log.error(`WA: Request failed for ${city} prefix ${prefix}: ${err.message}`);
        continue;
      }

      if (firstResponse.statusCode !== 200) {
        log.error(`WA: Status ${firstResponse.statusCode} for ${city} prefix ${prefix}`);
        continue;
      }

      if (firstResponse.cookies) cookies = firstResponse.cookies;

      if (this.detectCaptcha(firstResponse.body)) {
        log.warn(`WA: CAPTCHA on prefix ${prefix} for ${city}`);
        yield { _captcha: true, city, prefix };
        continue;
      }

      const $first = cheerio.load(firstResponse.body);
      const totalResults = this.extractResultCount($first);

      if (totalResults === 0) {
        continue;
      }

      const totalPages = Math.ceil(totalResults / this.pageSize);
      log.info(`WA: Prefix "${prefix}" in ${city}: ${totalResults} results (${totalPages} pages)`);

      // Parse first page
      const firstAttorneys = this.parseResultsPage($first);
      for (const attorney of firstAttorneys) {
        yield this.transformResult(attorney, practiceArea);
      }
      totalPagesFetched++;

      // Paginate
      for (let page = 1; page < totalPages; page++) {
        if (options.maxPages && totalPagesFetched >= options.maxPages) break;

        const pageUrl = this._buildSearchUrl({ city, lastName: prefix, page, eligibleOnly: true });

        let pageResponse;
        try {
          await rateLimiter.wait();
          pageResponse = await this._httpGetWithCookies(pageUrl, rateLimiter, cookies);
        } catch (err) {
          log.error(`WA: Pagination failed for ${city} prefix ${prefix} page ${page}: ${err.message}`);
          break;
        }

        if (pageResponse.statusCode !== 200) break;
        if (pageResponse.cookies) cookies = pageResponse.cookies;

        if (this.detectCaptcha(pageResponse.body)) {
          log.warn(`WA: CAPTCHA on prefix ${prefix} page ${page}`);
          yield { _captcha: true, city, prefix, page };
          break;
        }

        const $page = cheerio.load(pageResponse.body);
        const pageAttorneys = this.parseResultsPage($page);
        if (pageAttorneys.length === 0) break;

        for (const attorney of pageAttorneys) {
          yield this.transformResult(attorney, practiceArea);
        }
        totalPagesFetched++;
      }
    }
  }
}

module.exports = new WashingtonScraper();
