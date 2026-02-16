/**
 * Missouri Bar Scraper (LawyerDirectory)
 *
 * Source: https://mobar.org/site/content/For-the-Public/Lawyer_Directory.aspx
 * Method: ASP.NET POST with ViewState (iMIS + Telerik RadGrid)
 *
 * Flow:
 * 1. GET the directory page → obtain __VIEWSTATE, session cookies
 * 2. POST with search form fields (city, last name, etc.)
 * 3. Parse Telerik RadGrid table (.rgRow / .rgAltRow)
 * 4. Paginate via __doPostBack to RadGrid for subsequent pages
 *
 * Data returned: Name, City, Zip, Status (Good Standing / Inactive), Hidden GUID
 * No phone/email/firm — those require opt-in on a separate page.
 *
 * The directory includes ALL lawyers registered in MO (not just opt-in).
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
      baseUrl: 'https://mobar.org/site/content/For-the-Public/Lawyer_Directory.aspx',
      pageSize: 20,
      practiceAreaCodes: {},
      defaultCities: [
        'Kansas City', 'St. Louis', 'Springfield', 'Columbia',
        'Independence', "Lee's Summit", "O'Fallon", 'St. Joseph',
        'St. Charles', 'Jefferson City',
      ],
    });

    this.fieldPrefix = 'ctl01$TemplateBody$WebPartManager1$gwpciLawyerDirectory$ciLawyerDirectory$ResultsGrid$Sheet0';
    this.gridId = 'ctl01$TemplateBody$WebPartManager1$gwpciLawyerDirectory$ciLawyerDirectory$ResultsGrid$Grid1';
  }

  /**
   * NOTE: The MO Bar Official Directory does NOT provide individual profile/detail
   * pages. The Telerik RadGrid only shows: Name, City, Zip, Status.
   * No phone, email, firm, or website data is available from this source.
   *
   * The separate "Find a Lawyer" public search (mobar.org/public/LawyerSearch.aspx)
   * is an opt-in system with different data and does not link back to the Official
   * Directory entries.
   *
   * Enrichment for MO leads should come from cross-reference sources
   * (Martindale, Lawyers.com) rather than profile page fetching.
   *
   * parseProfilePage is intentionally NOT implemented — hasProfileParser will
   * return false, and the waterfall will skip profile fetching for MO leads.
   */

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * HTTP GET with cookie tracking.
   */
  _httpGet(url, rateLimiter, cookies) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
      };
      if (cookies) headers['Cookie'] = cookies;

      https.get(url, { headers, timeout: 25000 }, (res) => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
        const allCookies = [cookies, setCookies].filter(Boolean).join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `https://mobar.org${loc}`;
          return resolve(this._httpGet(loc, rateLimiter, allCookies));
        }

        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: allCookies }));
      }).on('error', reject);
    });
  }

  /**
   * HTTP POST with form data and cookie support.
   */
  _httpPost(url, formData, rateLimiter, cookies) {
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
          'Origin': 'https://mobar.org',
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 25000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
        const allCookies = [cookies, setCookies].filter(Boolean).join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `https://mobar.org${loc}`;
          return resolve(this._httpGet(loc, rateLimiter, allCookies));
        }

        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: allCookies,
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
    const $ = cheerio.load(html);
    $('input[type="hidden"]').each((_, el) => {
      const name = $(el).attr('name') || '';
      const value = $(el).attr('value') || '';
      if (name) fields[name] = value;
    });
    return fields;
  }

  /**
   * Extract total result count from the RadGrid pager ("Item 1 to 20 of 250").
   */
  _extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/Item\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);

    const altMatch = text.match(/([\d,]+)\s*result/i);
    if (altMatch) return parseInt(altMatch[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Parse attorney rows from the Telerik RadGrid.
   */
  _parseGridRows($) {
    const attorneys = [];

    $('.rgRow, .rgAltRow').each((_, row) => {
      const $row = $(row);
      const cells = $row.find('td');
      if (cells.length < 4) return;

      const fullName = $(cells[0]).text().trim();
      const city = $(cells[1]).text().trim();
      const zip = $(cells[2]).text().trim();
      const status = $(cells[3]).text().trim();
      const guid = cells.length > 4 ? $(cells[4]).text().trim() : '';

      if (!fullName) return;

      // Filter to active attorneys only
      if (status && status.toLowerCase() === 'inactive') return;

      // Parse name — format is "Last, First Middle" or "Last, First"
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        // First part of the "First Middle" portion
        const firstPart = (parts[1] || '').split(/\s+/);
        firstName = firstPart[0] || '';
      } else {
        const split = this.splitName(fullName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: `${firstName} ${lastName}`.trim(),
        firm_name: '',
        city: city,
        state: 'MO',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: status || 'Good Standing',
        zip: zip,
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the MO Bar Directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    if (practiceArea) {
      log.warn(`MO Bar Directory does not support practice area filtering — ignoring "${practiceArea}"`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Step 1: GET the directory page to obtain ViewState and session cookies
      let pageResponse;
      try {
        await rateLimiter.wait();
        pageResponse = await this._httpGet(this.baseUrl, rateLimiter, null);
      } catch (err) {
        log.error(`Failed to load directory page: ${err.message}`);
        continue;
      }

      if (pageResponse.statusCode !== 200) {
        log.error(`Directory page returned ${pageResponse.statusCode}`);
        continue;
      }

      const sessionCookies = pageResponse.cookies;
      const hiddenFields = this._extractHiddenFields(pageResponse.body);

      if (!hiddenFields.__VIEWSTATE) {
        log.error(`Could not extract __VIEWSTATE from MO Bar directory page`);
        continue;
      }

      // Step 2: POST search form with city
      const formData = {
        ...hiddenFields,
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        [`${this.fieldPrefix}$Input0$TextBox1`]: '',          // Last Name
        [`${this.fieldPrefix}$Input1$TextBox1`]: '',          // First Name
        [`${this.fieldPrefix}$Input2$TextBox1`]: city,        // City
        [`${this.fieldPrefix}$Input3$TextBox1`]: '',          // Zip Code
        [`${this.fieldPrefix}$Input4$TextBox1`]: '',          // Bar Number
        [`${this.fieldPrefix}$Input5$TextBox1`]: '',          // County Name
        [`${this.fieldPrefix}$SubmitButton`]: 'Find',
      };

      let searchResponse;
      try {
        await rateLimiter.wait();
        searchResponse = await this._httpPost(this.baseUrl, formData, rateLimiter, sessionCookies);
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
      const totalResults = this._extractResultCount($);

      if (totalResults > 0) {
        log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
      }

      // Parse first page
      const attorneys = this._parseGridRows($);
      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      if (totalResults === 0) {
        log.success(`Found ${attorneys.length} results for ${city}`);
      }

      for (const attorney of attorneys) {
        attorney.practice_area = practiceArea || '';
        yield this.transformResult(attorney, practiceArea);
      }

      // Step 3: Paginate through remaining pages
      let currentCookies = searchResponse.cookies;
      let currentHtml = searchResponse.body;
      let pagesFetched = 1;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Find next page link in RadGrid pager
        const $current = cheerio.load(currentHtml);

        // RadGrid pages use __doPostBack with the grid ID and 'Page$Next' or 'Page$<N>'
        let nextPageArg = null;
        let nextEventTarget = null;

        // Look for numbered page links
        $current('a[href*="__doPostBack"]').each((_, el) => {
          const href = $current(el).attr('href') || '';
          const text = $current(el).text().trim();

          // Look for "Page$Next" or the next page number
          const match = href.match(/__doPostBack\('([^']+)','([^']*)'\)/);
          if (!match) return;

          const target = match[1];
          const arg = match[2];

          // Only consider RadGrid pagination events
          if (!target.includes('Grid1') && !target.includes('ResultsGrid')) return;

          // "Page$Next" link
          if (arg.includes('Page$Next') || arg === `Page$${pagesFetched + 1}`) {
            nextEventTarget = target;
            nextPageArg = arg;
          }

          // Numeric page link
          if (text === String(pagesFetched + 1)) {
            nextEventTarget = target;
            nextPageArg = arg;
          }
        });

        if (!nextEventTarget || !nextPageArg) {
          log.info(`No more pages for ${city}`);
          break;
        }

        const pageHidden = this._extractHiddenFields(currentHtml);
        const pageFormData = {
          ...pageHidden,
          '__EVENTTARGET': nextEventTarget,
          '__EVENTARGUMENT': nextPageArg,
        };

        let pageResponse;
        try {
          await rateLimiter.wait();
          pageResponse = await this._httpPost(this.baseUrl, pageFormData, rateLimiter, currentCookies);
        } catch (err) {
          log.error(`Pagination failed for ${city} page ${pagesFetched + 1}: ${err.message}`);
          break;
        }

        if (pageResponse.statusCode !== 200) {
          log.error(`Page ${pagesFetched + 1} returned ${pageResponse.statusCode}`);
          break;
        }

        const $page = cheerio.load(pageResponse.body);
        const pageAttorneys = this._parseGridRows($page);

        if (pageAttorneys.length === 0) break;

        for (const attorney of pageAttorneys) {
          attorney.practice_area = practiceArea || '';
          yield this.transformResult(attorney, practiceArea);
        }

        currentHtml = pageResponse.body;
        currentCookies = pageResponse.cookies || currentCookies;
        pagesFetched++;
      }

      log.success(`Completed ${pagesFetched} page(s) for ${city} (${attorneys.length + (pagesFetched > 1 ? '+ more' : '')} attorneys)`);
    }
  }
}

module.exports = new MissouriScraper();
