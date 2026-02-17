/**
 * Maine Board of Overseers of the Bar Scraper
 *
 * Source: https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl
 * Method: CGI form POST with session cookie + 302 redirect to results page
 *
 * Flow:
 * 1. GET attorney_directory.pl — obtain session cookie (overseers=...)
 * 2. POST attorney_directory.pl — submit search form with city/name fields
 *    Server responds with 302 redirect to attorney_directory_results.pl
 * 3. GET attorney_directory_results.pl — fetch paginated results using session cookie
 *    Pagination via ?page=N query parameter on the results URL
 *
 * Results page: HTML table with columns: Name, City/State, County, Zip, Bar Number
 * Name format: "FirstName, LastName MiddleParts, Suffix" (e.g. "John, Smith A., Esq.")
 * Each name links to attorney_directory_details.pl?bar_num=XXXXXX
 *
 * Detail page fields: Name, Firm, Address, Phone, Fax, Website, Registration Status,
 *   First Admitted, Admitted to Maine Bar, Bar Number, Law School, Other Jurisdictions
 *
 * Results: 40 per page. City-only search for Portland = ~2040 results (51 pages).
 * The site does NOT filter by practice area — practiceAreaCodes are kept for
 * user-facing mapping only (passed through but not sent to the server).
 *
 * Bot detection: The server requires browser-like headers (Sec-Fetch-*, Referer,
 * Origin) and a valid session cookie. Without these, the results page returns 403.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MaineScraper extends BaseScraper {
  constructor() {
    super({
      name: 'maine',
      stateCode: 'ME',
      baseUrl: 'https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl',
      pageSize: 40,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BAN',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'corporate':              'COR',
        'criminal':               'CRI',
        'criminal defense':       'CRI',
        'elder':                  'ELD',
        'employment':             'EMP',
        'environmental':          'ENV',
        'estate planning':        'EST',
        'family':                 'FAM',
        'family law':             'FAM',
        'general practice':       'GEN',
        'immigration':            'IMM',
        'insurance':              'INS',
        'intellectual property':  'IPR',
        'labor':                  'LAB',
        'maritime':               'MAR',
        'medical malpractice':    'MED',
        'personal injury':        'PIN',
        'probate':                'PRO',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Portland', 'Lewiston', 'Bangor', 'South Portland',
        'Auburn', 'Augusta', 'Biddeford', 'Scarborough',
      ],
    });

    this.resultsUrl = 'https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory_results.pl';
    this.detailBaseUrl = 'https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory_details.pl';
  }

  /**
   * Browser-like headers required by the Maine CGI server to avoid 403.
   */
  _browserHeaders(ua, extra = {}) {
    return {
      'User-Agent': ua,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Connection': 'keep-alive',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      ...extra,
    };
  }

  /**
   * HTTP GET with cookie support and browser-like headers.
   */
  _httpGet(url, ua, cookies = '', extraHeaders = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: this._browserHeaders(ua, {
          'Sec-Fetch-Site': 'same-origin',
          ...(cookies ? { 'Cookie': cookies } : {}),
          ...extraHeaders,
        }),
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          setCookies,
          body: data,
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with form data, cookie support, and browser-like headers.
   * Does NOT follow redirects automatically — returns the raw 302.
   */
  _httpPost(url, formBody, ua, cookies = '') {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof formBody === 'string'
        ? formBody
        : new URLSearchParams(formBody).toString();

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: this._browserHeaders(ua, {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'Origin': `${parsed.protocol}//${parsed.host}`,
          'Referer': url,
          'Sec-Fetch-Site': 'same-origin',
          ...(cookies ? { 'Cookie': cookies } : {}),
        }),
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          setCookies,
          body: data,
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Establish a session by GETing the search page and extracting the session cookie.
   */
  async _getSession(ua) {
    const res = await this._httpGet(this.baseUrl, ua, '', {
      'Sec-Fetch-Site': 'none',
    });

    if (res.statusCode !== 200) {
      throw new Error(`Failed to load search page: status ${res.statusCode}`);
    }

    const cookie = res.setCookies.find(c => c.startsWith('overseers='));
    if (!cookie) {
      throw new Error('No session cookie received from search page');
    }

    return cookie;
  }

  /**
   * Submit a search and return the results page body for a given page number.
   *
   * On page 1: POST the form to initiate the search, follow 302, GET results.
   * On page N>1: GET results URL with ?page=N using the same session cookie.
   */
  async _fetchResultsPage(city, page, sessionCookie, ua) {
    if (page === 1) {
      // POST the search form
      const formData = {
        fname: '',
        mname: '',
        lname: '',
        city: city,
        state: '',
        zip: '',
        county: '',
        bar_number: '',
        submit: 'Search',
      };

      const postRes = await this._httpPost(this.baseUrl, formData, ua, sessionCookie);

      if (postRes.statusCode !== 302) {
        // Some responses may return results directly (unlikely but handle it)
        if (postRes.statusCode === 200) {
          return { statusCode: 200, body: postRes.body };
        }
        return { statusCode: postRes.statusCode, body: postRes.body };
      }

      // Merge any new cookies from the POST response
      const newCookie = postRes.setCookies.find(c => c.startsWith('overseers='));
      const cookie = newCookie || sessionCookie;

      // GET the results page
      const resultRes = await this._httpGet(this.resultsUrl, ua, cookie, {
        'Referer': this.baseUrl,
      });

      return { statusCode: resultRes.statusCode, body: resultRes.body, cookie };
    }

    // Page > 1: just GET with ?page=N
    const pageUrl = `${this.resultsUrl}?page=${page}`;
    const resultRes = await this._httpGet(pageUrl, ua, sessionCookie, {
      'Referer': this.resultsUrl,
    });

    return { statusCode: resultRes.statusCode, body: resultRes.body };
  }

  /**
   * Parse the name field from results table.
   * Format: "FirstName, LastName MiddleParts, Suffix"
   * Examples:
   *   "Richard, Abbondanza J., Esq."  -> first=Richard, last=Abbondanza
   *   "Kaighn, Smith, Jr. , Esq."     -> first=Kaighn, last=Smith
   *   "Victoria, Adams Dawn Degenhardt, Esq." -> first=Victoria, last=Adams
   */
  _parseMeName(raw) {
    if (!raw) return { firstName: '', lastName: '' };

    // Remove trailing "Esq." and trailing commas/whitespace
    let cleaned = raw
      .replace(/,?\s*Esq\.?\s*$/i, '')
      .replace(/,\s*$/, '')
      .trim();

    // Split on first comma: first part is first_name, rest is "LastName MiddleParts"
    const commaIdx = cleaned.indexOf(',');
    if (commaIdx === -1) {
      return { firstName: '', lastName: cleaned };
    }

    const firstName = cleaned.substring(0, commaIdx).trim();
    let rest = cleaned.substring(commaIdx + 1).trim();

    // Remove suffixes like "Jr.", "Sr.", "III", "II", "IV" after the last comma
    rest = rest.replace(/,\s*(Jr\.?|Sr\.?|III|II|IV)\s*$/i, '').trim();

    // The first word is the last name
    const parts = rest.split(/\s+/).filter(Boolean);
    const lastName = parts[0] || '';

    return { firstName, lastName };
  }

  /**
   * Not used — search() is overridden for session-based POST flow.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Parse the results table from a Cheerio-loaded page.
   * Table: table.tablesorter with columns: Name, City/State, County, Zip, Bar Number
   */
  parseResultsPage($) {
    const attorneys = [];

    $('table.tablesorter tbody tr').each((i, el) => {
      const $row = $(el);
      const tds = $row.find('td');
      if (tds.length < 5) return;

      // Column 0: Name (linked to detail page)
      const nameLink = $(tds[0]).find('a');
      const rawName = nameLink.text().trim();
      const detailHref = nameLink.attr('href') || '';

      // Column 1: City/State (e.g., "Portland, ME")
      const cityState = $(tds[1]).text().trim();

      // Column 2: County
      const county = $(tds[2]).text().trim();

      // Column 3: Zip
      const zip = $(tds[3]).text().trim();

      // Column 4: Bar Number
      const barNumber = $(tds[4]).text().trim();

      if (!rawName || !barNumber) return;

      // Parse name
      const { firstName, lastName } = this._parseMeName(rawName);

      // Parse city/state
      let city = '';
      let state = '';
      const csMatch = cityState.match(/^(.+),\s*([A-Z]{2})$/);
      if (csMatch) {
        city = csMatch[1].trim();
        state = csMatch[2];
      }

      // Build profile URL
      let profileUrl = '';
      if (detailHref && barNumber) {
        profileUrl = `${this.detailBaseUrl}?bar_num=${barNumber}`;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        city: city,
        state: state || 'ME',
        zip: zip,
        county: county,
        bar_number: barNumber,
        phone: '',
        email: '',
        website: '',
        firm_name: '',
        bar_status: '',
        admission_date: '',
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from pagination text.
   * Pagination format: "Page [ 1 2 3 ... 51 ]  |  Next >>"
   * We extract the highest page number and multiply by pageSize (40).
   */
  extractResultCount($) {
    const pagText = $('div.paginate').text().trim();
    if (!pagText) return 0;

    // Find the highest page number in pagination links/text
    const pageNumbers = pagText.match(/\d+/g);
    if (!pageNumbers || pageNumbers.length === 0) return 0;

    const maxPage = Math.max(...pageNumbers.map(Number));
    return maxPage * this.pageSize;
  }

  /**
   * Extract the total number of pages from the pagination div.
   */
  _extractTotalPages($) {
    const pagText = $('div.paginate').text().trim();
    if (!pagText) return 1;

    const pageNumbers = pagText.match(/\d+/g);
    if (!pageNumbers || pageNumbers.length === 0) return 1;

    return Math.max(...pageNumbers.map(Number));
  }

  /**
   * Parse the detail/profile page for additional fields.
   * Detail page has: Name, Firm, Address, Phone, Fax, Website,
   * Registration Status, First Admitted, Admitted to Maine Bar,
   * Bar Number, Law School, Other Jurisdictions
   */
  parseProfilePage($) {
    const result = {};

    // Firm name
    const firmLi = $('li:contains("Firm/Office Name:")');
    if (firmLi.length) {
      result.firm_name = firmLi.find('strong').text().trim();
    }

    // Phone
    const phoneLi = $('li:contains("Phone:")');
    if (phoneLi.length) {
      result.phone = phoneLi.find('strong').text().trim();
    }

    // Website
    const websiteLi = $('li:contains("Website:")');
    if (websiteLi.length) {
      let href = websiteLi.find('a').attr('href') || '';
      // Filter out empty "http://" links
      if (href && href !== 'http://' && href !== 'https://' && href.length > 10) {
        // Fix double-protocol prefix (e.g. "http://http://www.example.com")
        href = href.replace(/^(https?:\/\/)(https?:\/\/)/, '$2');
        result.website = href;
      }
    }

    // Registration Status
    const statusLi = $('li:contains("Registration Status:")');
    if (statusLi.length) {
      result.bar_status = statusLi.find('strong').text().trim();
    }

    // Admitted to Maine Bar
    const admitLi = $('li:contains("Admitted to Maine Bar:")');
    if (admitLi.length) {
      result.admission_date = admitLi.find('strong').text().trim();
    }

    // Address
    const addressLi = $('li:contains("Address:")');
    if (addressLi.length) {
      result.address = addressLi.find('strong').text().trim();
    }

    // Law School
    const lawSchoolLi = $('li:contains("Law School:")');
    if (lawSchoolLi.length) {
      let school = lawSchoolLi.find('strong').text().trim();
      // Clean up the "zzz " prefix some entries have
      school = school.replace(/^zzz\s+/i, '');
      if (school) result.law_school = school;
    }

    return result;
  }

  /**
   * Async generator: yields attorney records from Maine Bar search.
   *
   * Strategy: Search by city with no last name filter to get all attorneys
   * in each city. Paginate using ?page=N on the results URL.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const ua = rateLimiter.getUserAgent();

    if (practiceArea) {
      log.warn(`ME bar does not support practice area filtering — searching all attorneys`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Establish a new session for each city
      let sessionCookie;
      try {
        await rateLimiter.wait();
        sessionCookie = await this._getSession(ua);
      } catch (err) {
        log.error(`Failed to establish session for ${city}: ${err.message}`);
        continue;
      }

      let page = 1;
      let totalPages = null;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${page} for ${city} — ${page === 1 ? 'POST + redirect' : `GET ${this.resultsUrl}?page=${page}`}`);

        let result;
        try {
          await rateLimiter.wait();
          result = await this._fetchResultsPage(city, page, sessionCookie, ua);
        } catch (err) {
          log.error(`Request failed for ${city} page ${page}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Update session cookie if a new one was returned
        if (result.cookie) {
          sessionCookie = result.cookie;
        }

        // Handle rate limiting / bot detection
        if (result.statusCode === 429 || result.statusCode === 403) {
          log.warn(`Got ${result.statusCode} from ${this.name} for ${city}`);
          const shouldRetry = await rateLimiter.handleBlock(result.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (result.statusCode !== 200) {
          log.error(`Unexpected status ${result.statusCode} for ${city} page ${page} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(result.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(result.body);

        // Get total pages on first page
        if (page === 1) {
          totalPages = this._extractTotalPages($);
          const estimatedTotal = totalPages * this.pageSize;

          if (totalPages === 0 || $('table.tablesorter tbody tr').length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }

          log.success(`Found ~${estimatedTotal.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Yield each attorney
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we've reached the last page
        if (totalPages && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new MaineScraper();
