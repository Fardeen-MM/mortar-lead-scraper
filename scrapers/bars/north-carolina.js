/**
 * North Carolina State Bar Scraper
 *
 * Source: https://portal.ncbar.gov/Verification/search.aspx
 * Method: ASP.NET WebForms POST with VIEWSTATE + session cookies
 *
 * The NC Bar uses an ASP.NET WebForms application that requires:
 * 1. GET the search page to obtain VIEWSTATE, EVENTVALIDATION, and session cookie
 * 2. POST the search form with city/state/status filters
 * 3. Parse the HTML table results
 *
 * Results are in <table class="table table-hover"> with columns:
 * Bar ID, Name, Type, Status, Location, Judicial District
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NorthCarolinaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'north-carolina',
      stateCode: 'NC',
      baseUrl: 'https://portal.ncbar.gov/Verification/search.aspx',
      pageSize: 250,
      practiceAreaCodes: {},
      defaultCities: [
        'Charlotte', 'Raleigh', 'Durham', 'Greensboro', 'Winston-Salem',
        'Fayetteville', 'Wilmington', 'Asheville', 'Cary', 'High Point',
        'Chapel Hill', 'Gastonia', 'Concord', 'Greenville',
      ],
    });
  }

  /**
   * Establish a valid ASP.NET session for accessing viewer pages.
   *
   * The NC Bar viewer pages (viewer.aspx?ID=...) require server-side session state
   * that is only set after a POST search. Simply GETting the search page is not
   * sufficient. We perform a minimal search (last name "A" in "Raleigh") to
   * establish the session, then cache the cookies for subsequent viewer requests.
   *
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {boolean} true if session was established successfully
   */
  async _establishProfileSession(rateLimiter) {
    try {
      // Step 1: GET search page for VIEWSTATE + session cookie
      await rateLimiter.wait();
      const getResp = await this._httpGetWithCookies(this.baseUrl, rateLimiter);
      if (getResp.statusCode !== 200) {
        log.warn(`NC: GET search page returned ${getResp.statusCode}`);
        return false;
      }

      const $form = cheerio.load(getResp.body);
      const formFields = this._extractFormFields($form);
      if (!formFields.__VIEWSTATE) {
        log.warn(`NC: No VIEWSTATE in search page for session init`);
        return false;
      }

      // Step 2: POST a minimal search to establish server-side state
      const postData = new URLSearchParams();
      postData.set('__VIEWSTATE', formFields.__VIEWSTATE);
      if (formFields.__EVENTVALIDATION) postData.set('__EVENTVALIDATION', formFields.__EVENTVALIDATION);
      if (formFields.__VIEWSTATEGENERATOR) postData.set('__VIEWSTATEGENERATOR', formFields.__VIEWSTATEGENERATOR);
      postData.set('ctl00$Content$txtFirst', '');
      postData.set('ctl00$Content$txtMiddle', '');
      postData.set('ctl00$Content$txtLast', 'A');
      postData.set('ctl00$Content$txtCity', 'Raleigh');
      postData.set('ctl00$Content$ddState', 'NC');
      postData.set('ctl00$Content$ddLicStatus', 'A');
      postData.set('ctl00$Content$ddJudicialDistrict', '');
      postData.set('ctl00$Content$txtLicNum', '');
      postData.set('ctl00$Content$ddLicType', '');
      postData.set('ctl00$Content$ddSpecialization', '');
      postData.set('ctl00$Content$btnSubmit', 'Search');

      await rateLimiter.wait();
      const postResp = await this._httpPostForm(
        this.baseUrl,
        postData.toString(),
        rateLimiter,
        getResp.cookies,
      );

      if (postResp.statusCode !== 200) {
        log.warn(`NC: POST search for session init returned ${postResp.statusCode}`);
        return false;
      }

      // Merge all cookies from GET and POST responses
      const allCookies = [getResp.cookies, postResp.cookies].filter(Boolean).join('; ');
      this._sessionCookie = allCookies;
      log.info(`NC: Established session for profile page fetching via POST search`);
      return true;
    } catch (err) {
      log.warn(`NC: Failed to establish profile session: ${err.message}`);
      return false;
    }
  }

  /**
   * Fetch a profile page with session cookie support.
   *
   * NC viewer pages (viewer.aspx?ID=...) require a valid ASP.NET session that
   * has been initialized via a POST search. On first call (or after session
   * expiry), we perform a minimal search to establish the session, then cache
   * the cookies for subsequent viewer requests.
   *
   * @param {string} url - The profile page URL
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {CheerioStatic|null} Cheerio instance or null on failure
   */
  async fetchProfilePage(url, rateLimiter) {
    if (!url) return null;

    try {
      // Establish session if we don't have one
      if (!this._sessionCookie) {
        const ok = await this._establishProfileSession(rateLimiter);
        if (!ok) return null;
      }

      // Fetch the viewer page with the session cookie
      await rateLimiter.wait();
      const response = await this._httpGetWithCookies(url, rateLimiter, this._sessionCookie);

      if (response.statusCode !== 200) {
        log.warn(`NC profile page returned ${response.statusCode}: ${url}`);
        // Session may have expired — clear it so next call re-establishes
        if (response.statusCode === 302 || response.statusCode === 500) {
          this._sessionCookie = null;
          // Retry once with a fresh session
          const ok = await this._establishProfileSession(rateLimiter);
          if (!ok) return null;
          await rateLimiter.wait();
          const retry = await this._httpGetWithCookies(url, rateLimiter, this._sessionCookie);
          if (retry.statusCode !== 200) {
            log.warn(`NC profile page retry returned ${retry.statusCode}: ${url}`);
            return null;
          }
          if (retry.cookies) this._sessionCookie = retry.cookies;
          return cheerio.load(retry.body);
        }
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on NC profile page: ${url}`);
        return null;
      }

      // Update cookies if the server sent new ones
      if (response.cookies) {
        this._sessionCookie = response.cookies;
      }

      return cheerio.load(response.body);
    } catch (err) {
      log.warn(`Failed to fetch NC profile page: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a NC attorney profile/viewer page for additional fields.
   *
   * NC viewer pages (viewer.aspx?ID=...) use <dl> with <dt>/<dd> pairs:
   *   - Bar #: "32407"
   *   - Name: "Ms. Amanda Joy Smith"
   *   - Address: "207 Furches" (may include <br /> for multiple lines)
   *   - City: "Raleigh"
   *   - State: "NC"
   *   - Zip Code: "27607"
   *   - Work Phone: "919-645-1792"
   *   - Email: "mandy_smith@nced.uscourts.gov"
   *   - Status: (inside <span class="label">)
   *   - Date Admitted: "04/08/2004"
   *   - Status Date: "04/08/2004"
   *   - Judicial District: "10 - Wake"
   *   - Board Certified In: (optional specialization)
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields extracted from the profile
   */
  parseProfilePage($) {
    const result = {};

    // Build a label -> value map from all dt/dd pairs
    const fields = {};
    $('dt').each((_, el) => {
      const label = $(el).text().trim().replace(/:$/, '').toLowerCase();
      const dd = $(el).next('dd');
      if (dd.length) {
        fields[label] = {
          text: dd.text().trim().replace(/\s+/g, ' '),
          html: dd.html() || '',
          el: dd,
        };
      }
    });

    // Phone: "Work Phone"
    if (fields['work phone']) {
      const phone = fields['work phone'].text;
      if (phone && phone.length > 5) {
        result.phone = phone;
      }
    }

    // Email
    if (fields['email']) {
      const email = fields['email'].text.trim().toLowerCase();
      if (email && email.includes('@')) {
        result.email = email;
      }
    }

    // Address — may contain <br /> for multi-line
    if (fields['address']) {
      const addressHtml = fields['address'].html;
      const addressParts = addressHtml
        .split(/<br\s*\/?>/)
        .map(s => s.replace(/<[^>]+>/g, '').trim())
        .filter(Boolean);
      const address = addressParts.join(', ');
      if (address && address.length > 3) {
        result.address = address;
      }
    }

    // City (may be more detailed than the search results)
    if (fields['city']) {
      const city = fields['city'].text;
      if (city && city.length > 1) {
        result.city = city;
      }
    }

    // State
    if (fields['state']) {
      const state = fields['state'].text;
      if (state && state.length === 2) {
        result.state = state;
      }
    }

    // Zip code
    if (fields['zip code']) {
      const zip = fields['zip code'].text;
      if (zip && zip.length >= 5) {
        result.zip = zip;
      }
    }

    // Admission date
    if (fields['date admitted']) {
      const dateAdmitted = fields['date admitted'].text;
      if (dateAdmitted && dateAdmitted.length >= 4) {
        result.admission_date = dateAdmitted;
      }
    }

    // Board certified specialization
    if (fields['board certified in']) {
      const cert = fields['board certified in'].text;
      if (cert && cert.length > 2) {
        result.practice_areas = cert;
      }
    }

    // Judicial district
    if (fields['judicial district']) {
      const district = fields['judicial district'].text;
      if (district && district.length > 1) {
        result.judicial_district = district;
      }
    }

    return result;
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  /**
   * HTTP GET with cookie support — returns response + cookies.
   */
  _httpGetWithCookies(url, rateLimiter, cookies = '') {
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
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 15000,
      };

      const req = https.request(options, (res) => {
        // Follow 302 redirects (needed for viewer.aspx session redirects)
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `https://${parsed.hostname}${redirect}`;
          const newCookies = (res.headers['set-cookie'] || [])
            .map(c => c.split(';')[0])
            .join('; ');
          const allCookies = [cookies, newCookies].filter(Boolean).join('; ');
          res.resume();
          return resolve(this._httpGetWithCookies(redirect, rateLimiter, allCookies));
        }
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: setCookies }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with cookie + form data support.
   */
  _httpPostForm(url, formBody, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const bodyBuffer = Buffer.from(formBody, 'utf8');
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
          'Content-Length': bodyBuffer.length,
          'Origin': 'https://portal.ncbar.gov',
          'Referer': url,
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        // Follow redirects (302) with GET
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `https://${parsed.hostname}${redirect}`;
          const newCookies = (res.headers['set-cookie'] || [])
            .map(c => c.split(';')[0])
            .join('; ');
          const allCookies = [cookies, newCookies].filter(Boolean).join('; ');
          // Consume the redirect response body
          res.resume();
          return resolve(this._httpGetWithCookies(redirect, rateLimiter, allCookies));
        }
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: setCookies }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(bodyBuffer);
      req.end();
    });
  }

  /**
   * Extract ASP.NET hidden fields from HTML.
   */
  _extractFormFields($) {
    const fields = {};
    const viewstate = $('input#__VIEWSTATE').val();
    const eventvalidation = $('input#__EVENTVALIDATION').val();
    const viewstategenerator = $('input#__VIEWSTATEGENERATOR').val();
    if (viewstate) fields.__VIEWSTATE = viewstate;
    if (eventvalidation) fields.__EVENTVALIDATION = eventvalidation;
    if (viewstategenerator) fields.__VIEWSTATEGENERATOR = viewstategenerator;
    return fields;
  }

  /**
   * Parse the NC Bar results table.
   * Columns: Bar ID, Name, Type, Status, Location, Judicial District
   */
  _parseResultsTable($) {
    const attorneys = [];

    $('table.table-hover tbody tr, table.table-hover tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 5) return;

      const barNumber = $(cells[0]).text().trim();
      const fullName = $(cells[1]).text().trim();
      const type = $(cells[2]).text().trim();
      const status = $(cells[3]).text().trim();
      const location = $(cells[4]).text().trim();

      // Extract profile URL from the name link (e.g., /Verification/viewer.aspx?ID=32407)
      const nameLink = $(cells[1]).find('a').attr('href') || '';
      const profileUrl = nameLink ? `https://portal.ncbar.gov${nameLink}` : '';

      if (!fullName || type !== 'Attorney') return;

      // Parse "City, ST" from location
      let city = '';
      let state = 'NC';
      const locMatch = location.match(/^(.+),\s*([A-Z]{2})$/);
      if (locMatch) {
        city = locMatch[1].trim();
        state = locMatch[2];
      } else {
        city = location;
      }

      // Parse name — format is typically "Ms. First Last" or "Mr. First Middle Last"
      let firstName = '';
      let lastName = '';
      const nameWithoutPrefix = fullName.replace(/^(?:Mr\.|Ms\.|Mrs\.|Dr\.|Hon\.)\s*/i, '').trim();
      const nameParts = nameWithoutPrefix.split(/\s+/).filter(Boolean);
      if (nameParts.length >= 2) {
        firstName = nameParts[0];
        lastName = nameParts[nameParts.length - 1];
      } else if (nameParts.length === 1) {
        lastName = nameParts[0];
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: state,
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: status,
        practice_area: '',
        profile_url: profileUrl,
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the NC Bar directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`NC Bar directory does not support practice area filtering — searching all attorneys`);
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Step 1: GET the search page to establish session + get VIEWSTATE
      let sessionResponse;
      try {
        await rateLimiter.wait();
        sessionResponse = await this._httpGetWithCookies(this.baseUrl, rateLimiter);
      } catch (err) {
        log.error(`Failed to load search page for ${city}: ${err.message}`);
        continue;
      }

      if (sessionResponse.statusCode !== 200) {
        log.error(`Search page returned ${sessionResponse.statusCode} for ${city}`);
        continue;
      }

      const sessionCookies = sessionResponse.cookies;
      const $form = cheerio.load(sessionResponse.body);
      const formFields = this._extractFormFields($form);

      if (!formFields.__VIEWSTATE) {
        log.error(`Could not extract VIEWSTATE for ${city} — page structure may have changed`);
        continue;
      }

      // Step 2: POST the search form
      const postData = new URLSearchParams();
      postData.set('__VIEWSTATE', formFields.__VIEWSTATE);
      if (formFields.__EVENTVALIDATION) postData.set('__EVENTVALIDATION', formFields.__EVENTVALIDATION);
      if (formFields.__VIEWSTATEGENERATOR) postData.set('__VIEWSTATEGENERATOR', formFields.__VIEWSTATEGENERATOR);
      postData.set('ctl00$Content$txtFirst', '');
      postData.set('ctl00$Content$txtMiddle', '');
      postData.set('ctl00$Content$txtLast', '');
      postData.set('ctl00$Content$txtCity', city);
      postData.set('ctl00$Content$ddState', 'NC');
      postData.set('ctl00$Content$ddLicStatus', 'A');
      postData.set('ctl00$Content$ddJudicialDistrict', '');
      postData.set('ctl00$Content$txtLicNum', '');
      postData.set('ctl00$Content$ddLicType', '');
      postData.set('ctl00$Content$ddSpecialization', '');
      postData.set('ctl00$Content$btnSubmit', 'Search');

      log.info(`POST search for ${city}...`);

      let searchResponse;
      try {
        await rateLimiter.wait();
        searchResponse = await this._httpPostForm(
          this.baseUrl,
          postData.toString(),
          rateLimiter,
          sessionCookies,
        );
      } catch (err) {
        log.error(`Search POST failed for ${city}: ${err.message}`);
        continue;
      }

      if (searchResponse.statusCode !== 200) {
        log.error(`Search POST returned ${searchResponse.statusCode} for ${city}`);
        continue;
      }

      rateLimiter.resetBackoff();

      // Save session cookies for profile page fetching (viewer.aspx needs active session)
      const allSearchCookies = [sessionCookies, searchResponse.cookies].filter(Boolean).join('; ');
      this._sessionCookie = allSearchCookies;

      const $ = cheerio.load(searchResponse.body);
      const attorneys = this._parseResultsTable($);

      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      log.success(`Found ${attorneys.length} results for ${city}`);

      for (const attorney of attorneys) {
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }
        attorney.practice_area = practiceArea || '';
        yield attorney;
      }
    }
  }
}

module.exports = new NorthCarolinaScraper();
