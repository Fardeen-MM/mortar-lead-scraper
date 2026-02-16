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
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

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
   * Fetch a viewer page using the current session cookie.
   *
   * IMPORTANT: NC viewer pages (viewer.aspx?ID=...) are tied to the specific
   * search session that produced them. A viewer page can ONLY be accessed with
   * cookies from the POST search that returned that ID in its results. A generic
   * "establish session" search will NOT work for arbitrary IDs.
   *
   * Therefore, profile pages MUST be fetched inline during search(), immediately
   * after the POST search that found them. The waterfall's enrichFromProfile()
   * is overridden to return {} because post-hoc profile fetching is impossible.
   *
   * This helper is used by search() to fetch individual profiles within the
   * current search session.
   *
   * @param {string} url - The viewer page URL
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {CheerioStatic|null} Cheerio instance or null on failure
   */
  async fetchProfilePage(url, rateLimiter) {
    if (!url || !this._sessionCookie) return null;

    try {
      // Use shorter delay for profile pages (1-2s) vs full 5-10s for search pages.
      // Profile pages are lightweight GET requests within the same session.
      await sleep(1000 + Math.random() * 1000);
      const response = await this._httpGetWithCookies(url, rateLimiter, this._sessionCookie);

      if (response.statusCode !== 200) {
        log.warn(`NC profile page returned ${response.statusCode}: ${url}`);
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on NC profile page: ${url}`);
        return null;
      }

      // Validate the page has actual profile content (<dt> tags).
      // An expired or wrong-session response returns a generic 200 page
      // with no <dt> tags (because _httpGetWithCookies follows 302 redirects
      // transparently, masking the session failure).
      if (!response.body.includes('<dt>')) {
        log.warn(`NC profile page has no content (wrong session?): ${url}`);
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
   * Override enrichFromProfile to prevent the waterfall from attempting
   * post-hoc profile fetching. NC viewer pages are session-bound to the
   * specific search that returned them — they cannot be fetched later
   * with a different session. Profile data is fetched inline during search().
   */
  async enrichFromProfile(/* lead, rateLimiter */) {
    return {};
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
   *
   * NC viewer pages (viewer.aspx?ID=...) are tied to the specific ASP.NET
   * search session that produced them. A viewer page can only be accessed with
   * cookies from the POST search whose results included that ID. Therefore,
   * profile pages are fetched inline here — immediately after each city's
   * search, while the session is still valid for those results.
   *
   * Pass options.skipProfiles = true to skip profile fetching (e.g., in smoke tests).
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const skipProfiles = !!options.skipProfiles;

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

      // Fetch profile pages inline while the session is valid for this city's results.
      // NC viewer pages are session-bound — they can only be accessed with cookies
      // from the search that returned them, NOT from a separate session.
      // In test mode (maxPages set), limit profile fetches to avoid timeout.
      const maxProfileFetches = options.maxPages ? 20 : Infinity;
      if (!skipProfiles) {
        const fetchableCount = Math.min(attorneys.filter(a => a.profile_url).length, maxProfileFetches);
        log.info(`Fetching ${fetchableCount} profile pages for ${city}...`);
        let profilesFetched = 0;
        let profilesFailed = 0;
        for (const attorney of attorneys) {
          if (!attorney.profile_url) continue;
          if (profilesFetched + profilesFailed >= maxProfileFetches) break;
          try {
            const $profile = await this.fetchProfilePage(attorney.profile_url, rateLimiter);
            if ($profile) {
              const profileData = this.parseProfilePage($profile);
              // Merge profile data into attorney record without overwriting
              for (const [key, value] of Object.entries(profileData)) {
                if (value && (!attorney[key] || attorney[key] === '')) {
                  attorney[key] = value;
                }
              }
              profilesFetched++;
            } else {
              profilesFailed++;
            }
          } catch (err) {
            log.warn(`NC: Profile fetch error for ${attorney.first_name} ${attorney.last_name}: ${err.message}`);
            profilesFailed++;
          }
        }
        log.success(`Fetched ${profilesFetched} profiles for ${city} (${profilesFailed} failed)`);
      }

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
