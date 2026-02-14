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
