/**
 * Illinois Bar (IARDC) Scraper
 *
 * Source: https://www.iardc.org/Lawyer/Search
 * Method: ASP.NET MVC with MVC Grid — 3-step HTTP flow
 *
 * Flow:
 * 1. GET /Lawyer/Search — obtain __RequestVerificationToken + session cookie
 * 2. POST /Lawyer/SearchResults — submit search form to get PageKey
 * 3. POST /Lawyer/SearchGrid?page=N&rows=100 — fetch paginated HTML table results
 *
 * The IARDC search requires a last name (city-only returns no results).
 * This scraper iterates A-Z for each city to get comprehensive coverage.
 *
 * Grid columns: id, include-former-names, name, city, state, date-admitted, authorized-to-practice
 *
 * Profile pages: The IARDC grid renders attorney names as
 *   <a href="#" class="show-lawyer-profile" data-id="GUID">
 * but individual attorney detail is loaded via JavaScript row expansion
 * within the MVC Grid. There is no standalone profile URL accessible via
 * HTTP GET or POST — the detail view requires client-side JavaScript execution.
 * Therefore, parseProfilePage() CANNOT be implemented without a headless browser.
 * The scraper does NOT yield profile_url.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IllinoisScraper extends BaseScraper {
  constructor() {
    super({
      name: 'illinois',
      stateCode: 'IL',
      baseUrl: 'https://www.iardc.org',
      pageSize: 100,
      practiceAreaCodes: {},
      defaultCities: [
        'Chicago', 'Springfield', 'Rockford', 'Naperville', 'Peoria',
        'Joliet', 'Elgin', 'Aurora', 'Champaign', 'Bloomington',
        'Decatur', 'Schaumburg', 'Wheaton', 'Waukegan',
      ],
    });

    this.searchUrl = `${this.baseUrl}/Lawyer/Search`;
    this.resultsUrl = `${this.baseUrl}/Lawyer/SearchResults`;
    this.gridUrl = `${this.baseUrl}/Lawyer/SearchGrid`;
    this.lastNameLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for IARDC MVC Grid`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for IARDC MVC Grid`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for IARDC MVC Grid`);
  }

  /**
   * HTTP GET with cookie support.
   */
  _httpGet(url, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 15000,
      };

      const req = https.get(url, options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: setCookies }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * HTTP POST with form data and cookie support.
   */
  _httpPost(url, formBody, rateLimiter, cookies = '', extraHeaders = {}) {
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
          'Accept': 'text/html, */*; q=0.01',
          'Accept-Language': 'en-US,en;q=0.9',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': bodyBuffer.length,
          'Referer': this.searchUrl,
          ...(cookies ? { 'Cookie': cookies } : {}),
          ...extraHeaders,
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) redirect = `https://${parsed.hostname}${redirect}`;
          const newCookies = (res.headers['set-cookie'] || [])
            .map(c => c.split(';')[0])
            .join('; ');
          const allCookies = [cookies, newCookies].filter(Boolean).join('; ');
          res.resume();
          return resolve(this._httpGet(redirect, rateLimiter, allCookies));
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
   * Parse the MVC Grid HTML table response into attorney records.
   */
  _parseGridRows($) {
    const attorneys = [];

    $('table tr').each((_, row) => {
      const $row = $(row);
      // Skip header rows
      if ($row.find('th').length > 0) return;
      if ($row.hasClass('mvc-grid-headers')) return;

      const cells = $row.find('td');
      if (cells.length < 5) return;

      // Columns: id, include-former-names, name, city, state, date-admitted, authorized-to-practice
      const id = $(cells[0]).text().trim();
      const fullName = $(cells[2]).text().trim();
      const city = $(cells[3]).text().trim();
      const state = $(cells[4]).text().trim();
      const dateAdmitted = cells.length > 5 ? $(cells[5]).text().trim() : '';
      const authorized = cells.length > 6 ? $(cells[6]).text().trim() : '';

      if (!fullName || fullName === 'No Data Found') return;

      // Parse name — IARDC format: "Last, First Middle" or "Hyphenated-Last, First"
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const nameParts = fullName.split(/\s+/);
        if (nameParts.length >= 2) {
          firstName = nameParts[0];
          lastName = nameParts[nameParts.length - 1];
        } else {
          lastName = fullName;
        }
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: state || 'IL',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        admission_date: dateAdmitted,
        bar_status: authorized === 'Yes' ? 'Authorized' : authorized === 'No' ? 'Not Authorized' : authorized,
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Extract total page count from the MVC Grid pager.
   */
  _extractTotalPages($) {
    const pagerItems = [];
    $('[data-page]').each((_, el) => {
      const page = parseInt($(el).attr('data-page'), 10);
      if (!isNaN(page)) pagerItems.push(page);
    });
    return pagerItems.length > 0 ? Math.max(...pagerItems) : 1;
  }

  /**
   * Async generator that yields attorney records from the IARDC.
   * Iterates A-Z for each city since the search requires a last name.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    if (practiceArea) {
      log.warn(`Illinois IARDC does not support practice area filtering — ignoring "${practiceArea}"`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, IL`);

      let totalForCity = 0;

      const letters = options.maxPrefixes
        ? this.lastNameLetters.slice(0, options.maxPrefixes)
        : this.lastNameLetters;

      for (const letter of letters) {
        // Step 1: GET search page for token + session cookie
        let sessionResponse;
        try {
          await rateLimiter.wait();
          sessionResponse = await this._httpGet(this.searchUrl, rateLimiter);
        } catch (err) {
          log.error(`Failed to load search page: ${err.message}`);
          continue;
        }

        if (sessionResponse.statusCode !== 200) {
          log.error(`Search page returned ${sessionResponse.statusCode}`);
          continue;
        }

        const sessionCookies = sessionResponse.cookies;
        const $form = cheerio.load(sessionResponse.body);
        const token = $form('input[name="__RequestVerificationToken"]').val();

        if (!token) {
          log.error(`Could not extract anti-forgery token`);
          continue;
        }

        // Step 2: POST search to get PageKey
        const searchFormData = new URLSearchParams();
        searchFormData.set('__RequestVerificationToken', token);
        searchFormData.set('LastName', letter);
        searchFormData.set('FirstName', '');
        searchFormData.set('City', city);
        searchFormData.set('Status', 'AuthorizedToPractice');
        searchFormData.set('LastNameMatch', 'Exact');
        searchFormData.set('IncludeFormerNames', 'false');
        searchFormData.set('IsRecentSearch', 'false');

        let resultsResponse;
        try {
          await rateLimiter.wait();
          resultsResponse = await this._httpPost(
            this.resultsUrl,
            searchFormData.toString(),
            rateLimiter,
            sessionCookies,
          );
        } catch (err) {
          log.error(`Search POST failed for ${city}/${letter}: ${err.message}`);
          continue;
        }

        if (resultsResponse.statusCode !== 200) {
          log.error(`Search POST returned ${resultsResponse.statusCode} for ${city}/${letter}`);
          continue;
        }

        const allCookies = [sessionCookies, resultsResponse.cookies].filter(Boolean).join('; ');

        const $results = cheerio.load(resultsResponse.body);
        const pageKeyMatch = resultsResponse.body.match(/PageKey:\s*"([^"]+)"/);
        if (!pageKeyMatch) {
          log.info(`No PageKey for ${city}/${letter} — likely no results`);
          continue;
        }

        const pageKey = pageKeyMatch[1];
        const token2 = $results('input[name="__RequestVerificationToken"]').val() || token;

        // Step 3: Fetch grid pages
        let gridPage = 1;
        let pagesFetched = 0;
        let totalPages = 1;

        while (true) {
          if (options.maxPages && pagesFetched >= options.maxPages) {
            break;
          }

          const gridFormData = new URLSearchParams();
          gridFormData.set('__RequestVerificationToken', token2);
          gridFormData.set('PageKey', pageKey);
          gridFormData.set('LastName', letter);
          gridFormData.set('City', city);
          gridFormData.set('Status', '0');
          gridFormData.set('LastNameMatch', '0');
          gridFormData.set('IncludeFormerNames', 'false');
          gridFormData.set('FirstName', '');
          gridFormData.set('StatusLastName', '');
          gridFormData.set('State', '');
          gridFormData.set('Country', '');
          gridFormData.set('StatusChangeTimeFrame', '0');
          gridFormData.set('BusinessLocation', '0');
          gridFormData.set('County', '');
          gridFormData.set('LawyerCounty', '');
          gridFormData.set('JudicialCircuit', '');
          gridFormData.set('JudicialDistrict', '');
          gridFormData.set('IsRecentSearch', 'false');

          const gridUrlWithPage = `${this.gridUrl}?page=${gridPage}&rows=${this.pageSize}`;

          let gridResponse;
          try {
            await rateLimiter.wait();
            gridResponse = await this._httpPost(
              gridUrlWithPage,
              gridFormData.toString(),
              rateLimiter,
              allCookies,
              { 'X-Requested-With': 'XMLHttpRequest', 'Referer': this.resultsUrl },
            );
          } catch (err) {
            log.error(`Grid request failed for ${city}/${letter} page ${gridPage}: ${err.message}`);
            break;
          }

          if (gridResponse.statusCode !== 200) {
            log.error(`Grid returned ${gridResponse.statusCode} for ${city}/${letter}`);
            break;
          }

          rateLimiter.resetBackoff();

          const $grid = cheerio.load(gridResponse.body);
          const attorneys = this._parseGridRows($grid);

          if (pagesFetched === 0) {
            totalPages = this._extractTotalPages($grid);
          }

          if (attorneys.length === 0) {
            break;
          }

          for (const attorney of attorneys) {
            if (options.minYear && attorney.admission_date) {
              const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
              if (year > 0 && year < options.minYear) continue;
            }
            attorney.practice_area = practiceArea || '';
            yield attorney;
            totalForCity++;
          }

          if (gridPage >= totalPages) {
            break;
          }

          gridPage++;
          pagesFetched++;
        }

        if (pagesFetched > 0 || totalPages > 1) {
          log.info(`${city}/${letter}: ${totalPages} pages fetched`);
        }
      }

      if (totalForCity > 0) {
        log.success(`Found ${totalForCity} total results for ${city}`);
      } else {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      }
    }
  }
}

module.exports = new IllinoisScraper();
