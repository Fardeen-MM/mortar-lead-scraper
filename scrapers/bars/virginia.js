/**
 * Virginia State Bar (VSB) Scraper
 *
 * Source: https://vsb.org/Shared_Content/Directory/va-lawyer-directory.aspx
 * Method: ASP.NET async postback (UpdatePanel + RadGrid)
 *
 * The VSB uses an iMIS-based ASP.NET WebForms application with Telerik RadGrid.
 * Search is done via __doPostBack async postbacks.
 *
 * Flow:
 * 1. GET the search page to obtain __VIEWSTATE and hidden form fields
 * 2. POST async postback with search params (last name, etc.)
 * 3. Parse the RadGrid table from the delta response
 *
 * The VSB search does NOT have a city filter — results are searched by last name
 * and filtered by city client-side. This scraper iterates A-Z per city.
 *
 * Grid columns: BarID, Name, MemberType, LicenseType, Status, City, State, Zip,
 * HasDiscipline, SuspensionType, DateOfLicense, SortCode
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class VirginiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'virginia',
      stateCode: 'VA',
      baseUrl: 'https://vsb.org/Shared_Content/Directory/va-lawyer-directory.aspx',
      pageSize: 10, // RadGrid default page size
      practiceAreaCodes: {},
      defaultCities: [
        'Virginia Beach', 'Norfolk', 'Richmond', 'Arlington', 'Alexandria',
        'Newport News', 'Chesapeake', 'Hampton', 'Roanoke', 'Fairfax',
        'Charlottesville', 'Lynchburg', 'McLean', 'Tysons',
      ],
    });

    this.formPrefix = 'ctl01$TemplateBody$WebPartManager1$gwpciVirginiaLawyerSearch$ciVirginiaLawyerSearch$ResultsGrid$Sheet0';
    this.gridId = 'ctl01$TemplateBody$WebPartManager1$gwpciVirginiaLawyerSearch$ciVirginiaLawyerSearch$ResultsGrid$Grid1';
    this.lastNameLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for VSB async postback`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for VSB async postback`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for VSB async postback`);
  }

  /**
   * HTTP GET with cookie support.
   */
  _httpGet(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const req = https.get(url, {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Encoding': 'identity',
        },
        timeout: 20000,
      }, (res) => {
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
   * POST async postback (delta request) with form data.
   */
  _asyncPostback(formData, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(this.baseUrl);
      const postBody = new URLSearchParams(formData).toString();
      const bodyBuffer = Buffer.from(postBody, 'utf8');

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': '*/*',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': bodyBuffer.length,
          'X-Requested-With': 'XMLHttpRequest',
          'X-MicrosoftAjax': 'Delta=true',
          'Referer': this.baseUrl,
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
      req.write(bodyBuffer);
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
    return fields;
  }

  /**
   * Extract updated __VIEWSTATE from delta response.
   */
  _extractViewstateFromDelta(deltaBody) {
    const vsMatch = deltaBody.match(/\|hiddenField\|__VIEWSTATE\|([^|]*)\|/);
    return vsMatch ? vsMatch[1] : null;
  }

  /**
   * Parse RadGrid rows from delta response body.
   * Returns array of attorney objects.
   */
  _parseRadGridRows(deltaBody) {
    const attorneys = [];

    // Extract RadGrid HTML from delta response
    const rows = deltaBody.match(/<tr[^>]*class="rg(?:Row|AltRow)[^"]*"[^>]*>.*?<\/tr>/gs) || [];

    for (const rowHtml of rows) {
      const $ = cheerio.load(rowHtml);
      const cells = $('td');
      if (cells.length < 8) continue;

      const barNumber = $(cells[0]).text().trim();
      const fullName = $(cells[1]).text().trim();
      const memberType = $(cells[2]).text().trim();
      const licenseType = $(cells[3]).text().trim();
      const status = $(cells[4]).text().trim();
      const city = $(cells[5]).text().trim();
      const state = $(cells[6]).text().trim();
      const zip = $(cells[7]).text().trim();
      const dateOfLicense = cells.length > 10 ? $(cells[10]).text().trim() : '';

      if (!fullName) continue;

      // Parse name — VSB format: "First Last" or "First Middle Last"
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const nameParts = fullName.split(/\s+/).filter(Boolean);
        if (nameParts.length >= 2) {
          firstName = nameParts[0];
          lastName = nameParts[nameParts.length - 1];
        } else if (nameParts.length === 1) {
          lastName = nameParts[0];
        }
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: state || 'VA',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: dateOfLicense,
        bar_status: status,
        member_type: memberType,
        license_type: licenseType,
        source: `${this.name}_bar`,
      });
    }

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the VSB directory.
   * Iterates A-Z for each city since the search has no city filter.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`Virginia bar search does not support practice area filtering — searching all attorneys`);
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let totalForCity = 0;

      for (const letter of this.lastNameLetters) {
        // Step 1: GET the search page (fresh session per letter to avoid stale viewstate)
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
          log.error(`Could not extract __VIEWSTATE`);
          continue;
        }

        // Step 2: POST async postback with search params
        const submitBtn = `${this.formPrefix}$SubmitButton`;
        const form = { ...hiddenFields };
        form['__EVENTTARGET'] = submitBtn;
        form['__EVENTARGUMENT'] = '';
        form[`${this.formPrefix}$Input0$TextBox1`] = ''; // Bar ID#
        form[`${this.formPrefix}$Input1$TextBox1`] = ''; // First Name
        form[`${this.formPrefix}$Input2$TextBox1`] = letter; // Last Name
        form['ctl01$ScriptManager1'] = `${this.formPrefix}$ctl01|${submitBtn}`;
        form['__ASYNCPOST'] = 'true';
        form['IsControlPostBack'] = '1';

        let searchResponse;
        try {
          await rateLimiter.wait();
          searchResponse = await this._asyncPostback(form, rateLimiter, sessionCookies);
        } catch (err) {
          log.error(`Search POST failed for ${city}/${letter}: ${err.message}`);
          continue;
        }

        if (searchResponse.statusCode !== 200) {
          log.error(`Search POST returned ${searchResponse.statusCode} for ${city}/${letter}`);
          continue;
        }

        rateLimiter.resetBackoff();

        // Parse results from delta response
        const allAttorneys = this._parseRadGridRows(searchResponse.body);

        // Filter by city (case-insensitive) and active status
        const cityLower = city.toLowerCase();
        const cityAttorneys = allAttorneys.filter(a =>
          a.city.toLowerCase() === cityLower &&
          (a.bar_status === 'In Good Standing' || a.member_type === 'Active')
        );

        for (const attorney of cityAttorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          attorney.practice_area = practiceArea || '';
          delete attorney.member_type;
          delete attorney.license_type;
          yield attorney;
          totalForCity++;
        }

        // Handle RadGrid pagination — get additional pages if there were results
        let currentViewstate = this._extractViewstateFromDelta(searchResponse.body);
        let currentCookies = searchResponse.cookies;
        let pageNum = 2;
        let maxGridPages = options.maxPages || 20;

        while (allAttorneys.length >= this.pageSize && pageNum <= maxGridPages) {
          // Build page change postback
          const pageForm = { ...hiddenFields };
          if (currentViewstate) pageForm['__VIEWSTATE'] = currentViewstate;
          pageForm['__EVENTTARGET'] = this.gridId;
          pageForm['__EVENTARGUMENT'] = `FireCommand:${this.gridId.replace(/\$/g, '_')}$ctl00;PageSize;10`;
          pageForm[`${this.formPrefix}$Input0$TextBox1`] = '';
          pageForm[`${this.formPrefix}$Input1$TextBox1`] = '';
          pageForm[`${this.formPrefix}$Input2$TextBox1`] = letter;
          pageForm['ctl01$ScriptManager1'] = `${this.gridId.replace('$Grid1', '$ListerPanel')}|${this.gridId}`;
          pageForm['__ASYNCPOST'] = 'true';
          pageForm['IsControlPostBack'] = '1';
          // Telerik RadGrid page change uses __EVENTARGUMENT
          pageForm['__EVENTARGUMENT'] = `Page$${pageNum}`;

          let pageResponse2;
          try {
            await rateLimiter.wait();
            pageResponse2 = await this._asyncPostback(pageForm, rateLimiter, currentCookies);
          } catch (err) {
            log.error(`Grid page ${pageNum} failed for ${city}/${letter}: ${err.message}`);
            break;
          }

          if (pageResponse2.statusCode !== 200) break;

          const pageAttorneys = this._parseRadGridRows(pageResponse2.body);
          if (pageAttorneys.length === 0) break;

          const pageCityAttorneys = pageAttorneys.filter(a =>
            a.city.toLowerCase() === cityLower &&
            (a.bar_status === 'In Good Standing' || a.member_type === 'Active')
          );

          for (const attorney of pageCityAttorneys) {
            if (options.minYear && attorney.admission_date) {
              const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
              if (year > 0 && year < options.minYear) continue;
            }
            attorney.practice_area = practiceArea || '';
            delete attorney.member_type;
            delete attorney.license_type;
            yield attorney;
            totalForCity++;
          }

          currentViewstate = this._extractViewstateFromDelta(pageResponse2.body) || currentViewstate;
          currentCookies = pageResponse2.cookies;

          if (pageAttorneys.length < this.pageSize) break;
          pageNum++;
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

module.exports = new VirginiaScraper();
