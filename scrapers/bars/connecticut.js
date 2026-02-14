/**
 * Connecticut Judicial Branch Attorney Scraper
 *
 * Source: https://www.jud.ct.gov/attorneyfirminquiry/AttorneyFirmInquiry.aspx
 * Method: ASP.NET POST with __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION
 * The search form requires fetching the page first to obtain ViewState tokens,
 * then submitting a POST with the hidden fields plus search parameters.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ConnecticutScraper extends BaseScraper {
  constructor() {
    super({
      name: 'connecticut',
      stateCode: 'CT',
      baseUrl: 'https://www.jud.ct.gov/attorneyfirminquiry/AttorneyFirmInquiry.aspx',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'labor',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
        'insurance':            'insurance',
        'securities':           'securities',
      },
      defaultCities: [
        'Hartford', 'New Haven', 'Stamford', 'Bridgeport',
        'Waterbury', 'Norwalk', 'Danbury', 'Greenwich',
      ],
    });
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
          'Referer': this.baseUrl,
        },
        timeout: 20000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Fetch the search page and extract ASP.NET hidden fields (__VIEWSTATE, etc.)
   */
  async fetchViewState(rateLimiter) {
    const response = await this.httpGet(this.baseUrl, rateLimiter);
    if (response.statusCode !== 200) {
      throw new Error(`Failed to fetch ViewState: status ${response.statusCode}`);
    }

    const $ = cheerio.load(response.body);
    const viewState = $('input[name="__VIEWSTATE"]').val() || '';
    const viewStateGenerator = $('input[name="__VIEWSTATEGENERATOR"]').val() || '';
    const eventValidation = $('input[name="__EVENTVALIDATION"]').val() || '';

    if (!viewState) {
      log.warn(`Could not extract __VIEWSTATE from CT search page`);
    }

    return { viewState, viewStateGenerator, eventValidation };
  }

  /**
   * Not used directly — search() is overridden for ASP.NET ViewState handling.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ASP.NET POST`);
  }

  parseResultsPage($) {
    const attorneys = [];

    // CT results are typically in a GridView table
    $('table[id*="GridView"] tr, table[id*="grd"] tr, table.results tr, table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 3) return;

      const firstCellText = $(cells[0]).text().trim().toLowerCase();
      if (firstCellText === 'name' || firstCellText === 'juris #' || firstCellText === 'attorney') return;
      // Skip header rows with <th>
      if ($row.find('th').length > 0) return;

      // Typical layout: Juris # | Name | Firm | City | Status
      const jurisNumber = $(cells[0]).text().trim();
      const nameCell = $(cells[1]);
      const fullName = nameCell.text().trim();
      const profileLink = nameCell.find('a').attr('href') || '';
      const firmName = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const city = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const status = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const phone = cells.length > 5 ? $(cells[5]).text().trim() : '';

      if (!fullName || fullName.length < 2) return;

      // Parse name — may be "Last, First" format
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = parts[1] || '';
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      let profileUrl = '';
      if (profileLink) {
        profileUrl = profileLink.startsWith('http')
          ? profileLink
          : `https://www.jud.ct.gov/attorneyfirminquiry/${profileLink}`;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: firmName,
        city: city,
        state: 'CT',
        phone: phone,
        email: '',
        website: '',
        bar_number: jurisNumber.replace(/[^0-9]/g, ''),
        bar_status: status || 'Active',
        profile_url: profileUrl,
      });
    });

    // Fallback: div-based results
    if (attorneys.length === 0) {
      $('.attorney-result, .search-result, .result-item').each((_, el) => {
        const $el = $(el);

        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .attorney-name, h3, h4').text().trim();
        const profileLink = nameEl.attr('href') || '';

        if (!fullName || fullName.length < 2) return;

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] || '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        const jurisNumber = ($el.find('.juris, .bar-number, .juris-number').text().trim() || '').replace(/[^0-9]/g, '');
        const city = $el.find('.city, .location').text().trim();
        const phone = $el.find('.phone').text().trim();
        const email = $el.find('a[href^="mailto:"]').text().trim();
        const firmName = $el.find('.firm, .firm-name').text().trim();
        const status = $el.find('.status').text().trim();

        let profileUrl = '';
        if (profileLink) {
          profileUrl = profileLink.startsWith('http')
            ? profileLink
            : `https://www.jud.ct.gov/attorneyfirminquiry/${profileLink}`;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: firmName,
          city: city,
          state: 'CT',
          phone: phone,
          email: email,
          website: '',
          bar_number: jurisNumber,
          bar_status: status || 'Active',
          profile_url: profileUrl,
        });
      });
    }

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();

    const matchFound = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?|members?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total\s+(?:Records?|Results?):\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    // Count GridView rows as fallback
    const rowCount = $('table[id*="GridView"] tr td, table[id*="grd"] tr td').closest('tr').length;
    if (rowCount > 0) return rowCount;

    return 0;
  }

  /**
   * Override search() to handle ASP.NET ViewState-based POST submissions.
   * First fetches the page to get __VIEWSTATE, then submits the form.
   * Iterates last name prefixes per city for broad coverage since the
   * search form requires name-based input.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`CT bar search does not filter by practice area — searching all attorneys`);
    }

    const cities = this.getCities(options);

    // Common last name prefixes for broad coverage
    const lastNamePrefixes = [
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
      'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
      'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Fetch initial ViewState for this city
      let viewStateData;
      try {
        await rateLimiter.wait();
        viewStateData = await this.fetchViewState(rateLimiter);
      } catch (err) {
        log.error(`Failed to fetch ViewState for ${city}: ${err.message}`);
        continue;
      }

      let pagesFetched = 0;

      for (const prefix of lastNamePrefixes) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build ASP.NET form data with ViewState
        // Actual form fields: txtCivInqName (last name/firm), txtJurisNo (juris number)
        const formData = {
          '__VIEWSTATE': viewStateData.viewState,
          '__VIEWSTATEGENERATOR': viewStateData.viewStateGenerator,
          '__EVENTVALIDATION': viewStateData.eventValidation,
          'txtCivInqName': prefix,
          'txtJurisNo': '',
        };

        log.info(`Searching ${city} — last name prefix "${prefix}" — POST ${this.baseUrl}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} prefix ${prefix}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping prefix ${prefix}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} prefix ${prefix} — skipping`);
          yield { _captcha: true, city, prefix };
          continue;
        }

        const $ = cheerio.load(response.body);

        // Update ViewState from response for subsequent requests
        const newViewState = $('input[name="__VIEWSTATE"]').val();
        const newViewStateGen = $('input[name="__VIEWSTATEGENERATOR"]').val();
        const newEventValidation = $('input[name="__EVENTVALIDATION"]').val();
        if (newViewState) viewStateData.viewState = newViewState;
        if (newViewStateGen) viewStateData.viewStateGenerator = newViewStateGen;
        if (newEventValidation) viewStateData.eventValidation = newEventValidation;

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          continue;
        }

        log.success(`Found ${attorneys.length} results for ${city} prefix "${prefix}"`);

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        pagesFetched++;
      }
    }
  }
}

module.exports = new ConnecticutScraper();
