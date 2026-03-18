/**
 * South Carolina Contractor's Licensing Board (SC CLB) — Contractor Scraper
 *
 * Source: https://verify.llronline.com/LicLookup/Contractors/Contractor.aspx?div=69
 * Platform: ASP.NET WebForms (IIS 10.0, .NET 4.0), hosted by SC LLR
 * Records: Unknown total (no bulk download available)
 * Method: ASP.NET form POST with reCAPTCHA v2 enforcement
 *
 * STATUS: PLACEHOLDER — reCAPTCHA v2 is enforced server-side. POSTing without a valid
 * reCAPTCHA token results in a 302 redirect to /liclookup/ErrorPage.aspx.
 *
 * Form structure (Contractor.aspx?div=69):
 *   - ctl00$ContentPlaceHolder1$UserInputGen$txt_lastName   — Last Name
 *   - ctl00$ContentPlaceHolder1$UserInputGen$txt_firstName  — First Name
 *   - ctl00$ContentPlaceHolder1$UserInputGen$txt_licNum     — License Number (numbers only)
 *   - ctl00$ContentPlaceHolder1$UserInputGen$txt_city       — City
 *   - ctl00$ContentPlaceHolder1$UserInputGen$ddl_type       — License Type (classification code)
 *   - ctl00$ContentPlaceHolder1$btn_find                    — Submit button (hidden, triggered by reCAPTCHA callback)
 *   - __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION  — ASP.NET state tokens
 *
 * reCAPTCHA v2 site key: 6Lc2X-saAAAAAPC6HatgHFOd8rCxCl-2yPTh44PN
 * The Find button is a g-recaptcha element that triggers onSubmit(token) callback.
 * The callback clicks the hidden btn_find to submit the form WITH the reCAPTCHA token.
 *
 * Cookie handshake:
 *   1. GET /Contractor.aspx → 302 + Set-Cookie: AspxAutoDetectCookieSupport=1
 *   2. GET /Contractor.aspx?AspxAutoDetectCookieSupport=1 → 200 with form
 *   3. POST with ViewState + form data + reCAPTCHA token → results page
 *   Without a valid reCAPTCHA token, step 3 returns 302 to ErrorPage.aspx.
 *
 * Also available: Residential Builders (div=46) at Resbu/Resbu.aspx — same reCAPTCHA block.
 *
 * License classifications (from ddl_type dropdown with div=69):
 *   All  — All Classifications
 *   AC   — Air Conditioning
 *   AP   — Asphalt Paving
 *   BL   — Boiler Installation
 *   BT   — Boring And Tunneling
 *   BR   — Bridges
 *   BD   — Building
 *   CT   — Concrete
 *   CP   — Concrete Paving
 *   CCM  — Construction Manager
 *   EL   — Electrical
 *   GG   — Glass And Glazing
 *   GD   — Grading
 *   HT   — Heating
 *   HY   — Highway (includes AP, CP, BR, GD, HI)
 *   HI   — Highway Incidental
 *   LP   — Lightning Protection
 *   MR   — Marine
 *   MS   — Masonry
 *   MM   — Miscellaneous Metals
 *   NR   — Nonstructural Renovation
 *   PK   — Packaged Equipment
 *   PL   — Pipelines
 *   PB   — Plumbing
 *   MB   — Pre-engineered Metal Buildings
 *   1P   — Process Piping (1P or 2P)
 *   1U   — Public Utility Electrical (1U or 2U)
 *   RR   — Railroad
 *   RG   — Refrigeration
 *   RF   — Roofing
 *   SF   — Structural Framing
 *   SP   — Swimming Pools
 *   CLT  — Swimming Pools (Temp)
 *   WL   — Water And Sewer Lines
 *   WP   — Water And Sewer Plants
 *   WF   — Wood Frame Structures
 *
 * Result fields (from search results GridView, gv_results):
 *   License #, Name, City, State, Classification, Status, Expiration Date
 *
 * Contact: SC Contractor's Licensing Board
 *   Phone: (803) 896-4686
 *   Email: Contact.CLB@llr.sc.gov
 *   Web:   https://llr.sc.gov/clb/
 *
 * Future: Could be unblocked with Puppeteer + reCAPTCHA v2 solving service.
 */

const cheerio = require('cheerio');
const https = require('https');
const http = require('http');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://verify.llronline.com';
const SEARCH_URL = `${BASE_URL}/LicLookup/Contractors/Contractor.aspx`;
const SEARCH_URL_WITH_DIV = `${SEARCH_URL}?div=69`;

// License classification codes from the ddl_type dropdown
const CLASSIFICATION_CODES = {
  'All':  'All Classifications',
  'AC':   'Air Conditioning',
  'AP':   'Asphalt Paving',
  'BL':   'Boiler Installation',
  'BT':   'Boring And Tunneling',
  'BR':   'Bridges',
  'BD':   'Building',
  'CT':   'Concrete',
  'CP':   'Concrete Paving',
  'CCM':  'Construction Manager',
  'EL':   'Electrical',
  'GG':   'Glass And Glazing',
  'GD':   'Grading',
  'HT':   'Heating',
  'HY':   'Highway',
  'HI':   'Highway Incidental',
  'LP':   'Lightning Protection',
  'MR':   'Marine',
  'MS':   'Masonry',
  'MM':   'Miscellaneous Metals',
  'NR':   'Nonstructural Renovation',
  'PK':   'Packaged Equipment',
  'PL':   'Pipelines',
  'PB':   'Plumbing',
  'MB':   'Pre-engineered Metal Buildings',
  '1P':   'Process Piping',
  '1U':   'Public Utility Electrical',
  'RR':   'Railroad',
  'RG':   'Refrigeration',
  'RF':   'Roofing',
  'SF':   'Structural Framing',
  'SP':   'Swimming Pools',
  'CLT':  'Swimming Pools (Temp)',
  'WL':   'Water And Sewer Lines',
  'WP':   'Water And Sewer Plants',
  'WF':   'Wood Frame Structures',
};

// Practice area → classification code mapping
const PRACTICE_AREA_CODES = {
  'general contractor':   'BD',
  'building':             'BD',
  'residential':          'WF',
  'electrical':           'EL',
  'plumbing':             'PB',
  'hvac':                 'AC',
  'air conditioning':     'AC',
  'heating':              'HT',
  'roofing':              'RF',
  'concrete':             'CT',
  'masonry':              'MS',
  'grading':              'GD',
  'highway':              'HY',
  'swimming pools':       'SP',
  'pools':                'SP',
  'pipelines':            'PL',
  'refrigeration':        'RG',
  'marine':               'MR',
  'glazing':              'GG',
  'glass':                'GG',
  'framing':              'SF',
  'structural':           'SF',
  'paving':               'AP',
  'boiler':               'BL',
  'water sewer':          'WL',
  'construction manager': 'CCM',
};

// Major SC cities
const DEFAULT_CITIES = [
  'Columbia', 'Charleston', 'Greenville', 'Mount Pleasant', 'Rock Hill',
  'Spartanburg', 'Summerville', 'North Charleston', 'Florence', 'Myrtle Beach',
  'Anderson', 'Hilton Head Island', 'Aiken', 'Goose Creek', 'Bluffton',
  'Greer', 'Simpsonville', 'Lexington', 'Easley', 'Beaufort',
];


class ScGcScraper extends BaseScraper {
  constructor() {
    super({
      name: 'sc_gc',
      stateCode: 'SC-GC',
      baseUrl: SEARCH_URL_WITH_DIV,
      pageSize: 50,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() { throw new Error('sc_gc: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('sc_gc: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('sc_gc: extractResultCount() not used — search() overridden'); }

  /**
   * Override CAPTCHA detection — the SC LLR site has reCAPTCHA loaded in page HTML.
   * Detect by looking for the g-recaptcha class or recaptcha script tag.
   */
  detectCaptcha(body) {
    return body.includes('g-recaptcha') ||
           body.includes('recaptcha/api.js') ||
           body.includes('grecaptcha');
  }

  transformResult(lead, practiceArea) {
    lead.source = 'sc_gc';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTP GET with cookie jar support for ASP.NET cookie detection handshake.
   *
   * The SC LLR verify site does a two-step cookie check:
   *   1. First request → 302 + Set-Cookie: AspxAutoDetectCookieSupport=1
   *   2. Redirect to ?AspxAutoDetectCookieSupport=1 with cookie → 200
   *
   * This method handles the handshake and returns the final page body.
   */
  _httpGetWithCookies(url, rateLimiter, cookies = {}, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error('Too many redirects (>5)'));
      }

      const parsed = new URL(url);
      const protocol = parsed.protocol === 'https:' ? https : http;
      const cookieHeader = Object.entries(cookies)
        .map(([k, v]) => `${k}=${v}`)
        .join('; ');

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''),
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      if (cookieHeader) {
        options.headers['Cookie'] = cookieHeader;
      }

      const req = protocol.request(options, (res) => {
        // Collect Set-Cookie headers
        const setCookies = res.headers['set-cookie'] || [];
        for (const sc of setCookies) {
          const match = sc.match(/^([^=]+)=([^;]*)/);
          if (match) cookies[match[1]] = match[2];
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return this._httpGetWithCookies(redirect, rateLimiter, cookies, redirectCount + 1)
            .then(resolve).catch(reject);
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with cookie jar and ASP.NET form data.
   */
  _httpPostWithCookies(url, formData, rateLimiter, cookies = {}) {
    const querystring = require('querystring');
    const postData = querystring.stringify(formData);

    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const protocol = parsed.protocol === 'https:' ? https : http;
      const cookieHeader = Object.entries(cookies)
        .map(([k, v]) => `${k}=${v}`)
        .join('; ');

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''),
        method: 'POST',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Origin': BASE_URL,
          'Referer': url,
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      if (cookieHeader) {
        options.headers['Cookie'] = cookieHeader;
      }

      const req = protocol.request(options, (res) => {
        // Collect Set-Cookie headers
        const setCookies = res.headers['set-cookie'] || [];
        for (const sc of setCookies) {
          const match = sc.match(/^([^=]+)=([^;]*)/);
          if (match) cookies[match[1]] = match[2];
        }

        // Check for redirect (reCAPTCHA failure → 302 to ErrorPage.aspx)
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          const location = res.headers.location || '';
          return resolve({
            statusCode: res.statusCode,
            body: '',
            cookies,
            redirectedTo: location,
          });
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('POST request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Extract ASP.NET hidden form fields from page HTML.
   */
  _extractViewState($) {
    return {
      __VIEWSTATE: $('input#__VIEWSTATE').val() || '',
      __VIEWSTATEGENERATOR: $('input#__VIEWSTATEGENERATOR').val() || '',
      __EVENTVALIDATION: $('input#__EVENTVALIDATION').val() || '',
    };
  }

  /**
   * Parse the search results GridView (gv_results) from the response HTML.
   *
   * Expected columns: License #, Name, City, State, Classification, Status, Expiration Date
   */
  _parseResults($) {
    const results = [];

    // The GridView has ID ctl00_ContentPlaceHolder2_gv_results
    $('table[id*="gv_results"] tr').each((i, row) => {
      if (i === 0) return; // Skip header row

      const cells = $(row).find('td');
      if (cells.length < 7) return;

      const licenseNumber = $(cells[0]).text().trim();
      const name = $(cells[1]).text().trim();
      const city = $(cells[2]).text().trim();
      const state = $(cells[3]).text().trim();
      const classification = $(cells[4]).text().trim();
      const status = $(cells[5]).text().trim();
      const expirationDate = $(cells[6]).text().trim();

      if (!licenseNumber && !name) return;

      results.push({
        licenseNumber,
        name,
        city,
        state,
        classification,
        status,
        expirationDate,
      });
    });

    return results;
  }

  /**
   * Parse a name into first/last or firm, using company indicators.
   */
  _parseName(name) {
    if (!name) return { first_name: '', last_name: '', firm_name: '' };

    const companyIndicators = [
      /\bllc\b/i, /\binc\.?\b/i, /\bcorp\.?\b/i, /\bco\.?\b/i,
      /\bconstruction\b/i, /\bbuilders?\b/i, /\broofing\b/i,
      /\bplumbing\b/i, /\belectric(al)?\b/i, /\bcontracting\b/i,
      /\bservices?\b/i, /\benterprises?\b/i, /\bgroup\b/i,
      /\bcompany\b/i, /\bpartners?\b/i, /\bpllc\b/i, /\bltd\.?\b/i,
      /\bindustries\b/i, /\bsolutions\b/i, /\bmechanical\b/i,
      /\bhvac\b/i, /\bheating\b/i, /\bcooling\b/i, /\bair\b/i,
      /\bd\/b\/a\b/i, /\bt\/a\b/i,
    ];

    const isCompany = companyIndicators.some(re => re.test(name));

    if (isCompany) {
      return { first_name: '', last_name: '', firm_name: name };
    }

    // Individual name
    const { firstName, lastName } = this.splitName(name);
    return { first_name: firstName, last_name: lastName, firm_name: '' };
  }

  /**
   * Main search generator.
   *
   * Strategy:
   *   1. Perform ASP.NET cookie detection handshake (GET → redirect → GET with cookie)
   *   2. Extract ViewState tokens from the form
   *   3. Check for reCAPTCHA on the form page
   *   4. If reCAPTCHA detected, yield _captcha signal
   *   5. If somehow bypassed in the future, POST search and parse GridView results
   *
   * Currently: Always yields _captcha: true because reCAPTCHA v2 is enforced server-side.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape('SC Contractor License Board — Contractor Search');
    log.info(`Source: ${SEARCH_URL_WITH_DIV}`);
    if (practiceCode) {
      const className = CLASSIFICATION_CODES[practiceCode] || practiceCode;
      log.info(`Classification filter: ${className} (${practiceCode})`);
    }

    // Step 1: Cookie detection handshake — GET the form page
    log.info('SC-GC: Performing ASP.NET cookie detection handshake...');
    let formResponse;
    try {
      await rateLimiter.wait();
      formResponse = await this._httpGetWithCookies(SEARCH_URL_WITH_DIV, rateLimiter);
    } catch (err) {
      log.error(`SC-GC: Failed to load search page: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    if (formResponse.statusCode !== 200) {
      log.error(`SC-GC: Search page returned HTTP ${formResponse.statusCode}`);
      yield { _captcha: true, city: 'all', reason: `HTTP ${formResponse.statusCode}` };
      return;
    }

    // Step 2: Check for reCAPTCHA
    if (this.detectCaptcha(formResponse.body)) {
      log.warn('SC-GC: reCAPTCHA v2 detected on search form — server-side enforcement');
      log.warn('SC-GC: The form requires a valid reCAPTCHA token to submit.');
      log.warn('SC-GC: Without a CAPTCHA solving service, this scraper cannot proceed.');
      log.info('SC-GC: reCAPTCHA site key: 6Lc2X-saAAAAAPC6HatgHFOd8rCxCl-2yPTh44PN');

      yield { _captcha: true, city: 'all', reason: 'reCAPTCHA v2 enforced server-side — POST without valid token redirects to ErrorPage.aspx' };
      return;
    }

    // If we reach here, reCAPTCHA was not detected (unlikely but handle gracefully)
    log.info('SC-GC: No reCAPTCHA detected — attempting search...');

    const $ = cheerio.load(formResponse.body);
    const viewState = this._extractViewState($);
    const cookies = formResponse.cookies;

    // Determine search parameters
    const cities = this.getCities(options);
    const classCode = practiceCode || '';

    const seenLicenses = new Set();
    let totalYielded = 0;
    let totalWithPhone = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching ${city}, SC — classification: ${classCode || 'All'}`);

      // Build form POST data
      const formData = {
        ...viewState,
        'ctl00$ContentPlaceHolder1$UserInputGen$txt_lastName': '',
        'ctl00$ContentPlaceHolder1$UserInputGen$txt_firstName': '',
        'ctl00$ContentPlaceHolder1$UserInputGen$txt_licNum': '',
        'ctl00$ContentPlaceHolder1$UserInputGen$txt_city': city,
        'ctl00$ContentPlaceHolder1$UserInputGen$ddl_type': classCode,
        'ctl00$ContentPlaceHolder1$btn_find': 'Find',
      };

      let searchResponse;
      try {
        await rateLimiter.wait();
        // Build the POST URL — use the same URL with query params as the form action
        const postUrl = `${SEARCH_URL}?div=69&AspxAutoDetectCookieSupport=1`;
        searchResponse = await this._httpPostWithCookies(postUrl, formData, rateLimiter, { ...cookies });
      } catch (err) {
        log.error(`SC-GC: Search POST failed for ${city}: ${err.message}`);
        continue;
      }

      // Check for redirect (reCAPTCHA failure)
      if (searchResponse.redirectedTo) {
        if (searchResponse.redirectedTo.includes('ErrorPage')) {
          log.warn('SC-GC: POST redirected to ErrorPage — reCAPTCHA token required');
          yield { _captcha: true, city, reason: 'reCAPTCHA v2 required — POST without token redirects to error page' };
          return;
        }
        log.warn(`SC-GC: POST redirected to ${searchResponse.redirectedTo}`);
        continue;
      }

      if (searchResponse.statusCode !== 200) {
        log.warn(`SC-GC: Search returned HTTP ${searchResponse.statusCode} for ${city}`);
        continue;
      }

      // Parse results
      const $results = cheerio.load(searchResponse.body);

      // Check result count
      const resultText = $results('#ctl00_ContentPlaceHolder2_lbl_results').text().trim();
      if (resultText) {
        log.info(`SC-GC: ${resultText}`);
      }

      const results = this._parseResults($results);

      if (results.length === 0) {
        log.info(`SC-GC: No results for ${city}`);
        // Update ViewState for next request
        const newVS = this._extractViewState($results);
        if (newVS.__VIEWSTATE) {
          viewState.__VIEWSTATE = newVS.__VIEWSTATE;
          viewState.__VIEWSTATEGENERATOR = newVS.__VIEWSTATEGENERATOR;
          viewState.__EVENTVALIDATION = newVS.__EVENTVALIDATION;
        }
        continue;
      }

      log.info(`SC-GC: ${results.length} results for ${city}`);

      for (const result of results) {
        // Dedup by license number
        if (result.licenseNumber && seenLicenses.has(result.licenseNumber)) continue;
        if (result.licenseNumber) seenLicenses.add(result.licenseNumber);

        // Skip inactive licenses
        if (result.status && result.status.toLowerCase() !== 'active') continue;

        // Parse name
        const parsed = this._parseName(result.name);

        const lead = {
          first_name: parsed.first_name,
          last_name: parsed.last_name,
          firm_name: parsed.firm_name,
          city: result.city || city,
          state: result.state || 'SC',
          phone: '',
          email: '',
          website: '',
          bar_number: result.licenseNumber || '',
          bar_status: result.status || 'Active',
          license_type: result.classification || '',
          expiration_date: result.expirationDate || '',
        };

        totalYielded++;
        yield this.transformResult(lead, practiceArea || result.classification || '');
      }

      // Update ViewState for next POST (ASP.NET requires fresh tokens)
      const newVS = this._extractViewState($results);
      if (newVS.__VIEWSTATE) {
        viewState.__VIEWSTATE = newVS.__VIEWSTATE;
        viewState.__VIEWSTATEGENERATOR = newVS.__VIEWSTATEGENERATOR;
        viewState.__EVENTVALIDATION = newVS.__EVENTVALIDATION;
      }

      // Check maxPages limit
      if (options.maxPages && ci + 1 >= options.maxPages) {
        log.info(`SC-GC: Reached maxPages limit (${options.maxPages})`);
        break;
      }
    }

    log.success(`SC-GC scrape complete: ${totalYielded} yielded (${totalWithPhone} with phone)`);
  }
}

module.exports = new ScGcScraper();
