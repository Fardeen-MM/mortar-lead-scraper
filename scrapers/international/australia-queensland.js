/**
 * Queensland Law Society (QLS) — Find a Solicitor Scraper
 *
 * Source: https://www.youandthelaw.com.au/directory
 * Method: HTTP POST form submission + Cheerio (ASP.NET MVC server-rendered HTML)
 *
 * The Queensland Law Society operates "You & The Law" (youandthelaw.com.au),
 * a public-facing directory of ~4,000+ QLS member solicitors. The "Find a
 * Solicitor" tool allows searching by:
 *   - Type: "individual" (solicitor) or "organisation" (law firm)
 *   - Area of practice (GUID-based dropdown values)
 *   - Location (suburb/postcode with lat/lng from Google Places)
 *   - Distance radius (0, 10, 20, 30, 50, 100 km)
 *   - Name (keyword search)
 *   - Language spoken
 *
 * Strategy:
 *   1. GET /directory to obtain the __RequestVerificationToken (CSRF) and cookies.
 *   2. POST /directory/search with form data to initiate a search session.
 *      The server stores search state in the session cookie.
 *   3. Parse the HTML results page (10 solicitor cards per page).
 *   4. Paginate via GET /directory?searched=True&Page={n} using the session cookie.
 *   5. Each card contains: name, firm, email, phone, address, practice areas.
 *   6. Individual profile pages at /individual/{id}/{slug} contain the same data.
 *
 * Pagination: 10 results per page, session-based (cookies required).
 * Total solicitors: ~4,000+ across Queensland.
 *
 * City-to-coordinates mapping is required because the search uses lat/lng
 * for proximity-based results. We use a 50km radius for each city to capture
 * surrounding suburban solicitors.
 *
 * Technical stack: ASP.NET MVC 5 on IIS, Kentico CMS, Azure hosting.
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const https = require('https');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

// City-to-coordinates mapping for Queensland cities.
// The QLS directory requires lat/lng for proximity-based search.
const CITY_COORDS = {
  'Brisbane':        { lat: -27.4698, lng: 153.0251 },
  'Gold Coast':      { lat: -28.0167, lng: 153.4000 },
  'Sunshine Coast':  { lat: -26.6500, lng: 153.0667 },
  'Townsville':      { lat: -19.2590, lng: 146.8169 },
  'Cairns':          { lat: -16.9186, lng: 145.7781 },
  'Toowoomba':       { lat: -27.5598, lng: 151.9507 },
  'Mackay':          { lat: -21.1411, lng: 149.1860 },
  'Rockhampton':     { lat: -23.3791, lng: 150.5100 },
  'Bundaberg':       { lat: -24.8661, lng: 152.3489 },
  'Hervey Bay':      { lat: -25.2882, lng: 152.8531 },
  'Gladstone':       { lat: -23.8490, lng: 151.2660 },
  'Ipswich':         { lat: -27.6147, lng: 152.7609 },
  'Logan':           { lat: -27.6389, lng: 153.1092 },
  'Redlands':        { lat: -27.5330, lng: 153.2460 },
  'Caboolture':      { lat: -27.0847, lng: 152.9511 },
  'Mount Isa':       { lat: -20.7256, lng: 139.4927 },
};

// Practice area GUID mapping (from the <select> dropdown on the search form).
const PRACTICE_AREA_GUIDS = {
  'administrative law':                      'b206a6d4-2070-eb11-b1ab-000d3a79964c',
  'agribusiness':                            'b806a6d4-2070-eb11-b1ab-000d3a79964c',
  'agribusiness and primary production':     'b806a6d4-2070-eb11-b1ab-000d3a79964c',
  'banking':                                 'c206a6d4-2070-eb11-b1ab-000d3a79964c',
  'banking and finance':                     'c206a6d4-2070-eb11-b1ab-000d3a79964c',
  'bankruptcy':                              'c406a6d4-2070-eb11-b1ab-000d3a79964c',
  'bankruptcy and insolvency':               'c406a6d4-2070-eb11-b1ab-000d3a79964c',
  'insolvency':                              'c406a6d4-2070-eb11-b1ab-000d3a79964c',
  'building':                                'c606a6d4-2070-eb11-b1ab-000d3a79964c',
  'building and construction law':           'c606a6d4-2070-eb11-b1ab-000d3a79964c',
  'construction':                            'c606a6d4-2070-eb11-b1ab-000d3a79964c',
  'business and commercial law':             'c806a6d4-2070-eb11-b1ab-000d3a79964c',
  'commercial':                              'c806a6d4-2070-eb11-b1ab-000d3a79964c',
  'business law':                            'ca06a6d4-2070-eb11-b1ab-000d3a79964c',
  'civil law':                               'ce06a6d4-2070-eb11-b1ab-000d3a79964c',
  'civil litigation':                        'd006a6d4-2070-eb11-b1ab-000d3a79964c',
  'commercial litigation':                   'b685893b-3582-ec11-8d21-002248929242',
  'competition and consumer':                '51ef872c-e2d3-eb11-bacc-0022481809a8',
  'competition and consumer law':            'dc06a6d4-2070-eb11-b1ab-000d3a79964c',
  'consumer':                                'dc06a6d4-2070-eb11-b1ab-000d3a79964c',
  'corporations law':                        'e606a6d4-2070-eb11-b1ab-000d3a79964c',
  'corporate':                               'e606a6d4-2070-eb11-b1ab-000d3a79964c',
  'criminal':                                '052a9b79-3582-ec11-8d21-002248929242',
  'criminal law':                            '052a9b79-3582-ec11-8d21-002248929242',
  'elder law':                               'f806a6d4-2070-eb11-b1ab-000d3a79964c',
  'energy':                                  'fe06a6d4-2070-eb11-b1ab-000d3a79964c',
  'family - domestic violence':              '85ef872c-e2d3-eb11-bacc-0022481809a8',
  'domestic violence':                       '85ef872c-e2d3-eb11-bacc-0022481809a8',
  'family':                                  '961ce0df-3582-ec11-8d21-002248929242',
  'family law':                              '961ce0df-3582-ec11-8d21-002248929242',
  'franchising':                             '0a07a6d4-2070-eb11-b1ab-000d3a79964c',
  'franchising law':                         '0a07a6d4-2070-eb11-b1ab-000d3a79964c',
  'government':                              '1207a6d4-2070-eb11-b1ab-000d3a79964c',
  'immigration':                             '7ed00eda-3882-ec11-8d21-002248929242',
  'immigration law':                         '7ed00eda-3882-ec11-8d21-002248929242',
  'intellectual property':                   '2c07a6d4-2070-eb11-b1ab-000d3a79964c',
  'ip':                                      '2c07a6d4-2070-eb11-b1ab-000d3a79964c',
  'maritime':                                'bfef872c-e2d3-eb11-bacc-0022481809a8',
  'maritime and fisheries':                  'bfef872c-e2d3-eb11-bacc-0022481809a8',
  'migration':                               '3e07a6d4-2070-eb11-b1ab-000d3a79964c',
  'migration law':                           '3e07a6d4-2070-eb11-b1ab-000d3a79964c',
  'mining':                                  '4007a6d4-2070-eb11-b1ab-000d3a79964c',
  'mining and resources law':                '4007a6d4-2070-eb11-b1ab-000d3a79964c',
  'resources':                               '4007a6d4-2070-eb11-b1ab-000d3a79964c',
  'native title':                            '4607a6d4-2070-eb11-b1ab-000d3a79964c',
  'personal injury':                         '1cd108a4-3582-ec11-8d21-002248929242',
  'personal injuries law':                   '1cd108a4-3582-ec11-8d21-002248929242',
  'planning':                                '5807a6d4-2070-eb11-b1ab-000d3a79964c',
  'planning and environment law':            '5807a6d4-2070-eb11-b1ab-000d3a79964c',
  'environment':                             '5807a6d4-2070-eb11-b1ab-000d3a79964c',
  'professional misconduct':                 'fdef872c-e2d3-eb11-bacc-0022481809a8',
  'property - commercial':                   'f9ef872c-e2d3-eb11-bacc-0022481809a8',
  'property - residential':                  'fbef872c-e2d3-eb11-bacc-0022481809a8',
  'property':                                '6807a6d4-2070-eb11-b1ab-000d3a79964c',
  'property law':                            '6807a6d4-2070-eb11-b1ab-000d3a79964c',
  'succession':                              'a3a0440c-3882-ec11-8d21-002248929242',
  'succession law':                          'a3a0440c-3882-ec11-8d21-002248929242',
  'wills':                                   'a3a0440c-3882-ec11-8d21-002248929242',
  'estates':                                 'a3a0440c-3882-ec11-8d21-002248929242',
  'superannuation':                          '8607a6d4-2070-eb11-b1ab-000d3a79964c',
  'superannuation and trusts':               '8607a6d4-2070-eb11-b1ab-000d3a79964c',
  'trusts':                                  '8607a6d4-2070-eb11-b1ab-000d3a79964c',
  'taxation':                                '8a07a6d4-2070-eb11-b1ab-000d3a79964c',
  'taxation and state revenue':              '8a07a6d4-2070-eb11-b1ab-000d3a79964c',
  'tax':                                     '8c07a6d4-2070-eb11-b1ab-000d3a79964c',
  'taxation law':                            '8c07a6d4-2070-eb11-b1ab-000d3a79964c',
  'transport':                               '9807a6d4-2070-eb11-b1ab-000d3a79964c',
  'transport law':                           '9807a6d4-2070-eb11-b1ab-000d3a79964c',
  'workplace relations':                     'a807a6d4-2070-eb11-b1ab-000d3a79964c',
  'workplace relations law':                 'a807a6d4-2070-eb11-b1ab-000d3a79964c',
  'employment':                              'aa07a6d4-2070-eb11-b1ab-000d3a79964c',
  'employment law':                          'aa07a6d4-2070-eb11-b1ab-000d3a79964c',
  'workplace relations, employment and safety': 'aa07a6d4-2070-eb11-b1ab-000d3a79964c',
};

class QueenslandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-queensland',
      stateCode: 'AU-QLD',
      baseUrl: 'https://www.youandthelaw.com.au',
      pageSize: 10, // The directory returns 10 results per page
      practiceAreaCodes: PRACTICE_AREA_GUIDS,
      defaultCities: ['Brisbane', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns', 'Toowoomba'],
      maxConsecutiveEmpty: 2,
    });

    this.directoryUrl = `${this.baseUrl}/directory`;
    this.searchUrl = `${this.baseUrl}/directory/search`;
    this.defaultRadius = 50; // km radius for location-based search
  }

  // --- BaseScraper overrides (required but not used since search() is overridden) ---

  buildSearchUrl({ page }) {
    const params = new URLSearchParams();
    params.set('searched', 'True');
    if (page && page > 1) {
      params.set('Page', String(page));
    }
    return `${this.directoryUrl}?${params.toString()}`;
  }

  parseResultsPage() { return []; }
  extractResultCount() { return 0; }

  // --- HTTP helpers ---

  /**
   * Perform an HTTP GET request with cookie support.
   *
   * @param {string} url - The URL to fetch
   * @param {object} rateLimiter - RateLimiter instance
   * @param {string} cookieHeader - Cookie header string
   * @returns {Promise<{statusCode: number, body: string, cookies: string[]}>}
   */
  httpGetWithCookies(url, rateLimiter, cookieHeader) {
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
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      if (cookieHeader) {
        options.headers['Cookie'] = cookieHeader;
      }

      const req = https.get(options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          // Merge cookies from redirect
          const newCookies = this._extractCookies(res.headers['set-cookie'] || []);
          const mergedCookie = this._mergeCookies(cookieHeader, newCookies);
          return resolve(this.httpGetWithCookies(redirect, rateLimiter, mergedCookie));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          const setCookies = res.headers['set-cookie'] || [];
          resolve({
            statusCode: res.statusCode,
            body: data,
            cookies: setCookies,
          });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Perform an HTTP POST request with form data and cookie support.
   *
   * @param {string} url - The URL to POST to
   * @param {string} formBody - URL-encoded form body
   * @param {object} rateLimiter - RateLimiter instance
   * @param {string} cookieHeader - Cookie header string
   * @returns {Promise<{statusCode: number, body: string, cookies: string[], location: string}>}
   */
  httpPostForm(url, formBody, rateLimiter, cookieHeader) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
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
          'Content-Length': Buffer.byteLength(formBody),
          'Origin': this.baseUrl,
          'Referer': `${this.directoryUrl}`,
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

      if (cookieHeader) {
        options.headers['Cookie'] = cookieHeader;
      }

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          const setCookies = res.headers['set-cookie'] || [];
          const location = res.headers['location'] || '';
          resolve({
            statusCode: res.statusCode,
            body: data,
            cookies: setCookies,
            location: location,
          });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(formBody);
      req.end();
    });
  }

  /**
   * Extract cookie name=value pairs from Set-Cookie headers.
   */
  _extractCookies(setCookieHeaders) {
    const cookies = {};
    for (const header of setCookieHeaders) {
      const nameValue = header.split(';')[0];
      const eqIdx = nameValue.indexOf('=');
      if (eqIdx > 0) {
        const name = nameValue.substring(0, eqIdx).trim();
        const value = nameValue.substring(eqIdx + 1).trim();
        cookies[name] = value;
      }
    }
    return cookies;
  }

  /**
   * Merge existing cookie header with new cookies from Set-Cookie headers.
   */
  _mergeCookies(existingHeader, newCookies) {
    const cookies = {};

    // Parse existing cookie header
    if (existingHeader) {
      for (const pair of existingHeader.split(';')) {
        const eqIdx = pair.indexOf('=');
        if (eqIdx > 0) {
          const name = pair.substring(0, eqIdx).trim();
          const value = pair.substring(eqIdx + 1).trim();
          cookies[name] = value;
        }
      }
    }

    // Merge new cookies (overwrite existing)
    Object.assign(cookies, newCookies);

    // Build cookie header string
    return Object.entries(cookies)
      .map(([name, value]) => `${name}=${value}`)
      .join('; ');
  }

  /**
   * Build a cookie header string from Set-Cookie array.
   */
  _buildCookieHeader(setCookieHeaders) {
    const cookies = this._extractCookies(setCookieHeaders);
    return Object.entries(cookies)
      .map(([name, value]) => `${name}=${value}`)
      .join('; ');
  }

  // --- HTML parsing helpers ---

  /**
   * Extract the __RequestVerificationToken from the directory page HTML.
   */
  _extractCsrfToken(html) {
    const match = html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/);
    return match ? match[1] : null;
  }

  /**
   * Extract total result count from the results page.
   * Looks for: <h2 class="section-title ...">1241 Results</h2>
   */
  _extractTotalResults(html) {
    const $ = cheerio.load(html);
    const titleText = $('h2.section-title').text().trim();
    const countMatch = titleText.match(/([\d,]+)\s+Results?/i);
    if (countMatch) {
      return parseInt(countMatch[1].replace(/,/g, ''), 10);
    }
    return 0;
  }

  /**
   * Parse solicitor cards from the search results HTML.
   *
   * Each card has the structure:
   *   <div class="col-12 col-md-6 card" data-map-card>
   *     <div class="inner">
   *       <div class="top">
   *         <div class="card-detail">
   *           <div class="heading-row">
   *             <a href="/individual/{id}/{slug}"><h3 class="title">Name</h3></a>
   *           </div>
   *           <div class="sub-title">Firm Name</div>
   *           <div class="contact-info">
   *             <div class="email"><a href="mailto:...">email</a></div>
   *             <div class="phone"><a href="tel:...">phone</a></div>
   *             <div class="address"><span data-map-address ...>address</span></div>
   *           </div>
   *         </div>
   *       </div>
   *       <div class="bottom">
   *         <div class="area-of-practice">
   *           <div class="practice-list">Practice Area 1 | Practice Area 2</div>
   *         </div>
   *       </div>
   *     </div>
   *   </div>
   *
   * @param {string} html - HTML of the results page
   * @returns {object[]} Array of solicitor records
   */
  _parseResultCards(html) {
    const $ = cheerio.load(html);
    const attorneys = [];

    $('div.card').each((_, el) => {
      const $card = $(el);

      // Skip cards that are not solicitor cards (e.g. navigation cards)
      const $inner = $card.find('.inner');
      if (!$inner.length) return;

      // --- Name and profile URL ---
      const $nameLink = $card.find('.heading-row a[href*="/individual/"]');
      if (!$nameLink.length) return; // Skip non-solicitor cards

      const fullName = $nameLink.find('h3.title').text().trim() ||
                       $nameLink.text().trim();
      if (!fullName) return;

      const profilePath = $nameLink.attr('href') || '';
      const profileUrl = profilePath.startsWith('/')
        ? `${this.baseUrl}${profilePath}`
        : profilePath;

      // Extract solicitor ID from profile URL (/individual/{id}/{slug})
      const idMatch = profilePath.match(/\/individual\/(\d+)\//);
      const solicitorId = idMatch ? idMatch[1] : '';

      // --- Firm name ---
      const firmName = $card.find('.sub-title').first().text().trim();

      // --- Email ---
      let email = '';
      const $emailLink = $card.find('.email a[href^="mailto:"]');
      if ($emailLink.length) {
        email = $emailLink.attr('href').replace('mailto:', '').trim();
      }

      // --- Phone ---
      let phone = '';
      const $phoneLink = $card.find('.phone a[href^="tel:"]');
      if ($phoneLink.length) {
        phone = $phoneLink.text().trim();
      }

      // --- Address ---
      let address = '';
      let suburb = '';
      let postcode = '';
      const $addressSpan = $card.find('.address span[data-map-address]');
      if ($addressSpan.length) {
        address = $addressSpan.text().replace(/\s+/g, ' ').trim();
        // Parse postcode from address (e.g., "28 Nicholas Street, Ipswich, Queensland, Australia, 4305")
        const postcodeMatch = address.match(/,\s*(\d{4})\s*$/);
        if (postcodeMatch) {
          postcode = postcodeMatch[1];
        }
        // Extract suburb from address
        const parts = address.split(',').map(p => p.trim());
        if (parts.length >= 3) {
          suburb = parts[1]; // Usually: Street, Suburb, State, Country, Postcode
        }
      }

      // --- Practice areas ---
      let practiceAreas = '';
      const $practiceList = $card.find('.practice-list');
      if ($practiceList.length) {
        practiceAreas = $practiceList.text().replace(/\s+/g, ' ').trim();
      }

      // --- Parse name ---
      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: suburb,
        state: 'AU-QLD',
        zip: postcode,
        country: 'Australia',
        phone: phone,
        email: email,
        website: '',
        bar_number: solicitorId,
        bar_status: '',
        practice_areas: practiceAreas,
        address: address,
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  // --- Practice area resolution override ---

  /**
   * Override practice code resolution to use GUID mapping.
   */
  resolvePracticeCode(practiceArea) {
    if (!practiceArea) return null;
    const key = practiceArea.toLowerCase().trim();
    if (PRACTICE_AREA_GUIDS[key]) return PRACTICE_AREA_GUIDS[key];

    // Partial match
    for (const [name, code] of Object.entries(PRACTICE_AREA_GUIDS)) {
      if (name.includes(key) || key.includes(name)) return code;
    }

    // If it looks like a GUID already, pass through
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(practiceArea)) {
      return practiceArea;
    }

    return null;
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields solicitor records from the QLS directory.
   *
   * Strategy:
   * 1. For each city, establish a search session:
   *    a. GET /directory to obtain CSRF token and cookies
   *    b. POST /directory/search with form data
   * 2. Parse the initial results page
   * 3. Paginate via GET /directory?searched=True&Page={n} using the session
   * 4. Each page contains up to 10 solicitor cards
   * 5. Yield standardised attorney records
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`AU-QLD: Unknown practice area "${practiceArea}" -- searching without filter`);
      log.info(`AU-QLD: Available areas: ${Object.keys(this.practiceAreaCodes).slice(0, 20).join(', ')}, ...`);
    }

    const cities = this.getCities(options);
    const seenIds = new Set();

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`AU-QLD: Searching ${practiceArea || 'all'} solicitors near ${city}`);

      // Get coordinates for this city
      const coords = CITY_COORDS[city];
      if (!coords) {
        log.warn(`AU-QLD: No coordinates for "${city}" -- skipping`);
        continue;
      }

      // Phase 1: Get the directory page to obtain CSRF token and session cookies
      let csrfToken;
      let cookieHeader;

      try {
        await rateLimiter.wait();
        const dirResp = await this.httpGetWithCookies(this.directoryUrl, rateLimiter, '');

        if (dirResp.statusCode !== 200) {
          log.error(`AU-QLD: Directory page returned ${dirResp.statusCode} -- skipping ${city}`);
          continue;
        }

        csrfToken = this._extractCsrfToken(dirResp.body);
        if (!csrfToken) {
          log.error(`AU-QLD: Could not extract CSRF token -- skipping ${city}`);
          continue;
        }

        cookieHeader = this._buildCookieHeader(dirResp.cookies);
      } catch (err) {
        log.error(`AU-QLD: Failed to load directory page: ${err.message}`);
        continue;
      }

      // Phase 2: POST the search form to establish a search session
      const formParams = new URLSearchParams();
      formParams.set('__RequestVerificationToken', csrfToken);
      formParams.set('Type', 'individual');
      formParams.set('AreaOfPractice', practiceCode || '00000000-0000-0000-0000-000000000000');
      formParams.set('Location', city);
      formParams.set('Latitude', String(coords.lat));
      formParams.set('Longitude', String(coords.lng));
      formParams.set('Name', '');
      formParams.set('MaxDist', String(this.defaultRadius));
      formParams.set('LanguageSpoken', '');

      let searchResultHtml;

      try {
        await rateLimiter.wait();
        const searchResp = await this.httpPostForm(
          this.searchUrl,
          formParams.toString(),
          rateLimiter,
          cookieHeader,
        );

        // The POST may redirect (302) to /directory?searched=True
        if (searchResp.statusCode >= 300 && searchResp.statusCode < 400) {
          // Merge cookies from the redirect
          const newCookies = this._extractCookies(searchResp.cookies);
          cookieHeader = this._mergeCookies(cookieHeader, newCookies);

          let redirectUrl = searchResp.location;
          if (redirectUrl.startsWith('/')) {
            redirectUrl = `${this.baseUrl}${redirectUrl}`;
          }

          await sleep(1000);
          const redirectResp = await this.httpGetWithCookies(redirectUrl, rateLimiter, cookieHeader);

          if (redirectResp.statusCode !== 200) {
            log.error(`AU-QLD: Search redirect returned ${redirectResp.statusCode} for ${city}`);
            continue;
          }

          const newCookies2 = this._extractCookies(redirectResp.cookies);
          cookieHeader = this._mergeCookies(cookieHeader, newCookies2);
          searchResultHtml = redirectResp.body;
        } else if (searchResp.statusCode === 200) {
          // Some requests return results directly (no redirect)
          const newCookies = this._extractCookies(searchResp.cookies);
          cookieHeader = this._mergeCookies(cookieHeader, newCookies);
          searchResultHtml = searchResp.body;
        } else {
          log.error(`AU-QLD: Search returned ${searchResp.statusCode} for ${city}`);
          continue;
        }
      } catch (err) {
        log.error(`AU-QLD: Search failed for ${city}: ${err.message}`);
        continue;
      }

      // Phase 3: Parse results and paginate
      if (!searchResultHtml) {
        log.warn(`AU-QLD: No results HTML for ${city}`);
        continue;
      }

      const totalResults = this._extractTotalResults(searchResultHtml);
      if (totalResults === 0) {
        log.info(`AU-QLD: No results for ${practiceArea || 'all'} near ${city}`);
        continue;
      }

      const totalPages = Math.ceil(totalResults / this.pageSize);
      log.success(`AU-QLD: Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let currentHtml = searchResultHtml;

      while (true) {
        // Check max pages limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`AU-QLD: Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Check for CAPTCHA
        if (this.detectCaptcha(currentHtml)) {
          log.warn(`AU-QLD: CAPTCHA detected on page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        // Parse solicitor cards
        const attorneys = this._parseResultCards(currentHtml);
        log.info(`AU-QLD: Page ${page} -- ${attorneys.length} solicitors`);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`AU-QLD: ${this.maxConsecutiveEmpty} consecutive empty pages -- stopping for ${city}`);
            break;
          }
        } else {
          consecutiveEmpty = 0;

          // Yield results (deduplicate across cities by solicitor ID)
          for (const attorney of attorneys) {
            const solId = attorney.bar_number;
            if (solId && seenIds.has(solId)) continue;
            if (solId) seenIds.add(solId);

            yield this.transformResult(attorney, practiceArea);
          }
        }

        // Check if we've reached the last page
        if (page >= totalPages) {
          log.success(`AU-QLD: Completed all ${totalPages} pages for ${city}`);
          break;
        }

        // Fetch next page
        page++;
        pagesFetched++;

        const nextUrl = this.buildSearchUrl({ page });
        log.info(`AU-QLD: Page ${page} -- GET ${nextUrl}`);

        try {
          await rateLimiter.wait();
          const pageResp = await this.httpGetWithCookies(nextUrl, rateLimiter, cookieHeader);

          if (pageResp.statusCode === 429 || pageResp.statusCode === 403) {
            log.warn(`AU-QLD: Got ${pageResp.statusCode} on page ${page}`);
            const shouldRetry = await rateLimiter.handleBlock(pageResp.statusCode);
            if (!shouldRetry) break;
            page--; // Retry same page
            pagesFetched--;
            continue;
          }

          if (pageResp.statusCode !== 200) {
            log.error(`AU-QLD: Page ${page} returned ${pageResp.statusCode} -- stopping for ${city}`);
            break;
          }

          // Update cookies if new ones are set
          const newCookies = this._extractCookies(pageResp.cookies);
          cookieHeader = this._mergeCookies(cookieHeader, newCookies);
          currentHtml = pageResp.body;
        } catch (err) {
          log.error(`AU-QLD: Failed to fetch page ${page} for ${city}: ${err.message}`);
          break;
        }
      }
    }
  }
}

module.exports = new QueenslandScraper();
