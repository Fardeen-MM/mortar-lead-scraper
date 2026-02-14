/**
 * New Jersey Attorney Search Scraper
 *
 * Source: https://portalattysearch-cloud.njcourts.gov/
 * Method: HTTP GET with WAF (Incapsula) detection and graceful fallback
 *
 * NJ Courts uses a Pega-based application behind Incapsula WAF protection.
 * Automated access is frequently blocked. This scraper attempts to access the
 * search endpoint and gracefully handles Incapsula blocks by logging a warning
 * and yielding nothing when blocked.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NewJerseyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-jersey',
      stateCode: 'NJ',
      baseUrl: 'https://portalattysearch-cloud.njcourts.gov/prweb/PRServletPublicAuth/app/Attorney/-amRUHgepTwWWiiBQpI9_yQNuum4oN16*/!STANDARD',
      pageSize: 50,
      practiceAreaCodes: {},
      defaultCities: [
        'Newark', 'Jersey City', 'Trenton', 'Hackensack', 'Morristown',
        'Cherry Hill', 'New Brunswick', 'Woodbridge', 'Paterson', 'Camden',
        'Toms River', 'Princeton', 'Freehold', 'Somerville',
      ],
    });
  }

  /**
   * Not used — search() is fully overridden due to WAF-protected Pega application.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for NJ Pega application`);
  }

  /**
   * Not used — search() is fully overridden due to WAF-protected Pega application.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for NJ Pega application`);
  }

  /**
   * Not used — search() is fully overridden due to WAF-protected Pega application.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for NJ Pega application`);
  }

  /**
   * Detect Incapsula/Imperva WAF block in response body.
   * Incapsula blocks typically include specific markers in the HTML.
   * @param {string} body - Response body
   * @returns {boolean}
   */
  _isIncapsulaBlock(body) {
    if (!body) return false;
    const markers = [
      'Incapsula',
      'incap_ses',
      'visid_incap',
      '_Incapsula_Resource',
      'Request unsuccessful',
      'Access Denied',
      'You are being redirected',
      'robots check',
    ];
    const bodyLower = body.toLowerCase();
    return markers.some(marker => bodyLower.includes(marker.toLowerCase()));
  }

  /**
   * HTTP GET with headers tailored for the NJ Courts Pega application.
   * Sends browser-like headers to minimize WAF detection.
   */
  _httpGetNJ(url, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Upgrade-Insecure-Requests': '1',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'none',
          'Sec-Fetch-User': '?1',
          'Cache-Control': 'max-age=0',
        },
        timeout: 20000,
      };

      const req = https.get(url, options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          // Drain the response body to free the socket
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          return resolve(this._httpGetNJ(redirect, rateLimiter, redirectCount + 1));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Parse attorney listings from the NJ Courts Pega HTML response.
   * Pega applications render data in a dynamic grid; structure may vary.
   * @param {CheerioStatic} $ - Cheerio instance
   * @param {string} searchCity - The city being searched
   * @returns {object[]} Array of attorney objects
   */
  _parseAttorneyResults($, searchCity) {
    const attorneys = [];

    // Pega grids typically use table or repeating-layout elements.
    // Try common selectors for Pega-rendered attorney data.
    const selectors = [
      'table.gridTable tbody tr',
      '.repeating-layout tr',
      '[data-repeat] tr',
      'table tbody tr',
    ];

    let $rows = $();
    for (const sel of selectors) {
      $rows = $(sel);
      if ($rows.length > 0) break;
    }

    $rows.each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 2) return;

      // Attempt to extract data from cells — exact layout depends on the Pega config.
      // Common patterns: Name | Bar ID | Status | City | Phone | Email
      const cellTexts = [];
      cells.each((__, cell) => {
        cellTexts.push($(cell).text().trim());
      });

      // Skip header rows
      if (cellTexts[0] && /^(name|attorney)/i.test(cellTexts[0])) return;

      // Try to parse name from first cell
      let firstName = '';
      let lastName = '';
      const nameText = cellTexts[0] || '';
      if (nameText.includes(',')) {
        const parts = nameText.split(',');
        lastName = parts[0].trim();
        firstName = (parts[1] || '').trim().split(/\s+/)[0] || '';
      } else {
        const split = this.splitName(nameText);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      if (!firstName && !lastName) return;

      // Extract other fields by position (best-effort)
      const attorney = {
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: searchCity,
        state: 'NJ',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        admission_date: '',
        bar_status: '',
        source: `${this.name}_bar`,
      };

      // Scan remaining cells for recognizable patterns
      for (let i = 1; i < cellTexts.length; i++) {
        const text = cellTexts[i];
        if (!text) continue;

        // Bar number: typically numeric
        if (/^\d{4,}$/.test(text) && !attorney.bar_number) {
          attorney.bar_number = text;
          continue;
        }

        // Status: common values
        if (/^(active|inactive|retired|suspended|eligible)/i.test(text) && !attorney.bar_status) {
          attorney.bar_status = text;
          continue;
        }

        // Phone: matches phone pattern
        if (/\(\d{3}\)\s*\d{3}[.-]\d{4}/.test(text) || /^\d{3}[.-]\d{3}[.-]\d{4}$/.test(text)) {
          if (!attorney.phone) attorney.phone = text;
          continue;
        }

        // Email: contains @
        if (text.includes('@') && !attorney.email) {
          attorney.email = text;
          continue;
        }

        // Date: matches date pattern
        if (/\d{1,2}\/\d{1,2}\/\d{2,4}/.test(text) || /\d{4}-\d{2}-\d{2}/.test(text)) {
          if (!attorney.admission_date) attorney.admission_date = text;
          continue;
        }
      }

      // Also look for links containing email
      $(row).find('a[href^="mailto:"]').each((__, el) => {
        if (!attorney.email) {
          attorney.email = ($(el).attr('href') || '').replace('mailto:', '').trim();
        }
      });

      attorneys.push(attorney);
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the NJ Courts search.
   * Overrides BaseScraper.search() to handle WAF protection gracefully.
   *
   * Due to Incapsula WAF on the NJ Courts portal, this scraper has limited
   * functionality. When blocked, it logs a warning and moves to the next city.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`New Jersey attorney search does not support practice area filtering — searching all attorneys`);
    }

    log.info(`NJ Courts portal is protected by Incapsula WAF — automated access may be blocked`);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Build the search URL with city parameter
      const searchUrl = `${this.baseUrl}?AppName=AttorneySearch&City=${encodeURIComponent(city)}`;
      log.info(`Fetching — ${searchUrl}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._httpGetNJ(searchUrl, rateLimiter);
      } catch (err) {
        log.error(`Request failed for ${city}: ${err.message}`);
        // Network-level block is common with Incapsula
        if (err.message.includes('timed out') || err.message.includes('ECONNRESET')) {
          log.warn(`NJ Courts appears to be blocking automated access — connection failed for ${city}`);
          continue;
        }
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) {
          try {
            await rateLimiter.wait();
            response = await this._httpGetNJ(searchUrl, rateLimiter);
          } catch (retryErr) {
            log.error(`Retry failed for ${city}: ${retryErr.message}`);
            continue;
          }
        } else {
          continue;
        }
      }

      // Handle HTTP-level blocks
      if (response.statusCode === 403) {
        if (this._isIncapsulaBlock(response.body)) {
          log.warn(`Incapsula WAF blocked access for ${city} — NJ Courts requires manual browser access`);
          log.info(`To search NJ attorneys manually, visit: https://portalattysearch-cloud.njcourts.gov/`);
          // Don't retry Incapsula blocks — they require browser JS execution
          continue;
        }
        log.warn(`Got 403 from ${this.name} for ${city}`);
        const shouldRetry = await rateLimiter.handleBlock(403);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode === 429) {
        log.warn(`Got 429 from ${this.name} for ${city}`);
        const shouldRetry = await rateLimiter.handleBlock(429);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} for ${city} — skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      // Check for Incapsula block in 200 response (some WAFs return 200 with challenge page)
      if (this._isIncapsulaBlock(response.body)) {
        log.warn(`Incapsula challenge page detected for ${city} — NJ Courts requires JavaScript execution`);
        log.info(`To search NJ attorneys manually, visit: https://portalattysearch-cloud.njcourts.gov/`);
        continue;
      }

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA detected for ${city} — skipping`);
        yield { _captcha: true, city };
        continue;
      }

      // Parse the response with Cheerio
      const $ = cheerio.load(response.body);
      const attorneys = this._parseAttorneyResults($, city);

      if (attorneys.length === 0) {
        log.info(`No parseable results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      log.success(`Found ${attorneys.length} results for ${city}`);

      // Filter and yield
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

module.exports = new NewJerseyScraper();
