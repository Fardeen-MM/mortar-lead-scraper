/**
 * BaseScraper — shared scraper logic for all state bar directories
 *
 * Provides: HTTP fetching, rate limiting, pagination engine, CAPTCHA detection,
 * Cloudflare email decoding, name splitting, practice area resolution.
 *
 * Subclasses MUST override:
 *   buildSearchUrl({ city, practiceCode, page }) → string
 *   parseResultsPage($) → object[]
 *   extractResultCount($) → number
 *
 * Subclasses CAN override:
 *   getCities(options) → string[]
 *   transformResult(attorney, practiceArea) → object
 *   shouldTerminatePagination(page, totalPages, pagesFetched, options) → boolean
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const { log } = require('../lib/logger');
const { RateLimiter } = require('../lib/rate-limiter');

class BaseScraper {
  /**
   * @param {object} config
   * @param {string} config.name             - Scraper identifier (e.g., 'florida')
   * @param {string} config.stateCode        - 2-letter state code (e.g., 'FL')
   * @param {string} config.baseUrl          - Base URL of the bar directory
   * @param {number} [config.pageSize=50]    - Results per page
   * @param {object} config.practiceAreaCodes - Map of friendly name → state bar code
   * @param {string[]} config.defaultCities  - Major cities for this state
   * @param {number} [config.maxConsecutiveEmpty=2] - Empty pages before stopping
   */
  constructor(config) {
    this.name = config.name;
    this.stateCode = config.stateCode;
    this.baseUrl = config.baseUrl;
    this.pageSize = config.pageSize || 50;
    this.practiceAreaCodes = config.practiceAreaCodes || {};
    this.defaultCities = config.defaultCities || [];
    this.maxConsecutiveEmpty = config.maxConsecutiveEmpty || 2;
  }

  // --- Backward-compatible property aliases ---
  get PRACTICE_AREA_CODES() { return this.practiceAreaCodes; }
  get DEFAULT_CITIES() { return this.defaultCities; }

  // --- Shared utilities ---

  /**
   * Decode Cloudflare email protection.
   * First 2 hex chars = XOR key, remaining pairs = email chars XORed with key.
   */
  decodeCloudflareEmail(encoded) {
    if (!encoded) return '';
    const hex = encoded.replace(/.*#/, '');
    if (hex.length < 4) return '';
    const key = parseInt(hex.substring(0, 2), 16);
    let email = '';
    for (let i = 2; i < hex.length; i += 2) {
      const charCode = parseInt(hex.substring(i, i + 2), 16) ^ key;
      email += String.fromCharCode(charCode);
    }
    return email;
  }

  /**
   * Decode HTML entities.
   */
  decodeEntities(str) {
    if (!str) return '';
    return str
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&#x27;/g, "'")
      .replace(/&apos;/g, "'")
      .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)));
  }

  /**
   * Split a full name into first/last.
   */
  splitName(fullName) {
    const parts = (fullName || '').split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      return { firstName: parts[0], lastName: parts[parts.length - 1] };
    }
    if (parts.length === 1) {
      return { firstName: '', lastName: parts[0] };
    }
    return { firstName: '', lastName: '' };
  }

  /**
   * Parse "City, ST ZIP" pattern into components.
   */
  parseCityStateZip(text) {
    if (!text) return { city: '', state: '', zip: '' };
    const full = text.match(/^(.+),\s*([A-Z]{2})\s+([\d-]+)$/);
    if (full) return { city: full[1].trim(), state: full[2], zip: full[3] };
    const partial = text.match(/^(.+),\s*([A-Z]{2})/);
    if (partial) return { city: partial[1].trim(), state: partial[2], zip: '' };
    return { city: '', state: '', zip: '' };
  }

  /**
   * Check if response body contains CAPTCHA indicators.
   */
  detectCaptcha(body) {
    return body.includes('captcha') || body.includes('CAPTCHA') ||
           body.includes('challenge-form');
  }

  /**
   * HTTP GET with user agent rotation and redirect following.
   */
  httpGet(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 15000,
      };

      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  // --- Practice area resolution ---

  /**
   * Resolve a user-friendly practice area to the state's internal code.
   */
  resolvePracticeCode(practiceArea) {
    if (!practiceArea) return null;
    const key = practiceArea.toLowerCase().trim();
    // Direct match
    if (this.practiceAreaCodes[key]) return this.practiceAreaCodes[key];
    // Partial match
    for (const [name, code] of Object.entries(this.practiceAreaCodes)) {
      if (name.includes(key) || key.includes(name)) return code;
    }
    // Pass through if it looks like a code already
    if (/^[A-Z0-9]+$/.test(practiceArea) && practiceArea.length <= 5) return practiceArea;
    return null;
  }

  // --- Override points ---

  /**
   * Build the search URL for a specific city/page/practice code.
   * MUST be overridden by subclasses.
   */
  buildSearchUrl(/* { city, practiceCode, page } */) {
    throw new Error(`${this.name}: buildSearchUrl() must be overridden`);
  }

  /**
   * Parse a search results page with Cheerio.
   * MUST return an array of attorney objects.
   * MUST be overridden by subclasses.
   */
  parseResultsPage(/* $ */) {
    throw new Error(`${this.name}: parseResultsPage() must be overridden`);
  }

  /**
   * Extract total result count from the page.
   * MUST be overridden by subclasses.
   */
  extractResultCount(/* $ */) {
    throw new Error(`${this.name}: extractResultCount() must be overridden`);
  }

  /**
   * Get the list of cities to search. Override for county/zip-based states.
   */
  getCities(options) {
    return options.city ? [options.city] : this.defaultCities;
  }

  /**
   * Post-process each attorney record before yielding.
   */
  transformResult(attorney, practiceArea) {
    attorney.source = `${this.name}_bar`;
    attorney.practice_area = practiceArea || '';
    return attorney;
  }

  // --- Core pagination engine ---

  /**
   * Async generator that yields attorney records.
   * Handles city iteration, pagination, rate limiting, CAPTCHA detection.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this.buildSearchUrl({ city, practiceCode, page });
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

          if (totalResults === 10000 || totalResults === 5000) {
            log.warn(`Result count ${totalResults} looks capped — you may be missing data for ${city}`);
          }
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

        // Filter and yield
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = BaseScraper;
