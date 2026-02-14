/**
 * Kansas Bar Association Scraper
 *
 * Source: https://directory-kard.kscourts.gov/
 * Method: SPA that returns 403 on plain fetch — use full browser-like headers
 *
 * The Kansas Attorney Registration & Discipline directory is an SPA
 * that aggressively blocks automated requests. This scraper attempts
 * to mimic a real browser session with proper headers and tries to
 * discover the underlying API.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class KansasScraper extends BaseScraper {
  constructor() {
    super({
      name: 'kansas',
      stateCode: 'KS',
      baseUrl: 'https://directory-kard.kscourts.gov/',
      pageSize: 25,
      practiceAreaCodes: {
        'bankruptcy':            'bankruptcy',
        'business':              'business',
        'civil litigation':      'civil litigation',
        'corporate':             'corporate',
        'criminal':              'criminal',
        'criminal defense':      'criminal defense',
        'elder':                 'elder law',
        'employment':            'employment',
        'estate planning':       'estate planning',
        'family':                'family law',
        'family law':            'family law',
        'immigration':           'immigration',
        'intellectual property': 'intellectual property',
        'personal injury':       'personal injury',
        'real estate':           'real estate',
        'tax':                   'tax',
      },
      defaultCities: [
        'Wichita', 'Overland Park', 'Kansas City', 'Olathe',
        'Topeka', 'Lawrence', 'Shawnee', 'Manhattan',
      ],
    });

    this.origin = 'https://directory-kard.kscourts.gov';

    // API paths to probe
    this.apiCandidates = [
      '/api/attorneys/search',
      '/api/attorney/search',
      '/api/members/search',
      '/api/search',
      '/api/v1/attorneys',
      '/api/v1/search',
      '/api/directory/search',
      '/api/lawyers/search',
      '/api/find',
      '/Attorney/Search',
      '/Search/Attorney',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SPA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SPA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SPA`);
  }

  /**
   * Build full browser-like headers to bypass 403 blocking.
   */
  _getBrowserHeaders(rateLimiter) {
    return {
      'User-Agent': rateLimiter.getUserAgent(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache',
      'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"macOS"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'Connection': 'keep-alive',
    };
  }

  /**
   * HTTP GET with full browser headers.
   */
  _httpGetBrowser(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._getBrowserHeaders(rateLimiter);
      headers['Referer'] = this.baseUrl;

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers,
        timeout: 15000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        // Handle redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this._httpGetBrowser(redirect, rateLimiter));
        }
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST for JSON API requests.
   */
  httpPost(url, data, rateLimiter, headers = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof data === 'string' ? data : JSON.stringify(data);
      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json,text/html,*/*',
          'Origin': this.origin,
          'Referer': this.baseUrl,
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'same-origin',
          ...headers,
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(opts, (res) => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(postData);
      req.end();
    });
  }

  /**
   * Try to discover API endpoint from the SPA shell page.
   */
  async _discoverApi(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this._httpGetBrowser(this.baseUrl, rateLimiter);

      if (response.statusCode === 403) {
        log.warn('KS directory returned 403 even with browser headers');
        return null;
      }

      if (response.statusCode !== 200) {
        log.warn(`KS directory returned status ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Check for API URLs in inline scripts
      const apiMatches = response.body.match(/["'](\/api\/[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        if (url.includes('search') || url.includes('attorney') || url.includes('member')) {
          log.info(`Found API reference in page: ${url}`);
          return `${this.origin}${url}`;
        }
      }

      // Check JS bundles
      const scripts = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('chunk') || src.includes('app') || src.includes('main') || src.includes('bundle')) {
          scripts.push(src.startsWith('http') ? src : `${this.origin}${src.startsWith('/') ? '' : '/'}${src}`);
        }
      });

      for (const scriptUrl of scripts.slice(0, 5)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this._httpGetBrowser(scriptUrl, rateLimiter);
          if (jsResp.statusCode === 200) {
            const jsApiMatches = jsResp.body.match(/["'](\/api\/[^"'\s]+)["']/g) || [];
            for (const match of jsApiMatches) {
              const url = match.replace(/["']/g, '');
              if (url.includes('search') || url.includes('attorney') || url.includes('member') || url.includes('directory')) {
                log.info(`Found API endpoint in JS: ${url}`);
                return `${this.origin}${url}`;
              }
            }
          }
        } catch (err) {
          // Continue
        }
      }
    } catch (err) {
      log.warn(`Failed to fetch KS directory: ${err.message}`);
    }

    return null;
  }

  /**
   * Try known API patterns with GET and POST.
   */
  async _tryApiEndpoints(city, rateLimiter) {
    for (const path of this.apiCandidates) {
      const url = `${this.origin}${path}`;

      // Try POST
      try {
        await rateLimiter.wait();
        const resp = await this.httpPost(url, {
          city: city, state: 'KS', status: 'Active',
          page: 1, pageSize: this.pageSize,
        }, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.members)) {
              log.success(`API endpoint found (POST): ${url}`);
              return { url, method: 'POST', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }

      // Try GET
      try {
        const getUrl = `${url}?city=${encodeURIComponent(city)}&state=KS&page=1&pageSize=${this.pageSize}`;
        await rateLimiter.wait();
        const resp = await this._httpGetBrowser(getUrl, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.members)) {
              log.success(`API endpoint found (GET): ${url}`);
              return { url, method: 'GET', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }
    }

    return null;
  }

  /**
   * Parse attorney records from JSON API response.
   */
  _parseApiResponse(data) {
    const attorneys = [];
    const records = Array.isArray(data)
      ? data
      : (data.results || data.data || data.members || data.attorneys || data.items || []);

    if (!Array.isArray(records)) return attorneys;

    for (const rec of records) {
      const fullName = rec.fullName || rec.full_name || rec.name ||
        `${rec.firstName || rec.first_name || ''} ${rec.lastName || rec.last_name || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.firstName || rec.first_name || firstName,
        last_name: rec.lastName || rec.last_name || lastName,
        full_name: fullName,
        firm_name: rec.firmName || rec.firm_name || rec.firm || '',
        city: rec.city || '',
        state: rec.state || 'KS',
        phone: rec.phone || rec.phoneNumber || '',
        email: rec.email || '',
        website: rec.website || '',
        bar_number: String(rec.barNumber || rec.bar_number || rec.kansasNumber || rec.id || ''),
        bar_status: rec.status || 'Active',
        profile_url: rec.profileUrl || rec.profile_url || '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Kansas SPA with 403 protection.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    // Step 1: Try to discover API
    let apiUrl = await this._discoverApi(rateLimiter);
    let apiMethod = null;

    // Step 2: Try common patterns
    if (!apiUrl) {
      log.info('No API discovered from page — trying common API patterns...');
      const discovery = await this._tryApiEndpoints(cities[0], rateLimiter);
      if (discovery) {
        apiUrl = discovery.url;
        apiMethod = discovery.method;

        const attorneys = this._parseApiResponse(discovery.data);
        if (attorneys.length > 0) {
          yield { _cityProgress: { current: 1, total: cities.length } };
          for (const attorney of attorneys) {
            yield this.transformResult(attorney, practiceArea);
          }
        }
      }
    }

    if (!apiUrl) {
      log.warn(`KS: SPA returns 403 on automated access — could not discover API endpoint.`);
      log.warn(`KS: The directory at ${this.baseUrl} blocks non-browser requests.`);
      log.warn(`KS: Try visiting the URL in a browser and checking Network tab for API calls.`);
      yield { _captcha: true, city: 'all', reason: 'SPA returns 403 — API endpoint not discovered' };
      return;
    }

    // Step 3: Iterate cities
    const startCity = apiMethod ? 1 : 0;

    for (let ci = startCity; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        let response;
        try {
          await rateLimiter.wait();
          if (apiMethod === 'POST' || !apiMethod) {
            response = await this.httpPost(apiUrl, {
              city, state: 'KS', status: 'Active',
              practiceArea: practiceArea || '',
              page, pageSize: this.pageSize,
            }, rateLimiter);
          } else {
            const params = new URLSearchParams({
              city, state: 'KS', page: String(page),
              pageSize: String(this.pageSize),
            });
            if (practiceArea) params.set('practiceArea', practiceArea);
            response = await this._httpGetBrowser(`${apiUrl}?${params}`, rateLimiter);
          }
        } catch (err) {
          log.error(`API request failed for ${city}: ${err.message}`);
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        let data;
        try {
          data = JSON.parse(response.body);
        } catch (_) {
          log.warn(`Non-JSON response for ${city}`);
          break;
        }

        const attorneys = this._parseApiResponse(data);

        if (attorneys.length === 0) {
          if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          break;
        }

        if (page === 1) {
          const total = data.totalCount || data.total || attorneys.length;
          log.success(`Found ${total} results for ${city}`);
        }

        for (const attorney of attorneys) {
          yield this.transformResult(attorney, practiceArea);
        }

        if (attorneys.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new KansasScraper();
