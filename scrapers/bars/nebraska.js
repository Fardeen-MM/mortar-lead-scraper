/**
 * Nebraska Bar Association Scraper
 *
 * Source: https://attorneys.nejudicial.gov/member-search
 * Method: Vue.js SPA — try to discover API endpoint from page source or common patterns
 *
 * The Nebraska Judicial Branch attorney directory is a Vue.js single-page application.
 * Plain HTML scraping returns only the SPA shell. This scraper attempts to:
 *  1. Fetch the page and extract API endpoint URLs from embedded JS bundles
 *  2. Try common API patterns (e.g., /api/members/search)
 *  3. Query discovered endpoints with JSON POST/GET
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NebraskaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nebraska',
      stateCode: 'NE',
      baseUrl: 'https://attorneys.nejudicial.gov/member-search',
      pageSize: 50,
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
        'family':                'family',
        'family law':            'family law',
        'immigration':           'immigration',
        'intellectual property': 'intellectual property',
        'personal injury':       'personal injury',
        'real estate':           'real estate',
        'tax':                   'tax',
        'workers comp':          'workers compensation',
      },
      defaultCities: [
        'Omaha', 'Lincoln', 'Bellevue', 'Grand Island',
        'Kearney', 'Fremont', 'Hastings', 'Norfolk',
      ],
    });

    this.origin = 'https://attorneys.nejudicial.gov';

    // Common API paths to try for Vue.js SPAs
    this.apiCandidates = [
      '/api/members/search',
      '/api/member/search',
      '/api/attorneys/search',
      '/api/attorney/search',
      '/api/search',
      '/api/v1/members/search',
      '/api/v1/attorneys/search',
      '/api/members',
      '/api/attorneys',
      '/api/lawyer/search',
      '/api/directory/search',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SPA API discovery`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SPA API discovery`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SPA API discovery`);
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
   * Attempt to discover API endpoints from the SPA page source and JS bundles.
   */
  async _discoverApiEndpoint(rateLimiter) {
    log.info('Fetching SPA page to discover API endpoints...');

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`SPA page returned status ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Look for API URLs in inline scripts
      const bodyText = response.body;
      const apiMatches = bodyText.match(/["'](\/api\/[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        log.info(`Found API reference in page source: ${url}`);
        return `${this.origin}${url}`;
      }

      // Look for JS bundle files and scan them for API references
      const scriptSrcs = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('chunk') || src.includes('app') || src.includes('main') || src.includes('bundle')) {
          scriptSrcs.push(src.startsWith('http') ? src : `${this.origin}${src.startsWith('/') ? '' : '/'}${src}`);
        }
      });

      for (const scriptUrl of scriptSrcs.slice(0, 5)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(scriptUrl, rateLimiter);
          if (jsResp.statusCode === 200) {
            const jsApiMatches = jsResp.body.match(/["'](\/api\/[^"'\s]+)["']/g) || [];
            for (const match of jsApiMatches) {
              const url = match.replace(/["']/g, '');
              if (url.includes('search') || url.includes('member') || url.includes('attorney')) {
                log.info(`Found API endpoint in JS bundle: ${url}`);
                return `${this.origin}${url}`;
              }
            }

            // Also look for baseURL or apiUrl patterns
            const baseUrlMatch = jsResp.body.match(/(?:baseURL|apiUrl|API_URL|apiBase)['":\s]+['"]([^"']+)['"]/);
            if (baseUrlMatch) {
              const apiBase = baseUrlMatch[1];
              log.info(`Found API base URL in JS bundle: ${apiBase}`);
              return apiBase.startsWith('http') ? apiBase : `${this.origin}${apiBase}`;
            }
          }
        } catch (err) {
          log.info(`Could not fetch JS bundle ${scriptUrl}: ${err.message}`);
        }
      }
    } catch (err) {
      log.warn(`Failed to fetch SPA page: ${err.message}`);
    }

    return null;
  }

  /**
   * Try candidate API endpoints with both GET and POST.
   */
  async _tryApiEndpoints(city, rateLimiter) {
    for (const path of this.apiCandidates) {
      const url = `${this.origin}${path}`;

      // Try POST with JSON body
      try {
        await rateLimiter.wait();
        const postResp = await this.httpPost(url, {
          city: city,
          state: 'NE',
          status: 'Active',
          page: 1,
          pageSize: this.pageSize,
        }, rateLimiter);

        if (postResp.statusCode === 200) {
          try {
            const data = JSON.parse(postResp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.members || data.attorneys)) {
              log.success(`API endpoint found (POST): ${url}`);
              return { url, method: 'POST', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (err) {
        // Continue to next candidate
      }

      // Try GET with query params
      try {
        const getUrl = `${url}?city=${encodeURIComponent(city)}&state=NE&status=Active&page=1&pageSize=${this.pageSize}`;
        await rateLimiter.wait();
        const getResp = await this.httpGet(getUrl, rateLimiter);

        if (getResp.statusCode === 200) {
          try {
            const data = JSON.parse(getResp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.members || data.attorneys)) {
              log.success(`API endpoint found (GET): ${url}`);
              return { url, method: 'GET', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (err) {
        // Continue to next candidate
      }
    }

    return null;
  }

  /**
   * Parse attorney data from JSON API response.
   */
  _parseApiResponse(data) {
    const attorneys = [];
    const records = Array.isArray(data)
      ? data
      : (data.results || data.data || data.members || data.attorneys || data.items || data.records || []);

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
        firm_name: rec.firmName || rec.firm_name || rec.firm || rec.company || '',
        city: rec.city || '',
        state: rec.state || 'NE',
        phone: rec.phone || rec.phoneNumber || rec.telephone || '',
        email: rec.email || rec.emailAddress || '',
        website: rec.website || rec.url || '',
        bar_number: String(rec.barNumber || rec.bar_number || rec.licenseNumber || rec.id || ''),
        bar_status: rec.status || rec.barStatus || rec.bar_status || 'Active',
        profile_url: rec.profileUrl || rec.profile_url || '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Vue.js SPA with API discovery.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    // Step 1: Try to discover the API endpoint from the SPA
    let apiUrl = await this._discoverApiEndpoint(rateLimiter);
    let apiMethod = null;

    // Step 2: If no endpoint discovered, try common patterns with first city
    if (!apiUrl) {
      log.info('No API discovered from page source — trying common API patterns...');
      const discovery = await this._tryApiEndpoints(cities[0], rateLimiter);
      if (discovery) {
        apiUrl = discovery.url;
        apiMethod = discovery.method;

        // Yield results from discovery call
        const attorneys = this._parseApiResponse(discovery.data);
        if (attorneys.length > 0) {
          log.success(`Discovered ${attorneys.length} attorneys from ${cities[0]} via ${apiUrl}`);
          yield { _cityProgress: { current: 1, total: cities.length } };
          for (const attorney of attorneys) {
            yield this.transformResult(attorney, practiceArea);
          }
        }
      }
    }

    if (!apiUrl) {
      log.warn(`NE: Vue.js SPA — could not discover API endpoint.`);
      log.warn(`NE: The directory at ${this.baseUrl} requires JavaScript rendering.`);
      log.warn(`NE: Try visiting the URL in a browser and checking Network tab for API calls.`);
      yield { _captcha: true, city: 'all', reason: 'SPA requires JavaScript rendering — no API endpoint discovered' };
      return;
    }

    // Step 3: Iterate through cities and fetch data
    const startCity = apiMethod ? 1 : 0; // Skip first city if already fetched during discovery

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
              city: city,
              state: 'NE',
              status: 'Active',
              practiceArea: practiceArea || '',
              page: page,
              pageSize: this.pageSize,
            }, rateLimiter);
          } else {
            const params = new URLSearchParams({
              city: city,
              state: 'NE',
              status: 'Active',
              page: String(page),
              pageSize: String(this.pageSize),
            });
            if (practiceArea) params.set('practiceArea', practiceArea);
            response = await this.httpGet(`${apiUrl}?${params}`, rateLimiter);
          }
        } catch (err) {
          log.error(`API request failed for ${city}: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        let data;
        try {
          data = JSON.parse(response.body);
        } catch (_) {
          log.warn(`Non-JSON response for ${city} — stopping`);
          break;
        }

        const attorneys = this._parseApiResponse(data);

        if (attorneys.length === 0) {
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          }
          break;
        }

        if (page === 1) {
          const total = data.totalCount || data.total || data.count || attorneys.length;
          log.success(`Found ${total} results for ${city}`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
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

module.exports = new NebraskaScraper();
