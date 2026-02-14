/**
 * Michigan Bar Association Scraper
 *
 * Source: https://sbm.reliaguide.com/lawyer/search
 * Method: React SPA (ReliaGuide platform) — discover API endpoint from JS bundles
 *
 * The State Bar of Michigan uses the ReliaGuide platform, a React SPA
 * that renders attorney data via a REST API. This scraper attempts to:
 *  1. Fetch the SPA shell and extract API endpoint URLs from JS bundles
 *  2. Try common ReliaGuide API patterns like /api/lawyer/search
 *  3. Query discovered endpoints with JSON payloads
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MichiganScraper extends BaseScraper {
  constructor() {
    super({
      name: 'michigan',
      stateCode: 'MI',
      baseUrl: 'https://sbm.reliaguide.com/lawyer/search',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'elder':                 'Elder Law',
        'employment':            'Employment Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration Law',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Detroit', 'Grand Rapids', 'Ann Arbor', 'Lansing',
        'Troy', 'Southfield', 'Farmington Hills', 'Kalamazoo',
      ],
    });

    this.origin = 'https://sbm.reliaguide.com';

    // ReliaGuide-specific API patterns
    this.apiCandidates = [
      '/api/lawyer/search',
      '/api/lawyers/search',
      '/api/v1/lawyer/search',
      '/api/v1/lawyers/search',
      '/api/member/search',
      '/api/members/search',
      '/api/attorney/search',
      '/api/attorneys/search',
      '/api/search',
      '/api/directory/search',
      '/lawyer/api/search',
      '/api/v2/lawyer/search',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ReliaGuide SPA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for ReliaGuide SPA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for ReliaGuide SPA`);
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
   * Discover API endpoint from SPA page source and JS bundles.
   */
  async _discoverApiEndpoint(rateLimiter) {
    log.info('Fetching ReliaGuide SPA to discover API endpoints...');

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`ReliaGuide page returned status ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Look for API references in inline scripts and page source
      const apiMatches = response.body.match(/["'](\/api\/[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        if (url.includes('search') || url.includes('lawyer') || url.includes('member')) {
          log.info(`Found API reference in page: ${url}`);
          return `${this.origin}${url}`;
        }
      }

      // Look for REST API patterns in data attributes
      $('[data-api], [data-url], [data-endpoint]').each((_, el) => {
        const apiUrl = $(el).attr('data-api') || $(el).attr('data-url') || $(el).attr('data-endpoint');
        if (apiUrl) {
          log.info(`Found API URL in data attribute: ${apiUrl}`);
        }
      });

      // Scan JS bundles for API references
      const scriptSrcs = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('chunk') || src.includes('app') || src.includes('main') ||
            src.includes('bundle') || src.includes('static/js')) {
          scriptSrcs.push(src.startsWith('http') ? src : `${this.origin}${src.startsWith('/') ? '' : '/'}${src}`);
        }
      });

      for (const scriptUrl of scriptSrcs.slice(0, 6)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(scriptUrl, rateLimiter);
          if (jsResp.statusCode === 200) {
            // Look for API endpoint patterns in React app bundles
            const patterns = [
              /["'](\/api\/[^"'\s]+(?:search|lawyer|member|attorney)[^"'\s]*)["']/g,
              /(?:fetch|axios|ajax)\s*\(\s*["'](\/[^"'\s]+)["']/g,
              /(?:baseURL|apiUrl|API_BASE|apiBase)\s*[:=]\s*["']([^"']+)["']/g,
            ];

            for (const pattern of patterns) {
              let m;
              while ((m = pattern.exec(jsResp.body)) !== null) {
                const found = m[1];
                if (found.includes('search') || found.includes('lawyer') || found.includes('member')) {
                  log.info(`Found API endpoint in JS bundle: ${found}`);
                  return found.startsWith('http') ? found : `${this.origin}${found}`;
                }
              }
            }
          }
        } catch (err) {
          // Continue
        }
      }
    } catch (err) {
      log.warn(`Failed to fetch ReliaGuide page: ${err.message}`);
    }

    return null;
  }

  /**
   * Try candidate API endpoints.
   */
  async _tryApiEndpoints(city, rateLimiter) {
    for (const path of this.apiCandidates) {
      const url = `${this.origin}${path}`;

      // Try POST with JSON
      try {
        await rateLimiter.wait();
        const resp = await this.httpPost(url, {
          city: city,
          state: 'MI',
          status: 'Active',
          page: 1,
          pageSize: this.pageSize,
          searchType: 'city',
        }, rateLimiter);

        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.lawyers || data.members)) {
              log.success(`ReliaGuide API found (POST): ${url}`);
              return { url, method: 'POST', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }

      // Try GET
      try {
        const getUrl = `${url}?city=${encodeURIComponent(city)}&state=MI&page=1&pageSize=${this.pageSize}`;
        await rateLimiter.wait();
        const resp = await this.httpGet(getUrl, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.lawyers || data.members)) {
              log.success(`ReliaGuide API found (GET): ${url}`);
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
      : (data.results || data.data || data.lawyers || data.members || data.attorneys || data.items || []);

    if (!Array.isArray(records)) return attorneys;

    for (const rec of records) {
      const fullName = rec.fullName || rec.full_name || rec.name || rec.displayName ||
        `${rec.firstName || rec.first_name || ''} ${rec.lastName || rec.last_name || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.firstName || rec.first_name || firstName,
        last_name: rec.lastName || rec.last_name || lastName,
        full_name: fullName,
        firm_name: rec.firmName || rec.firm_name || rec.firm || rec.organization || '',
        city: rec.city || '',
        state: rec.state || 'MI',
        phone: rec.phone || rec.phoneNumber || rec.telephone || '',
        email: rec.email || rec.emailAddress || '',
        website: rec.website || rec.url || rec.websiteUrl || '',
        bar_number: String(rec.barNumber || rec.bar_number || rec.pNumber || rec.licenseNumber || rec.id || ''),
        bar_status: rec.status || rec.memberStatus || 'Active',
        profile_url: rec.profileUrl || rec.profile_url || rec.detailUrl || '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Michigan ReliaGuide SPA.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    // Step 1: Discover API
    let apiUrl = await this._discoverApiEndpoint(rateLimiter);
    let apiMethod = null;

    // Step 2: Try common patterns
    if (!apiUrl) {
      log.info('No API discovered from page — trying ReliaGuide API patterns...');
      const discovery = await this._tryApiEndpoints(cities[0], rateLimiter);
      if (discovery) {
        apiUrl = discovery.url;
        apiMethod = discovery.method;

        const attorneys = this._parseApiResponse(discovery.data);
        if (attorneys.length > 0) {
          yield { _cityProgress: { current: 1, total: cities.length } };
          log.success(`Found ${attorneys.length} attorneys for ${cities[0]}`);
          for (const attorney of attorneys) {
            yield this.transformResult(attorney, practiceArea);
          }
        }
      }
    }

    if (!apiUrl) {
      log.warn(`MI: ReliaGuide React SPA — could not discover API endpoint.`);
      log.warn(`MI: The directory at ${this.baseUrl} requires JavaScript rendering.`);
      log.warn(`MI: Try visiting the URL in a browser and checking Network tab for API calls.`);
      yield { _captcha: true, city: 'all', reason: 'React SPA — no API endpoint discovered' };
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
            const payload = {
              city: city,
              state: 'MI',
              status: 'Active',
              page: page,
              pageSize: this.pageSize,
              searchType: 'city',
            };
            if (practiceArea) {
              payload.practiceArea = this.resolvePracticeCode(practiceArea) || practiceArea;
            }
            response = await this.httpPost(apiUrl, payload, rateLimiter);
          } else {
            const params = new URLSearchParams({
              city, state: 'MI', page: String(page),
              pageSize: String(this.pageSize),
            });
            if (practiceArea) params.set('practiceArea', this.resolvePracticeCode(practiceArea) || practiceArea);
            response = await this.httpGet(`${apiUrl}?${params}`, rateLimiter);
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
          const total = data.totalCount || data.total || data.count || attorneys.length;
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

module.exports = new MichiganScraper();
