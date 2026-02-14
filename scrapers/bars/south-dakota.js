/**
 * South Dakota Bar Association Scraper
 *
 * Source: https://findalawyerinsd.com/
 * Method: React SPA, referral-style service — try to discover API
 *
 * The "Find a Lawyer in SD" site is a React single-page application
 * that serves as a lawyer referral service. This scraper attempts to:
 *  1. Fetch the SPA shell and extract API endpoints from JS bundles
 *  2. Try common API patterns for React apps
 *  3. Query discovered endpoints for attorney data
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class SouthDakotaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'south-dakota',
      stateCode: 'SD',
      baseUrl: 'https://findalawyerinsd.com/',
      pageSize: 25,
      practiceAreaCodes: {
        'agricultural':          'Agricultural Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'elder':                 'Elder Law',
        'employment':            'Employment Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'native american':       'Native American Law',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Sioux Falls', 'Rapid City', 'Aberdeen', 'Brookings',
        'Watertown', 'Mitchell', 'Pierre', 'Yankton',
      ],
    });

    this.origin = 'https://findalawyerinsd.com';

    // API patterns to try
    this.apiCandidates = [
      '/api/lawyers/search',
      '/api/lawyer/search',
      '/api/attorneys/search',
      '/api/attorney/search',
      '/api/search',
      '/api/v1/lawyers',
      '/api/v1/search',
      '/api/members/search',
      '/api/directory/search',
      '/api/find',
      '/graphql',
      '/wp-json/wp/v2/lawyer',
      '/wp-json/findlawyer/v1/search',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for React SPA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for React SPA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for React SPA`);
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
   * Discover API endpoint from the React SPA.
   */
  async _discoverApi(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`SD site returned status ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Check for API references in inline scripts and page source
      const apiMatches = response.body.match(/["']((?:\/api\/|https?:\/\/[^"']*api[^"']*)[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        if (url.includes('search') || url.includes('lawyer') || url.includes('attorney') || url.includes('member')) {
          log.info(`Found API reference: ${url}`);
          return url.startsWith('http') ? url : `${this.origin}${url}`;
        }
      }

      // Check for GraphQL endpoint
      if (response.body.includes('graphql') || response.body.includes('__NEXT_DATA__')) {
        log.info('Detected potential GraphQL or Next.js app');
      }

      // Check for Next.js __NEXT_DATA__ with pre-rendered data
      const nextDataMatch = response.body.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
      if (nextDataMatch) {
        try {
          const nextData = JSON.parse(nextDataMatch[1]);
          if (nextData.props?.pageProps) {
            log.info('Found Next.js pre-rendered data');
            return '__NEXT_DATA__';
          }
        } catch (_) { /* not valid JSON */ }
      }

      // Scan JS bundles
      const scripts = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('chunk') || src.includes('app') || src.includes('main') ||
            src.includes('bundle') || src.includes('_next') || src.includes('static/js')) {
          scripts.push(src.startsWith('http') ? src : `${this.origin}${src.startsWith('/') ? '' : '/'}${src}`);
        }
      });

      for (const scriptUrl of scripts.slice(0, 5)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(scriptUrl, rateLimiter);
          if (jsResp.statusCode === 200) {
            const jsApiMatches = jsResp.body.match(/["'](\/api\/[^"'\s]+)["']/g) || [];
            for (const match of jsApiMatches) {
              const url = match.replace(/["']/g, '');
              if (url.includes('search') || url.includes('lawyer') || url.includes('attorney')) {
                log.info(`Found API in JS bundle: ${url}`);
                return `${this.origin}${url}`;
              }
            }

            // Check for API base URL
            const baseMatch = jsResp.body.match(/(?:baseURL|apiUrl|API_BASE)\s*[:=]\s*["']([^"']+)["']/);
            if (baseMatch) {
              log.info(`Found API base: ${baseMatch[1]}`);
              return baseMatch[1].startsWith('http') ? baseMatch[1] : `${this.origin}${baseMatch[1]}`;
            }
          }
        } catch (_) { /* continue */ }
      }
    } catch (err) {
      log.warn(`Failed to fetch SD site: ${err.message}`);
    }

    return null;
  }

  /**
   * Try candidate API endpoints.
   */
  async _tryApiEndpoints(city, practiceArea, rateLimiter) {
    for (const path of this.apiCandidates) {
      const url = `${this.origin}${path}`;

      // Try POST
      try {
        await rateLimiter.wait();
        const payload = {
          city, state: 'SD',
          page: 1, pageSize: this.pageSize,
        };
        if (practiceArea) payload.practiceArea = practiceArea;

        const resp = await this.httpPost(url, payload, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.lawyers || data.attorneys)) {
              log.success(`API found (POST): ${url}`);
              return { url, method: 'POST', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }

      // Try GET
      try {
        const getUrl = `${url}?city=${encodeURIComponent(city)}&state=SD&page=1&pageSize=${this.pageSize}`;
        await rateLimiter.wait();
        const resp = await this.httpGet(getUrl, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.lawyers || data.attorneys)) {
              log.success(`API found (GET): ${url}`);
              return { url, method: 'GET', data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }
    }

    return null;
  }

  /**
   * Parse attorney records from JSON.
   */
  _parseApiResponse(data) {
    const attorneys = [];
    const records = Array.isArray(data)
      ? data
      : (data.results || data.data || data.lawyers || data.attorneys || data.members || data.items || []);

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
        state: rec.state || 'SD',
        phone: rec.phone || rec.phoneNumber || '',
        email: rec.email || '',
        website: rec.website || rec.url || '',
        bar_number: String(rec.barNumber || rec.bar_number || rec.id || ''),
        bar_status: rec.status || 'Active',
        profile_url: rec.profileUrl || rec.profile_url || '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for South Dakota React SPA.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    log.scrape('Attempting to access SD lawyer referral directory...');

    // Step 1: Discover API
    let apiUrl = await this._discoverApi(rateLimiter);
    let apiMethod = null;

    // Step 2: Try candidate API endpoints
    if (!apiUrl || apiUrl === '__NEXT_DATA__') {
      if (apiUrl === '__NEXT_DATA__') {
        log.info('SD site uses Next.js — trying _next/data API patterns...');
      }
      log.info('Trying common API patterns...');
      const discovery = await this._tryApiEndpoints(cities[0], practiceArea, rateLimiter);
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

    if (!apiUrl || apiUrl === '__NEXT_DATA__') {
      log.warn(`SD: React SPA — could not discover API endpoint.`);
      log.warn(`SD: The referral service at ${this.baseUrl} requires JavaScript rendering.`);
      log.warn(`SD: Try visiting the URL in a browser and checking Network tab for API calls.`);
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
              city, state: 'SD', page, pageSize: this.pageSize,
            };
            if (practiceArea) payload.practiceArea = this.resolvePracticeCode(practiceArea) || practiceArea;
            response = await this.httpPost(apiUrl, payload, rateLimiter);
          } else {
            const params = new URLSearchParams({
              city, state: 'SD', page: String(page), pageSize: String(this.pageSize),
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

module.exports = new SouthDakotaScraper();
