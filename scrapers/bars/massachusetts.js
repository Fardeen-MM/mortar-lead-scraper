/**
 * Massachusetts Bar Scraper
 *
 * Source: https://www.massbbo.org/s/
 * Method: Salesforce Lightning SPA — best-effort scraper
 *
 * The Massachusetts Board of Bar Overseers (BBO) directory is built on
 * Salesforce Lightning Experience, a complex JavaScript SPA. This scraper:
 *  1. Attempts to fetch the page and find accessible API endpoints
 *  2. Tries Salesforce Aura/LWC component API patterns
 *  3. If no API is accessible, yields a captcha/SPA signal
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MassachusettsScraper extends BaseScraper {
  constructor() {
    super({
      name: 'massachusetts',
      stateCode: 'MA',
      baseUrl: 'https://www.massbbo.org/s/',
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
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'securities':            'Securities',
        'tax':                   'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Boston', 'Worcester', 'Springfield', 'Cambridge',
        'Lowell', 'Brockton', 'Quincy', 'New Bedford',
      ],
    });

    this.origin = 'https://www.massbbo.org';

    // Salesforce Lightning API patterns to try
    this.sfApiCandidates = [
      '/s/sfsites/aura',
      '/aura',
      '/services/apexrest/attorney/',
      '/services/apexrest/member/',
      '/services/apexrest/search/',
      '/services/apexrest/BBO/',
      '/services/data/v58.0/query/',
      '/api/attorney/search',
      '/api/members/search',
      '/api/search',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Salesforce Lightning SPA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Salesforce Lightning SPA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Salesforce Lightning SPA`);
  }

  /**
   * HTTP POST for API requests.
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
   * Try to discover Salesforce API endpoints from the page source.
   */
  async _discoverApi(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        return { status: response.statusCode, apiUrl: null, auraToken: null };
      }

      const $ = cheerio.load(response.body);

      // Look for Aura framework token
      const auraTokenMatch = response.body.match(/auraConfig\s*(?:=|:)\s*\{[^}]*"token"\s*:\s*"([^"]+)"/);
      const auraToken = auraTokenMatch ? auraTokenMatch[1] : null;

      // Look for Aura context
      const contextMatch = response.body.match(/auraConfig\s*(?:=|:)\s*(\{[^;]+?\});/);

      // Search for API endpoints in page source
      const apiMatches = response.body.match(/["'](\/(?:s\/sfsites|services|aura|api)\/[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        if (url.includes('search') || url.includes('attorney') || url.includes('member') || url.includes('query') || url.includes('aura')) {
          log.info(`Found potential API endpoint: ${url}`);
          return { status: 200, apiUrl: `${this.origin}${url}`, auraToken };
        }
      }

      // Check for community search page patterns
      const searchPages = [];
      $('a[href*="search"], a[href*="attorney"], a[href*="lawyer"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        if (href && !href.includes('javascript:')) {
          searchPages.push(href.startsWith('http') ? href : `${this.origin}${href}`);
        }
      });

      // Scan JS bundles
      const scripts = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('app') || src.includes('components') || src.includes('aura_prod')) {
          scripts.push(src.startsWith('http') ? src : `${this.origin}${src.startsWith('/') ? '' : '/'}${src}`);
        }
      });

      for (const scriptUrl of scripts.slice(0, 4)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(scriptUrl, rateLimiter);
          if (jsResp.statusCode === 200) {
            const jsApiMatches = jsResp.body.match(/["'](\/(?:services|api|aura)\/[^"'\s]+)["']/g) || [];
            for (const match of jsApiMatches) {
              const url = match.replace(/["']/g, '');
              if (url.includes('search') || url.includes('attorney') || url.includes('member')) {
                log.info(`Found API in JS bundle: ${url}`);
                return { status: 200, apiUrl: `${this.origin}${url}`, auraToken };
              }
            }
          }
        } catch (_) { /* continue */ }
      }

      return { status: 200, apiUrl: null, auraToken };
    } catch (err) {
      return { status: 0, apiUrl: null, auraToken: null };
    }
  }

  /**
   * Try Salesforce Aura endpoint with action descriptor.
   */
  async _tryAuraEndpoint(city, rateLimiter) {
    const auraUrl = `${this.origin}/s/sfsites/aura`;

    // Build Salesforce Aura action message
    const message = {
      actions: [{
        id: '1',
        descriptor: 'apex://AttorneySearchController/ACTION$searchAttorneys',
        callingDescriptor: 'UNKNOWN',
        params: {
          city: city,
          state: 'MA',
          status: 'Active',
        },
      }],
    };

    try {
      await rateLimiter.wait();
      const resp = await this.httpPost(auraUrl, {
        message: JSON.stringify(message),
        'aura.context': '{"mode":"PROD","fwuid":""}',
        'aura.token': 'undefined',
      }, rateLimiter, {
        'Content-Type': 'application/x-www-form-urlencoded',
      });

      if (resp.statusCode === 200) {
        try {
          const data = JSON.parse(resp.body);
          if (data.actions && data.actions[0] && data.actions[0].returnValue) {
            return data.actions[0].returnValue;
          }
        } catch (_) { /* not valid aura response */ }
      }
    } catch (_) { /* aura endpoint not available */ }

    return null;
  }

  /**
   * Try common API patterns.
   */
  async _tryApiEndpoints(city, rateLimiter) {
    for (const path of this.sfApiCandidates) {
      const url = `${this.origin}${path}`;

      // Try POST
      try {
        await rateLimiter.wait();
        const resp = await this.httpPost(url, {
          city, state: 'MA', status: 'Active',
          page: 1, pageSize: this.pageSize,
        }, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.records)) {
              log.success(`API found (POST): ${url}`);
              return { url, data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }

      // Try GET
      try {
        const getUrl = `${url}?city=${encodeURIComponent(city)}&state=MA&page=1&pageSize=${this.pageSize}`;
        await rateLimiter.wait();
        const resp = await this.httpGet(getUrl, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            if (data && (Array.isArray(data) || data.results || data.data || data.records)) {
              log.success(`API found (GET): ${url}`);
              return { url, data };
            }
          } catch (_) { /* not JSON */ }
        }
      } catch (_) { /* continue */ }
    }

    return null;
  }

  /**
   * Parse attorney records from API response.
   */
  _parseApiResponse(data) {
    const attorneys = [];
    const records = Array.isArray(data)
      ? data
      : (data.results || data.records || data.data || data.members || data.attorneys || []);

    if (!Array.isArray(records)) return attorneys;

    for (const rec of records) {
      const fullName = rec.Name || rec.fullName || rec.full_name || rec.name ||
        `${rec.FirstName || rec.firstName || ''} ${rec.LastName || rec.lastName || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.FirstName || rec.firstName || rec.first_name || firstName,
        last_name: rec.LastName || rec.lastName || rec.last_name || lastName,
        full_name: fullName,
        firm_name: rec.Company || rec.firmName || rec.firm || '',
        city: rec.City || rec.city || '',
        state: rec.State || rec.state || 'MA',
        phone: rec.Phone || rec.phone || '',
        email: rec.Email || rec.email || '',
        website: rec.Website || rec.website || '',
        bar_number: String(rec.BarNumber || rec.barNumber || rec.BBONumber || rec.MemberNumber || rec.Id || ''),
        bar_status: rec.Status || rec.status || 'Active',
        profile_url: '',
      });
    }

    return attorneys;
  }

  /**
   * Override search() for Massachusetts BBO Salesforce Lightning SPA.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    log.scrape('Attempting to access MA BBO attorney directory...');
    log.warn('MA BBO uses Salesforce Lightning — this is a best-effort scraper.');

    // Step 1: Discover API
    const discovery = await this._discoverApi(rateLimiter);

    if (discovery.status !== 200 && discovery.status !== 0) {
      log.warn(`MA BBO returned status ${discovery.status}`);
    }

    // Step 2: Try Aura endpoint
    log.info('Trying Salesforce Aura endpoint...');
    const auraResult = await this._tryAuraEndpoint(cities[0], rateLimiter);

    if (auraResult) {
      const attorneys = this._parseApiResponse(auraResult);
      if (attorneys.length > 0) {
        log.success(`Aura endpoint returned ${attorneys.length} attorneys for ${cities[0]}`);
        yield { _cityProgress: { current: 1, total: cities.length } };
        for (const attorney of attorneys) {
          yield this.transformResult(attorney, practiceArea);
        }

        // Continue with remaining cities via Aura
        for (let ci = 1; ci < cities.length; ci++) {
          const city = cities[ci];
          yield { _cityProgress: { current: ci + 1, total: cities.length } };
          log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

          const result = await this._tryAuraEndpoint(city, rateLimiter);
          if (result) {
            const atts = this._parseApiResponse(result);
            for (const attorney of atts) {
              yield this.transformResult(attorney, practiceArea);
            }
          }
        }
        return;
      }
    }

    // Step 3: Try discovered or common API endpoints
    if (discovery.apiUrl) {
      log.info(`Trying discovered API: ${discovery.apiUrl}`);
    }

    const apiResult = await this._tryApiEndpoints(cities[0], rateLimiter);
    if (apiResult) {
      const attorneys = this._parseApiResponse(apiResult.data);
      if (attorneys.length > 0) {
        yield { _cityProgress: { current: 1, total: cities.length } };
        for (const attorney of attorneys) {
          yield this.transformResult(attorney, practiceArea);
        }
        // Could continue with more cities here, but since this is best-effort we stop
      }
      return;
    }

    // Step 4: No API accessible — report limitation
    log.warn(`MA BBO: Salesforce Lightning SPA prevents automated access.`);
    log.warn(`MA: The directory at ${this.baseUrl} requires:`);
    log.warn(`MA:   1. Salesforce Lightning Web Component rendering (JavaScript)`);
    log.warn(`MA:   2. Possible Salesforce Community authentication`);
    log.warn(`MA: Manual data collection or a headless browser is required.`);

    yield { _captcha: true, city: 'all', reason: 'Salesforce Lightning SPA — requires JavaScript rendering' };
  }
}

module.exports = new MassachusettsScraper();
