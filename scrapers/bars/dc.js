/**
 * District of Columbia Bar Scraper
 *
 * Source: https://my.dcbar.org/memberdirectory
 * Method: Salesforce + reCAPTCHA v3 — best-effort scraper
 *
 * The DC Bar member directory is built on Salesforce Communities with
 * reCAPTCHA v3 protection. This is a best-effort scraper that:
 *  1. Attempts to fetch the directory page
 *  2. Checks if reCAPTCHA blocks access
 *  3. Tries to find any accessible API or data endpoints
 *  4. If blocked, yields a clear captcha signal
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class DCScraper extends BaseScraper {
  constructor() {
    super({
      name: 'dc',
      stateCode: 'DC',
      baseUrl: 'https://my.dcbar.org/memberdirectory',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'antitrust':             'Antitrust',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Law',
        'civil litigation':      'Civil Litigation',
        'constitutional':        'Constitutional Law',
        'corporate':             'Corporate Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Defense',
        'employment':            'Employment Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'government':            'Government',
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'international':         'International Law',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'securities':            'Securities',
        'tax':                   'Tax Law',
      },
      defaultCities: [
        'Washington',
      ],
    });

    this.origin = 'https://my.dcbar.org';

    // Salesforce API patterns
    this.sfApiCandidates = [
      '/services/data/v58.0/query/',
      '/services/apexrest/MemberDirectory/',
      '/services/apexrest/members/',
      '/aura?r=',
      '/s/sfsites/aura',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Salesforce SPA`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Salesforce SPA`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Salesforce SPA`);
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
   * Check for reCAPTCHA and Salesforce barriers in the page.
   */
  _detectBarriers(body) {
    const barriers = [];
    if (body.includes('recaptcha') || body.includes('reCAPTCHA') || body.includes('grecaptcha')) {
      barriers.push('reCAPTCHA v3');
    }
    if (body.includes('challenge-form') || body.includes('challenge-running')) {
      barriers.push('Challenge page');
    }
    if (body.includes('sfdcPage') || body.includes('lightning') || body.includes('aura:')) {
      barriers.push('Salesforce Lightning');
    }
    if (body.includes('force.com') || body.includes('visualforce')) {
      barriers.push('Salesforce Visualforce');
    }
    return barriers;
  }

  /**
   * Try to discover Salesforce Aura/LWC endpoints.
   */
  async _discoverSalesforceApi(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        return { status: response.statusCode, barriers: [], apiUrl: null };
      }

      const barriers = this._detectBarriers(response.body);
      const $ = cheerio.load(response.body);

      // Look for Salesforce Aura context and framework boot
      const auraMatch = response.body.match(/auraConfig\s*=\s*(\{[^;]+\})/);
      if (auraMatch) {
        log.info('Found Salesforce Aura configuration');
      }

      // Look for API endpoints in the source
      const apiMatches = response.body.match(/["'](\/(?:services|s|aura|api)\/[^"']+)["']/g) || [];
      for (const match of apiMatches) {
        const url = match.replace(/["']/g, '');
        if (url.includes('member') || url.includes('directory') || url.includes('search') || url.includes('query')) {
          log.info(`Found potential Salesforce API: ${url}`);
          return { status: 200, barriers, apiUrl: `${this.origin}${url}` };
        }
      }

      return { status: 200, barriers, apiUrl: null };
    } catch (err) {
      return { status: 0, barriers: [`Connection error: ${err.message}`], apiUrl: null };
    }
  }

  /**
   * Override search() for DC Bar with Salesforce + reCAPTCHA.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    log.scrape(`Attempting to access DC Bar member directory...`);
    log.warn(`DC Bar uses Salesforce + reCAPTCHA v3 — this is a best-effort scraper.`);

    // Step 1: Try to access the directory and detect barriers
    const discovery = await this._discoverSalesforceApi(rateLimiter);

    if (discovery.barriers.length > 0) {
      log.warn(`DC Bar barriers detected: ${discovery.barriers.join(', ')}`);
    }

    if (discovery.status === 403 || discovery.status === 401) {
      log.warn(`DC Bar returned ${discovery.status} — access blocked.`);
      yield { _captcha: true, city: 'Washington', reason: `HTTP ${discovery.status} — Salesforce login/reCAPTCHA required` };
      return;
    }

    // Step 2: If API discovered, try using it
    if (discovery.apiUrl) {
      log.info(`Attempting Salesforce API: ${discovery.apiUrl}`);

      for (let ci = 0; ci < cities.length; ci++) {
        const city = cities[ci];
        yield { _cityProgress: { current: ci + 1, total: cities.length } };

        let page = 1;
        let pagesFetched = 0;

        while (true) {
          if (options.maxPages && pagesFetched >= options.maxPages) break;

          let response;
          try {
            await rateLimiter.wait();
            response = await this.httpPost(discovery.apiUrl, {
              city: city,
              state: 'DC',
              status: 'Active',
              practiceArea: practiceArea || '',
              page: page,
              pageSize: this.pageSize,
            }, rateLimiter);
          } catch (err) {
            log.error(`Salesforce API failed: ${err.message}`);
            break;
          }

          if (response.statusCode !== 200) {
            log.warn(`Salesforce API returned ${response.statusCode}`);
            break;
          }

          let data;
          try {
            data = JSON.parse(response.body);
          } catch (_) {
            log.warn('Non-JSON response from Salesforce API');
            break;
          }

          const records = Array.isArray(data) ? data
            : (data.results || data.records || data.data || data.members || []);

          if (!Array.isArray(records) || records.length === 0) {
            if (page === 1) log.info(`No results from Salesforce API for ${city}`);
            break;
          }

          for (const rec of records) {
            const fullName = rec.Name || rec.fullName || rec.name ||
              `${rec.FirstName || rec.firstName || ''} ${rec.LastName || rec.lastName || ''}`.trim();
            if (!fullName) continue;

            const { firstName, lastName } = this.splitName(fullName);
            yield this.transformResult({
              first_name: rec.FirstName || rec.firstName || firstName,
              last_name: rec.LastName || rec.lastName || lastName,
              full_name: fullName,
              firm_name: rec.Company || rec.firmName || rec.firm || '',
              city: rec.City || rec.city || city,
              state: 'DC',
              phone: rec.Phone || rec.phone || '',
              email: rec.Email || rec.email || '',
              website: rec.Website || rec.website || '',
              bar_number: String(rec.BarNumber || rec.barNumber || rec.MemberNumber || rec.Id || ''),
              bar_status: rec.Status || rec.status || 'Active',
              profile_url: '',
            }, practiceArea);
          }

          if (records.length < this.pageSize) break;
          page++;
          pagesFetched++;
        }
      }
      return;
    }

    // Step 3: No API found — report limitation
    log.warn(`DC Bar: Salesforce + reCAPTCHA v3 prevents automated access.`);
    log.warn(`DC: The member directory at ${this.baseUrl} requires:`);
    log.warn(`DC:   1. Salesforce Lightning Web Component rendering (JavaScript)`);
    log.warn(`DC:   2. reCAPTCHA v3 token validation`);
    log.warn(`DC:   3. Possible Salesforce Community login`);
    log.warn(`DC: Manual data collection or a headless browser with CAPTCHA solving is required.`);

    yield { _captcha: true, city: 'Washington', reason: 'Salesforce + reCAPTCHA v3 — requires browser-based CAPTCHA solving' };
  }
}

module.exports = new DCScraper();
