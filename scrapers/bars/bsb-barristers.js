/**
 * Bar Standards Board (BSB) — Barristers Register Scraper
 *
 * Source: https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html
 * Method: Attempt bulk CSV/list download first, fall back to HTML form search
 *
 * The BSB regulates barristers in England and Wales. The Barristers' Register
 * provides practising status, chambers/address, reserved legal activities,
 * and disciplinary findings. Barristers are typically contacted through their
 * chambers rather than directly (no personal email/phone).
 *
 * Overrides search() to first try the downloadable register, then fall back
 * to the HTML search form with Name/Organisation fields.
 */

const https = require('https');
const http = require('http');
const { Readable } = require('stream');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class BSBBarristersScraper extends BaseScraper {
  constructor() {
    super({
      name: 'bsb-barristers',
      stateCode: 'UK-EW-BAR',
      baseUrl: 'https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html',
      pageSize: 20,
      practiceAreaCodes: {
        'crime':                 'crime',
        'criminal':              'crime',
        'family':                'family',
        'family law':            'family',
        'civil':                 'civil',
        'commercial':            'commercial',
        'chancery':              'chancery',
        'employment':            'employment',
        'personal injury':       'personal injury',
        'public law':            'public law',
        'immigration':           'immigration',
        'tax':                   'tax',
        'intellectual property': 'intellectual property',
        'property':              'property',
        'real property':         'property',
      },
      defaultCities: [
        'London', 'Manchester', 'Birmingham', 'Leeds',
        'Bristol', 'Cardiff', 'Nottingham',
      ],
    });

    this.registerUrl = 'https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html';
    this.searchUrl = 'https://www.barstandardsboard.org.uk/for-the-public/search-a-barristers-record/the-barristers-register.html';

    // Known paths where the BSB may host a downloadable CSV/Excel register
    this.bulkDownloadPaths = [
      '/for-the-public/search-a-barristers-record/the-barristers-register/download',
      '/for-the-public/search-a-barristers-record/download-register',
      '/media/barristers-register.csv',
      '/media/barristers-register.xlsx',
      '/downloads/barristers-register.csv',
    ];
  }

  /**
   * Not used -- search() is fully overridden.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Not used -- search() is fully overridden.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  /**
   * Not used -- search() is fully overridden.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * HTTP POST for the BSB search form.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof data === 'string'
        ? data
        : (contentType.includes('json') ? JSON.stringify(data) : new URLSearchParams(data).toString());
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.barstandardsboard.org.uk',
          'Referer': this.registerUrl,
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        // Follow redirects with GET
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
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
   * HTTP GET with extended timeout for bulk downloads.
   */
  _httpGetRaw(url, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/csv,text/plain,application/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Accept-Encoding': 'identity',
        },
        timeout: 60000,
      };
      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this._httpGetRaw(redirect, rateLimiter, redirectCount + 1));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          contentType: res.headers['content-type'] || '',
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Try to discover and download a bulk register file from the BSB site.
   * Returns CSV content string or null.
   */
  async _tryBulkDownload(rateLimiter) {
    const origin = 'https://www.barstandardsboard.org.uk';

    // First, check the register page for download links
    log.info('Checking BSB register page for download links...');
    try {
      await rateLimiter.wait();
      const pageResp = await this.httpGet(this.registerUrl, rateLimiter);
      if (pageResp.statusCode === 200) {
        const $ = cheerio.load(pageResp.body);
        const downloadSelectors = [
          'a[href*=".csv"]', 'a[href*=".xlsx"]', 'a[href*="download"]',
          'a:contains("Download")', 'a:contains("CSV")', 'a:contains("Excel")',
          'a:contains("register")',
        ];
        for (const sel of downloadSelectors) {
          const link = $(sel).first();
          if (link.length) {
            let href = link.attr('href') || '';
            if (href && !href.includes('javascript:')) {
              if (href.startsWith('/')) href = `${origin}${href}`;
              else if (!href.startsWith('http')) href = `${origin}/${href}`;
              log.info(`Found potential download link: ${href}`);
              try {
                await rateLimiter.wait();
                const dlResp = await this._httpGetRaw(href, rateLimiter);
                if (dlResp.statusCode === 200 && dlResp.body.length > 500) {
                  const lines = dlResp.body.split('\n');
                  if (lines.length > 10 && lines[0].includes(',')) {
                    log.success(`Downloaded bulk register — ${lines.length} lines`);
                    return dlResp.body;
                  }
                }
              } catch (err) {
                log.info(`Download link failed: ${err.message}`);
              }
            }
          }
        }
      }
    } catch (err) {
      log.info(`Could not check register page: ${err.message}`);
    }

    // Try known bulk download paths
    for (const dlPath of this.bulkDownloadPaths) {
      const url = `${origin}${dlPath}`;
      log.info(`Trying bulk download: ${url}`);
      try {
        await rateLimiter.wait();
        const resp = await this._httpGetRaw(url, rateLimiter);
        if (resp.statusCode === 200 && resp.body.length > 500) {
          const lines = resp.body.split('\n');
          if (lines.length > 10 && lines[0].includes(',')) {
            log.success(`Bulk download successful from ${url} — ${lines.length} lines`);
            return resp.body;
          }
        }
      } catch (err) {
        log.info(`Bulk path ${dlPath} failed: ${err.message}`);
      }
    }

    return null;
  }

  /**
   * Parse CSV content into barrister records.
   */
  _parseCsvRows(csvContent, cities, practiceCode) {
    const attorneys = [];
    const lines = csvContent.split('\n');
    if (lines.length < 2) return attorneys;

    // Parse header row
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, '').toLowerCase());

    const citySet = new Set(cities.map(c => c.toLowerCase()));

    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      // Simple CSV parsing (handles quoted fields)
      const values = [];
      let current = '';
      let inQuotes = false;
      for (let j = 0; j < line.length; j++) {
        const ch = line[j];
        if (ch === '"') {
          inQuotes = !inQuotes;
        } else if (ch === ',' && !inQuotes) {
          values.push(current.trim());
          current = '';
        } else {
          current += ch;
        }
      }
      values.push(current.trim());

      const row = {};
      headers.forEach((h, idx) => { row[h] = values[idx] || ''; });

      // Map to standard fields
      const get = (keys) => {
        for (const key of keys) {
          if (row[key]) return row[key];
          const found = Object.keys(row).find(k => k.includes(key));
          if (found && row[found]) return row[found];
        }
        return '';
      };

      const fullName = get(['name', 'barrister name', 'full name', 'full_name']);
      const firstName = get(['first name', 'first_name', 'forename', 'given name']);
      const lastName = get(['last name', 'last_name', 'surname', 'family name']);
      const chambers = get(['chambers', 'organisation', 'firm', 'practice address']);
      const location = get(['city', 'town', 'location', 'address']);
      const status = get(['status', 'practising status', 'type']);
      const bsbRef = get(['bsb ref', 'bsb reference', 'reference', 'bar number', 'id']);
      const activities = get(['reserved activities', 'activities', 'practice areas']);

      // Build name parts
      let fName = firstName;
      let lName = lastName;
      let fFullName = fullName;
      if (!fName && !lName && fullName) {
        if (fullName.includes(',')) {
          const parts = fullName.split(',');
          lName = parts[0].trim();
          fName = (parts[1] || '').trim().split(/\s+/)[0];
          fFullName = `${fName} ${lName}`;
        } else {
          const split = this.splitName(fullName);
          fName = split.firstName;
          lName = split.lastName;
        }
      }
      if (!fFullName && (fName || lName)) {
        fFullName = `${fName} ${lName}`.trim();
      }

      if (!fFullName) continue;

      // Filter by city
      const locationLower = location.toLowerCase();
      const chambersLower = chambers.toLowerCase();
      let matchCity = '';
      for (const c of citySet) {
        if (locationLower.includes(c) || chambersLower.includes(c)) {
          matchCity = cities.find(ct => ct.toLowerCase() === c) || c;
          break;
        }
      }
      if (!matchCity) continue;

      // Filter by practice area
      if (practiceCode) {
        const allText = `${activities} ${chambers}`.toLowerCase();
        if (!allText.includes(practiceCode.toLowerCase())) continue;
      }

      attorneys.push({
        first_name: fName,
        last_name: lName,
        full_name: fFullName,
        firm_name: chambers,
        city: matchCity,
        state: 'UK-EW-BAR',
        phone: '',
        email: '',
        website: '',
        bar_number: bsbRef,
        bar_status: status || 'Practising',
        profile_url: '',
        reserved_activities: activities,
      });
    }

    return attorneys;
  }

  /**
   * Parse HTML search results from the BSB register page.
   */
  _parseHtmlSearchResults($) {
    const attorneys = [];

    // Try various selectors for barrister result cards
    const selectors = [
      '.search-result', '.result-item', '.barrister-result',
      'table tbody tr', '.card', 'li.result', '.register-entry',
      'article', '.panel',
    ];

    let $items = $([]);
    for (const sel of selectors) {
      $items = $(sel);
      if ($items.length > 0) break;
    }

    $items.each((_, el) => {
      const $el = $(el);

      // Extract name
      const nameEl = $el.find('h2 a, h3 a, h4 a, .name a, .barrister-name a').first();
      let fullName = nameEl.text().trim();
      if (!fullName) {
        fullName = $el.find('h2, h3, h4, .name, .barrister-name, td:first-child').first().text().trim();
      }
      if (!fullName || fullName.length < 2) return;

      // Profile URL
      let profileUrl = nameEl.attr('href') || '';
      if (profileUrl && profileUrl.startsWith('/')) {
        profileUrl = `https://www.barstandardsboard.org.uk${profileUrl}`;
      }

      // Extract chambers/organisation
      const chambers = ($el.find('.chambers, .organisation, .firm, td:nth-child(2)').text() || '').trim();

      // Extract status
      const status = ($el.find('.status, .badge, td:nth-child(3)').text() || '').trim();

      // Extract BSB reference
      const bsbRef = ($el.text().match(/BSB\s*(?:Ref|Reference|No)?:?\s*(\d+)/i) || ['', ''])[1].trim();

      // Extract location
      const location = ($el.find('.location, .address, .city, td:nth-child(4)').text() || '').trim();

      // Extract reserved activities
      const activities = ($el.find('.activities, .reserved-activities').text() || '').trim();

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: chambers,
        city: location,
        state: 'UK-EW-BAR',
        phone: '',
        email: '',
        website: '',
        bar_number: bsbRef,
        bar_status: status || 'Practising',
        profile_url: profileUrl,
        reserved_activities: activities,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the HTML search results page.
   */
  _extractHtmlResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:results?|records?|barristers?)\s+found/i) ||
                  text.match(/(?:Showing|Found)\s+(?:\d+\s*[-–]\s*\d+\s+of\s+)?([\d,]+)/i) ||
                  text.match(/Results:\s*([\d,]+)/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }

  /**
   * Async generator that yields barrister records.
   *
   * Strategy:
   *  1. Try to download the bulk CSV/register from the BSB site
   *  2. If bulk download available, parse CSV and filter by city/practice area
   *  3. If not available, fall back to HTML form search per city
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    // --- Attempt bulk CSV download ---
    log.info('Attempting bulk register download from BSB...');
    const csvContent = await this._tryBulkDownload(rateLimiter);

    if (csvContent) {
      log.success('Bulk register downloaded — parsing and filtering');
      const attorneys = this._parseCsvRows(csvContent, cities, practiceCode);

      yield { _cityProgress: { current: 1, total: cities.length } };

      let yieldCount = 0;
      for (const attorney of attorneys) {
        attorney.practice_area = practiceArea || '';
        yield this.transformResult(attorney, practiceArea);
        yieldCount++;

        if (options.maxPages && yieldCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max records limit (${options.maxPages * this.pageSize}) from bulk download`);
          return;
        }
      }

      log.success(`Yielded ${yieldCount} barristers from bulk register`);
      return;
    }

    // --- Fallback: HTML form search per city ---
    log.warn('Bulk download not available — falling back to HTML form search');

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} barristers in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build search form data
        const formData = {
          Name: '',
          Organisation: city,
        };
        if (page > 1) {
          formData.page = String(page);
        }

        log.info(`Page ${page} — POST ${this.searchUrl} [Organisation=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.searchUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} — skipping city ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this._extractHtmlResultCount($);
          if (totalResults === 0) {
            const testResults = this._parseHtmlSearchResults($);
            if (testResults.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testResults.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this._parseHtmlSearchResults($);

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

        // Filter by practice area (keyword match in chambers name or text)
        for (const attorney of attorneys) {
          if (practiceCode) {
            const allText = `${attorney.firm_name} ${attorney.reserved_activities || ''}`.toLowerCase();
            if (!allText.includes(practiceCode.toLowerCase())) continue;
          }
          attorney.city = attorney.city || city;
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === '>>' || text.includes('next page');
        }).length > 0;

        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages && !hasNext) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new BSBBarristersScraper();
