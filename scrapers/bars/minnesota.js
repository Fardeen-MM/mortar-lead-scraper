/**
 * Minnesota Bar Association Scraper
 *
 * Source: https://mars.courts.state.mn.us/
 * Method: CSV download of the entire public attorney list, parsed with csv-parser
 *
 * The MN Lawyer Registration Office (MARS) publishes a downloadable CSV of all
 * registered attorneys. This scraper attempts to fetch that CSV directly. If the
 * CSV download link is not available via simple GET, it falls back to the HTML
 * search form for city-by-city scraping.
 */

const https = require('https');
const http = require('http');
const { Readable } = require('stream');
const csvParser = require('csv-parser');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MinnesotaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'minnesota',
      stateCode: 'MN',
      baseUrl: 'https://mars.courts.state.mn.us/',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'estate planning':      'estate',
        'estate':               'estate',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'employment',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real estate',
        'civil litigation':     'litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual property',
        'personal injury':      'personal injury',
        'workers comp':         'workers compensation',
        'environmental':        'environmental',
        'health':               'health',
      },
      defaultCities: [
        'Minneapolis', 'St. Paul', 'Rochester', 'Duluth',
        'Bloomington', 'Brooklyn Park', 'Plymouth', 'Edina',
      ],
    });

    // Known CSV download paths to try on the MARS site
    this.csvDownloadPaths = [
      '/api/lawyers/download',
      '/lawyers/download',
      '/export/csv',
      '/data/attorneys.csv',
      '/api/export',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CSV/HTML scraping`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CSV/HTML scraping`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CSV/HTML scraping`);
  }

  /**
   * HTTP GET that returns raw binary/text data (for CSV downloads).
   * Similar to BaseScraper.httpGet but with longer timeout for large files.
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
          'Accept': 'text/csv,text/plain,application/csv,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 60000, // 60s timeout for large CSV files
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
   * Parse CSV content into attorney objects using csv-parser.
   * Returns a promise that resolves to an array of attorney objects.
   */
  _parseCsvContent(csvContent) {
    return new Promise((resolve, reject) => {
      const attorneys = [];
      const stream = Readable.from([csvContent]);

      stream
        .pipe(csvParser())
        .on('data', (row) => {
          attorneys.push(row);
        })
        .on('end', () => resolve(attorneys))
        .on('error', reject);
    });
  }

  /**
   * Normalize a CSV row into our standard attorney object format.
   * CSV column names vary; this handles common MARS column patterns.
   */
  _normalizeRow(row) {
    // Try common column name patterns (case-insensitive lookup)
    const get = (keys) => {
      for (const key of keys) {
        // Exact match
        if (row[key] !== undefined && row[key] !== null) return String(row[key]).trim();
        // Case-insensitive match
        const found = Object.keys(row).find(k => k.toLowerCase().replace(/[_\s-]/g, '') === key.toLowerCase().replace(/[_\s-]/g, ''));
        if (found && row[found] !== undefined && row[found] !== null) return String(row[found]).trim();
      }
      return '';
    };

    const firstName = get(['FirstName', 'First_Name', 'first_name', 'First Name', 'fname', 'FIRST_NAME']);
    const lastName = get(['LastName', 'Last_Name', 'last_name', 'Last Name', 'lname', 'LAST_NAME']);
    const fullName = get(['FullName', 'Full_Name', 'full_name', 'Name', 'Attorney Name', 'AttorneyName', 'FULL_NAME']);
    const firmName = get(['FirmName', 'Firm_Name', 'firm_name', 'Firm Name', 'Company', 'Firm', 'FIRM_NAME']);
    const city = get(['City', 'city', 'CITY', 'Office City', 'OfficeCity']);
    const state = get(['State', 'state', 'STATE', 'Office State', 'OfficeState']);
    const zip = get(['Zip', 'zip', 'ZIP', 'ZipCode', 'Zip Code', 'PostalCode']);
    const phone = get(['Phone', 'phone', 'PHONE', 'Telephone', 'PhoneNumber', 'Phone Number', 'Office Phone']);
    const email = get(['Email', 'email', 'EMAIL', 'EmailAddress', 'Email Address', 'E-mail']);
    const website = get(['Website', 'website', 'WEBSITE', 'WebAddress', 'Web Address', 'URL']);
    const barNumber = get(['BarNumber', 'Bar_Number', 'bar_number', 'Bar Number', 'License Number', 'LicenseNumber', 'BarNum', 'BARNUMBER', 'AttorneyId', 'Attorney ID']);
    const barStatus = get(['Status', 'status', 'STATUS', 'Bar Status', 'BarStatus', 'LicenseStatus', 'License Status', 'MemberStatus']);
    const admissionDate = get(['AdmissionDate', 'Admission_Date', 'admission_date', 'Admission Date', 'DateAdmitted', 'Date Admitted', 'AdmitDate']);

    // Build first/last from full name if individual parts are missing
    let fName = firstName;
    let lName = lastName;
    let fFullName = fullName;

    if (!fName && !lName && fullName) {
      // Handle "Last, First" format
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

    return {
      first_name: fName,
      last_name: lName,
      full_name: fFullName,
      firm_name: firmName,
      city: city,
      state: state || 'MN',
      zip: zip,
      phone: phone,
      email: email,
      website: website,
      bar_number: barNumber,
      bar_status: barStatus,
      admission_date: admissionDate,
      profile_url: barNumber ? `${this.baseUrl}Attorney/${barNumber}` : '',
      source: `${this.name}_bar`,
    };
  }

  /**
   * Attempt to find the CSV download link from the MARS homepage.
   * Returns the URL string or null if not found.
   */
  async _findCsvDownloadLink(rateLimiter) {
    log.info('Fetching MARS homepage to find CSV download link...');

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`MARS homepage returned status ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Look for CSV/download links in the page
      const downloadPatterns = [
        'a[href*=".csv"]',
        'a[href*="download"]',
        'a[href*="export"]',
        'a[href*="CSV"]',
        'a[href*="spreadsheet"]',
        'a:contains("Download")',
        'a:contains("Export")',
        'a:contains("CSV")',
        'a:contains("Spreadsheet")',
      ];

      for (const selector of downloadPatterns) {
        const link = $(selector).first();
        if (link.length) {
          let href = link.attr('href') || '';
          if (href && !href.includes('javascript:')) {
            if (href.startsWith('/')) {
              href = `${new URL(this.baseUrl).origin}${href}`;
            } else if (!href.startsWith('http')) {
              href = `${this.baseUrl}${href}`;
            }
            log.info(`Found potential CSV download link: ${href}`);
            return href;
          }
        }
      }

      // Also check for links in search-related pages
      const searchLinks = [];
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().toLowerCase();
        if (text.includes('search') || text.includes('find') || text.includes('attorney') || text.includes('lawyer')) {
          if (href.startsWith('/')) {
            searchLinks.push(`${new URL(this.baseUrl).origin}${href}`);
          } else if (href.startsWith('http')) {
            searchLinks.push(href);
          }
        }
      });

      // Try search pages for CSV links
      for (const searchUrl of searchLinks.slice(0, 3)) {
        try {
          await rateLimiter.wait();
          const searchResp = await this.httpGet(searchUrl, rateLimiter);
          if (searchResp.statusCode === 200) {
            const $s = cheerio.load(searchResp.body);
            for (const selector of downloadPatterns) {
              const link = $s(selector).first();
              if (link.length) {
                let href = link.attr('href') || '';
                if (href && !href.includes('javascript:')) {
                  if (href.startsWith('/')) {
                    href = `${new URL(this.baseUrl).origin}${href}`;
                  } else if (!href.startsWith('http')) {
                    href = `${this.baseUrl}${href}`;
                  }
                  log.info(`Found CSV download link on search page: ${href}`);
                  return href;
                }
              }
            }
          }
        } catch (err) {
          log.info(`Could not check search page ${searchUrl}: ${err.message}`);
        }
      }

      return null;
    } catch (err) {
      log.warn(`Failed to fetch MARS homepage: ${err.message}`);
      return null;
    }
  }

  /**
   * Try known CSV download paths directly.
   * Returns the CSV content as a string, or null if none work.
   */
  async _tryCsvDownloadPaths(rateLimiter) {
    const origin = new URL(this.baseUrl).origin;

    for (const csvPath of this.csvDownloadPaths) {
      const url = `${origin}${csvPath}`;
      log.info(`Trying CSV download: ${url}`);

      try {
        await rateLimiter.wait();
        const response = await this._httpGetRaw(url, rateLimiter);

        if (response.statusCode === 200) {
          const ct = response.contentType.toLowerCase();
          const bodyStart = response.body.substring(0, 500).toLowerCase();

          // Check if the response is actually CSV content
          if (ct.includes('csv') || ct.includes('text/plain') || ct.includes('octet-stream') ||
              bodyStart.includes(',') && (bodyStart.includes('name') || bodyStart.includes('attorney') || bodyStart.includes('bar'))) {
            // Verify it looks like CSV (has commas and multiple lines)
            const lines = response.body.split('\n');
            if (lines.length > 10 && lines[0].includes(',')) {
              log.success(`CSV download successful from ${url} — ${lines.length} lines`);
              return response.body;
            }
          }
        }
      } catch (err) {
        log.info(`CSV path ${csvPath} failed: ${err.message}`);
      }
    }

    return null;
  }

  /**
   * Fallback: parse the HTML search form results page.
   * Used when CSV download is not available.
   */
  _parseHtmlResults($) {
    const attorneys = [];

    // Common patterns for attorney listing pages
    const selectors = [
      'table tbody tr',
      '.attorney-result',
      '.search-result',
      '.member-result',
      'div.result',
      'li.result',
    ];

    // Try table-based results first
    $('table tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 2) return;

      const texts = [];
      cells.each((__, cell) => { texts.push($(cell).text().trim()); });

      // Try to identify columns by header or position
      const nameLink = $(row).find('a').first();
      const fullName = nameLink.length ? nameLink.text().trim() : (texts[0] || '');
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || fullName.length < 2) return;

      let fName = '';
      let lName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',');
        lName = parts[0].trim();
        fName = (parts[1] || '').trim().split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        fName = split.firstName;
        lName = split.lastName;
      }

      attorneys.push({
        first_name: fName,
        last_name: lName,
        full_name: fullName.includes(',') ? `${fName} ${lName}` : fullName,
        firm_name: texts[1] || '',
        city: texts[2] || '',
        state: 'MN',
        phone: '',
        email: '',
        website: '',
        bar_number: (texts[3] || '').match(/\d+/) ? texts[3] : '',
        bar_status: texts[4] || '',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `${new URL(this.baseUrl).origin}${profileUrl}` : ''),
        source: `${this.name}_bar`,
      });
    });

    return attorneys;
  }

  /**
   * Build the search URL for the HTML fallback mode.
   */
  _buildFallbackSearchUrl(city, page) {
    const params = new URLSearchParams();
    params.set('city', city);
    params.set('state', 'MN');
    params.set('status', 'Active');
    if (page > 1) params.set('page', String(page));
    return `${this.baseUrl}Search?${params.toString()}`;
  }

  /**
   * Async generator that yields attorney records.
   *
   * Strategy:
   *  1. Try to find and download a CSV file from the MARS site
   *  2. If CSV is available, parse it and yield filtered results
   *  3. If CSV is not available, fall back to HTML search form scraping
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const citySet = new Set(cities.map(c => c.toLowerCase()));

    // --- Attempt CSV download ---
    log.info('Attempting CSV download from MARS...');

    let csvContent = null;

    // Step 1: Try to find CSV link from the homepage
    const csvLink = await this._findCsvDownloadLink(rateLimiter);
    if (csvLink) {
      try {
        await rateLimiter.wait();
        const csvResp = await this._httpGetRaw(csvLink, rateLimiter);
        if (csvResp.statusCode === 200 && csvResp.body.length > 100) {
          const lines = csvResp.body.split('\n');
          if (lines.length > 10 && lines[0].includes(',')) {
            csvContent = csvResp.body;
            log.success(`Downloaded CSV from discovered link — ${lines.length} lines`);
          }
        }
      } catch (err) {
        log.warn(`CSV download from discovered link failed: ${err.message}`);
      }
    }

    // Step 2: Try known CSV paths
    if (!csvContent) {
      csvContent = await this._tryCsvDownloadPaths(rateLimiter);
    }

    // --- CSV path: parse and yield ---
    if (csvContent) {
      log.success('CSV download available — parsing entire attorney list');

      let rows;
      try {
        rows = await this._parseCsvContent(csvContent);
      } catch (err) {
        log.error(`Failed to parse CSV: ${err.message}`);
        rows = null;
      }

      if (rows && rows.length > 0) {
        log.success(`Parsed ${rows.length} attorney records from CSV`);

        // Report overall progress
        yield { _cityProgress: { current: 1, total: cities.length } };

        let yieldCount = 0;
        for (const row of rows) {
          const attorney = this._normalizeRow(row);

          // Filter by target cities
          if (attorney.city && !citySet.has(attorney.city.toLowerCase())) {
            continue;
          }

          // Filter by practice area if specified (keyword match in any text field)
          if (practiceArea) {
            const practiceCode = this.resolvePracticeCode(practiceArea);
            const searchTerm = (practiceCode || practiceArea).toLowerCase();
            const allText = Object.values(row).join(' ').toLowerCase();
            // CSV may not have practice area columns, so skip this filter
            // if there's no obvious match field — yield all for target cities
          }

          // Filter by admission year
          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
          yieldCount++;

          // Respect maxPages as a rough limit (pageSize * maxPages records)
          if (options.maxPages && yieldCount >= options.maxPages * this.pageSize) {
            log.info(`Reached max records limit (${options.maxPages * this.pageSize}) from CSV`);
            return;
          }
        }

        log.success(`Yielded ${yieldCount} attorneys from CSV for target cities`);
        return;
      }
    }

    // --- Fallback: HTML search form ---
    log.warn('CSV download not available — falling back to HTML search form');

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this._buildFallbackSearchUrl(city, page);
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} for ${city} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this._parseHtmlResults($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping for ${city}`);
            break;
          }
          // If first page is empty, no results at all
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        if (page === 1) {
          log.success(`Found results for ${city} — scraping pages`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page link
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === '>>';
        }).length > 0;

        if (!hasNext || attorneys.length < this.pageSize) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new MinnesotaScraper();
