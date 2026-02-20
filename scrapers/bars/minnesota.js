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
const zlib = require('zlib');
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

    // Known download URLs for the MARS attorney list
    this.csvDownloadUrls = [
      'https://mars.courts.state.mn.us/marstxt.zip',
      'https://mars.courts.state.mn.us/marstxt.txt',
      'https://mars.courts.state.mn.us/marstxt.csv',
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
   * HTTP GET that returns raw binary data (for ZIP/CSV downloads).
   * Returns a Buffer body for binary-safe handling.
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
          'Accept': '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 60000, // 60s timeout for large files
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

        const chunks = [];
        res.on('data', chunk => { chunks.push(chunk); });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: Buffer.concat(chunks),
          contentType: res.headers['content-type'] || '',
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Extract a file from a ZIP buffer.
   * Implements minimal ZIP parsing (local file header) without external dependencies.
   * Returns the decompressed content as a string.
   */
  _extractFromZip(zipBuffer) {
    // ZIP local file header signature: 0x04034b50
    if (zipBuffer.length < 30 || zipBuffer.readUInt32LE(0) !== 0x04034b50) {
      throw new Error('Not a valid ZIP file');
    }

    const compressionMethod = zipBuffer.readUInt16LE(8);
    const compressedSize = zipBuffer.readUInt32LE(18);
    const fileNameLength = zipBuffer.readUInt16LE(26);
    const extraFieldLength = zipBuffer.readUInt16LE(28);
    const dataOffset = 30 + fileNameLength + extraFieldLength;
    const compressedData = zipBuffer.slice(dataOffset, dataOffset + compressedSize);

    let rawBuffer;
    if (compressionMethod === 0) {
      // Stored (no compression)
      rawBuffer = compressedData;
    } else if (compressionMethod === 8) {
      // Deflated — use raw inflate (no header)
      rawBuffer = zlib.inflateRawSync(compressedData);
    } else {
      throw new Error(`Unsupported ZIP compression method: ${compressionMethod}`);
    }

    // Detect UTF-16LE BOM (0xFF 0xFE) and convert to UTF-8
    if (rawBuffer.length >= 2 && rawBuffer[0] === 0xFF && rawBuffer[1] === 0xFE) {
      return rawBuffer.slice(2).toString('utf16le');
    }

    return rawBuffer.toString('utf-8');
  }

  /**
   * Parse CSV content into attorney objects using csv-parser.
   * Returns a promise that resolves to an array of attorney objects.
   */
  _parseCsvContent(csvContent) {
    return new Promise((resolve, reject) => {
      const attorneys = [];
      const stream = Readable.from([csvContent]);

      // MARS CSV has no header row — columns are positional:
      //   0: LastName, 1: FirstName, 2: MiddleName, 3: (unused),
      //   4: City, 5: State, 6: Zip, 7: Active, 8: Insured,
      //   9: InsuranceCompany, 10: (unknown bool)
      const headers = [
        'LastName', 'FirstName', 'MiddleName', '_unused',
        'City', 'State', 'Zip', 'Active', 'Insured',
        'InsuranceCompany', '_flag'
      ];

      stream
        .pipe(csvParser({ headers, skipLines: 0 }))
        .on('data', (row) => {
          attorneys.push(row);
        })
        .on('end', () => resolve(attorneys))
        .on('error', reject);
    });
  }

  /**
   * Normalize a CSV row into our standard attorney object format.
   * Uses the explicit MARS column names assigned in _parseCsvContent().
   */
  _normalizeRow(row) {
    const val = (key) => (row[key] !== undefined && row[key] !== null) ? String(row[key]).trim() : '';

    const firstName = this._titleCase(val('FirstName'));
    const lastName = this._titleCase(val('LastName'));
    const middleName = val('MiddleName');
    const city = this._titleCase(val('City'));
    const state = val('State') || 'MN';
    const zip = val('Zip');
    const isActive = val('Active') === 'true';
    const insurer = val('InsuranceCompany');

    // Build full name
    const fullName = middleName
      ? `${firstName} ${this._titleCase(middleName)} ${lastName}`
      : `${firstName} ${lastName}`;

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName.trim(),
      firm_name: (insurer && insurer !== 'NA') ? insurer : '',
      city: city,
      state: state,
      zip: zip,
      phone: '',
      email: '',
      website: '',
      bar_number: '',
      bar_status: isActive ? 'Active' : 'Inactive',
      admission_date: '',
      profile_url: '',
      source: `${this.name}_bar`,
    };
  }

  /**
   * Title-case a string (e.g. "MINNEAPOLIS" -> "Minneapolis").
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  /**
   * Try known MARS download URLs (ZIP then plain text).
   * Returns the CSV/text content as a string, or null if none work.
   */
  async _downloadMarsData(rateLimiter) {
    for (const url of this.csvDownloadUrls) {
      log.info(`Trying MARS download: ${url}`);

      try {
        await rateLimiter.wait();
        const response = await this._httpGetRaw(url, rateLimiter);

        if (response.statusCode !== 200) {
          log.info(`${url} returned status ${response.statusCode}`);
          continue;
        }

        const isZip = url.endsWith('.zip') ||
          (response.body.length >= 4 && response.body.readUInt32LE(0) === 0x04034b50);

        if (isZip) {
          try {
            const csvContent = this._extractFromZip(response.body);
            const lines = csvContent.split('\n');
            if (lines.length > 10) {
              log.success(`Extracted CSV from ZIP (${url}) — ${lines.length} lines`);
              return csvContent;
            }
          } catch (zipErr) {
            log.warn(`Failed to extract ZIP from ${url}: ${zipErr.message}`);
            continue;
          }
        } else {
          // Plain text/CSV file
          const csvContent = response.body.toString('utf-8');
          const lines = csvContent.split('\n');
          if (lines.length > 10 && lines[0].includes(',')) {
            log.success(`CSV download successful from ${url} — ${lines.length} lines`);
            return csvContent;
          }
        }
      } catch (err) {
        log.info(`Download from ${url} failed: ${err.message}`);
      }
    }

    return null;
  }

  /**
   * Parse a Minnesota attorney profile page for additional contact info.
   *
   * Used by the waterfall pipeline when profile_url is available (typically
   * from the HTML fallback search results, not the CSV download path).
   * Profile pages may contain: phone, email, firm name, address, and
   * admission details.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields extracted from the profile
   */
  parseProfilePage($) {
    const result = {};
    const bodyText = $('body').text();

    // Phone — look for tel: links first, then labeled patterns
    const telLink = $('a[href^="tel:"]').first();
    if (telLink.length) {
      result.phone = telLink.attr('href').replace('tel:', '').trim();
    } else {
      const phoneMatch = bodyText.match(/(?:Phone|Telephone|Office|Work|Business)[:\s]*([\d().\s-]+)/i) ||
                         bodyText.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
      if (phoneMatch) {
        result.phone = phoneMatch[1].trim();
      }
    }

    // Email — look for mailto: links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
    } else {
      const emailMatch = bodyText.match(/(?:Email|E-mail)[:\s]+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i);
      if (emailMatch) {
        result.email = emailMatch[1].toLowerCase();
      }
    }

    // Website — external links that aren't MN courts or excluded domains
    const mnExcluded = [
      'mars.courts.state.mn.us', 'courts.state.mn.us', 'mncourts.gov',
      'mnbar.org', 'lprb.mncourts.gov',
    ];
    const isExcluded = (href) =>
      this.isExcludedDomain(href) || mnExcluded.some(d => href.includes(d));

    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (!isExcluded(href)) {
        result.website = href;
        return false; // break
      }
    });

    // Firm name / employer
    const firmMatch = bodyText.match(/(?:Firm|Employer|Company|Organization)[:\s]+(.+?)(?:\n|$)/i);
    if (firmMatch) {
      const firm = firmMatch[1].trim();
      if (firm && firm.length > 1 && firm.length < 200) {
        result.firm_name = firm;
      }
    }

    // Address
    const addrMatch = bodyText.match(/(?:Address|Location)[:\s]+(.+?)(?:\n\n|\nPhone|\nEmail|\nFirm|$)/is);
    if (addrMatch) {
      result.address = addrMatch[1].trim().replace(/\s+/g, ' ');
    }

    // Admission date
    const admitMatch = bodyText.match(/(?:Admit(?:ted|ssion)\s*(?:Date)?)[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4}|\d{4})/i);
    if (admitMatch) {
      result.admission_date = admitMatch[1].trim();
    }

    // Bar status
    const statusMatch = bodyText.match(/(?:Status|Standing|Registration)[:\s]+(Active|Inactive|Suspended|Retired|Resigned|Deceased|Disbarred)/i);
    if (statusMatch) {
      result.bar_status = statusMatch[1].trim();
    }

    // Remove empty string values before returning
    for (const key of Object.keys(result)) {
      if (result[key] === '' || result[key] === undefined || result[key] === null) {
        delete result[key];
      }
    }

    return result;
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
   *  1. Download the MARS ZIP/CSV file from mars.courts.state.mn.us
   *  2. Parse the comma-delimited data and yield filtered results
   *  3. If download fails, fall back to HTML search form scraping
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const citySet = new Set(cities.map(c => c.toLowerCase()));

    // --- Attempt MARS data download (ZIP or plain CSV) ---
    log.info('Attempting MARS data download from mars.courts.state.mn.us...');

    const csvContent = await this._downloadMarsData(rateLimiter);

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
