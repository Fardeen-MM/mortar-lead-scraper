/**
 * North Carolina General Contractors (NCLBGC) — Licensing Board Scraper
 *
 * Source: https://portal.nclbgc.org/Public/Search
 * Method: ASP.NET MVC + jQuery AJAX (two-step: search list → detail fetch)
 *
 * Step 1: POST /Public/_Search/ with form data (classification, city, last name, etc.)
 *         Returns HTML table with all matching accounts (jQuery DataTables, client-side pagination)
 *         Each row has: License/Qualifier #, Type (License|Qualifier), Owner name, detail key
 *
 * Step 2: GET /Public/_ShowAccountDetails/?key=XXX for each account
 *         Returns HTML with: Name, Address, Phone, License #, Status, Issue/Expiry dates,
 *         License Limitation, Active Classifications
 *
 * Classification types (30 options):
 *   27: Building, 28: Residential, 29: Highway, 30: Public Utilities,
 *   31: H (Grading & Excavating), 32-40: PU subtypes, 41-52: S (Specialty) subtypes,
 *   53: S (Asbestos), 54: Accredited Builder, 55: Accredited Master Builder,
 *   56: S (Sign/Billboard), 26: Unclassified
 *
 * Strategy: Iterate NC cities × classification types, POST search, parse result table,
 * then batch-fetch detail pages for phone/address/status enrichment.
 * The search endpoint returns ALL matching rows at once (no server-side pagination).
 *
 * No CSRF tokens or ViewState needed — plain form POST with jQuery serialization.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Classification dropdown value → name mapping (from the <select> element)
const CLASSIFICATIONS = {
  '54': 'Accredited Builder',
  '55': 'Accredited Master Builder',
  '27': 'Building',
  '31': 'H (Grading & Excavating)',
  '29': 'Highway',
  '32': 'PU (Communications)',
  '34': 'PU (Fuel Distribution)',
  '35': 'PU (Sewage Disposal)',
  '36': 'PU (Sewer Lines)',
  '37': 'PU (Water Lines)',
  '33': 'PU (Electrical-Ahead of P.O.D.)',
  '38': 'PU (Water Lines & Sewer Lines)',
  '40': 'PU (Water Pur. & Sewage Disp.)',
  '39': 'PU (Water Purification)',
  '30': 'Public Utilities',
  '28': 'Residential',
  '53': 'S (Asbestos)',
  '41': 'S (Boring & Tunneling)',
  '42': 'S (Concrete Construction)',
  '43': 'S (Insulation)',
  '44': 'S (Interior Construction)',
  '45': 'S (Marine Construction)',
  '46': 'S (Masonry Construction)',
  '47': 'S (Metal Erection)',
  '48': 'S (Railroad Construction)',
  '49': 'S (Roofing)',
  '50': 'S (Sidewalk Curb & Gutter)',
  '56': 'S (Sign/Billboard)',
  '51': 'S (Swimming Pools)',
  '52': 'S (Wind Turbine)',
  '26': 'Unclassified',
};

// Practice area → classification ID(s) mapping for user-friendly search
const PRACTICE_AREA_CODES = {
  'building': '27',
  'residential': '28',
  'highway': '29',
  'public utilities': '30',
  'grading': '31',
  'excavating': '31',
  'roofing': '49',
  'concrete': '42',
  'masonry': '46',
  'insulation': '43',
  'interior': '44',
  'swimming pools': '51',
  'pools': '51',
  'metal erection': '47',
  'asbestos': '53',
  'electrical': '33',
  'communications': '32',
  'plumbing': '36',
  'sewer': '36',
  'water': '37',
  'general contractor': '27',
  'accredited builder': '54',
};

// Major NC cities for iteration
const DEFAULT_CITIES = [
  'Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem',
  'Fayetteville', 'Cary', 'Wilmington', 'High Point', 'Asheville',
  'Concord', 'Gastonia', 'Jacksonville', 'Chapel Hill', 'Huntersville',
  'Apex', 'Wake Forest', 'Mooresville', 'Hickory', 'Burlington',
  'Kannapolis', 'Goldsboro', 'Monroe', 'Sanford', 'New Bern',
];

// Default classification IDs to search when no practice area specified
// These are the most common/valuable contractor types
const DEFAULT_CLASSIFICATION_IDS = [
  '27',  // Building
  '28',  // Residential
  '49',  // S (Roofing)
  '42',  // S (Concrete Construction)
  '44',  // S (Interior Construction)
  '46',  // S (Masonry Construction)
];


class NcGcScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nc_gc',
      stateCode: 'NC-GC',
      baseUrl: 'https://portal.nclbgc.org',
      pageSize: 500, // No server-side pagination — all results returned at once
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() { throw new Error('nc_gc: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('nc_gc: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('nc_gc: extractResultCount() not used — search() overridden'); }

  /**
   * Override transformResult for contractor-specific source name.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'nc_gc';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTP POST with form data.
   * Returns { statusCode, body }.
   */
  _httpPost(url, formData, rateLimiter) {
    const https = require('https');
    const querystring = require('querystring');
    const postData = querystring.stringify(formData);

    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const urlObj = new URL(url);

      const options = {
        hostname: urlObj.hostname,
        port: 443,
        path: urlObj.pathname + urlObj.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'Accept': 'text/html, */*; q=0.01',
          'X-Requested-With': 'XMLHttpRequest',
          'Referer': 'https://portal.nclbgc.org/Public/Search',
          'Origin': 'https://portal.nclbgc.org',
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('POST request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Search the NCLBGC portal for contractors.
   * POST /Public/_Search/ returns an HTML table with all matching accounts.
   *
   * @param {object} params - Search parameters
   * @param {string} [params.city] - City filter
   * @param {string} [params.classificationId] - Classification dropdown value
   * @param {string} [params.lastName] - Last name filter
   * @param {string} [params.companyName] - Company name filter
   * @param {RateLimiter} rateLimiter
   * @returns {Array<{key: string, licenseNumber: string, type: string, owner: string, isActive: boolean}>}
   */
  async _searchAccounts(params, rateLimiter) {
    const formData = {
      ClassificationDefinitionIdnt: params.classificationId || '',
      AccountNumber: '',
      QualifierAccountNumber: '',
      CompanyName: params.companyName || '',
      FirstName: '',
      LastName: params.lastName || '',
      PhoneNumber: '',
      useSoundex: 'false',
      streetAddress: '',
      PostalCode: '',
      City: params.city || '',
      StateCode: 'NC',
    };

    const url = `${this.baseUrl}/Public/_Search/`;

    await rateLimiter.wait();
    const response = await this._httpPost(url, formData, rateLimiter);

    if (response.statusCode !== 200) {
      log.warn(`NC-GC search returned ${response.statusCode}`);
      return [];
    }

    if (this.detectCaptcha(response.body)) {
      log.warn('NC-GC: CAPTCHA detected on search');
      return [];
    }

    // Parse the results table
    const $ = cheerio.load(response.body);
    const accounts = [];

    $('table#AccountSearchTable tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 3) return;

      // Cell 0: License number + link with detail key + optional "Not Active" span
      const cell0 = $(cells[0]);
      const link = cell0.find('a[onclick*="ShowAccountDetails"]');
      if (!link.length) return;

      // Extract the encrypted key from onclick="ShowAccountDetails('KEY')"
      const onclick = link.attr('onclick') || '';
      const keyMatch = onclick.match(/ShowAccountDetails\(\s*'([^']+)'\s*\)/);
      if (!keyMatch) return;
      const key = keyMatch[1];

      const licenseNumber = link.text().trim();
      const isActive = !cell0.find('span').text().includes('Not Active');

      // Cell 1: Type (License or Qualifier)
      const type = $(cells[1]).text().trim();

      // Cell 2: Owner name (may contain AKA and line breaks)
      const ownerHtml = $(cells[2]).html() || '';
      const owner = $(cells[2]).text().trim().split('\n')[0].trim();

      accounts.push({ key, licenseNumber, type, owner, isActive });
    });

    return accounts;
  }

  /**
   * Fetch account detail page for a given key.
   * GET /Public/_ShowAccountDetails/?key=XXX
   *
   * Returns parsed fields: name, address, city, state, zip, phone,
   * licenseNumber, status, issuedDate, expirationDate, limitation, classifications.
   */
  async _fetchAccountDetail(key, rateLimiter) {
    const url = `${this.baseUrl}/Public/_ShowAccountDetails/?key=${key}&Source=Search`;

    await rateLimiter.wait();
    const response = await this.httpGet(url, rateLimiter);

    if (response.statusCode !== 200) {
      log.warn(`NC-GC detail returned ${response.statusCode} for key ${key}`);
      return null;
    }

    const $ = cheerio.load(response.body);
    const result = {};

    // Parse label/value pairs from display-label / display-field divs
    const fields = {};
    $('div.display-label').each((_, el) => {
      const label = $(el).text().trim();
      const valueEl = $(el).next('div.display-field');
      if (valueEl.length) {
        // Get HTML for address (has <br> tags) and text for everything else
        fields[label] = {
          text: valueEl.text().trim(),
          html: valueEl.html() || '',
        };
      }
    });

    // Name
    result.name = (fields['Name']?.text || '').trim();

    // Address — split on <br> to get street, city/state/zip
    if (fields['Address']) {
      const addressHtml = fields['Address'].html;
      const addressParts = addressHtml.split('<br>').map(p =>
        p.replace(/<[^>]*>/g, '').trim()
      ).filter(Boolean);

      if (addressParts.length >= 2) {
        result.address = addressParts[0];
        // Last part is typically "City, ST  ZIP"
        const cityStateZip = addressParts[addressParts.length - 1];
        const cszMatch = cityStateZip.match(/^(.+?),\s*([A-Z]{2})\s+([\d-]+)$/);
        if (cszMatch) {
          result.city = cszMatch[1].trim();
          result.state = cszMatch[2];
          result.zip = cszMatch[3];
        } else {
          // Try without zip
          const csMatch = cityStateZip.match(/^(.+?),\s*([A-Z]{2})/);
          if (csMatch) {
            result.city = csMatch[1].trim();
            result.state = csMatch[2];
          }
          result.zip = '';
        }
        // If there are 3+ parts, middle ones are address line 2
        if (addressParts.length > 2) {
          result.address = addressParts.slice(0, -1).join(', ');
        }
      } else if (addressParts.length === 1) {
        result.address = addressParts[0];
      }
    }

    // Phone — comes as raw digits like "9198510210"
    const rawPhone = (fields['Phone']?.text || '').trim();
    if (rawPhone) {
      const digits = rawPhone.replace(/[^\d]/g, '');
      if (digits.length === 10) {
        result.phone = `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
      } else if (digits.length === 11 && digits[0] === '1') {
        result.phone = `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
      } else if (digits.length >= 7) {
        result.phone = rawPhone;
      }
    }

    // License fields
    result.licenseNumber = (fields['License #']?.text || '').trim();
    result.accountType = (fields['Account Type']?.text || '').trim();
    result.issuedDate = (fields['First Issued Date']?.text || '').trim();
    result.expirationDate = (fields['Expiration Date']?.text || '').trim();
    result.limitation = (fields['License Limitation']?.text || '').trim();

    // Status — text includes the red span text, extract just the status word
    const statusText = (fields['Status']?.text || '').trim();
    // Status field may contain "Active" or "Archived - License Not Valid" etc.
    result.status = statusText.replace(/\s*-\s*License Not Valid.*$/, '').trim();
    result.isValid = !statusText.includes('Not Valid');

    // Active Classifications
    const classifications = [];
    $('fieldset:contains("Active Classifications") div.display-field').each((_, el) => {
      const cls = $(el).text().trim();
      if (cls) classifications.push(cls);
    });
    result.classifications = classifications;

    return result;
  }

  /**
   * Split a name like "John Thomas Coston" or "ABC Construction Co., Inc." into
   * first_name/last_name for individuals, or firm_name for companies.
   *
   * Heuristic: if the name contains LLC, Inc, Corp, Co., Construction, Builders,
   * Roofing, Plumbing, etc. — treat as a company name.
   */
  _parseName(name) {
    if (!name) return { first_name: '', last_name: '', firm_name: '' };

    const companyIndicators = [
      /\bllc\b/i, /\binc\.?\b/i, /\bcorp\.?\b/i, /\bco\.?\b/i,
      /\bconstruction\b/i, /\bbuilders?\b/i, /\broofing\b/i,
      /\bplumbing\b/i, /\belectric(al)?\b/i, /\bcontracting\b/i,
      /\bservices?\b/i, /\benterprises?\b/i, /\bgroup\b/i,
      /\bassociates?\b/i, /\bcompany\b/i, /\bpartners?\b/i,
      /\bt\/a\b/i, /\bd\/b\/a\b/i, /\baka:\b/i,
      /\bpllc\b/i, /\bltd\.?\b/i, /\blp\b/i,
      /\bindustries\b/i, /\bsolutions\b/i, /\bpro(s)?\b/i,
    ];

    const isCompany = companyIndicators.some(re => re.test(name));

    if (isCompany) {
      // Clean up AKA lines
      const cleanName = name.split(/\bAKA:/i)[0].trim();
      return { first_name: '', last_name: '', firm_name: cleanName };
    }

    // Individual name — split into first/last
    const { firstName, lastName } = this.splitName(name);
    return { first_name: firstName, last_name: lastName, firm_name: '' };
  }

  /**
   * Main search generator.
   *
   * Strategy: For each city, search with the requested classification (or default set).
   * The search returns a flat list of accounts. For each account, fetch the detail page
   * to get phone, address, and license status.
   *
   * Rate limiting is critical — each detail fetch is a separate HTTP request.
   * In test mode (maxPages=2), we limit to 2 cities and 20 detail fetches per city.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape(`NC General Contractors Search — ${practiceArea || 'all trades'}`);

    // Determine which classification IDs to search
    let classificationIds;
    if (practiceCode) {
      classificationIds = [practiceCode];
      log.info(`Classification filter: ${CLASSIFICATIONS[practiceCode] || practiceCode}`);
    } else {
      classificationIds = DEFAULT_CLASSIFICATION_IDS;
      log.info(`No practice area filter — searching ${classificationIds.length} default classifications`);
    }

    const cities = this.getCities(options);
    const maxDetailFetches = options.maxPages ? options.maxPages * 20 : Infinity;

    // Track dedup across cities (license numbers seen)
    const seenLicenses = new Set();
    let totalYielded = 0;
    let totalDetailsFetched = 0;
    let totalWithPhone = 0;
    let totalSkippedInactive = 0;
    let totalSkippedDups = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching ${city}, NC — ${classificationIds.length} classification(s)`);

      let cityDetailCount = 0;

      for (const classId of classificationIds) {
        const className = CLASSIFICATIONS[classId] || classId;
        log.info(`  ${className} in ${city}...`);

        let accounts;
        try {
          accounts = await this._searchAccounts({ city, classificationId: classId }, rateLimiter);
        } catch (err) {
          log.error(`Search failed for ${className} in ${city}: ${err.message}`);
          // Check for rate limiting
          if (err.message.includes('429') || err.message.includes('403')) {
            const shouldRetry = await rateLimiter.handleBlock(429);
            if (!shouldRetry) break;
          }
          continue;
        }

        if (!accounts.length) {
          log.info(`  No results for ${className} in ${city}`);
          continue;
        }

        log.info(`  Found ${accounts.length} accounts for ${className} in ${city}`);

        // Filter to License type only (skip Qualifier entries — those are individuals,
        // not the licensed entity). We fetch details for License accounts.
        const licenseAccounts = accounts.filter(a => a.type === 'License');
        // Also skip accounts marked "Not Active" in the search results to avoid
        // wasting detail page fetches on archived/inactive licenses
        const activeAccounts = licenseAccounts.filter(a => a.isActive);
        const inactiveInList = licenseAccounts.length - activeAccounts.length;
        log.info(`  ${activeAccounts.length} active license accounts (${accounts.length - licenseAccounts.length} qualifiers, ${inactiveInList} inactive skipped)`);

        for (const account of activeAccounts) {
          // Dedup by license number
          if (seenLicenses.has(account.licenseNumber)) {
            totalSkippedDups++;
            continue;
          }
          seenLicenses.add(account.licenseNumber);

          // Check detail fetch limit (test mode)
          if (totalDetailsFetched >= maxDetailFetches) {
            log.info(`Reached max detail fetches (${maxDetailFetches})`);
            break;
          }

          // Fetch detail page for full data
          let detail;
          try {
            detail = await this._fetchAccountDetail(account.key, rateLimiter);
            totalDetailsFetched++;
            cityDetailCount++;
          } catch (err) {
            log.warn(`Detail fetch failed for ${account.licenseNumber}: ${err.message}`);
            if (err.message.includes('429') || err.message.includes('403')) {
              const shouldRetry = await rateLimiter.handleBlock(429);
              if (!shouldRetry) break;
            }
            continue;
          }

          if (!detail) continue;

          // Skip invalid/archived licenses unless they have useful data
          if (!detail.isValid) {
            totalSkippedInactive++;
            continue;
          }

          // Parse the name into first/last or firm
          const parsed = this._parseName(detail.name || account.owner);

          // Build the lead object
          const lead = {
            first_name: parsed.first_name,
            last_name: parsed.last_name,
            firm_name: parsed.firm_name,
            address: detail.address || '',
            city: detail.city || city,
            state: detail.state || 'NC',
            zip: detail.zip || '',
            phone: detail.phone || '',
            email: '',
            website: '',
            bar_number: detail.licenseNumber || account.licenseNumber,
            bar_status: detail.status || (account.isActive ? 'Active' : 'Inactive'),
            admission_date: detail.issuedDate || '',
            license_type: detail.classifications.join(', ') || '',
            license_limitation: detail.limitation || '',
            expiration_date: detail.expirationDate || '',
            profile_url: `${this.baseUrl}/Public/_ShowAccountDetails/?key=${encodeURIComponent(account.key)}&Source=Search`,
          };

          if (lead.phone) totalWithPhone++;
          totalYielded++;

          yield this.transformResult(lead, practiceArea || detail.classifications[0] || '');
        }

        // Break out of classification loop if we hit the detail limit
        if (totalDetailsFetched >= maxDetailFetches) break;
      }

      if (cityDetailCount > 0) {
        log.info(`  ${city}: fetched ${cityDetailCount} details`);
      }

      // Break out of city loop if we hit the detail limit
      if (totalDetailsFetched >= maxDetailFetches) break;
    }

    log.success([
      `NC-GC scrape complete:`,
      `${totalDetailsFetched} details fetched`,
      `${totalYielded} yielded`,
      `${totalWithPhone} with phone`,
      `${totalSkippedInactive} inactive skipped`,
      `${totalSkippedDups} duplicates skipped`,
    ].join(' | '));
  }
}

module.exports = new NcGcScraper();
