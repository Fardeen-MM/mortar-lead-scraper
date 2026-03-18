/**
 * Louisiana State Licensing Board for Contractors (LSLBC) — Contractor Scraper
 *
 * Source: https://arlspublic.lslbc.louisiana.gov/
 * Records: ~20K+ licensed contractors across 64 Louisiana parishes
 * Method: HTML form POST (ByCounty) + AJAX detail fetch per result
 *
 * How it works:
 *   1. Iterate through 64 Louisiana parishes via POST to /Public/_DetailedSearch/
 *      (SearchType=ByCounty, LocationCounty={parishCode})
 *   2. Parse the HTML table of results (business name, city, state, contactKey)
 *   3. For each result, fetch detail page: /Public/_DisplayOnlineDetails/?key={contactKey}
 *   4. Parse detail HTML for: name, address, phone, email, license#, type, status,
 *      effective/expiration dates, classifications, qualifying party
 *
 * Result cap: The search API returns max 1500 results per query.
 *   - Most parishes are well under 1500
 *   - For large parishes (East Baton Rouge, Jefferson, Orleans, St Tammany, etc.)
 *     the scraper falls back to ByName search with last name prefix (A-Z)
 *     combined with the parish filter to stay under 1500
 *
 * Fields returned:
 *   - firm_name (business/contractor name)
 *   - first_name, last_name (from qualifying party or name split)
 *   - address, city, state, zip
 *   - phone, email
 *   - bar_number (license number, e.g., "CL.48336")
 *   - bar_status (Active/Inactive)
 *   - license_type (Commercial/Residential/Mold Remediation/Home Improvement)
 *   - classification (e.g., ELECTRICAL, BUILDING CONSTRUCTION, PLUMBING)
 *   - qualifying_party (name of the qualifying individual)
 *   - effective_date, expiration_date, first_issued
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://arlspublic.lslbc.louisiana.gov';
const SEARCH_URL = `${BASE_URL}/Public/_DetailedSearch/`;
const DETAIL_URL = `${BASE_URL}/Public/_DisplayOnlineDetails/`;

// Max results the LSLBC search returns before truncation
const MAX_RESULTS = 1500;

// All 64 Louisiana parishes with their LSLBC internal codes
// Extracted from the ByCounty search page checkbox values
const PARISHES = [
  { code: '2821', name: 'Acadia' },
  { code: '915', name: 'Allen' },
  { code: '2636', name: 'Ascension' },
  { code: '702', name: 'Assumption' },
  { code: '382', name: 'Avoyelles' },
  { code: '3338', name: 'Beauregard' },
  { code: '1549', name: 'Bienville' },
  { code: '1815', name: 'Bossier' },
  { code: '2098', name: 'Caddo' },
  { code: '1261', name: 'Calcasieu' },
  { code: '3281', name: 'Caldwell' },
  { code: '1372', name: 'Cameron' },
  { code: '584', name: 'Catahoula' },
  { code: '625', name: 'Claiborne' },
  { code: '1813', name: 'Concordia' },
  { code: '1422', name: 'De Soto' },
  { code: '1940', name: 'East Baton Rouge' },
  { code: '342', name: 'East Carroll' },
  { code: '3247', name: 'East Feliciana' },
  { code: '2402', name: 'Evangeline' },
  { code: '1970', name: 'Franklin' },
  { code: '2424', name: 'Grant' },
  { code: '3007', name: 'Iberia' },
  { code: '1808', name: 'Iberville' },
  { code: '2661', name: 'Jackson' },
  { code: '1212', name: 'Jefferson' },
  { code: '2455', name: 'Jefferson Davis' },
  { code: '3102', name: 'La Salle' },
  { code: '1467', name: 'Lafayette' },
  { code: '583', name: 'Lafourche' },
  { code: '3253', name: 'Lincoln' },
  { code: '1841', name: 'Livingston' },
  { code: '2181', name: 'Madison' },
  { code: '1415', name: 'Morehouse' },
  { code: '3290', name: 'Natchitoches' },
  { code: '1960', name: 'Orleans' },
  { code: '2119', name: 'Ouachita' },
  { code: '1441', name: 'Plaquemines' },
  { code: '1423', name: 'Pointe Coupee' },
  { code: '2945', name: 'Rapides' },
  { code: '2692', name: 'Red River' },
  { code: '2209', name: 'Richland' },
  { code: '1499', name: 'Sabine' },
  { code: '555', name: 'St Bernard' },
  { code: '2134', name: 'St Charles' },
  { code: '644', name: 'St Helena' },
  { code: '1879', name: 'St James' },
  { code: '3181', name: 'St John The Baptist' },
  { code: '397', name: 'St Landry' },
  { code: '2267', name: 'St Martin' },
  { code: '2596', name: 'St Mary' },
  { code: '2048', name: 'St Tammany' },
  { code: '2940', name: 'Tangipahoa' },
  { code: '2485', name: 'Tensas' },
  { code: '2681', name: 'Terrebonne' },
  { code: '1701', name: 'Union' },
  { code: '1031', name: 'Vermilion' },
  { code: '959', name: 'Vernon' },
  { code: '1314', name: 'Washington' },
  { code: '2526', name: 'Webster' },
  { code: '2944', name: 'West Baton Rouge' },
  { code: '3302', name: 'West Carroll' },
  { code: '2450', name: 'West Feliciana' },
  { code: '749', name: 'Winn' },
];

// Default cities (used for filtering, not iteration — we iterate by parish)
const DEFAULT_CITIES = [
  'Baton Rouge', 'New Orleans', 'Shreveport', 'Lafayette', 'Lake Charles',
  'Kenner', 'Bossier City', 'Monroe', 'Alexandria', 'Houma',
];

// Classification-based practice area codes
const PRACTICE_AREA_CODES = {
  'general contractor': 'GENERAL',
  'building': 'BUILDING CONSTRUCTION',
  'electrical': 'ELECTRICAL',
  'plumbing': 'PLUMBING',
  'hvac': 'MECHANICAL/HVAC',
  'mechanical': 'MECHANICAL/HVAC',
  'roofing': 'ROOFING',
  'residential': 'RESIDENTIAL',
  'commercial': 'COMMERCIAL',
  'mold': 'MOLD REMEDIATION',
  'home improvement': 'HOME IMPROVEMENT',
};

// Name suffixes to strip
const NAME_SUFFIXES = new Set([
  'JR', 'JR.', 'SR', 'SR.', 'II', 'III', 'IV', 'V',
]);

/**
 * Title-case a string (UPPERCASE or mixed -> Title Case).
 */
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|[\s\-'.])(\w)/g, (match) => match.toUpperCase());
}

/**
 * Split a full name into first/last, handling suffixes and middle names.
 */
function splitPersonName(fullName) {
  if (!fullName) return { first: '', last: '' };

  const parts = fullName.trim().split(/\s+/).filter(Boolean);

  // Strip trailing suffixes
  while (parts.length > 1 && NAME_SUFFIXES.has(parts[parts.length - 1].toUpperCase().replace('.', ''))) {
    parts.pop();
  }

  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: '', last: titleCase(parts[0]) };

  return {
    first: titleCase(parts[0]),
    last: titleCase(parts[parts.length - 1]),
  };
}

/**
 * Check if a name looks like a company (LLC, Inc, Corp, etc.) vs an individual.
 */
function isCompanyName(name) {
  if (!name) return false;
  const lower = name.toLowerCase();
  return /\b(llc|l\.l\.c\.|inc|corp|co\.|company|enterprises|group|contractors|construction|services|builders|plumbing|electric|roofing|mechanical|hvac|heating|foundation)\b/i.test(lower)
    || /,\s*(llc|inc|corp|ltd)/i.test(lower);
}

/**
 * Normalize a phone number.
 */
function normalizePhone(raw) {
  if (!raw) return '';
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits[0] === '1') {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return digits.length >= 7 ? raw.trim() : '';
}

/**
 * Parse address string like "PO Box 351<br />Tioga, LA 71477"
 * into { address, city, state, zip }.
 */
function parseAddress(html) {
  if (!html) return { address: '', city: '', state: '', zip: '' };

  // Split on <br /> or <br> tags
  const parts = html.split(/<br\s*\/?>/i).map(p => p.trim()).filter(Boolean);

  if (parts.length === 0) return { address: '', city: '', state: '', zip: '' };

  // Last part should be "City, ST ZIP"
  const lastPart = parts[parts.length - 1];
  const cityStateZip = lastPart.match(/^(.+),\s*([A-Z]{2})\s+([\d-]+)$/);

  if (cityStateZip) {
    const addressLines = parts.slice(0, -1);
    return {
      address: addressLines.join(', '),
      city: titleCase(cityStateZip[1].trim()),
      state: cityStateZip[2],
      zip: cityStateZip[3],
    };
  }

  // Fallback: try "City, ST" without zip
  const cityState = lastPart.match(/^(.+),\s*([A-Z]{2})$/);
  if (cityState) {
    const addressLines = parts.slice(0, -1);
    return {
      address: addressLines.join(', '),
      city: titleCase(cityState[1].trim()),
      state: cityState[2],
      zip: '',
    };
  }

  // Can't parse — return the whole thing as address
  return { address: parts.join(', '), city: '', state: '', zip: '' };
}


class LaLslbcScraper extends BaseScraper {
  constructor() {
    super({
      name: 'la_lslbc_contractors',
      stateCode: 'LA-LSLBC',
      baseUrl: BASE_URL,
      pageSize: 1500, // max results per search query
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() { throw new Error(`${this.name}: not used`); }
  parseResultsPage() { throw new Error(`${this.name}: not used`); }
  extractResultCount() { throw new Error(`${this.name}: not used`); }

  transformResult(lead, practiceArea) {
    lead.source = 'la_lslbc_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTP POST helper that returns { statusCode, body }.
   */
  _httpPost(url, formData, rateLimiter) {
    const https = require('https');
    const http = require('http');
    const postData = typeof formData === 'string' ? formData : new URLSearchParams(formData).toString();

    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const protocol = parsed.protocol === 'https:' ? https : http;

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''),
        method: 'POST',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'Accept': 'text/html, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': BASE_URL,
          'Referer': `${BASE_URL}/Public/DetailedSearch/ByCounty`,
        },
        timeout: 30000,
      };

      const req = protocol.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return resolve({ statusCode: res.statusCode, body: '' });
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString('utf-8') });
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('POST request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Search by parish. Returns array of { businessName, city, state, contactKey }.
   * @param {string} parishCode - LSLBC parish ID
   * @param {RateLimiter} rateLimiter
   * @returns {Promise<{results: object[], truncated: boolean, count: number}>}
   */
  async _searchByParish(parishCode, rateLimiter) {
    const formData = {
      LocationCounty: parishCode,
      SearchType: 'ByCounty',
    };

    const response = await this._httpPost(SEARCH_URL, formData, rateLimiter);

    if (response.statusCode !== 200) {
      throw new Error(`Parish search returned ${response.statusCode}`);
    }

    return this._parseSearchResults(response.body);
  }

  /**
   * Search by name within a parish. Used when parish search hits 1500 limit.
   * Searches by last name prefix (3+ chars) to stay under limit.
   * @param {string} parishCode - LSLBC parish ID
   * @param {string} lastNamePrefix - Last name prefix (e.g., "Smi")
   * @param {RateLimiter} rateLimiter
   * @returns {Promise<{results: object[], truncated: boolean, count: number}>}
   */
  async _searchByNameInParish(parishCode, lastNamePrefix, rateLimiter) {
    const formData = {
      LastName: lastNamePrefix,
      LocationCounty: parishCode,
      SearchType: 'ByName',
    };

    const response = await this._httpPost(SEARCH_URL, formData, rateLimiter);

    if (response.statusCode !== 200) {
      throw new Error(`Name+parish search returned ${response.statusCode}`);
    }

    return this._parseSearchResults(response.body);
  }

  /**
   * Parse search results HTML into structured array.
   * Returns { results: [], truncated: boolean, count: number }.
   */
  _parseSearchResults(html) {
    const $ = cheerio.load(html);
    const results = [];

    // Extract the count from the DataTable init script
    // Pattern: "if (187 >= 1500)" where 187 is the result count
    let count = 0;
    let truncated = false;
    const countMatch = html.match(/if\s*\(\s*(\d+)\s*>=\s*1500\s*\)/);
    if (countMatch) {
      count = parseInt(countMatch[1], 10);
      truncated = count >= MAX_RESULTS;
    }

    $('table#AccountSearchTable tbody tr').each((_, row) => {
      const $row = $(row);
      const $link = $row.find('a[onclick*="ShowAccountDetails"]');
      if (!$link.length) return;

      // Extract contactKey from onclick="ShowAccountDetails('key', );"
      const onclick = $link.attr('onclick') || '';
      const keyMatch = onclick.match(/ShowAccountDetails\('([^']+)'/);
      if (!keyMatch) return;

      const contactKey = keyMatch[1];
      const businessName = ($link.text() || '').trim();
      const tds = $row.find('td');
      const city = tds.eq(1).text().trim();
      const state = tds.eq(2).text().trim();

      results.push({ businessName, city, state, contactKey });
    });

    if (count === 0) count = results.length;

    return { results, truncated, count };
  }

  /**
   * Fetch contractor detail page and parse all fields.
   * @param {string} contactKey - URL-encoded contact key
   * @param {RateLimiter} rateLimiter
   * @returns {Promise<object>} Parsed detail fields
   */
  async _fetchDetail(contactKey, rateLimiter) {
    const url = `${DETAIL_URL}?key=${encodeURIComponent(contactKey)}&Source=DetailedSearch`;
    const response = await this.httpGet(url, rateLimiter);

    if (response.statusCode !== 200) {
      return null;
    }

    const $ = cheerio.load(response.body);
    const detail = {};

    // Parse label/value pairs from the detail page
    // Pattern: <div class="display-label">Label</div><div class="display-field">Value</div>
    const labels = $('div.display-label');
    labels.each((_, el) => {
      const label = $(el).text().trim();
      const $field = $(el).next('div.display-field');
      if (!$field.length) return;

      // Use html() for address (preserves <br /> tags) and text() for everything else
      if (label === 'Mailing Address') {
        detail.addressHtml = $field.html() || '';
        detail.addressText = $field.text().trim();
      } else {
        const value = $field.text().trim();
        switch (label) {
          case 'Name': detail.name = value; break;
          case 'Phone Number': detail.phone = value; break;
          case 'Email Address': detail.email = value; break;
          case 'License': detail.license = value; break;
          case 'Type': detail.licenseType = value; break;
          case 'Status': detail.status = value; break;
          case 'Effective Date': detail.effectiveDate = value; break;
          case 'Expiration Date': detail.expirationDate = value; break;
          case 'First Issued': detail.firstIssued = value; break;
        }
      }
    });

    // Parse classifications table
    const classifications = [];
    let qualifyingParty = '';
    $('table#ClassificationListing tr').each((_, row) => {
      const tds = $(row).find('td');
      if (tds.length >= 2) {
        const classification = tds.eq(0).text().trim();
        const qp = tds.eq(1).text().trim();
        if (classification) classifications.push(classification);
        if (qp && !qualifyingParty) qualifyingParty = qp;
      }
    });

    detail.classifications = classifications;
    detail.qualifyingParty = qualifyingParty;

    return detail;
  }

  /**
   * Convert a raw detail object into a normalized lead.
   */
  _mapDetailToLead(searchResult, detail, parish) {
    if (!detail) return null;

    const name = detail.name || searchResult.businessName || '';
    const isCompany = isCompanyName(name);

    let firstName = '';
    let lastName = '';
    let firmName = '';

    if (isCompany) {
      firmName = name;
      // Use qualifying party as the person name
      if (detail.qualifyingParty) {
        const split = splitPersonName(detail.qualifyingParty);
        firstName = split.first;
        lastName = split.last;
      }
    } else {
      // Individual license
      const split = splitPersonName(name);
      firstName = split.first;
      lastName = split.last;
    }

    // Parse address
    const addr = parseAddress(detail.addressHtml || '');

    // Email validation — basic sanity check
    let email = (detail.email || '').trim().toLowerCase();
    if (email && !email.includes('@')) email = '';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      address: addr.address,
      city: addr.city || titleCase(searchResult.city),
      state: addr.state || searchResult.state || 'LA',
      zip: addr.zip,
      county: parish || '',
      phone: normalizePhone(detail.phone || ''),
      email: email,
      website: '',
      bar_number: detail.license || '',
      bar_status: detail.status || '',
      license_type: detail.licenseType || '',
      classification: (detail.classifications || []).join(', '),
      qualifying_party: detail.qualifyingParty || '',
      effective_date: detail.effectiveDate || '',
      expiration_date: detail.expirationDate || '',
      first_issued: detail.firstIssued || '',
      source: 'la_lslbc_contractors',
      practice_area: '',
    };
  }

  /**
   * Main search generator.
   *
   * Strategy:
   *   1. Iterate through all 64 Louisiana parishes
   *   2. For each parish, search by county code
   *   3. If results are truncated (>=1500), split into alphabetical sub-searches (A-Z)
   *   4. For each result, fetch detail page for full contact info
   *   5. Dedup by contactKey to avoid duplicates across parish/name overlaps
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape('Louisiana LSLBC — Contractor License Search');
    if (practiceCode) {
      log.info(`Classification filter: ${practiceCode}`);
    }

    // Determine which parishes to search
    let parishList = PARISHES;
    if (options.city) {
      // If city filter provided, try to find matching parish
      const cityLower = options.city.toLowerCase();
      const match = PARISHES.find(p => p.name.toLowerCase() === cityLower);
      if (match) {
        parishList = [match];
        log.info(`Parish filter: ${match.name}`);
      } else {
        log.info(`City filter: ${options.city} (will filter results by city)`);
      }
    }

    // Test mode: limit parishes
    if (options.maxCities && parishList.length > options.maxCities) {
      parishList = parishList.slice(0, options.maxCities);
      log.info(`Test mode: limiting to ${parishList.length} parishes`);
    }

    const seenKeys = new Set();
    let totalYielded = 0;
    let totalWithPhone = 0;
    let totalWithEmail = 0;
    let totalSearchResults = 0;
    let detailsFetched = 0;
    let detailErrors = 0;

    // Max detail fetches limit (for test mode)
    const maxDetails = options.maxPages ? options.maxPages * 50 : Infinity;

    for (let pi = 0; pi < parishList.length; pi++) {
      const parish = parishList[pi];
      yield { _cityProgress: { current: pi + 1, total: parishList.length } };
      log.scrape(`Searching ${parish.name} Parish (${pi + 1}/${parishList.length})`);

      // Collect all search results for this parish
      let parishResults = [];

      try {
        await rateLimiter.wait();
        const searchData = await this._searchByParish(parish.code, rateLimiter);
        log.info(`${parish.name}: ${searchData.count} results${searchData.truncated ? ' (TRUNCATED at 1500)' : ''}`);

        if (searchData.truncated) {
          // Parish has too many results — split by last name letter
          log.info(`${parish.name}: splitting by last name letter (A-Z) to get all results`);
          const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

          for (const letter of letters) {
            if (totalYielded >= maxDetails) break;

            try {
              await rateLimiter.wait();
              const letterData = await this._searchByNameInParish(parish.code, letter, rateLimiter);
              log.info(`  ${parish.name} / ${letter}: ${letterData.count} results${letterData.truncated ? ' (truncated)' : ''}`);

              for (const r of letterData.results) {
                if (!seenKeys.has(r.contactKey)) {
                  parishResults.push(r);
                }
              }
            } catch (err) {
              log.warn(`  ${parish.name} / ${letter}: search failed — ${err.message}`);
            }
          }
        } else {
          parishResults = searchData.results;
        }
      } catch (err) {
        log.error(`${parish.name}: parish search failed — ${err.message}`);
        continue;
      }

      totalSearchResults += parishResults.length;
      log.info(`${parish.name}: ${parishResults.length} unique results to fetch details for`);

      // Fetch details for each result
      for (const result of parishResults) {
        if (totalYielded >= maxDetails) {
          log.info(`Reached max detail limit (${maxDetails})`);
          break;
        }

        // Dedup by contactKey
        if (seenKeys.has(result.contactKey)) continue;
        seenKeys.add(result.contactKey);

        let detail;
        try {
          await rateLimiter.wait();
          detail = await this._fetchDetail(result.contactKey, rateLimiter);
          detailsFetched++;
        } catch (err) {
          detailErrors++;
          if (detailErrors <= 5) {
            log.warn(`Detail fetch failed for "${result.businessName}": ${err.message}`);
          }
          continue;
        }

        if (!detail) {
          detailErrors++;
          continue;
        }

        const lead = this._mapDetailToLead(result, detail, parish.name);
        if (!lead) continue;

        // Filter by practice area / classification
        if (practiceCode) {
          const classLower = (lead.classification || '').toLowerCase();
          const typeLower = (lead.license_type || '').toLowerCase();
          const filterLower = practiceCode.toLowerCase();
          if (!classLower.includes(filterLower) && !typeLower.includes(filterLower)) {
            continue;
          }
        }

        // Filter by city if specified and not matching a parish
        if (options.city && parishList.length > 1) {
          const cityLower = options.city.toLowerCase();
          const leadCity = (lead.city || '').toLowerCase();
          if (!leadCity.includes(cityLower) && !cityLower.includes(leadCity)) {
            continue;
          }
        }

        // Track stats
        if (lead.phone) totalWithPhone++;
        if (lead.email) totalWithEmail++;

        yield this.transformResult(lead, practiceArea || lead.classification || '');
        totalYielded++;
      }

      if (totalYielded >= maxDetails) break;
    }

    log.success([
      'LA LSLBC scrape complete:',
      `${totalSearchResults.toLocaleString()} search results`,
      `${detailsFetched.toLocaleString()} details fetched`,
      `${totalYielded.toLocaleString()} yielded`,
      `(${totalWithPhone} with phone, ${totalWithEmail} with email)`,
      detailErrors > 0 ? `(${detailErrors} detail errors)` : '',
    ].filter(Boolean).join(' | '));
  }
}

module.exports = new LaLslbcScraper();
