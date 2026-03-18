/**
 * Connecticut Home Improvement Contractor (CT HIC) — Socrata Open Data Scraper
 *
 * Source: https://data.ct.gov/resource/5r9m-qgni.json
 * Platform: Socrata SODA API (free, no auth required)
 * Records: ~26K active HIC licensees
 * Method: JSON API with $limit/$offset pagination
 *
 * Fields from API:
 *   credentialid      — Internal record ID
 *   name              — Licensee name (UPPERCASE, person or company)
 *   type              — INDIVIDUAL or LIMITED LIABILITY COMPANY / CORPORATION / etc.
 *   businessname      — Business name (optional, may match name for companies)
 *   dba               — DBA / trade name (optional)
 *   fullcredentialcode— e.g., "HIC.0631057"
 *   credentialtype    — Always "HIC"
 *   credentialnumber  — License number (e.g., "631057")
 *   credential        — Always "HOME IMPROVEMENT CONTRACTOR"
 *   status            — ACTIVE or INACTIVE
 *   statusreason      — CURRENT, DUE TO NON-RENEWAL OF CREDENTIAL, etc.
 *   active            — "1" or "0"
 *   issuedate         — ISO 8601 datetime
 *   effectivedate     — ISO 8601 datetime
 *   expirationdate    — ISO 8601 datetime
 *   city              — City (mixed case or uppercase)
 *   state             — Two-letter state code (usually CT, some out-of-state)
 *   zip               — Zip code (sometimes 9-digit without hyphen, e.g., "064683005")
 *   recordrefreshedon — Last refresh date
 *
 * Notes:
 *   - type=INDIVIDUAL records have person names in the `name` field (e.g., "HARRY J PAUL III")
 *     and may have businessname/dba for their company
 *   - type=LLC/CORP/etc. records have company names in `name` and `businessname`
 *   - No phone, email, or website in this dataset — enrichment pipeline handles that
 *   - Zip codes may be 5-digit or 9-digit (without hyphen); we normalize to 5-digit
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_BASE = 'https://data.ct.gov/resource/5r9m-qgni.json';
const PAGE_SIZE = 1000;

// Name suffixes to strip when splitting names
const NAME_SUFFIXES = new Set([
  'JR', 'SR', 'II', 'III', 'IV', 'V', 'JR.', 'SR.',
]);

/**
 * Title-case a string (UPPERCASE -> Title Case).
 * Handles hyphens and apostrophes.
 */
function titleCase(str) {
  if (!str) return '';
  return str
    .toLowerCase()
    .replace(/(?:^|[\s\-'.])(\w)/g, (match) => match.toUpperCase());
}

/**
 * Normalize a zip code — take first 5 digits if 9-digit format without hyphen.
 * "064683005" -> "06468", "06804" -> "06804"
 */
function normalizeZip(raw) {
  if (!raw) return '';
  const digits = raw.replace(/[^\d]/g, '');
  if (digits.length === 9) return digits.slice(0, 5);
  if (digits.length === 5) return digits;
  // Return as-is if it has a hyphen already or other format
  return raw.trim();
}

/**
 * Split a full name into first/last, handling suffixes and middle names/initials.
 * "HARRY J PAUL III" -> { first: "Harry", last: "Paul" }
 * "MARC DEMARTINO" -> { first: "Marc", last: "Demartino" }
 * "RAYMOND A DECKER" -> { first: "Raymond", last: "Decker" }
 */
function splitPersonName(fullName) {
  if (!fullName) return { first: '', last: '' };

  const parts = fullName.trim().split(/\s+/).filter(Boolean);

  // Strip trailing suffixes
  while (parts.length > 1 && NAME_SUFFIXES.has(parts[parts.length - 1].toUpperCase())) {
    parts.pop();
  }

  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: '', last: titleCase(parts[0]) };

  // First token = first name, last token = last name (skip middle names/initials)
  return {
    first: titleCase(parts[0]),
    last: titleCase(parts[parts.length - 1]),
  };
}

class CtHicScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ct_hic_contractors',
      stateCode: 'CT-HIC',
      baseUrl: API_BASE,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {},
      defaultCities: [
        'Hartford', 'New Haven', 'Stamford', 'Bridgeport', 'Waterbury',
        'Norwalk', 'Danbury', 'New Britain', 'Greenwich', 'Fairfield',
      ],
    });
  }

  // Not used — search() is overridden
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() not used — search() is overridden`);
  }

  transformResult(lead, practiceArea) {
    lead.source = 'ct_hic_contractors';
    lead.practice_area = practiceArea || lead.practice_area || 'Home Improvement';
    return lead;
  }

  /**
   * Fetch a page of results from the Socrata SODA API.
   * Returns parsed JSON array.
   */
  _fetchPage(offset, rateLimiter) {
    const params = new URLSearchParams({
      '$limit': String(PAGE_SIZE),
      '$offset': String(offset),
      '$where': "active='1'",
      '$order': 'credentialid',
    });
    const url = `${API_BASE}?${params.toString()}`;

    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const req = https.request({
        hostname: parsed.hostname,
        path: `${parsed.pathname}?${parsed.searchParams.toString()}`,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Accept-Encoding': 'identity',
        },
        timeout: 30000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return reject(new Error(`Redirect ${res.statusCode} to ${res.headers.location}`));
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf-8');
          resolve({ statusCode: res.statusCode, body });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Socrata API request timed out')); });
      req.end();
    });
  }

  /**
   * Map a Socrata JSON record to a normalized lead object.
   */
  _mapRecord(rec) {
    const isIndividual = (rec.type || '').toUpperCase() === 'INDIVIDUAL';
    const rawName = (rec.name || '').trim();
    const businessName = (rec.businessname || '').trim();
    const dba = (rec.dba || '').trim();

    let firstName = '';
    let lastName = '';
    let firmName = '';

    if (isIndividual) {
      // Individual: name is the person, businessname/dba is the firm
      const split = splitPersonName(rawName);
      firstName = split.first;
      lastName = split.last;
      firmName = titleCase(dba || businessName || '');
    } else {
      // Company: name/businessname is the firm, no person name
      firstName = '';
      lastName = '';
      firmName = titleCase(businessName || rawName || '');
      // If DBA is different, append it
      if (dba && dba.toUpperCase() !== (businessName || rawName || '').toUpperCase()) {
        firmName = titleCase(dba) || firmName;
      }
    }

    const zip = normalizeZip(rec.zip || '');
    const city = titleCase((rec.city || '').trim());
    const state = (rec.state || 'CT').trim().toUpperCase();

    // Parse dates to YYYY-MM-DD
    const issueDate = rec.issuedate ? rec.issuedate.split('T')[0] : '';
    const expirationDate = rec.expirationdate ? rec.expirationdate.split('T')[0] : '';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      city: city,
      state: state,
      zip: zip,
      phone: '',
      email: '',
      website: '',
      bar_number: (rec.credentialnumber || '').trim(),
      bar_status: (rec.status || '').trim(),
      license_type: (rec.credential || 'Home Improvement Contractor'),
      license_code: (rec.fullcredentialcode || '').trim(),
      entity_type: (rec.type || '').trim(),
      issue_date: issueDate,
      expiration_date: expirationDate,
      source: 'ct_hic_contractors',
      practice_area: 'Home Improvement',
    };
  }

  /**
   * Main search generator — paginates through Socrata API, yields leads.
   *
   * The Socrata SODA API returns JSON arrays with $limit/$offset pagination.
   * We filter active=1 server-side and apply optional city filter client-side.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    log.scrape('CT HIC — Connecticut Home Improvement Contractors (Socrata API)');

    // Determine city filter
    const cityFilter = options.city ? options.city.toLowerCase().trim() : null;
    if (cityFilter) {
      log.info(`City filter: ${options.city}`);
    }

    yield { _cityProgress: { current: 1, total: 1 } };

    // Apply maxPages as a record limit
    const maxRecords = options.maxPages
      ? options.maxPages * PAGE_SIZE
      : Infinity;

    let offset = 0;
    let yieldedCount = 0;
    let totalFetched = 0;
    let withFirm = 0;
    let individuals = 0;
    let companies = 0;
    let pageNum = 0;

    while (true) {
      if (yieldedCount >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords.toLocaleString()})`);
        break;
      }

      pageNum++;
      log.info(`Page ${pageNum} — offset ${offset}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._fetchPage(offset, rateLimiter);
      } catch (err) {
        log.error(`Socrata API request failed: ${err.message}`);
        break;
      }

      if (response.statusCode === 429) {
        log.warn('Socrata API rate limited (429) — waiting and retrying');
        await new Promise(r => setTimeout(r, 5000));
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Socrata API returned ${response.statusCode}`);
        break;
      }

      let records;
      try {
        records = JSON.parse(response.body);
      } catch (err) {
        log.error(`Failed to parse Socrata JSON: ${err.message}`);
        break;
      }

      if (!Array.isArray(records) || records.length === 0) {
        log.info('No more records — pagination complete');
        break;
      }

      totalFetched += records.length;
      log.info(`Fetched ${records.length} records (${totalFetched.toLocaleString()} total)`);

      for (const rec of records) {
        if (yieldedCount >= maxRecords) break;

        // City filter (client-side)
        if (cityFilter) {
          const recCity = (rec.city || '').toLowerCase().trim();
          if (!recCity.includes(cityFilter) && !cityFilter.includes(recCity)) {
            continue;
          }
        }

        const lead = this._mapRecord(rec);

        // Track stats
        if (lead.firm_name) withFirm++;
        if ((rec.type || '').toUpperCase() === 'INDIVIDUAL') {
          individuals++;
        } else {
          companies++;
        }

        yield this.transformResult(lead, practiceArea);
        yieldedCount++;
      }

      // If we got fewer than PAGE_SIZE, we've reached the end
      if (records.length < PAGE_SIZE) {
        log.info('Last page reached (fewer records than page size)');
        break;
      }

      offset += PAGE_SIZE;
    }

    log.success([
      'CT HIC scrape complete:',
      `${totalFetched.toLocaleString()} fetched from API`,
      `${yieldedCount.toLocaleString()} yielded`,
      `(${individuals} individuals, ${companies} companies, ${withFirm} with firm name)`,
    ].join(' | '));
  }
}

module.exports = new CtHicScraper();
