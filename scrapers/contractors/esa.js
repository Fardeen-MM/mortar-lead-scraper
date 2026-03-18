/**
 * Ontario ESA (Electrical Safety Authority) — Contractor Locator
 *
 * Source: https://licensing.esasafe.com/contractor-locator-tool/
 * Platform: jQuery-based page with ./data JSON endpoint (PowerApps portal)
 * Method: GET https://licensing.esasafe.com/contractor-locator-tool/data
 *
 * Returns full JSON array (~18K records) of all licensed electrical contractors
 * in Ontario, Canada. Fields: licence number, status, phone, contact phone,
 * website, address, work types, GPS coordinates, complaint flag.
 *
 * Work type codes (positive = can perform, negative = cannot perform):
 *   1/-1 = Residential    2/-2 = Commercial/Industrial
 *   3/-3 = HVAC           4 = Fire Alarm
 *   5 = Generator          6/-6 = Pole Line
 *   7 = EV Charger         8 = Energy Storage
 *   9 = Solar/Renewable
 *
 * Licence statuses: Valid, Expired, Closed, Suspended, Revoked, Unlicenced
 *
 * Contact data: ~10K valid contractors, ~100% phone, ~12% website, no emails.
 * The 'e' field in the JSON is a numeric internal reference, NOT an email.
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const DATA_URL = 'https://licensing.esasafe.com/contractor-locator-tool/data';
const CACHE_DIR = path.join(__dirname, '..', '..', 'data', 'contractor-cache');
const CACHE_FILE = path.join(CACHE_DIR, 'esa-contractors.json');
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

// Work type integer -> label mapping
// Positive codes = contractor CAN do this work type
// Negative codes = contractor CANNOT do this work type (restriction)
const WORK_TYPE_MAP = {
  1: 'Residential',
  2: 'Commercial/Industrial',
  3: 'HVAC',
  4: 'Fire Alarm',
  5: 'Generator',
  6: 'Pole Line',
  7: 'EV Charger',
  8: 'Energy Storage',
  9: 'Solar/Renewable',
};

class EsaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'esa-contractors',
      stateCode: 'ESA',
      baseUrl: DATA_URL,
      pageSize: 500,
      practiceAreaCodes: {
        'residential': 'Residential',
        'commercial': 'Commercial/Industrial',
        'hvac': 'HVAC',
        'fire alarm': 'Fire Alarm',
        'generator': 'Generator',
        'pole line': 'Pole Line',
        'ev charger': 'EV Charger',
        'energy storage': 'Energy Storage',
        'solar': 'Solar/Renewable',
        'electrical': 'all',
      },
      defaultCities: [
        'Toronto', 'Ottawa', 'Mississauga', 'Brampton', 'Hamilton',
        'London', 'Markham', 'Vaughan', 'Kitchener', 'Windsor',
      ],
    });
  }

  // Not used -- search() is overridden
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override transformResult to use 'esa_contractors' source instead of default '_bar' suffix.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'esa_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTPS GET returning the full response body as a string.
   */
  _fetchData(rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(DATA_URL);
      const req = https.request({
        hostname: parsed.hostname,
        path: parsed.pathname,
        method: 'GET',
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Referer': 'https://licensing.esasafe.com/contractor-locator-tool/',
        },
        timeout: 60000, // 60s -- large payload (~5MB)
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          resolve({ statusCode: res.statusCode, redirect: res.headers.location, body: '' });
          return;
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf-8');
          resolve({ statusCode: res.statusCode, body });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('ESA data request timed out (60s)')); });
      req.end();
    });
  }

  /**
   * Load contractor data -- from cache if fresh, otherwise fetch + cache.
   */
  async _loadContractors(rateLimiter) {
    // Check cache
    if (fs.existsSync(CACHE_FILE)) {
      try {
        const stat = fs.statSync(CACHE_FILE);
        const ageMs = Date.now() - stat.mtimeMs;
        if (ageMs < CACHE_TTL_MS) {
          log.info(`Loading ESA data from cache (${Math.round(ageMs / 60000)}m old)`);
          const raw = fs.readFileSync(CACHE_FILE, 'utf-8');
          const data = JSON.parse(raw);
          log.success(`Cache loaded: ${data.length} contractors`);
          return data;
        }
        log.info('ESA cache expired — refetching');
      } catch (err) {
        log.warn(`Cache read failed: ${err.message} — refetching`);
      }
    }

    // Fetch live data
    log.info(`Fetching ESA contractor data from ${DATA_URL}`);
    const response = await this._fetchData(rateLimiter);

    if (response.statusCode !== 200) {
      throw new Error(`ESA data endpoint returned ${response.statusCode}`);
    }

    let data;
    try {
      data = JSON.parse(response.body);
    } catch (err) {
      throw new Error(`Failed to parse ESA JSON: ${err.message}`);
    }

    if (!Array.isArray(data)) {
      throw new Error('ESA data endpoint did not return a JSON array');
    }

    log.success(`Fetched ${data.length} contractors from ESA`);

    // Cache to disk
    try {
      if (!fs.existsSync(CACHE_DIR)) {
        fs.mkdirSync(CACHE_DIR, { recursive: true });
      }
      fs.writeFileSync(CACHE_FILE, JSON.stringify(data));
      log.info(`Cached ESA data to ${CACHE_FILE}`);
    } catch (err) {
      log.warn(`Failed to cache ESA data: ${err.message}`);
    }

    return data;
  }

  /**
   * Resolve work type integer codes to human-readable labels.
   * Positive codes = allowed work types, negative codes = restrictions (excluded).
   * Only returns the positive (allowed) work types.
   */
  _resolveWorkTypes(wt) {
    if (!Array.isArray(wt) || wt.length === 0) return '';
    return wt
      .filter(code => code > 0) // Only include allowed work types
      .map(code => WORK_TYPE_MAP[code] || `Type ${code}`)
      .join(', ');
  }

  /**
   * Check if a contractor matches the requested work type filter.
   * Only matches against positive (allowed) work type codes.
   */
  _matchesWorkType(wt, practiceCode) {
    if (!practiceCode || practiceCode === 'all') return true;
    if (!Array.isArray(wt) || wt.length === 0) return false;

    const allowedLabels = wt
      .filter(code => code > 0)
      .map(code => (WORK_TYPE_MAP[code] || '').toLowerCase());

    return allowedLabels.some(label => label === practiceCode.toLowerCase());
  }

  /**
   * Map an ESA JSON record to a normalized lead object.
   *
   * ESA JSON fields:
   *   n  = company name       ln = licence number     ls = licence status
   *   lv = licence valid      s  = street address     u  = unit/suite
   *   c  = city               p  = province           cn = country
   *   la = latitude           lo = longitude          ph = phone
   *   e  = internal ref (NOT email!)                  cp = contact phone
   *   w  = website            wt = work types (int[]) a  = UUID
   *   hc = hire complaint flag
   */
  _mapRecord(rec) {
    const firmName = (rec.n || '').trim();
    // Use primary phone, fall back to contact phone
    const phone = (rec.ph || '').trim() || (rec.cp || '').trim();
    // Contact phone as alternate (if different from primary)
    const altPhone = (rec.cp || '').trim();
    let website = (rec.w || '').trim();

    // Normalize website -- ensure it has a protocol
    if (website && !website.match(/^https?:\/\//i)) {
      website = 'https://' + website;
    }

    // Build address from parts
    const street = (rec.s || '').trim();
    const unit = (rec.u || '').trim();
    const address = unit ? `${street}, ${unit}` : street;

    // For contractor records, the "firm" is the company name.
    // These are business licences — no individual names.
    return {
      first_name: '',
      last_name: '',
      firm_name: firmName,
      address: address,
      city: (rec.c || '').trim(),
      state: (rec.p || 'ON').trim(),
      zip: '',
      country: (rec.cn || 'CA').trim(),
      phone: phone,
      alt_phone: (altPhone && altPhone !== phone) ? altPhone : '',
      email: '',  // ESA does not provide emails
      website: website,
      bar_number: (rec.ln || '').trim(),
      bar_status: (rec.ls || '').trim(),
      licence_valid: !!rec.lv,
      work_types: this._resolveWorkTypes(rec.wt),
      has_complaint: !!rec.hc,
      latitude: rec.la || '',
      longitude: rec.lo || '',
      source: 'esa_contractors',
    };
  }

  /**
   * Main search generator -- fetches full dataset, filters, yields leads.
   *
   * Unlike bar scrapers, ESA returns ALL contractors in one JSON payload (~5MB).
   * We filter client-side by: status (Valid only), city, and work type.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    log.scrape(`ESA Contractor Locator — searching Ontario electricians`);
    if (practiceCode && practiceCode !== 'all') {
      log.info(`Work type filter: ${practiceCode}`);
    }

    // Determine city filter
    const cityFilter = options.city
      ? [options.city.toLowerCase()]
      : null;

    yield { _cityProgress: { current: 1, total: 1 } };

    // Fetch all contractor data (cached for 24h)
    let contractors;
    try {
      contractors = await this._loadContractors(rateLimiter);
    } catch (err) {
      log.error(`Failed to load ESA data: ${err.message}`);
      return;
    }

    if (!contractors || contractors.length === 0) {
      log.warn('ESA returned 0 contractors');
      return;
    }

    // Stats
    const total = contractors.length;
    let validCount = 0;
    let matchedCount = 0;
    let yieldedCount = 0;
    let withPhone = 0;
    let withWebsite = 0;

    // Apply maxPages as a record limit (500 per "page")
    const maxRecords = options.maxPages
      ? options.maxPages * this.pageSize
      : Infinity;

    log.info(`Processing ${total.toLocaleString()} contractors (limit: ${maxRecords === Infinity ? 'none' : maxRecords})`);

    for (const rec of contractors) {
      if (yieldedCount >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords})`);
        break;
      }

      // Filter: Valid licence only (skip Expired, Closed, Suspended, Revoked, Unlicenced)
      if (!rec.lv) continue;
      validCount++;

      // Filter: Work type
      if (!this._matchesWorkType(rec.wt, practiceCode)) continue;
      matchedCount++;

      // Filter: City (if specified)
      const recCity = (rec.c || '').trim().toLowerCase();
      if (cityFilter && !cityFilter.some(cf => recCity.includes(cf) || cf.includes(recCity))) {
        continue;
      }

      const lead = this._mapRecord(rec);
      lead.practice_area = practiceArea || this._resolveWorkTypes(rec.wt) || 'Electrical';

      // Track contact stats
      if (lead.phone) withPhone++;
      if (lead.website) withWebsite++;

      yield this.transformResult(lead, practiceArea);
      yieldedCount++;
    }

    log.success([
      `ESA scrape complete:`,
      `${total.toLocaleString()} total`,
      `${validCount.toLocaleString()} valid`,
      `${matchedCount.toLocaleString()} matched work type`,
      `${yieldedCount.toLocaleString()} yielded`,
      `(${withPhone} phone, ${withWebsite} website)`,
    ].join(' | '));
  }
}

module.exports = new EsaScraper();
