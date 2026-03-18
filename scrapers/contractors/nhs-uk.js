/**
 * NHS.UK Practice Scraper — GP Surgeries & Dental Practices (England)
 *
 * Source: https://www.nhs.uk/services/
 * Records: ~9,800 dental practices + ~12,800 GP surgeries (England only)
 * Method: ODS API for practice codes → NHS.UK profile pages for contact details
 *
 * Strategy:
 *   1. Enumerate all active practice ODS codes from the NHS ODS (Organisation Data
 *      Service) Spine Directory API, paginating via the next-page response header.
 *      - Dental: PrimaryRoleId=RO110 ("GENERAL DENTAL PRACTICE") — V-prefix codes
 *      - GP: PrimaryRoleId=RO177 ("PRESCRIBING COST CENTRE") — A/E/etc codes
 *
 *   2. For each ODS code, fetch the NHS.UK profile page:
 *      - GP: https://www.nhs.uk/services/gp-surgery/x/{ODS_CODE}  (redirects to slug)
 *      - Dental: https://www.nhs.uk/services/dentist/x/{ODS_CODE}  (redirects to slug)
 *
 *   3. Extract structured data from the JSON-LD script tag (id="json-ld-script"):
 *      - @type: "Physician" (GP) or "Dentist" (dental)
 *      - name, telephone, email, url, address, geo coordinates
 *
 *   NOTE: The JSON-LD script tag uses HTML-encoded type attribute:
 *   type="application/ld&#x2B;json" (the + is encoded as &#x2B;)
 *   We match by the id="json-ld-script" attribute instead.
 *
 *   Not all ODS codes have corresponding NHS.UK pages (many return 404).
 *   The scraper handles this gracefully and skips missing pages.
 *
 * Practice area filter:
 *   - "dental" or "dentist" → only dental practices (RO110)
 *   - "gp" or "general practice" → only GP surgeries (RO177)
 *   - default (no filter) → dental practices first, then GP surgeries
 *
 * ODS API docs: https://digital.nhs.uk/services/organisation-data-service
 * Rate limits: No documented limits, but we use 1s delay between NHS.UK page fetches.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// ODS Spine Directory API
const ODS_API_BASE = 'https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations';

// NHS.UK profile page base
const NHS_UK_BASE = 'https://www.nhs.uk';

// ODS Role IDs
const ROLE_DENTAL = 'RO110'; // GENERAL DENTAL PRACTICE
const ROLE_GP = 'RO177';     // PRESCRIBING COST CENTRE (includes GP surgeries)

// Practice area mapping
const PRACTICE_AREA_CODES = {
  'dental': 'dental',
  'dentist': 'dental',
  'gp': 'gp',
  'general practice': 'gp',
  'general practitioner': 'gp',
  'doctor': 'gp',
  'surgery': 'gp',
};

// Default: scrape both dental and GP
const DEFAULT_CITIES = [];

// ODS page size (max 100)
const ODS_PAGE_SIZE = 100;


class NhsUkScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nhs_uk',
      stateCode: 'NHS-UK',
      baseUrl: NHS_UK_BASE,
      pageSize: ODS_PAGE_SIZE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('nhs_uk: buildSearchUrl() not used'); }
  parseResultsPage() { throw new Error('nhs_uk: parseResultsPage() not used'); }
  extractResultCount() { throw new Error('nhs_uk: extractResultCount() not used'); }

  transformResult(lead, practiceArea) {
    lead.source = 'nhs_uk';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * HTTP GET with redirect following (up to 5 redirects).
   * Returns { statusCode, body }.
   */
  _httpGet(url, ua, maxRedirects = 5) {
    return new Promise((resolve, reject) => {
      if (maxRedirects <= 0) return reject(new Error('Too many redirects'));

      const req = https.get(url, {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = NHS_UK_BASE + loc;
          return resolve(this._httpGet(loc, ua, maxRedirects - 1));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Fetch JSON from the ODS Spine Directory API.
   * Returns { data, nextPage }.
   */
  _fetchOdsPage(url) {
    return new Promise((resolve, reject) => {
      https.get(url, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000,
      }, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            return reject(new Error(`ODS API returned ${res.statusCode}`));
          }
          try {
            const parsed = JSON.parse(data);
            const nextPage = res.headers['next-page'] || null;
            resolve({ data: parsed, nextPage });
          } catch (err) {
            reject(new Error(`ODS API returned invalid JSON: ${err.message}`));
          }
        });
      }).on('error', reject);
    });
  }

  /**
   * Enumerate all active ODS codes for a given role ID.
   * Yields { OrgId, Name, PostCode } for each practice.
   */
  async *_enumerateOdsCodes(roleId, maxPages = Infinity) {
    let url = `${ODS_API_BASE}?PrimaryRoleId=${roleId}&Status=Active&Limit=${ODS_PAGE_SIZE}`;
    let page = 0;
    let total = 0;

    while (url && page < maxPages) {
      let resp;
      try {
        resp = await this._fetchOdsPage(url);
      } catch (err) {
        log.error(`ODS API error on page ${page}: ${err.message}`);
        break;
      }

      const orgs = resp.data.Organisations || [];
      if (orgs.length === 0) break;

      for (const org of orgs) {
        yield { odsCode: org.OrgId, name: org.Name, postCode: org.PostCode };
        total++;
      }

      url = resp.nextPage || null;
      page++;
    }

    log.info(`ODS: Enumerated ${total.toLocaleString()} ${roleId} codes across ${page} pages`);
  }

  /**
   * Extract JSON-LD structured data from an NHS.UK profile page body.
   * The script tag has id="json-ld-script" and type="application/ld&#x2B;json".
   * Returns parsed JSON object or null.
   */
  _extractJsonLd(body) {
    const match = body.match(/<script[^>]*id="json-ld-script"[^>]*>(.*?)<\/script>/s);
    if (!match) return null;
    try {
      return JSON.parse(match[1]);
    } catch (err) {
      return null;
    }
  }

  /**
   * Build NHS.UK profile URL for a practice.
   * Uses 'x' as a dummy slug — NHS.UK redirects to the correct slug.
   */
  _buildProfileUrl(odsCode, type) {
    const prefix = type === 'dental' ? 'dentist' : 'gp-surgery';
    return `${NHS_UK_BASE}/services/${prefix}/x/${odsCode}`;
  }

  /**
   * Parse a UK postcode into city/region.
   * UK postcodes: "XX(X) NXX" format. Area is the first 1-2 letters.
   * We extract the outward code (first half) which maps to a district.
   */
  _parsePostcode(postcode) {
    if (!postcode) return { city: '', zip: postcode || '' };
    return { city: '', zip: postcode.trim().toUpperCase() };
  }

  /**
   * Parse the street address from JSON-LD into components.
   * NHS.UK format: "Street, Area, City, County"
   */
  _parseAddress(streetAddress, postcode) {
    if (!streetAddress) return { address: '', city: '' };

    const parts = streetAddress.split(',').map(p => p.trim()).filter(Boolean);
    let city = '';

    // The last 1-2 parts are typically city and county
    if (parts.length >= 2) {
      // Take the second-to-last part as city (last is usually county)
      city = parts[parts.length - 2] || '';
      // If it looks like a county, go one more back
      const countyKeywords = ['greater', 'county', 'shire', 'south', 'north', 'east', 'west', 'mid'];
      if (city && countyKeywords.some(kw => city.toLowerCase().includes(kw)) && parts.length >= 3) {
        city = parts[parts.length - 3] || city;
      }
    } else if (parts.length === 1) {
      city = parts[0];
    }

    return {
      address: streetAddress,
      city: city,
    };
  }

  /**
   * Normalize a UK phone number.
   * Formats: 02087992525 -> 020 8799 2525, 01234567890 -> 01234 567890
   */
  _normalizePhone(raw) {
    if (!raw) return '';
    const digits = raw.replace(/[^\d]/g, '');
    if (digits.length === 11 && digits[0] === '0') {
      // UK landline
      if (digits.startsWith('020')) {
        return `${digits.slice(0, 3)} ${digits.slice(3, 7)} ${digits.slice(7)}`;
      }
      return `${digits.slice(0, 5)} ${digits.slice(5)}`;
    }
    if (digits.length === 10) {
      return `${digits.slice(0, 5)} ${digits.slice(5)}`;
    }
    return raw.trim();
  }

  /**
   * Map JSON-LD data + ODS metadata to a normalized lead object.
   */
  _mapPractice(jsonLd, odsCode, practiceType) {
    const addr = jsonLd.address || {};
    const geo = jsonLd.geo || {};
    const parsed = this._parseAddress(addr.streetAddress, addr.postalCode);

    return {
      first_name: '',
      last_name: '',
      firm_name: jsonLd.name || '',
      address: parsed.address,
      city: parsed.city,
      state: 'UK',
      zip: (addr.postalCode || '').trim(),
      phone: this._normalizePhone(jsonLd.telephone || ''),
      email: (jsonLd.email || '').trim().toLowerCase(),
      website: jsonLd.url || '',
      bar_number: odsCode, // ODS code in bar_number field
      bar_status: 'Active',
      practice_area: practiceType === 'dental' ? 'Dental' : 'GP',
      nhs_type: jsonLd['@type'] || '',
      latitude: geo.latitude || '',
      longitude: geo.longitude || '',
      profile_url: '',
      source: 'nhs_uk',
    };
  }

  /**
   * Main search generator.
   *
   * Enumerates ODS codes, fetches each NHS.UK profile page,
   * extracts JSON-LD structured data, yields normalized leads.
   *
   * Practice area filter:
   *   "dental" → dental only (RO110)
   *   "gp" → GP only (RO177)
   *   default → dental first, then GP
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter({ minDelay: 1000, maxDelay: 2000 });

    // Determine which types to scrape
    let types = [];
    if (practiceArea) {
      const key = practiceArea.toLowerCase().trim();
      const resolved = PRACTICE_AREA_CODES[key];
      if (resolved === 'dental') {
        types = ['dental'];
      } else if (resolved === 'gp') {
        types = ['gp'];
      } else {
        types = ['dental', 'gp'];
      }
    } else {
      types = ['dental', 'gp'];
    }

    log.scrape(`NHS.UK: Scraping ${types.join(' + ')} practices`);

    // Stats
    let totalYielded = 0;
    let totalWithEmail = 0;
    let totalWithPhone = 0;
    let totalWithWebsite = 0;
    let total404 = 0;
    let totalErrors = 0;
    let totalOdsCodes = 0;

    // Respect maxPages as a total page limit for the ODS enumeration
    const maxOdsPages = options.maxPages || Infinity;
    // In test mode, limit pages aggressively
    const maxPagesPerType = options.maxPages ? Math.ceil(options.maxPages / types.length) : Infinity;

    for (let ti = 0; ti < types.length; ti++) {
      const type = types[ti];
      const roleId = type === 'dental' ? ROLE_DENTAL : ROLE_GP;
      const urlPrefix = type === 'dental' ? 'dentist' : 'gp-surgery';

      log.scrape(`NHS.UK: Enumerating ${type} practices (ODS role: ${roleId})`);

      let typeYielded = 0;
      let typeChecked = 0;

      // Collect ODS codes first
      const codes = [];
      for await (const org of this._enumerateOdsCodes(roleId, maxPagesPerType)) {
        codes.push(org);
      }
      totalOdsCodes += codes.length;

      log.success(`NHS.UK: ${codes.length.toLocaleString()} ${type} ODS codes collected`);

      yield { _cityProgress: { current: ti + 1, total: types.length } };

      // Fetch each NHS.UK profile page
      for (let i = 0; i < codes.length; i++) {
        const { odsCode, name } = codes[i];
        typeChecked++;

        // Progress logging every 100 practices
        if (typeChecked % 100 === 0) {
          log.info(`NHS.UK ${type}: ${typeChecked}/${codes.length} checked, ${typeYielded} found (${totalWithEmail} with email)`);
        }

        // Build profile URL
        const profileUrl = `${NHS_UK_BASE}/services/${urlPrefix}/x/${odsCode}`;

        let response;
        try {
          await rateLimiter.wait();
          const ua = rateLimiter.getUserAgent();
          response = await this._httpGet(profileUrl, ua);
        } catch (err) {
          totalErrors++;
          if (totalErrors % 50 === 0) {
            log.warn(`NHS.UK: ${totalErrors} errors so far (latest: ${err.message})`);
          }
          continue;
        }

        // Handle rate limiting
        if (response.statusCode === 429) {
          log.warn('NHS.UK: Rate limited — backing off 30s');
          await new Promise(r => setTimeout(r, 30000));
          // Retry once
          try {
            const ua = rateLimiter.getUserAgent();
            response = await this._httpGet(profileUrl, ua);
          } catch (err) {
            totalErrors++;
            continue;
          }
        }

        if (response.statusCode === 404) {
          total404++;
          continue;
        }

        if (response.statusCode !== 200) {
          totalErrors++;
          continue;
        }

        // Extract JSON-LD
        const jsonLd = this._extractJsonLd(response.body);
        if (!jsonLd) {
          totalErrors++;
          continue;
        }

        // Map to lead
        const lead = this._mapPractice(jsonLd, odsCode, type);
        lead.profile_url = profileUrl;

        // Track stats
        if (lead.email) totalWithEmail++;
        if (lead.phone) totalWithPhone++;
        if (lead.website) totalWithWebsite++;

        yield this.transformResult(lead, lead.practice_area);
        totalYielded++;
        typeYielded++;
      }

      log.success(`NHS.UK ${type}: ${typeYielded.toLocaleString()} practices found from ${typeChecked.toLocaleString()} codes (${total404} had no NHS.UK page)`);
    }

    log.success([
      'NHS.UK scrape complete:',
      `${totalOdsCodes.toLocaleString()} ODS codes enumerated`,
      `${totalYielded.toLocaleString()} practices yielded`,
      `(${totalWithEmail.toLocaleString()} with email,`,
      `${totalWithPhone.toLocaleString()} with phone,`,
      `${totalWithWebsite.toLocaleString()} with website)`,
      `${total404.toLocaleString()} pages not found`,
      `${totalErrors.toLocaleString()} errors`,
    ].join(' | '));
  }
}

module.exports = new NhsUkScraper();
