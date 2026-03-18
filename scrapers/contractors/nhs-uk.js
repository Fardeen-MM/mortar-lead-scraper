/**
 * NHS.UK Practice Scraper — GP Surgeries & Dental Practices (England)
 *
 * Source: https://www.nhs.uk/services/
 * Records: ~12,800 GP surgeries + ~12,000 dental practices (England only)
 * Method: Two-pronged approach depending on practice type
 *
 * === GP Practices ===
 * 1. Enumerate all active GP ODS codes from the NHS Spine Directory API:
 *    - PrimaryRoleId=RO177 ("PRESCRIBING COST CENTRE") → ~12,800 codes
 *    - Pagination via next-page response header
 * 2. Fetch each NHS.UK profile page: /services/gp-surgery/x/{ODS_CODE}
 *    - Uses 'x' as dummy slug — NHS.UK 302 redirects to the correct slug
 *    - ~95% of ODS codes have a corresponding NHS.UK page
 *    - ~50% of those pages contain an email address
 *
 * === Dental Practices ===
 * The ODS Spine Directory uses V-prefix codes (RO110) that do NOT match
 * NHS.UK pages. NHS.UK uses its own XV-prefix codes for dentists.
 * Strategy: Crawl the NHS.UK dentist search endpoint using ~200 UK postcodes
 * across all regions, each returning ~50 nearby practices.
 * - Search URL: /service-search/find-a-dentist/results/{POSTCODE}?distance=25
 * - Extract XV codes from result links: /services/dentist/{slug}/{XV_CODE}
 * - Deduplicate codes, then fetch each profile page
 * - ~12,000 unique dental practices discoverable across all postcodes
 *
 * === Structured Data ===
 * Each NHS.UK profile page contains a JSON-LD script tag (id="json-ld-script")
 * with @type "Physician" (GP) or "Dentist" (dental), plus:
 * name, telephone, email, url (website), address, geo coordinates.
 *
 * NOTE: The script tag type attribute uses HTML entity: ld&#x2B;json
 * We match by id="json-ld-script" instead of type attribute.
 *
 * Practice area filter:
 *   - "dental" or "dentist" → only dental practices
 *   - "gp" or "general practice" → only GP surgeries
 *   - default (no filter) → both dental and GP
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// ODS Spine Directory API
const ODS_API_BASE = 'https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations';

// NHS.UK base
const NHS_UK_BASE = 'https://www.nhs.uk';

// ODS Role IDs
const ROLE_GP = 'RO177'; // PRESCRIBING COST CENTRE (GP surgeries)

// ODS page size (max 100)
const ODS_PAGE_SIZE = 100;

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

const DEFAULT_CITIES = [];

/**
 * UK postcodes covering all regions of England.
 * Used to discover dental practice XV codes via the NHS.UK search endpoint.
 * Each postcode search returns ~50 nearby practices within distance=25 miles.
 * 200+ postcodes ensures comprehensive coverage with significant overlap for dedup.
 */
const DISCOVERY_POSTCODES = [
  // London boroughs
  'E1','E2','E3','E4','E5','E6','E7','E8','E9','E10','E11','E12','E13','E14','E15','E16','E17','E18',
  'EC1','EC2','EC3','EC4',
  'N1','N2','N3','N4','N5','N6','N7','N8','N9','N10','N11','N12','N13','N14','N15','N16','N17','N18','N19','N20','N21','N22',
  'NW1','NW2','NW3','NW4','NW5','NW6','NW7','NW8','NW9','NW10','NW11',
  'SE1','SE2','SE3','SE4','SE5','SE6','SE7','SE8','SE9','SE10','SE11','SE12','SE13','SE14','SE15','SE16','SE17','SE18','SE19','SE20','SE21','SE22','SE23','SE24','SE25','SE26','SE27','SE28',
  'SW1','SW2','SW3','SW4','SW5','SW6','SW7','SW8','SW9','SW10','SW11','SW12','SW13','SW14','SW15','SW16','SW17','SW18','SW19','SW20',
  'W1','W2','W3','W4','W5','W6','W7','W8','W9','W10','W11','W12','W13','W14',
  'WC1','WC2',
  // Greater London outer
  'BR1','CR0','DA1','EN1','HA1','IG1','KT1','RM1','SM1','TW1','UB1','WD1',
  // South East
  'BN1','BN2','CT1','GU1','HP1','ME1','MK1','OX1','PO1','RG1','RH1','SL1','SO1','SP1','TN1',
  // South West
  'BA1','BH1','BS1','DT1','EX1','GL1','PL1','SN1','TA1','TQ1','TR1',
  // East of England
  'AL1','CB1','CM1','CO1','IP1','LU1','NR1','PE1','SG1','SS1',
  // East Midlands
  'DE1','LE1','LN1','NG1','NN1',
  // West Midlands
  'B1','B2','B3','B4','B5','CV1','DY1','HR1','ST1','TF1','WR1','WS1','WV1',
  // North West
  'BB1','BL1','CA1','CH1','CW1','FY1','L1','L2','L3','L4','LA1','M1','M2','M3','M4','OL1','PR1','SK1','WA1','WN1',
  // Yorkshire and Humber
  'BD1','DN1','HD1','HG1','HU1','HX1','LS1','S1','S2','WF1','YO1',
  // North East
  'DH1','DL1','NE1','NE2','NE3','SR1','TS1',
];


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
   * Enumerate all active GP ODS codes via the Spine Directory API.
   * Returns array of { odsCode, name, postCode }.
   */
  async _enumerateGpCodes(maxPages = Infinity) {
    const codes = [];
    let url = `${ODS_API_BASE}?PrimaryRoleId=${ROLE_GP}&Status=Active&Limit=${ODS_PAGE_SIZE}`;
    let page = 0;

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
        codes.push({ odsCode: org.OrgId, name: org.Name, postCode: org.PostCode });
      }

      url = resp.nextPage || null;
      page++;
    }

    log.info(`ODS: Enumerated ${codes.length.toLocaleString()} GP codes across ${page} pages`);
    return codes;
  }

  /**
   * Discover dental practice XV codes by crawling NHS.UK search results.
   * Searches ~200 UK postcodes with distance=25 to find all dental practices.
   * Returns array of unique XV codes.
   */
  async _discoverDentalCodes(rateLimiter, maxPostcodes = Infinity) {
    const allCodes = new Set();
    const postcodes = DISCOVERY_POSTCODES.slice(0, Math.min(maxPostcodes, DISCOVERY_POSTCODES.length));

    log.info(`NHS.UK: Discovering dental codes from ${postcodes.length} postcode searches`);

    for (let i = 0; i < postcodes.length; i++) {
      const pc = postcodes[i];

      if ((i + 1) % 25 === 0) {
        log.info(`NHS.UK dental discovery: ${i + 1}/${postcodes.length} postcodes searched, ${allCodes.size} unique codes found`);
      }

      const searchUrl = `${NHS_UK_BASE}/service-search/find-a-dentist/results/${encodeURIComponent(pc)}?distance=25`;

      let response;
      try {
        await rateLimiter.wait();
        const ua = rateLimiter.getUserAgent();
        response = await this._httpGet(searchUrl, ua);
      } catch (err) {
        continue; // Skip this postcode
      }

      if (response.statusCode !== 200) continue;

      // Extract XV codes from dental practice links
      const pattern = /\/services\/dentist\/[\w-]+\/(XV[\dA-Z]+)/g;
      let match;
      while ((match = pattern.exec(response.body)) !== null) {
        allCodes.add(match[1]);
      }
    }

    const codes = [...allCodes];
    log.success(`NHS.UK: Discovered ${codes.length.toLocaleString()} unique dental practice codes from ${postcodes.length} postcodes`);
    return codes;
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
   * Parse the street address from JSON-LD into components.
   * NHS.UK format: "Street, Area, City, County"
   *
   * Heuristic: Walk backwards through the comma-separated parts.
   * Skip parts that look like counties (Greater London, Hertfordshire, etc.)
   * Skip parts that look like street addresses (contain digits or Road/Street/Lane).
   * The first part that looks like a place name is the city.
   */
  _parseAddress(streetAddress) {
    if (!streetAddress) return { address: '', city: '' };

    const parts = streetAddress.split(',').map(p => p.trim()).filter(Boolean);
    let city = '';

    // Walk backwards to find the city (skip counties and street-like parts)
    const countyRe = /\b(greater|county|south east|south west|north east|north west|east of|west of)\b|shire$/i;
    const streetRe = /\b(road|street|lane|drive|avenue|close|place|way|terrace|court|square|crescent|row|mews|hill|park|house|centre|floor|heath|suite|unit)\b/i;
    const hasDigit = /\d/;

    for (let i = parts.length - 1; i >= 0; i--) {
      const part = parts[i];
      // Skip if it looks like a county/region
      if (countyRe.test(part)) continue;
      // Skip if it starts with a digit (street address)
      if (hasDigit.test(part.charAt(0))) continue;
      // Skip if it contains street-like words AND digits
      if (streetRe.test(part) && hasDigit.test(part)) continue;
      // Skip if it's clearly a building/street name (has digits anywhere)
      if (hasDigit.test(part) && parts.length > 2) continue;
      // Accept this as city
      city = part;
      break;
    }

    // Fallback: if nothing matched, use the last part
    if (!city && parts.length > 0) {
      city = parts[parts.length - 1];
    }

    return { address: streetAddress, city };
  }

  /**
   * Normalize a UK phone number.
   * 02087992525 -> 020 8799 2525, 01234567890 -> 01234 567890
   */
  _normalizePhone(raw) {
    if (!raw) return '';
    const digits = raw.replace(/[^\d]/g, '');
    if (digits.length === 11 && digits[0] === '0') {
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
   * Map JSON-LD data to a normalized lead object.
   */
  _mapPractice(jsonLd, code, practiceType) {
    const addr = jsonLd.address || {};
    const geo = jsonLd.geo || {};
    const parsed = this._parseAddress(addr.streetAddress);

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
      bar_number: code,
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
   * Fetch a single NHS.UK profile page, extract JSON-LD, return mapped lead.
   * Returns lead object or null on failure.
   */
  async _fetchProfileAndMap(code, practiceType, rateLimiter) {
    const prefix = practiceType === 'dental' ? 'dentist' : 'gp-surgery';
    const profileUrl = `${NHS_UK_BASE}/services/${prefix}/x/${code}`;

    let response;
    try {
      await rateLimiter.wait();
      const ua = rateLimiter.getUserAgent();
      response = await this._httpGet(profileUrl, ua);
    } catch (err) {
      return { error: 'fetch', code };
    }

    if (response.statusCode === 429) {
      // Rate limited — back off 30s and retry once
      log.warn('NHS.UK: Rate limited — backing off 30s');
      await new Promise(r => setTimeout(r, 30000));
      try {
        const ua = rateLimiter.getUserAgent();
        response = await this._httpGet(profileUrl, ua);
      } catch (err) {
        return { error: 'fetch', code };
      }
    }

    if (response.statusCode === 404) return { error: '404', code };
    if (response.statusCode !== 200) return { error: 'http', code };

    const jsonLd = this._extractJsonLd(response.body);
    if (!jsonLd) return { error: 'jsonld', code };

    const lead = this._mapPractice(jsonLd, code, practiceType);
    lead.profile_url = profileUrl;
    return { lead };
  }

  /**
   * Main search generator.
   *
   * GP: Enumerate ODS codes → fetch NHS.UK profile pages
   * Dental: Discover XV codes via postcode search → fetch profile pages
   *
   * Practice area filter:
   *   "dental" → dental only
   *   "gp" → GP only
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

    const maxPages = options.maxPages || Infinity;

    for (let ti = 0; ti < types.length; ti++) {
      const type = types[ti];

      yield { _cityProgress: { current: ti + 1, total: types.length } };

      let codes; // Array of code strings

      if (type === 'gp') {
        // GP: enumerate ODS codes
        const maxOdsPages = options.maxPages ? Math.min(options.maxPages, 200) : Infinity;
        log.scrape(`NHS.UK: Enumerating GP practices via ODS API`);
        const gpCodes = await this._enumerateGpCodes(maxOdsPages);
        codes = gpCodes.map(c => c.odsCode);
      } else {
        // Dental: discover XV codes from NHS.UK search
        const maxPostcodes = options.maxPages ? Math.min(options.maxPages * 5, DISCOVERY_POSTCODES.length) : DISCOVERY_POSTCODES.length;
        log.scrape(`NHS.UK: Discovering dental practices via postcode search`);
        codes = await this._discoverDentalCodes(rateLimiter, maxPostcodes);
      }

      log.success(`NHS.UK: ${codes.length.toLocaleString()} ${type} codes to process`);

      let typeYielded = 0;

      for (let i = 0; i < codes.length; i++) {
        const code = codes[i];

        // Progress logging every 100 practices
        if ((i + 1) % 100 === 0) {
          log.info(`NHS.UK ${type}: ${i + 1}/${codes.length} checked, ${typeYielded} yielded (${totalWithEmail} with email)`);
        }

        const result = await this._fetchProfileAndMap(code, type, rateLimiter);

        if (result.error) {
          if (result.error === '404') total404++;
          else totalErrors++;
          continue;
        }

        const lead = result.lead;

        // Track stats
        if (lead.email) totalWithEmail++;
        if (lead.phone) totalWithPhone++;
        if (lead.website) totalWithWebsite++;

        yield this.transformResult(lead, lead.practice_area);
        totalYielded++;
        typeYielded++;
      }

      log.success(`NHS.UK ${type}: ${typeYielded.toLocaleString()} practices from ${codes.length.toLocaleString()} codes`);
    }

    log.success([
      'NHS.UK scrape complete:',
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
