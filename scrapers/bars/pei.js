/**
 * Prince Edward Island Law Society Scraper
 *
 * Source: https://lawsocietypei.ca/find-a-lawyer
 * Method: PHP JSON API at bin/find-a-lawyer.api.php — POST with FormData
 * ~200 lawyers total
 *
 * The API accepts POST with FormData fields: search[name], search[firm], search[city].
 * Returns JSON arrays with fields: id, fullname, lspei_membershiptype,
 * lspei_companyname, address2_city, address2_stateorprovince,
 * lspei_businessphonenumber, emailaddress1.
 *
 * Profile data is fetched via the same API with POST field: profile={id}.
 * Profile responses include additional fields: lspei_baradmissiondate,
 * membership_type_name, address2_line1, address2_postalcode,
 * lspei_businessfaxnumber, and emailaddress1 (often only in profile).
 *
 * Overrides search() to query the JSON API directly (no HTML parsing needed).
 * Overrides fetchProfilePage() / parseProfilePage() for JSON-based profile enrichment.
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class PEIScraper extends BaseScraper {
  constructor() {
    super({
      name: 'pei',
      stateCode: 'CA-PE',
      baseUrl: 'https://lawsocietypei.ca/',
      pageSize: 100,
      practiceAreaCodes: {
        'family':                'family',
        'family law':            'family',
        'criminal':              'criminal',
        'criminal defense':      'criminal',
        'real estate':           'real-estate',
        'corporate/commercial':  'corporate-commercial',
        'corporate':             'corporate-commercial',
        'commercial':            'corporate-commercial',
        'personal injury':       'personal-injury',
        'employment':            'employment',
        'labour':                'employment',
        'immigration':           'immigration',
        'estate planning/wills': 'wills-estates',
        'estate planning':       'wills-estates',
        'wills':                 'wills-estates',
        'intellectual property': 'intellectual-property',
        'civil litigation':      'civil-litigation',
        'litigation':            'civil-litigation',
        'tax':                   'tax',
        'administrative':        'administrative',
        'environmental':         'environmental',
      },
      defaultCities: [
        'Charlottetown', 'Summerside',
      ],
    });

    this.apiUrl = 'https://lawsocietypei.ca/bin/find-a-lawyer.api.php';
  }

  /**
   * HTTP POST with multipart/form-data (URL-encoded fallback) for the PEI API.
   */
  httpPostForm(url, formFields, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);

      // Build multipart/form-data body
      const boundary = '----FormBoundary' + Math.random().toString(36).substring(2);
      let body = '';
      for (const [key, value] of Object.entries(formFields)) {
        body += `--${boundary}\r\n`;
        body += `Content-Disposition: form-data; name="${key}"\r\n\r\n`;
        body += `${value}\r\n`;
      }
      body += `--${boundary}--\r\n`;

      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': Buffer.byteLength(body),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json,*/*',
          'Origin': 'https://lawsocietypei.ca',
          'Referer': 'https://lawsocietypei.ca/find-a-lawyer',
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        let responseBody = '';
        res.on('data', c => responseBody += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body: responseBody }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(body);
      req.end();
    });
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Override fetchProfilePage to handle JSON API-based profile fetching.
   *
   * Profile data is fetched by POSTing { profile: id } to the same API endpoint.
   * The response is a JSON array (not HTML), so we return the parsed JSON object
   * instead of a Cheerio instance.
   *
   * @param {string} url - Profile URL (pei-api://{id} format)
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {object|null} Parsed profile JSON object or null on failure
   */
  async fetchProfilePage(url, rateLimiter) {
    if (!url) return null;

    // Parse the pei-api:// pseudo URL
    const match = url.match(/^pei-api:\/\/(\d+)$/);
    if (!match) {
      log.warn(`${this.name}: Invalid profile URL format: ${url}`);
      return null;
    }

    const lawyerId = match[1];

    try {
      await rateLimiter.wait();
      const response = await this.httpPostForm(this.apiUrl, {
        'profile': lawyerId,
      }, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`${this.name}: Profile API returned ${response.statusCode} for id=${lawyerId}`);
        return null;
      }

      const data = JSON.parse(response.body);

      // API returns an array; take the first element
      const profile = Array.isArray(data) ? data[0] : data;
      if (!profile || !profile.fullname) {
        log.warn(`${this.name}: Empty profile response for id=${lawyerId}`);
        return null;
      }

      return profile;
    } catch (err) {
      log.warn(`${this.name}: Failed to fetch profile for id=${lawyerId}: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a PEI profile response for additional contact info.
   *
   * The profile API returns JSON with these fields:
   *   - fullname                     — Full name
   *   - lspei_membershiptype         — Membership type code (e.g. 848150001)
   *   - membership_type_name         — Status description (e.g. "Practising Lawyer")
   *   - lspei_baradmissiondate       — Admission date (e.g. "1990-06-01")
   *   - lspei_companyname            — Firm/company name
   *   - address2_line1               — Street address
   *   - address2_city                — City
   *   - address2_stateorprovince     — Province
   *   - address2_postalcode          — Postal code
   *   - lspei_businessphonenumber    — Phone
   *   - lspei_businessfaxnumber      — Fax
   *   - emailaddress1                — Email
   *
   * NOTE: The parameter here is a JSON object, NOT a Cheerio instance.
   * This is because PEI profiles are fetched via JSON API, not HTML pages.
   *
   * @param {object} profile - Parsed JSON profile object (from fetchProfilePage)
   * @returns {object} Additional fields: email, phone, firm_name, address, admission_date, fax
   */
  parseProfilePage(profile) {
    if (!profile || typeof profile !== 'object') return {};

    const result = {};

    // Email — often only available in the profile response
    const email = (profile.emailaddress1 || '').trim().toLowerCase();
    if (email && email.includes('@')) {
      result.email = email;
    }

    // Phone
    const phone = (profile.lspei_businessphonenumber || '').trim();
    if (phone) {
      result.phone = phone;
    }

    // Fax
    const fax = (profile.lspei_businessfaxnumber || '').trim();
    if (fax) {
      result.fax = fax;
    }

    // Firm name
    const firmName = (profile.lspei_companyname || '').trim();
    if (firmName) {
      result.firm_name = firmName;
    }

    // Bar status (descriptive name)
    const statusName = (profile.membership_type_name || '').trim();
    if (statusName) {
      result.bar_status = statusName;
    }

    // Admission date
    const admissionDate = (profile.lspei_baradmissiondate || '').trim();
    if (admissionDate) {
      // Format: "1990-06-01" or similar — store as-is
      result.admission_date = admissionDate;
    }

    // Build address from address2 fields
    const addrParts = [
      (profile.address2_line1 || '').trim(),
      (profile.address2_city || '').trim(),
      (profile.address2_stateorprovince || '').trim(),
      (profile.address2_postalcode || '').trim(),
    ].filter(Boolean);

    if (addrParts.length > 0) {
      result.address = addrParts.join(', ');
    }

    return result;
  }

  /**
   * Async generator that yields attorney records from the PEI JSON API.
   * The API accepts POST with FormData: search[name], search[firm], search[city].
   * Returns JSON arrays with known fields.
   */
  /**
   * Map Dynamics CRM membership type codes to human-readable labels.
   * Profile API returns membership_type_name, but search results only have the numeric code.
   */
  _resolveStatus(code) {
    if (!code) return 'Active';
    const str = String(code).trim();
    // If it's already a text label, return as-is
    if (!/^\d+$/.test(str)) return str;
    const STATUS_MAP = {
      '848150000': 'Non-Practising',
      '848150001': 'Practising Lawyer',
      '848150002': 'Practising Lawyer (Restricted)',
      '848150004': 'Retired',
      '848150005': 'Honorary Member',
      '848150006': 'Student-at-Law',
      '848150008': 'Canadian Legal Advisor',
      '848150009': 'Suspended',
      '848150010': 'Resigned',
      '848150015': 'Active',
    };
    return STATUS_MAP[str] || 'Active';
  }

  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} -- searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching PEI lawyers in ${city}`);

      // POST to the JSON API with FormData fields
      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpPostForm(this.apiUrl, {
          'search[name]': '',
          'search[firm]': '',
          'search[city]': city,
        }, rateLimiter);
      } catch (err) {
        log.error(`Request failed: ${err.message}`);
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} -- skipping ${city}`);
        continue;
      }

      rateLimiter.resetBackoff();

      let records;
      try {
        records = JSON.parse(response.body);
      } catch (err) {
        log.error(`Failed to parse JSON response: ${err.message}`);
        continue;
      }

      if (!Array.isArray(records)) {
        records = records.data || records.results || records.lawyers || records.members || [];
      }

      log.success(`Fetched ${records.length} PEI lawyers for ${city}`);

      let cityCount = 0;
      for (const rec of records) {
        // Map the known API fields to our standard attorney object
        const fullName = (rec.fullname || '').trim();
        if (!fullName) continue;

        const { firstName, lastName } = this.splitName(fullName);

        // Build profile_url using the API id field
        // Format: pei-api://{id} — a pseudo-URL handled by fetchProfilePage()
        const lawyerId = (rec.id || '').toString().trim();
        const profileUrl = lawyerId ? `pei-api://${lawyerId}` : '';

        const attorney = {
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: (rec.lspei_companyname || '').trim(),
          city: (rec.address2_city || city).trim(),
          state: 'CA-PE',
          phone: (rec.lspei_businessphonenumber || '').trim(),
          email: (rec.emailaddress1 || '').trim(),
          website: '',
          bar_number: lawyerId,
          bar_status: this._resolveStatus(rec.lspei_membershiptype),
          profile_url: profileUrl,
        };

        // Apply min year filter
        if (options.minYear && rec.admission_date) {
          const year = parseInt((rec.admission_date.toString().match(/\d{4}/) || ['0'])[0], 10);
          if (year > 0 && year < options.minYear) continue;
        }

        cityCount++;
        yield this.transformResult(attorney, practiceArea);

        // Respect maxPages (treat each batch of pageSize as a "page")
        if (options.maxPages && cityCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max results limit for ${city}`);
          break;
        }
      }

      if (cityCount === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      } else {
        log.success(`Found ${cityCount} lawyers in ${city}`);
      }
    }
  }
}

module.exports = new PEIScraper();
