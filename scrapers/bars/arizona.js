/**
 * Arizona State Bar Association Scraper
 *
 * Source: https://www.azbar.org/find-a-lawyer/
 * API: https://api-proxy.azbar.org/MemberSearch/Search
 * Method: HTTP POST to JSON API
 *
 * The AZ Bar has a public JSON API at api-proxy.azbar.org that accepts
 * POST requests with search criteria (city, specialization, etc.) and
 * returns attorney records as JSON. The API is used by the front-end SPA.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ArizonaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'arizona',
      stateCode: 'AZ',
      baseUrl: 'https://api-proxy.azbar.org/MemberSearch/Search',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'Immigration and Naturalization Law',
        'family':               'Family Law',
        'family law':           'Family Law',
        'criminal':             'Criminal Law',
        'criminal defense':     'Criminal Law',
        'estate planning':      'Estate and Trust Planning',
        'estate':               'Estate and Trust Planning',
        'tax':                  'Taxation',
        'tax law':              'Taxation',
        'employment':           'Employment Law',
        'labor':                'Labor and Employment Law',
        'bankruptcy':           'Bankruptcy and Debtor-Creditor Law',
        'real estate':          'Real Property Law',
        'civil litigation':     'Civil Litigation',
        'business':             'Business Law',
        'corporate':            'Corporate Law',
        'elder':                'Elder Law',
        'intellectual property':'Intellectual Property Law',
        'personal injury':      'Personal Injury Litigation',
        'workers comp':         'Workers Compensation',
        'environmental':        'Environmental and Natural Resources Law',
        'health':               'Health Law',
        'construction':         'Construction Law',
        'insurance':            'Insurance Law',
        'medical malpractice':  'Medical Malpractice',
        'securities':           'Securities Law',
        'appellate':            'Appellate Practice',
        'juvenile':             'Juvenile Law',
        'dui':                  'DUI/DWI',
        'adoption':             'Adoption Law',
      },
      defaultCities: [
        'Phoenix', 'Tucson', 'Mesa', 'Scottsdale', 'Chandler',
        'Tempe', 'Gilbert', 'Glendale', 'Peoria', 'Flagstaff',
      ],
    });

    this.apiUrl = 'https://api-proxy.azbar.org/MemberSearch/Search';
    this.refererUrl = 'https://www.azbar.org/find-a-lawyer/';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for AZ Bar API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for AZ Bar API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for AZ Bar API`);
  }

  /**
   * POST JSON to the AZ Bar API and return parsed JSON response.
   *
   * The API may require headers like Origin/Referer to allow CORS-style requests.
   * We also attempt common auth header patterns in case an API key is needed.
   */
  _apiPost(body, rateLimiter, extraHeaders = {}) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const jsonBody = JSON.stringify(body);
      const parsed = new URL(this.apiUrl);

      const headers = {
        'User-Agent': ua,
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        'Origin': 'https://www.azbar.org',
        'Referer': this.refererUrl,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Content-Length': Buffer.byteLength(jsonBody),
        'Connection': 'keep-alive',
        ...extraHeaders,
      };

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers,
        timeout: 20000,
      };

      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(data), rawBody: data });
          } catch {
            resolve({ statusCode: res.statusCode, body: null, rawBody: data });
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(jsonBody);
      req.end();
    });
  }

  /**
   * Try to discover the API key from the AZ Bar website.
   * Checks the Find-a-Lawyer page source for API keys in JS bundles.
   */
  async _discoverApiKey(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.refererUrl, rateLimiter);

      if (response.statusCode !== 200) return null;

      // Look for API key patterns in the page source
      const keyPatterns = [
        /['"]?(?:x-api-key|apiKey|api_key|apikey|API_KEY)['"]?\s*[:=]\s*['"]([a-zA-Z0-9_\-]+)['"]/gi,
        /['"]?(?:Ocp-Apim-Subscription-Key)['"]?\s*[:=]\s*['"]([a-zA-Z0-9_\-]+)['"]/gi,
        /['"]?(?:Authorization)['"]?\s*[:=]\s*['"](?:Bearer\s+)?([a-zA-Z0-9_\-\.]+)['"]/gi,
      ];

      for (const pattern of keyPatterns) {
        const match = pattern.exec(response.body);
        if (match && match[1] && match[1].length > 8) {
          log.info(`Discovered potential API key from page source (${match[1].substring(0, 8)}...)`);
          return match[1];
        }
      }

      // Check linked JS files for API keys
      const cheerio = require('cheerio');
      const $ = cheerio.load(response.body);
      const scriptSrcs = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        if (src.includes('chunk') || src.includes('main') || src.includes('app') || src.includes('bundle')) {
          if (src.startsWith('/')) {
            scriptSrcs.push(`https://www.azbar.org${src}`);
          } else if (src.startsWith('http')) {
            scriptSrcs.push(src);
          }
        }
      });

      // Check up to 3 JS bundles
      for (const jsSrc of scriptSrcs.slice(0, 3)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(jsSrc, rateLimiter);
          if (jsResp.statusCode === 200) {
            for (const pattern of keyPatterns) {
              pattern.lastIndex = 0;
              const match = pattern.exec(jsResp.body);
              if (match && match[1] && match[1].length > 8) {
                log.info(`Discovered API key from JS bundle (${match[1].substring(0, 8)}...)`);
                return match[1];
              }
            }
          }
        } catch {
          // JS bundle fetch failed, continue
        }
      }

      return null;
    } catch (err) {
      log.warn(`API key discovery failed: ${err.message}`);
      return null;
    }
  }

  /**
   * Build the search payload for the AZ Bar API.
   */
  _buildSearchPayload(city, practiceCode, skip = 0) {
    const payload = {
      city: city || '',
      state: 'AZ',
      firstName: '',
      lastName: '',
      firmName: '',
      zipCode: '',
      county: '',
      jurisdiction: '',
      language: '',
      lawSchool: '',
      legalNeed: practiceCode || '',
      specialization: '',
      pageSize: this.pageSize,
      skip: skip,
    };

    // If the practice code looks like a specialization (longer, more specific), put it there instead
    if (practiceCode && practiceCode.includes('Certification')) {
      payload.legalNeed = '';
      payload.specialization = practiceCode;
    }

    return payload;
  }

  /**
   * Normalize an API response record into our standard attorney object format.
   */
  _normalizeRecord(record) {
    // The API can return different field name patterns
    const get = (obj, keys) => {
      for (const key of keys) {
        if (obj[key] !== undefined && obj[key] !== null) return String(obj[key]).trim();
        // Case-insensitive match
        const found = Object.keys(obj).find(k => k.toLowerCase() === key.toLowerCase());
        if (found && obj[found] !== undefined && obj[found] !== null) return String(obj[found]).trim();
      }
      return '';
    };

    const firstName = get(record, ['firstName', 'FirstName', 'first_name', 'fname']);
    const lastName = get(record, ['lastName', 'LastName', 'last_name', 'lname']);
    const fullName = get(record, ['fullName', 'FullName', 'full_name', 'name', 'Name']) || `${firstName} ${lastName}`.trim();
    const firmName = get(record, ['firmName', 'FirmName', 'firm_name', 'firm', 'Firm', 'company', 'Company']);
    const city = get(record, ['city', 'City']);
    const state = get(record, ['state', 'State']) || 'AZ';
    const phone = get(record, ['phone', 'Phone', 'phoneNumber', 'PhoneNumber', 'telephone', 'Telephone', 'officePhone']);
    const email = get(record, ['email', 'Email', 'emailAddress', 'EmailAddress']);
    const website = get(record, ['website', 'Website', 'webAddress', 'WebAddress', 'url', 'URL']);
    const barNumber = get(record, ['barNumber', 'BarNumber', 'bar_number', 'memberNumber', 'MemberNumber', 'licenseNumber', 'id', 'Id']);
    const barStatus = get(record, ['status', 'Status', 'memberStatus', 'MemberStatus', 'barStatus', 'BarStatus']);
    const admissionDate = get(record, ['admissionDate', 'AdmissionDate', 'admission_date', 'admitDate', 'AdmitDate', 'dateAdmitted']);
    const zip = get(record, ['zip', 'Zip', 'zipCode', 'ZipCode', 'postalCode', 'PostalCode']);

    // Derive first/last from fullName if missing
    let fName = firstName;
    let lName = lastName;
    if (!fName && !lName && fullName) {
      if (fullName.includes(',')) {
        const parts = fullName.split(',');
        lName = parts[0].trim();
        fName = (parts[1] || '').trim().split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        fName = split.firstName;
        lName = split.lastName;
      }
    }

    return {
      first_name: fName,
      last_name: lName,
      full_name: fullName || `${fName} ${lName}`.trim(),
      firm_name: firmName,
      city: city,
      state: state,
      zip: zip,
      phone: phone,
      email: email,
      website: website,
      bar_number: barNumber,
      bar_status: barStatus,
      admission_date: admissionDate,
      profile_url: barNumber ? `https://www.azbar.org/find-a-lawyer/member-profile/${barNumber}` : '',
      source: `${this.name}_bar`,
    };
  }

  /**
   * Extract the array of attorney records from the API response.
   * The API response structure may vary; this handles common patterns.
   */
  _extractRecords(responseBody) {
    if (!responseBody) return { records: [], total: 0 };

    // Direct array response
    if (Array.isArray(responseBody)) {
      return { records: responseBody, total: responseBody.length };
    }

    // Common wrapper patterns
    const arrayKeys = ['results', 'Results', 'members', 'Members', 'data', 'Data',
                       'attorneys', 'Attorneys', 'records', 'Records', 'items', 'Items',
                       'lawyers', 'Lawyers', 'searchResults', 'SearchResults'];
    const totalKeys = ['totalCount', 'TotalCount', 'total', 'Total', 'totalRows', 'TotalRows',
                       'totalResults', 'TotalResults', 'count', 'Count', 'recordCount', 'RecordCount'];

    let records = [];
    let total = 0;

    for (const key of arrayKeys) {
      if (Array.isArray(responseBody[key])) {
        records = responseBody[key];
        break;
      }
    }

    for (const key of totalKeys) {
      if (typeof responseBody[key] === 'number') {
        total = responseBody[key];
        break;
      }
    }

    if (records.length === 0 && total === 0) {
      // Maybe the entire response IS the record array wrapped in an object
      // Check if the response has typical attorney fields
      if (responseBody.firstName || responseBody.lastName || responseBody.name) {
        records = [responseBody];
        total = 1;
      }
    }

    return { records, total: total || records.length };
  }

  /**
   * Async generator that yields attorney records from the AZ Bar API.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    // Try to discover API key
    log.info('Checking AZ Bar API for authentication requirements...');
    const apiKey = await this._discoverApiKey(rateLimiter);

    const extraHeaders = {};
    if (apiKey) {
      // Try common header patterns
      extraHeaders['x-api-key'] = apiKey;
      log.info('Using discovered API key');
    }

    // First, test the API with a simple request
    log.info('Testing AZ Bar API connectivity...');
    try {
      await rateLimiter.wait();
      const testPayload = this._buildSearchPayload('Phoenix', null, 0);
      testPayload.pageSize = 1;
      const testResp = await this._apiPost(testPayload, rateLimiter, extraHeaders);

      if (testResp.statusCode === 401 || testResp.statusCode === 403) {
        // Try without API key or with different auth patterns
        log.warn(`API returned ${testResp.statusCode} — trying without API key`);
        delete extraHeaders['x-api-key'];

        await rateLimiter.wait();
        const retryResp = await this._apiPost(testPayload, rateLimiter, {});
        if (retryResp.statusCode === 401 || retryResp.statusCode === 403) {
          // Try with Ocp-Apim-Subscription-Key (Azure API Management)
          if (apiKey) {
            extraHeaders['Ocp-Apim-Subscription-Key'] = apiKey;
            await rateLimiter.wait();
            const azureResp = await this._apiPost(testPayload, rateLimiter, extraHeaders);
            if (azureResp.statusCode !== 200) {
              delete extraHeaders['Ocp-Apim-Subscription-Key'];
              log.error(`AZ Bar API requires authentication — cannot proceed`);
              return;
            }
          } else {
            log.error(`AZ Bar API requires authentication and no key was found — cannot proceed`);
            return;
          }
        }
      } else if (testResp.statusCode === 200) {
        log.success('AZ Bar API is accessible');
      } else {
        log.warn(`AZ Bar API test returned status ${testResp.statusCode} — attempting to continue`);
      }
    } catch (err) {
      log.error(`AZ Bar API test failed: ${err.message} — attempting to continue`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let skip = 0;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const payload = this._buildSearchPayload(city, practiceCode, skip);
        log.info(`Page ${pagesFetched + 1} — POST ${this.apiUrl} [city=${city}, skip=${skip}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiPost(payload, rateLimiter, extraHeaders);
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
          if (response.rawBody) {
            log.info(`Response body: ${response.rawBody.substring(0, 200)}`);
          }
          break;
        }

        rateLimiter.resetBackoff();

        // Check if response is HTML (potential CAPTCHA/block page)
        if (typeof response.rawBody === 'string' && this.detectCaptcha(response.rawBody)) {
          log.warn(`CAPTCHA detected for ${city} — skipping`);
          yield { _captcha: true, city };
          break;
        }

        const { records, total } = this._extractRecords(response.body);

        if (pagesFetched === 0) {
          totalResults = total;
          if (records.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
        }

        if (records.length === 0) {
          log.info(`No more results for ${city}`);
          break;
        }

        for (const record of records) {
          const attorney = this._normalizeRecord(record);

          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        skip += records.length;
        pagesFetched++;

        // Check if we've fetched all results
        if (skip >= totalResults || records.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }
      }
    }
  }
}

module.exports = new ArizonaScraper();
