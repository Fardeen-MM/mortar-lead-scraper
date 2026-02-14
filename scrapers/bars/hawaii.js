/**
 * Hawaii State Bar Association Scraper
 *
 * Source: https://hsba.org/find-a-lawyer
 * Method: Algolia search-as-a-service API
 *
 * The HSBA Find-a-Lawyer directory is powered by Algolia. The public Algolia
 * Application ID is PE7QKUXU6Z. The search API key is extracted from the
 * page source/JS at runtime, then we query the Algolia REST API directly.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class HawaiiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'hawaii',
      stateCode: 'HI',
      baseUrl: 'https://hsba.org/find-a-lawyer',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'Immigration',
        'family':               'Family Law',
        'family law':           'Family Law',
        'criminal':             'Criminal Law',
        'criminal defense':     'Criminal Defense',
        'estate planning':      'Estate Planning',
        'estate':               'Estate Planning',
        'tax':                  'Tax Law',
        'tax law':              'Tax Law',
        'employment':           'Employment Law',
        'labor':                'Labor Law',
        'bankruptcy':           'Bankruptcy',
        'real estate':          'Real Estate',
        'civil litigation':     'Civil Litigation',
        'business':             'Business Law',
        'corporate':            'Corporate Law',
        'elder':                'Elder Law',
        'intellectual property':'Intellectual Property',
        'personal injury':      'Personal Injury',
        'workers comp':         'Workers Compensation',
        'environmental':        'Environmental Law',
        'health':               'Health Law',
        'construction':         'Construction Law',
        'insurance':            'Insurance',
        'medical malpractice':  'Medical Malpractice',
        'appellate':            'Appellate',
        'administrative':       'Administrative Law',
      },
      defaultCities: [
        'Honolulu', 'Hilo', 'Kailua', 'Pearl City',
        'Waipahu', 'Kaneohe', 'Kapolei', 'Wailuku',
      ],
    });

    this.algoliaAppId = 'PE7QKUXU6Z';
    this.algoliaApiUrl = `https://${this.algoliaAppId}-dsn.algolia.net/1/indexes/*/queries`;
    this.algoliaApiKey = null; // Discovered at runtime from page source
    this.findLawyerUrl = 'https://hsba.org/find-a-lawyer';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Algolia API`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Algolia API`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Algolia API`);
  }

  /**
   * POST to the Algolia API and return parsed JSON response.
   */
  _algoliaPost(body, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const jsonBody = JSON.stringify(body);
      const parsed = new URL(this.algoliaApiUrl);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'x-algolia-application-id': this.algoliaAppId,
          'x-algolia-api-key': this.algoliaApiKey,
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Length': Buffer.byteLength(jsonBody),
          'Origin': 'https://hsba.org',
          'Referer': 'https://hsba.org/',
          'Connection': 'keep-alive',
        },
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
   * Discover the Algolia search API key from the HSBA website.
   *
   * The search API key is typically embedded in:
   *  - Inline <script> tags as part of Algolia client initialization
   *  - JS bundle files loaded by the page
   *  - Data attributes on search-related DOM elements
   */
  async _discoverAlgoliaKey(rateLimiter) {
    log.info('Fetching HSBA page to extract Algolia search API key...');

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.findLawyerUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`HSBA page returned status ${response.statusCode}`);
        return null;
      }

      const pageSource = response.body;

      // Pattern 1: Algolia key in inline JS — algoliasearch('APP_ID', 'API_KEY')
      const algoliaInitPatterns = [
        /algoliasearch\s*\(\s*['"][^'"]+['"]\s*,\s*['"]([a-zA-Z0-9]+)['"]\s*\)/g,
        /algolia[._]?(?:search)?[._]?(?:api)?[._]?key['":\s]*['"]?([a-zA-Z0-9]{20,})/gi,
        /x-algolia-api-key['":\s]*['"]?([a-zA-Z0-9]{20,})/gi,
        /apiKey['":\s]*['"]([a-zA-Z0-9]{20,})['"]/gi,
        /searchKey['":\s]*['"]([a-zA-Z0-9]{20,})['"]/gi,
        /search_api_key['":\s]*['"]([a-zA-Z0-9]{20,})['"]/gi,
        /ALGOLIA_SEARCH_KEY['":\s]*['"]([a-zA-Z0-9]{20,})['"]/gi,
        /algoliaSearchKey['":\s]*['"]([a-zA-Z0-9]{20,})['"]/gi,
      ];

      for (const pattern of algoliaInitPatterns) {
        let match;
        while ((match = pattern.exec(pageSource)) !== null) {
          const key = match[1];
          // Algolia search keys are typically 32+ hex chars but can vary
          if (key && key.length >= 16 && key !== this.algoliaAppId) {
            log.success(`Found Algolia API key in page source: ${key.substring(0, 8)}...`);
            return key;
          }
        }
      }

      // Pattern 2: Check data attributes on DOM elements
      const cheerio = require('cheerio');
      const $ = cheerio.load(pageSource);

      const dataAttrPatterns = [
        '[data-algolia-api-key]',
        '[data-api-key]',
        '[data-search-key]',
        '[data-algolia-key]',
      ];
      for (const selector of dataAttrPatterns) {
        const el = $(selector).first();
        if (el.length) {
          const key = el.attr(selector.replace(/^\[|\]$/g, '').replace('data-', '')) ||
                      el.attr(selector.replace(/^\[|\]$/g, ''));
          if (key && key.length >= 16) {
            log.success(`Found Algolia API key in data attribute: ${key.substring(0, 8)}...`);
            return key;
          }
        }
      }

      // Pattern 3: Check linked JS files for the Algolia key
      const scriptSrcs = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src') || '';
        // Look for JS bundles that might contain Algolia config
        if (src.includes('algolia') || src.includes('search') || src.includes('chunk') ||
            src.includes('main') || src.includes('app') || src.includes('bundle') ||
            src.includes('vendor') || src.includes('config')) {
          if (src.startsWith('//')) {
            scriptSrcs.push(`https:${src}`);
          } else if (src.startsWith('/')) {
            scriptSrcs.push(`https://hsba.org${src}`);
          } else if (src.startsWith('http')) {
            scriptSrcs.push(src);
          }
        }
      });

      // Also add any inline scripts that reference algolia as potential source URLs
      // Sometimes the key is in a config endpoint
      const configPatterns = pageSource.match(/['"]([^'"]*(?:config|settings|env)[^'"]*\.js(?:\?[^'"]*)?)['"]/gi);
      if (configPatterns) {
        for (const match of configPatterns) {
          const url = match.replace(/['"]/g, '');
          if (url.startsWith('/')) {
            scriptSrcs.push(`https://hsba.org${url}`);
          } else if (url.startsWith('http')) {
            scriptSrcs.push(url);
          }
        }
      }

      // Check up to 5 JS files for the Algolia key
      for (const jsSrc of scriptSrcs.slice(0, 5)) {
        try {
          await rateLimiter.wait();
          const jsResp = await this.httpGet(jsSrc, rateLimiter);
          if (jsResp.statusCode === 200) {
            for (const pattern of algoliaInitPatterns) {
              pattern.lastIndex = 0;
              let match;
              while ((match = pattern.exec(jsResp.body)) !== null) {
                const key = match[1];
                if (key && key.length >= 16 && key !== this.algoliaAppId) {
                  log.success(`Found Algolia API key in JS bundle ${jsSrc}: ${key.substring(0, 8)}...`);
                  return key;
                }
              }
            }
          }
        } catch (err) {
          log.info(`Could not fetch JS bundle ${jsSrc}: ${err.message}`);
        }
      }

      // Pattern 4: Try the Algolia public search key format
      // Some sites use predictable patterns or the key is embedded in window.__ENV__
      const envPatterns = [
        /window\.__ENV__\s*=\s*({[^}]+})/,
        /window\.env\s*=\s*({[^}]+})/,
        /window\.config\s*=\s*({[^}]+})/,
        /window\.__CONFIG__\s*=\s*({[^}]+})/,
        /window\.ALGOLIA\s*=\s*({[^}]+})/,
        /__NEXT_DATA__.*?"algolia":\s*({[^}]+})/,
      ];

      for (const envPattern of envPatterns) {
        const envMatch = pageSource.match(envPattern);
        if (envMatch) {
          try {
            // Try to extract key from the JSON-like object
            const keyMatch = envMatch[1].match(/(?:apiKey|searchKey|api_key|search_key)['":\s]*['"]([a-zA-Z0-9]{16,})['"]/i);
            if (keyMatch && keyMatch[1] !== this.algoliaAppId) {
              log.success(`Found Algolia API key in env config: ${keyMatch[1].substring(0, 8)}...`);
              return keyMatch[1];
            }
          } catch {}
        }
      }

      log.warn('Could not find Algolia API key in page source or JS bundles');
      return null;
    } catch (err) {
      log.error(`Failed to discover Algolia key: ${err.message}`);
      return null;
    }
  }

  /**
   * Build the Algolia multi-query request body.
   * Algolia uses a "requests" array, each with an indexName and params string.
   */
  _buildAlgoliaQuery(searchText, filters, page = 0, hitsPerPage = 50, indexName = null) {
    // Algolia params are URL-encoded key=value pairs joined by &
    const paramParts = [];
    paramParts.push(`query=${encodeURIComponent(searchText || '')}`);
    paramParts.push(`hitsPerPage=${hitsPerPage}`);
    paramParts.push(`page=${page}`);

    if (filters) {
      paramParts.push(`filters=${encodeURIComponent(filters)}`);
    }

    // Common Algolia facet configuration
    paramParts.push('facets=[]');
    paramParts.push('tagFilters=');

    return {
      requests: [
        {
          indexName: indexName || 'lawyers',
          params: paramParts.join('&'),
        },
      ],
    };
  }

  /**
   * Normalize an Algolia hit into our standard attorney object format.
   */
  _normalizeHit(hit) {
    const get = (keys) => {
      for (const key of keys) {
        if (hit[key] !== undefined && hit[key] !== null) {
          const val = hit[key];
          return typeof val === 'string' ? val.trim() : String(val);
        }
        // Case-insensitive match
        const found = Object.keys(hit).find(k => k.toLowerCase() === key.toLowerCase());
        if (found && hit[found] !== undefined && hit[found] !== null) {
          const val = hit[found];
          return typeof val === 'string' ? val.trim() : String(val);
        }
      }
      return '';
    };

    const firstName = get(['firstName', 'FirstName', 'first_name', 'fname', 'first']);
    const lastName = get(['lastName', 'LastName', 'last_name', 'lname', 'last']);
    const fullName = get(['fullName', 'FullName', 'full_name', 'name', 'Name', 'displayName']) || `${firstName} ${lastName}`.trim();
    const firmName = get(['firmName', 'FirmName', 'firm_name', 'firm', 'Firm', 'company', 'Company', 'organization', 'Organization']);
    const city = get(['city', 'City', 'officeCity', 'OfficeCity']);
    const state = get(['state', 'State', 'officeState', 'OfficeState']) || 'HI';
    const phone = get(['phone', 'Phone', 'phoneNumber', 'PhoneNumber', 'telephone', 'Telephone', 'officePhone', 'OfficePhone']);
    const email = get(['email', 'Email', 'emailAddress', 'EmailAddress']);
    const website = get(['website', 'Website', 'webAddress', 'WebAddress', 'url', 'URL', 'firmUrl', 'FirmUrl']);
    const barNumber = get(['barNumber', 'BarNumber', 'bar_number', 'memberNumber', 'MemberNumber', 'licenseNumber', 'LicenseNumber', 'objectID', 'id']);
    const barStatus = get(['status', 'Status', 'memberStatus', 'MemberStatus', 'barStatus', 'BarStatus', 'memberType', 'MemberType']);
    const admissionDate = get(['admissionDate', 'AdmissionDate', 'admission_date', 'admitDate', 'AdmitDate', 'dateAdmitted', 'DateAdmitted']);
    const zip = get(['zip', 'Zip', 'zipCode', 'ZipCode', 'postalCode', 'PostalCode']);
    const practiceAreas = get(['practiceAreas', 'PracticeAreas', 'practice_areas', 'specialties', 'Specialties', 'areas', 'Areas']);

    // Handle address as object
    let resolvedCity = city;
    let resolvedState = state;
    let resolvedZip = zip;
    if (hit.address && typeof hit.address === 'object') {
      if (!resolvedCity) resolvedCity = (hit.address.city || '').trim();
      if (!resolvedState || resolvedState === 'HI') resolvedState = (hit.address.state || 'HI').trim();
      if (!resolvedZip) resolvedZip = (hit.address.zip || hit.address.zipCode || hit.address.postalCode || '').trim();
    }

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
      city: resolvedCity,
      state: resolvedState,
      zip: resolvedZip,
      phone: phone,
      email: email,
      website: website,
      bar_number: barNumber,
      bar_status: barStatus,
      admission_date: admissionDate,
      practice_areas_raw: practiceAreas,
      profile_url: barNumber ? `https://hsba.org/HSBA/Directory/MemberProfile.aspx?id=${barNumber}` : '',
      source: `${this.name}_bar`,
    };
  }

  /**
   * Try to discover the Algolia index name from the page source.
   * Returns the index name or falls back to common defaults.
   */
  _extractIndexName(pageSource) {
    // Common patterns for Algolia index name in page source
    const indexPatterns = [
      /indexName['":\s]*['"]([a-zA-Z0-9_-]+)['"]/gi,
      /index['":\s]*['"]([a-zA-Z0-9_-]+)['"]/gi,
      /algolia[._]?index['":\s]*['"]([a-zA-Z0-9_-]+)['"]/gi,
    ];

    const candidates = new Set();
    for (const pattern of indexPatterns) {
      let match;
      while ((match = pattern.exec(pageSource)) !== null) {
        const name = match[1];
        // Filter out common non-index-name matches
        if (name && name.length > 2 && name.length < 60 &&
            !['true', 'false', 'null', 'undefined', 'function', 'string', 'number', 'object'].includes(name.toLowerCase())) {
          candidates.add(name);
        }
      }
    }

    // Prefer candidates that look like attorney/lawyer indexes
    for (const candidate of candidates) {
      const lower = candidate.toLowerCase();
      if (lower.includes('lawyer') || lower.includes('attorney') || lower.includes('member') ||
          lower.includes('directory') || lower.includes('hsba')) {
        return candidate;
      }
    }

    // Return first candidate or fallback
    if (candidates.size > 0) {
      return candidates.values().next().value;
    }

    return null;
  }

  /**
   * Async generator that yields attorney records from the HSBA Algolia index.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    // Step 1: Discover the Algolia search API key from the page source
    this.algoliaApiKey = await this._discoverAlgoliaKey(rateLimiter);

    if (!this.algoliaApiKey) {
      log.error('Could not discover Algolia search API key — cannot query HSBA directory');
      return;
    }

    // Also try to get the index name from the page
    let indexName = null;
    try {
      await rateLimiter.wait();
      const pageResp = await this.httpGet(this.findLawyerUrl, rateLimiter);
      if (pageResp.statusCode === 200) {
        indexName = this._extractIndexName(pageResp.body);
        if (indexName) {
          log.info(`Discovered Algolia index name: ${indexName}`);
        }
      }
    } catch {
      // Index name extraction failed, will use fallbacks
    }

    // Common index name fallbacks for HSBA
    const indexNames = [
      indexName,
      'lawyers',
      'members',
      'hsba_members',
      'hsba_lawyers',
      'directory',
      'attorneys',
      'hsba_directory',
      'hsba-members',
      'hsba-lawyers',
    ].filter(Boolean);

    // Step 2: Test the Algolia API with each candidate index name
    let workingIndex = null;
    for (const testIndex of indexNames) {
      try {
        log.info(`Testing Algolia index: ${testIndex}`);
        await rateLimiter.wait();
        const testQuery = this._buildAlgoliaQuery('', '', 0, 1, testIndex);
        const testResp = await this._algoliaPost(testQuery, rateLimiter);

        if (testResp.statusCode === 200 && testResp.body && testResp.body.results) {
          const result = testResp.body.results[0];
          if (result && !result.message && (result.hits !== undefined)) {
            workingIndex = testIndex;
            log.success(`Algolia index "${testIndex}" is valid — ${result.nbHits || 0} total records`);
            break;
          }
        }
      } catch (err) {
        log.info(`Index "${testIndex}" test failed: ${err.message}`);
      }
    }

    if (!workingIndex) {
      log.error('Could not find a valid Algolia index name — cannot query HSBA directory');
      return;
    }

    // Step 3: Query for each city
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let algoliaPage = 0;
      let pagesFetched = 0;
      let totalHits = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build Algolia query with city filter
        // Algolia filters use the syntax: field:"value"
        // Common filter patterns for location
        const filterParts = [];
        filterParts.push(`city:"${city}"`);
        if (practiceCode) {
          // Practice area might be a facet filter
          filterParts.push(`practiceAreas:"${practiceCode}"`);
        }
        const filterStr = filterParts.join(' AND ');

        // Also search with city as the query text in case filters don't match field names
        const searchText = city;
        const queryBody = this._buildAlgoliaQuery(searchText, filterStr, algoliaPage, this.pageSize, workingIndex);

        log.info(`Page ${algoliaPage + 1} — Algolia query [city=${city}, page=${algoliaPage}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._algoliaPost(queryBody, rateLimiter);
        } catch (err) {
          log.error(`Algolia request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from Algolia`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          // If we get a filter error, retry without the filter and use query text only
          if (response.rawBody && response.rawBody.includes('filter')) {
            log.warn(`Algolia filter error — retrying with query text only`);
            const fallbackQuery = this._buildAlgoliaQuery(
              `${city}${practiceCode ? ' ' + practiceCode : ''}`,
              '',
              algoliaPage,
              this.pageSize,
              workingIndex
            );
            try {
              await rateLimiter.wait();
              response = await this._algoliaPost(fallbackQuery, rateLimiter);
            } catch (err) {
              log.error(`Algolia fallback request failed: ${err.message}`);
              break;
            }

            if (response.statusCode !== 200) {
              log.error(`Algolia returned status ${response.statusCode} for ${city} — skipping`);
              break;
            }
          } else {
            log.error(`Algolia returned status ${response.statusCode} for ${city} — skipping`);
            break;
          }
        }

        rateLimiter.resetBackoff();

        if (!response.body || !response.body.results || !response.body.results[0]) {
          log.error(`Unexpected Algolia response format for ${city} — skipping`);
          break;
        }

        const result = response.body.results[0];

        // Check for error in Algolia response (e.g., unknown filter attribute)
        if (result.message) {
          log.warn(`Algolia returned message: ${result.message}`);
          // Retry without filters if this is a filter error
          if (result.message.includes('filter') || result.message.includes('attribute')) {
            log.info('Retrying without facet filters...');
            const plainQuery = this._buildAlgoliaQuery(
              `${city}${practiceCode ? ' ' + practiceCode : ''}`,
              '',
              algoliaPage,
              this.pageSize,
              workingIndex
            );
            try {
              await rateLimiter.wait();
              response = await this._algoliaPost(plainQuery, rateLimiter);
              if (response.statusCode !== 200 || !response.body || !response.body.results || !response.body.results[0]) {
                log.error(`Algolia retry failed for ${city} — skipping`);
                break;
              }
              const retryResult = response.body.results[0];
              if (retryResult.message) {
                log.error(`Algolia still returning errors: ${retryResult.message} — skipping ${city}`);
                break;
              }
              // Use the retried result
              Object.assign(result, retryResult);
            } catch (err) {
              log.error(`Algolia retry request failed: ${err.message}`);
              break;
            }
          } else {
            break;
          }
        }

        const hits = result.hits || [];
        totalHits = result.nbHits || 0;
        const totalPages = result.nbPages || 0;

        if (pagesFetched === 0) {
          if (hits.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          log.success(`Found ${totalHits.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        if (hits.length === 0) {
          log.info(`No more results for ${city}`);
          break;
        }

        for (const hit of hits) {
          const attorney = this._normalizeHit(hit);

          // Verify the hit is actually for the target city (Algolia text search may match loosely)
          if (attorney.city && attorney.city.toLowerCase() !== city.toLowerCase()) {
            // Allow if the hit city contains the search city or vice versa
            const hitCity = attorney.city.toLowerCase();
            const searchCity = city.toLowerCase();
            if (!hitCity.includes(searchCity) && !searchCity.includes(hitCity)) {
              continue;
            }
          }

          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          attorney.practice_area = practiceArea || '';
          yield attorney;
        }

        algoliaPage++;
        pagesFetched++;

        // Check if we've reached the last page
        if (algoliaPage >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }
      }
    }
  }
}

module.exports = new HawaiiScraper();
