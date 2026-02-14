/**
 * Law Society of Northern Ireland Scraper
 *
 * Source: https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor
 * Method: HTML form with client-side rendering, paginated results
 *
 * The Law Society of Northern Ireland maintains a voluntary Find a Solicitor
 * directory. This is NOT the complete official roll -- it is a voluntary listing
 * with approximately 563 entries total, displayed 8 results per page.
 *
 * Search params: town/city/postcode, practice area (40+ categories),
 * solicitor/firm name, legal aid filter.
 *
 * Results include firm/solicitor name, full address, phone number, map location.
 * Professional designations: PPC (Consultant), PPP (Principal), PPA (Assistant).
 *
 * Uses Leaflet/Mapbox for map display on the frontend. The underlying data
 * may be available via an AJAX/JSON endpoint that powers the map.
 *
 * Overrides search() to query the directory via both the AJAX endpoint
 * (if discoverable) and the HTML form as fallback.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NorthernIrelandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'northern-ireland',
      stateCode: 'UK-NI',
      baseUrl: 'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor',
      pageSize: 8,
      practiceAreaCodes: {
        'criminal law':             'criminal law',
        'criminal':                 'criminal law',
        'family law':               'family law',
        'family':                   'family law',
        'personal injury':          'personal injury',
        'employment':               'employment',
        'employment law':           'employment',
        'immigration':              'immigration',
        'property/conveyancing':    'property/conveyancing',
        'property':                 'property/conveyancing',
        'conveyancing':             'property/conveyancing',
        'wills/probate':            'wills/probate',
        'wills':                    'wills/probate',
        'probate':                  'wills/probate',
        'commercial':               'commercial',
        'medical negligence':       'medical negligence',
        'housing':                  'housing',
        'judicial review':          'judicial review',
        'welfare benefits':         'welfare benefits',
      },
      defaultCities: [
        'Belfast', 'Derry', 'Londonderry', 'Lisburn', 'Newry',
        'Bangor', 'Craigavon', 'Ballymena', 'Newtownabbey',
        'Omagh', 'Enniskillen',
      ],
    });

    this.directoryUrl = 'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor';

    // Possible AJAX endpoints that power the client-side rendering / Leaflet map
    this.ajaxEndpoints = [
      'https://lawsoc-ni.org/api/solicitors',
      'https://lawsoc-ni.org/api/find-a-solicitor',
      'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor/search',
      'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor?ajax=1',
      'https://lawsoc-ni.org/wp-json/wp/v2/solicitor',
      'https://lawsoc-ni.org/wp-json/fas/v1/search',
    ];
  }

  /**
   * Not used -- search() is fully overridden.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Not used -- search() is fully overridden.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  /**
   * Not used -- search() is fully overridden.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * HTTP POST for the Law Society of NI search/AJAX endpoints.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/json') {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof data === 'string' ? data : JSON.stringify(data);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json,text/html,*/*',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://lawsoc-ni.org',
          'Referer': 'https://lawsoc-ni.org/using-a-solicitor/find-a-solicitor',
          'X-Requested-With': 'XMLHttpRequest',
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(postData);
      req.end();
    });
  }

  /**
   * Try to discover and fetch all solicitor data from an AJAX endpoint.
   * Returns array of attorney objects or null if no endpoint works.
   */
  async _tryAjaxEndpoints(rateLimiter) {
    // First, check the main page for embedded JSON data or AJAX endpoint references
    log.info('Checking NI Law Society page for AJAX endpoints...');

    try {
      await rateLimiter.wait();
      const pageResp = await this.httpGet(this.directoryUrl, rateLimiter);

      if (pageResp.statusCode === 200) {
        // Look for embedded JSON data in script tags
        const $ = cheerio.load(pageResp.body);
        let embeddedData = null;

        $('script').each((_, el) => {
          const scriptContent = $(el).html() || '';

          // Look for JSON arrays of solicitor data embedded in the page
          const jsonPatterns = [
            /var\s+solicitors?\s*=\s*(\[[\s\S]*?\]);/,
            /var\s+markers?\s*=\s*(\[[\s\S]*?\]);/,
            /var\s+data\s*=\s*(\[[\s\S]*?\]);/,
            /JSON\.parse\('(\[.*?\])'\)/,
            /"solicitors?":\s*(\[[\s\S]*?\])/,
            /mapData\s*=\s*(\[[\s\S]*?\]);/,
          ];

          for (const pattern of jsonPatterns) {
            const match = scriptContent.match(pattern);
            if (match) {
              try {
                embeddedData = JSON.parse(match[1]);
                if (Array.isArray(embeddedData) && embeddedData.length > 10) {
                  log.success(`Found embedded solicitor data — ${embeddedData.length} records`);
                  return false; // break .each()
                }
                embeddedData = null;
              } catch {
                // Not valid JSON
              }
            }
          }

          // Also look for AJAX endpoint URLs
          const ajaxMatch = scriptContent.match(/(?:ajax_url|apiUrl|endpoint|dataUrl)\s*[:=]\s*['"]([^'"]+)['"]/);
          if (ajaxMatch) {
            let ajaxUrl = ajaxMatch[1];
            if (ajaxUrl.startsWith('/')) {
              ajaxUrl = `https://lawsoc-ni.org${ajaxUrl}`;
            }
            if (!this.ajaxEndpoints.includes(ajaxUrl)) {
              this.ajaxEndpoints.unshift(ajaxUrl);
              log.info(`Discovered AJAX endpoint: ${ajaxUrl}`);
            }
          }
        });

        if (embeddedData && Array.isArray(embeddedData) && embeddedData.length > 0) {
          return embeddedData;
        }
      }
    } catch (err) {
      log.info(`Could not check main page: ${err.message}`);
    }

    // Try known AJAX endpoints
    for (const endpoint of this.ajaxEndpoints) {
      log.info(`Trying AJAX endpoint: ${endpoint}`);

      // Try GET first
      try {
        await rateLimiter.wait();
        const resp = await this.httpGet(endpoint, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            const records = Array.isArray(data) ? data : (data.results || data.data || data.solicitors || data.items || []);
            if (Array.isArray(records) && records.length > 10) {
              log.success(`AJAX endpoint returned ${records.length} records`);
              return records;
            }
          } catch {
            // Not JSON
          }
        }
      } catch (err) {
        log.info(`GET ${endpoint} failed: ${err.message}`);
      }

      // Try POST
      try {
        await rateLimiter.wait();
        const resp = await this.httpPost(endpoint, { page: 1, per_page: 100 }, rateLimiter);
        if (resp.statusCode === 200) {
          try {
            const data = JSON.parse(resp.body);
            const records = Array.isArray(data) ? data : (data.results || data.data || data.solicitors || data.items || []);
            if (Array.isArray(records) && records.length > 0) {
              log.success(`AJAX POST endpoint returned ${records.length} records`);
              return records;
            }
          } catch {
            // Not JSON
          }
        }
      } catch (err) {
        log.info(`POST ${endpoint} failed: ${err.message}`);
      }
    }

    return null;
  }

  /**
   * Parse a JSON record (from AJAX or embedded data) into a standard attorney object.
   */
  _normalizeJsonRecord(rec) {
    const firmName = (rec.name || rec.firm_name || rec.firmName || rec.title || rec.company || '').trim();
    const solicitorName = (rec.solicitor_name || rec.solicitorName || rec.contact || '').trim();
    const fullName = solicitorName || firmName;
    const { firstName, lastName } = this.splitName(fullName);

    // Address handling
    let address = '';
    let recCity = '';
    let postcode = '';

    if (typeof rec.address === 'string') {
      address = rec.address.trim();
    } else if (typeof rec.address === 'object' && rec.address) {
      address = [
        rec.address.line1 || rec.address.address1 || '',
        rec.address.line2 || rec.address.address2 || '',
        rec.address.city || rec.address.town || '',
        rec.address.postcode || rec.address.postal_code || '',
      ].filter(Boolean).join(', ');
      recCity = (rec.address.city || rec.address.town || '').trim();
      postcode = (rec.address.postcode || rec.address.postal_code || '').trim();
    }

    if (!recCity) {
      recCity = (rec.city || rec.town || rec.location || '').trim();
    }
    if (!postcode) {
      postcode = (rec.postcode || rec.postal_code || rec.zip || '').trim();
      if (!postcode) {
        const pcMatch = address.match(/\b(BT\d{1,2}\s?\d[A-Z]{2})\b/i);
        if (pcMatch) postcode = pcMatch[1];
      }
    }

    const phone = (rec.phone || rec.telephone || rec.tel || rec.phone_number || '').trim();
    const email = (rec.email || rec.emailAddress || '').trim();
    const website = (rec.website || rec.url || rec.web || '').trim();

    // Professional designation
    const designation = (rec.designation || rec.type || rec.role || '').trim();

    // Practice areas
    const areas = rec.practice_areas || rec.practiceAreas || rec.areas || rec.specialisms || [];
    const areasStr = Array.isArray(areas) ? areas.join(', ') : areas.toString();

    // Geo coords (from Leaflet/Mapbox data)
    const lat = rec.lat || rec.latitude || (rec.location && rec.location.lat) || '';
    const lng = rec.lng || rec.longitude || (rec.location && rec.location.lng) || '';

    return {
      first_name: solicitorName ? firstName : '',
      last_name: solicitorName ? lastName : '',
      full_name: fullName,
      firm_name: firmName,
      city: recCity,
      state: 'UK-NI',
      phone,
      email,
      website,
      bar_number: (rec.id || rec.member_id || rec.solicitor_id || '').toString().trim(),
      bar_status: designation || 'Listed',
      profile_url: '',
      address,
      postcode,
      practice_areas: areasStr,
      latitude: lat.toString(),
      longitude: lng.toString(),
    };
  }

  /**
   * Parse HTML search results from the NI Law Society page.
   */
  _parseHtmlResults($, city) {
    const attorneys = [];

    // Try various selectors for result items
    const selectors = [
      '.search-result', '.result-item', '.solicitor-result',
      '.find-solicitor-result', '.card', '.listing',
      'article', '.member-result', 'li.result',
      'table tbody tr', '.panel',
    ];

    let $items = $([]);
    for (const sel of selectors) {
      $items = $(sel);
      if ($items.length > 0) break;
    }

    // Fallback: look for repeated structural elements
    if ($items.length === 0) {
      $items = $('div[class*="result"], div[class*="solicitor"], div[class*="listing"]');
    }

    $items.each((_, el) => {
      const $el = $(el);

      // Extract firm/solicitor name
      const nameEl = $el.find('h2 a, h3 a, h4 a, .name a, .firm-name a, a.solicitor-link').first();
      let fullName = nameEl.text().trim();
      if (!fullName) {
        fullName = $el.find('h2, h3, h4, .name, .firm-name, strong').first().text().trim();
      }
      if (!fullName || fullName.length < 2) return;

      // Profile URL
      let profileUrl = nameEl.attr('href') || '';
      if (profileUrl && profileUrl.startsWith('/')) {
        profileUrl = `https://lawsoc-ni.org${profileUrl}`;
      }

      // Extract address
      const addressText = ($el.find('.address, address, .location').text() || '').trim();

      // Extract postcode (NI postcodes start with BT)
      const postcodeMatch = ($el.text() || '').match(/\b(BT\d{1,2}\s?\d[A-Z]{2})\b/i);
      const postcode = postcodeMatch ? postcodeMatch[1] : '';

      // Extract phone
      let phone = ($el.find('a[href^="tel:"]').attr('href') || '').replace('tel:', '').trim();
      if (!phone) {
        const phoneMatch = $el.text().match(/(?:Tel|Phone|Telephone):\s*([\d\s+()-]+)/i) ||
                           $el.text().match(/\b(028\s?\d{4}\s?\d{4})\b/) ||
                           $el.text().match(/\b(\+44\s?\d{2,4}\s?\d{3,4}\s?\d{3,4})\b/);
        if (phoneMatch) phone = phoneMatch[1].trim();
      }

      // Extract email
      const email = ($el.find('a[href^="mailto:"]').attr('href') || '').replace('mailto:', '').trim();

      // Extract website
      const website = ($el.find('a[href^="http"]').not('a[href*="lawsoc-ni"]').attr('href') || '').trim();

      // Professional designation (PPC, PPP, PPA)
      const designationMatch = $el.text().match(/\b(PPC|PPP|PPA)\b/);
      const designation = designationMatch ? designationMatch[1] : '';
      let designationLabel = '';
      if (designation === 'PPC') designationLabel = 'Consultant';
      else if (designation === 'PPP') designationLabel = 'Principal';
      else if (designation === 'PPA') designationLabel = 'Assistant';

      // Firm name (may be separate from solicitor name)
      let firmName = ($el.find('.firm, .firm-name, .organisation').text() || '').trim();
      if (!firmName) firmName = fullName;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'UK-NI',
        phone,
        email,
        website,
        bar_number: '',
        bar_status: designationLabel || designation || 'Listed',
        profile_url: profileUrl,
        address: addressText,
        postcode,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the HTML page.
   */
  _extractHtmlResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:results?|records?|solicitors?|firms?|entries)\s+found/i) ||
                  text.match(/(?:Showing|Found|Displaying)\s+(?:\d+\s*[-–]\s*\d+\s+of\s+)?([\d,]+)/i) ||
                  text.match(/(\d+)\s+(?:results?|solicitors?|firms?)/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }

  /**
   * Async generator that yields solicitor records from NI Law Society directory.
   *
   * Strategy:
   *  1. Try to discover AJAX/JSON endpoints or embedded data (client-side rendered)
   *  2. If found, parse all records and filter by city/practice area
   *  3. If not found, fall back to paginated HTML scraping per city
   *
   * Note: this is a small dataset (~563 entries), so full enumeration is feasible.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    const citySet = new Set(cities.map(c => c.toLowerCase()));
    // Also match "Derry" for "Londonderry" and vice versa
    if (citySet.has('derry') || citySet.has('londonderry')) {
      citySet.add('derry');
      citySet.add('londonderry');
      citySet.add('derry/londonderry');
    }

    // --- Attempt AJAX/embedded data discovery ---
    log.info('Attempting to discover AJAX endpoint or embedded data for NI solicitors...');
    const ajaxData = await this._tryAjaxEndpoints(rateLimiter);

    if (ajaxData && Array.isArray(ajaxData) && ajaxData.length > 0) {
      log.success(`Found ${ajaxData.length} solicitor records via AJAX/embedded data`);

      yield { _cityProgress: { current: 1, total: cities.length } };

      let yieldCount = 0;
      const seen = new Set();

      for (const rec of ajaxData) {
        const attorney = this._normalizeJsonRecord(rec);

        // Filter by city
        const recCityLower = attorney.city.toLowerCase();
        const addressLower = (attorney.address || '').toLowerCase();
        let matchesCity = false;

        for (const c of citySet) {
          if (recCityLower.includes(c) || addressLower.includes(c)) {
            matchesCity = true;
            // Set city to the canonical name from our list
            attorney.city = cities.find(ct => ct.toLowerCase() === c) || attorney.city;
            break;
          }
        }

        if (!matchesCity) continue;

        // Filter by practice area
        if (practiceCode) {
          const allText = `${attorney.practice_areas || ''} ${attorney.firm_name}`.toLowerCase();
          if (!allText.includes(practiceCode.toLowerCase())) continue;
        }

        // Deduplicate
        const key = `${attorney.full_name}|${attorney.firm_name}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);

        yield this.transformResult(attorney, practiceArea);
        yieldCount++;

        if (options.maxPages && yieldCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max records limit`);
          return;
        }
      }

      log.success(`Yielded ${yieldCount} solicitors from AJAX data`);
      return;
    }

    // --- Fallback: HTML paginated search ---
    log.warn('AJAX discovery unsuccessful — falling back to HTML paginated search');

    const seen = new Set();

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} solicitors in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build search URL with query parameters
        const params = new URLSearchParams();
        params.set('search', city);
        if (practiceCode) {
          params.set('practice_area', practiceCode);
        }
        if (page > 1) {
          params.set('page', String(page));
        }

        const searchUrl = `${this.directoryUrl}?${params.toString()}`;
        log.info(`Page ${page} — GET ${searchUrl}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(searchUrl, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} — skipping city ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this._extractHtmlResultCount($);
          if (totalResults === 0) {
            const testResults = this._parseHtmlResults($, city);
            if (testResults.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testResults.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this._parseHtmlResults($, city);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const attorney of attorneys) {
          // Deduplicate
          const key = `${attorney.full_name}|${attorney.firm_name}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);

          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page (8 results per page)
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          const href = $(el).attr('href') || '';
          return text === 'next' || text === 'next >' || text === '>>' ||
                 text.includes('next page') || href.includes('page=' + (page + 1));
        }).length > 0;

        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages && !hasNext) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new NorthernIrelandScraper();
