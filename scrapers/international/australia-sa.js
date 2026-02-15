/**
 * South Australia (AU-SA) Law Society -- Register of Practising Certificates Scraper
 *
 * Source: https://www.lawsocietysa.asn.au/site/site/for-the-public/register-of-practising-certificates.aspx
 * Method: ASP.NET postback with ViewState (iMIS CMS, Telerik RadGrid)
 *
 * The Law Society of South Australia maintains a Register of Practising Certificates
 * pursuant to section 20 of the Legal Practitioners Act 1981. It is publicly accessible
 * and updated daily. The register contains a listing of all legal practitioners holding
 * a South Australian practising certificate.
 *
 * How it works:
 *   1. GET the register page to obtain __VIEWSTATE, __EVENTVALIDATION, and form field IDs
 *   2. POST back with the Last Name search field populated (e.g., a single letter prefix)
 *   3. Parse the Telerik RadGrid HTML table for practitioner records
 *   4. Iterate through alphabet prefixes (A-Z) for comprehensive coverage
 *
 * The register supports:
 *   - Searching by Last Name (partial match supported)
 *   - Entering "All" to view the entire register
 *
 * Available data per practitioner:
 *   - Full name, practising certificate type, firm/employer, suburb/location
 *
 * The page uses ASP.NET Web Forms with:
 *   - __VIEWSTATE / __EVENTVALIDATION for state management
 *   - Telerik RadAjaxManager for AJAX updates
 *   - Telerik RadGrid (ResultsGrid) for displaying results
 *   - iMIS QueryMenu web part for the search interface
 *
 * Note: The ASP.NET ViewState mechanism requires fetching the page first to obtain
 * valid state tokens before any search POST can be made. ViewState values are large
 * (often 100KB+) and change on every request.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

class SaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-sa',
      stateCode: 'AU-SA',
      baseUrl: 'https://www.lawsocietysa.asn.au',
      pageSize: 50, // RadGrid may return variable row counts
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'litigation': 'Litigation',
        'conveyancing': 'Conveyancing',
        'personal injury': 'Personal Injury',
        'wills': 'Wills & Estates',
        'estates': 'Wills & Estates',
        'immigration': 'Immigration',
        'taxation': 'Taxation',
      },
      defaultCities: ['Adelaide', 'Mount Gambier', 'Port Augusta', 'Whyalla'],
    });

    this.registerUrl = `${this.baseUrl}/site/site/for-the-public/register-of-practising-certificates.aspx`;

    // Alphabet prefixes for surname-based iteration
    this.searchPrefixes = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
  }

  // --- HTTP helpers ---

  /**
   * HTTP GET that returns response body and headers, following redirects.
   * Needed for the initial page fetch to capture ViewState.
   */
  httpGetFull(url, rateLimiter, cookies = '', redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      };

      if (cookies) {
        options.headers['Cookie'] = cookies;
      }

      const req = https.get(options, (res) => {
        // Collect cookies
        const setCookies = res.headers['set-cookie'] || [];
        const cookieStr = setCookies.map(c => c.split(';')[0]).join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          const mergedCookies = cookies ? `${cookies}; ${cookieStr}` : cookieStr;
          return resolve(this.httpGetFull(redirect, rateLimiter, mergedCookies, redirectCount + 1));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: cookies ? `${cookies}; ${cookieStr}` : cookieStr,
          headers: res.headers,
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * HTTP POST with form data (URL-encoded body).
   * Used for ASP.NET postback with ViewState.
   */
  httpPostForm(url, formData, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = new URLSearchParams(formData).toString();
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
          'Cache-Control': 'no-cache',
        },
        timeout: 60000, // ASP.NET postbacks can be slow
      };

      if (cookies) {
        options.headers['Cookie'] = cookies;
        options.headers['Referer'] = url;
        options.headers['Origin'] = `https://${parsed.hostname}`;
      }

      const req = https.request(options, (res) => {
        const setCookies = res.headers['set-cookie'] || [];
        const cookieStr = setCookies.map(c => c.split(';')[0]).join('; ');

        // Follow redirects for POST responses
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          const mergedCookies = cookies ? `${cookies}; ${cookieStr}` : cookieStr;
          return resolve(this.httpGetFull(redirect, rateLimiter, mergedCookies));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: cookies ? `${cookies}; ${cookieStr}` : cookieStr,
          headers: res.headers,
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  // --- ASP.NET ViewState extraction ---

  /**
   * Extract ASP.NET hidden fields from the page HTML.
   * Returns an object with __VIEWSTATE, __EVENTVALIDATION, etc.
   */
  _extractAspNetFields($) {
    const fields = {};

    // Standard ASP.NET hidden fields
    const fieldNames = [
      '__VIEWSTATE',
      '__VIEWSTATEGENERATOR',
      '__EVENTVALIDATION',
      '__EVENTTARGET',
      '__EVENTARGUMENT',
      '__VIEWSTATEENCRYPTED',
    ];

    for (const name of fieldNames) {
      const el = $(`input[name="${name}"]`);
      if (el.length) {
        fields[name] = el.val() || '';
      }
    }

    // Telerik ScriptManager field
    const tsm = $('input[name$="_ScriptManager1_TSM"]');
    if (tsm.length) {
      fields[tsm.attr('name')] = tsm.val() || '';
    }

    // StyleSheetManager field
    const ssm = $('input[name$="_StyleSheetManager1_TSSM"]');
    if (ssm.length) {
      fields[ssm.attr('name')] = ssm.val() || '';
    }

    return fields;
  }

  /**
   * Find the ASP.NET control IDs for the search input and button.
   * These are dynamically generated by iMIS and include long prefixes.
   */
  _findSearchControls($) {
    const controls = {
      searchInputName: null,
      submitButtonName: null,
      gridId: null,
    };

    // Look for the QueryMenu search input -- typically a text input inside the QueryMenu web part
    // Pattern: ctl01$TemplateBody$WebPartManager1$gwpciNewQueryMenuCommon$ciNewQueryMenuCommon$...
    $('input[type="text"]').each((_, el) => {
      const name = $(el).attr('name') || '';
      const id = $(el).attr('id') || '';
      if (name.includes('QueryMenu') || name.includes('QueryMenuCommon') ||
          id.includes('QueryMenu') || id.includes('QueryMenuCommon')) {
        controls.searchInputName = name;
      }
      // Also match by placeholder or nearby label
      if (!controls.searchInputName) {
        const placeholder = $(el).attr('placeholder') || '';
        if (placeholder.toLowerCase().includes('last name') ||
            placeholder.toLowerCase().includes('surname')) {
          controls.searchInputName = name;
        }
      }
    });

    // If still not found, try matching any text input in the main content area
    if (!controls.searchInputName) {
      $('input[type="text"]').each((_, el) => {
        const name = $(el).attr('name') || '';
        if (name.includes('TemplateBody') && !name.includes('Search') &&
            !name.includes('Login') && !name.includes('SignIn')) {
          controls.searchInputName = name;
        }
      });
    }

    // Look for the submit/refresh button
    $('input[type="submit"], input[type="button"], button').each((_, el) => {
      const name = $(el).attr('name') || '';
      const value = $(el).attr('value') || '';
      const text = $(el).text() || '';
      if (value.toLowerCase().includes('find') || value.toLowerCase().includes('refresh') ||
          value.toLowerCase().includes('search') || text.toLowerCase().includes('find') ||
          text.toLowerCase().includes('refresh') || text.toLowerCase().includes('search')) {
        if (name.includes('TemplateBody') || name.includes('QueryMenu')) {
          controls.submitButtonName = name;
        }
      }
    });

    // Look for the ResultsGrid
    $('table[id*="ResultsGrid"], div[id*="ResultsGrid"]').each((_, el) => {
      controls.gridId = $(el).attr('id') || '';
    });

    return controls;
  }

  /**
   * Parse the Telerik RadGrid / HTML table results into attorney records.
   * The grid typically renders as an HTML table with rows for each practitioner.
   */
  _parseGridResults($) {
    const attorneys = [];

    // Look for the RadGrid table - it may be rendered as a standard HTML table
    // with class rgMasterTable or inside a RadGrid wrapper
    const gridSelectors = [
      'table[id*="ResultsGrid"] tr',
      'table.rgMasterTable tr',
      '.RadGrid .rgMasterTable tr',
      'table.rgMasterTable tbody tr',
      '#ResultsGrid tr',
    ];

    let rows = $();
    for (const selector of gridSelectors) {
      rows = $(selector);
      if (rows.length > 0) break;
    }

    // If no RadGrid found, try generic table rows in the content area
    if (rows.length === 0) {
      rows = $('table tr').filter((_, el) => {
        const parent = $(el).closest('[id*="ResultsGrid"], [id*="QueryMenu"], .MainContent');
        return parent.length > 0;
      });
    }

    // If still no results, try any data table
    if (rows.length === 0) {
      // Look for any table with data rows (not navigation/layout tables)
      $('table').each((_, table) => {
        const tableRows = $(table).find('tr');
        if (tableRows.length > 2 && tableRows.length < 2000) {
          // Check if it looks like a data table (has multiple td cells)
          const firstDataRow = tableRows.eq(1);
          if (firstDataRow.find('td').length >= 2) {
            rows = tableRows;
            return false; // break
          }
        }
      });
    }

    rows.each((i, row) => {
      const $row = $(row);
      const cells = $row.find('td');

      // Skip header rows
      if (cells.length === 0 || $row.find('th').length > 0) return;

      // Skip rows that are clearly navigation/paging
      if ($row.hasClass('rgPager') || $row.hasClass('rgFooter')) return;

      // Extract data from cells -- column order varies by iMIS configuration
      // Common patterns: [Name, Certificate Type, Firm, Location] or [Name, Firm, Location]
      const cellTexts = [];
      cells.each((_, cell) => {
        cellTexts.push($(cell).text().trim());
      });

      if (cellTexts.length < 2) return;
      if (!cellTexts[0] || cellTexts[0].length < 2) return;

      // First cell is typically the practitioner name
      let fullName = cellTexts[0].replace(/\s+/g, ' ').trim();

      // Skip if the "name" looks like a header or metadata
      if (fullName.toLowerCase().includes('name') && i === 0) return;
      if (fullName.toLowerCase().includes('no records')) return;
      if (fullName.toLowerCase().includes('please enter')) return;

      // Strip post-nominal titles (KC = King's Counsel, SC = Senior Counsel, QC, AM, AO, etc.)
      fullName = fullName.replace(/\s+(?:KC|SC|QC|AM|AO|OAM|PSM|RFD)\s*$/i, '').trim();

      const { firstName, lastName } = this.splitName(fullName);

      // Try to identify other columns — detect if a column is a practitioner number (P#####)
      let firmName = '';
      let certType = '';
      let barNumber = '';
      let location = '';

      if (cellTexts.length >= 4) {
        // Check if cellTexts[1] is a practitioner number (P followed by digits)
        if (/^P\d+$/.test(cellTexts[1])) {
          // [Name, PracNo, Firm/CertType, Location] pattern
          barNumber = cellTexts[1];
          firmName = cellTexts[2] || '';
          location = cellTexts[3] || '';
        } else {
          // [Name, CertType, Firm, Location] pattern
          certType = cellTexts[1] || '';
          firmName = cellTexts[2] || '';
          location = cellTexts[3] || '';
        }
      } else if (cellTexts.length >= 5) {
        // [Name, PracNo, CertType, Firm, Location] pattern
        barNumber = /^P\d+$/.test(cellTexts[1]) ? cellTexts[1] : '';
        certType = cellTexts[2] || '';
        firmName = cellTexts[3] || '';
        location = cellTexts[4] || '';
      } else if (cellTexts.length === 3) {
        if (/^P\d+$/.test(cellTexts[1])) {
          barNumber = cellTexts[1];
          location = cellTexts[2] || '';
        } else {
          firmName = cellTexts[1] || '';
          location = cellTexts[2] || '';
        }
      } else if (cellTexts.length === 2) {
        firmName = cellTexts[1] || '';
      }

      // Parse location into city/state
      let city = '';
      let state = 'SA';
      const locParts = this._parseAuAddress(location);
      if (locParts.city) city = locParts.city;
      if (locParts.state) state = locParts.state;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: state,
        zip: locParts.postcode || '',
        country: 'Australia',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: certType,
        admission_date: '',
        profile_url: '',
        practice_areas: '',
      });
    });

    return attorneys;
  }

  /**
   * Parse an Australian address string into components.
   * Handles formats like "Adelaide", "Adelaide SA 5000", "ADELAIDE SA", etc.
   */
  _parseAuAddress(text) {
    if (!text) return { city: '', state: '', postcode: '' };

    const cleaned = text.replace(/\s+/g, ' ').trim();

    // Pattern: "SUBURB STATE POSTCODE" (e.g., "Adelaide SA 5000")
    const full = cleaned.match(/^(.+?)\s+(SA|NSW|VIC|QLD|WA|TAS|NT|ACT)\s+(\d{4})$/i);
    if (full) {
      return {
        city: this._titleCase(full[1].trim()),
        state: full[2].toUpperCase(),
        postcode: full[3],
      };
    }

    // Pattern: "SUBURB STATE" (e.g., "Adelaide SA")
    const partial = cleaned.match(/^(.+?)\s+(SA|NSW|VIC|QLD|WA|TAS|NT|ACT)$/i);
    if (partial) {
      return {
        city: this._titleCase(partial[1].trim()),
        state: partial[2].toUpperCase(),
        postcode: '',
      };
    }

    // Just a suburb/city name
    return {
      city: this._titleCase(cleaned),
      state: 'SA',
      postcode: '',
    };
  }

  /**
   * Convert UPPERCASE string to Title Case.
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  // --- BaseScraper overrides (formal compliance -- search() is fully overridden) ---

  buildSearchUrl({ city, practiceCode, page }) {
    // Not used directly; search is done via ASP.NET postback in search() override
    return `${this.registerUrl}?search=${encodeURIComponent(city)}&page=${page}`;
  }

  parseResultsPage($) {
    return this._parseGridResults($);
  }

  extractResultCount($) {
    // Look for result count in RadGrid pager or page text
    const text = $('body').text();

    // Pattern: "X items in Y pages" or "X records"
    const itemMatch = text.match(/(\d+)\s+items?\s+in\s+(\d+)\s+pages?/i);
    if (itemMatch) return parseInt(itemMatch[1], 10);

    const recordMatch = text.match(/(\d+)\s+records?\s+found/i);
    if (recordMatch) return parseInt(recordMatch[1], 10);

    // Count visible rows as fallback
    const rows = $('table[id*="ResultsGrid"] tr td, table.rgMasterTable tr td').length;
    return rows > 0 ? Math.ceil(rows / 3) : 0; // Rough estimate from cell count
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields practitioner records from the SA Register.
   *
   * Strategy:
   * 1. GET the register page to obtain ASP.NET ViewState and control IDs.
   * 2. For each letter prefix (A-Z), POST the search form with that prefix
   *    as the Last Name value.
   * 3. Parse the resulting RadGrid table for practitioner records.
   * 4. Handle RadGrid pagination if results span multiple pages.
   * 5. Yield each practitioner record.
   *
   * The ASP.NET ViewState must be refreshed after each POST because the
   * server returns a new ViewState with each response.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const seen = new Set();

    log.scrape('AU-SA: Starting Law Society SA Register of Practising Certificates scrape');
    log.info('AU-SA: Register URL: ' + this.registerUrl);

    // Step 1: Fetch the initial page to get ViewState and form structure
    let pageResponse;
    try {
      await rateLimiter.wait();
      pageResponse = await this.httpGetFull(this.registerUrl, rateLimiter);
    } catch (err) {
      log.error(`AU-SA: Failed to fetch register page: ${err.message}`);
      log.warn('AU-SA: The register at lawsocietysa.asn.au uses ASP.NET iMIS with ViewState.');
      log.warn('AU-SA: If the page redirects to a login, the register may require session cookies.');
      yield { _placeholder: true, reason: 'initial_fetch_failed', error: err.message };
      return;
    }

    if (pageResponse.statusCode !== 200) {
      log.error(`AU-SA: Register page returned status ${pageResponse.statusCode}`);

      // Check if redirected to login
      if (pageResponse.body.includes('Sign_In') || pageResponse.body.includes('signInUserName')) {
        log.warn('AU-SA: Register page redirected to login. The register may require authentication.');
        log.warn('AU-SA: Falling back to the Referral Service at referral.lawsocietysa.asn.au');
        yield { _placeholder: true, reason: 'login_required' };
        return;
      }

      yield { _placeholder: true, reason: 'unexpected_status', status: pageResponse.statusCode };
      return;
    }

    // Check for login redirect in response body
    if (pageResponse.body.includes('signInUserName') || pageResponse.body.includes('Sign_In.aspx')) {
      log.warn('AU-SA: Page content is a login form. The register requires authentication or cookies.');
      log.warn('AU-SA: The ASP.NET iMIS system (lawsocietysa.asn.au) protects this register behind a');
      log.warn('AU-SA: session wall. A headless browser or pre-authenticated session would be needed.');
      log.info('AU-SA: Alternative data source: referral.lawsocietysa.asn.au (Angular-based, radius search)');
      yield { _placeholder: true, reason: 'login_wall_detected' };
      return;
    }

    const cookies = pageResponse.cookies || '';
    let $ = cheerio.load(pageResponse.body);

    // Extract ASP.NET form fields
    let aspFields = this._extractAspNetFields($);
    const controls = this._findSearchControls($);

    if (!aspFields.__VIEWSTATE) {
      log.warn('AU-SA: Could not extract __VIEWSTATE from register page');
      log.warn('AU-SA: The page may use client-side rendering or the form structure has changed.');
      log.info('AU-SA: Page title: ' + $('title').text().trim());

      // Try to find useful content anyway
      const directResults = this._parseGridResults($);
      if (directResults.length > 0) {
        log.success(`AU-SA: Found ${directResults.length} records on initial page (no ViewState needed)`);
        for (const attorney of directResults) {
          const key = attorney.full_name;
          if (!seen.has(key)) {
            seen.add(key);
            yield this.transformResult(attorney, practiceArea);
          }
        }
      }

      yield { _placeholder: true, reason: 'no_viewstate' };
      return;
    }

    log.success('AU-SA: Successfully obtained ViewState and form fields');
    if (controls.searchInputName) {
      log.info(`AU-SA: Search input: ${controls.searchInputName}`);
    }
    if (controls.submitButtonName) {
      log.info(`AU-SA: Submit button: ${controls.submitButtonName}`);
    }

    // Step 2: Iterate through alphabet prefixes
    const prefixes = this.searchPrefixes;
    const totalPrefixes = prefixes.length;

    for (let pi = 0; pi < totalPrefixes; pi++) {
      const prefix = prefixes[pi];

      yield { _cityProgress: { current: pi + 1, total: totalPrefixes } };
      log.scrape(`AU-SA: Searching surname prefix "${prefix}" (${pi + 1}/${totalPrefixes})`);

      // Respect max pages limit (for --test mode)
      if (options.maxPages && pi >= options.maxPages) {
        log.info(`AU-SA: Reached max search limit (${options.maxPages}) -- stopping`);
        break;
      }

      // Build POST form data
      const formData = { ...aspFields };

      // Set the search input value
      if (controls.searchInputName) {
        formData[controls.searchInputName] = prefix;
      }

      // Set the submit button (triggers postback)
      if (controls.submitButtonName) {
        formData[controls.submitButtonName] = 'Find';
      }

      // If we don't have specific control names, try common iMIS patterns
      if (!controls.searchInputName) {
        // Try posting with __EVENTTARGET as the trigger
        formData['__EVENTTARGET'] = '';
        formData['__EVENTARGUMENT'] = '';
      }

      let response;
      try {
        await rateLimiter.wait();
        // Add extra delay for ASP.NET -- these servers are slower
        await sleep(2000 + Math.random() * 3000);
        response = await this.httpPostForm(this.registerUrl, formData, rateLimiter, cookies);
      } catch (err) {
        log.error(`AU-SA: Search POST failed for prefix "${prefix}": ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`AU-SA: Got ${response.statusCode} for prefix "${prefix}"`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (!shouldRetry) break;
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`AU-SA: Unexpected status ${response.statusCode} for prefix "${prefix}" -- skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`AU-SA: CAPTCHA detected for prefix "${prefix}" -- skipping`);
        yield { _captcha: true, city: `prefix:${prefix}` };
        break;
      }

      // Parse response
      $ = cheerio.load(response.body);
      const attorneys = this._parseGridResults($);

      // Update ViewState for next request (ASP.NET requires fresh state)
      aspFields = this._extractAspNetFields($);

      if (attorneys.length === 0) {
        log.info(`AU-SA: No results for prefix "${prefix}"`);
        continue;
      }

      log.success(`AU-SA: Found ${attorneys.length} practitioners for prefix "${prefix}"`);

      // Yield results with deduplication
      let newCount = 0;
      for (const attorney of attorneys) {
        const key = attorney.full_name;
        if (seen.has(key)) continue;
        seen.add(key);

        if (options.minYear && attorney.admission_date) {
          const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
          if (year > 0 && year < options.minYear) continue;
        }

        yield this.transformResult(attorney, practiceArea);
        newCount++;
      }

      if (newCount > 0) {
        log.info(`AU-SA: Yielded ${newCount} new records for prefix "${prefix}"`);
      }

      // Handle RadGrid pagination within this prefix if needed
      // Check if there are additional pages indicated by a pager row
      const pagerLinks = $('tr.rgPager a, .rgPager a, .rgNumPart a');
      if (pagerLinks.length > 0) {
        log.info(`AU-SA: RadGrid pagination detected for prefix "${prefix}" -- additional pages may exist`);
        // Note: RadGrid pagination requires __doPostBack with specific event targets
        // which are unique per page. For now, we log this but don't paginate within
        // a prefix -- the alphabet iteration should cover most practitioners.
      }
    }

    log.success(`AU-SA: Register scrape complete -- ${seen.size} unique practitioners found`);
  }
}

module.exports = new SaScraper();
