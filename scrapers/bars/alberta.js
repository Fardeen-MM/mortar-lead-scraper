/**
 * Alberta Law Society Scraper
 *
 * Source: https://lsa.memberpro.net/main/body.cfm
 * Method: ColdFusion (MemberPro platform) — session-based POST + Cheerio HTML parsing
 *
 * MemberPro uses a ColdFusion session with a 3-step flow:
 *   1. GET search page → session cookies (LANGUAGE, XN_CHECKLIST)
 *   2. POST search form → results table with pickStep(record_id, table_id) links
 *   3. POST pickStep form → individual lawyer profile page
 *
 * Profile pages contain: firm name, office phone, fax, address, practice areas,
 * enrolment date, and sometimes email (mailto links).
 *
 * The search iterates last name prefixes per city since the directory requires
 * at least a partial last name for search.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class AlbertaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alberta',
      stateCode: 'CA-AB',
      baseUrl: 'https://lsa.memberpro.net/main/body.cfm',
      pageSize: 25,
      practiceAreaCodes: {
        'family':                'Matrimonial/Family',
        'family law':            'Matrimonial/Family',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'real estate':           'Real Estate Conveyancing',
        'corporate/commercial':  'Corporate',
        'corporate':             'Corporate',
        'commercial':            'Commercial',
        'personal injury':       'Civil Litigation',
        'employment':            'Employment/Labour',
        'labour':                'Employment/Labour',
        'immigration':           'Immigration',
        'estate planning/wills': 'Estate Planning and Administration',
        'estate planning':       'Estate Planning and Administration',
        'wills':                 'Estate Planning and Administration',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative/Boards/Tribunals',
        'environmental':         'Environmental',
        'aboriginal':            'Aboriginal',
        'bankruptcy':            'Bankruptcy/Insolvency/Receivership',
        'mediation':             'Mediation',
        'arbitration':           'Arbitration',
        'entertainment':         'Entertainment',
        'international':         'International Business',
      },
      defaultCities: [
        'Calgary', 'Edmonton', 'Red Deer', 'Lethbridge',
        'Medicine Hat', 'St. Albert', 'Grande Prairie',
      ],
    });

    // Search URL for the practising member directory
    this.searchUrl = 'https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop';
    this.searchPageUrl = 'https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&page_id=366';

    // ColdFusion session cookies — set during search(), reused by fetchProfilePage()
    this._sessionCookies = '';

    // Last name prefixes for iteration (MemberPro requires partial name)
    this.lastNamePrefixes = [
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
      'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
      'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
  }

  /**
   * HTTP request with cookie support for ColdFusion session management.
   * Handles both GET and POST requests, follows redirects, and accumulates cookies.
   *
   * @param {string} method - HTTP method (GET or POST)
   * @param {string} url - Full URL
   * @param {string|null} postData - URL-encoded form data for POST requests
   * @param {string} cookies - Cookie header string
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @param {number} redirectCount - Redirect depth counter
   * @returns {Promise<{statusCode: number, body: string, cookies: string}>}
   */
  _httpReqWithCookies(method, url, postData, cookies, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: method,
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,*/*',
          'Cookie': cookies || '',
        },
        timeout: 15000,
      };

      if (method === 'POST' && postData) {
        options.headers['Content-Type'] = 'application/x-www-form-urlencoded';
        options.headers['Content-Length'] = Buffer.byteLength(postData);
      }

      const req = https.request(options, (res) => {
        // Accumulate cookies
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]).join('; ');
        const allCookies = cookies
          ? (newCookies ? cookies + '; ' + newCookies : cookies)
          : newCookies;

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          } else if (!redirect.startsWith('http')) {
            const base = url.substring(0, url.lastIndexOf('/') + 1);
            redirect = base + redirect;
          }
          return resolve(this._httpReqWithCookies('GET', redirect, null, allCookies, rateLimiter, redirectCount + 1));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body, cookies: allCookies }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      if (method === 'POST' && postData) {
        req.write(postData);
      }
      req.end();
    });
  }

  /**
   * Not used — search() is fully overridden for POST-based workflow.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST workflow`);
  }

  /**
   * Parse the MemberPro #member-directory results table.
   *
   * Table structure (6 columns):
   *   Name (with pickStep link) | City | Gender | Practising Status | Enrolment Date | Firm
   *
   * The name cell contains a link like: javascript:pickStep(record_id, table_id)
   * which is used to navigate to the individual profile via POST.
   *
   * Name cell also contains embedded firm and city info in whitespace,
   * so we extract the clean name from just the link text.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('#member-directory tbody tr').each((_, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 4) return;

      // Column 0: Name (with pickStep link)
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a[href*="pickStep"]');
      const rawName = nameLink.text().trim();
      if (!rawName || rawName.length < 2) return;

      // Extract record_id and table_id from pickStep(record_id, table_id)
      const href = nameLink.attr('href') || '';
      const stepMatch = href.match(/pickStep\((\d+),(\d+)\)/);
      const recordId = stepMatch ? stepMatch[1] : '';
      const tableId = stepMatch ? stepMatch[2] : '';

      // Column 1: City
      const city = $(cells[1]).text().trim();

      // Column 2: Gender (skip it, but note the index)

      // Column 3: Practising Status
      const status = $(cells[3]).text().trim();

      // Column 4: Enrolment Date
      const enrollDate = cells.length > 4 ? $(cells[4]).text().trim() : '';

      // Column 5: Firm
      const firm = cells.length > 5 ? $(cells[5]).text().trim() : '';

      // Strip honorifics (KC, QC)
      const cleanName = rawName.replace(/,?\s*(KC|QC|K\.C\.|Q\.C\.)$/i, '').trim();

      // Parse name: "First Last" or "First Middle Last"
      const nameParts = this.splitName(cleanName);

      // Build a pseudo profile_url using table_id for later POST-based fetching
      // Format: memberpro://record_id/table_id (not a real URL — handled by fetchProfilePage)
      const profileUrl = (recordId && tableId)
        ? `memberpro://${recordId}/${tableId}`
        : '';

      attorneys.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: cleanName,
        firm_name: firm,
        city: city,
        state: 'CA-AB',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        admission_date: enrollDate,
        bar_status: status || 'Active',
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from MemberPro page.
   */
  extractResultCount($) {
    const text = $('body').text();

    const matchOf = text.match(/(?:Displaying|Showing|Results?)\s*:?\s*\d+\s*[-\u2013to]+\s*\d+\s+of\s+([\d,]+)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total\s*:?\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override fetchProfilePage to handle MemberPro POST-based profile navigation.
   *
   * Profile pages are accessed via a POST form (the "Pick" form) with:
   *   menu=directory, submenu=directoryPractisingMember, mode=search,
   *   record_id={n}, table_id={id}
   *
   * The profile_url is stored as "memberpro://record_id/table_id" during search.
   *
   * @param {string} url - Profile URL (memberpro://record_id/table_id format)
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {CheerioStatic|null} Cheerio instance or null on failure
   */
  async fetchProfilePage(url, rateLimiter) {
    if (!url) return null;

    // Parse the memberpro:// pseudo URL
    const match = url.match(/^memberpro:\/\/(\d+)\/(\d+)$/);
    if (!match) {
      log.warn(`${this.name}: Invalid profile URL format: ${url}`);
      return null;
    }

    const recordId = match[1];
    const tableId = match[2];

    try {
      // Ensure we have session cookies
      if (!this._sessionCookies) {
        await rateLimiter.wait();
        const initResp = await this._httpReqWithCookies(
          'GET', this.searchPageUrl, null, '', rateLimiter
        );
        this._sessionCookies = initResp.cookies || '';
        log.info(`${this.name}: Initialized ColdFusion session for profile fetching`);
      }

      // POST the Pick form to load the profile
      const formData = new URLSearchParams();
      formData.set('menu', 'directory');
      formData.set('submenu', 'directoryPractisingMember');
      formData.set('mode', 'search');
      formData.set('record_id', recordId);
      formData.set('table_id', tableId);

      await rateLimiter.wait();
      const response = await this._httpReqWithCookies(
        'POST', this.baseUrl, formData.toString(), this._sessionCookies, rateLimiter
      );

      // Update stored cookies
      if (response.cookies) {
        this._sessionCookies = response.cookies;
      }

      if (response.statusCode !== 200) {
        log.warn(`Profile page returned ${response.statusCode} for record ${recordId}`);
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on profile page for record ${recordId}`);
        return null;
      }

      // Verify we got a profile page (look for the heading with lawyer name)
      if (!response.body.includes('content-heading') && !response.body.includes('Practice Location')) {
        log.warn(`Profile page did not load for record ${recordId} (session expired?)`);
        return null;
      }

      return cheerio.load(response.body);
    } catch (err) {
      log.warn(`Failed to fetch profile page: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a MemberPro profile page for additional contact info.
   *
   * Profile HTML structure:
   *   - Name in <DIV CLASS="content-heading">
   *   - Status/Enrolment in a table-auto with table-result-header/table-result cells
   *   - Firm in <div class="content-subheading">
   *   - Address as plain text with <BR> tags
   *   - Phone/Fax in nested table with form-label cells ("Office", "Fax")
   *   - Practice areas in a table with percentages
   *   - Email as mailto: links (when available)
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields: phone, firm_name, email, address, admission_date
   */
  parseProfilePage($) {
    const result = {};

    // Firm name from content-subheading
    const firmHeading = $('.content-subheading').first().text().trim();
    if (firmHeading && firmHeading.length > 1 && firmHeading.length < 200) {
      result.firm_name = firmHeading;
    }

    // Phone — look for "Office" label in form-label cells
    $('td.form-label, td.table-result-header').each((_, el) => {
      const label = $(el).text().trim().toLowerCase();
      const valueCell = $(el).next('td');
      const value = valueCell.text().trim();

      if (label === 'office' && value) {
        const phoneMatch = value.match(/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/);
        if (phoneMatch) {
          result.phone = phoneMatch[0].trim();
        }
      }
    });

    // Email from mailto links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      const email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
      if (email && email.includes('@')) {
        result.email = email;
      }
    }

    // Address — extract from the Practice Location section
    // The address is in a table after content-subheading, as plain text with <BR> tags
    const locationSection = $('td.table-result-header').filter((_, el) =>
      $(el).text().trim() === 'Practice Location'
    ).closest('table');

    if (locationSection.length) {
      // Find the cell containing the address (after content-subheading)
      const addrCell = locationSection.find('td[colspan]');
      if (addrCell.length) {
        // Clone and remove the firm name div to avoid duplicating it in the address
        const addrClone = addrCell.clone();
        addrClone.find('.content-subheading').remove();
        const addrHtml = addrClone.html() || '';
        // Split on <BR> tags and collect address lines
        const lines = addrHtml
          .split(/<br\s*\/?>/i)
          .map(line => line.replace(/<[^>]+>/g, '').trim())
          .filter(line => line && line.length > 0);

        // Remove country-only lines
        const addrLines = lines.filter(line =>
          line !== 'Canada' &&
          line !== 'United States'
        );

        if (addrLines.length > 0) {
          result.address = addrLines.join(', ').replace(/\s+/g, ' ').trim();
          result.address = result.address.replace(/,\s*$/, '');
        }
      }
    }

    // Admission date (enrolment date) — in the status table
    $('td.table-result-header').each((_, el) => {
      const header = $(el).text().trim();
      if (header === 'Enrolment Date') {
        const $parentRow = $(el).closest('tr');
        const $dataRow = $parentRow.next('tr');
        if ($dataRow.length) {
          const cells = $dataRow.find('td.table-result');
          // Find the cell that corresponds to Enrolment Date column
          const headerCells = $parentRow.find('td.table-result-header');
          headerCells.each((idx, hCell) => {
            if ($(hCell).text().trim() === 'Enrolment Date') {
              const dateText = $(cells[idx]).text().trim().replace(/&nbsp;/g, '').replace(/\u00A0/g, '');
              if (dateText) {
                result.admission_date = dateText;
              }
            }
          });
        }
      }
    });

    // Practice areas
    const practiceAreas = [];
    $('td.table-result').each((_, el) => {
      const text = $(el).text().trim();
      if (text.includes('%') && text.match(/\(\d+%\)/)) {
        practiceAreas.push(text);
      }
    });
    if (practiceAreas.length > 0) {
      result.practice_areas = practiceAreas.join('; ');
    }

    return result;
  }

  /**
   * Override search() for MemberPro session-based POST workflow.
   *
   * Flow per city/prefix:
   *   1. GET search page → session cookies
   *   2. POST search form with person_nm, city_nm, member_status_cl → results page
   *   3. Parse #member-directory table for results
   *
   * Iterates A-Z last name prefixes per city.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    // Step 1: Initialize ColdFusion session
    try {
      await rateLimiter.wait();
      const initResp = await this._httpReqWithCookies(
        'GET', this.searchPageUrl, null, '', rateLimiter
      );
      this._sessionCookies = initResp.cookies || '';
      log.info(`${this.name}: Initialized ColdFusion session`);
    } catch (err) {
      log.error(`${this.name}: Failed to initialize session: ${err.message}`);
      return;
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let totalForCity = 0;

      for (const prefix of this.lastNamePrefixes) {
        if (options.maxPages && totalForCity >= options.maxPages * this.pageSize) break;

        // Step 2: POST search form
        const formData = new URLSearchParams();
        formData.set('person_nm', prefix);
        formData.set('first_nm', '');
        formData.set('member_status_cl', 'PRAC');
        formData.set('city_nm', city);
        formData.set('location_nm', '');
        formData.set('gender_cl', '');
        formData.set('language_cl', '');
        formData.set('area_ds', practiceCode || '');
        formData.set('LSR_in', 'N');
        formData.set('mode', 'search');

        log.info(`POST ${this.searchUrl} [City=${city}, Prefix=${prefix}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpReqWithCookies(
            'POST', this.searchUrl, formData.toString(), this._sessionCookies, rateLimiter
          );
          if (response.cookies) {
            this._sessionCookies = response.cookies;
          }
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          continue;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city}/${prefix} — skipping`);
          yield { _captcha: true, city, prefix };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) continue;

        log.success(`Found ${attorneys.length} results for ${city}/${prefix}`);

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
          totalForCity++;
        }
      }

      if (totalForCity > 0) {
        log.success(`Found ${totalForCity} total results for ${city}`);
      } else {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      }
    }
  }
}

module.exports = new AlbertaScraper();
