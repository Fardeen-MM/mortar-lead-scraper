/**
 * British Columbia Law Society Scraper
 *
 * Source: https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/mbr-search.cfm
 * Method: ColdFusion GET form + DataTables HTML parsing
 *
 * The search form uses GET with fields: txt_last_nm, txt_given_nm, txt_city,
 * txt_search_type, is_submitted, results_no, member_search.
 * Results table (#searchResultTable) has a single Name column with links to detail pages.
 * Iterates last name initials (A-Z) per city since city-only search may not work.
 *
 * Profile pages (mbr-details.cfm) require ColdFusion session cookies from a prior search.
 * They provide firm name, phone, address, and call (admission) date.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class BritishColumbiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'british-columbia',
      stateCode: 'CA-BC',
      baseUrl: 'https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/mbr-search.cfm',
      pageSize: 25,
      practiceAreaCodes: {
        'family':                'Family',
        'family law':            'Family',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'real estate':           'Real Estate',
        'corporate/commercial':  'Corporate Commercial',
        'corporate':             'Corporate Commercial',
        'commercial':            'Corporate Commercial',
        'personal injury':       'Personal Injury',
        'employment':            'Employment',
        'labour':                'Labour',
        'immigration':           'Immigration',
        'estate planning/wills': 'Wills Estates',
        'estate planning':       'Wills Estates',
        'wills':                 'Wills Estates',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative',
        'environmental':         'Environmental',
        'aboriginal':            'Aboriginal',
        'insurance':             'Insurance',
      },
      defaultCities: [
        'Vancouver', 'Victoria', 'Surrey', 'Burnaby',
        'Richmond', 'Kelowna', 'Kamloops', 'Nanaimo',
      ],
    });

    // BC requires at least 2 characters for last name search
    this.lastNamePrefixes = ['Smith', 'Lee', 'Brown', 'Chan', 'Wong'];

    // ColdFusion session cookies — set during search(), reused by fetchProfilePage()
    this._sessionCookies = '';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Clean a BC profile URL by removing fragment and encoded newlines (%0A).
   * The ColdFusion encrypted parameter sometimes includes a trailing %0A
   * which causes URL parsing issues in Node.js.
   */
  _cleanProfileUrl(href) {
    if (!href) return '';
    // Remove fragment
    let clean = href.split('#')[0];
    // Remove encoded newlines/carriage returns
    clean = clean.replace(/%0[aAdD]/g, '');
    // Remove actual whitespace
    clean = clean.replace(/[\n\r\s]+/g, '');
    return clean;
  }

  /**
   * HTTP GET with cookie support for ColdFusion session-based profile pages.
   * Follows redirects and accumulates cookies.
   *
   * @param {string} url - Full URL to fetch
   * @param {string} cookies - Cookie header string
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @param {number} redirectCount - Redirect depth counter
   * @returns {Promise<{statusCode: number, body: string, cookies: string}>}
   */
  _httpGetWithCookies(url, cookies, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        headers: {
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,*/*',
          'Cookie': cookies || '',
        },
        timeout: 15000,
      };

      https.get(options, (res) => {
        // Accumulate cookies
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]).join('; ');
        const allCookies = cookies
          ? (newCookies ? cookies + '; ' + newCookies : cookies)
          : newCookies;

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          // Handle relative URLs
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          } else if (!redirect.startsWith('http')) {
            const base = url.substring(0, url.lastIndexOf('/') + 1);
            redirect = base + redirect;
          }
          return resolve(this._httpGetWithCookies(redirect, allCookies, rateLimiter, redirectCount + 1));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body, cookies: allCookies }));
      }).on('error', reject);
    });
  }

  /**
   * Override fetchProfilePage to handle ColdFusion session cookies.
   * BC profile pages require cookies from a prior search session.
   * If no session cookies are available, initiates a fresh session
   * by hitting the search page first.
   *
   * @param {string} url - The profile page URL
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {CheerioStatic|null} Cheerio instance or null on failure
   */
  async fetchProfilePage(url, rateLimiter) {
    if (!url) return null;

    // Clean the URL
    const cleanUrl = this._cleanProfileUrl(url);
    if (!cleanUrl) return null;

    try {
      // If we don't have session cookies, get them from the search page
      if (!this._sessionCookies) {
        await rateLimiter.wait();
        const initResp = await this._httpGetWithCookies(this.baseUrl, '', rateLimiter);
        this._sessionCookies = initResp.cookies || '';
        log.info(`${this.name}: Initialized ColdFusion session for profile fetching`);
      }

      await rateLimiter.wait();
      const response = await this._httpGetWithCookies(cleanUrl, this._sessionCookies, rateLimiter);

      // Update stored cookies
      if (response.cookies) {
        this._sessionCookies = response.cookies;
      }

      if (response.statusCode !== 200) {
        log.warn(`Profile page returned ${response.statusCode}: ${cleanUrl}`);
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on profile page: ${cleanUrl}`);
        return null;
      }

      // Verify we got a profile page (not the search form)
      if (!response.body.includes("Lawyer's Profile") && !response.body.includes('form-label')) {
        log.warn(`Profile page did not load (session expired?): ${cleanUrl}`);
        return null;
      }

      return cheerio.load(response.body);
    } catch (err) {
      log.warn(`Failed to fetch profile page: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a BC Lawyer Directory profile page for additional contact info.
   *
   * Profile pages use Bootstrap row/col layout with form-label divs:
   *   <div class="row mb-2">
   *     <div class="col-sm-3 form-label">Phone number</div>
   *     <div class="col-sm-9">604 800-0774 [Firm]</div>
   *   </div>
   *
   * Available fields: Current status, Call date, Primary location (firm),
   * Contact address, Phone number, Fax number, Email (usually CAPTCHA-protected).
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields: phone, firm_name, admission_date, address
   */
  parseProfilePage($) {
    const result = {};

    // Build a label→value map from the Bootstrap row layout
    const fields = {};
    $('.row.mb-2, .row.mb-3').each((_, el) => {
      const label = $(el).find('.form-label').first().text().trim().toLowerCase();
      const valueEl = $(el).find('.col-sm-9');
      if (label && valueEl.length) {
        fields[label] = valueEl;
      }
    });

    // Phone number — format: "604 800-0774 [Firm]" or "604 800-0774"
    if (fields['phone number']) {
      const phoneText = fields['phone number'].text().trim();
      // Extract phone number, stripping labels like [Firm], [Direct], etc.
      const phoneMatch = phoneText.match(/([\d().\s-]{7,})/);
      if (phoneMatch) {
        result.phone = phoneMatch[1].trim();
      }
    }

    // Firm name from "Primary location"
    if (fields['primary location']) {
      const firmText = fields['primary location'].text().trim();
      if (firmText && firmText.length > 1 && firmText.length < 200) {
        result.firm_name = firmText;
      }
    }

    // Call date (admission date) — format: "September 15, 2020"
    if (fields['call date']) {
      const dateText = fields['call date'].text().trim();
      if (dateText) {
        result.admission_date = dateText;
      }
    }

    // Contact address — multi-line with <br> tags
    if (fields['contact address']) {
      const addrEl = fields['contact address'];
      // Get text but stop before action links like "Show Map"
      const addrHtml = addrEl.html() || '';
      // Split on <br> tags and collect address lines, stop at action links
      const lines = addrHtml
        .split(/<br\s*\/?>/i)
        .map(line => line.replace(/<[^>]+>/g, '').trim())
        .filter(line => line && !line.startsWith('[') && !line.includes('Show Map')
                && !line.includes('Add to Outlook') && !line.includes('QRCode'));
      if (lines.length > 0) {
        result.address = lines.join(', ').replace(/\s+/g, ' ').trim();
        // Remove trailing comma
        result.address = result.address.replace(/,\s*$/, '');
      }
    }

    // Email — usually behind CAPTCHA, but check anyway
    if (fields['email']) {
      const emailText = fields['email'].text().trim();
      if (emailText && emailText !== 'Not Available') {
        // Check for actual email pattern
        const emailMatch = emailText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
        if (emailMatch) {
          result.email = emailMatch[0].toLowerCase();
        }
      }
      // Also check for mailto links (in case CAPTCHA was bypassed)
      const mailtoLink = fields['email'].find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
      }
    }

    // Bar status from "Current status"
    if (fields['current status']) {
      const statusText = fields['current status'].text().trim();
      if (statusText) {
        result.bar_status = statusText;
      }
    }

    return result;
  }

  /**
   * Parse #searchResultTable — single Name column with links.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('#searchResultTable tbody tr').each((_, el) => {
      const $row = $(el);
      const td = $row.find('td[data-title="Name"]');
      if (!td.length) return;

      const link = td.find('a');
      const fullName = (link.text() || td.text()).trim();
      if (!fullName || fullName.length < 3) return;

      const rawProfileLink = link.attr('href') || '';
      // Clean profile URL: remove fragment and encoded newlines
      const profileLink = this._cleanProfileUrl(rawProfileLink);

      // Strip honorifics (KC, QC, etc.)
      const cleanName = fullName.replace(/,?\s*(KC|QC|K\.C\.|Q\.C\.)$/i, '').trim();
      const nameParts = this.splitName(cleanName);

      attorneys.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: cleanName,
        firm_name: '',
        city: '',
        state: 'CA-BC',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Practising',
        profile_url: profileLink.startsWith('http') ? profileLink
          : (profileLink ? `https://www.lawsociety.bc.ca/lsbc/apps/lkup/directory/${profileLink}` : ''),
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?)\s+found/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    return 0;
  }

  /**
   * Override search() — GET-based ColdFusion form with last name initial iteration.
   * Stores session cookies for later profile page fetching.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let totalForCity = 0;

      for (const prefix of this.lastNamePrefixes) {
        if (options.maxPages && totalForCity >= 5) break;

        const params = new URLSearchParams({
          is_submitted: '1',
          txt_search_type: 'begins',
          txt_last_nm: prefix,
          txt_given_nm: '',
          txt_city: city,
          member_search: 'Search',
          results_no: String(this.pageSize),
        });

        const url = `${this.baseUrl}?${params.toString()}`;
        log.info(`GET ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          // Use cookie-aware GET to maintain ColdFusion session
          response = await this._httpGetWithCookies(url, this._sessionCookies, rateLimiter);
          // Store session cookies for profile page fetching
          if (response.cookies) {
            this._sessionCookies = response.cookies;
          }
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          continue;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city}/${prefix} — skipping`);
          yield { _captcha: true, city };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) continue;

        log.success(`Found ${attorneys.length} results for ${city}/${prefix}`);

        for (const attorney of attorneys) {
          attorney.city = city;
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

module.exports = new BritishColumbiaScraper();
