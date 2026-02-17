/**
 * Alaska Bar Association — CV5 (cvweb v5.4) Search API
 *
 * Source: https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm
 * Platform: Euclid Technology CV Web Templates v5.4.0
 * Method: POST search + GET profile + GET address AJAX
 *
 * Search API:
 *   POST https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/CustomList
 *   Form params: SQLNAME=AKMEMDIR, CITY=..., RANGE=start/pagesize
 *   Returns HTML table: Bar Number, Full Name (Last, First), Organization, Type, Status, Admission Date
 *
 * Address API (AJAX, no separate profile page needed):
 *   GET utilities.dll/customlist?SQLNAME=GETMEMDIRADDR&CUSTOMERCD={id}&ADDRESSTYPE=Work&wbp=Customer_Address.htm
 *   Returns: Organization, Address, Phone, Fax, Email
 *
 * Total records: ~4,000 attorneys
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const SEARCH_URL = 'https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/CustomList';
const ADDRESS_URL = 'https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/customlist';

class AlaskaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alaska',
      stateCode: 'AK',
      baseUrl: SEARCH_URL,
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Anchorage', 'Fairbanks', 'Juneau', 'Wasilla',
        'Sitka', 'Kenai', 'Palmer', 'Kodiak',
      ],
    });
  }

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
   * Override CAPTCHA detection — AK's CV5 pages include a commented-out
   * reCAPTCHA script tag that triggers the base class false positive.
   * Strip HTML comments before checking.
   */
  detectCaptcha(body) {
    const stripped = body.replace(/<!--[\s\S]*?-->/g, '');
    return stripped.includes('captcha') || stripped.includes('CAPTCHA') ||
           stripped.includes('challenge-form');
  }

  /**
   * HTTP POST to the cvweb search API.
   */
  _httpPost(url, formData, ua) {
    return new Promise((resolve, reject) => {
      const postBody = new URLSearchParams(formData).toString();
      const parsed = new URL(url);

      const req = https.request({
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Origin': 'https://member.alaskabar.org',
          'Referer': 'https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `https://member.alaskabar.org${loc}`;
          return resolve(this.httpGet(loc, { getUserAgent: () => ua, wait: async () => {} }));
        }
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse search results HTML table.
   *
   * Table columns: Bar Number | Full Name | Organization | Type | Status | Class | Admission Date
   * Name format: "Last, First" with onclick containing customerCd
   */
  _parseSearchResults($) {
    const results = [];

    $('table tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 5) return;

      const barNumber = $(cells[0]).text().trim();
      const nameCell = $(cells[1]);
      const nameLink = nameCell.find('a[onclick*="memberProfile"]').first();
      if (!nameLink.length) return;

      // Extract customerCd from onclick="return cvweb.getCVPageLink('memberProfile','154')"
      const onclick = nameLink.attr('onclick') || '';
      const idMatch = onclick.match(/getCVPageLink\s*\(\s*'memberProfile'\s*,\s*'(\d+)'\s*\)/);
      if (!idMatch) return;

      const customerCd = idMatch[1];
      const rawName = nameLink.text().trim();
      if (!rawName) return;

      const organization = $(cells[2]).text().trim();
      const memberType = $(cells[3]).text().trim();
      const status = $(cells[4]).text().trim();
      const admissionDate = cells.length > 6 ? $(cells[6]).text().trim() : '';

      results.push({ rawName, customerCd, barNumber, organization, memberType, status, admissionDate });
    });

    return results;
  }

  /**
   * Extract total result count from heading.
   * Format: "Member List - 62 Match(es)"
   */
  _getResultCount($) {
    const heading = $('h1, h2, h3, h4').text() || '';
    const match = heading.match(/([\d,]+)\s+Match/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse "Last, First" name format used by AK results.
   */
  _parseReversedName(rawName) {
    // Handle "Last, First Middle" format
    const parts = rawName.split(',');
    if (parts.length >= 2) {
      const lastName = parts[0].trim();
      const firstParts = parts.slice(1).join(',').trim().split(/\s+/);
      const firstName = firstParts[0] || '';
      return { firstName, lastName, fullName: `${firstName} ${lastName}`.trim() };
    }
    // Fallback: space-separated
    const { firstName, lastName } = this.splitName(rawName);
    return { firstName, lastName, fullName: rawName };
  }

  /**
   * Fetch address/contact info via AJAX endpoint.
   * Returns: organization, address, phone, fax, email
   */
  async _fetchAddress(customerCd, rateLimiter) {
    const url = `${ADDRESS_URL}?SQLNAME=GETMEMDIRADDR&CUSTOMERCD=${customerCd}&ADDRESSTYPE=Work&wmt=none&whp=none&wbp=Customer_Address.htm&wnr=Customer_Address_None.htm`;

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(url, rateLimiter);

      if (response.statusCode !== 200) {
        return null;
      }

      const $ = cheerio.load(response.body);
      const result = {};

      // Parse dt/dd pairs
      $('dl dt').each((_, dt) => {
        const label = $(dt).text().trim().toLowerCase();
        const dd = $(dt).next('dd');
        if (!dd.length) return;

        if (label === 'organization' || label === 'employer') {
          result.firm_name = dd.text().trim();
        } else if (label === 'work phone' || label === 'phone') {
          const phone = dd.text().trim();
          if (phone.length > 5) result.phone = phone;
        } else if (label === 'fax') {
          result.fax = dd.text().trim();
        } else if (label === 'email address' || label === 'email') {
          const emailEl = dd.find('a#emaildd, a[href^="mailto:"]');
          const email = emailEl.length ? emailEl.text().trim() : dd.text().trim();
          if (email && email.includes('@')) result.email = email.toLowerCase();
        }
      });

      // Parse address from <address> element
      const addrEl = $('address').first();
      if (addrEl.length) {
        const addrHtml = addrEl.html() || '';
        const lines = addrHtml
          .replace(/<br\s*\/?>/gi, '\n')
          .replace(/<[^>]+>/g, '')
          .split('\n')
          .map(l => l.trim())
          .filter(l => l.length > 0);

        for (const line of lines) {
          const cityStateZip = line.match(/^(.+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
          if (cityStateZip) {
            result.city = cityStateZip[1].trim();
            result.state = cityStateZip[2];
            result.zip = cityStateZip[3];
            break;
          }
        }

        // Street address is typically the first line before city/state/zip
        if (lines.length > 0 && !lines[0].match(/^\d{5}/)) {
          result.address = lines[0];
        }
      }

      return result;
    } catch (err) {
      log.warn(`Failed to fetch AK address for ${customerCd}: ${err.message}`);
      return null;
    }
  }

  /**
   * Search the Alaska Bar directory by city, paginate, and fetch contact info.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const ua = rateLimiter.getUserAgent();
    const cities = this.getCities(options);

    // Limit address fetches in test mode
    const maxDetailFetches = options.maxPages ? 5 : Infinity;
    let totalDetailFetches = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const rangeStart = (page - 1) * this.pageSize + 1;
        const formData = {
          'SQLNAME': 'AKMEMDIR',
          'WBP': 'Customer_List.htm',
          'WHP': 'Customer_Header.htm',
          'WNR': 'Customer_norec.htm',
          'RANGE': `${rangeStart}/${this.pageSize}`,
          'SORT': 'LASTNAME, FIRSTNAME',
          'LASTNAME': '',
          'FIRSTNAME': '',
          'EMAIL': '',
          'ORGNAME': '',
          'CITY': city,
          'ZIP': '',
          'STATECD': '',
          'COUNTRY': '',
          'CUSTOMERALTCD': '',
        };

        log.info(`Page ${page} — RANGE=${rangeStart}/${this.pageSize} for ${city}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpPost(SEARCH_URL, formData, ua);
        } catch (err) {
          log.error(`AK search request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from AK Bar`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`AK Bar returned ${response.statusCode}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on AK page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this._getResultCount($);
          if (totalResults === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const searchResults = this._parseSearchResults($);

        if (searchResults.length === 0) {
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
        log.info(`Page ${page}: ${searchResults.length} results parsed`);

        for (const sr of searchResults) {
          const { firstName, lastName, fullName } = this._parseReversedName(sr.rawName);

          // Fetch address/contact AJAX for email/phone
          let addrInfo = null;
          if (totalDetailFetches < maxDetailFetches) {
            addrInfo = await this._fetchAddress(sr.customerCd, rateLimiter);
            totalDetailFetches++;
          }

          const attorney = {
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: addrInfo?.firm_name || sr.organization || '',
            address: addrInfo?.address || '',
            city: addrInfo?.city || city,
            state: addrInfo?.state || 'AK',
            zip: addrInfo?.zip || '',
            phone: addrInfo?.phone || '',
            email: addrInfo?.email || '',
            fax: addrInfo?.fax || '',
            bar_number: sr.barNumber,
            bar_status: sr.status || '',
            admission_date: sr.admissionDate || '',
            profile_url: `https://member.alaskabar.org/cv5/cgi-bin/memberdll.dll/info?customercd=${sr.customerCd}&wrp=customer_profile.htm`,
          };

          yield this.transformResult(attorney, practiceArea);
        }

        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new AlaskaScraper();
