/**
 * Indiana Bar Association Scraper
 *
 * Source: https://courtapps.in.gov/rollofattorneys/search
 * Method: Returns 403 on automated fetch — try with browser-like headers
 *
 * The Indiana Roll of Attorneys is hosted on the courts.in.gov system.
 * It aggressively blocks non-browser requests. This scraper attempts to:
 *  1. Fetch the page with full browser headers to bypass 403
 *  2. Submit the search form with proper Referer and session cookies
 *  3. Parse HTML table results
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IndianaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'indiana',
      stateCode: 'IN',
      baseUrl: 'https://courtapps.in.gov/rollofattorneys/search',
      pageSize: 50,
      practiceAreaCodes: {
        'bankruptcy':            'bankruptcy',
        'business':              'business',
        'civil litigation':      'civil litigation',
        'corporate':             'corporate',
        'criminal':              'criminal',
        'criminal defense':      'criminal defense',
        'elder':                 'elder law',
        'employment':            'employment',
        'environmental':         'environmental',
        'estate planning':       'estate planning',
        'family':                'family law',
        'family law':            'family law',
        'immigration':           'immigration',
        'intellectual property': 'intellectual property',
        'personal injury':       'personal injury',
        'real estate':           'real estate',
        'tax':                   'tax',
      },
      defaultCities: [
        'Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend',
        'Carmel', 'Fishers', 'Bloomington', 'Hammond',
      ],
    });

    this.origin = 'https://courtapps.in.gov';
    this.searchUrl = 'https://courtapps.in.gov/rollofattorneys/search';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for IN Roll of Attorneys`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for IN Roll of Attorneys`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for IN Roll of Attorneys`);
  }

  /**
   * Build browser-like headers for bypassing 403.
   */
  _getBrowserHeaders(rateLimiter, extra = {}) {
    return {
      'User-Agent': rateLimiter.getUserAgent(),
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache',
      'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'Connection': 'keep-alive',
      'Referer': this.baseUrl,
      ...extra,
    };
  }

  /**
   * HTTP GET with browser headers, capturing cookies.
   */
  _httpGetBrowser(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._getBrowserHeaders(rateLimiter);
      if (this._cookies) {
        headers['Cookie'] = this._cookies;
      }

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers,
        timeout: 15000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        // Capture cookies
        const setCookies = res.headers['set-cookie'] || [];
        if (setCookies.length > 0) {
          this._cookies = setCookies.map(c => c.split(';')[0]).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this._httpGetBrowser(redirect, rateLimiter));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST with form data and browser headers.
   */
  httpPost(url, formData, rateLimiter, headers = {}) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          ...this._getBrowserHeaders(rateLimiter),
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Referer': this.baseUrl,
          'Origin': this.origin,
          ...(this._cookies ? { 'Cookie': this._cookies } : {}),
          ...headers,
        },
        timeout: 20000,
      };

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(opts, (res) => {
        const setCookies = res.headers['set-cookie'] || [];
        if (setCookies.length > 0) {
          this._cookies = setCookies.map(c => c.split(';')[0]).join('; ');
        }

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this._httpGetBrowser(redirect, rateLimiter));
        }

        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse an Indiana Roll of Attorneys profile page for additional contact info.
   *
   * Indiana attorney profile pages (linked from search results) are HTML pages
   * at courtapps.in.gov/rollofattorneys/ that may contain: phone, email,
   * firm name, address, admission date, and status details.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields extracted from the profile
   */
  parseProfilePage($) {
    const result = {};
    const bodyText = $('body').text();

    // Phone — look for tel: links first, then labeled patterns
    const telLink = $('a[href^="tel:"]').first();
    if (telLink.length) {
      result.phone = telLink.attr('href').replace('tel:', '').trim();
    } else {
      const phoneMatch = bodyText.match(/(?:Phone|Telephone|Office|Work|Business)[:\s]*([\d().\s-]+)/i) ||
                         bodyText.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
      if (phoneMatch) {
        result.phone = phoneMatch[1].trim();
      }
    }

    // Email — look for mailto: links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
    } else {
      // Fallback: email pattern in text
      const emailMatch = bodyText.match(/(?:Email|E-mail)[:\s]+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i);
      if (emailMatch) {
        result.email = emailMatch[1].toLowerCase();
      }
    }

    // Website — external links that aren't IN courts or excluded domains
    const inExcluded = ['courtapps.in.gov', 'courts.in.gov', 'in.gov', 'indianabar.org', 'inbar.org'];
    const isExcluded = (href) =>
      this.isExcludedDomain(href) || inExcluded.some(d => href.includes(d));

    $('a[href^="http"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (!isExcluded(href)) {
        result.website = href;
        return false; // break
      }
    });

    // Firm name / employer
    const firmMatch = bodyText.match(/(?:Firm|Employer|Company|Organization)[:\s]+(.+?)(?:\n|$)/i);
    if (firmMatch) {
      const firm = firmMatch[1].trim();
      if (firm && firm.length > 1 && firm.length < 200) {
        result.firm_name = firm;
      }
    }

    // Address — look for labeled address or structured address blocks
    const addrMatch = bodyText.match(/(?:Address|Location)[:\s]+(.+?)(?:\n\n|\nPhone|\nEmail|\nFirm|$)/is);
    if (addrMatch) {
      result.address = addrMatch[1].trim().replace(/\s+/g, ' ');
    }

    // Admission date
    const admitMatch = bodyText.match(/(?:Admit(?:ted|ssion)\s*(?:Date)?)[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\w+\s+\d{1,2},?\s+\d{4}|\d{4})/i);
    if (admitMatch) {
      result.admission_date = admitMatch[1].trim();
    }

    // Bar status
    const statusMatch = bodyText.match(/(?:Status|Standing)[:\s]+(Active|Inactive|Suspended|Retired|Resigned|Deceased|Disbarred)/i);
    if (statusMatch) {
      result.bar_status = statusMatch[1].trim();
    }

    // Education / law school
    const eduMatch = bodyText.match(/(?:Law\s*School|Education|J\.?D\.?)[:\s]+(.+?)(?:\n|$)/i);
    if (eduMatch) {
      const edu = eduMatch[1].trim();
      if (edu && edu.length > 2 && edu.length < 200) {
        result.education = edu;
      }
    }

    // Remove empty string values before returning
    for (const key of Object.keys(result)) {
      if (result[key] === '' || result[key] === undefined || result[key] === null) {
        delete result[key];
      }
    }

    return result;
  }

  /**
   * Parse attorney records from HTML results page.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Try table-based results
    $('table tr, .table tr').each((i, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|result|attorney)/i.test(fullName)) return;

      // Indiana Roll format: Name | Bar # | City | Status | Admission Date
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      const barNumber = cells.length > 1 ? $(cells[1]).text().trim().replace(/[^0-9-]/g, '') : '';
      const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const status = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const admissionDate = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const phone = cells.length > 5 ? $(cells[5]).text().trim().replace(/[^\d()-.\s+]/g, '') : '';

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
        firm_name: '',
        city: city,
        state: 'IN',
        phone: phone,
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        admission_date: admissionDate,
        profile_url: profileUrl ? (profileUrl.startsWith('http') ? profileUrl : `${this.origin}${profileUrl}`) : '',
      });
    });

    // Fallback: div/card-based results
    if (attorneys.length === 0) {
      $('.search-result, .attorney-result, .result-item, .attorney-listing').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = (parts[1] || '').split(/\s+/)[0];
        } else {
          const split = this.splitName(fullName);
          firstName = split.firstName;
          lastName = split.lastName;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}` : fullName,
          firm_name: $el.find('.firm, .firm-name').text().trim(),
          city: $el.find('.city, .location').text().trim(),
          state: 'IN',
          phone: ($el.find('.phone').text().trim() || '').replace(/[^\d()-.\s+]/g, ''),
          email: $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '',
          website: '',
          bar_number: $el.find('.bar-number, .barnum').text().trim().replace(/[^0-9-]/g, ''),
          bar_status: $el.find('.status').text().trim() || 'Active',
          profile_url: nameEl.attr('href') ? `${this.origin}${nameEl.attr('href')}` : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract result count from HTML.
   */
  _extractResultCount(body) {
    const text = cheerio.load(body)('body').text();
    const match = text.match(/([\d,]+)\s*(?:results?|records?|attorneys?|members?)\s*(?:found|returned|total)/i) ||
                  text.match(/(?:of|total[:\s]*)\s*([\d,]+)/i) ||
                  text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Override search() for Indiana Roll of Attorneys.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    this._cookies = null;

    // Step 1: Try to establish a session by fetching the search page
    log.info('Establishing session with IN Roll of Attorneys...');

    let sessionEstablished = false;
    let formTokens = {};

    try {
      await rateLimiter.wait();
      const response = await this._httpGetBrowser(this.searchUrl, rateLimiter);

      if (response.statusCode === 200) {
        sessionEstablished = true;
        log.success('Session established with IN Roll of Attorneys');

        // Extract any form tokens (CSRF, ViewState, etc.)
        const $ = cheerio.load(response.body);
        $('input[type="hidden"]').each((_, el) => {
          const name = $(el).attr('name') || '';
          const value = $(el).attr('value') || '';
          if (name) formTokens[name] = value;
        });
      } else if (response.statusCode === 403) {
        log.warn(`IN Roll of Attorneys returned 403 even with browser headers`);
        log.warn(`IN: This directory at ${this.baseUrl} blocks automated access.`);
        yield { _captcha: true, city: 'all', reason: '403 Forbidden — browser headers insufficient' };
        return;
      } else {
        log.warn(`IN Roll of Attorneys returned status ${response.statusCode}`);
      }
    } catch (err) {
      log.error(`Failed to establish session: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: `Connection failed: ${err.message}` };
      return;
    }

    if (!sessionEstablished) {
      yield { _captcha: true, city: 'all', reason: 'Could not establish session with IN Roll of Attorneys' };
      return;
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build form data
        const formData = {
          ...formTokens,
          'City': city,
          'State': 'IN',
          'Status': 'Active',
          'Submit': 'Search',
        };

        if (page > 1) {
          formData['Page'] = String(page);
        }

        log.info(`Page ${page} — POST ${this.searchUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.searchUrl, formData, rateLimiter);
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
          log.error(`Unexpected status ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        if (page === 1) {
          totalResults = this._extractResultCount(response.body);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        const attorneys = this._parseAttorneys(response.body);

        if (attorneys.length === 0) {
          if (page === 1) log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          break;
        }

        if (page === 1 && totalResults === 0) {
          log.success(`Found ${attorneys.length} results for ${city}`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        if (attorneys.length < this.pageSize) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        const totalPages = totalResults > 0 ? Math.ceil(totalResults / this.pageSize) : 0;
        if (totalPages > 0 && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new IndianaScraper();
