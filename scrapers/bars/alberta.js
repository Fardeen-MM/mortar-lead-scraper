/**
 * Alberta Law Society Scraper
 *
 * Source: https://lsa.memberpro.net/main/body.cfm
 * Method: ColdFusion (MemberPro platform) — form POST + Cheerio HTML parsing
 *
 * MemberPro is a hosted ColdFusion directory used by several Canadian law societies.
 * Overrides search() for POST-based workflow.
 */

const https = require('https');
const http = require('http');
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
        'estate planning/wills': 'Wills and Estates',
        'estate planning':       'Wills and Estates',
        'wills':                 'Wills and Estates',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative',
        'environmental':         'Environmental',
        'oil and gas':           'Oil and Gas',
        'aboriginal':            'Aboriginal',
        'banking/finance':       'Banking and Finance',
      },
      defaultCities: [
        'Calgary', 'Edmonton', 'Red Deer', 'Lethbridge',
        'Medicine Hat', 'St. Albert', 'Grande Prairie',
      ],
    });

    this.searchUrl = 'https://lsa.memberpro.net/main/body.cfm?menu=directory';
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
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
          'Accept': 'text/html,application/json,*/*',
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
   * Not used — search() is fully overridden for POST-based workflow.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST workflow`);
  }

  /**
   * Parse MemberPro ColdFusion results page.
   * MemberPro typically renders results in a table with member details.
   */
  parseResultsPage($) {
    const attorneys = [];

    // MemberPro table-based results
    $('table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstText = $(cells[0]).text().trim();
      // Skip header rows and empty rows
      if (/^(name|member|last|first|#|no\.)$/i.test(firstText)) return;
      // Skip multi-word headers (e.g., "Lawyer Name", "Practising Status", "Member Number")
      if (/\b(name|status|member|number|city|province|area)\b/i.test(firstText) &&
          /\b(lawyer|practising|member|bar|first|last|firm|phone)\b/i.test(firstText)) return;
      // Skip if second cell also looks like a header
      if (cells.length > 1) {
        const secondText = $(cells[1]).text().trim();
        if (/\b(city|status|member|province|number)\b/i.test(secondText) &&
            /\b(name|status|city|member|practising)\b/i.test(firstText)) return;
      }
      if (!firstText || firstText.length < 2) return;

      const nameCell = $(cells[0]);
      const profileLink = nameCell.find('a').attr('href') || '';

      // MemberPro layout: Name | City | Status | Member #
      let fullName = firstText;
      let city = cells.length > 1 ? $(cells[1]).text().trim() : '';
      let status = cells.length > 2 ? $(cells[2]).text().trim() : '';
      let barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';
      let firm = '';
      let phone = '';
      let email = '';

      // Check for additional columns
      for (let c = 1; c < cells.length; c++) {
        const cellText = $(cells[c]).text().trim();
        // Detect phone numbers
        if (/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/.test(cellText) && !phone) {
          phone = cellText;
        }
      }

      // Check for mailto links
      const mailtoLink = $row.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

      // Parse name — "Last, First" format is common in MemberPro
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = parts[1] ? parts[1].split(/\s+/)[0] : '';
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: displayName,
        firm_name: firm,
        city: city,
        state: 'CA-AB',
        phone,
        email,
        website: '',
        bar_number: barNumber.replace(/^#?\s*/, '').trim(),
        bar_status: status || 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://lsa.memberpro.net${profileLink}` : ''),
      });
    });

    // Fallback: div-based results
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .directory-item').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .member-name').text().trim();
        if (!fullName || fullName.length < 3) return;

        const profileLink = nameEl.attr('href') || '';
        const city = $el.find('.city, .location').text().trim();
        const phone = $el.find('.phone').text().trim();
        const status = $el.find('.status').text().trim();
        const barNumber = $el.find('.member-number, .bar-number').text().trim();

        let email = '';
        const mailtoLink = $el.find('a[href^="mailto:"]');
        if (mailtoLink.length) {
          email = mailtoLink.attr('href').replace('mailto:', '').trim();
        }

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] ? parts[1].split(/\s+/)[0] : '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: displayName,
          firm_name: '',
          city: city,
          state: 'CA-AB',
          phone,
          email,
          website: '',
          bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
          bar_status: status || 'Active',
          profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://lsa.memberpro.net${profileLink}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from MemberPro page.
   */
  extractResultCount($) {
    const text = $('body').text();

    const matchOf = text.match(/(?:Displaying|Showing|Results?)\s*:?\s*\d+\s*[-–to]+\s*\d+\s+of\s+([\d,]+)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total\s*:?\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for MemberPro POST-based workflow.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // MemberPro form data
        const formData = new URLSearchParams();
        formData.set('menu', 'directory');
        formData.set('action', 'search');
        formData.set('city', city);
        formData.set('province', 'AB');
        formData.set('status', 'Active');
        if (practiceCode) {
          formData.set('practiceArea', practiceCode);
        }
        if (page > 1) {
          formData.set('page', String(page));
          formData.set('startrow', String((page - 1) * this.pageSize + 1));
        }

        log.info(`Page ${page} — POST ${this.searchUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.searchUrl, formData.toString(), rateLimiter);
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
          log.error(`Unexpected status ${response.statusCode} — skipping`);
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
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            const testAttorneys = this.parseResultsPage($);
            if (testAttorneys.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testAttorneys.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === '>>';
        }).length > 0;

        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages && !hasNext) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new AlbertaScraper();
