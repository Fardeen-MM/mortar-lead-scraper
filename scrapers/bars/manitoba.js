/**
 * Manitoba Law Society Scraper
 *
 * Source: https://portal.lawsociety.mb.ca/lookup/
 * Method: PHP + reCAPTCHA v3 (score-based, no visible prompt)
 *
 * The member lookup uses reCAPTCHA v3, which is score-based and does not present
 * a visual challenge. The scraper submits POST requests to the lookup endpoint.
 * If CAPTCHA enforcement increases, it yields a _captcha signal.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ManitobaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'manitoba',
      stateCode: 'CA-MB',
      baseUrl: 'https://portal.lawsociety.mb.ca/lookup/',
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
        'aboriginal/indigenous': 'Aboriginal',
        'insurance':             'Insurance',
      },
      defaultCities: [
        'Winnipeg', 'Brandon', 'Steinbach', 'Thompson',
      ],
    });
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
   * Parse PHP member lookup results page.
   * Manitoba's lookup renders results in a table or list format.
   */
  parseResultsPage($) {
    const attorneys = [];

    // Table-based results
    $('table tr, .results-table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstText = $(cells[0]).text().trim();
      if (/^(name|member|last|first|#)$/i.test(firstText)) return;
      if (!firstText || firstText.length < 2) return;

      const nameCell = $(cells[0]);
      const profileLink = nameCell.find('a').attr('href') || '';

      let fullName = firstText;
      let city = cells.length > 1 ? $(cells[1]).text().trim() : '';
      let status = cells.length > 2 ? $(cells[2]).text().trim() : '';
      let barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';
      let firm = '';
      let phone = '';
      let email = '';

      // Detect phone in any cell
      for (let c = 1; c < cells.length; c++) {
        const cellText = $(cells[c]).text().trim();
        if (/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/.test(cellText) && !phone) {
          phone = cellText;
        }
      }

      // Check for email
      const mailtoLink = $row.find('a[href^="mailto:"]');
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
        firm_name: firm,
        city: city,
        state: 'CA-MB',
        phone,
        email,
        website: '',
        bar_number: barNumber.replace(/^#?\s*/, '').trim(),
        bar_status: status || 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://portal.lawsociety.mb.ca${profileLink}` : ''),
      });
    });

    // Fallback: div-based or list-based results
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .lookup-result, .result-item').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, .name, .member-name, strong').first();
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 3) return;

        const profileLink = nameEl.attr('href') || $el.find('a').first().attr('href') || '';
        const firm = $el.find('.firm, .company, .employer').text().trim();
        const city = $el.find('.city, .location').text().trim();
        const phone = $el.find('.phone, .telephone').text().trim();
        const status = $el.find('.status, .member-status').text().trim();
        const barNumber = $el.find('.member-number, .bar-number, .call-number').text().trim();

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
          firm_name: firm,
          city: city,
          state: 'CA-MB',
          phone,
          email,
          website: '',
          bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
          bar_status: status || 'Active',
          profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://portal.lawsociety.mb.ca${profileLink}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count.
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
   * Override search() for PHP + reCAPTCHA v3 POST-based workflow.
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

        // Build POST form data for the PHP lookup
        const formData = new URLSearchParams();
        formData.set('action', 'search');
        formData.set('city', city);
        formData.set('status', 'Practising');
        formData.set('submitted', '1');
        if (practiceCode) {
          formData.set('practice_area', practiceCode);
        }
        if (page > 1) {
          formData.set('page', String(page));
          formData.set('offset', String((page - 1) * this.pageSize));
        }
        // reCAPTCHA v3 token placeholder — server may still process without it
        formData.set('g-recaptcha-response', '');

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData.toString(), rateLimiter);
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

        // Check for reCAPTCHA enforcement
        if (this.detectCaptcha(response.body) || response.body.includes('recaptcha') || response.body.includes('g-recaptcha')) {
          // reCAPTCHA v3 is score-based — if we still get results, continue
          // Only skip if response body is clearly a CAPTCHA challenge page
          if (response.body.includes('challenge-form') || response.body.includes('Please verify')) {
            log.warn(`reCAPTCHA v3 enforcement detected on page ${page} for ${city} — skipping`);
            yield { _captcha: true, city, page };
            break;
          }
          log.info(`reCAPTCHA v3 present but not blocking — continuing`);
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

module.exports = new ManitobaScraper();
