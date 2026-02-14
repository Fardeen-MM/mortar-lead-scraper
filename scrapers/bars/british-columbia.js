/**
 * British Columbia Law Society Scraper
 *
 * Source: https://www.lawsociety.bc.ca/lsbc/apps/mbr-search/mbr-search.cfm
 * Method: ColdFusion + DataTables — classic form POST + Cheerio HTML parsing
 *
 * The member search uses POST requests to submit search criteria and returns
 * paginated HTML results. Overrides search() for POST-based workflow.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class BritishColumbiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'british-columbia',
      stateCode: 'CA-BC',
      baseUrl: 'https://www.lawsociety.bc.ca/lsbc/apps/mbr-search/mbr-search.cfm',
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

    this.resultUrl = 'https://www.lawsociety.bc.ca/lsbc/apps/mbr-search/result.cfm';
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
   * Parse ColdFusion/DataTables member search results.
   */
  parseResultsPage($) {
    const attorneys = [];

    // DataTables-style results
    $('table#tblResults tr, table.dataTable tr, table.results tr, table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstText = $(cells[0]).text().trim();
      // Skip header rows
      if (/^(name|member|#)$/i.test(firstText)) return;
      if (!firstText || firstText.length < 2) return;

      const nameCell = $(cells[0]);
      const profileLink = nameCell.find('a').attr('href') || '';

      // ColdFusion results typically: Name | City | Status | Member #
      let fullName = firstText;
      let city = cells.length > 1 ? $(cells[1]).text().trim() : '';
      let status = cells.length > 2 ? $(cells[2]).text().trim() : '';
      let barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';
      let firm = '';
      let phone = '';

      // Some layouts may have more columns
      if (cells.length > 4) {
        firm = $(cells[4]).text().trim();
      }
      if (cells.length > 5) {
        phone = $(cells[5]).text().trim();
      }

      // Check for email
      let email = '';
      const mailtoLink = $row.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

      // Parse name — ColdFusion often uses "Last, First" format
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

      // Clean up bar number (remove non-numeric except for alphanumeric IDs)
      barNumber = barNumber.replace(/^#?\s*/, '').trim();

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: displayName,
        firm_name: firm,
        city: city,
        state: 'CA-BC',
        phone,
        email,
        website: '',
        bar_number: barNumber,
        bar_status: status || 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://www.lawsociety.bc.ca${profileLink}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from ColdFusion result page.
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
   * Override search() for POST-based ColdFusion workflow.
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

        // Build ColdFusion POST form data
        const formData = new URLSearchParams();
        formData.set('city', city);
        formData.set('status', 'Practising');
        formData.set('submit', 'Search');
        if (practiceCode) {
          formData.set('practiceArea', practiceCode);
        }
        if (page > 1) {
          formData.set('page', String(page));
          formData.set('startRow', String((page - 1) * this.pageSize + 1));
        }

        const postUrl = page === 1 ? this.baseUrl : this.resultUrl;
        log.info(`Page ${page} — POST ${postUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(postUrl, formData.toString(), rateLimiter);
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

        // Check for next page
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

module.exports = new BritishColumbiaScraper();
