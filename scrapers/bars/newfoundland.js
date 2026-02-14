/**
 * Newfoundland and Labrador Law Society Scraper
 *
 * Source: https://lsnl.memberpro.net/main/body.cfm
 * Method: ColdFusion (MemberPro platform) — form POST + Cheerio
 *
 * The form named "Next" uses fields: person_nm (last name), first_nm (first name),
 * city_nm (city dropdown), area_ds (practice area), member_status_cl, gender_cl, language_cl.
 * Overrides search() for POST-based workflow.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NewfoundlandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'newfoundland',
      stateCode: 'CA-NL',
      baseUrl: 'https://lsnl.memberpro.net/main/body.cfm',
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
        'maritime':              'Maritime',
        'municipal':             'Municipal',
      },
      defaultCities: [
        "St. John's", 'Corner Brook', 'Mount Pearl', 'Conception Bay South',
      ],
    });

    this.searchUrl = 'https://lsnl.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop';
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
   * Same MemberPro platform as Alberta — table-based results with member details.
   */
  parseResultsPage($) {
    const attorneys = [];

    // MemberPro results: TD.table-result cells in rows
    // Columns: Name (with link), City, Practising Status, Called (year), Firm
    const rows = $('tr').filter((_, el) => {
      return $(el).find('td.table-result').length >= 2;
    });

    rows.each((_, el) => {
      const cells = $(el).find('td.table-result');
      if (cells.length < 3) return;

      // Name cell contains a link with <div class='font-size-plus'>Name, KC</div>
      const nameCell = $(cells[0]);
      const nameDiv = nameCell.find('div.font-size-plus');
      let fullName = (nameDiv.length ? nameDiv.text() : nameCell.text()).trim();
      if (!fullName || fullName.length < 3) return;

      // Strip honorifics (KC, QC)
      fullName = fullName.replace(/,?\s*(KC|QC|K\.C\.|Q\.C\.)$/i, '').trim();

      const city = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const status = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const calledYear = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const firm = cells.length > 4 ? $(cells[4]).text().trim() : '';

      const nameParts = this.splitName(fullName);

      attorneys.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: fullName,
        firm_name: firm,
        city: city,
        state: 'CA-NL',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        admission_date: calledYear,
        bar_status: status || 'Practising',
        profile_url: '',
      });
    });

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

        // MemberPro form "Next" uses these actual field names:
        //   person_nm (last name), first_nm (first name), city_nm (city dropdown),
        //   area_ds (practice area), member_status_cl, gender_cl, language_cl
        const formData = new URLSearchParams();
        formData.set('person_nm', '');
        formData.set('first_nm', '');
        formData.set('city_nm', city);
        formData.set('member_status_cl', 'PRAC');
        if (practiceCode) {
          formData.set('area_ds', practiceCode);
        }
        formData.set('location_nm', '');
        formData.set('language_cl', '');
        formData.set('mode', 'search');
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

module.exports = new NewfoundlandScraper();
