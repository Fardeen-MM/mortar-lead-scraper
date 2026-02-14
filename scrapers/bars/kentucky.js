/**
 * Kentucky Bar Association Scraper
 *
 * Source: https://kybar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=LawyerLocator.htm
 * Method: AJAX POST to cvweb.xhrPost endpoint with sanitized form data
 *
 * The KY Bar uses a CV5 (Community Voice) system similar to Alaska.
 * The Lawyer Locator form submits via cvweb.xhrPost which serializes form
 * fields into a POST body. The response is an HTML fragment containing
 * member listings rendered on the server side.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class KentuckyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'kentucky',
      stateCode: 'KY',
      baseUrl: 'https://kybar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=LawyerLocator.htm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BNK',
        'business':               'BUS',
        'civil litigation':       'CLT',
        'corporate':              'COR',
        'criminal':               'CRM',
        'criminal defense':       'CRM',
        'domestic relations':     'DOM',
        'elder':                  'ELD',
        'employment':             'EMP',
        'environmental':          'ENV',
        'estate planning':        'EST',
        'family':                 'FAM',
        'family law':             'FAM',
        'general practice':       'GEN',
        'immigration':            'IMM',
        'insurance':              'INS',
        'intellectual property':  'IPR',
        'labor':                  'LAB',
        'medical malpractice':    'MMP',
        'personal injury':        'PIN',
        'probate':                'PRB',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Louisville', 'Lexington', 'Bowling Green', 'Owensboro',
        'Covington', 'Richmond', 'Georgetown', 'Florence',
      ],
    });

    this.xhrPostUrl = 'https://kybar.org/cv5/cgi-bin/memberdll.dll/List';
    this.searchPageUrl = 'https://kybar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=LawyerLocator.htm';
  }

  /**
   * Not used directly -- search() is overridden for AJAX POST requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for AJAX POST requests`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for AJAX POST requests`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for AJAX POST requests`);
  }

  /**
   * HTTP POST with URL-encoded form data for the CV5 xhrPost endpoint.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.searchPageUrl,
          'Origin': 'https://kybar.org',
          'X-Requested-With': 'XMLHttpRequest',
        },
        timeout: 15000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
      req.setTimeout(15000);
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Build sanitized CV5 form data for the xhrPost endpoint.
   * CV5 systems expect specific field names with sanitized values.
   */
  buildFormData(city, practiceCode, page) {
    const offset = (page - 1) * this.pageSize;
    const data = {
      'City': city.replace(/[^a-zA-Z\s.-]/g, ''),
      'State': 'KY',
      'Status': 'Active',
      'PageSize': String(this.pageSize),
      'Offset': String(offset),
      'SortBy': 'LastName',
      'WRP': 'LawyerLocator.htm',
    };

    if (practiceCode) {
      data['PracticeArea'] = practiceCode;
    }

    return data;
  }

  /**
   * Parse the HTML fragment response from the CV5 AJAX endpoint.
   */
  parseAjaxResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // CV5 typically returns member rows in a table or list
    $('tr.memberRow, .member-row, tr[class*="member"], tr[class*="result"]').each((_, el) => {
      const $row = $(el);
      const attorney = this.extractFromTableRow($, $row);
      if (attorney) attorneys.push(attorney);
    });

    // Fallback: div-based card layout
    if (attorneys.length === 0) {
      $('div.member-card, .member-item, .lawyer-result, .result-item, .cv-member').each((_, el) => {
        const $card = $(el);
        const attorney = this.extractFromCard($, $card);
        if (attorney) attorneys.push(attorney);
      });
    }

    // Fallback: generic table parsing
    if (attorneys.length === 0) {
      $('table').each((_, table) => {
        const rows = $(table).find('tr');
        rows.each((i, row) => {
          if (i === 0) return; // skip header
          const cells = $(row).find('td');
          if (cells.length < 2) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|member|attorney|last)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $(cells[0]).find('a').attr('href') || '';
          const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
            city: cells.length > 2 ? $(cells[2]).text().trim() : '',
            state: 'KY',
            phone: cells.length > 3 ? $(cells[3]).text().trim() : '',
            email: '',
            website: '',
            bar_number: barNumber,
            bar_status: 'Active',
            profile_url: profileLink
              ? new URL(profileLink, 'https://kybar.org').href
              : '',
          });
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract attorney data from a table row.
   */
  extractFromTableRow($, $row) {
    const cells = $row.find('td');
    if (cells.length < 2) return null;

    const nameEl = $row.find('a').first();
    const fullName = nameEl.text().trim() || $(cells[0]).text().trim();
    if (!fullName) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.attr('href') || '';
    const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

    // Extract phone - look for tel: links or phone patterns
    let phone = '';
    const telLink = $row.find('a[href^="tel:"]');
    if (telLink.length) {
      phone = telLink.attr('href').replace('tel:', '');
    } else {
      const rowText = $row.text();
      const phoneMatch = rowText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
      if (phoneMatch) phone = phoneMatch[1];
    }

    // Extract email
    let email = '';
    const mailLink = $row.find('a[href^="mailto:"]');
    if (mailLink.length) {
      email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
    }

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
      city: cells.length > 2 ? $(cells[2]).text().trim() : '',
      state: 'KY',
      phone: phone,
      email: email,
      website: '',
      bar_number: barNumber,
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://kybar.org').href
        : '',
    };
  }

  /**
   * Extract attorney data from a card/div element.
   */
  extractFromCard($, $card) {
    const nameEl = $card.find('a, .member-name, .name, .lawyer-name').first();
    const fullName = nameEl.text().trim();
    if (!fullName) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.attr('href') || '';
    const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

    let phone = '';
    const telLink = $card.find('a[href^="tel:"]');
    if (telLink.length) {
      phone = telLink.attr('href').replace('tel:', '');
    } else {
      phone = $card.find('.phone, .member-phone').text().trim();
    }

    let email = '';
    const mailLink = $card.find('a[href^="mailto:"]');
    if (mailLink.length) {
      email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
    }

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: $card.find('.firm, .company, .member-firm').text().trim(),
      city: $card.find('.city, .location, .member-city').text().trim(),
      state: 'KY',
      phone: phone,
      email: email,
      website: '',
      bar_number: barNumber,
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://kybar.org').href
        : '',
    };
  }

  /**
   * Extract total result count from the AJAX response body.
   */
  extractCountFromResponse(body) {
    const $ = cheerio.load(body);
    const text = $.text();

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|members?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?)\s*(?:found|returned)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for Kentucky Bar AJAX POST requests via cvweb.xhrPost.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

    const cities = this.getCities(options);

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

        const formData = this.buildFormData(city, practiceCode, page);
        log.info(`Page ${page} — POST ${this.xhrPostUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.xhrPostUrl, formData, rateLimiter);
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

        if (page === 1) {
          totalResults = this.extractCountFromResponse(response.body);
          if (totalResults > 0) {
            const totalPages = Math.ceil(totalResults / this.pageSize);
            log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
          }
        }

        const attorneys = this.parseAjaxResponse(response.body);

        if (attorneys.length === 0) {
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
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

        if (page === 1 && totalResults === 0) {
          totalResults = attorneys.length;
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

module.exports = new KentuckyScraper();
