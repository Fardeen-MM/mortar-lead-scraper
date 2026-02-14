/**
 * Mississippi Bar Association Scraper
 *
 * Source: https://courts.ms.gov/bar/barroll/brsearch.php
 * Method: HTTP POST (AJAX) returning pipe-delimited data
 * Search form: POST with last_name, first_name, city, zip
 * The response is not standard HTML — it returns pipe-delimited rows.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MississippiScraper extends BaseScraper {
  constructor() {
    super({
      name: 'mississippi',
      stateCode: 'MS',
      baseUrl: 'https://courts.ms.gov/bar/barroll/brsearch.php',
      pageSize: 100,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'employment',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
      },
      defaultCities: [
        'Jackson', 'Gulfport', 'Hattiesburg', 'Tupelo',
        'Meridian', 'Biloxi', 'Oxford', 'Southaven',
      ],
    });
  }

  /**
   * HTTP POST with URL-encoded form data.
   * Mirrors the pattern of BaseScraper.httpGet but uses POST method.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
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
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
          'Referer': 'https://courts.ms.gov/bar/barroll/brsearch.php',
          'X-Requested-With': 'XMLHttpRequest',
        },
        timeout: 15000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse pipe-delimited response data into attorney objects.
   * MS bar returns rows like: BarNumber|LastName|FirstName|City|State|Zip|Status|Phone|...
   * Each row is separated by newlines, fields by pipes.
   */
  parsePipeDelimitedData(body) {
    const attorneys = [];
    const lines = body.split('\n').filter(line => line.trim().length > 0);

    for (const line of lines) {
      const fields = line.split('|').map(f => f.trim());

      // Skip header rows or lines that don't have enough fields
      if (fields.length < 5) continue;
      if (/^bar\s*number$/i.test(fields[0])) continue;

      const barNumber = fields[0] || '';
      const lastName = fields[1] || '';
      const firstName = fields[2] || '';
      const city = fields[3] || '';
      const state = fields[4] || 'MS';
      const zip = fields[5] || '';
      const status = fields[6] || '';
      const phone = fields[7] || '';
      const firmName = fields[8] || '';
      const email = fields[9] || '';

      if (!lastName && !firstName) continue;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: `${firstName} ${lastName}`.trim(),
        firm_name: firmName,
        city: city,
        state: state || 'MS',
        phone: phone,
        email: email,
        website: '',
        bar_number: barNumber,
        bar_status: status,
        profile_url: '',
      });
    }

    return attorneys;
  }

  /**
   * Not used directly — search() is overridden for POST-based requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST requests`);
  }

  /**
   * Parse HTML results if the server returns HTML instead of pipe-delimited data.
   * Fallback parser for HTML table results.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('table tr').each((i, el) => {
      if (i === 0) return; // skip header row
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 4) return;

      const barNumber = $(cells[0]).text().trim();
      const fullName = $(cells[1]).text().trim();
      const city = $(cells[2]).text().trim();
      const status = $(cells[3]).text().trim();
      const phone = cells.length > 4 ? $(cells[4]).text().trim() : '';
      const email = cells.length > 5 ? $(cells[5]).text().trim() : '';
      const profileLink = $(cells[1]).find('a').attr('href') || '';

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: '',
        city: city,
        state: 'MS',
        phone: phone,
        email: email,
        website: '',
        bar_number: barNumber,
        bar_status: status,
        profile_url: profileLink ? `https://courts.ms.gov${profileLink}` : '',
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?)\s+found/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() to handle POST-based AJAX requests with pipe-delimited responses.
   * MS bar uses POST with city parameter and returns all results at once.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`MS bar search does not filter by practice area — searching all attorneys`);
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

        // Build POST form data — MS bar uses city-based search
        const formData = {
          last_name: '',
          first_name: '',
          city: city,
          zip: '',
          search: 'Search',
        };

        if (page > 1) {
          formData.page = String(page);
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
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

        // Determine response type — pipe-delimited or HTML
        let attorneys;
        if (response.body.includes('|') && !response.body.includes('<html')) {
          attorneys = this.parsePipeDelimitedData(response.body);
        } else {
          const $ = cheerio.load(response.body);

          if (page === 1) {
            totalResults = this.extractResultCount($);
          }

          attorneys = this.parseResultsPage($);
        }

        if (page === 1 && totalResults === 0) {
          totalResults = attorneys.length;
        }

        if (page === 1) {
          if (attorneys.length === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          log.success(`Found ${totalResults > 0 ? totalResults.toLocaleString() : attorneys.length} results for ${city}`);
        }

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
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // MS bar typically returns all results at once for a city search
        // If we got fewer results than page size, we are done
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

module.exports = new MississippiScraper();
