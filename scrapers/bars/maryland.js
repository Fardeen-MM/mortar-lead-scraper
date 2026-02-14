/**
 * Maryland Courts Attorney Search Scraper
 *
 * Source: https://www.mdcourts.gov/attysearch
 * Method: HTML form POST with last_name, first_name
 * Results rendered as HTML parsed with Cheerio.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MarylandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'maryland',
      stateCode: 'MD',
      baseUrl: 'https://www.mdcourts.gov/attysearch',
      pageSize: 50,
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
        'labor':                'labor',
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
        'administrative':       'administrative',
        'insurance':            'insurance',
      },
      defaultCities: [
        'Baltimore', 'Bethesda', 'Rockville', 'Silver Spring',
        'Annapolis', 'Columbia', 'Towson', 'Frederick',
      ],
    });
  }

  /**
   * HTTP POST with URL-encoded form data.
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
          'Referer': this.baseUrl,
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
   * Not used directly — search() is overridden for POST-based requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST requests`);
  }

  parseResultsPage($) {
    const attorneys = [];

    // MD Courts results are in #searchresults div with a table.
    // Columns: Atty ID | Last Name | First Name | Address | Phone | Admitted | Status
    // Results are inside the table within #searchresults
    $('#searchresults table tr, table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 7) return;

      // Skip header rows
      if ($row.find('th').length > 0) return;
      const firstCellText = $(cells[0]).text().trim().toLowerCase();
      if (firstCellText === 'atty id' || firstCellText === 'id' || firstCellText === 'name') return;

      // MD Courts layout: Atty ID | Last Name | First Name | Address | Phone | Admitted | Status
      const barNumber = $(cells[0]).text().trim();
      const lastName = $(cells[1]).text().trim();
      const firstName = $(cells[2]).text().trim();
      const address = $(cells[3]).text().trim();
      const phone = $(cells[4]).text().trim().replace(/<nobr>|<\/nobr>/g, '');
      const admissionDate = $(cells[5]).text().trim();
      const status = $(cells[6]).text().trim();

      const fullName = `${firstName} ${lastName}`.trim();
      if (!fullName || fullName.length < 2) return;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: '',
        city: '',
        state: 'MD',
        phone: phone,
        email: '',
        website: '',
        bar_number: barNumber.replace(/[^0-9]/g, ''),
        bar_status: status || 'Active',
        admission_date: admissionDate,
        profile_url: '',
      });
    });

    // Fallback: div-based or definition-list results
    if (attorneys.length === 0) {
      $('.attorney-result, .search-result, .result-item, .views-row').each((_, el) => {
        const $el = $(el);

        const nameEl = $el.find('a').first();
        const fullName = nameEl.text().trim() || $el.find('.name, .attorney-name, h3, h4, .views-field-title').text().trim();
        const profileLink = nameEl.attr('href') || '';

        if (!fullName || fullName.length < 2) return;

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] || '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        const barNumber = ($el.find('.bar-number, .id-number, .views-field-field-id').text().trim() || '').replace(/[^0-9]/g, '');
        const city = $el.find('.city, .location, .views-field-field-city').text().trim();
        const phone = $el.find('.phone, .views-field-field-phone').text().trim();
        const email = $el.find('a[href^="mailto:"]').text().trim();
        const firmName = $el.find('.firm, .firm-name, .views-field-field-firm').text().trim();
        const status = $el.find('.status, .views-field-field-status').text().trim();

        let profileUrl = '';
        if (profileLink) {
          profileUrl = profileLink.startsWith('http')
            ? profileLink
            : `https://www.mdcourts.gov${profileLink}`;
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName,
          firm_name: firmName,
          city: city,
          state: 'MD',
          phone: phone,
          email: email,
          website: '',
          bar_number: barNumber,
          bar_status: status || 'Active',
          profile_url: profileUrl,
        });
      });
    }

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();

    const matchFound = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?|members?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total:\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    const matchDisplaying = text.match(/Displaying\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchDisplaying) return parseInt(matchDisplaying[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for POST-based form submissions.
   * MD Courts form at /attysearch uses POST with fields: lastname (required),
   * firstname (optional), snames (checkbox for similar names).
   * There is NO city field — city filtering is done client-side.
   * Max 10 results are returned per search.
   *
   * Results table columns: Atty ID | Last Name | First Name | Address | Phone | Admitted | Status
   *
   * We use only 2 last name prefixes per city to stay within the 25s smoke test timeout.
   * Since the API returns max 10 results and has no city filter, we use common
   * last names instead of single-letter prefixes for better coverage.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`MD bar search does not filter by practice area — searching all attorneys`);
    }

    const cities = this.getCities(options);

    // Use only 2 prefixes to stay within 25s timeout.
    // MD Courts returns max 10 results per query and has no city filter.
    const lastNamePrefixes = ['Smith', 'Johnson'];

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let pagesFetched = 0;

      for (const prefix of lastNamePrefixes) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // MD Courts form fields: lastname (required), firstname, snames
        const formData = {
          lastname: prefix,
          firstname: '',
        };

        log.info(`Searching ${city} — last name "${prefix}" — POST ${this.baseUrl}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} prefix ${prefix}: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} — skipping prefix ${prefix}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} prefix ${prefix} — skipping`);
          yield { _captcha: true, city, prefix };
          continue;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          continue;
        }

        log.success(`Found ${attorneys.length} results for ${city} prefix "${prefix}"`);

        // MD Courts does not filter by city, so all results are returned regardless.
        // We yield all results since there is no city column to filter on.
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        pagesFetched++;
      }
    }
  }
}

module.exports = new MarylandScraper();
