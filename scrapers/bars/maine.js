/**
 * Maine Board of Overseers of the Bar Scraper
 *
 * Source: https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl
 * Method: HTTP POST to a CGI-Perl script with HTML form parameters
 *
 * The Maine attorney directory is a classic CGI-Perl script that accepts
 * form POST requests with last name, first name, and city parameters.
 * The response is a full HTML page with results rendered in a table.
 * No JavaScript/AJAX — straightforward form submission and HTML parsing.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MaineScraper extends BaseScraper {
  constructor() {
    super({
      name: 'maine',
      stateCode: 'ME',
      baseUrl: 'https://apps.web.maine.gov/cgi-bin/online/maine_bar/attorney_directory.pl',
      pageSize: 100,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BAN',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'corporate':              'COR',
        'criminal':               'CRI',
        'criminal defense':       'CRI',
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
        'maritime':               'MAR',
        'medical malpractice':    'MED',
        'personal injury':        'PIN',
        'probate':                'PRO',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Portland', 'Lewiston', 'Bangor', 'South Portland',
        'Auburn', 'Augusta', 'Biddeford', 'Scarborough',
      ],
    });
  }

  /**
   * Not used directly -- search() is overridden for POST-based CGI requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CGI POST`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CGI POST`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CGI POST`);
  }

  /**
   * HTTP POST to the CGI-Perl script with URL-encoded form data.
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
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.baseUrl,
          'Origin': 'https://apps.web.maine.gov',
          'Connection': 'keep-alive',
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
   * Build form data for the CGI-Perl attorney search.
   * The Perl script accepts: last_name, first_name, city, action
   */
  buildFormData(city, lastName) {
    return {
      'last_name': lastName || '',
      'first_name': '',
      'city': city,
      'action': 'Search',
    };
  }

  /**
   * Parse the HTML results page from the CGI-Perl script.
   * Results are rendered as an HTML table with attorney details.
   */
  parseHtmlResults(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Look for result tables
    $('table').each((_, table) => {
      const $table = $(table);
      const rows = $table.find('tr');

      rows.each((i, row) => {
        const $row = $(row);
        if ($row.find('th').length > 0) return; // skip header rows
        const cells = $row.find('td');
        if (cells.length < 3) return;

        // Typical CGI output: Name | City | Phone | Status | Bar#
        const nameCell = $(cells[0]);
        const fullName = nameCell.text().trim();
        if (!fullName || /^(name|attorney|member|last|search)/i.test(fullName)) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = nameCell.find('a').attr('href') || '';

        // Extract fields based on the number of columns
        let firmName = '';
        let city = '';
        let phone = '';
        let email = '';
        let barNumber = '';
        let barStatus = '';

        if (cells.length >= 6) {
          // Full layout: Name | Firm | City | Phone | Status | Bar#
          firmName = $(cells[1]).text().trim();
          city = $(cells[2]).text().trim();
          phone = $(cells[3]).text().trim();
          barStatus = $(cells[4]).text().trim();
          barNumber = $(cells[5]).text().trim();
        } else if (cells.length >= 5) {
          // Layout: Name | City | Phone | Status | Bar#
          city = $(cells[1]).text().trim();
          phone = $(cells[2]).text().trim();
          barStatus = $(cells[3]).text().trim();
          barNumber = $(cells[4]).text().trim();
        } else if (cells.length >= 4) {
          // Layout: Name | City | Phone | Status
          city = $(cells[1]).text().trim();
          phone = $(cells[2]).text().trim();
          barStatus = $(cells[3]).text().trim();
        } else {
          // Minimal layout: Name | City | Phone
          city = $(cells[1]).text().trim();
          phone = cells.length > 2 ? $(cells[2]).text().trim() : '';
        }

        // Try to extract email from mailto link in the row
        const mailLink = $row.find('a[href^="mailto:"]');
        if (mailLink.length) {
          email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
        }

        // Try to extract phone from tel: link
        const telLink = $row.find('a[href^="tel:"]');
        if (telLink.length) {
          phone = telLink.attr('href').replace('tel:', '');
        }

        // Clean up bar number
        barNumber = barNumber.replace(/[^\d]/g, '');

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmName,
          city: city,
          state: 'ME',
          phone: phone.replace(/[^\d()-\s+.]/g, ''),
          email: email,
          website: '',
          bar_number: barNumber,
          bar_status: barStatus || 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://apps.web.maine.gov').href
            : '',
        });
      });
    });

    // Fallback: look for definition list or pre-formatted text
    if (attorneys.length === 0) {
      $('dl, .attorney-list, .results').each((_, el) => {
        const $container = $(el);
        $container.find('dt, .attorney-entry, .result-item').each((_, entry) => {
          const $entry = $(entry);
          const fullName = $entry.find('a, strong, b').first().text().trim() || $entry.text().split('\n')[0].trim();
          if (!fullName) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $entry.find('a').attr('href') || '';

          // Get the corresponding dd element for details
          const $details = $entry.next('dd');
          const detailText = $details.length ? $details.text() : '';

          const phoneMatch = detailText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
          const cityMatch = detailText.match(/^([^,\n]+),?\s*ME/i);

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: '',
            city: cityMatch ? cityMatch[1].trim() : '',
            state: 'ME',
            phone: phoneMatch ? phoneMatch[1] : '',
            email: '',
            website: '',
            bar_number: '',
            bar_status: 'Active',
            profile_url: profileLink
              ? new URL(profileLink, 'https://apps.web.maine.gov').href
              : '',
          });
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from the response page.
   */
  extractCountFromHtml(body) {
    const $ = cheerio.load(body);
    const text = $.text();

    const matchFound = text.match(/([\d,]+)\s*(?:results?|records?|attorneys?|members?)\s*(?:found|returned|matched)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+([\d,]+)\s*(?:results?|records?|attorneys?)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Generate common last name prefixes to use as search terms.
   * Since the CGI script may require at least a partial name, we use
   * letter-based searches to enumerate attorneys in a city.
   */
  getLastNamePrefixes() {
    return [
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
      'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
      'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
  }

  /**
   * Override search() for Maine's CGI-Perl POST-based directory.
   * Since the CGI script may require name input, we iterate through
   * last name letter prefixes for comprehensive coverage.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (practiceCode && practiceArea) {
      log.warn(`Maine CGI directory does not support practice area filtering — searching all attorneys`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // First, try a city-only search (no last name filter)
      let pagesFetched = 0;

      if (options.maxPages && pagesFetched >= options.maxPages) {
        log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
        continue;
      }

      const formData = this.buildFormData(city, '');
      log.info(`POST ${this.baseUrl} [City=${city}]`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpPost(this.baseUrl, formData, rateLimiter);
      } catch (err) {
        log.error(`Request failed for ${city}: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (!shouldRetry) continue;
        // Retry once
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err2) {
          log.error(`Retry failed for ${city}: ${err2.message}`);
          continue;
        }
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (!shouldRetry) continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} for ${city} — skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA detected for ${city} — skipping`);
        yield { _captcha: true, city, page: 1 };
        continue;
      }

      let attorneys = this.parseHtmlResults(response.body);
      const totalCount = this.extractCountFromHtml(response.body);

      if (attorneys.length > 0) {
        // City-only search worked
        log.success(`Found ${totalCount > 0 ? totalCount.toLocaleString() : attorneys.length} results for ${city}`);

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }
        pagesFetched++;
        continue;
      }

      // If city-only returned nothing, try letter-by-letter
      log.info(`City-only search returned no results for ${city} — trying letter prefixes`);
      const prefixes = this.getLastNamePrefixes();
      const seenNames = new Set();

      for (const prefix of prefixes) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const letterFormData = this.buildFormData(city, prefix);
        log.info(`POST ${this.baseUrl} [City=${city}, LastName=${prefix}]`);

        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, letterFormData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city}/${prefix}: ${err.message}`);
          continue;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (!shouldRetry) break;
          continue;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for prefix ${prefix} — skipping`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city}/${prefix} — skipping remaining letters`);
          yield { _captcha: true, city, page: prefix };
          break;
        }

        attorneys = this.parseHtmlResults(response.body);
        pagesFetched++;

        if (attorneys.length === 0) continue;

        log.info(`Found ${attorneys.length} results for ${city} prefix "${prefix}"`);

        for (const attorney of attorneys) {
          // Dedup within same city across letter searches
          const dedupeKey = `${attorney.full_name}|${attorney.city}`;
          if (seenNames.has(dedupeKey)) continue;
          seenNames.add(dedupeKey);

          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }
      }

      if (seenNames.size > 0) {
        log.success(`Completed letter search for ${city} — ${seenNames.size} unique attorneys`);
      } else {
        log.info(`No results found for ${city}`);
      }
    }
  }
}

module.exports = new MaineScraper();
