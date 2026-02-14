/**
 * Prince Edward Island Law Society Scraper
 *
 * Source: https://lawsocietypei.ca/find-a-lawyer
 * Method: PHP JSON API at bin/find-a-lawyer.api.php — POST with FormData
 * ~200 lawyers total
 *
 * The API accepts POST with FormData fields: search[name], search[firm], search[city].
 * Returns JSON arrays with fields: id, fullname, lspei_membershiptype,
 * lspei_companyname, address2_city, address2_stateorprovince,
 * lspei_businessphonenumber, emailaddress1.
 *
 * Overrides search() to query the JSON API directly (no HTML parsing needed).
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class PEIScraper extends BaseScraper {
  constructor() {
    super({
      name: 'pei',
      stateCode: 'CA-PE',
      baseUrl: 'https://lawsocietypei.ca/',
      pageSize: 100,
      practiceAreaCodes: {
        'family':                'family',
        'family law':            'family',
        'criminal':              'criminal',
        'criminal defense':      'criminal',
        'real estate':           'real-estate',
        'corporate/commercial':  'corporate-commercial',
        'corporate':             'corporate-commercial',
        'commercial':            'corporate-commercial',
        'personal injury':       'personal-injury',
        'employment':            'employment',
        'labour':                'employment',
        'immigration':           'immigration',
        'estate planning/wills': 'wills-estates',
        'estate planning':       'wills-estates',
        'wills':                 'wills-estates',
        'intellectual property': 'intellectual-property',
        'civil litigation':      'civil-litigation',
        'litigation':            'civil-litigation',
        'tax':                   'tax',
        'administrative':        'administrative',
        'environmental':         'environmental',
      },
      defaultCities: [
        'Charlottetown', 'Summerside',
      ],
    });

    this.apiUrl = 'https://lawsocietypei.ca/bin/find-a-lawyer.api.php';
  }

  /**
   * HTTP POST with multipart/form-data (URL-encoded fallback) for the PEI API.
   */
  httpPostForm(url, formFields, rateLimiter) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);

      // Build multipart/form-data body
      const boundary = '----FormBoundary' + Math.random().toString(36).substring(2);
      let body = '';
      for (const [key, value] of Object.entries(formFields)) {
        body += `--${boundary}\r\n`;
        body += `Content-Disposition: form-data; name="${key}"\r\n\r\n`;
        body += `${value}\r\n`;
      }
      body += `--${boundary}--\r\n`;

      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': Buffer.byteLength(body),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json,*/*',
          'Origin': 'https://lawsocietypei.ca',
          'Referer': 'https://lawsocietypei.ca/find-a-lawyer',
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        let responseBody = '';
        res.on('data', c => responseBody += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body: responseBody }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(body);
      req.end();
    });
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Not used -- search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used -- search() is overridden for JSON API`);
  }

  /**
   * Async generator that yields attorney records from the PEI JSON API.
   * The API accepts POST with FormData: search[name], search[firm], search[city].
   * Returns JSON arrays with known fields.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} -- searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching PEI lawyers in ${city}`);

      // POST to the JSON API with FormData fields
      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpPostForm(this.apiUrl, {
          'search[name]': '',
          'search[firm]': '',
          'search[city]': city,
        }, rateLimiter);
      } catch (err) {
        log.error(`Request failed: ${err.message}`);
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} -- skipping ${city}`);
        continue;
      }

      rateLimiter.resetBackoff();

      let records;
      try {
        records = JSON.parse(response.body);
      } catch (err) {
        log.error(`Failed to parse JSON response: ${err.message}`);
        continue;
      }

      if (!Array.isArray(records)) {
        records = records.data || records.results || records.lawyers || records.members || [];
      }

      log.success(`Fetched ${records.length} PEI lawyers for ${city}`);

      let cityCount = 0;
      for (const rec of records) {
        // Map the known API fields to our standard attorney object
        const fullName = (rec.fullname || '').trim();
        if (!fullName) continue;

        const { firstName, lastName } = this.splitName(fullName);

        const attorney = {
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: (rec.lspei_companyname || '').trim(),
          city: (rec.address2_city || city).trim(),
          state: 'CA-PE',
          phone: (rec.lspei_businessphonenumber || '').trim(),
          email: (rec.emailaddress1 || '').trim(),
          website: '',
          bar_number: (rec.id || '').toString().trim(),
          bar_status: (rec.lspei_membershiptype || 'Active').trim(),
          profile_url: '',
        };

        // Apply min year filter
        if (options.minYear && rec.admission_date) {
          const year = parseInt((rec.admission_date.toString().match(/\d{4}/) || ['0'])[0], 10);
          if (year > 0 && year < options.minYear) continue;
        }

        cityCount++;
        yield this.transformResult(attorney, practiceArea);

        // Respect maxPages (treat each batch of pageSize as a "page")
        if (options.maxPages && cityCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max results limit for ${city}`);
          break;
        }
      }

      if (cityCount === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      } else {
        log.success(`Found ${cityCount} lawyers in ${city}`);
      }
    }
  }
}

module.exports = new PEIScraper();
