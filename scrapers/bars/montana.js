/**
 * Montana State Bar Association Scraper
 *
 * Source: https://www.montanabar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=membersearch.htm
 * Method: HTML form POST, response uses tilde (~) delimiters
 *
 * The Montana Bar uses a custom CGI-based member search with utilities.dll.
 * The search form POSTs to the same endpoint and returns HTML with results
 * that may use tilde-delimited data in certain response fields.
 *
 * Flow:
 * 1. GET the search form page
 * 2. POST form data with Name, City, Practice Area filters
 * 3. Parse HTML results (tilde-delimited fields in some response formats)
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MontanaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'montana',
      stateCode: 'MT',
      baseUrl: 'https://www.montanabar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=membersearch.htm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business/Corporate',
        'civil litigation':      'Civil Litigation',
        'commercial':            'Commercial Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Law',
        'elder':                 'Elder Law',
        'employment':            'Employment/Labor',
        'labor':                 'Employment/Labor',
        'environmental':         'Environmental/Natural Resources',
        'estate planning':       'Estate Planning/Probate',
        'estate':                'Estate Planning/Probate',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'insurance':             'Insurance',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate/Property',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'water law':             'Water Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Billings', 'Missoula', 'Great Falls', 'Bozeman',
        'Helena', 'Butte', 'Kalispell', 'Whitefish',
      ],
    });

    this.searchPostUrl = 'https://www.montanabar.org/cv5/cgi-bin/utilities.dll/openpage?WRP=membersearchresults.htm';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for MT Bar CGI form`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for MT Bar CGI form`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for MT Bar CGI form`);
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
      const protocol = parsed.protocol === 'https:' ? https : http;

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
          'Referer': this.baseUrl,
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      };

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
   * Parse tilde-delimited response data.
   * Montana Bar sometimes returns data in format: field1~field2~field3
   */
  _parseTildeDelimited(text) {
    if (!text || !text.includes('~')) return null;
    const parts = text.split('~').map(s => s.trim());
    return parts;
  }

  /**
   * Parse attorneys from search results HTML.
   * Handles both table-based and tilde-delimited response formats.
   */
  _parseAttorneys(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Try table-based results first
    $('table tr').each((_, row) => {
      const $row = $(row);
      if ($row.find('th').length > 0) return;

      const cells = $row.find('td');
      if (cells.length < 2) return;

      // Montana Bar typically shows: Name (linked), City, Phone, Practice Area(s)
      const nameCell = $(cells[0]);
      const nameLink = nameCell.find('a');
      const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
      const profileUrl = nameLink.attr('href') || '';

      if (!fullName || /^(name|search|member)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);

      // Parse remaining cells
      let city = '';
      let phone = '';
      let firmName = '';
      let email = '';

      if (cells.length >= 2) {
        const secondCell = $(cells[1]).text().trim();
        // Check if second cell is a city or firm
        if (/^[A-Z][a-z]/.test(secondCell) && secondCell.length < 40) {
          city = secondCell;
        } else {
          firmName = secondCell;
        }
      }
      if (cells.length >= 3) {
        const thirdCell = $(cells[2]).text().trim();
        if (/[\d()-]/.test(thirdCell) && thirdCell.length <= 20) {
          phone = thirdCell.replace(/[^\d()-.\s+]/g, '');
        } else if (!city) {
          city = thirdCell;
        }
      }
      if (cells.length >= 4) {
        const fourthCell = $(cells[3]).text().trim();
        if (/[\d()-]/.test(fourthCell) && fourthCell.length <= 20 && !phone) {
          phone = fourthCell.replace(/[^\d()-.\s+]/g, '');
        }
      }

      // Check for email links
      const emailLink = $row.find('a[href^="mailto:"]');
      if (emailLink.length) {
        email = emailLink.attr('href').replace('mailto:', '').trim();
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'MT',
        phone: phone,
        email: email,
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://www.montanabar.org${profileUrl}` : ''),
      });
    });

    // Fallback: div/list-based results
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .result-item, .member-listing').each((_, el) => {
        const $el = $(el);
        const nameEl = $el.find('a, h3, h4, .name, .member-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileUrl = nameEl.attr('href') || '';
        const firmName = $el.find('.firm, .firm-name, .company').text().trim();
        const city = $el.find('.city, .location').text().trim();
        const phone = ($el.find('.phone, .telephone').text().trim() || '').replace(/[^\d()-.\s+]/g, '');
        const email = $el.find('a[href^="mailto:"]').attr('href')?.replace('mailto:', '') || '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmName,
          city: city,
          state: 'MT',
          phone: phone,
          email: email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://www.montanabar.org${profileUrl}` : ''),
        });
      });
    }

    // Fallback: tilde-delimited data embedded in page
    if (attorneys.length === 0) {
      const bodyText = body;
      const tildeLines = bodyText.split('\n').filter(line => (line.match(/~/g) || []).length >= 3);

      for (const line of tildeLines) {
        const parts = line.split('~').map(s => s.replace(/<[^>]*>/g, '').trim());
        if (parts.length < 4) continue;

        // Expected format: Name~Firm~City~Phone or similar
        const fullName = parts[0];
        if (!fullName || /^(name|header|field)/i.test(fullName)) continue;

        const { firstName, lastName } = this.splitName(fullName);

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: parts[1] || '',
          city: parts[2] || '',
          state: 'MT',
          phone: (parts[3] || '').replace(/[^\d()-.\s+]/g, ''),
          email: parts[4] || '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: '',
        });
      }
    }

    return attorneys;
  }

  /**
   * Extract total result count from response.
   */
  _extractResultCountFromHtml($) {
    const text = $('body').text();
    const matchOf = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?|lawyers?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total[:\s]+([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Async generator that yields attorney records from the MT Bar directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for MT — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Build POST form data for the CGI search
      const formData = {
        'City': city,
        'State': 'MT',
      };

      if (practiceCode) {
        formData['PracticeArea'] = practiceCode;
      }

      // Some CGI forms use different field names
      formData['CITY'] = city;
      formData['STATE'] = 'MT';
      formData['Submit'] = 'Search';

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        if (page > 1) {
          formData['Page'] = String(page);
          formData['Start'] = String((page - 1) * this.pageSize + 1);
        }

        const targetUrl = page === 1 ? this.searchPostUrl : this.searchPostUrl;
        log.info(`Page ${page} — POST ${targetUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(targetUrl, formData, rateLimiter);
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
          yield { _captcha: true, city };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this._extractResultCountFromHtml($);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        const attorneys = this._parseAttorneys(response.body);

        if (attorneys.length === 0) {
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
          }
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

        // Check for next page
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === 'next page' || text === '>';
        }).length > 0;

        const totalPages = totalResults > 0 ? Math.ceil(totalResults / this.pageSize) : 0;
        if (!hasNext && (totalPages === 0 || page >= totalPages)) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new MontanaScraper();
