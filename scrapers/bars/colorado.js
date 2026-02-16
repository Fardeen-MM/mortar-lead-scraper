/**
 * Colorado Attorney Registration Scraper
 *
 * Source: https://www.coloradolegalregulation.com/attorney-search/
 * Method: HTTP POST with LName/FName/RegNum fields -> HTML table results
 *
 * The Colorado Supreme Court Office of Attorney Regulation Counsel provides
 * a WordPress-based attorney search. Search is "contains" (not "starts with").
 * We iterate over 2-letter last name prefixes (Aa, Ab, ..., Zz) and filter
 * results to those whose last name actually starts with that prefix.
 *
 * Results are returned in a single HTML table with columns:
 *   Last Name | First Name | Middle Name | Status | Registration Number
 *
 * Profile pages at /attorney-search/attorney-information?Regnum=XXXXX provide:
 *   Name, Reg Number, Status, Firm Name, Admission Date
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ColoradoScraper extends BaseScraper {
  constructor() {
    super({
      name: 'colorado',
      stateCode: 'CO',
      baseUrl: 'https://www.coloradolegalregulation.com/attorney-search/',
      pageSize: 500,
      practiceAreaCodes: {},
      defaultCities: [
        'Denver', 'Colorado Springs', 'Aurora', 'Fort Collins',
        'Lakewood', 'Boulder', 'Thornton', 'Pueblo',
      ],
    });

    this.searchUrl = 'https://www.coloradolegalregulation.com/attorney-search/attorney-search-results/';
    this.profileBaseUrl = 'https://www.coloradolegalregulation.com/attorney-search/attorney-information';
  }

  /**
   * Generate 3-letter prefixes for systematic last name iteration.
   * We use common starting patterns to get smaller, more targeted batches.
   */
  _getSearchPrefixes() {
    // 3-letter prefixes give manageable result sizes (10-200 each)
    const letters = 'abcdefghijklmnopqrstuvwxyz';
    const consonants = 'bcdfghjklmnpqrstvwxyz';
    const vowels = 'aeiou';
    const prefixes = [];

    // Generate common 2-letter combos that produce reasonable result counts
    for (const c1 of letters) {
      for (const c2 of letters) {
        prefixes.push(c1 + c2);
      }
    }

    return prefixes;
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const postBody = new URLSearchParams(formData).toString();
      const parsed = new URL(url);

      const req = https.request({
        hostname: parsed.hostname,
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
        timeout: 30000,
      }, (res) => {
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

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for POST requests`);
  }

  /**
   * Parse attorney results from HTML table.
   * Table columns: Last Name | First Name | Middle Name | Status | Registration Number
   */
  parseResultsPage($) {
    const attorneys = [];

    $('table.table-striped tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 5) return;

      const lastNameCell = $(cells[0]);
      const lastName = lastNameCell.text().trim();
      const profileLink = lastNameCell.find('a').attr('href') || '';
      const firstName = $(cells[1]).text().trim();
      const middleName = $(cells[2]).text().trim();
      const statusRaw = $(cells[3]).text().trim();
      const regNumber = $(cells[4]).text().trim();

      if (!lastName || lastName.length < 2) return;

      // Parse status: "ACTV – Active" -> extract code and description
      const statusParts = statusRaw.split(/\s*[–—-]\s*/);
      const statusCode = (statusParts[0] || '').trim();
      const statusDesc = statusParts.length > 1
        ? statusParts.slice(1).join(' ').trim()
        : statusParts[0].trim();

      // Build profile URL
      let profileUrl = '';
      if (profileLink) {
        profileUrl = profileLink.startsWith('http')
          ? profileLink
          : `https://www.coloradolegalregulation.com${profileLink}`;
      } else if (regNumber) {
        profileUrl = `${this.profileBaseUrl}?Regnum=${regNumber}`;
      }

      // Title-case names (they come as ALL CAPS)
      const toTitleCase = (s) => (s || '').trim().replace(/\b\w+/g,
        w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());

      attorneys.push({
        first_name: toTitleCase(firstName),
        last_name: toTitleCase(lastName),
        full_name: toTitleCase(`${firstName} ${middleName ? middleName + ' ' : ''}${lastName}`).trim(),
        firm_name: '',
        city: '',
        state: 'CO',
        phone: '',
        email: '',
        website: '',
        bar_number: regNumber.replace(/[^0-9]/g, ''),
        bar_status: statusDesc || 'Unknown',
        _statusCode: statusCode,
        profile_url: profileUrl,
        _rawLastName: lastName.toUpperCase(),
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?)/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse a profile page for additional detail fields.
   */
  parseProfilePage($) {
    const result = {};

    // Extract from card-text elements
    $('p.card-text').each((_, el) => {
      const text = $(el).text().trim();

      const firmMatch = text.match(/Firm Name:\s*(.+)/i);
      if (firmMatch && firmMatch[1].trim()) {
        result.firm_name = firmMatch[1].trim();
      }

      const admMatch = text.match(/Admission Date:\s*(.+)/i);
      if (admMatch) {
        result.admission_date = admMatch[1].trim();
      }
    });

    // Business Address
    const bodyText = $('body').text();
    const addrMatch = bodyText.match(/Business Address:\s*(.+?)(?:\n|$)/i);
    if (addrMatch) {
      const addr = addrMatch[1].trim();
      if (addr.length > 2) {
        result.address = addr;
      }
    }

    return result;
  }

  /**
   * Override getCities to return last name prefixes instead of cities.
   * Colorado's search is name-based, not city-based.
   */
  getCities(options) {
    if (options.city) {
      // If user specifies a "city", treat it as a last name prefix
      return [options.city];
    }
    // Use common last names for default search batches
    const commonNames = [
      'Adams', 'Allen', 'Anderson', 'Baker', 'Brown', 'Campbell', 'Carter',
      'Clark', 'Cohen', 'Collins', 'Cooper', 'Davis', 'Edwards', 'Evans',
      'Fisher', 'Garcia', 'Gonzalez', 'Green', 'Hall', 'Harris', 'Henderson',
      'Hill', 'Howard', 'Jackson', 'James', 'Johnson', 'Jones', 'Kelly',
      'King', 'Lee', 'Lewis', 'Lopez', 'Martin', 'Martinez', 'Meyer',
      'Miller', 'Mitchell', 'Moore', 'Morgan', 'Morris', 'Murphy', 'Nelson',
      'Parker', 'Patterson', 'Peterson', 'Phillips', 'Roberts', 'Robinson',
      'Rodriguez', 'Rogers', 'Ross', 'Russell', 'Sanders', 'Scott', 'Smith',
      'Stewart', 'Sullivan', 'Taylor', 'Thomas', 'Thompson', 'Turner',
      'Walker', 'Ward', 'Washington', 'Watson', 'White', 'Williams',
      'Wilson', 'Wood', 'Wright', 'Young',
    ];
    return options.maxCities ? commonNames.slice(0, options.maxCities) : commonNames;
  }

  /**
   * Override search() — iterate by last name and yield active attorneys.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const searchNames = this.getCities(options);
    const seenRegNumbers = new Set();

    for (let si = 0; si < searchNames.length; si++) {
      const searchName = searchNames[si];
      yield { _cityProgress: { current: si + 1, total: searchNames.length } };
      log.scrape(`Searching: "${searchName}" attorneys in ${this.stateCode}`);

      if (options.maxPages && si >= options.maxPages) {
        log.info(`Reached max pages limit (${options.maxPages})`);
        break;
      }

      let response;
      try {
        await rateLimiter.wait();
        response = await this.httpPost(this.searchUrl, {
          LName: searchName,
          FName: '',
          RegNum: '',
        }, rateLimiter);
      } catch (err) {
        log.error(`Request failed for "${searchName}": ${err.message}`);
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (shouldRetry) {
          try {
            await rateLimiter.wait();
            response = await this.httpPost(this.searchUrl, {
              LName: searchName,
              FName: '',
              RegNum: '',
            }, rateLimiter);
          } catch (retryErr) {
            log.error(`Retry failed for "${searchName}": ${retryErr.message}`);
            continue;
          }
        } else {
          continue;
        }
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} for "${searchName}"`);
        continue;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA detected for "${searchName}"`);
        yield { _captcha: true, city: searchName, page: 0 };
        continue;
      }

      const $ = cheerio.load(response.body);
      const attorneys = this.parseResultsPage($);

      if (attorneys.length === 0) {
        log.info(`No results for "${searchName}"`);
        continue;
      }

      // Filter: active only, last name matches search, deduplicate by reg number
      let yieldCount = 0;
      for (const attorney of attorneys) {
        // Only active attorneys
        if (attorney._statusCode !== 'ACTV') continue;

        // Deduplicate by registration number
        if (seenRegNumbers.has(attorney.bar_number)) continue;
        seenRegNumbers.add(attorney.bar_number);

        // Only those whose last name actually matches/starts with search term
        const rawLast = attorney._rawLastName;
        const searchUpper = searchName.toUpperCase();
        if (!rawLast.startsWith(searchUpper) && rawLast !== searchUpper) continue;

        // Clean up internal fields
        delete attorney._statusCode;
        delete attorney._rawLastName;

        attorney.practice_area = practiceArea || '';
        yield this.transformResult(attorney, practiceArea);
        yieldCount++;
      }

      log.success(`Found ${attorneys.length} results for "${searchName}" (${yieldCount} yielded)`);
    }
  }
}

module.exports = new ColoradoScraper();
