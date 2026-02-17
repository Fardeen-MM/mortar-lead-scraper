/**
 * Kentucky Bar Association Scraper
 *
 * Source: https://kybar.org/page/FindaLawyer
 * Method: cvweb API — POST search + GET profile pages
 *
 * Search API:
 *   POST https://kybar.org/cv5/cgi-bin/utilities.dll/customlist
 *   Form params: QNAME=LAWYERLOCATORSEARCH, City=..., RANGE=start/pagesize
 *   Returns HTML with attorney names, customer codes, and "X matches" count
 *
 * Profile API:
 *   GET https://kybar.org/cv5/cgi-bin/utilities.dll/customlist?QNAME=LAWYERLOCATORINFO&CUSTOMERCD={id}&...
 *   Returns: full name, organization, address, phone, email, website,
 *            admission date, practice areas, member type, discipline history
 *
 * Profile pages include email, phone, firm, website, admission date, practice areas.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class KentuckyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'kentucky',
      stateCode: 'KY',
      baseUrl: 'https://kybar.org/cv5/cgi-bin/utilities.dll/customlist',
      pageSize: 25,
      practiceAreaCodes: {},
      defaultCities: [
        'Louisville', 'Lexington', 'Bowling Green', 'Covington',
        'Frankfort', 'Owensboro', 'Paducah', 'Ashland',
      ],
    });

    this.profileBaseUrl = 'https://kybar.org/cv5/cgi-bin/utilities.dll/customlist';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * HTTP POST to the cvweb search API.
   */
  _httpPost(url, formData, ua) {
    return new Promise((resolve, reject) => {
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
          'Origin': 'https://kybar.org',
          'Referer': 'https://kybar.org/page/FindaLawyer',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) {
            loc = `https://kybar.org${loc}`;
          }
          return resolve(this.httpGet(loc, { getUserAgent: () => ua, wait: async () => {} }));
        }

        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Build profile page URL for a customer code.
   */
  _buildProfileUrl(customerCd) {
    return `${this.profileBaseUrl}?QNAME=LAWYERLOCATORINFO&CUSTOMERCD=${customerCd}&WBP=LawyerLocatorInfo.htm&WHP=LawyerLocator_H.htm&WMT=none&WNR=none&WEM=none`;
  }

  /**
   * Parse search results HTML to extract attorney names and customer codes.
   *
   * Each result is a .flex-item div containing:
   *   - <a onclick="openLawyerInfo('CUSTOMERCD');">Full Name</a>
   *   - <address> with city, state, zip
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object[]} Array of { fullName, customerCd, city, zip }
   */
  _parseSearchResults($) {
    const results = [];

    $('div.flex-item').each((_, el) => {
      const $el = $(el);

      // Extract customer code from onclick="openLawyerInfo('XXXXX')"
      const nameLink = $el.find('a[onclick*="openLawyerInfo"]').first();
      if (!nameLink.length) return;

      const onclick = nameLink.attr('onclick') || '';
      const idMatch = onclick.match(/openLawyerInfo\('(\d+)'\)/);
      if (!idMatch) return;

      const customerCd = idMatch[1];
      const fullName = nameLink.text().trim();
      if (!fullName) return;

      // Extract address info
      const address = $el.find('address');
      let city = '';
      let zip = '';
      if (address.length) {
        const addressText = address.html() || '';
        const lines = addressText
          .replace(/<br\s*\/?>/gi, '\n')
          .replace(/<[^>]+>/g, '')
          .split('\n')
          .map(l => l.trim())
          .filter(l => l.length > 0);

        // Typical format: City \n Kentucky \n ZIP \n United States
        if (lines.length >= 1) city = lines[0];
        if (lines.length >= 3) zip = lines[2];
      }

      results.push({ fullName, customerCd, city, zip });
    });

    return results;
  }

  /**
   * Extract total result count from search results HTML.
   * Format: "Search Results - 5446 matches"
   */
  _getResultCount($) {
    const heading = $('h3').text() || '';
    const match = heading.match(/([\d,]+)\s+match/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse a Kentucky Bar profile page for attorney details.
   *
   * Profile page structure:
   *   <legend class="h2">Full Name <small>Member Type</small></legend>
   *   <div class="card">
   *     <h4>Official Address Information</h4>
   *     <p><strong>Organization</strong>:<span>Firm Name</span></p>
   *     <address>Street<br>City, ST ZIP<br>Country</address>
   *     <p><strong>Phone</strong>:<span>(...) ...-...</span></p>
   *     <p><strong>Email Address</strong>:<span><a id=emaildd>email</a></span></p>
   *     <p><strong>Professional Website</strong>:<span><a id=websitedd>url</a></span></p>
   *   </div>
   *   <div class="card">
   *     <h4>Other Information</h4>
   *     <p><strong>Date of Admission</strong>:<span>MM/DD/YYYY</span></p>
   *     <p><strong>Area(s) of Practice</strong>:<span>Area1, Area2</span></p>
   *     <p><strong>Public Discipline History</strong>:<span>...</span></p>
   *   </div>
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Extracted attorney fields
   */
  parseProfilePage($) {
    const result = {};

    // Full name from <legend class="h2">
    const legend = $('legend.h2').first();
    if (legend.length) {
      // Remove the <small> member type tag and get just the name
      const legendClone = legend.clone();
      legendClone.find('small').remove();
      const fullName = legendClone.text().trim();
      if (fullName) {
        result.full_name = fullName;
        const { firstName, lastName } = this.splitName(fullName);
        result.first_name = firstName;
        result.last_name = lastName;
      }
    }

    // Member type from <small> inside legend (e.g., "Regular 2")
    const memberType = $('legend.h2 small').text().trim();
    if (memberType) {
      result.bar_status = memberType;
    }

    // Parse all <p> elements with <strong> labels inside .card containers
    $('div.card p').each((_, el) => {
      const $p = $(el);
      const strong = $p.find('strong').first().text().trim().replace(/:$/, '');
      // Get the text after the strong element (in the <span>)
      const span = $p.find('span').first();
      const value = span.length ? span.text().trim() : '';

      if (!strong || !value) return;

      const label = strong.toLowerCase();

      if (label === 'organization') {
        result.firm_name = value;
      } else if (label === 'phone') {
        if (value.length > 5) result.phone = value;
      } else if (label === 'email address') {
        // Email may be inside an <a> tag
        const emailLink = span.find('a#emaildd');
        const email = emailLink.length ? emailLink.text().trim() : value;
        if (email && email.includes('@')) {
          result.email = email.toLowerCase();
        }
      } else if (label === 'professional website') {
        const websiteLink = span.find('a#websitedd');
        const website = websiteLink.length ? websiteLink.text().trim() : value;
        if (website && website.startsWith('http') && !this.isExcludedDomain(website)) {
          result.website = website;
        }
      } else if (label === 'date of admission') {
        result.admission_date = value;
      } else if (label.includes('area') && label.includes('practice')) {
        result.practice_areas = value.trim();
      }
    });

    // Parse address block
    const addressEl = $('div.card address').first();
    if (addressEl.length) {
      const addrHtml = addressEl.html() || '';
      const lines = addrHtml
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<[^>]+>/g, '')
        .split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 0);

      // Typical format:
      //   Line 1: "Street Address"
      //   Line 2: "City, ST ZIP"     (or sometimes split as "City, ST" + ZIP separately)
      //   Line 3: "Country"          (e.g., "United States")
      //
      // But sometimes:
      //   Line 1: "Street"
      //   Line 2: ""  (empty, already filtered)
      //   Line 3: "City, KY ZIP"
      //   Line 4: "United States"

      if (lines.length > 0) {
        // Find the line with "City, ST ZIP" pattern
        let addressLines = [];
        for (let i = 0; i < lines.length; i++) {
          const cityStateZip = lines[i].match(/^(.+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
          if (cityStateZip) {
            result.city = cityStateZip[1].trim();
            result.state = cityStateZip[2];
            result.zip = cityStateZip[3];
            addressLines = lines.slice(0, i);
            break;
          }
        }

        if (addressLines.length > 0) {
          result.address = addressLines.join(', ');
        }
      }
    }

    return result;
  }

  /**
   * Fetch a profile page and parse it for full attorney data.
   */
  async _fetchProfile(customerCd, rateLimiter) {
    const url = this._buildProfileUrl(customerCd);

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(url, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`KY profile page returned ${response.statusCode} for customer ${customerCd}`);
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on KY profile page for customer ${customerCd}`);
        return null;
      }

      const $ = cheerio.load(response.body);
      return this.parseProfilePage($);
    } catch (err) {
      log.warn(`Failed to fetch KY profile for customer ${customerCd}: ${err.message}`);
      return null;
    }
  }

  /**
   * Override search() for Kentucky Bar cvweb API.
   *
   * Flow:
   * 1. For each city, POST search to get result count and attorney list
   * 2. Paginate using RANGE parameter (start/pagesize)
   * 3. For each attorney, GET their profile page for full details
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const ua = rateLimiter.getUserAgent();
    const cities = this.getCities(options);

    // In test mode, limit detail page fetches to avoid timeouts
    const maxDetailFetches = options.maxPages ? 5 : Infinity;
    let totalDetailFetches = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit (test mode sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // RANGE parameter: start position / page size
        const rangeStart = (page - 1) * this.pageSize + 1;
        const formData = {
          'QNAME': 'LAWYERLOCATORSEARCH',
          'WBP': 'LawyerLocator_L.htm',
          'WHP': 'LawyerLocator_H.htm',
          'WMT': 'none',
          'WNR': 'none',
          'WEM': 'none',
          'RANGE': `${rangeStart}/${this.pageSize}`,
          'LastName': '',
          'FirstName': '',
          'City': city,
          'County': '',
          'PostalCode': '',
          'AreaOfPractice': '',
          'Language': '',
          'Sections': '',
          'Fellow': '',
        };

        log.info(`Page ${page} — RANGE=${rangeStart}/${this.pageSize} for ${city}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpPost(this.baseUrl, formData, ua);
        } catch (err) {
          log.error(`KY search request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting / blocking
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from KY Bar`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`KY Bar returned unexpected status ${response.statusCode}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on KY Bar page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = this._getResultCount($);
          if (totalResults === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const searchResults = this._parseSearchResults($);

        if (searchResults.length === 0) {
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
        log.info(`Page ${page}: ${searchResults.length} results parsed`);

        // Fetch profile page for each attorney
        for (const sr of searchResults) {
          const profileUrl = this._buildProfileUrl(sr.customerCd);
          let detail = null;

          if (totalDetailFetches < maxDetailFetches) {
            detail = await this._fetchProfile(sr.customerCd, rateLimiter);
            totalDetailFetches++;
          }

          // Split name from search results as fallback
          const { firstName, lastName } = this.splitName(sr.fullName);

          const attorney = {
            first_name: detail?.first_name || firstName,
            last_name: detail?.last_name || lastName,
            full_name: detail?.full_name || sr.fullName,
            firm_name: detail?.firm_name || '',
            address: detail?.address || '',
            city: detail?.city || sr.city || city,
            state: detail?.state || 'KY',
            zip: detail?.zip || sr.zip || '',
            phone: detail?.phone || '',
            email: detail?.email || '',
            website: detail?.website || '',
            bar_number: sr.customerCd,
            bar_status: detail?.bar_status || '',
            admission_date: detail?.admission_date || '',
            practice_areas: detail?.practice_areas || '',
            profile_url: profileUrl,
          };

          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }

          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
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
