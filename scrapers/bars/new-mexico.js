/**
 * State Bar of New Mexico — CV5 (cvweb) Search API
 *
 * Source: https://www.sbnm.org/For-Public/I-Need-a-Lawyer/Online-Bar-Directory
 * Platform: Euclid Technology CV Web Templates v4 (embedded iframe)
 * Method: POST to utilities.dll/customList with QNAME=FINDALAWYER
 *
 * Search API:
 *   POST https://www.sbnm.org/cvweb/cgi-bin/utilities.dll/customList
 *   Form params: QNAME=FINDALAWYER, CITY=..., RANGE=start/pagesize
 *   Returns HTML table: Name (with customerCd), Status, Phone, County, Admission Date
 *
 * Profile API:
 *   GET customList?QNAME=FINDALAWYER&WBP=LawyerProfilex.htm&customercd={id}
 *   Returns: full name, organization, address, phone, fax, email, admission date
 *
 * Total records: ~8,600 active attorneys
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const SEARCH_URL = 'https://www.sbnm.org/cvweb/cgi-bin/utilities.dll/customList';

class NewMexicoScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-mexico',
      stateCode: 'NM',
      baseUrl: SEARCH_URL,
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':         'ADMIN',
        'appellate':              'APPELLATE',
        'bankruptcy':             'BANKRUPTCY',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'criminal':               'CRIMINAL',
        'criminal defense':       'CRIMINAL',
        'elder':                  'ELDER',
        'employment':             'EMPLOYMENT',
        'environmental':          'ENVIRONMENT',
        'estate planning':        'PROBATE',
        'family':                 'FAMILY',
        'family law':             'FAMILY',
        'immigration':            'IMMIGRATION',
        'indian law':             'INDIAN',
        'insurance':              'INSURANCE',
        'intellectual property':  'IP',
        'personal injury':        'PERSONALINJURY',
        'real estate':            'REALESTATE',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'water law':              'WATER',
        'workers comp':           'WORKERS',
      },
      defaultCities: [
        'Albuquerque', 'Santa Fe', 'Las Cruces', 'Rio Rancho',
        'Roswell', 'Farmington', 'Hobbs', 'Carlsbad',
      ],
    });

    this.profileBaseUrl = SEARCH_URL;
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
   * Override CAPTCHA detection — NM's CV5 pages include reCAPTCHA v3 script
   * that triggers the base class false positive. Only flag actual CAPTCHA
   * challenge forms, not the presence of the recaptcha library.
   */
  detectCaptcha(body) {
    const stripped = body.replace(/<script[^>]*recaptcha[^>]*>[\s\S]*?<\/script>/gi, '')
                        .replace(/<!--[\s\S]*?-->/g, '');
    return stripped.includes('captcha') || stripped.includes('CAPTCHA') ||
           stripped.includes('challenge-form');
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
          'Origin': 'https://www.sbnm.org',
          'Referer': 'https://www.sbnm.org/cvweb/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `https://www.sbnm.org${loc}`;
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
    return `${this.profileBaseUrl}?QNAME=FINDALAWYER&WHP=none&WBP=LawyerProfilex.htm&customercd=${customerCd}`;
  }

  /**
   * Parse search results HTML table.
   *
   * Table structure:
   *   <table id="myTable">
   *     <thead>: Name | Status | Phone | County | Admission Date
   *     <tbody>:
   *       <tr>
   *         <td id="lawyerName_{id}"><a onclick="getCVPageLink('lawyerProfile','{id}')">Name</a></td>
   *         <td>Active Attorney</td>
   *         <td>(505) 200-2331</td>
   *         <td>Bernalillo</td>
   *         <td>9/30/2002</td>
   *       </tr>
   */
  _parseSearchResults($) {
    const results = [];

    $('table#myTable tbody tr, table.tablesorter tbody tr').each((_, row) => {
      const cells = $(row).find('td');
      if (cells.length < 3) return;

      // Extract customer code from onclick
      const nameLink = $(cells[0]).find('a[onclick*="getCVPageLink"], a.profile-link').first();
      if (!nameLink.length) return;

      const onclick = nameLink.attr('onclick') || '';
      const idMatch = onclick.match(/getCVPageLink\s*\(\s*'lawyerProfile'\s*,\s*'(\d+)'\s*\)/);
      if (!idMatch) return;

      const customerCd = idMatch[1];

      // Get full name (remove any nickname span)
      const nameClone = nameLink.clone();
      nameClone.find('span').remove();
      const fullName = nameClone.text().trim();
      if (!fullName) return;

      const status = $(cells[1]).text().trim();
      const phone = $(cells[2]).text().trim();
      const county = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const admissionDate = cells.length > 4 ? $(cells[4]).text().trim() : '';

      results.push({ fullName, customerCd, status, phone, county, admissionDate });
    });

    return results;
  }

  /**
   * Extract total result count from heading.
   * Format: " - 83 Match(es)"
   */
  _getResultCount($) {
    const heading = $('h2, h3').text() || '';
    const match = heading.match(/([\d,]+)\s+Match/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse a NM profile page for additional attorney fields.
   *
   * Profile uses <p class="nomargin"> elements in order:
   *   Member Type, Organization, Address, City/State/Zip, Phone, Fax, Email, Admission Date
   */
  /**
   * Override enrichFromProfile to prevent waterfall from fetching profiles
   * post-scrape. NM handles profile fetching inline during search() via
   * _fetchProfile(). The waterfall's sequential profile fetch is too slow
   * for NM's server (~8s per request due to reCAPTCHA v3).
   */
  async enrichFromProfile() {
    return {};
  }

  _parseProfilePage($) {
    const result = {};

    // Full name from <h3> or heading
    const heading = $('h3').first().text().trim();
    if (heading) {
      result.full_name = heading;
      const { firstName, lastName } = this.splitName(heading);
      result.first_name = firstName;
      result.last_name = lastName;
    }

    // Try to extract labeled fields from text content
    const bodyText = $('body').text() || '';

    // Email from mailto link
    const emailLink = $('a[href^="mailto:"]').first();
    if (emailLink.length) {
      const email = emailLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
      if (email && email.includes('@')) result.email = email;
    }

    // Organization
    const orgMatch = bodyText.match(/(?:Organization|Firm|Company)\s*[:\-]\s*([^\n]+)/i);
    if (orgMatch) result.firm_name = orgMatch[1].trim();

    // Phone from profile (may be different from search result)
    const phoneMatch = bodyText.match(/(?:Phone|Telephone)\s*[:\-]\s*\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/i);
    if (phoneMatch) {
      const ph = phoneMatch[0].replace(/.*?[:\-]\s*/, '').trim();
      if (ph.length > 5) result.phone = ph;
    }

    // Fax
    const faxMatch = bodyText.match(/Fax\s*[:\-]\s*\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/i);
    if (faxMatch) {
      result.fax = faxMatch[0].replace(/.*?[:\-]\s*/, '').trim();
    }

    // Address from text patterns
    const addrMatch = bodyText.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
    if (addrMatch) {
      result.city = addrMatch[1].trim();
      result.state = addrMatch[2];
      result.zip = addrMatch[3];
    }

    // Also parse structured <p> elements (CV5 profile layout)
    $('p.nomargin, div.row p').each((_, el) => {
      const text = $(el).text().trim();
      if (!text) return;

      // City, ST ZIP pattern
      const cityStateZip = text.match(/^(.+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
      if (cityStateZip) {
        result.city = cityStateZip[1].trim();
        result.state = cityStateZip[2];
        result.zip = cityStateZip[3];
      }
    });

    // Website link
    $('a[href]').each((_, el) => {
      const href = ($(el).attr('href') || '').trim();
      if (href.startsWith('http') && !href.includes('mailto:') && !href.includes('sbnm.org') && !this.isExcludedDomain(href)) {
        if (!result.website) result.website = href;
      }
    });

    return result;
  }

  /**
   * Fetch a profile page and parse it.
   */
  async _fetchProfile(customerCd, rateLimiter) {
    const url = this._buildProfileUrl(customerCd);

    try {
      await rateLimiter.wait();
      const response = await this.httpGet(url, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`NM profile returned ${response.statusCode} for ${customerCd}`);
        return null;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA on NM profile for ${customerCd}`);
        return null;
      }

      const $ = cheerio.load(response.body);
      return this._parseProfilePage($);
    } catch (err) {
      log.warn(`Failed to fetch NM profile for ${customerCd}: ${err.message}`);
      return null;
    }
  }

  /**
   * Search the SBNM CV5 directory by city, paginate, and fetch profiles.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const ua = rateLimiter.getUserAgent();
    const cities = this.getCities(options);
    const practiceCode = this.resolvePracticeCode(practiceArea) || '';

    // Limit profile fetches in test mode to avoid timeouts (NM profiles are slow ~8s each)
    const maxDetailFetches = options.maxPages ? 3 : Infinity;
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
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const rangeStart = (page - 1) * this.pageSize + 1;
        const formData = {
          'QNAME': 'FINDALAWYER',
          'WBP': 'LawyerList.htm',
          'WHP': 'LawyerList_header.htm',
          'WMT': 'none',
          'WNR': 'none',
          'WEM': 'none',
          'RANGE': `${rangeStart}/${this.pageSize}`,
          'SORT': 'LASTNAME,FIRSTNAME',
          'SHOWSQL': 'N',
          'DISPLAYLAWYERPROFILE': 'N',
          'LISTDESCRIPTION': 'Find a Lawyer',
          'LASTNAME': '',
          'FIRSTNAME': '',
          'PREFNAME': '',
          'ORGNAME': '',
          'CITY': city,
          'STATECD': '',
          'SECTIONLIST': '',
          'PRACTICEAREALIST': practiceCode,
          'COUNTYLIST': '',
          'LANGUAGELIST': '',
          'CONTACTTYPE': '',
          'APPROVEDMENTOR': '',
        };

        log.info(`Page ${page} — RANGE=${rangeStart}/${this.pageSize} for ${city}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpPost(SEARCH_URL, formData, ua);
        } catch (err) {
          log.error(`NM search request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from NM Bar`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`NM Bar returned ${response.statusCode}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on NM page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

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
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;
        log.info(`Page ${page}: ${searchResults.length} results parsed`);

        for (const sr of searchResults) {
          const profileUrl = this._buildProfileUrl(sr.customerCd);

          // Fetch profile for email/firm details
          let detail = null;
          if (totalDetailFetches < maxDetailFetches) {
            detail = await this._fetchProfile(sr.customerCd, rateLimiter);
            totalDetailFetches++;
          }

          const { firstName, lastName } = this.splitName(sr.fullName);

          const attorney = {
            first_name: detail?.first_name || firstName,
            last_name: detail?.last_name || lastName,
            full_name: detail?.full_name || sr.fullName,
            firm_name: detail?.firm_name || '',
            city: detail?.city || city,
            state: detail?.state || 'NM',
            zip: detail?.zip || '',
            county: sr.county || '',
            phone: sr.phone || detail?.phone || '',
            email: detail?.email || '',
            website: detail?.website || '',
            fax: detail?.fax || '',
            bar_number: sr.customerCd,
            bar_status: sr.status || '',
            admission_date: sr.admissionDate || '',
            profile_url: profileUrl,
          };

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

module.exports = new NewMexicoScraper();
