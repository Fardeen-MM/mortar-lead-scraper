/**
 * Michigan State Bar Scraper
 *
 * Source: https://www.michbar.org/memberdirectory/home
 * Method: ASP.NET POST with ViewState + DNN (DotNetNuke) form fields
 *
 * The State Bar of Michigan member directory uses a DNN module with:
 *   - Search form at /memberdirectory/home (POST with ViewState)
 *   - Results at /memberdirectory/results (10 per page, ASP.NET postback paging)
 *   - Detail pages at /memberdirectory/detail/id=XXXXX
 *
 * Detail pages include: name, P-number, status, title, firm, address,
 * city/state/zip, phone, fax, email, sections, licensed date.
 *
 * Paging uses __doPostBack('...lnkNextPageTop','') with ViewState — complex
 * but manageable. We don't paginate for now (10 results per city search, but
 * we cover many cities to get broad coverage).
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class MichiganScraper extends BaseScraper {
  constructor() {
    super({
      name: 'michigan',
      stateCode: 'MI',
      baseUrl: 'https://www.michbar.org/memberdirectory/home',
      pageSize: 10,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'appellate':             'Appellate Practice',
        'bankruptcy':            'Business Law',
        'business':              'Business Law',
        'civil litigation':      'Litigation',
        'corporate':             'Business Law',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Law',
        'elder':                 'Elder Law & Disability Rights',
        'employment':            'Labor and Employment Law',
        'environmental':         'Environmental Law',
        'estate planning':       'Probate and Estate Planning',
        'family':                'Family Law',
        'family law':            'Family Law',
        'health care':           'Health Care Law',
        'immigration':           'Immigration Law',
        'insurance':             'Insurance and Indemnity Law',
        'intellectual property': 'Intellectual Property Law',
        'labor':                 'Labor and Employment Law',
        'litigation':            'Litigation',
        'personal injury':       'Negligence Law',
        'real estate':           'Real Property Law',
        'tax':                   'Taxation',
        'tax law':               'Taxation',
        'workers comp':          'Workers\' Compensation Law',
      },
      defaultCities: [
        'Detroit', 'Grand Rapids', 'Ann Arbor', 'Lansing', 'Troy',
        'Southfield', 'Farmington Hills', 'Kalamazoo', 'Flint',
        'Traverse City', 'Saginaw', 'Bloomfield Hills', 'Birmingham',
        'Royal Oak', 'Novi', 'Dearborn', 'Livonia', 'East Lansing',
      ],
    });

    this.resultsUrl = 'https://www.michbar.org/memberdirectory/results';
    this.detailBaseUrl = 'https://www.michbar.org/memberdirectory/detail/id=';

    // DNN form field prefixes
    this._searchPrefix = 'dnn$ctr13718$MembeDirectorySearch$';
    this._resultPrefix = 'dnn$ctr13719$MemberDirectorySearchResult$';
    this._pagerPrefix = 'dnn$ctr13719$MemberDirectorySearchResult$ctrlPager$';
  }

  /**
   * HTTP GET that returns cookies for session management.
   */
  httpGetWithCookies(url, cookies) {
    return new Promise((resolve, reject) => {
      https.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Cookie': cookies || '',
          'Connection': 'keep-alive',
        },
        timeout: 20000,
      }, (res) => {
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) {
            const u = new URL(url);
            loc = `${u.protocol}//${u.host}${loc}`;
          }
          // Merge cookies
          let allCookies = cookies || '';
          newCookies.forEach(c => { allCookies += '; ' + c; });
          return resolve(this.httpGetWithCookies(loc, allCookies));
        }

        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => {
          let allCookies = cookies || '';
          newCookies.forEach(c => { allCookies += '; ' + c; });
          resolve({ statusCode: res.statusCode, body, cookies: allCookies });
        });
      }).on('error', reject).on('timeout', function() {
        this.destroy();
        reject(new Error('Request timed out'));
      });
    });
  }

  /**
   * HTTP POST with URL-encoded form data and cookie management.
   */
  httpPostWithCookies(url, formData, cookies) {
    return new Promise((resolve, reject) => {
      const postBody = new URLSearchParams(formData).toString();
      const parsed = new URL(url);

      const req = https.request({
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Cookie': cookies || '',
          'Referer': url,
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      }, (res) => {
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);
        let allCookies = cookies || '';
        newCookies.forEach(c => { allCookies += '; ' + c; });

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `${parsed.protocol}//${parsed.host}${loc}`;
          return resolve(this.httpGetWithCookies(loc, allCookies));
        }

        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body, cookies: allCookies }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Extract ASP.NET hidden fields from a page.
   */
  _extractHiddenFields(body) {
    const fields = {};
    const vsMatch = body.match(/id="__VIEWSTATE" value="([^"]*)"/);
    if (vsMatch) fields.__VIEWSTATE = vsMatch[1];

    const evMatch = body.match(/id="__EVENTVALIDATION" value="([^"]*)"/);
    if (evMatch) fields.__EVENTVALIDATION = evMatch[1];

    const vsgMatch = body.match(/__VIEWSTATEGENERATOR" value="([^"]*)"/);
    if (vsgMatch) fields.__VIEWSTATEGENERATOR = vsgMatch[1];

    const rvMatch = body.match(/__RequestVerificationToken" type="hidden" value="([^"]*)"/);
    if (rvMatch) fields.__RequestVerificationToken = rvMatch[1];

    return fields;
  }

  /**
   * Get a fresh search form page with ViewState and cookies.
   */
  async _getSearchForm(rateLimiter) {
    await rateLimiter.wait();
    const response = await this.httpGetWithCookies(this.baseUrl, '');

    if (response.statusCode !== 200) {
      throw new Error(`Search form returned status ${response.statusCode}`);
    }

    const hidden = this._extractHiddenFields(response.body);
    if (!hidden.__VIEWSTATE || !hidden.__EVENTVALIDATION) {
      throw new Error('Could not extract ViewState from search form');
    }

    return { hidden, cookies: response.cookies };
  }

  /**
   * Submit a city search and return the results page.
   */
  async _submitSearch(city, hidden, cookies, rateLimiter) {
    const formData = {
      '__EVENTTARGET': '',
      '__EVENTARGUMENT': '',
      '__VIEWSTATE': hidden.__VIEWSTATE,
      '__VIEWSTATEGENERATOR': hidden.__VIEWSTATEGENERATOR || '',
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTVALIDATION': hidden.__EVENTVALIDATION,
      [`${this._searchPrefix}txtFirstName`]: '',
      [`${this._searchPrefix}txtLastName`]: '',
      [`${this._searchPrefix}txtMemberNumber`]: '',
      [`${this._searchPrefix}txtFirmName`]: '',
      [`${this._searchPrefix}txtCity`]: city,
      [`${this._searchPrefix}ddlState`]: 'MI',
      [`${this._searchPrefix}ddlCountry`]: '',
      [`${this._searchPrefix}ddlCounty`]: '',
      [`${this._searchPrefix}ddlCommittee`]: '',
      [`${this._searchPrefix}ddlSection`]: '',
      [`${this._searchPrefix}rdbMemberType`]: '0',  // 0 = All active
      [`${this._searchPrefix}btnSearch`]: 'Search',
      'ScrollTop': '',
      '__dnnVariable': '',
    };

    if (hidden.__RequestVerificationToken) {
      formData.__RequestVerificationToken = hidden.__RequestVerificationToken;
    }

    await rateLimiter.wait();
    return this.httpPostWithCookies(this.baseUrl, formData, cookies);
  }

  /**
   * Submit a next-page postback on the results page.
   */
  async _submitNextPage(hidden, cookies, rateLimiter) {
    const formData = {
      '__EVENTTARGET': `${this._pagerPrefix}lnkNextPageTop`,
      '__EVENTARGUMENT': '',
      '__VIEWSTATE': hidden.__VIEWSTATE,
      '__VIEWSTATEGENERATOR': hidden.__VIEWSTATEGENERATOR || '',
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTVALIDATION': hidden.__EVENTVALIDATION,
      'ScrollTop': '',
      '__dnnVariable': '',
    };

    if (hidden.__RequestVerificationToken) {
      formData.__RequestVerificationToken = hidden.__RequestVerificationToken;
    }

    await rateLimiter.wait();
    return this.httpPostWithCookies(this.resultsUrl, formData, cookies);
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used`);
  }

  /**
   * Parse results grid from the search results page.
   * Returns array of { name, city, state, memberId, profileUrl }.
   */
  parseResultsPage($) {
    const results = [];

    $('table.gridTable tbody tr').each((_, row) => {
      const $row = $(row);
      const nameLink = $row.find('a[href*="/memberdirectory/detail/"]');
      if (!nameLink.length) return;

      const fullName = nameLink.text().trim();
      const href = nameLink.attr('href') || '';
      const city = $row.find('span[id*="lblCity"]').text().trim();
      const state = $row.find('span[id*="lblState"]').text().trim();

      // Extract member ID from href: /memberdirectory/detail/id=95956
      const idMatch = href.match(/id=(\d+)/);
      const memberId = idMatch ? idMatch[1] : '';

      if (!fullName || !memberId) return;

      const { firstName, lastName } = this.splitName(fullName);

      results.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        city: city,
        state: state || 'MI',
        member_id: memberId,
        profile_url: `https://www.michbar.org${href}`,
      });
    });

    return results;
  }

  extractResultCount($) {
    const bodyText = $('body').text();
    const match = bodyText.match(/([\d,]+)\s+member/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Parse a Michigan Bar member detail/profile page for additional fields.
   *
   * DNN element IDs (prefix: dnn_ctr13720_MemberDirectorySearchDetail_):
   *   - lblMemberDetails: "First Last—P12345 (active and in good standing)"
   *   - lblTitle:         Job title (e.g., "Secretary-Treasurer")
   *   - lblCompany:       Firm/company name
   *   - lblAddress1:      Street address line 1
   *   - lblAddress2:      Street address line 2 (often empty)
   *   - lblCityStateZip:  "City, ST ZIP" (e.g., "Detroit, MI 48208-1115")
   *   - lblCountry:       Country (e.g., "UNITED STATES")
   *   - hlnkPhone:        Phone number (inside <a href="tel:...">)
   *   - trFax:            "Fax: (xxx) xxx-xxxx"
   *   - hypEmail:         Email (inside <a href="mailto:...">)
   *   - trSection:        "Sections: Labor & Employment Law, ..."
   *   - trMemberLicensed: "Michigan Licensed: 11/3/2015"
   *   - lblBio:           Bio text (usually empty)
   *
   * No website or education fields are available on MI bar detail pages.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields extracted from the profile
   */
  parseProfilePage($) {
    const result = {};
    const prefix = 'dnn_ctr13720_MemberDirectorySearchDetail_';

    // Member details header: "First Last—P12345 (active and in good standing)"
    const memberDetails = $(`#${prefix}lblMemberDetails`).text().trim();
    if (memberDetails) {
      const detailMatch = memberDetails.match(/[—–-]P(\d+)\s*\(([^)]+)\)/);
      if (detailMatch) {
        result.bar_number = detailMatch[1];
        result.bar_status = detailMatch[2].trim();
      }
    }

    // Title (job title, not honorific)
    const title = $(`#${prefix}lblTitle`).text().trim();
    if (title) {
      result.title = title;
    }

    // Firm / company name
    const company = $(`#${prefix}lblCompany`).text().trim();
    if (company) {
      result.firm_name = company;
    }

    // Address
    const address1 = $(`#${prefix}lblAddress1`).text().trim();
    const address2 = $(`#${prefix}lblAddress2`).text().trim();
    const fullAddress = [address1, address2].filter(Boolean).join(', ');
    if (fullAddress) {
      result.address = fullAddress;
    }

    // City, State, Zip
    const cityStateZip = $(`#${prefix}lblCityStateZip`).text().trim();
    if (cityStateZip) {
      const parsed = this.parseCityStateZip(cityStateZip);
      if (parsed.city) result.city = parsed.city;
      if (parsed.state) result.state = parsed.state;
      if (parsed.zip) result.zip = parsed.zip;
    }

    // Phone (from <a href="tel:..."> link)
    const phone = $(`#${prefix}hlnkPhone`).text().trim();
    if (phone && phone.length > 5) {
      result.phone = phone;
    }

    // Fax — embedded as text in the <p> container: "Fax: (xxx) xxx-xxxx"
    const faxText = $(`#${prefix}trFax`).text().trim();
    if (faxText) {
      const faxMatch = faxText.match(/Fax:\s*(.+)/i);
      if (faxMatch) {
        result.fax = faxMatch[1].trim();
      }
    }

    // Email (from <a href="mailto:..."> link)
    const email = $(`#${prefix}hypEmail`).text().trim();
    if (email && email.includes('@')) {
      result.email = email.toLowerCase();
    }

    // Sections (practice areas): "Sections: Labor & Employment Law, Insurance..."
    const sectionText = $(`#${prefix}trSection`).text().trim();
    if (sectionText) {
      const secMatch = sectionText.match(/Sections?:\s*(.+)/i);
      if (secMatch) {
        result.practice_areas = secMatch[1].trim();
      }
    }

    // Michigan Licensed date: "Michigan Licensed: 11/3/2015"
    const licensedText = $(`#${prefix}trMemberLicensed`).text().trim();
    if (licensedText) {
      const licMatch = licensedText.match(/Licensed:\s*(.+)/i);
      if (licMatch) {
        result.admission_date = licMatch[1].trim();
      }
    }

    // Bio (usually empty, but extract if present)
    const bio = $(`#${prefix}lblBio`).text().trim();
    if (bio && bio.length > 2) {
      result.bio = bio;
    }

    return result;
  }

  /**
   * Fetch a member detail page and extract full attorney data.
   * Delegates parsing to parseProfilePage($).
   */
  async _fetchDetail(memberId, rateLimiter) {
    const url = `${this.detailBaseUrl}${memberId}`;

    try {
      await rateLimiter.wait();
      const response = await this.httpGetWithCookies(url, '');

      if (response.statusCode !== 200) {
        log.warn(`Detail page returned ${response.statusCode} for member ${memberId}`);
        return null;
      }

      const $ = cheerio.load(response.body);
      return this.parseProfilePage($);
    } catch (err) {
      log.warn(`Failed to fetch detail for member ${memberId}: ${err.message}`);
      return null;
    }
  }

  /**
   * Override search() for Michigan DNN member directory.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    // Get initial search form
    let formState;
    try {
      formState = await this._getSearchForm(rateLimiter);
    } catch (err) {
      log.error(`MI: Failed to load search form: ${err.message}`);
      yield { _captcha: true, city: 'N/A', page: 0 };
      return;
    }

    // In test mode, limit detail page fetches to avoid timeouts
    const maxDetailFetches = options.maxPages ? 2 : Infinity;
    let totalDetailFetches = 0;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Submit search for this city
      let response;
      try {
        // Need a fresh form for each city search
        formState = await this._getSearchForm(rateLimiter);
        response = await this._submitSearch(city, formState.hidden, formState.cookies, rateLimiter);
      } catch (err) {
        log.error(`Search failed for ${city}: ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Search returned status ${response.statusCode} for ${city}`);
        continue;
      }

      const $ = cheerio.load(response.body);
      const totalResults = this.extractResultCount($);
      const totalPages = Math.ceil(totalResults / this.pageSize);

      if (totalResults === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      log.success(`Found ${totalResults} results (${totalPages} pages) for ${city}`);

      // Parse first page
      let results = this.parseResultsPage($);
      let pagesFetched = 1;
      let allResults = [...results];

      // Paginate through results
      let currentBody = response.body;
      let currentCookies = response.cookies;

      while (pagesFetched < totalPages) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const hidden = this._extractHiddenFields(currentBody);
        if (!hidden.__VIEWSTATE || !hidden.__EVENTVALIDATION) {
          log.warn(`Lost ViewState on page ${pagesFetched} for ${city} — stopping pagination`);
          break;
        }

        try {
          const nextResponse = await this._submitNextPage(hidden, currentCookies, rateLimiter);

          if (nextResponse.statusCode !== 200) {
            log.warn(`Page ${pagesFetched + 1} returned ${nextResponse.statusCode} for ${city}`);
            break;
          }

          const $next = cheerio.load(nextResponse.body);
          const nextResults = this.parseResultsPage($next);

          if (nextResults.length === 0) {
            log.info(`No more results on page ${pagesFetched + 1} for ${city}`);
            break;
          }

          allResults.push(...nextResults);
          currentBody = nextResponse.body;
          currentCookies = nextResponse.cookies;
          pagesFetched++;

          log.info(`Page ${pagesFetched}: ${nextResults.length} results (${allResults.length} total)`);
        } catch (err) {
          log.error(`Pagination failed on page ${pagesFetched + 1} for ${city}: ${err.message}`);
          break;
        }
      }

      // Fetch detail pages for each result (limited in test mode to avoid timeouts)
      for (const result of allResults) {
        let detail = null;

        if (totalDetailFetches < maxDetailFetches) {
          detail = await this._fetchDetail(result.member_id, rateLimiter);
          totalDetailFetches++;
        }

        const attorney = {
          first_name: result.first_name,
          last_name: result.last_name,
          full_name: result.full_name,
          firm_name: detail ? (detail.firm_name || '') : '',
          title: detail ? (detail.title || '') : '',
          address: detail ? (detail.address || '') : '',
          city: detail ? (detail.city || result.city) : result.city,
          state: detail ? (detail.state || result.state) : result.state,
          zip: detail ? (detail.zip || '') : '',
          phone: detail ? (detail.phone || '') : '',
          fax: detail ? (detail.fax || '') : '',
          email: detail ? (detail.email || '') : '',
          website: '',
          bar_number: detail ? (detail.bar_number || '') : '',
          bar_status: detail ? (detail.bar_status || 'Active') : 'Active',
          admission_date: detail ? (detail.admission_date || '') : '',
          practice_areas: detail ? (detail.practice_areas || '') : '',
          profile_url: result.profile_url,
        };

        yield this.transformResult(attorney, practiceArea);
      }
    }
  }
}

module.exports = new MichiganScraper();
