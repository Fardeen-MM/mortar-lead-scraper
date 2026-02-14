/**
 * Utah State Bar Scraper
 *
 * Source: https://services.utahbar.org/Member-Directory
 * Method: DotNetNuke (DNN) with auto-search AJAX pattern
 *
 * The Utah Bar Member Directory runs on DotNetNuke (DNN) CMS.
 * The search interface uses an AJAX auto-search pattern with a 2-character
 * minimum input requirement. The DNN module makes XHR requests to a
 * web service endpoint, returning HTML fragments or JSON data that are
 * injected into the page. This scraper discovers and queries the DNN AJAX endpoint.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class UtahScraper extends BaseScraper {
  constructor() {
    super({
      name: 'utah',
      stateCode: 'UT',
      baseUrl: 'https://services.utahbar.org/Member-Directory',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'appellate':              'Appellate Practice',
        'banking':                'Banking & Finance',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'collections':            'Collections',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'education':              'Education Law',
        'elder':                  'Elder Law',
        'employment':             'Employment Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'general practice':       'General Practice',
        'government':             'Government',
        'health':                 'Health Care Law',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'international':          'International Law',
        'labor':                  'Labor Relations',
        'military':               'Military Law',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Property',
        'securities':             'Securities',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Salt Lake City', 'West Valley City', 'Provo', 'West Jordan',
        'Orem', 'Sandy', 'Ogden', 'St. George',
      ],
    });

    // DNN AJAX service endpoints
    this.serviceBase = 'https://services.utahbar.org';
    this.dnnServiceUrl = 'https://services.utahbar.org/DesktopModules/MemberDirectory/API/MemberSearch/Search';
    this.moduleId = null; // Will be discovered from the page
    this.tabId = null;    // Will be discovered from the page
  }

  /**
   * Not used directly -- search() is overridden for DNN AJAX pattern.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for DNN AJAX`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for DNN AJAX`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for DNN AJAX`);
  }

  /**
   * HTTP POST for DNN AJAX service requests.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/json') {
    return new Promise((resolve, reject) => {
      const postData = typeof data === 'string' ? data : JSON.stringify(data);

      const parsed = new URL(url);
      const headers = {
        'Content-Type': contentType,
        'Content-Length': Buffer.byteLength(postData),
        'User-Agent': rateLimiter.getUserAgent(),
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': this.baseUrl,
        'Origin': this.serviceBase,
        'X-Requested-With': 'XMLHttpRequest',
      };

      // Add DNN-specific headers if module/tab IDs are discovered
      if (this.moduleId) headers['ModuleId'] = String(this.moduleId);
      if (this.tabId) headers['TabId'] = String(this.tabId);

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: headers,
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
      req.write(postData);
      req.end();
    });
  }

  /**
   * Discover the DNN module configuration from the directory page.
   * Extracts moduleId, tabId, and potential service URLs from the page HTML.
   */
  async discoverDnnConfig(rateLimiter) {
    log.info(`Discovering DNN module configuration for ${this.name}...`);

    try {
      const response = await this.httpGet(this.baseUrl, rateLimiter);
      if (response.statusCode !== 200) {
        log.warn(`Could not load directory page: ${response.statusCode}`);
        return;
      }

      const $ = cheerio.load(response.body);
      const html = response.body;

      // Extract moduleId from DNN page
      const moduleMatch = html.match(/(?:moduleId|ModuleId|module_id)\s*[=:]\s*['"]?(\d+)/i);
      if (moduleMatch) {
        this.moduleId = parseInt(moduleMatch[1], 10);
        log.info(`Discovered DNN moduleId: ${this.moduleId}`);
      }

      // Extract tabId
      const tabMatch = html.match(/(?:tabId|TabId|tab_id)\s*[=:]\s*['"]?(\d+)/i);
      if (tabMatch) {
        this.tabId = parseInt(tabMatch[1], 10);
        log.info(`Discovered DNN tabId: ${this.tabId}`);
      }

      // Look for alternative service URLs in scripts
      const scripts = $('script').map((_, el) => $(el).html()).get().join('\n');

      const serviceMatch = scripts.match(/(?:serviceUrl|apiUrl|serviceBase)\s*[=:]\s*['"](\/[^'"]+)['"]/i);
      if (serviceMatch) {
        this.dnnServiceUrl = `${this.serviceBase}${serviceMatch[1]}`;
        log.info(`Discovered DNN service URL: ${this.dnnServiceUrl}`);
      }

      // Look for module wrapper with data attributes
      const moduleWrapper = $('[data-moduleid], [id*="dnn_ctr"]').first();
      if (moduleWrapper.length && !this.moduleId) {
        const modId = moduleWrapper.attr('data-moduleid') || '';
        const idAttr = moduleWrapper.attr('id') || '';
        const idMatch = idAttr.match(/dnn_ctr(\d+)/);
        this.moduleId = parseInt(modId || (idMatch ? idMatch[1] : '0'), 10) || null;
        if (this.moduleId) log.info(`Discovered DNN moduleId from wrapper: ${this.moduleId}`);
      }

      // Look for form action or AJAX URL patterns
      const formAction = $('form[action*="Member"], form[action*="Search"]').attr('action');
      if (formAction) {
        log.info(`Found form action: ${formAction}`);
      }

    } catch (err) {
      log.warn(`DNN config discovery failed: ${err.message}`);
    }
  }

  /**
   * Build search request data for the DNN AJAX endpoint.
   * DNN services typically expect JSON with specific field names.
   */
  buildSearchData(city, practiceCode, page) {
    return {
      SearchText: city.substring(0, 2), // 2-char minimum for auto-search
      City: city,
      State: 'UT',
      Status: 'Active',
      PracticeArea: practiceCode || '',
      PageIndex: page - 1,
      PageSize: this.pageSize,
      SortBy: 'LastName',
      SortDirection: 'ASC',
    };
  }

  /**
   * Build URL-encoded form data for fallback POST requests.
   */
  buildFormData(city, practiceCode, page) {
    const data = {
      'searchText': city,
      'city': city,
      'state': 'UT',
      'status': 'Active',
      'pageIndex': String(page - 1),
      'pageSize': String(this.pageSize),
    };

    if (practiceCode) {
      data['practiceArea'] = practiceCode;
    }

    return data;
  }

  /**
   * Parse JSON API response from the DNN service.
   */
  parseJsonResponse(body) {
    const attorneys = [];

    let data;
    try {
      data = JSON.parse(body);
    } catch {
      return attorneys;
    }

    const records = Array.isArray(data)
      ? data
      : (data.Results || data.results || data.Members || data.members ||
         data.Data || data.data || data.Records || data.Items || []);

    for (const rec of records) {
      const fullName = rec.FullName || rec.DisplayName ||
        `${rec.FirstName || rec.firstName || ''} ${rec.LastName || rec.lastName || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.FirstName || rec.firstName || firstName,
        last_name: rec.LastName || rec.lastName || lastName,
        full_name: fullName,
        firm_name: (rec.Company || rec.Firm || rec.FirmName || rec.Organization || '').trim(),
        city: (rec.City || rec.city || '').trim(),
        state: (rec.State || rec.state || 'UT').trim(),
        phone: (rec.Phone || rec.phone || rec.WorkPhone || rec.PhoneNumber || '').trim(),
        email: (rec.Email || rec.email || '').trim(),
        website: (rec.Website || rec.website || rec.WebsiteUrl || '').trim(),
        bar_number: String(rec.BarNumber || rec.bar_number || rec.MemberNumber || rec.BarId || ''),
        bar_status: (rec.Status || rec.status || rec.MemberStatus || 'Active').trim(),
        admission_date: (rec.AdmissionDate || rec.admissionDate || rec.DateAdmitted || '').trim(),
        profile_url: rec.ProfileUrl || rec.ProfileURL || rec.DetailUrl ||
          (rec.MemberId ? `${this.serviceBase}/Member-Directory/MemberId/${rec.MemberId}` : ''),
      });
    }

    return attorneys;
  }

  /**
   * Parse HTML response for attorneys (fallback for non-JSON responses).
   */
  parseHtmlResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Look for member result elements
    $('table tbody tr, .member-result, .search-result, .directory-entry, .result-item').each((_, el) => {
      const $el = $(el);
      if ($el.find('th').length > 0) return;
      const cells = $el.find('td');

      let fullName, profileLink, firmName, city, phone, email;

      if (cells.length >= 3) {
        // Table row
        fullName = $(cells[0]).text().trim();
        profileLink = $(cells[0]).find('a').attr('href') || '';
        firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
        city = cells.length > 2 ? $(cells[2]).text().trim() : '';
        phone = cells.length > 3 ? $(cells[3]).text().trim() : '';
        email = '';
      } else {
        // Card/div
        const nameEl = $el.find('a, .name, .member-name, h3, h4, strong').first();
        fullName = nameEl.text().trim();
        profileLink = nameEl.is('a') ? nameEl.attr('href') : ($el.find('a').first().attr('href') || '');
        firmName = $el.find('.firm, .company').text().trim();
        city = $el.find('.city, .location').text().trim();
        phone = $el.find('.phone').text().trim();
        email = '';
      }

      if (!fullName || fullName.length < 3) return;
      if (/^(name|member|attorney|first|last|search|find)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);

      const telLink = $el.find('a[href^="tel:"]');
      if (telLink.length) phone = telLink.attr('href').replace('tel:', '');

      const mailLink = $el.find('a[href^="mailto:"]');
      if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

      const barMatch = (profileLink || '').match(/(?:Id|Num|Bar)\/(\d+)/i);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName || '',
        city: city || '',
        state: 'UT',
        phone: (phone || '').replace(/[^\d()-\s+.]/g, ''),
        email: email || '',
        website: '',
        bar_number: barMatch ? barMatch[1] : '',
        bar_status: 'Active',
        profile_url: profileLink
          ? new URL(profileLink, this.serviceBase).href
          : '',
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from JSON or HTML response.
   */
  extractCountFromResponse(body) {
    try {
      const data = JSON.parse(body);
      return data.TotalCount || data.totalCount || data.Total || data.total ||
             data.TotalRows || data.totalRows || data.RecordCount || data.Count || 0;
    } catch {
      // Not JSON
    }

    const $ = cheerio.load(body);
    const text = $.text();

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|members?|records?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?)\s*(?:found|returned)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for Utah Bar's DNN AJAX pattern.
   * Step 1: Discover DNN module configuration from the page
   * Step 2: Try JSON API, fall back to form POST, then HTML scraping
   * Step 3: Handle pagination through pageIndex/pageSize parameters
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

    // Step 1: Discover DNN configuration
    await rateLimiter.wait();
    await this.discoverDnnConfig(rateLimiter);

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let useJsonApi = true;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        let response;

        try {
          await rateLimiter.wait();

          if (useJsonApi) {
            // Try DNN JSON API first
            const searchData = this.buildSearchData(city, practiceCode, page);
            log.info(`Page ${page} — POST ${this.dnnServiceUrl} [City=${city}]`);
            response = await this.httpPost(this.dnnServiceUrl, searchData, rateLimiter, 'application/json');

            // If JSON API returns non-200, fall back to form POST
            if (response.statusCode !== 200) {
              log.info(`DNN JSON API returned ${response.statusCode} — falling back to form POST`);
              useJsonApi = false;
              const formData = this.buildFormData(city, practiceCode, page);
              log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);
              response = await this.httpPost(this.baseUrl, formData, rateLimiter, 'application/x-www-form-urlencoded');
            }
          } else {
            const formData = this.buildFormData(city, practiceCode, page);
            log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);
            response = await this.httpPost(this.baseUrl, formData, rateLimiter, 'application/x-www-form-urlencoded');
          }
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
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        // Try JSON parsing first, then HTML
        let attorneys = this.parseJsonResponse(response.body);
        if (attorneys.length === 0) {
          attorneys = this.parseHtmlResponse(response.body);
        }

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

module.exports = new UtahScraper();
