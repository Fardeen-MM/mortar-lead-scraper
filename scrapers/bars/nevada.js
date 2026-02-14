/**
 * State Bar of Nevada Scraper
 *
 * Source: https://nvbar.org/for-the-public/find-a-lawyer/
 * Method: WordPress site with referral form — discover actual database endpoint
 *
 * The Nevada Bar's "Find a Lawyer" page is hosted on WordPress (nvbar.org).
 * The public-facing page may use a referral form, embedded iframe, or
 * WordPress REST API / AJAX handler (admin-ajax.php) to query the member
 * database. This scraper discovers the actual search endpoint by examining
 * the page source for AJAX handlers, shortcode configurations, or
 * REST API routes, then queries it for attorney data.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NevadaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nevada',
      stateCode: 'NV',
      baseUrl: 'https://nvbar.org/for-the-public/find-a-lawyer/',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'appellate':              'Appellate',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'collections':            'Collections',
        'construction':           'Construction Law',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment Law',
        'entertainment':          'Entertainment & Gaming Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'gaming':                 'Gaming Law',
        'general practice':       'General Practice',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor Law',
        'mining':                 'Mining & Natural Resources',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'water law':              'Water Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Las Vegas', 'Henderson', 'Reno', 'North Las Vegas',
        'Sparks', 'Carson City', 'Elko', 'Mesquite',
      ],
    });

    // Potential WordPress/backend endpoints
    this.wpAjaxUrl = 'https://nvbar.org/wp-admin/admin-ajax.php';
    this.wpRestBase = 'https://nvbar.org/wp-json';
    this.discoveredEndpoint = null;
    this.ajaxAction = null;
    this.ajaxNonce = null;
  }

  /**
   * Not used directly -- search() is overridden for WordPress endpoint discovery.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for WP endpoint discovery`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for WP endpoint discovery`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for WP endpoint discovery`);
  }

  /**
   * HTTP POST for WordPress AJAX and REST API requests.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
    return new Promise((resolve, reject) => {
      const postData = typeof data === 'string' ? data : (
        contentType.includes('json') ? JSON.stringify(data) : new URLSearchParams(data).toString()
      );

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json, text/html, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.baseUrl,
          'Origin': 'https://nvbar.org',
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
   * Discover the actual search endpoint from the WordPress page.
   * Examines the page for:
   * - admin-ajax.php action names and nonces
   * - WP REST API endpoints
   * - Embedded iframes pointing to member databases
   * - JavaScript configuration objects
   */
  async discoverEndpoint(rateLimiter) {
    if (this.discoveredEndpoint) return this.discoveredEndpoint;

    log.info(`Discovering search endpoint for ${this.name}...`);

    try {
      const response = await this.httpGet(this.baseUrl, rateLimiter);
      if (response.statusCode !== 200) {
        log.warn(`Could not load find-a-lawyer page: ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);
      const html = response.body;

      // Check for iframe pointing to external member directory
      const iframe = $('iframe[src*="member"], iframe[src*="directory"], iframe[src*="lawyer"], iframe[src*="search"]');
      if (iframe.length) {
        const src = iframe.attr('src');
        log.info(`Found iframe endpoint: ${src}`);
        this.discoveredEndpoint = src;
        return src;
      }

      // Check for form action
      const form = $('form[action*="member"], form[action*="lawyer"], form[action*="search"], form[action*="directory"]');
      if (form.length) {
        const action = form.attr('action');
        const fullUrl = action.startsWith('http') ? action : new URL(action, 'https://nvbar.org').href;
        log.info(`Found form endpoint: ${fullUrl}`);
        this.discoveredEndpoint = fullUrl;
        return fullUrl;
      }

      // Extract WP AJAX action and nonce from script tags
      const scripts = $('script').map((_, el) => $(el).html()).get().join('\n');

      // Look for admin-ajax.php action
      const actionMatch = scripts.match(/['"]action['"]\s*:\s*['"](\w+_search|\w+_directory|\w+_lawyer|\w+_member)['"]/i);
      if (actionMatch) {
        this.ajaxAction = actionMatch[1];
        log.info(`Found WP AJAX action: ${this.ajaxAction}`);
        this.discoveredEndpoint = this.wpAjaxUrl;
      }

      // Look for nonce
      const nonceMatch = scripts.match(/['"](?:nonce|_wpnonce|security)['"]\s*:\s*['"]([a-f0-9]+)['"]/i);
      if (nonceMatch) {
        this.ajaxNonce = nonceMatch[1];
        log.info(`Found WP AJAX nonce: ${this.ajaxNonce}`);
      }

      // Look for localized script variables with AJAX URL
      const ajaxUrlMatch = scripts.match(/(?:ajaxurl|ajax_url|ajaxUrl)\s*[=:]\s*['"]([^'"]+)['"]/i);
      if (ajaxUrlMatch && !this.discoveredEndpoint) {
        this.discoveredEndpoint = ajaxUrlMatch[1];
        log.info(`Found AJAX URL: ${this.discoveredEndpoint}`);
      }

      // Look for WP REST API endpoints
      const restMatch = scripts.match(/['"](?:rest_url|apiUrl|restBase)['"]\s*:\s*['"]([^'"]+)['"]/i);
      if (restMatch && !this.discoveredEndpoint) {
        this.discoveredEndpoint = restMatch[1];
        log.info(`Found REST API base: ${this.discoveredEndpoint}`);
      }

      // Look for data attributes on search elements
      const searchEl = $('[data-action], [data-ajax-url], [data-search-url]').first();
      if (searchEl.length && !this.discoveredEndpoint) {
        this.ajaxAction = searchEl.attr('data-action') || this.ajaxAction;
        const dataUrl = searchEl.attr('data-ajax-url') || searchEl.attr('data-search-url');
        if (dataUrl) {
          this.discoveredEndpoint = dataUrl;
          log.info(`Found data attribute endpoint: ${dataUrl}`);
        }
      }

      if (this.discoveredEndpoint) return this.discoveredEndpoint;

    } catch (err) {
      log.warn(`Endpoint discovery failed: ${err.message}`);
    }

    // Default to WP admin-ajax.php with a generic action
    this.discoveredEndpoint = this.wpAjaxUrl;
    this.ajaxAction = this.ajaxAction || 'member_search';
    log.info(`Using default WP AJAX endpoint: ${this.discoveredEndpoint} with action: ${this.ajaxAction}`);
    return this.discoveredEndpoint;
  }

  /**
   * Build WordPress AJAX form data for admin-ajax.php requests.
   */
  buildWpAjaxData(city, practiceCode, page) {
    const data = {
      'action': this.ajaxAction || 'member_search',
      'city': city,
      'state': 'NV',
      'status': 'Active',
      'page': String(page),
      'per_page': String(this.pageSize),
    };

    if (practiceCode) {
      data['practice_area'] = practiceCode;
    }

    if (this.ajaxNonce) {
      data['nonce'] = this.ajaxNonce;
      data['_wpnonce'] = this.ajaxNonce;
      data['security'] = this.ajaxNonce;
    }

    return data;
  }

  /**
   * Build a standard search form data payload.
   */
  buildFormData(city, practiceCode, page) {
    const data = {
      'city': city,
      'state': 'NV',
      'status': 'Active',
      'page': String(page),
      'pageSize': String(this.pageSize),
    };

    if (practiceCode) {
      data['practice_area'] = practiceCode;
    }

    return data;
  }

  /**
   * Parse JSON API response from WordPress or member database.
   */
  parseJsonResponse(body) {
    const attorneys = [];

    let data;
    try {
      data = JSON.parse(body);
    } catch {
      return attorneys;
    }

    // Handle WordPress AJAX response wrapper
    if (data.success !== undefined) {
      data = data.data || data;
    }

    const records = Array.isArray(data)
      ? data
      : (data.results || data.members || data.data || data.Records ||
         data.attorneys || data.lawyers || data.Items || []);

    for (const rec of records) {
      const fullName = rec.FullName || rec.full_name || rec.Name || rec.name ||
        `${rec.FirstName || rec.first_name || rec.fname || ''} ${rec.LastName || rec.last_name || rec.lname || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.FirstName || rec.first_name || rec.fname || firstName,
        last_name: rec.LastName || rec.last_name || rec.lname || lastName,
        full_name: fullName,
        firm_name: (rec.Company || rec.Firm || rec.firm_name || rec.FirmName || rec.firm || '').trim(),
        city: (rec.City || rec.city || '').trim(),
        state: (rec.State || rec.state || 'NV').trim(),
        phone: (rec.Phone || rec.phone || rec.PhoneNumber || rec.work_phone || '').trim(),
        email: (rec.Email || rec.email || '').trim(),
        website: (rec.Website || rec.website || rec.url || '').trim(),
        bar_number: String(rec.BarNumber || rec.bar_number || rec.MemberNumber || rec.bar_id || ''),
        bar_status: (rec.Status || rec.status || rec.MemberStatus || 'Active').trim(),
        profile_url: rec.ProfileUrl || rec.profile_url || rec.link || rec.permalink || '',
      });
    }

    return attorneys;
  }

  /**
   * Parse HTML response for attorney data.
   */
  parseHtmlResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Look for result containers
    $('.attorney-result, .lawyer-result, .member-result, .search-result, .result-item, .directory-entry, .entry').each((_, el) => {
      const $card = $(el);
      const nameEl = $card.find('a, .name, .member-name, .attorney-name, h3, h4, strong').first();
      const fullName = nameEl.text().trim();
      if (!fullName || fullName.length < 3) return;
      if (/^(find|search|results|page|home|about|contact|next)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);
      const profileLink = nameEl.is('a') ? nameEl.attr('href') : ($card.find('a').first().attr('href') || '');

      let phone = '';
      const telLink = $card.find('a[href^="tel:"]');
      if (telLink.length) {
        phone = telLink.attr('href').replace('tel:', '');
      } else {
        phone = $card.find('.phone, .member-phone, .attorney-phone').text().trim();
        if (!phone) {
          const cardText = $card.text();
          const phoneMatch = cardText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
          if (phoneMatch) phone = phoneMatch[1];
        }
      }

      let email = '';
      const mailLink = $card.find('a[href^="mailto:"]');
      if (mailLink.length) {
        email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
      }

      const firmName = $card.find('.firm, .company, .firm-name, .attorney-firm').text().trim();
      let city = $card.find('.city, .location, .member-city').text().trim();
      if (!city) {
        const addrText = $card.find('.address, .member-address').text().trim();
        const parsed = this.parseCityStateZip(addrText);
        city = parsed.city;
      }

      let website = '';
      const webLink = $card.find('a[href^="http"]').filter((_, a) => {
        const href = $(a).attr('href') || '';
        return !href.includes('nvbar.org') && !href.includes('mailto:') && !href.includes('tel:');
      }).first();
      if (webLink.length) website = webLink.attr('href');

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'NV',
        phone: phone.replace(/[^\d()-\s+.]/g, ''),
        email: email,
        website: website,
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileLink
          ? (profileLink.startsWith('http') ? profileLink : new URL(profileLink, 'https://nvbar.org').href)
          : '',
      });
    });

    // Fallback: table-based results
    if (attorneys.length === 0) {
      $('table').each((_, table) => {
        const rows = $(table).find('tr');
        rows.each((i, row) => {
          if (i === 0) return;
          const cells = $(row).find('td');
          if (cells.length < 2) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|member|attorney|first|last)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $(cells[0]).find('a').attr('href') || '';

          let email = '';
          const mailLink = $(row).find('a[href^="mailto:"]');
          if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
            city: cells.length > 2 ? $(cells[2]).text().trim() : '',
            state: 'NV',
            phone: cells.length > 3 ? $(cells[3]).text().trim() : '',
            email: email,
            website: '',
            bar_number: cells.length > 4 ? $(cells[4]).text().trim().replace(/[^\d]/g, '') : '',
            bar_status: 'Active',
            profile_url: profileLink
              ? (profileLink.startsWith('http') ? profileLink : new URL(profileLink, 'https://nvbar.org').href)
              : '',
          });
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from JSON or HTML response.
   */
  extractCountFromResponse(body) {
    try {
      const data = JSON.parse(body);
      const source = data.data || data;
      return source.totalCount || source.TotalCount || source.total || source.Total ||
             source.totalRows || source.found_posts || source.count || 0;
    } catch {
      // Not JSON
    }

    const $ = cheerio.load(body);
    const text = $.text();

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|members?|records?|attorneys?|lawyers?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?|lawyers?)\s*(?:found|returned)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() to discover the Nevada Bar's actual search endpoint
   * and query it. Handles WordPress AJAX, REST API, and HTML form patterns.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

    // Step 1: Discover the search endpoint
    await rateLimiter.wait();
    const endpoint = await this.discoverEndpoint(rateLimiter);

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

        let response;

        try {
          await rateLimiter.wait();

          if (endpoint === this.wpAjaxUrl) {
            // WordPress admin-ajax.php
            const ajaxData = this.buildWpAjaxData(city, practiceCode, page);
            log.info(`Page ${page} — POST ${this.wpAjaxUrl} [City=${city}, Action=${this.ajaxAction}]`);
            response = await this.httpPost(this.wpAjaxUrl, ajaxData, rateLimiter);
          } else if (endpoint && endpoint.includes('wp-json')) {
            // WordPress REST API
            const params = new URLSearchParams({
              city: city,
              state: 'NV',
              status: 'Active',
              page: String(page),
              per_page: String(this.pageSize),
            });
            if (practiceCode) params.set('practice_area', practiceCode);
            const restUrl = `${endpoint}?${params.toString()}`;
            log.info(`Page ${page} — GET ${restUrl}`);
            response = await this.httpGet(restUrl, rateLimiter);
          } else if (endpoint) {
            // Custom endpoint (iframe src, form action, etc.)
            const formData = this.buildFormData(city, practiceCode, page);
            log.info(`Page ${page} — POST ${endpoint} [City=${city}]`);
            response = await this.httpPost(endpoint, formData, rateLimiter);
          } else {
            // Fallback to the main page with form POST
            const formData = this.buildFormData(city, practiceCode, page);
            log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);
            response = await this.httpPost(this.baseUrl, formData, rateLimiter);
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

        // Try JSON first, then HTML
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

module.exports = new NevadaScraper();
