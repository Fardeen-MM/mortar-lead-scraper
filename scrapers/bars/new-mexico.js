/**
 * State Bar of New Mexico Scraper
 *
 * Source: https://www.sbnm.org/For-Public/I-Need-a-Lawyer/Online-Bar-Directory
 * Method: Discover and query underlying search API endpoint
 *
 * The SBNM Online Bar Directory is hosted on a CMS that wraps an internal
 * member database (~8,618 lawyers, mandatory bar). The search interface
 * may use an embedded iframe, AJAX call, or redirect to a member database
 * provider. This scraper discovers the actual API endpoint and queries it
 * with city/practice area filters.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NewMexicoScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-mexico',
      stateCode: 'NM',
      baseUrl: 'https://www.sbnm.org/For-Public/I-Need-a-Lawyer/Online-Bar-Directory',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative',
        'appellate':              'Appellate',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate',
        'criminal':               'Criminal',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment',
        'environmental':          'Environmental',
        'estate planning':        'Estate Planning',
        'family':                 'Family',
        'family law':             'Family',
        'general practice':       'General Practice',
        'immigration':            'Immigration',
        'indian law':             'Indian/Tribal Law',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor',
        'medical malpractice':    'Medical Malpractice',
        'natural resources':      'Natural Resources',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'water law':              'Water Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Albuquerque', 'Santa Fe', 'Las Cruces', 'Rio Rancho',
        'Roswell', 'Farmington', 'Hobbs', 'Carlsbad',
      ],
    });

    // Potential API endpoints to discover
    this.apiEndpoints = [
      'https://www.sbnm.org/api/member/search',
      'https://www.sbnm.org/DesktopModules/MemberDirectory/API/Search',
      'https://members.sbnm.org/search',
      'https://members.sbnm.org/api/members',
    ];

    this.discoveredEndpoint = null;
  }

  /**
   * Not used directly -- search() is overridden with API discovery.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for API discovery`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for API discovery`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for API discovery`);
  }

  /**
   * HTTP POST with JSON or form data for the discovered API endpoint.
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
          'Accept': 'text/html,application/json,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.baseUrl,
          'Origin': 'https://www.sbnm.org',
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
   * Discover the actual search endpoint by examining the directory page.
   * Looks for iframe src, AJAX URLs in scripts, form actions, and API calls.
   */
  async discoverEndpoint(rateLimiter) {
    if (this.discoveredEndpoint) return this.discoveredEndpoint;

    log.info(`Discovering search endpoint for ${this.name}...`);

    try {
      const response = await this.httpGet(this.baseUrl, rateLimiter);
      if (response.statusCode !== 200) {
        log.warn(`Could not load directory page: ${response.statusCode}`);
        return null;
      }

      const $ = cheerio.load(response.body);

      // Check for iframe pointing to member directory
      const iframe = $('iframe[src*="member"], iframe[src*="directory"], iframe[src*="search"]');
      if (iframe.length) {
        const src = iframe.attr('src');
        log.info(`Found iframe endpoint: ${src}`);
        this.discoveredEndpoint = src;
        return src;
      }

      // Check for form action
      const form = $('form[action*="member"], form[action*="directory"], form[action*="search"]');
      if (form.length) {
        const action = form.attr('action');
        const fullUrl = action.startsWith('http') ? action : new URL(action, 'https://www.sbnm.org').href;
        log.info(`Found form endpoint: ${fullUrl}`);
        this.discoveredEndpoint = fullUrl;
        return fullUrl;
      }

      // Check script tags for API URLs
      const scripts = $('script').map((_, el) => $(el).html()).get().join('\n');
      const apiMatch = scripts.match(/(?:url|endpoint|api|ajax)\s*[:=]\s*['"](https?:\/\/[^'"]+(?:member|directory|search)[^'"]*)['"]/i);
      if (apiMatch) {
        log.info(`Found API endpoint in scripts: ${apiMatch[1]}`);
        this.discoveredEndpoint = apiMatch[1];
        return apiMatch[1];
      }

      // Check for data attributes
      const dataUrl = $('[data-url*="member"], [data-url*="search"], [data-api*="member"]').first();
      if (dataUrl.length) {
        const url = dataUrl.attr('data-url') || dataUrl.attr('data-api');
        log.info(`Found data attribute endpoint: ${url}`);
        this.discoveredEndpoint = url;
        return url;
      }
    } catch (err) {
      log.warn(`Endpoint discovery failed: ${err.message}`);
    }

    // Try known API endpoints
    for (const endpoint of this.apiEndpoints) {
      try {
        const response = await this.httpGet(endpoint, rateLimiter);
        if (response.statusCode === 200 || response.statusCode === 400) {
          log.info(`Found working API endpoint: ${endpoint}`);
          this.discoveredEndpoint = endpoint;
          return endpoint;
        }
      } catch {
        // Try next endpoint
      }
    }

    return null;
  }

  /**
   * Parse JSON API response into attorney objects.
   */
  parseJsonResponse(body) {
    const attorneys = [];

    let data;
    try {
      data = JSON.parse(body);
    } catch {
      return attorneys;
    }

    // Handle various JSON response shapes
    const records = Array.isArray(data)
      ? data
      : (data.results || data.members || data.data || data.Records || data.Items || []);

    for (const rec of records) {
      const fullName = rec.FullName || rec.full_name || rec.Name ||
        `${rec.FirstName || rec.first_name || ''} ${rec.LastName || rec.last_name || ''}`.trim();
      if (!fullName) continue;

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: rec.FirstName || rec.first_name || firstName,
        last_name: rec.LastName || rec.last_name || lastName,
        full_name: fullName,
        firm_name: (rec.Company || rec.Firm || rec.firm_name || rec.FirmName || '').trim(),
        city: (rec.City || rec.city || '').trim(),
        state: (rec.State || rec.state || 'NM').trim(),
        phone: (rec.Phone || rec.phone || rec.PhoneNumber || '').trim(),
        email: (rec.Email || rec.email || '').trim(),
        website: (rec.Website || rec.website || rec.WebsiteUrl || '').trim(),
        bar_number: String(rec.BarNumber || rec.bar_number || rec.MemberNumber || ''),
        bar_status: (rec.Status || rec.status || rec.MemberStatus || 'Active').trim(),
        profile_url: rec.ProfileUrl || rec.profile_url || rec.DetailUrl || '',
      });
    }

    return attorneys;
  }

  /**
   * Parse HTML response into attorney objects.
   */
  parseHtmlResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Try table-based results
    $('table').each((_, table) => {
      const rows = $(table).find('tr');
      rows.each((i, row) => {
        if (i === 0) return;
        const cells = $(row).find('td');
        if (cells.length < 3) return;

        const fullName = $(cells[0]).text().trim();
        if (!fullName || /^(name|member|attorney|first|last)/i.test(fullName)) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = $(cells[0]).find('a').attr('href') || '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
          city: cells.length > 2 ? $(cells[2]).text().trim() : '',
          state: 'NM',
          phone: cells.length > 3 ? $(cells[3]).text().trim() : '',
          email: '',
          website: '',
          bar_number: cells.length > 4 ? $(cells[4]).text().trim().replace(/[^\d]/g, '') : '',
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://www.sbnm.org').href
            : '',
        });
      });
    });

    // Try card-based results
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .directory-entry, .attorney-card, .result-item').each((_, el) => {
        const $card = $(el);
        const nameEl = $card.find('a, .name, .member-name, h3, h4, strong').first();
        const fullName = nameEl.text().trim();
        if (!fullName || fullName.length < 3) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = $card.find('a').first().attr('href') || '';

        let phone = '';
        const telLink = $card.find('a[href^="tel:"]');
        if (telLink.length) phone = telLink.attr('href').replace('tel:', '');

        let email = '';
        const mailLink = $card.find('a[href^="mailto:"]');
        if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $card.find('.firm, .company').text().trim(),
          city: $card.find('.city, .location').text().trim(),
          state: 'NM',
          phone: phone,
          email: email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://www.sbnm.org').href
            : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from JSON or HTML response.
   */
  extractCountFromResponse(body) {
    // Try JSON first
    try {
      const data = JSON.parse(body);
      return data.totalCount || data.TotalCount || data.total || data.Total ||
             data.totalRows || data.TotalRows || data.recordCount || 0;
    } catch {
      // Not JSON, try HTML
    }

    const $ = cheerio.load(body);
    const text = $.text();

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|members?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?)\s*(?:found|returned)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() to discover the SBNM search endpoint and query it.
   * Tries multiple approaches: JSON API, HTML form POST, and direct page scraping.
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

        const offset = (page - 1) * this.pageSize;
        let response;

        try {
          await rateLimiter.wait();

          if (endpoint) {
            // Try JSON API POST
            const searchData = {
              city: city,
              state: 'NM',
              status: 'Active',
              practiceArea: practiceCode || '',
              pageSize: this.pageSize,
              pageIndex: page - 1,
              skip: offset,
              take: this.pageSize,
            };

            log.info(`Page ${page} — POST ${endpoint} [City=${city}]`);
            response = await this.httpPost(endpoint, searchData, rateLimiter, 'application/json');
          } else {
            // Fallback: POST to the main directory URL
            const formData = {
              'city': city,
              'state': 'NM',
              'status': 'Active',
              'page': String(page),
            };

            if (practiceCode) {
              formData['practiceArea'] = practiceCode;
            }

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

        // Try parsing as JSON first, then as HTML
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

module.exports = new NewMexicoScraper();
