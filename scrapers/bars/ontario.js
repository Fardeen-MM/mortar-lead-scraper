/**
 * Ontario Law Society Scraper
 *
 * Source: https://lawyerandparalegal.directory
 * Method: SPA with underlying JSON API — ~85,000 records (largest Canadian directory)
 *
 * The Law Society of Ontario directory is a single-page application backed by a
 * JSON API. Overrides search() to discover and query the API directly for
 * efficient bulk retrieval.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class OntarioScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ontario',
      stateCode: 'CA-ON',
      baseUrl: 'https://lawyerandparalegal.directory',
      pageSize: 100,
      practiceAreaCodes: {
        'family':                'Family',
        'family law':            'Family',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'real estate':           'Real Estate',
        'corporate/commercial':  'Corporate Commercial',
        'corporate':             'Corporate Commercial',
        'commercial':            'Corporate Commercial',
        'personal injury':       'Personal Injury',
        'employment':            'Employment',
        'labour':                'Labour',
        'immigration':           'Immigration',
        'estate planning/wills': 'Wills and Estates',
        'estate planning':       'Wills and Estates',
        'wills':                 'Wills and Estates',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative',
        'environmental':         'Environmental',
        'insurance':             'Insurance',
        'banking/finance':       'Banking and Finance',
        'human rights':          'Human Rights',
      },
      defaultCities: [
        'Toronto', 'Ottawa', 'Mississauga', 'Hamilton', 'Brampton',
        'London', 'Markham', 'Vaughan', 'Kitchener', 'Windsor',
      ],
    });

    // Known and potential API endpoints for the LSO directory SPA
    this.apiEndpoints = [
      `${this.baseUrl}/api/members`,
      `${this.baseUrl}/api/search`,
      `${this.baseUrl}/api/directory`,
      `${this.baseUrl}/api/v1/members`,
      `${this.baseUrl}/api/v1/search`,
    ];
  }

  /**
   * HTTP POST for API requests.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const postData = typeof data === 'string' ? data : JSON.stringify(data);
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/json,*/*',
        },
      };
      const proto = parsed.protocol === 'https:' ? require('https') : require('http');
      const req = proto.request(options, (res) => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
      req.setTimeout(15000);
      req.write(postData);
      req.end();
    });
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for JSON API`);
  }

  /**
   * Not used — search() is fully overridden for the JSON API.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for JSON API`);
  }

  /**
   * Map an LSO API record to a standard attorney object.
   */
  mapLsoRecord(rec) {
    const firstName = (rec.firstName || rec.FirstName || rec.first_name || rec.givenName || '').trim();
    const lastName = (rec.lastName || rec.LastName || rec.last_name || rec.surname || '').trim();
    const fullName = (rec.fullName || rec.FullName || rec.displayName || rec.name ||
                      rec.Name || `${firstName} ${lastName}`).trim();

    return {
      first_name: firstName || this.splitName(fullName).firstName,
      last_name: lastName || this.splitName(fullName).lastName,
      full_name: fullName,
      firm_name: (rec.firmName || rec.FirmName || rec.firm || rec.employerName ||
                  rec.EmployerName || rec.organization || rec.companyName || '').trim(),
      city: (rec.city || rec.City || rec.businessCity || rec.practiceCity || '').trim(),
      state: 'CA-ON',
      phone: (rec.phone || rec.Phone || rec.phoneNumber || rec.businessPhone ||
              rec.PhoneNumber || '').trim(),
      email: (rec.email || rec.Email || rec.publicEmail || '').trim(),
      website: (rec.website || rec.Website || rec.url || '').trim(),
      bar_number: (rec.memberNumber || rec.MemberNumber || rec.registrationNumber ||
                   rec.licenceNumber || rec.LicenceNumber || rec.lsoNumber || '').toString().trim(),
      bar_status: (rec.status || rec.Status || rec.memberStatus || rec.registrationStatus ||
                   rec.MemberStatus || 'Active').trim(),
      profile_url: rec.profileUrl || rec.ProfileUrl || rec.detailUrl || '',
    };
  }

  /**
   * Attempt to discover the API URL by loading the SPA and inspecting scripts.
   */
  async discoverApiUrl(rateLimiter) {
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.baseUrl, rateLimiter);

      if (response.statusCode !== 200) return null;

      rateLimiter.resetBackoff();

      const $ = cheerio.load(response.body);

      // Look for API URLs in script tags
      const scripts = [];
      $('script[src]').each((_, el) => {
        const src = $(el).attr('src');
        if (src) scripts.push(src);
      });

      // Check inline scripts for API base URL
      let apiUrl = null;
      $('script').each((_, el) => {
        const text = $(el).html() || '';
        // Look for patterns like apiUrl: "...", baseUrl: "...", API_BASE: "..."
        const apiMatch = text.match(/(?:apiUrl|api_url|apiBase|API_BASE|baseUrl|apiEndpoint)\s*[:=]\s*['"](https?:\/\/[^'"]+)['"]/i);
        if (apiMatch) {
          apiUrl = apiMatch[1];
        }
        // Also look for fetch/axios calls
        const fetchMatch = text.match(/(?:fetch|axios\.get|axios\.post|\.get|\.post)\s*\(\s*['"]((?:https?:\/\/[^'"]+|\/api\/[^'"]+))['"]/i);
        if (fetchMatch && !apiUrl) {
          apiUrl = fetchMatch[1];
          if (apiUrl.startsWith('/')) {
            apiUrl = `${this.baseUrl}${apiUrl}`;
          }
        }
      });

      return apiUrl;
    } catch (err) {
      log.info(`API discovery failed: ${err.message}`);
      return null;
    }
  }

  /**
   * Async generator that yields attorney records from the LSO directory.
   * Discovers and queries the JSON API for efficient retrieval of ~85,000 records.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    // Step 1: Try to discover the API URL from the SPA
    log.scrape(`Discovering LSO directory API endpoint`);
    const discoveredApiUrl = await this.discoverApiUrl(rateLimiter);
    if (discoveredApiUrl) {
      log.success(`Discovered API endpoint: ${discoveredApiUrl}`);
      // Prepend discovered URL to the list of endpoints to try
      this.apiEndpoints.unshift(discoveredApiUrl);
    }

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let workingApiUrl = null;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        let attorneys = [];
        let apiSuccess = false;

        // Try each API endpoint
        const endpointsToTry = workingApiUrl ? [workingApiUrl] : [...new Set(this.apiEndpoints)];

        for (const baseApiUrl of endpointsToTry) {
          // Try GET with query params
          const params = new URLSearchParams();
          params.set('city', city);
          params.set('page', String(page));
          params.set('pageSize', String(this.pageSize));
          params.set('limit', String(this.pageSize));
          params.set('offset', String((page - 1) * this.pageSize));
          params.set('status', 'Active');
          if (practiceCode) {
            params.set('practiceArea', practiceCode);
          }

          const getUrl = `${baseApiUrl}?${params.toString()}`;
          log.info(`Page ${page} — GET ${getUrl}`);

          try {
            await rateLimiter.wait();
            const response = await this.httpGet(getUrl, rateLimiter);

            if (response.statusCode === 200) {
              let data;
              try { data = JSON.parse(response.body); } catch (e) { continue; }

              // Extract records from various response structures
              const records = Array.isArray(data) ? data :
                (data.items || data.Items || data.records || data.Records ||
                 data.results || data.Results || data.data || data.members ||
                 data.Members || data.lawyers || []);

              // Extract total count if available
              if (page === 1) {
                totalResults = data.total || data.Total || data.totalCount ||
                               data.TotalCount || data.count || data.totalRecords || 0;
              }

              if (records.length > 0) {
                log.info(`API returned ${records.length} records`);
                attorneys = records.map(rec => this.mapLsoRecord(rec));
                apiSuccess = true;
                workingApiUrl = baseApiUrl;
                rateLimiter.resetBackoff();
                break;
              }
            } else if (response.statusCode === 429 || response.statusCode === 403) {
              if (this.detectCaptcha(response.body)) {
                log.warn(`CAPTCHA detected on API — skipping ${city}`);
                yield { _captcha: true, city, page };
                apiSuccess = false;
                break;
              }
              const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
              if (!shouldRetry) break;
            }
          } catch (err) {
            log.info(`API GET failed for ${baseApiUrl}: ${err.message}`);
          }

          // Try POST if GET did not work
          if (!apiSuccess) {
            try {
              const searchPayload = JSON.stringify({
                city: city,
                page: page,
                pageSize: this.pageSize,
                offset: (page - 1) * this.pageSize,
                limit: this.pageSize,
                status: 'Active',
                practiceArea: practiceCode || '',
              });

              await rateLimiter.wait();
              const response = await this.httpPost(
                baseApiUrl,
                searchPayload,
                rateLimiter,
                'application/json'
              );

              if (response.statusCode === 200) {
                let data;
                try { data = JSON.parse(response.body); } catch (e) { continue; }

                const records = Array.isArray(data) ? data :
                  (data.items || data.Items || data.records || data.Records ||
                   data.results || data.Results || data.data || data.members ||
                   data.Members || data.lawyers || []);

                if (page === 1) {
                  totalResults = data.total || data.Total || data.totalCount ||
                                 data.TotalCount || data.count || data.totalRecords || 0;
                }

                if (records.length > 0) {
                  attorneys = records.map(rec => this.mapLsoRecord(rec));
                  apiSuccess = true;
                  workingApiUrl = baseApiUrl;
                  rateLimiter.resetBackoff();
                  break;
                }
              }
            } catch (err) {
              log.info(`API POST failed for ${baseApiUrl}: ${err.message}`);
            }
          }
        }

        // HTML fallback: scrape the SPA's server-rendered content
        if (!apiSuccess && !workingApiUrl) {
          const htmlUrl = `${this.baseUrl}/search?city=${encodeURIComponent(city)}&page=${page}`;
          log.info(`API not available — trying HTML fallback: ${htmlUrl}`);

          try {
            await rateLimiter.wait();
            const response = await this.httpGet(htmlUrl, rateLimiter);

            if (response.statusCode === 200) {
              rateLimiter.resetBackoff();

              if (this.detectCaptcha(response.body)) {
                log.warn(`CAPTCHA detected — skipping ${city}`);
                yield { _captcha: true, city, page };
                break;
              }

              const $ = cheerio.load(response.body);

              // Try to parse any visible lawyer listings
              $('table tr, .lawyer-card, .member-card, .result-item, .search-result').each((_, el) => {
                const $el = $(el);
                const cells = $el.find('td');

                let fullName, profileLink;

                if (cells.length >= 2) {
                  fullName = $(cells[0]).text().trim();
                  profileLink = $(cells[0]).find('a').attr('href') || '';
                } else {
                  fullName = $el.find('a, .name, .lawyer-name').first().text().trim();
                  profileLink = $el.find('a').first().attr('href') || '';
                }

                if (!fullName || fullName.length < 3) return;
                if (/^(name|member|last|first|#)$/i.test(fullName)) return;

                const cityText = cells.length > 1 ? $(cells[1]).text().trim() : ($el.find('.city, .location').text().trim() || city);
                const status = cells.length > 2 ? $(cells[2]).text().trim() : ($el.find('.status').text().trim() || '');
                const barNumber = cells.length > 3 ? $(cells[3]).text().trim() : ($el.find('.member-number, .licence-number').text().trim() || '');

                let email = '';
                const mailtoLink = $el.find('a[href^="mailto:"]');
                if (mailtoLink.length) {
                  email = mailtoLink.attr('href').replace('mailto:', '').trim();
                }

                let phone = '';
                const phoneMatch = $el.text().match(/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/);
                if (phoneMatch) phone = phoneMatch[0];

                let firstName = '';
                let lastName = '';
                if (fullName.includes(',')) {
                  const parts = fullName.split(',').map(s => s.trim());
                  lastName = parts[0];
                  firstName = parts[1] ? parts[1].split(/\s+/)[0] : '';
                } else {
                  const nameParts = this.splitName(fullName);
                  firstName = nameParts.firstName;
                  lastName = nameParts.lastName;
                }

                const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

                attorneys.push({
                  first_name: firstName,
                  last_name: lastName,
                  full_name: displayName,
                  firm_name: '',
                  city: cityText,
                  state: 'CA-ON',
                  phone,
                  email,
                  website: '',
                  bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
                  bar_status: status || 'Active',
                  profile_url: profileLink && profileLink.startsWith('http') ? profileLink : (profileLink ? `${this.baseUrl}${profileLink}` : ''),
                });
              });
            }
          } catch (err) {
            log.error(`HTML fallback failed: ${err.message}`);
          }
        }

        if (attorneys.length === 0) {
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
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

        if (page === 1 && totalResults > 0) {
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        } else if (page === 1) {
          log.success(`Fetching results for ${city} (first batch: ${attorneys.length} records)`);
        }

        consecutiveEmpty = 0;

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.toString().match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if this is the last page
        if (attorneys.length < this.pageSize) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        if (totalResults > 0) {
          const totalPages = Math.ceil(totalResults / this.pageSize);
          if (page >= totalPages) {
            log.success(`Completed all ${totalPages} pages for ${city}`);
            break;
          }
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new OntarioScraper();
