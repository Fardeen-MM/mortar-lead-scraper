/**
 * Northwest Territories Law Society Scraper
 *
 * Source: https://lsnt.ca.thentiacloud.net/webs/lsnt/register/
 * Method: Thentia Cloud SaaS platform — API discovery needed
 *
 * Thentia Cloud is a regulatory SaaS platform used by several Canadian professional
 * regulators. The public register is typically backed by a JSON API at
 * /webs/{org}/api/. Overrides search() to discover and query the Thentia API.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NorthwestTerritoriesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'northwest-territories',
      stateCode: 'CA-NT',
      baseUrl: 'https://lsnt.ca.thentiacloud.net/webs/lsnt/register/',
      pageSize: 50,
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
        'aboriginal/indigenous': 'Aboriginal',
      },
      defaultCities: [
        'Yellowknife',
      ],
    });

    this.thentiaApiBase = 'https://lsnt.ca.thentiacloud.net/webs/lsnt';
  }

  /**
   * HTTP POST for Thentia API requests.
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
   * Not used — search() is fully overridden.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Thentia API`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Thentia API`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Thentia API`);
  }

  /**
   * Map a Thentia API record to a standard attorney object.
   */
  mapThentiaRecord(rec) {
    const firstName = (rec.firstName || rec.FirstName || rec.first_name || rec.givenName || '').trim();
    const lastName = (rec.lastName || rec.LastName || rec.last_name || rec.surname || '').trim();
    const fullName = (rec.fullName || rec.FullName || rec.displayName || rec.name || `${firstName} ${lastName}`).trim();

    return {
      first_name: firstName || this.splitName(fullName).firstName,
      last_name: lastName || this.splitName(fullName).lastName,
      full_name: fullName,
      firm_name: (rec.employerName || rec.EmployerName || rec.firmName || rec.organization || rec.firm || '').trim(),
      city: (rec.city || rec.City || rec.mailingCity || rec.practiceCity || '').trim(),
      state: 'CA-NT',
      phone: (rec.phone || rec.Phone || rec.phoneNumber || rec.businessPhone || '').trim(),
      email: (rec.email || rec.Email || rec.publicEmail || '').trim(),
      website: '',
      bar_number: (rec.registrationNumber || rec.RegistrationNumber || rec.memberNumber || rec.licenceNumber || rec.certificateNumber || '').toString().trim(),
      bar_status: (rec.status || rec.Status || rec.registrationStatus || rec.licenceStatus || 'Active').trim(),
      profile_url: rec.profileUrl || rec.ProfileUrl || '',
    };
  }

  /**
   * Parse lawyers from Thentia HTML register page (fallback).
   */
  parseLawyersFromHtml(html) {
    const $ = cheerio.load(html);
    const attorneys = [];

    // Thentia renders a table or card-based register
    $('table tbody tr, .register-row, .registrant-card, .member-row').each((_, el) => {
      const $el = $(el);
      const cells = $el.find('td');

      let fullName, city, status, barNumber, phone, email, profileLink;

      if (cells.length >= 2) {
        fullName = $(cells[0]).text().trim();
        profileLink = $(cells[0]).find('a').attr('href') || '';
        city = cells.length > 1 ? $(cells[1]).text().trim() : '';
        status = cells.length > 2 ? $(cells[2]).text().trim() : '';
        barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';
        phone = cells.length > 4 ? $(cells[4]).text().trim() : '';
      } else {
        fullName = $el.find('.name, .registrant-name, a').first().text().trim();
        profileLink = $el.find('a').first().attr('href') || '';
        city = $el.find('.city, .location').text().trim();
        status = $el.find('.status').text().trim();
        barNumber = $el.find('.registration-number, .licence-number').text().trim();
        phone = $el.find('.phone').text().trim();
      }

      if (!fullName || fullName.length < 3) return;

      email = '';
      const mailtoLink = $el.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

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
        city: city || 'Yellowknife',
        state: 'CA-NT',
        phone: phone || '',
        email: email || '',
        website: '',
        bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
        bar_status: status || 'Active',
        profile_url: profileLink && profileLink.startsWith('http') ? profileLink : (profileLink ? `${this.thentiaApiBase}${profileLink}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the NWT Law Society.
   * Discovers and queries the Thentia Cloud API, with HTML fallback.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let useApi = true;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        let attorneys = [];

        if (useApi) {
          // Try Thentia Cloud API endpoints
          const apiEndpoints = [
            `${this.thentiaApiBase}/api/register?city=${encodeURIComponent(city)}&page=${page}&pageSize=${this.pageSize}`,
            `${this.thentiaApiBase}/api/publicregister?city=${encodeURIComponent(city)}&page=${page}&size=${this.pageSize}`,
            `${this.thentiaApiBase}/api/registrants?city=${encodeURIComponent(city)}&offset=${(page - 1) * this.pageSize}&limit=${this.pageSize}`,
            `${this.thentiaApiBase}/api/members/search?city=${encodeURIComponent(city)}&page=${page}`,
          ];

          let apiSuccess = false;
          for (const apiUrl of apiEndpoints) {
            try {
              await rateLimiter.wait();
              const response = await this.httpGet(apiUrl, rateLimiter);

              if (response.statusCode === 200) {
                let data;
                try { data = JSON.parse(response.body); } catch (e) { continue; }

                const records = Array.isArray(data) ? data :
                  (data.items || data.Items || data.records || data.Records ||
                   data.results || data.Results || data.data || data.registrants || []);

                if (records.length > 0) {
                  log.info(`Thentia API returned ${records.length} records from ${apiUrl}`);
                  attorneys = records.map(rec => this.mapThentiaRecord(rec));
                  apiSuccess = true;
                  rateLimiter.resetBackoff();
                  break;
                }
              } else if (response.statusCode === 429 || response.statusCode === 403) {
                if (this.detectCaptcha(response.body)) {
                  log.warn(`CAPTCHA detected on Thentia API — skipping ${city}`);
                  yield { _captcha: true, city, page };
                  useApi = false;
                  break;
                }
                const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
                if (!shouldRetry) { useApi = false; break; }
              }
            } catch (err) {
              log.info(`Thentia API endpoint failed: ${err.message}`);
            }
          }

          // Try POST-based search
          if (!apiSuccess && useApi) {
            try {
              const searchPayload = JSON.stringify({
                city: city,
                page: page,
                pageSize: this.pageSize,
                status: 'Active',
              });

              await rateLimiter.wait();
              const response = await this.httpPost(
                `${this.thentiaApiBase}/api/register/search`,
                searchPayload,
                rateLimiter,
                'application/json'
              );

              if (response.statusCode === 200) {
                let data;
                try { data = JSON.parse(response.body); } catch (e) { data = null; }

                if (data) {
                  const records = Array.isArray(data) ? data :
                    (data.items || data.Items || data.records || data.Records ||
                     data.results || data.Results || data.data || []);
                  if (records.length > 0) {
                    attorneys = records.map(rec => this.mapThentiaRecord(rec));
                    apiSuccess = true;
                    rateLimiter.resetBackoff();
                  }
                }
              }
            } catch (err) {
              log.info(`Thentia POST search failed: ${err.message}`);
            }
          }

          if (!apiSuccess && page === 1) {
            log.info(`Thentia API not available — falling back to HTML scraping`);
            useApi = false;
          }
        }

        // HTML fallback
        if (!useApi) {
          const htmlUrl = `${this.baseUrl}?city=${encodeURIComponent(city)}&page=${page}`;
          log.info(`Page ${page} — GET ${htmlUrl}`);

          try {
            await rateLimiter.wait();
            const response = await this.httpGet(htmlUrl, rateLimiter);

            if (response.statusCode === 429 || response.statusCode === 403) {
              log.warn(`Got ${response.statusCode} from ${this.name}`);
              const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
              if (!shouldRetry) break;
              continue;
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

            attorneys = this.parseLawyersFromHtml(response.body);
          } catch (err) {
            log.error(`Request failed: ${err.message}`);
            break;
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

        if (page === 1) {
          log.success(`Fetching results for ${city} (first batch: ${attorneys.length} records)`);
        }

        consecutiveEmpty = 0;

        for (const attorney of attorneys) {
          yield this.transformResult(attorney, practiceArea);
        }

        if (attorneys.length < this.pageSize) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new NorthwestTerritoriesScraper();
