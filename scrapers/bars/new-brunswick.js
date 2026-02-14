/**
 * New Brunswick Law Society Scraper
 *
 * Source: https://lsbnb.alinityapp.com/client/publicdirectory
 * Method: Alinity platform (same as Saskatchewan) — SPA with underlying API
 *
 * Uses the same Alinity SaaS registration platform as Saskatchewan.
 * Overrides search() to discover and query the Alinity API, with HTML fallback.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NewBrunswickScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-brunswick',
      stateCode: 'CA-NB',
      baseUrl: 'https://lsbnb.alinityapp.com/client/publicdirectory',
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
        'bilingual services':    'Bilingual',
        'francophone':           'Francophone',
      },
      defaultCities: [
        'Moncton', 'Saint John', 'Fredericton', 'Dieppe', 'Miramichi',
      ],
    });

    this.apiBaseUrl = 'https://lsbnb.alinityapp.com/api';
  }

  /**
   * HTTP POST for Alinity API requests.
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
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for Alinity API`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for Alinity API`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for Alinity API`);
  }

  /**
   * Map an Alinity API record to a standard attorney object.
   */
  mapAlinityRecord(rec) {
    const firstName = (rec.FirstName || rec.firstName || rec.first_name || '').trim();
    const lastName = (rec.LastName || rec.lastName || rec.last_name || '').trim();
    const fullName = (rec.FullName || rec.fullName || rec.DisplayName || rec.displayName || `${firstName} ${lastName}`).trim();

    return {
      first_name: firstName || this.splitName(fullName).firstName,
      last_name: lastName || this.splitName(fullName).lastName,
      full_name: fullName,
      firm_name: (rec.EmployerName || rec.employerName || rec.FirmName || rec.firmName || rec.Organization || '').trim(),
      city: (rec.City || rec.city || rec.MailingCity || '').trim(),
      state: 'CA-NB',
      phone: (rec.Phone || rec.phone || rec.PhoneNumber || rec.BusinessPhone || '').trim(),
      email: (rec.Email || rec.email || rec.PublicEmail || '').trim(),
      website: '',
      bar_number: (rec.RegistrationNumber || rec.registrationNumber || rec.MemberNumber || rec.memberNumber || rec.RegistrantNo || '').toString().trim(),
      bar_status: (rec.Status || rec.status || rec.RegistrationStatus || rec.registrationStatus || 'Active').trim(),
      profile_url: rec.ProfileUrl || rec.profileUrl || '',
    };
  }

  /**
   * Parse lawyers from Alinity HTML page (fallback).
   */
  parseLawyersFromHtml(html) {
    const $ = cheerio.load(html);
    const attorneys = [];

    $('table tbody tr, .directory-row, .registrant-row, .member-card').each((_, el) => {
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
        barNumber = $el.find('.registration-number, .member-number').text().trim();
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
        city: city,
        state: 'CA-NB',
        phone: phone || '',
        email: email || '',
        website: '',
        bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
        bar_status: status || 'Active',
        profile_url: profileLink && profileLink.startsWith('http') ? profileLink : (profileLink ? `https://lsbnb.alinityapp.com${profileLink}` : ''),
      });
    });

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the NB Law Society.
   * Attempts Alinity JSON API first, then falls back to HTML scraping.
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
          // Try Alinity JSON API endpoints
          const apiEndpoints = [
            `${this.apiBaseUrl}/publicdirectory?city=${encodeURIComponent(city)}&page=${page}&pageSize=${this.pageSize}`,
            `${this.apiBaseUrl}/PublicDirectory/Search?city=${encodeURIComponent(city)}&page=${page}&pageSize=${this.pageSize}`,
            `${this.apiBaseUrl}/registrant/search?city=${encodeURIComponent(city)}&page=${page}&limit=${this.pageSize}`,
          ];

          let apiSuccess = false;
          for (const apiUrl of apiEndpoints) {
            try {
              await rateLimiter.wait();
              const response = await this.httpGet(apiUrl, rateLimiter);

              if (response.statusCode === 200) {
                let data;
                try { data = JSON.parse(response.body); } catch (e) { continue; }

                const records = Array.isArray(data) ? data : (data.Items || data.items || data.Records || data.records || data.Results || data.results || data.data || []);

                if (records.length > 0) {
                  log.info(`Alinity API returned ${records.length} records from ${apiUrl}`);
                  attorneys = records.map(rec => this.mapAlinityRecord(rec));
                  apiSuccess = true;
                  rateLimiter.resetBackoff();
                  break;
                }
              } else if (response.statusCode === 429 || response.statusCode === 403) {
                if (this.detectCaptcha(response.body)) {
                  log.warn(`CAPTCHA detected on Alinity API — skipping ${city}`);
                  yield { _captcha: true, city, page };
                  useApi = false;
                  break;
                }
                const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
                if (!shouldRetry) { useApi = false; break; }
              }
            } catch (err) {
              log.info(`Alinity API endpoint failed: ${err.message}`);
            }
          }

          // Fall back to POST-based search
          if (!apiSuccess && useApi) {
            try {
              const searchPayload = JSON.stringify({
                City: city,
                Page: page,
                PageSize: this.pageSize,
                Status: 'Active',
              });

              await rateLimiter.wait();
              const response = await this.httpPost(
                `${this.apiBaseUrl}/publicdirectory/search`,
                searchPayload,
                rateLimiter,
                'application/json'
              );

              if (response.statusCode === 200) {
                let data;
                try { data = JSON.parse(response.body); } catch (e) { data = null; }

                if (data) {
                  const records = Array.isArray(data) ? data : (data.Items || data.items || data.Records || data.records || data.Results || data.results || data.data || []);
                  if (records.length > 0) {
                    attorneys = records.map(rec => this.mapAlinityRecord(rec));
                    apiSuccess = true;
                    rateLimiter.resetBackoff();
                  }
                }
              }
            } catch (err) {
              log.info(`Alinity POST search failed: ${err.message}`);
            }
          }

          if (!apiSuccess && page === 1) {
            log.info(`Alinity API not available — falling back to HTML scraping`);
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

module.exports = new NewBrunswickScraper();
