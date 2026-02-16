/**
 * South Carolina Bar Scraper (SC Courts)
 *
 * Source: https://www.sccourts.org/attorneys/
 * Method: GET search by last name prefix, paginated. POST to fetch detail pages.
 *
 * Flow:
 * 1. GET /attorneys/ — establish session (antiforgery cookie)
 * 2. GET /attorneys/?last=<prefix>&page=<N> — paginated name search (20 per page)
 *    Returns attorney names with internal IDs (which ARE the bar numbers).
 * 3. POST /attorneys/detail/ with __RequestVerificationToken + id — full detail:
 *    name, firm, address, city, state, zip, phone, email, bar number, admission date
 *
 * Since there is no city/location filter, we iterate A-Z last name prefixes.
 * For test mode (maxPages=1), we fetch 1 page per letter (1 letter = 20 results).
 *
 * The site requires full browser headers (Sec-Fetch-*, Sec-Ch-Ua-*) on POST
 * requests to avoid 406 Not Acceptable bot protection.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class SouthCarolinaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'south-carolina',
      stateCode: 'SC',
      baseUrl: 'https://www.sccourts.org/attorneys/',
      pageSize: 20,
      practiceAreaCodes: {},
      defaultCities: [
        'Charleston', 'Columbia', 'Greenville', 'Mount Pleasant',
        'Rock Hill', 'Summerville', 'North Charleston', 'Spartanburg',
      ],
    });

    this.origin = 'https://www.sccourts.org';
    this.detailUrl = 'https://www.sccourts.org/attorneys/detail/';
    this.lastNameLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
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
   * Build full browser headers to bypass 406 bot protection.
   */
  _browserHeaders(ua, cookies, isPost) {
    const h = {
      'User-Agent': ua,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'identity',
      'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"macOS"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': isPost ? 'same-origin' : 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'Connection': 'keep-alive',
    };
    if (cookies) h['Cookie'] = cookies;
    return h;
  }

  /**
   * HTTP GET with cookie tracking and redirect following.
   */
  _httpGet(url, ua, cookies) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._browserHeaders(ua, cookies, false);

      https.get(url, { headers, timeout: 15000 }, (res) => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
        const allCookies = [cookies, setCookies].filter(Boolean).join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `${this.origin}${loc}`;
          return resolve(this._httpGet(loc, ua, allCookies));
        }

        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: allCookies }));
      }).on('error', reject);
    });
  }

  /**
   * HTTP POST with full browser headers.
   */
  _httpPost(url, postBody, ua, cookies) {
    return new Promise((resolve, reject) => {
      const parsed = new URL(url);
      const headers = this._browserHeaders(ua, cookies, true);
      headers['Content-Type'] = 'application/x-www-form-urlencoded';
      headers['Content-Length'] = Buffer.byteLength(postBody);
      headers['Origin'] = this.origin;
      headers['Referer'] = this.baseUrl;

      const opts = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers,
        timeout: 15000,
      };

      const req = https.request(opts, (res) => {
        const setCookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
        const allCookies = [cookies, setCookies].filter(Boolean).join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let loc = res.headers.location;
          if (loc.startsWith('/')) loc = `${this.origin}${loc}`;
          return resolve(this._httpGet(loc, ua, allCookies));
        }

        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: allCookies }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse the search results page to extract attorney name/ID pairs.
   * Returns: [{ name, id }]
   */
  _parseSearchResults($) {
    const results = [];
    $('.result form').each((_, el) => {
      const name = $(el).find('button').text().trim();
      const id = $(el).find('input[name="id"]').val();
      if (name && id) {
        results.push({ name, id });
      }
    });
    return results;
  }

  /**
   * Extract total result count from the page text.
   */
  _getResultCount($) {
    const text = $('body').text();
    const match = text.match(/([\d,]+)\s*result/i);
    return match ? parseInt(match[1].replace(/,/g, ''), 10) : 0;
  }

  /**
   * Extract the antiforgery token from a results page.
   */
  _getToken($) {
    return $('input[name="__RequestVerificationToken"]').first().val() || '';
  }

  /**
   * Fetch attorney detail page and parse contact info.
   */
  async _fetchDetail(id, token, ua, cookies, rateLimiter) {
    const postBody = new URLSearchParams({
      '__RequestVerificationToken': token,
      'id': id,
    }).toString();

    await rateLimiter.wait();
    const res = await this._httpPost(this.detailUrl, postBody, ua, cookies);

    if (res.statusCode !== 200) {
      log.warn(`Detail page returned ${res.statusCode} for id=${id}`);
      return null;
    }

    const $ = cheerio.load(res.body);

    // Parse name from h2 heading (format: "Mr./Ms. First Middle Last" or "Title. Last, First Middle")
    const nameHeading = $('h2').first().text().trim();
    // Remove title prefix (Mr., Mrs., Ms., Hon., etc.)
    const cleanName = nameHeading.replace(/^(?:Mr\.|Mrs\.|Ms\.|Dr\.|Hon\.|Judge)\s*/i, '').trim();

    // Parse contact info from .attorney-contact
    const contact = $('.attorney-contact').first();
    if (!contact.length) {
      log.warn(`No contact info found for id=${id}`);
      return null;
    }

    // Firm name from <strong> inside contact
    const firmName = contact.find('strong').first().text().trim();

    // Phone from "Office: (xxx) xxx-xxxx"
    const contactText = contact.text();
    const phoneMatch = contactText.match(/Office:\s*([\d()\s.-]+)/);
    const phone = phoneMatch ? phoneMatch[1].trim() : '';

    // Email from mailto link
    const emailLink = contact.find('a[href^="mailto:"]').first();
    const email = emailLink.length ? emailLink.attr('href').replace('mailto:', '').trim() : '';

    // Address parsing from the first <p>
    const firstP = contact.find('p').first();
    let city = '';
    let state = '';
    let zip = '';
    let address = '';

    if (firstP.length) {
      // The first <p> has address lines separated by <br>
      const addressHtml = firstP.html() || '';
      const lines = addressHtml
        .replace(/<strong[^>]*>.*?<\/strong>/gi, '') // remove firm name
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<[^>]+>/g, '')
        .split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 0);

      // Last line is usually "City, ST ZIP"
      if (lines.length > 0) {
        const lastLine = lines[lines.length - 1];
        const cityStateZip = lastLine.match(/^(.+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
        if (cityStateZip) {
          city = cityStateZip[1].trim();
          state = cityStateZip[2];
          zip = cityStateZip[3];
          address = lines.slice(0, -1).join(', ');
        } else {
          // Try without zip
          const cityState = lastLine.match(/^(.+),\s*([A-Z]{2})$/);
          if (cityState) {
            city = cityState[1].trim();
            state = cityState[2];
            address = lines.slice(0, -1).join(', ');
          }
        }
      }
    }

    // Admission date
    const admMatch = res.body.match(/(?:Admitted|Admission)[:\s]*(\w+\s+\d{1,2},?\s+\d{4})/i);
    const admissionDate = admMatch ? admMatch[1] : '';

    // Parse first/last from cleanName
    let firstName = '';
    let lastName = '';
    if (cleanName.includes(',')) {
      const parts = cleanName.split(',').map(s => s.trim());
      lastName = parts[0];
      firstName = (parts[1] || '').split(/\s+/)[0];
    } else {
      const nameParts = cleanName.split(/\s+/);
      if (nameParts.length >= 2) {
        firstName = nameParts[0];
        lastName = nameParts[nameParts.length - 1];
      } else {
        lastName = cleanName;
      }
    }

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: cleanName,
      firm_name: firmName,
      city: city,
      state: state || 'SC',
      phone: phone,
      email: email,
      website: '',
      bar_number: id,
      bar_status: 'Active',
      admission_date: admissionDate,
      address: address,
      zip: zip,
      source: `${this.name}_bar`,
    };
  }

  /**
   * Override search() — iterate A-Z last name prefixes, paginate, fetch details.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const ua = rateLimiter.getUserAgent();

    if (practiceArea) {
      log.warn(`SC Courts does not support practice area filtering — ignoring "${practiceArea}"`);
    }

    // Step 1: Establish session
    log.info('Establishing session with SC Courts directory...');
    let sessionCookies;

    try {
      await rateLimiter.wait();
      const session = await this._httpGet(this.baseUrl, ua, null);
      if (session.statusCode !== 200) {
        log.error(`SC Courts returned ${session.statusCode} on session init`);
        yield { _captcha: true, city: 'all', reason: `HTTP ${session.statusCode}` };
        return;
      }
      sessionCookies = session.cookies;
      log.success('Session established with SC Courts');
    } catch (err) {
      log.error(`Failed to connect to SC Courts: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: err.message };
      return;
    }

    // Determine letters to iterate
    const letters = options.maxPrefixes
      ? this.lastNameLetters.slice(0, options.maxPrefixes)
      : this.lastNameLetters;

    // In test mode with maxCities=1, only do 1 letter
    const maxLetters = options.maxCities || letters.length;
    const activeLetters = letters.slice(0, maxLetters);

    let totalYielded = 0;
    let totalDetailFetches = 0;
    const maxDetailFetches = options.maxPages ? 10 : Infinity;

    for (let li = 0; li < activeLetters.length; li++) {
      const letter = activeLetters[li];
      yield { _cityProgress: { current: li + 1, total: activeLetters.length } };
      log.scrape(`Searching: attorneys with last name starting with "${letter}" in SC`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for letter ${letter}`);
          break;
        }

        const searchUrl = page === 1
          ? `${this.baseUrl}?last=${letter}`
          : `${this.baseUrl}?last=${letter}&page=${page}`;

        let response;
        try {
          await rateLimiter.wait();
          response = await this._httpGet(searchUrl, ua, sessionCookies);
        } catch (err) {
          log.error(`Search request failed for ${letter} page ${page}: ${err.message}`);
          break;
        }

        if (response.statusCode === 406) {
          log.warn('SC Courts returned 406 — bot protection triggered');
          const shouldRetry = await rateLimiter.handleBlock(406);
          if (shouldRetry) continue;
          yield { _captcha: true, city: letter, reason: '406 Not Acceptable' };
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Search returned ${response.statusCode} for letter ${letter}`);
          break;
        }

        rateLimiter.resetBackoff();
        sessionCookies = response.cookies;

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this._getResultCount($);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} attorneys for letter "${letter}"`);
          }
        }

        const searchResults = this._parseSearchResults($);
        const token = this._getToken($);

        if (searchResults.length === 0) {
          if (page === 1) log.info(`No results for letter "${letter}"`);
          break;
        }

        if (page === 1 && totalResults === 0) {
          log.success(`Found ${searchResults.length} results for letter "${letter}"`);
        }

        // Fetch detail for each result (limited in test mode)
        for (const sr of searchResults) {
          if (totalDetailFetches >= maxDetailFetches) {
            // In test mode, yield listing-level data without detail fetch
            yield this.transformResult({
              first_name: sr.name.includes(',') ? sr.name.split(',')[1]?.trim().split(' ')[0] || '' : '',
              last_name: sr.name.includes(',') ? sr.name.split(',')[0]?.trim() || '' : sr.name,
              city: '', state: 'SC', bar_number: sr.id, bar_status: 'Active',
              profile_url: `${this.detailUrl}${sr.id}`,
            }, practiceArea);
            totalYielded++;
            continue;
          }
          try {
            totalDetailFetches++;
            const attorney = await this._fetchDetail(sr.id, token, ua, sessionCookies, rateLimiter);
            if (attorney) {
              // Apply city filter if specified
              if (options.cities && options.cities.length > 0) {
                const matchesCity = options.cities.some(c =>
                  attorney.city.toLowerCase().includes(c.toLowerCase())
                );
                if (!matchesCity) continue;
              }

              attorney.practice_area = practiceArea || '';
              yield this.transformResult(attorney, practiceArea);
              totalYielded++;
            }
          } catch (err) {
            log.warn(`Failed to fetch detail for ${sr.name} (id=${sr.id}): ${err.message}`);
          }
        }

        // Check for more pages
        if (searchResults.length < this.pageSize) {
          log.success(`Completed all results for letter "${letter}"`);
          break;
        }

        const totalPages = totalResults > 0 ? Math.ceil(totalResults / this.pageSize) : 0;
        if (totalPages > 0 && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for letter "${letter}"`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }

    log.success(`SC scrape complete: ${totalYielded} attorneys yielded`);
  }
}

module.exports = new SouthCarolinaScraper();
