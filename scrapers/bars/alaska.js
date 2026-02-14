/**
 * Alaska Bar Association Scraper
 *
 * Source: https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm
 * Method: AJAX GET to memberdll.dll/List with tilde-delimited parameters
 *
 * The Alaska Bar uses a CV5 (Community Voice) member directory system.
 * The search form sends AJAX GET requests to a DLL endpoint with parameters
 * delimited by tildes (~). The response is HTML fragments containing
 * member listings that are injected into the page via JavaScript.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class AlaskaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alaska',
      stateCode: 'AK',
      baseUrl: 'https://member.alaskabar.org/cv5/cgi-bin/utilities.dll/openpage?wrp=membersearch.htm',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'ADM',
        'bankruptcy':             'BKR',
        'business':               'BUS',
        'civil litigation':       'CIV',
        'corporate':              'COR',
        'criminal':               'CRM',
        'criminal defense':       'CRM',
        'elder':                  'ELD',
        'employment':             'EMP',
        'environmental':          'ENV',
        'estate planning':        'EST',
        'family':                 'FAM',
        'family law':             'FAM',
        'general practice':       'GEN',
        'immigration':            'IMM',
        'intellectual property':  'IPR',
        'labor':                  'LAB',
        'medical malpractice':    'MED',
        'native law':             'NAT',
        'oil and gas':            'OIL',
        'personal injury':        'PIN',
        'real estate':            'REA',
        'tax':                    'TAX',
        'tax law':                'TAX',
        'workers comp':           'WCM',
      },
      defaultCities: [
        'Anchorage', 'Fairbanks', 'Juneau', 'Wasilla',
        'Sitka', 'Kenai', 'Palmer', 'Kodiak',
      ],
    });

    this.ajaxBaseUrl = 'https://member.alaskabar.org/cv5/cgi-bin/memberdll.dll/List';
  }

  /**
   * Not used directly -- search() is overridden for AJAX GET requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for AJAX requests`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for AJAX requests`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for AJAX requests`);
  }

  /**
   * Build the tilde-delimited AJAX URL for the Alaska Bar CV5 system.
   * The CV5 system uses tildes as parameter delimiters in a specific order:
   * Field~Value~Field~Value~... appended to the DLL path.
   *
   * @param {string} city - City to search
   * @param {string|null} practiceCode - Practice area code
   * @param {number} page - Page number (1-indexed)
   * @returns {string} Fully constructed AJAX URL
   */
  buildAjaxUrl(city, practiceCode, page) {
    const offset = (page - 1) * this.pageSize;
    const params = [
      'CIT', city,
      'STA', 'AK',
      'STAT', 'Active',
      'OFFSET', String(offset),
      'LIMIT', String(this.pageSize),
    ];

    if (practiceCode) {
      params.push('PRA', practiceCode);
    }

    const tildeParams = params.join('~');
    return `${this.ajaxBaseUrl}?${tildeParams}`;
  }

  /**
   * Parse the AJAX HTML response from the CV5 member listing.
   * Results are typically returned as an HTML fragment with member cards/rows.
   */
  parseAjaxResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // CV5 systems typically render results in table rows or div-based cards
    $('tr.memberRow, .member-row, .cv-member-row, tr[class*="member"]').each((_, el) => {
      const $row = $(el);
      const attorney = this.extractFromRow($, $row);
      if (attorney) attorneys.push(attorney);
    });

    // Fallback: look for repeated structured containers
    if (attorneys.length === 0) {
      $('div.member-card, div.memberCard, .cv-member-card, .member-item').each((_, el) => {
        const $card = $(el);
        const attorney = this.extractFromCard($, $card);
        if (attorney) attorneys.push(attorney);
      });
    }

    // Fallback: parse HTML tables generically
    if (attorneys.length === 0) {
      $('table').each((_, table) => {
        const $table = $(table);
        const rows = $table.find('tr');
        if (rows.length < 2) return;

        rows.each((i, row) => {
          if (i === 0) return; // skip header
          const cells = $(row).find('td');
          if (cells.length < 3) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|member|attorney)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
          const cityText = cells.length > 2 ? $(cells[2]).text().trim() : '';
          const phone = cells.length > 3 ? $(cells[3]).text().trim() : '';
          const profileLink = $(cells[0]).find('a').attr('href') || '';
          const barNumber = profileLink.match(/ID=(\d+)/i)?.[1] || '';

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: firmName,
            city: cityText || '',
            state: 'AK',
            phone: phone.replace(/[^\d()-\s+.]/g, ''),
            email: '',
            website: '',
            bar_number: barNumber,
            bar_status: 'Active',
            profile_url: profileLink ? `https://member.alaskabar.org${profileLink.startsWith('/') ? '' : '/'}${profileLink}` : '',
          });
        });
      });
    }

    // Final fallback: try parsing line-by-line text if structured HTML not found
    if (attorneys.length === 0 && body.includes('~')) {
      const lines = body.split('\n').filter(l => l.trim());
      for (const line of lines) {
        const parts = line.split('~').map(p => p.trim());
        if (parts.length < 3) continue;
        const fullName = parts[0] || '';
        if (!fullName || /^(name|header|total)/i.test(fullName)) continue;

        const { firstName, lastName } = this.splitName(fullName);
        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: parts[1] || '',
          city: parts[2] || '',
          state: 'AK',
          phone: parts[3] || '',
          email: parts[4] || '',
          website: '',
          bar_number: parts[5] || '',
          bar_status: 'Active',
          profile_url: '',
        });
      }
    }

    return attorneys;
  }

  /**
   * Extract attorney data from a table row element.
   */
  extractFromRow($, $row) {
    const cells = $row.find('td');
    if (cells.length < 2) return null;

    const nameEl = $row.find('a').first();
    const fullName = nameEl.text().trim() || $(cells[0]).text().trim();
    if (!fullName) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.attr('href') || '';
    const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
      city: cells.length > 2 ? $(cells[2]).text().trim() : '',
      state: 'AK',
      phone: cells.length > 3 ? $(cells[3]).text().trim() : '',
      email: '',
      website: '',
      bar_number: barNumber,
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://member.alaskabar.org').href
        : '',
    };
  }

  /**
   * Extract attorney data from a card/div element.
   */
  extractFromCard($, $card) {
    const nameEl = $card.find('a, .member-name, .name').first();
    const fullName = nameEl.text().trim();
    if (!fullName) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.attr('href') || '';
    const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

    const firmName = $card.find('.firm, .company, .member-firm').text().trim();
    const city = $card.find('.city, .location, .member-city').text().trim();
    const phone = $card.find('.phone, .member-phone').text().trim();
    const email = $card.find('a[href^="mailto:"]').text().trim();

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: 'AK',
      phone: phone,
      email: email,
      website: '',
      bar_number: barNumber,
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://member.alaskabar.org').href
        : '',
    };
  }

  /**
   * Extract total result count from the AJAX response.
   */
  extractCountFromAjax(body) {
    const $ = cheerio.load(body);
    const text = $.text();

    // Look for "X results", "X members found", "Showing X of Y", etc.
    const matchOf = text.match(/(?:of|total[:\s]*)\s*([\d,]+)\s*(?:results?|members?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?)\s*(?:found|returned|matched)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for Alaska Bar AJAX GET requests with tilde-delimited params.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

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

        const url = this.buildAjaxUrl(city, practiceCode, page);
        log.info(`Page ${page} — AJAX GET ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
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
          totalResults = this.extractCountFromAjax(response.body);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        const attorneys = this.parseAjaxResponse(response.body);

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

module.exports = new AlaskaScraper();
