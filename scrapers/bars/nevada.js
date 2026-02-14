/**
 * State Bar of Nevada Scraper
 *
 * Source: https://members.nvbar.org/cvweb/cgi-bin/memberdll.dll/info?WRP=lrs_referralNew.htm
 * Method: CV5 (Community Voice) memberdll system with tilde-delimited parameters
 *
 * The Nevada Bar uses a CV5 member directory system similar to Alaska and Kentucky.
 * The search form submits to a memberdll.dll endpoint with tilde-delimited params.
 * The response is HTML fragments containing member listings.
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
      baseUrl: 'https://members.nvbar.org/cvweb/cgi-bin/memberdll.dll/info?WRP=lrs_referralNew.htm',
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

    this.ajaxBaseUrl = 'https://members.nvbar.org/cvweb/cgi-bin/memberdll.dll/List';
  }

  /**
   * Not used directly -- search() is overridden for CV5 AJAX requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CV5 AJAX`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CV5 AJAX`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CV5 AJAX`);
  }

  /**
   * Build the tilde-delimited AJAX URL for the Nevada Bar CV5 system.
   * Similar to Alaska's CV5 system — params are delimited by tildes.
   */
  buildAjaxUrl(city, practiceCode, page) {
    const offset = (page - 1) * this.pageSize;
    const params = [
      'CIT', city,
      'STA', 'NV',
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
   */
  parseAjaxResponse(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // CV5 systems typically render results in table rows or div-based cards
    $('tr.memberRow, .member-row, .cv-member-row, tr[class*="member"]').each((_, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const nameEl = $row.find('a').first();
      const fullName = nameEl.text().trim() || $(cells[0]).text().trim();
      if (!fullName) return;

      const { firstName, lastName } = this.splitName(fullName);
      const profileLink = nameEl.attr('href') || '';
      const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

      let phone = '';
      const telLink = $row.find('a[href^="tel:"]');
      if (telLink.length) {
        phone = telLink.attr('href').replace('tel:', '');
      } else {
        const rowText = $row.text();
        const phoneMatch = rowText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
        if (phoneMatch) phone = phoneMatch[1];
      }

      let email = '';
      const mailLink = $row.find('a[href^="mailto:"]');
      if (mailLink.length) {
        email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
        city: cells.length > 2 ? $(cells[2]).text().trim() : '',
        state: 'NV',
        phone: phone.replace(/[^\d()-\s+.]/g, ''),
        email: email,
        website: '',
        bar_number: barNumber,
        bar_status: 'Active',
        profile_url: profileLink
          ? new URL(profileLink, 'https://members.nvbar.org').href
          : '',
      });
    });

    // Fallback: div-based card layout
    if (attorneys.length === 0) {
      $('div.member-card, div.memberCard, .cv-member-card, .member-item').each((_, el) => {
        const $card = $(el);
        const nameEl = $card.find('a, .member-name, .name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = nameEl.attr('href') || '';
        const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $card.find('.firm, .company, .member-firm').text().trim(),
          city: $card.find('.city, .location, .member-city').text().trim(),
          state: 'NV',
          phone: $card.find('.phone, .member-phone').text().trim(),
          email: $card.find('a[href^="mailto:"]').text().trim(),
          website: '',
          bar_number: barNumber,
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://members.nvbar.org').href
            : '',
        });
      });
    }

    // Fallback: generic table parsing
    if (attorneys.length === 0) {
      $('table').each((_, table) => {
        const rows = $(table).find('tr');
        rows.each((i, row) => {
          if (i === 0) return; // skip header
          const cells = $(row).find('td');
          if (cells.length < 2) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|member|attorney|first|last)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $(cells[0]).find('a').attr('href') || '';
          const barNumber = profileLink.match(/(?:ID|num|bar)=(\d+)/i)?.[1] || '';

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
            bar_number: barNumber,
            bar_status: 'Active',
            profile_url: profileLink
              ? new URL(profileLink, 'https://members.nvbar.org').href
              : '',
          });
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from the AJAX response.
   */
  extractCountFromAjax(body) {
    const $ = cheerio.load(body);
    const text = $.text();

    const matchOf = text.match(/(?:of|total[:\s]*)\s*([\d,]+)\s*(?:results?|members?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?)\s*(?:found|returned|matched)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for Nevada Bar CV5 AJAX GET requests with tilde-delimited params.
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

module.exports = new NevadaScraper();
