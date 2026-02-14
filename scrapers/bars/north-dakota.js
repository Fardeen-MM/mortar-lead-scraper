/**
 * State Bar Association of North Dakota Scraper
 *
 * Source: https://www.sband.org/page/FindaLawyer
 * Method: CMS-based search with POST/GET form submission
 *
 * The SBAND "Find a Lawyer" directory is a small bar (~2,900 members).
 * The search form on the CMS-driven page sends requests with city/name
 * parameters. Results are rendered as HTML within the CMS template.
 * Due to the small bar size, comprehensive coverage is achievable
 * with city-based searches alone.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NorthDakotaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'north-dakota',
      stateCode: 'ND',
      baseUrl: 'https://www.sband.org/page/FindaLawyer',
      pageSize: 50,
      practiceAreaCodes: {
        'administrative':         'Administrative',
        'agricultural':           'Agricultural',
        'appellate':              'Appellate',
        'banking':                'Banking & Finance',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate',
        'criminal':               'Criminal',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment',
        'energy':                 'Energy & Natural Resources',
        'environmental':          'Environmental',
        'estate planning':        'Estate Planning',
        'family':                 'Family',
        'family law':             'Family',
        'general practice':       'General Practice',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor',
        'personal injury':        'Personal Injury',
        'probate':                'Probate & Trust',
        'real estate':            'Real Estate',
        'tax':                    'Tax',
        'tax law':                'Tax',
        'tribal law':             'Tribal Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Fargo', 'Bismarck', 'Grand Forks', 'Minot',
        'West Fargo', 'Williston', 'Dickinson', 'Mandan',
      ],
    });

    this.searchUrl = 'https://www.sband.org/page/FindaLawyer';
  }

  /**
   * Not used directly -- search() is overridden for CMS POST requests.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CMS POST`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CMS POST`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CMS POST`);
  }

  /**
   * HTTP POST with URL-encoded form data for the CMS search.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const postBody = typeof formData === 'string'
        ? formData
        : new URLSearchParams(formData).toString();

      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.searchUrl,
          'Origin': 'https://www.sband.org',
          'Connection': 'keep-alive',
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
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Build form data for the SBAND search.
   */
  buildFormData(city, practiceCode, page) {
    const data = {
      'city': city,
      'state': 'ND',
      'search': 'Search',
    };

    if (practiceCode) {
      data['practice_area'] = practiceCode;
    }

    if (page > 1) {
      data['page'] = String(page);
      data['offset'] = String((page - 1) * this.pageSize);
    }

    return data;
  }

  /**
   * Parse HTML results from the CMS directory response.
   */
  parseHtmlResults(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Look for result cards/entries
    $('.member-result, .lawyer-result, .directory-result, .search-result, .result-item, .attorney-item').each((_, el) => {
      const $card = $(el);
      const attorney = this.extractFromCard($, $card);
      if (attorney) attorneys.push(attorney);
    });

    // Fallback: table-based results
    if (attorneys.length === 0) {
      $('table').each((_, table) => {
        const $table = $(table);
        const rows = $table.find('tr');
        if (rows.length < 2) return;

        rows.each((i, row) => {
          if (i === 0) return;
          const cells = $(row).find('td');
          if (cells.length < 2) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|member|attorney|first|last|search)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $(cells[0]).find('a').attr('href') || '';

          let phone = '';
          const telLink = $(row).find('a[href^="tel:"]');
          if (telLink.length) phone = telLink.attr('href').replace('tel:', '');

          let email = '';
          const mailLink = $(row).find('a[href^="mailto:"]');
          if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
            city: cells.length > 2 ? $(cells[2]).text().trim() : '',
            state: 'ND',
            phone: phone || (cells.length > 3 ? $(cells[3]).text().trim() : ''),
            email: email,
            website: '',
            bar_number: '',
            bar_status: 'Active',
            profile_url: profileLink
              ? new URL(profileLink, 'https://www.sband.org').href
              : '',
          });
        });
      });
    }

    // Fallback: look for structured list items or definition lists
    if (attorneys.length === 0) {
      const contentArea = $('#content, .content, .main-content, .page-content, #main, .iMISContent').first();
      const target = contentArea.length ? contentArea : $('body');

      target.find('h3, h4, strong, b, .name').each((_, el) => {
        const $el = $(el);
        const fullName = $el.text().trim();
        if (!fullName || fullName.length < 3 || fullName.length > 60) return;
        if (/^(find|search|results|page|home|about|contact|next|prev|back)/i.test(fullName)) return;
        // Skip if it looks like an address or non-name text
        if (/^\d|^(no|the|this|your|our|a\s)/i.test(fullName)) return;

        const { firstName, lastName } = this.splitName(fullName);
        const link = $el.find('a').attr('href') || $el.parent().find('a').first().attr('href') || '';

        // Gather surrounding text for phone/email extraction
        const parentText = $el.parent().text() + ' ' + $el.parent().next().text();
        const phoneMatch = parentText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
        const emailMatch = parentText.match(/[\w.+-]+@[\w-]+\.[\w.-]+/);
        const firmText = $el.next().text().trim();

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: (firmText && firmText.length < 80 && !phoneMatch) ? firmText : '',
          city: '',
          state: 'ND',
          phone: phoneMatch ? phoneMatch[1] : '',
          email: emailMatch ? emailMatch[0] : '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: link
            ? new URL(link, 'https://www.sband.org').href
            : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract attorney data from a card/div element.
   */
  extractFromCard($, $card) {
    const nameEl = $card.find('a, .name, .member-name, .attorney-name, h3, h4, strong').first();
    const fullName = nameEl.text().trim();
    if (!fullName || fullName.length < 3) return null;
    if (/^(find|search|results|page)/i.test(fullName)) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.is('a') ? nameEl.attr('href') : ($card.find('a').first().attr('href') || '');

    let phone = '';
    const telLink = $card.find('a[href^="tel:"]');
    if (telLink.length) {
      phone = telLink.attr('href').replace('tel:', '');
    } else {
      phone = $card.find('.phone, .member-phone').text().trim();
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

    const firmName = $card.find('.firm, .company, .member-firm').text().trim();
    let city = $card.find('.city, .location, .member-city').text().trim();
    if (!city) {
      const addressText = $card.find('.address, .member-address').text().trim();
      const parsed = this.parseCityStateZip(addressText);
      city = parsed.city;
    }

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: 'ND',
      phone: phone.replace(/[^\d()-\s+.]/g, ''),
      email: email,
      website: '',
      bar_number: '',
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://www.sband.org').href
        : '',
    };
  }

  /**
   * Extract total result count from the HTML response.
   */
  extractCountFromHtml(body) {
    const $ = cheerio.load(body);
    const text = $.text();

    const matchFound = text.match(/([\d,]+)\s*(?:results?|records?|attorneys?|lawyers?|members?)\s*(?:found|returned|matched)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for SBAND's CMS-based Find a Lawyer directory.
   * The small bar size (~2,900) means city-based searches should provide
   * good coverage without needing letter-by-letter enumeration.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

    log.info(`North Dakota has ~2,900 bar members — small bar, expect manageable result sets`);

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

        const formData = this.buildFormData(city, practiceCode, page);
        log.info(`Page ${page} — POST ${this.searchUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.searchUrl, formData, rateLimiter);
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
          totalResults = this.extractCountFromHtml(response.body);
          if (totalResults > 0) {
            log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
          }
        }

        const attorneys = this.parseHtmlResults(response.body);

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

module.exports = new NorthDakotaScraper();
