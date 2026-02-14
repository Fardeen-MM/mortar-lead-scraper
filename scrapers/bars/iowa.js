/**
 * Iowa State Bar Association Scraper
 *
 * Source: https://www.iowabar.org/?pg=findalawyerdirectory
 * Method: CMS form with POST-based search (opt-in directory)
 *
 * The Iowa Bar uses a CMS-driven "Find a Lawyer" directory where only
 * attorneys who have opted in are listed. The form submits via POST
 * with city, practice area, and name fields. Results are rendered as
 * server-side HTML within the CMS template. Pagination is handled
 * through page parameters in the form data.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IowaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'iowa',
      stateCode: 'IA',
      baseUrl: 'https://www.iowabar.org/?pg=findalawyerdirectory',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'agricultural':           'Agricultural Law',
        'appellate':              'Appellate Practice',
        'banking':                'Banking Law',
        'bankruptcy':             'Bankruptcy',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'government':             'Government',
        'immigration':            'Immigration',
        'insurance':              'Insurance',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Labor Law',
        'mediation':              'Mediation/Arbitration',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax Law',
        'tax law':                'Tax Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Des Moines', 'Cedar Rapids', 'Davenport', 'Sioux City',
        'Iowa City', 'Waterloo', 'Ames', 'Dubuque',
      ],
    });

    this.searchUrl = 'https://www.iowabar.org/?pg=findalawyerdirectory';
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
          'Origin': 'https://www.iowabar.org',
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
   * Build form data for the Iowa Bar search form.
   */
  buildFormData(city, practiceCode, page) {
    const data = {
      'pg': 'findalawyerdirectory',
      'action': 'search',
      'city': city,
      'state': 'IA',
    };

    if (practiceCode) {
      data['practice_area'] = practiceCode;
    }

    if (page > 1) {
      data['page'] = String(page);
    }

    return data;
  }

  /**
   * Parse the HTML results from the CMS directory response.
   */
  parseHtmlResults(body) {
    const attorneys = [];
    const $ = cheerio.load(body);

    // Look for result cards/entries within the CMS content area
    $('.attorney-result, .lawyer-result, .directory-result, .member-result, .search-result-item').each((_, el) => {
      const $card = $(el);
      const attorney = this.extractFromCard($, $card);
      if (attorney) attorneys.push(attorney);
    });

    // Fallback: look for table-based results
    if (attorneys.length === 0) {
      $('table.results, table.directory, table[class*="lawyer"], table[class*="attorney"]').each((_, table) => {
        const rows = $(table).find('tr');
        rows.each((i, row) => {
          if (i === 0) return;
          const cells = $(row).find('td');
          if (cells.length < 2) return;

          const fullName = $(cells[0]).text().trim();
          if (!fullName || /^(name|attorney|member|first|last)/i.test(fullName)) return;

          const { firstName, lastName } = this.splitName(fullName);
          const profileLink = $(cells[0]).find('a').attr('href') || '';

          let firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
          let city = cells.length > 2 ? $(cells[2]).text().trim() : '';
          let phone = cells.length > 3 ? $(cells[3]).text().trim() : '';

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: firmName,
            city: city,
            state: 'IA',
            phone: phone.replace(/[^\d()-\s+.]/g, ''),
            email: '',
            website: '',
            bar_number: '',
            bar_status: 'Active',
            profile_url: profileLink
              ? new URL(profileLink, 'https://www.iowabar.org').href
              : '',
          });
        });
      });
    }

    // Fallback: look for repeated div patterns within the content area
    if (attorneys.length === 0) {
      const contentArea = $('#content, .content, .main-content, .page-content, #main').first();
      const target = contentArea.length ? contentArea : $('body');

      // Look for name patterns: links followed by address/phone info
      target.find('h3 a, h4 a, strong a, .name a, p > a').each((_, el) => {
        const $link = $(el);
        const fullName = $link.text().trim();
        if (!fullName || fullName.length < 3 || fullName.length > 60) return;
        if (/^(home|about|contact|search|find|back|next|page|prev)/i.test(fullName)) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = $link.attr('href') || '';

        // Look for sibling/parent text with details
        const parentText = $link.parent().parent().text();
        const phoneMatch = parentText.match(/(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4})/);
        const emailMatch = parentText.match(/[\w.+-]+@[\w-]+\.[\w.-]+/);

        // Try to find firm name - usually the next text block
        const nextText = $link.parent().next().text().trim();
        const firmName = (nextText && nextText.length < 100 && !phoneMatch) ? nextText : '';

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmName,
          city: '',
          state: 'IA',
          phone: phoneMatch ? phoneMatch[1] : '',
          email: emailMatch ? emailMatch[0] : '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://www.iowabar.org').href
            : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract attorney data from a card/result div.
   */
  extractFromCard($, $card) {
    const nameEl = $card.find('a, .name, .attorney-name, .lawyer-name, h3, h4, strong').first();
    const fullName = nameEl.text().trim();
    if (!fullName || fullName.length < 3) return null;

    const { firstName, lastName } = this.splitName(fullName);
    const profileLink = nameEl.is('a') ? nameEl.attr('href') : ($card.find('a').first().attr('href') || '');

    // Extract firm name
    const firmName = $card.find('.firm, .company, .firm-name, .attorney-firm').text().trim();

    // Extract location
    let city = $card.find('.city, .location, .attorney-city').text().trim();
    if (!city) {
      const locationText = $card.find('.address, .attorney-address').text().trim();
      const parsed = this.parseCityStateZip(locationText);
      city = parsed.city;
    }

    // Extract phone
    let phone = '';
    const telLink = $card.find('a[href^="tel:"]');
    if (telLink.length) {
      phone = telLink.attr('href').replace('tel:', '');
    } else {
      phone = $card.find('.phone, .attorney-phone').text().trim();
    }

    // Extract email
    let email = '';
    const mailLink = $card.find('a[href^="mailto:"]');
    if (mailLink.length) {
      email = mailLink.attr('href').replace('mailto:', '').split('?')[0];
    }

    // Extract website
    let website = '';
    const webLink = $card.find('a[href^="http"]').filter((_, a) => {
      const href = $(a).attr('href') || '';
      return !href.includes('iowabar.org') && !href.includes('mailto:') && !href.includes('tel:');
    }).first();
    if (webLink.length) website = webLink.attr('href');

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: 'IA',
      phone: phone.replace(/[^\d()-\s+.]/g, ''),
      email: email,
      website: website,
      bar_number: '',
      bar_status: 'Active',
      profile_url: profileLink
        ? new URL(profileLink, 'https://www.iowabar.org').href
        : '',
    };
  }

  /**
   * Extract total result count from the response.
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
   * Check if there is a next page link in the response.
   */
  hasNextPage(body) {
    const $ = cheerio.load(body);
    return $('a[href*="page="], a.next, a:contains("Next"), .pagination a.next, a:contains(">>")').length > 0;
  }

  /**
   * Override search() for Iowa's CMS-based Find a Lawyer directory.
   * This is an opt-in directory, so results may be limited.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
    }

    log.info(`Iowa Bar is an opt-in directory — not all attorneys are listed`);

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
          log.success(`Found ${attorneys.length} results for ${city} (opt-in directory)`);
        }

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if there is a next page
        if (!this.hasNextPage(response.body)) {
          log.success(`Completed all results for ${city}`);
          break;
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

module.exports = new IowaScraper();
