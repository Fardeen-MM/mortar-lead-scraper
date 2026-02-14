/**
 * Vermont Bar Association Scraper
 *
 * Source: https://www.vtbar.org/online-directory/
 * Method: HTML form with name/firm, practice areas, language, city, state fields
 *
 * The VBA online directory is a WordPress-based form (likely Gravity Forms or
 * a custom plugin) that supports filtering by name, firm, practice area,
 * language, and city. Results are rendered as HTML on the same page or a
 * results page.
 *
 * Flow:
 * 1. GET the directory page to inspect the form structure
 * 2. POST or GET with search parameters (city, practice area)
 * 3. Parse HTML results
 * 4. Paginate via query params or next-page links
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class VermontScraper extends BaseScraper {
  constructor() {
    super({
      name: 'vermont',
      stateCode: 'VT',
      baseUrl: 'https://www.vtbar.org/online-directory/',
      pageSize: 20,
      practiceAreaCodes: {
        'administrative':        'Administrative Law',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business/Corporate',
        'civil litigation':      'Civil Litigation',
        'criminal':              'Criminal Law',
        'criminal defense':      'Criminal Law',
        'elder':                 'Elder Law',
        'employment':            'Employment/Labor',
        'labor':                 'Employment/Labor',
        'environmental':         'Environmental Law',
        'estate planning':       'Estate Planning/Probate',
        'estate':                'Estate Planning/Probate',
        'family':                'Family Law',
        'family law':            'Family Law',
        'immigration':           'Immigration',
        'intellectual property': 'Intellectual Property',
        'personal injury':       'Personal Injury/Tort',
        'real estate':           'Real Estate',
        'tax':                   'Tax Law',
        'tax law':               'Tax Law',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Burlington', 'South Burlington', 'Montpelier', 'Rutland',
        'Barre', 'St. Albans', 'Winooski', 'Brattleboro',
      ],
    });
  }

  /**
   * Build search URL with query parameters for WordPress-based directory.
   */
  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    if (city) params.set('city', city);
    if (practiceCode) params.set('practice_area', practiceCode);
    if (page && page > 1) params.set('pg', String(page));
    params.set('state', 'VT');

    const queryStr = params.toString();
    return queryStr ? `${this.baseUrl}?${queryStr}` : this.baseUrl;
  }

  /**
   * Parse attorneys from search results HTML.
   */
  parseResultsPage($) {
    const attorneys = [];

    // WordPress directories often use article/div cards or table rows
    // Try common WordPress directory plugin patterns
    $('.member-directory-item, .directory-entry, .lawyer-listing, article.directory, .attorney-card, .result-item').each((_, el) => {
      const $el = $(el);
      const nameEl = $el.find('h2 a, h3 a, h4 a, .name a, .attorney-name a, .member-name a').first();
      let fullName = nameEl.text().trim();
      let profileUrl = nameEl.attr('href') || '';

      if (!fullName) {
        fullName = $el.find('h2, h3, h4, .name, .attorney-name').first().text().trim();
      }

      if (!fullName) return;

      const { firstName, lastName } = this.splitName(fullName);
      const firmName = $el.find('.firm, .firm-name, .company, .organization').text().trim();
      const cityText = $el.find('.city, .location, .address-city').text().trim();
      const phone = ($el.find('.phone, .telephone, a[href^="tel:"]').text().trim() || '').replace(/[^\d()-.\s+]/g, '');
      const website = $el.find('a[href*="http"]:not([href*="vtbar.org"]):not([href^="mailto:"])').attr('href') || '';

      let email = '';
      const emailLink = $el.find('a[href^="mailto:"]');
      if (emailLink.length) {
        email = emailLink.attr('href').replace('mailto:', '').trim();
      }

      // Check for Cloudflare-protected email
      const cfEmail = $el.find('[data-cfemail], a[href*="email-protection"]');
      if (cfEmail.length) {
        const encoded = cfEmail.attr('data-cfemail') || (cfEmail.attr('href') || '').replace(/.*#/, '');
        if (encoded) email = this.decodeCloudflareEmail(encoded);
      }

      // Parse practice areas from the listing if present
      const practiceAreas = $el.find('.practice-areas, .areas-of-practice, .specialties').text().trim();

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: cityText || '',
        state: 'VT',
        phone: phone,
        email: email,
        website: website,
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileUrl,
      });
    });

    // Fallback: table-based results
    if (attorneys.length === 0) {
      $('table tr').each((_, row) => {
        const $row = $(row);
        if ($row.find('th').length > 0) return;

        const cells = $row.find('td');
        if (cells.length < 2) return;

        const nameCell = $(cells[0]);
        const nameLink = nameCell.find('a');
        const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
        const profileUrl = nameLink.attr('href') || '';

        if (!fullName || /^(name|search|last)/i.test(fullName)) return;

        const { firstName, lastName } = this.splitName(fullName);
        const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
        const city = cells.length > 2 ? $(cells[2]).text().trim() : '';
        const phone = cells.length > 3 ? $(cells[3]).text().trim().replace(/[^\d()-.\s+]/g, '') : '';
        let email = '';
        const emailLink = $row.find('a[href^="mailto:"]');
        if (emailLink.length) {
          email = emailLink.attr('href').replace('mailto:', '').trim();
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firmName,
          city: city,
          state: 'VT',
          phone: phone,
          email: email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl,
        });
      });
    }

    // Fallback: generic link-based listing
    if (attorneys.length === 0) {
      $('a[href*="directory"], a[href*="attorney"], a[href*="lawyer"], a[href*="member"]').each((_, el) => {
        const $link = $(el);
        const fullName = $link.text().trim();
        if (!fullName || fullName.length > 60 || fullName.length < 4) return;
        if (/^(search|back|next|prev|home|about|contact)/i.test(fullName)) return;
        // Ensure it looks like a name (has at least a space)
        if (!fullName.includes(' ')) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileUrl = $link.attr('href') || '';

        // Try to get sibling/parent info
        const $parent = $link.parent();
        const parentText = $parent.text().replace(fullName, '').trim();

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: '',
          city: '',
          state: 'VT',
          phone: '',
          email: '',
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileUrl.startsWith('http') ? profileUrl : (profileUrl ? `https://www.vtbar.org${profileUrl}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from page.
   */
  extractResultCount($) {
    const text = $('body').text();

    const matchOf = text.match(/([\d,]+)\s+(?:results?|records?|attorneys?|members?|lawyers?)\s+found/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchPage = text.match(/Page\s+\d+\s+of\s+(\d+)/i);
    if (matchPage) return parseInt(matchPage[1], 10) * this.pageSize;

    return 0;
  }

  /**
   * Override search to handle WordPress form POST/GET with pagination.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for VT — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
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

        const url = this.buildSearchUrl({ city, practiceCode, page });
        log.info(`Page ${page} — ${url}`);

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

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            const testAttorneys = this.parseResultsPage($);
            if (testAttorneys.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testAttorneys.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
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

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page link in WordPress pagination
        const hasNext = $('a.next, a.page-numbers.next, .pagination a[rel="next"], a:contains("Next")').length > 0;
        const totalPages = Math.ceil(totalResults / this.pageSize);

        if (!hasNext && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        if (!hasNext && totalPages <= 1) {
          log.success(`Completed all results for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new VermontScraper();
