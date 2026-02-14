/**
 * Yukon Law Society Scraper
 *
 * Source: https://lawsocietyyukon.com
 * Method: WordPress/Divi site — attempts WP REST API first, falls back to HTML scraping
 * ~120 lawyers total
 *
 * Overrides search() to attempt WP REST API at /wp-json/wp/v2/ endpoints,
 * then falls back to HTML scraping of the member directory page.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class YukonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'yukon',
      stateCode: 'CA-YT',
      baseUrl: 'https://lawsocietyyukon.com',
      pageSize: 50,
      practiceAreaCodes: {
        'family':                'family',
        'family law':            'family',
        'criminal':              'criminal',
        'criminal defense':      'criminal',
        'real estate':           'real-estate',
        'corporate/commercial':  'corporate-commercial',
        'corporate':             'corporate-commercial',
        'commercial':            'corporate-commercial',
        'personal injury':       'personal-injury',
        'employment':            'employment',
        'labour':                'employment',
        'immigration':           'immigration',
        'estate planning/wills': 'wills-estates',
        'estate planning':       'wills-estates',
        'wills':                 'wills-estates',
        'intellectual property': 'intellectual-property',
        'civil litigation':      'civil-litigation',
        'litigation':            'civil-litigation',
        'tax':                   'tax',
        'administrative':        'administrative',
        'environmental':         'environmental',
      },
      defaultCities: [
        'Whitehorse',
      ],
    });

    this.wpApiUrl = `${this.baseUrl}/wp-json/wp/v2`;
    this.directoryPath = '/find-a-lawyer';
  }

  /**
   * Not used — search() is fully overridden.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }

  /**
   * Not used — search() is fully overridden.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Parse lawyers from HTML page content.
   * Handles common WordPress/Divi directory layouts: tables, divs, lists.
   */
  parseLawyersFromHtml(html) {
    const $ = cheerio.load(html);
    const attorneys = [];

    // Strategy 1: Table-based directory
    $('table tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstText = $(cells[0]).text().trim();
      // Skip header rows
      if (/^name$/i.test(firstText)) return;
      if (!firstText || firstText.length < 3) return;

      let fullName = firstText;
      let firm = cells.length > 1 ? $(cells[1]).text().trim() : '';
      let phone = cells.length > 2 ? $(cells[2]).text().trim() : '';
      let email = '';

      // Check for mailto links
      const mailtoLink = $row.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

      // Check for phone patterns in remaining cells
      for (let c = 1; c < cells.length; c++) {
        const text = $(cells[c]).text().trim();
        if (/\(\d{3}\)\s*\d{3}[- ]?\d{4}/.test(text) || /\d{3}[- .]\d{3}[- .]\d{4}/.test(text)) {
          phone = text;
        }
      }

      const profileLink = $row.find('a').first().attr('href') || '';
      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firm,
        city: 'Whitehorse',
        state: 'CA-YT',
        phone,
        email,
        website: '',
        bar_number: '',
        bar_status: 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `${this.baseUrl}${profileLink}` : ''),
      });
    });

    // Strategy 2: Div-based directory (Divi/WordPress)
    if (attorneys.length === 0) {
      $('.et_pb_text_inner, .entry-content, .page-content, .member-listing, .lawyer-listing, .directory-entry').find('p, li, .member, .lawyer').each((_, el) => {
        const $el = $(el);
        const text = $el.text().trim();
        if (!text || text.length < 5) return;

        // Try to extract name - phone - email patterns
        const nameMatch = text.match(/^([A-Z][a-zA-Z'-]+(?:\s+[A-Z][a-zA-Z'-]+)+)/);
        if (!nameMatch) return;

        const fullName = nameMatch[1].trim();
        const { firstName, lastName } = this.splitName(fullName);

        const phoneMatch = text.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
        const emailEl = $el.find('a[href^="mailto:"]');
        const email = emailEl.length ? emailEl.attr('href').replace('mailto:', '').trim() : '';

        // Look for firm name after the person name
        let firm = '';
        const afterName = text.substring(nameMatch[0].length).trim();
        if (afterName && !afterName.match(/^\(?\d/)) {
          const firmMatch = afterName.match(/^[,\s-]*([A-Z][^,\n\d]+)/);
          if (firmMatch) firm = firmMatch[1].trim();
        }

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: firm,
          city: 'Whitehorse',
          state: 'CA-YT',
          phone: phoneMatch ? phoneMatch[1] : '',
          email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Async generator that yields attorney records from the Yukon Law Society.
   * Tries WP REST API first, falls back to HTML directory page scraping.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    let allLawyers = [];

    // Strategy 1: Try WP REST API for custom post types
    log.scrape(`Attempting WP REST API at ${this.wpApiUrl}`);

    const wpEndpoints = [
      `${this.wpApiUrl}/lawyer?per_page=100`,
      `${this.wpApiUrl}/member?per_page=100`,
      `${this.wpApiUrl}/directory?per_page=100`,
      `${this.wpApiUrl}/pages?per_page=100&search=lawyer`,
    ];

    for (const endpoint of wpEndpoints) {
      try {
        await rateLimiter.wait();
        const response = await this.httpGet(endpoint, rateLimiter);

        if (response.statusCode === 200) {
          const data = JSON.parse(response.body);
          if (Array.isArray(data) && data.length > 0) {
            log.success(`WP REST API returned ${data.length} records from ${endpoint}`);

            for (const rec of data) {
              const title = (rec.title?.rendered || rec.title || '').replace(/<[^>]+>/g, '').trim();
              if (!title || title.length < 3) continue;

              const content = (rec.content?.rendered || '').replace(/<[^>]+>/g, ' ').trim();
              const { firstName, lastName } = this.splitName(title);

              const phoneMatch = content.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
              const emailMatch = content.match(/[\w.+-]+@[\w.-]+\.\w+/);

              allLawyers.push({
                first_name: firstName,
                last_name: lastName,
                full_name: title,
                firm_name: '',
                city: 'Whitehorse',
                state: 'CA-YT',
                phone: phoneMatch ? phoneMatch[1] : '',
                email: emailMatch ? emailMatch[0] : '',
                website: '',
                bar_number: '',
                bar_status: 'Active',
                profile_url: rec.link || '',
              });
            }
            break; // Found data, stop trying endpoints
          }
        }
        rateLimiter.resetBackoff();
      } catch (err) {
        log.info(`WP API endpoint ${endpoint} failed: ${err.message}`);
      }
    }

    // Strategy 2: Fall back to HTML scraping
    if (allLawyers.length === 0) {
      log.info(`WP REST API did not return results — falling back to HTML scraping`);

      const directoryUrls = [
        `${this.baseUrl}${this.directoryPath}`,
        `${this.baseUrl}/lawyer-directory`,
        `${this.baseUrl}/member-directory`,
        `${this.baseUrl}/directory`,
      ];

      for (const dirUrl of directoryUrls) {
        try {
          await rateLimiter.wait();
          const response = await this.httpGet(dirUrl, rateLimiter);

          if (response.statusCode === 200) {
            if (this.detectCaptcha(response.body)) {
              log.warn(`CAPTCHA detected at ${dirUrl}`);
              yield { _captcha: true, page: 1 };
              continue;
            }

            const lawyers = this.parseLawyersFromHtml(response.body);
            if (lawyers.length > 0) {
              log.success(`Parsed ${lawyers.length} lawyers from ${dirUrl}`);
              allLawyers = lawyers;
              break;
            }
          }
          rateLimiter.resetBackoff();
        } catch (err) {
          log.info(`Directory URL ${dirUrl} failed: ${err.message}`);
        }
      }
    }

    if (allLawyers.length === 0) {
      log.warn(`No lawyers found from Yukon Law Society — site structure may have changed`);
      return;
    }

    // Filter and yield by city
    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Filtering: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      const cityLower = city.toLowerCase();
      let cityCount = 0;

      for (const attorney of allLawyers) {
        const recCity = (attorney.city || '').toLowerCase();
        if (recCity && recCity !== cityLower && !recCity.includes(cityLower)) {
          continue;
        }

        cityCount++;
        yield this.transformResult(attorney, practiceArea);

        if (options.maxPages && cityCount >= options.maxPages * this.pageSize) {
          log.info(`Reached max results limit for ${city}`);
          break;
        }
      }

      if (cityCount === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      } else {
        log.success(`Found ${cityCount} lawyers in ${city}`);
      }
    }
  }
}

module.exports = new YukonScraper();
