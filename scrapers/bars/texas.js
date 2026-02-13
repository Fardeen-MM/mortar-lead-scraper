/**
 * Texas Bar Association Scraper
 *
 * Source: https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer
 * Method: HTTP POST (ColdFusion form) + Cheerio for HTML parsing
 * Search form: SearchForm_Public with POST to Result_form_client.cfm
 * Detail page: MemberDirectoryDetail.cfm?ContactID={id}
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class TexasScraper extends BaseScraper {
  constructor() {
    super({
      name: 'texas',
      stateCode: 'TX',
      baseUrl: 'https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer&Template=/CustomSource/MemberDirectory/Result_form_client.cfm',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':        'Administrative',
        'bankruptcy':            'Bankruptcy',
        'business':              'Business Organizations',
        'civil litigation':      'Civil Trial',
        'commercial':            'Commercial',
        'construction':          'Construction',
        'consumer':              'Consumer',
        'corporate':             'Corporate',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'elder':                 'Elder',
        'employment':            'Employment and Labor',
        'labor':                 'Employment and Labor',
        'environmental':         'Environmental',
        'estate planning':       'Estate Planning and Probate',
        'estate':                'Estate Planning and Probate',
        'family':                'Family',
        'family law':            'Family',
        'health':                'Health',
        'immigration':           'Immigration',
        'insurance':             'Insurance',
        'intellectual property': 'Intellectual Property',
        'international':         'International',
        'juvenile':              'Juvenile',
        'oil and gas':           'Oil, Gas, and Mineral',
        'personal injury':       'Personal Injury',
        'real estate':           'Real Estate',
        'tax':                   'Tax',
        'tax law':               'Tax',
        'workers comp':          'Workers Compensation',
      },
      defaultCities: [
        'Houston', 'Dallas', 'San Antonio', 'Austin', 'Fort Worth',
        'El Paso', 'Plano', 'Arlington', 'Corpus Christi', 'Lubbock',
        'McAllen', 'Amarillo', 'Beaumont', 'Midland', 'Tyler',
      ],
    });

    this.detailBaseUrl = 'https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer&template=/Customsource/MemberDirectory/MemberDirectoryDetail.cfm&ContactID=';
  }

  /**
   * HTTP POST with URL-encoded form data.
   * Mirrors the pattern of BaseScraper.httpGet but uses POST method.
   */
  httpPost(url, formData, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
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
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postBody),
          'Connection': 'keep-alive',
        },
        timeout: 15000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        // Follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          // Follow redirect with GET
          return resolve(this.httpGet(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Parse search results page HTML.
   * Texas Bar results are rendered in HTML table rows.
   */
  parseResultsPage($) {
    const attorneys = [];

    // Look for result rows in the table — each row has Name (linked), City, Bar Card Number
    $('table tr').each((_, row) => {
      const $row = $(row);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      // Look for a link to the detail page
      const nameLink = $row.find('a[href*="ContactID"]');
      if (!nameLink.length) return;

      const fullName = nameLink.text().trim();
      if (!fullName) return;

      const href = nameLink.attr('href') || '';
      const contactIdMatch = href.match(/ContactID=(\d+)/i);
      const contactId = contactIdMatch ? contactIdMatch[1] : '';

      // Extract city and bar card number from table cells
      let city = '';
      let barNumber = '';

      cells.each((i, cell) => {
        const text = $(cell).text().trim();
        // Skip the cell that contains the name link
        if ($(cell).find('a[href*="ContactID"]').length) return;
        // Bar card numbers are typically numeric strings
        if (/^\d{6,10}$/.test(text)) {
          barNumber = text;
        } else if (text && !barNumber && /^[A-Za-z\s.'-]+$/.test(text)) {
          // Likely a city name
          city = text;
        }
      });

      // Split name — Texas Bar typically formats as "Last, First Middle"
      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = (parts[1] || '').split(/\s+/)[0];
      } else {
        const split = this.splitName(fullName);
        firstName = split.firstName;
        lastName = split.lastName;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: '',
        city: city,
        state: 'TX',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: '',
        contact_id: contactId,
        profile_url: contactId ? this.detailBaseUrl + contactId : '',
        source: 'texas_bar',
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the page.
   */
  extractResultCount($) {
    const text = $('body').text();
    // Look for patterns like "Results: 1 - 25 of 1,234" or "X records found"
    const matchOf = text.match(/of\s+([\d,]+)\s+(?:records?|results?|members?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:records?|results?|members?|attorneys?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() entirely since Texas Bar uses POST requests.
   * Async generator that yields attorney records.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for TX — searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);

    for (const city of cities) {
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, TX`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Check max pages limit (--test flag sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build POST form data
        const formData = {
          City: city,
          State: 'Texas',
        };
        if (practiceCode) {
          formData.PracticeArea = practiceCode;
        }
        if (page > 1) {
          formData.PageNum = String(page);
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from texas bar`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            // Even if we can't parse a total, try to get results from the page
            const testAttorneys = this.parseResultsPage($);
            if (testAttorneys.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            // We have results but no total — estimate and continue
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

        // Filter and yield
        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for "Next" page links
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          return text === 'next' || text === 'next >' || text === 'next >>';
        }).length > 0;

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages && !hasNext) {
          log.success(`Completed all pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new TexasScraper();
