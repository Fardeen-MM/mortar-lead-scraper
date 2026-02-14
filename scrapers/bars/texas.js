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
          // Drain the response body to free the socket
          res.resume();
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
   * Texas Bar results are rendered as div-based cards with microformat spans,
   * not HTML tables. Each result has span.given-name, span.family-name, etc.
   * Also handles table-based layouts as a fallback.
   */
  parseResultsPage($) {
    const attorneys = [];

    // Primary: Each result card contains a link to MemberDirectoryDetail with ContactID
    $('a[href*="ContactID"]').each((_, el) => {
      const $link = $(el);
      const href = $link.attr('href') || '';
      const contactIdMatch = href.match(/ContactID=(\d+)/i);
      if (!contactIdMatch) return;

      const contactId = contactIdMatch[1];

      // Find the parent result container
      const $parent = $link.closest('h3, .result, div');
      if (!$parent.length) return;

      // Try microformat spans first (in h3 or parent)
      const $h3 = $link.closest('h3');
      let firstName = '';
      let lastName = '';

      if ($h3.length) {
        firstName = $h3.find('span.given-name').text().trim();
        lastName = $h3.find('span.family-name').text().trim();
      }

      // Fallback: extract name from the link text itself
      if (!firstName && !lastName) {
        const linkText = $link.text().trim();
        if (linkText && linkText.length > 2) {
          const parsed = this.splitName(linkText);
          firstName = parsed.firstName;
          lastName = parsed.lastName;
        }
      }

      if (!firstName && !lastName) return;

      // Get the result card container
      const $card = $h3.length ? $h3.parent() : $parent;

      // Extract bar status from status icon
      let barStatus = '';
      const statusContainer = $h3.length ? $h3 : $card;
      const statusIcon = statusContainer.find('span.status-icon');
      if (statusIcon.length) {
        if (statusIcon.hasClass('green')) barStatus = 'Eligible';
        else if (statusIcon.hasClass('red')) barStatus = 'Not Eligible';
        else if (statusIcon.hasClass('yellow')) barStatus = 'Non-Practicing';
        else if (statusIcon.hasClass('aqua')) barStatus = 'Inactive';
        else if (statusIcon.hasClass('blue')) barStatus = 'Deceased';
      }

      // Extract bar card number from card text
      let barNumber = '';
      const cardText = $card.text();
      const barCardMatch = cardText.match(/Bar Card(?:\s+Number)?[:\s]*(\d+)/i);
      if (barCardMatch) barNumber = barCardMatch[1];

      // Extract city from card text
      let city = '';
      const locationMatch = cardText.match(/(?:City|Location)[:\s]*([A-Za-z\s.'-]+?)(?:\s*(?:Bar|Phone|Email|Fax|\n|$))/i);
      if (locationMatch) city = locationMatch[1].trim();

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: `${firstName} ${lastName}`.trim(),
        firm_name: '',
        city: city,
        state: 'TX',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: '',
        bar_status: barStatus,
        contact_id: contactId,
        profile_url: contactId ? this.detailBaseUrl + contactId : '',
        source: 'texas_bar',
      });
    });

    // Fallback: try table-based results if no ContactID links found
    if (attorneys.length === 0) {
      $('table tr').each((i, row) => {
        if (i === 0) return; // skip header
        const $row = $(row);
        if ($row.find('th').length > 0) return;
        const cells = $row.find('td');
        if (cells.length < 2) return;

        const nameCell = $(cells[0]);
        const nameLink = nameCell.find('a');
        const fullName = (nameLink.length ? nameLink.text() : nameCell.text()).trim();
        if (!fullName || fullName.length < 3) return;
        if (/^(name|attorney|search|result|page|home|about|contact)/i.test(fullName)) return;
        if (!fullName.includes(' ') && !fullName.includes(',')) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = nameLink.attr('href') || '';
        const contactIdMatch = profileLink.match(/ContactID=(\d+)/i);

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: cells.length > 1 ? $(cells[1]).text().trim() : '',
          city: cells.length > 2 ? $(cells[2]).text().trim() : '',
          state: 'TX',
          phone: cells.length > 3 ? $(cells[3]).text().trim() : '',
          email: '',
          website: '',
          bar_number: '',
          admission_date: '',
          bar_status: cells.length > 4 ? $(cells[4]).text().trim() : '',
          contact_id: contactIdMatch ? contactIdMatch[1] : '',
          profile_url: contactIdMatch ? this.detailBaseUrl + contactIdMatch[1] : '',
          source: 'texas_bar',
        });
      });
    }

    // Deduplicate by contactId (multiple links per card)
    const seen = new Set();
    return attorneys.filter(a => {
      if (!a.contact_id) return true; // keep entries without contact_id
      if (seen.has(a.contact_id)) return false;
      seen.add(a.contact_id);
      return true;
    });
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

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
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
        // Form field is PPlCityName (not City) and Submitted=1 is required
        const formData = {
          Submitted: '1',
          LastName: '',
          FirstName: '',
          CompanyName: '',
          PPlCityName: city,
          State: 'Texas',
          County: '',
          BarCardNumber: '',
          MaxNumber: String(this.pageSize),
        };
        if (practiceCode) {
          formData.PracticeArea = practiceCode;
        }
        if (page > 1) {
          formData.Start = String((page - 1) * this.pageSize + 1);
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
