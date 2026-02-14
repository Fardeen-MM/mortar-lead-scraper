/**
 * Solicitors Regulation Authority (SRA) — England & Wales Scraper
 *
 * Source: https://www.sra.org.uk/consumers/register/
 * API: SRA public consumer register with AJAX endpoint at /consumers/register/setfilter
 * Developer portal: https://sra-prod-apim.developer.azure-api.net/
 * Method: HTTP POST to AJAX filter endpoint + JSON/HTML parsing
 *
 * The SRA regulates solicitors in England and Wales. Their consumer register
 * provides firm/organisation data including SRA number, office name, address,
 * email, telephone, website, areas of law, and licence status.
 *
 * Overrides search() to POST filter parameters to the SRA AJAX endpoint
 * and parse the response for firm/solicitor data.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class SRAEnglandWalesScraper extends BaseScraper {
  constructor() {
    super({
      name: 'sra-england-wales',
      stateCode: 'UK-EW',
      baseUrl: 'https://www.sra.org.uk/consumers/register/',
      pageSize: 10,
      practiceAreaCodes: {
        'commercial':               'commercial',
        'corporate':                'corporate',
        'employment':               'employment',
        'family':                   'family',
        'immigration':              'immigration',
        'personal injury':          'personal injury',
        'property/conveyancing':    'property/conveyancing',
        'property':                 'property/conveyancing',
        'conveyancing':             'property/conveyancing',
        'criminal':                 'criminal',
        'dispute resolution':       'dispute resolution',
        'tax':                      'tax',
        'intellectual property':    'intellectual property',
        'banking/finance':          'banking/finance',
        'banking':                  'banking/finance',
        'finance':                  'banking/finance',
        'wills/probate':            'wills/probate',
        'wills':                    'wills/probate',
        'probate':                  'wills/probate',
        'clinical negligence':      'clinical negligence',
        'human rights':             'human rights',
      },
      defaultCities: [
        'London', 'Manchester', 'Birmingham', 'Leeds', 'Bristol',
        'Liverpool', 'Sheffield', 'Newcastle', 'Nottingham', 'Cambridge',
        'Oxford', 'Reading', 'Southampton', 'Edinburgh',
      ],
    });

    this.filterUrl = 'https://www.sra.org.uk/consumers/register/setfilter';
  }

  /**
   * Not used -- search() is fully overridden for the SRA AJAX endpoint.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for SRA AJAX endpoint`);
  }

  /**
   * Not used -- search() is fully overridden for the SRA AJAX endpoint.
   */
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for SRA AJAX endpoint`);
  }

  /**
   * Not used -- search() is fully overridden for the SRA AJAX endpoint.
   */
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for SRA AJAX endpoint`);
  }

  /**
   * HTTP POST with JSON or form data for the SRA AJAX endpoint.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/json') {
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
          'Accept': 'application/json,text/html,*/*',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.sra.org.uk',
          'Referer': 'https://www.sra.org.uk/consumers/register/',
          'X-Requested-With': 'XMLHttpRequest',
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
   * Parse firm/solicitor records from the SRA AJAX response.
   * The response may be JSON with an array of results, or HTML fragments
   * containing firm cards. We handle both formats.
   */
  _parseResponse(body, city) {
    const attorneys = [];

    // Try JSON parse first
    let data;
    try {
      data = JSON.parse(body);
    } catch {
      // Not JSON — try HTML parsing
      return this._parseHtmlResults(body, city);
    }

    // JSON response: may have results at top level or nested
    const records = Array.isArray(data)
      ? data
      : (data.results || data.items || data.organisations || data.records || data.data || []);

    if (!Array.isArray(records) || records.length === 0) {
      // If JSON but no results array, check if it contains HTML
      if (data.html || data.content || data.markup) {
        return this._parseHtmlResults(data.html || data.content || data.markup, city);
      }
      return attorneys;
    }

    for (const rec of records) {
      // SRA records are firm/organisation-level, not individual solicitor
      const firmName = (rec.name || rec.organisationName || rec.firmName || rec.title || '').trim();
      const sraNumber = (rec.sraNumber || rec.sraId || rec.id || rec.organisationId || '').toString().trim();
      const status = (rec.status || rec.licenceStatus || rec.regulatoryStatus || '').trim();
      const phone = (rec.phone || rec.telephone || rec.tel || '').trim();
      const email = (rec.email || rec.emailAddress || '').trim();
      const website = (rec.website || rec.url || rec.webAddress || '').trim();
      const address = rec.address || rec.officeAddress || {};
      const recCity = (typeof address === 'string' ? '' : (address.city || address.town || address.locality || '')).trim();

      // Extract individual contact name if available
      const contactName = (rec.contactName || rec.principalSolicitor || rec.contact || '').trim();
      const { firstName, lastName } = this.splitName(contactName || firmName);

      // Build areas of law string
      const areasOfLaw = rec.areasOfLaw || rec.practiceAreas || rec.areas || [];
      const areasStr = Array.isArray(areasOfLaw) ? areasOfLaw.join(', ') : areasOfLaw.toString();

      attorneys.push({
        first_name: contactName ? firstName : '',
        last_name: contactName ? lastName : '',
        full_name: contactName || firmName,
        firm_name: firmName,
        city: recCity || city,
        state: 'UK-EW',
        phone,
        email,
        website,
        bar_number: sraNumber,
        bar_status: status || 'Authorised',
        profile_url: sraNumber
          ? `https://www.sra.org.uk/consumers/register/organisation/?sraNumber=${sraNumber}`
          : '',
        areas_of_law: areasStr,
      });
    }

    return attorneys;
  }

  /**
   * Fallback HTML parsing for SRA response fragments.
   */
  _parseHtmlResults(html, city) {
    const attorneys = [];
    const $ = cheerio.load(html);

    // SRA register results typically render as card/list items
    const selectors = [
      '.search-result', '.result-item', '.organisation-result',
      '[data-sra-number]', '.register-result', 'article',
      '.card', 'li.result',
    ];

    let $items = $([]);
    for (const sel of selectors) {
      $items = $(sel);
      if ($items.length > 0) break;
    }

    // Fallback: try table rows
    if ($items.length === 0) {
      $items = $('table tbody tr');
    }

    $items.each((_, el) => {
      const $el = $(el);
      const firmName = ($el.find('h2, h3, h4, .name, .firm-name, .title').first().text() || '').trim();
      const sraNumber = ($el.attr('data-sra-number') || '').trim() ||
        ($el.find('[data-sra-number]').attr('data-sra-number') || '').trim();

      // Extract contact details
      const phone = ($el.find('a[href^="tel:"]').attr('href') || '').replace('tel:', '').trim() ||
        ($el.text().match(/(?:Tel|Phone|Telephone):\s*([\d\s+()-]+)/i) || ['', ''])[1].trim();

      const email = ($el.find('a[href^="mailto:"]').attr('href') || '').replace('mailto:', '').trim();
      const website = ($el.find('a[href^="http"]').not('a[href*="sra.org"]').attr('href') || '').trim();

      const profileLink = $el.find('a[href*="register"]').attr('href') || '';
      let profileUrl = '';
      if (profileLink) {
        profileUrl = profileLink.startsWith('http') ? profileLink : `https://www.sra.org.uk${profileLink}`;
      } else if (sraNumber) {
        profileUrl = `https://www.sra.org.uk/consumers/register/organisation/?sraNumber=${sraNumber}`;
      }

      const address = ($el.find('.address, address, .location').text() || '').trim();
      const statusText = ($el.find('.status, .badge, .label').text() || '').trim();

      if (!firmName && !sraNumber) return;

      attorneys.push({
        first_name: '',
        last_name: '',
        full_name: firmName,
        firm_name: firmName,
        city: city,
        state: 'UK-EW',
        phone,
        email,
        website,
        bar_number: sraNumber,
        bar_status: statusText || 'Authorised',
        profile_url: profileUrl,
        address,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from a JSON or HTML response.
   */
  _extractTotalFromResponse(body) {
    // Try JSON
    try {
      const data = JSON.parse(body);
      if (data.totalCount !== undefined) return data.totalCount;
      if (data.total !== undefined) return data.total;
      if (data.count !== undefined) return data.count;
      if (data.resultCount !== undefined) return data.resultCount;
      const results = data.results || data.items || data.organisations || [];
      if (Array.isArray(results)) return results.length;
    } catch {
      // Try HTML
      const $ = cheerio.load(body);
      const text = $('body').text();
      const match = text.match(/([\d,]+)\s+(?:results?|records?|organisations?|firms?)\s+found/i) ||
                    text.match(/(?:Showing|Found)\s+(?:\d+\s*[-–]\s*\d+\s+of\s+)?([\d,]+)/i);
      if (match) return parseInt(match[1].replace(/,/g, ''), 10);
    }
    return 0;
  }

  /**
   * Async generator that yields solicitor/firm records from the SRA register.
   *
   * Strategy:
   *  - POST to the SRA AJAX filter endpoint with city and optional practice area
   *  - Parse JSON or HTML response
   *  - Paginate through results
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
      log.scrape(`Searching: ${practiceArea || 'all'} solicitors in ${city}, ${this.stateCode}`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build filter payload for the SRA AJAX endpoint
        const filterPayload = {
          Location: city,
          Page: page,
          PageSize: this.pageSize,
        };
        if (practiceCode) {
          filterPayload.AreaOfLaw = practiceCode;
        }

        log.info(`Page ${page} — POST ${this.filterUrl} [Location=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.filterUrl, filterPayload, rateLimiter);
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
          log.error(`Unexpected status ${response.statusCode} — skipping city ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        // Get total count on first page
        if (page === 1) {
          totalResults = this._extractTotalFromResponse(response.body);
          if (totalResults === 0) {
            // Still try to parse — the response might have results without a count
            const testResults = this._parseResponse(response.body, city);
            if (testResults.length === 0) {
              log.info(`No results for ${practiceArea || 'all'} in ${city}`);
              break;
            }
            totalResults = testResults.length;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this._parseResponse(response.body, city);

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
          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we have reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new SRAEnglandWalesScraper();
