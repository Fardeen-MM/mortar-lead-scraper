/**
 * Western Australia Legal Practice Board (LPBWA) Scraper
 *
 * Source: https://www.lpbwa.org.au/lawyer-search (redirected from /practitioner-search)
 * Method: HTTP GET + Cheerio (server-rendered HTML, Bootstrap cards)
 *
 * The LPBWA search accepts firstname and surname parameters and returns
 * 10 results per page. It does not support city-based search, so we
 * override search() to iterate through two-letter surname prefixes
 * (aa, ab, ac, ..., zz) to achieve comprehensive coverage.
 *
 * Each result card contains: full name, certificated status,
 * certificate category, admission date, and primary law practice.
 * There is no total result count displayed, so we paginate until
 * we receive an empty page.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-wa',
      stateCode: 'AU-WA',
      baseUrl: 'https://www.lpbwa.org.au',
      pageSize: 10,
      practiceAreaCodes: {
        'family': 'Family',
        'criminal': 'Criminal',
        'property': 'Property',
        'commercial': 'Commercial',
        'employment': 'Employment',
        'immigration': 'Immigration',
        'litigation': 'Litigation',
      },
      defaultCities: ['Perth', 'Fremantle', 'Mandurah', 'Bunbury'],
    });

    // Two-letter surname prefixes for comprehensive alphabet iteration.
    // Single letters return too many results (hundreds of pages each).
    // Two-letter combos keep each query manageable.
    this.surnamePrefixes = [];
    for (let i = 0; i < 26; i++) {
      for (let j = 0; j < 26; j++) {
        this.surnamePrefixes.push(
          String.fromCharCode(97 + i) + String.fromCharCode(97 + j)
        );
      }
    }
  }

  /**
   * Build the LPBWA practitioner search URL.
   *
   * @param {object} params
   * @param {string} params.surname - Surname search term (prefix)
   * @param {string} [params.firstname] - First name search term
   * @param {number} params.page - Page number (1-based)
   * @returns {string}
   */
  buildSearchUrl({ surname, firstname, page }) {
    const params = new URLSearchParams();
    if (firstname) {
      params.set('firstname', firstname);
    }
    if (surname) {
      params.set('surname', surname);
    }
    params.set('page', String(page || 1));
    return `${this.baseUrl}/lawyer-search?${params.toString()}`;
  }

  /**
   * Parse the LPBWA search results page.
   *
   * Each practitioner is rendered as a Bootstrap card:
   *   <div class="card my-3">
   *     <div class="card-header">
   *       <h3 class="clean"><a href="/slug/id"><i class="fa-solid fa-user"></i> Full Name</a></h3>
   *     </div>
   *     <div class="card-body">
   *       <dl class="horizontal">
   *         <dt>Certificated</dt><dd>Yes</dd>
   *         <dt>Certificate Category</dt><dd>Employee of a law practice</dd>
   *         <dt>Admission Date</dt><dd>04/11/2016</dd>
   *         <dt>Primary Law Practice</dt><dd>The Defence Lawyers</dd>
   *       </dl>
   *     </div>
   *   </div>
   */
  parseResultsPage($) {
    const attorneys = [];

    $('div.card.my-3').each((_, el) => {
      const $card = $(el);

      // Extract name and profile URL from the card header
      const nameLink = $card.find('.card-header h3.clean a');
      if (!nameLink.length) return;

      const fullName = nameLink.text().trim();
      if (!fullName || fullName.length < 2) return;

      const profilePath = nameLink.attr('href') || '';
      const profileUrl = profilePath.startsWith('http')
        ? profilePath
        : `${this.baseUrl}${profilePath}`;

      // Extract bar number from the profile URL path (e.g., /slug/813033)
      const barNumMatch = profilePath.match(/\/(\d+)$/);
      const barNumber = barNumMatch ? barNumMatch[1] : '';

      // Parse the definition list fields
      const fields = {};
      $card.find('dl.horizontal dt').each((_, dt) => {
        const key = $(dt).text().trim().toLowerCase();
        const value = $(dt).next('dd').text().trim();
        if (key && value) {
          fields[key] = value;
        }
      });

      const certificated = fields['certificated'] || '';
      const certificateCategory = fields['certificate category'] || '';
      const admissionDate = fields['admission date'] || fields['admission date in wa'] || '';
      const firmName = fields['primary law practice'] || '';

      // Determine bar status from certificated + category
      let barStatus = '';
      if (certificated.toLowerCase() === 'yes') {
        barStatus = certificateCategory ? `Certificated - ${certificateCategory}` : 'Certificated';
      } else if (certificated.toLowerCase() === 'no') {
        barStatus = 'Not Certificated';
      }

      // Split full name into first and last
      // Names are in "First Middle ... Last" format
      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName === 'Not Practising' ? '' : firmName,
        city: '',
        state: 'AU-WA',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        bar_status: barStatus,
        admission_date: admissionDate,
        certificate_category: certificateCategory,
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  /**
   * Parse a WA practitioner profile/detail page for additional fields.
   *
   * Profile pages live at /{slug}/{id} and contain:
   *   - Full name (with middle names) in .card-header h3.clean
   *   - Definition list (dl.horizontal) with:
   *     - Certificated: Yes/No
   *     - Certificate Category: e.g. "Employee of a law practice"
   *     - On the local (WA) roll of practitioners: Yes/No
   *     - Admission date in WA: DD/MM/YYYY
   *     - Primary Law Practice: firm name or "Not Practising"
   *   - Conditions section with restriction codes:
   *     - UNRESTRICTED, PMC (Practice Management Course),
   *       NOTRUST (no trust money), TRUSTAU (authorised for trust), etc.
   *     - Tooltip text on condition <span> elements gives full description.
   *
   * The search results already have most of the dl fields, but the profile
   * adds: local roll status, conditions/restrictions with descriptions.
   * There is no email, phone, address, or website on WA profile pages.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the profile page
   * @returns {object} Additional fields from the profile
   */
  parseProfilePage($) {
    const result = {};
    const detailPage = $('.container.practitioner-detail-page');
    if (!detailPage.length) return result;

    // --- Full name (may include middle names) ---
    const nameEl = detailPage.find('.card-header h3.clean');
    if (nameEl.length) {
      const fullName = nameEl.text().trim();
      if (fullName) {
        result.full_name = fullName;
        const { firstName, lastName } = this.splitName(fullName);
        if (firstName) result.first_name = firstName;
        if (lastName) result.last_name = lastName;
      }
    }

    // --- Definition list fields ---
    const fields = {};
    detailPage.find('dl.horizontal dt').each((_, el) => {
      const key = $(el).text().trim().replace(/:$/, '').toLowerCase();
      const dd = $(el).next('dd');
      if (dd.length && key && key.length > 1) {
        const value = dd.text().trim();
        if (value && value.length > 0) {
          fields[key] = value;
        }
      }
    });

    // Certificated status + category -> bar_status
    const certificated = fields['certificated'] || '';
    const certificateCategory = fields['certificate category'] || '';
    if (certificated.toLowerCase() === 'yes') {
      result.bar_status = certificateCategory
        ? `Certificated - ${certificateCategory}`
        : 'Certificated';
    } else if (certificated.toLowerCase() === 'no') {
      result.bar_status = 'Not Certificated';
    }

    if (certificateCategory) {
      result.certificate_category = certificateCategory;
    }

    // Local roll status
    const onLocalRoll = fields['on the local (wa) roll of practitioners'] || '';
    if (onLocalRoll) {
      result.local_roll = onLocalRoll;
    }

    // Admission date
    const admissionDate = fields['admission date in wa'] || fields['admission date'] || '';
    if (admissionDate) {
      result.admission_date = admissionDate;
    }

    // Firm name
    const firmName = fields['primary law practice'] || '';
    if (firmName && firmName !== 'Not Practising') {
      result.firm_name = firmName;
    }

    // --- Conditions / restrictions ---
    // Extract short condition codes (e.g., UNRESTRICTED, PMC, NOTRUST, TRUSTAU).
    // The tooltip text on <span> elements has full descriptions, but we store
    // just the codes for brevity in CSV output.
    const conditions = [];
    detailPage.find('.col-md-6 ul li').each((_, el) => {
      const $li = $(el);
      const $span = $li.find('span[data-bs-title]');
      const code = ($span.length ? $span.text() : $li.text()).trim();
      if (code) {
        conditions.push(code);
      }
    });

    if (conditions.length > 0) {
      result.conditions = conditions.join(' | ');
    }

    return result;
  }

  /**
   * Extract total result count from the page.
   *
   * The LPBWA search does not display a total count. We estimate it from
   * the highest page number in the pagination links.
   */
  extractResultCount($) {
    let maxPage = 1;

    // Pagination links use onclick="renderPageData('N')" pattern
    $('ul.pagination .page-link').each((_, el) => {
      const onclick = $(el).attr('onclick') || '';
      const match = onclick.match(/renderPageData\('(\d+)'\)/);
      if (match) {
        const pageNum = parseInt(match[1], 10);
        if (pageNum > maxPage) maxPage = pageNum;
      }

      // Also check active page text
      const text = $(el).text().trim();
      const num = parseInt(text, 10);
      if (!isNaN(num) && num > maxPage) maxPage = num;
    });

    // Estimate total results from max visible page number
    // This is an underestimate since pagination shows limited pages,
    // but it gives a useful lower bound
    return maxPage * this.pageSize;
  }

  /**
   * Override search() to iterate through two-letter surname prefixes
   * instead of cities, since LPBWA only supports name-based search.
   *
   * Strategy: iterate through 'aa', 'ab', ..., 'zz' (676 combos).
   * For each prefix, paginate through all result pages.
   * Deduplication is handled downstream by the pipeline.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    const allPrefixes = this.surnamePrefixes;
    const prefixes = options.maxPrefixes
      ? allPrefixes.slice(0, options.maxPrefixes)
      : allPrefixes;
    const totalPrefixes = prefixes.length;
    const seen = new Set(); // Track bar numbers to avoid duplicates within a run

    log.scrape(`AU-WA: Starting LPBWA surname prefix search (${totalPrefixes} prefixes)`);

    for (let pi = 0; pi < totalPrefixes; pi++) {
      const prefix = prefixes[pi];

      // Emit progress
      yield { _cityProgress: { current: pi + 1, total: totalPrefixes } };

      if (pi % 26 === 0) {
        log.info(`AU-WA: Progress — prefix "${prefix}" (${pi + 1}/${totalPrefixes})`);
      }

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        // Respect max pages limit (e.g., --test sets this to 2)
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for prefix "${prefix}"`);
          break;
        }

        const url = this.buildSearchUrl({ surname: prefix, page });

        if (page === 1) {
          log.info(`Searching surname prefix "${prefix}" — ${url}`);
        }

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for prefix "${prefix}" page ${page}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from LPBWA for prefix "${prefix}"`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for prefix "${prefix}" — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for prefix "${prefix}" page ${page} — skipping`);
          yield { _captcha: true, city: `prefix:${prefix}`, page };
          break;
        }

        const $ = cheerio.load(response.body);
        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (page === 1) {
            // No results at all for this prefix — move on
            break;
          }
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.info(`No more results for prefix "${prefix}" after page ${page}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Yield results, deduplicating by bar number
        let newCount = 0;
        for (const attorney of attorneys) {
          const dedupKey = attorney.bar_number || attorney.full_name;
          if (seen.has(dedupKey)) continue;
          seen.add(dedupKey);

          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          yield this.transformResult(attorney, practiceArea);
          newCount++;
        }

        if (page === 1 && newCount > 0) {
          log.success(`Found results for prefix "${prefix}" — page 1 yielded ${newCount} new records`);
        }

        // If we got fewer results than a full page, this is the last page
        if (attorneys.length < this.pageSize) {
          break;
        }

        page++;
        pagesFetched++;
      }
    }

    log.success(`AU-WA: LPBWA search complete — ${seen.size} unique practitioners found`);
  }
}

module.exports = new WaScraper();
