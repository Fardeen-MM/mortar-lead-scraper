/**
 * Washington State Bar Association Scraper
 *
 * Source: https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx
 * Method: Telerik ASP.NET AJAX with UpdatePanels and comprehensive search fields
 *
 * The WSBA Legal Directory is a Personify eBusiness platform running on ASP.NET
 * WebForms with Telerik controls. The search form includes extensive filtering
 * options (name, city, county, practice area, admission date, etc.).
 * Each search requires ViewState extraction and postback simulation.
 * The Telerik RadGrid handles server-side pagination.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class WashingtonScraper extends BaseScraper {
  constructor() {
    super({
      name: 'washington',
      stateCode: 'WA',
      baseUrl: 'https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'animal law':             'Animal Law',
        'antitrust':              'Antitrust, Consumer Protection',
        'appellate':              'Appellate Practice',
        'bankruptcy':             'Creditor Debtor Rights',
        'business':               'Business Law',
        'civil litigation':       'Litigation',
        'civil rights':           'Civil Rights Law',
        'construction':           'Construction Law',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Law',
        'education':              'Education Law',
        'elder':                  'Elder Law',
        'employment':             'Labor & Employment Law',
        'environmental':          'Environmental & Land Use Law',
        'estate planning':        'Trusts & Estates',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'government':             'Government Law',
        'health':                 'Health Law',
        'immigration':            'Immigration Law',
        'indian law':             'Indian Law',
        'insurance':              'Insurance Law',
        'intellectual property':  'Intellectual Property',
        'international':          'International Law',
        'juvenile':               'Juvenile Law',
        'labor':                  'Labor & Employment Law',
        'military':               'Military & Veterans Law',
        'personal injury':        'Tort Law',
        'real estate':            'Real Property, Probate & Trust',
        'solo practice':          'Solo & Small Practice',
        'tax':                    'Taxation',
        'tax law':                'Taxation',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue',
        'Kent', 'Everett', 'Renton', 'Olympia', 'Kirkland',
      ],
    });
  }

  /**
   * Not used directly -- search() is overridden for ASP.NET AJAX postback.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ASP.NET AJAX`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for ASP.NET AJAX`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for ASP.NET AJAX`);
  }

  /**
   * HTTP POST for ASP.NET AJAX form submissions with Telerik headers.
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
          'Accept': 'text/html,application/xhtml+xml,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.baseUrl,
          'Origin': 'https://www.mywsba.org',
          'X-Requested-With': 'XMLHttpRequest',
          'X-MicrosoftAjax': 'Delta=true',
          'Cache-Control': 'no-cache',
        },
        timeout: 20000,
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
      req.setTimeout(20000);
      req.write(postBody);
      req.end();
    });
  }

  /**
   * Extract ASP.NET ViewState and related hidden tokens from full page HTML.
   */
  extractAspNetTokens(html) {
    const $ = cheerio.load(html);
    return {
      viewState: $('input[name="__VIEWSTATE"]').val() || '',
      viewStateGenerator: $('input[name="__VIEWSTATEGENERATOR"]').val() || '',
      eventValidation: $('input[name="__EVENTVALIDATION"]').val() || '',
    };
  }

  /**
   * Extract updated tokens from an UpdatePanel partial postback response.
   */
  extractTokensFromPartial(body) {
    const tokens = {};

    const vsMatch = body.match(/\|__VIEWSTATE\|([^|]*)\|/);
    if (vsMatch) tokens.viewState = vsMatch[1];

    const vsgMatch = body.match(/\|__VIEWSTATEGENERATOR\|([^|]*)\|/);
    if (vsgMatch) tokens.viewStateGenerator = vsgMatch[1];

    const evMatch = body.match(/\|__EVENTVALIDATION\|([^|]*)\|/);
    if (evMatch) tokens.eventValidation = evMatch[1];

    return tokens;
  }

  /**
   * Extract HTML content from UpdatePanel partial response.
   */
  extractHtmlFromPartial(body) {
    const parts = body.split('|');
    let html = '';

    for (let i = 0; i < parts.length - 3; i++) {
      if (parts[i + 1] === 'updatePanel') {
        html += parts[i + 3] || '';
      }
    }

    return html || body;
  }

  /**
   * Build the initial search postback form data.
   * Personify eBusiness uses long control IDs for its WebForms fields.
   */
  buildSearchPostData(tokens, city, practiceCode) {
    const prefix = 'ctl00$ctl00$MainContent$MainContent$';

    const data = {
      '__VIEWSTATE': tokens.viewState,
      '__VIEWSTATEGENERATOR': tokens.viewStateGenerator,
      '__EVENTVALIDATION': tokens.eventValidation,
      '__EVENTTARGET': '',
      '__EVENTARGUMENT': '',
      [`${prefix}txtCity`]: city,
      [`${prefix}ddlState`]: 'WA',
      [`${prefix}ddlMemberStatus`]: 'Active',
      [`${prefix}btnSearch`]: 'Search',
    };

    if (practiceCode) {
      data[`${prefix}ddlPracticeArea`] = practiceCode;
    }

    // Add the ScriptManager field for UpdatePanel
    data[`${prefix}ScriptManager1`] = `${prefix}UpdatePanel1|${prefix}btnSearch`;

    return data;
  }

  /**
   * Build pagination postback data for Telerik RadGrid.
   */
  buildPaginationPostData(tokens, city, practiceCode, page) {
    const prefix = 'ctl00$ctl00$MainContent$MainContent$';

    const data = {
      '__VIEWSTATE': tokens.viewState,
      '__VIEWSTATEGENERATOR': tokens.viewStateGenerator,
      '__EVENTVALIDATION': tokens.eventValidation,
      '__EVENTTARGET': `${prefix}grdResults`,
      '__EVENTARGUMENT': `Page$${page}`,
      [`${prefix}txtCity`]: city,
      [`${prefix}ddlState`]: 'WA',
      [`${prefix}ddlMemberStatus`]: 'Active',
    };

    if (practiceCode) {
      data[`${prefix}ddlPracticeArea`] = practiceCode;
    }

    data[`${prefix}ScriptManager1`] = `${prefix}UpdatePanel1|${prefix}grdResults`;

    return data;
  }

  /**
   * Parse attorney records from the WSBA results HTML.
   * The results are rendered in a Telerik RadGrid table.
   */
  parseResultsHtml(html) {
    const attorneys = [];
    const $ = cheerio.load(html);

    // Telerik RadGrid table rows
    $('tr.rgRow, tr.rgAltRow, table.rgMasterTable tbody tr, table[id*="grdResults"] tbody tr').each((_, el) => {
      const $row = $(el);
      if ($row.find('th').length > 0) return;
      const cells = $row.find('td');
      if (cells.length < 3) return;

      const nameEl = $row.find('a').first();
      const fullName = nameEl.text().trim() || $(cells[0]).text().trim();
      if (!fullName || /^(name|first|last|member)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);
      const profileLink = nameEl.attr('href') || '';

      // Extract bar number from profile link or dedicated column
      const barMatch = profileLink.match(/(?:Bar|ID|Num)=(\d+)/i);
      let barNumber = barMatch ? barMatch[1] : '';
      if (!barNumber) {
        // Look for a bar number column
        for (let i = 0; i < cells.length; i++) {
          const cellText = $(cells[i]).text().trim();
          if (/^\d{4,6}$/.test(cellText)) {
            barNumber = cellText;
            break;
          }
        }
      }

      const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const cityText = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const admissionDate = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const phone = cells.length > 4 ? $(cells[4]).text().trim() : '';

      // Extract phone from tel: links
      let phoneNum = phone;
      const telLink = $row.find('a[href^="tel:"]');
      if (telLink.length) phoneNum = telLink.attr('href').replace('tel:', '');

      // Extract email from mailto links
      let email = '';
      const mailLink = $row.find('a[href^="mailto:"]');
      if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: cityText || city,
        state: 'WA',
        phone: phoneNum.replace(/[^\d()-\s+.]/g, ''),
        email: email,
        website: '',
        bar_number: barNumber,
        bar_status: 'Active',
        admission_date: admissionDate,
        profile_url: profileLink
          ? new URL(profileLink, 'https://www.mywsba.org').href
          : '',
      });
    });

    // Fallback: div-based member cards
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .directory-result, .result-card').each((_, el) => {
        const $card = $(el);
        const nameEl = $card.find('a, .name, .member-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = nameEl.attr('href') || '';

        let phone = '';
        const telLink = $card.find('a[href^="tel:"]');
        if (telLink.length) phone = telLink.attr('href').replace('tel:', '');

        let email = '';
        const mailLink = $card.find('a[href^="mailto:"]');
        if (mailLink.length) email = mailLink.attr('href').replace('mailto:', '').split('?')[0];

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $card.find('.firm, .company').text().trim(),
          city: $card.find('.city, .location').text().trim(),
          state: 'WA',
          phone: phone,
          email: email,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://www.mywsba.org').href
            : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from the HTML response.
   */
  extractCountFromHtml(html) {
    const $ = cheerio.load(html);
    const text = $.text();

    const matchOf = text.match(/(?:of|total[:\s]*)\s*([\d,]+)\s*(?:results?|members?|records?|items?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?|attorneys?)\s*(?:found|returned)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    // Telerik RadGrid page count
    const pageMatch = text.match(/page\s+\d+\s+of\s+(\d+)/i);
    if (pageMatch) return parseInt(pageMatch[1], 10) * this.pageSize;

    return 0;
  }

  /**
   * Override search() for WSBA's Telerik ASP.NET AJAX postback pattern.
   * Step 1: GET the Legal Directory page to extract ViewState tokens
   * Step 2: POST search with city/practice area + tokens
   * Step 3: Parse results, update tokens, continue pagination via RadGrid postbacks
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

      // Step 1: GET the search page to extract ASP.NET tokens
      log.info(`Fetching search page to extract ViewState tokens for ${city}`);
      let tokens;
      try {
        await rateLimiter.wait();
        const pageResponse = await this.httpGet(this.baseUrl, rateLimiter);
        if (pageResponse.statusCode !== 200) {
          log.error(`Failed to load search page: status ${pageResponse.statusCode}`);
          continue;
        }
        tokens = this.extractAspNetTokens(pageResponse.body);
        if (!tokens.viewState) {
          log.error(`Could not extract __VIEWSTATE from search page — skipping ${city}`);
          continue;
        }
      } catch (err) {
        log.error(`Failed to load search page: ${err.message}`);
        continue;
      }

      // Step 2: POST initial search
      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      log.info(`Page 1 — POST ${this.baseUrl} [City=${city}]`);
      let response;
      try {
        await rateLimiter.wait();
        const searchData = this.buildSearchPostData(tokens, city, practiceCode);
        response = await this.httpPost(this.baseUrl, searchData, rateLimiter);
      } catch (err) {
        log.error(`Search request failed for ${city}: ${err.message}`);
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`Got ${response.statusCode} from ${this.name}`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (!shouldRetry) continue;
      }

      if (response.statusCode !== 200) {
        log.error(`Unexpected status ${response.statusCode} for ${city} — skipping`);
        continue;
      }

      rateLimiter.resetBackoff();

      if (this.detectCaptcha(response.body)) {
        log.warn(`CAPTCHA detected for ${city} — skipping`);
        yield { _captcha: true, city, page };
        continue;
      }

      // Parse UpdatePanel response
      let resultsHtml = this.extractHtmlFromPartial(response.body);
      let updatedTokens = this.extractTokensFromPartial(response.body);

      if (updatedTokens.viewState) tokens.viewState = updatedTokens.viewState;
      if (updatedTokens.viewStateGenerator) tokens.viewStateGenerator = updatedTokens.viewStateGenerator;
      if (updatedTokens.eventValidation) tokens.eventValidation = updatedTokens.eventValidation;

      totalResults = this.extractCountFromHtml(resultsHtml);
      let attorneys = this.parseResultsHtml(resultsHtml);

      if (attorneys.length === 0) {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
        continue;
      }

      if (totalResults > 0) {
        log.success(`Found ${totalResults.toLocaleString()} results for ${city}`);
      } else {
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

      pagesFetched++;

      if (attorneys.length < this.pageSize) {
        log.success(`Completed all results for ${city}`);
        continue;
      }

      page++;

      // Step 3: Paginate via RadGrid postbacks
      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        try {
          await rateLimiter.wait();
          const pageData = this.buildPaginationPostData(tokens, city, practiceCode, page);
          response = await this.httpPost(this.baseUrl, pageData, rateLimiter);
        } catch (err) {
          log.error(`Page ${page} request failed: ${err.message}`);
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

        resultsHtml = this.extractHtmlFromPartial(response.body);
        updatedTokens = this.extractTokensFromPartial(response.body);

        if (updatedTokens.viewState) tokens.viewState = updatedTokens.viewState;
        if (updatedTokens.viewStateGenerator) tokens.viewStateGenerator = updatedTokens.viewStateGenerator;
        if (updatedTokens.eventValidation) tokens.eventValidation = updatedTokens.eventValidation;

        attorneys = this.parseResultsHtml(resultsHtml);

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

module.exports = new WashingtonScraper();
