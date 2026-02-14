/**
 * Alabama State Bar Association Scraper
 *
 * Source: https://members.alabar.org/Meetings/Member_Portal/Member-Search.aspx
 * Method: ASP.NET AJAX with UpdatePanels and Telerik controls
 *
 * Alabama's member directory runs on a Telerik/ASP.NET WebForms stack.
 * Each search requires first GETting the page to extract hidden form tokens
 * (__VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION) then POSTing back
 * with those tokens plus the search criteria. The UpdatePanel pattern means
 * the response contains partial HTML delimited by pipe characters.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class AlabamaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'alabama',
      stateCode: 'AL',
      baseUrl: 'https://members.alabar.org/Meetings/Member_Portal/Member-Search.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'administrative':         'Administrative Law',
        'bankruptcy':             'Bankruptcy & Insolvency',
        'business':               'Business Law',
        'civil litigation':       'Civil Litigation',
        'corporate':              'Corporate Law',
        'criminal':               'Criminal Law',
        'criminal defense':       'Criminal Defense',
        'elder':                  'Elder Law',
        'employment':             'Employment & Labor Law',
        'environmental':          'Environmental Law',
        'estate planning':        'Estate Planning',
        'family':                 'Family Law',
        'family law':             'Family Law',
        'general practice':       'General Practice',
        'immigration':            'Immigration Law',
        'insurance':              'Insurance Law',
        'intellectual property':  'Intellectual Property',
        'labor':                  'Employment & Labor Law',
        'medical malpractice':    'Medical Malpractice',
        'personal injury':        'Personal Injury',
        'probate':                'Probate',
        'real estate':            'Real Estate',
        'tax':                    'Tax Law',
        'tax law':                'Tax Law',
        'workers comp':           'Workers Compensation',
      },
      defaultCities: [
        'Birmingham', 'Montgomery', 'Huntsville', 'Mobile',
        'Tuscaloosa', 'Hoover', 'Dothan', 'Auburn',
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
   * HTTP POST for ASP.NET form submission with ViewState tokens.
   */
  httpPost(url, formData, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
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
          'Content-Type': contentType,
          'Content-Length': Buffer.byteLength(postBody),
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'text/html,application/xhtml+xml,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': this.baseUrl,
          'Origin': 'https://members.alabar.org',
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
   * Extract ASP.NET form tokens from an HTML page.
   * These tokens are required for every postback request.
   */
  extractAspNetTokens(html) {
    const $ = cheerio.load(html);
    return {
      viewState: $('input[name="__VIEWSTATE"]').val() || '',
      viewStateGenerator: $('input[name="__VIEWSTATEGENERATOR"]').val() || '',
      eventValidation: $('input[name="__EVENTVALIDATION"]').val() || '',
      eventTarget: '',
      eventArgument: '',
    };
  }

  /**
   * Extract updated ASP.NET tokens from an UpdatePanel partial response.
   * UpdatePanel responses use pipe-delimited format:
   * length|type|id|content|...
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
   * Extract the HTML content from an UpdatePanel partial response.
   */
  extractHtmlFromPartial(body) {
    // UpdatePanel format: length|updatePanel|panelId|<html content>|...
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
   * Build the ASP.NET postback form data with ViewState tokens.
   */
  buildPostbackData(tokens, city, practiceCode, page) {
    const data = {
      '__VIEWSTATE': tokens.viewState,
      '__VIEWSTATEGENERATOR': tokens.viewStateGenerator,
      '__EVENTVALIDATION': tokens.eventValidation,
      '__EVENTTARGET': 'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ResultsGrid$Page',
      '__EVENTARGUMENT': String(page),
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$txtCity': city,
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlState': 'AL',
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlStatus': 'Active',
      'ctl00_ctl00_TemplateBody_WebPartManager1_gwpciMemberSearch_ciMemberSearch_UpdatePanel1': '',
    };

    if (practiceCode) {
      data['ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlPracticeArea'] = practiceCode;
    }

    return data;
  }

  /**
   * Build the initial search button click postback data.
   */
  buildSearchPostData(tokens, city, practiceCode) {
    const data = {
      '__VIEWSTATE': tokens.viewState,
      '__VIEWSTATEGENERATOR': tokens.viewStateGenerator,
      '__EVENTVALIDATION': tokens.eventValidation,
      '__EVENTTARGET': '',
      '__EVENTARGUMENT': '',
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$txtCity': city,
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlState': 'AL',
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlStatus': 'Active',
      'ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$btnSearch': 'Search',
      'ctl00_ctl00_TemplateBody_WebPartManager1_gwpciMemberSearch_ciMemberSearch_UpdatePanel1': '',
    };

    if (practiceCode) {
      data['ctl00$ctl00$TemplateBody$WebPartManager1$gwpciMemberSearch$ciMemberSearch$ddlPracticeArea'] = practiceCode;
    }

    return data;
  }

  /**
   * Parse attorney data from the results HTML (either full page or UpdatePanel fragment).
   */
  parseResultsHtml(html) {
    const attorneys = [];
    const $ = cheerio.load(html);

    // Look for result grid rows
    $('table.rgMasterTable tr, table[id*="ResultsGrid"] tr, .rgRow, .rgAltRow').each((i, el) => {
      const $row = $(el);
      if ($row.find('th').length > 0) return; // skip header rows
      const cells = $row.find('td');
      if (cells.length < 3) return;

      const nameEl = $row.find('a').first();
      const fullName = nameEl.text().trim() || $(cells[0]).text().trim();
      if (!fullName || /^(name|member|page|first)/i.test(fullName)) return;

      const { firstName, lastName } = this.splitName(fullName);
      const profileLink = nameEl.attr('href') || '';
      const barNumber = profileLink.match(/(?:ID|bar|num)=(\d+)/i)?.[1] || '';

      const firmName = cells.length > 1 ? $(cells[1]).text().trim() : '';
      const cityText = cells.length > 2 ? $(cells[2]).text().trim() : '';
      const phone = cells.length > 3 ? $(cells[3]).text().trim() : '';
      const email = cells.length > 4 ? $(cells[4]).text().trim() : '';

      // Try to extract email from mailto link
      let emailAddr = email;
      const mailLink = $row.find('a[href^="mailto:"]');
      if (mailLink.length) {
        emailAddr = mailLink.attr('href').replace('mailto:', '').split('?')[0];
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: cityText,
        state: 'AL',
        phone: phone.replace(/[^\d()-\s+.]/g, ''),
        email: emailAddr,
        website: '',
        bar_number: barNumber,
        bar_status: 'Active',
        profile_url: profileLink
          ? new URL(profileLink, 'https://members.alabar.org').href
          : '',
      });
    });

    // Fallback: try div-based result cards
    if (attorneys.length === 0) {
      $('.member-result, .search-result, .result-row').each((_, el) => {
        const $card = $(el);
        const nameEl = $card.find('a, .name, .member-name').first();
        const fullName = nameEl.text().trim();
        if (!fullName) return;

        const { firstName, lastName } = this.splitName(fullName);
        const profileLink = nameEl.attr('href') || '';

        let phone = '';
        const telLink = $card.find('a[href^="tel:"]');
        if (telLink.length) phone = telLink.attr('href').replace('tel:', '');

        let emailAddr = '';
        const mailLink = $card.find('a[href^="mailto:"]');
        if (mailLink.length) emailAddr = mailLink.attr('href').replace('mailto:', '').split('?')[0];

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
          firm_name: $card.find('.firm, .company').text().trim(),
          city: $card.find('.city, .location').text().trim(),
          state: 'AL',
          phone: phone,
          email: emailAddr,
          website: '',
          bar_number: '',
          bar_status: 'Active',
          profile_url: profileLink
            ? new URL(profileLink, 'https://members.alabar.org').href
            : '',
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from the response.
   */
  extractCountFromHtml(html) {
    const $ = cheerio.load(html);
    const text = $.text();

    const matchOf = text.match(/(?:of|total)\s*([\d,]+)\s*(?:results?|members?|records?|items?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s*(?:results?|members?|records?)\s*(?:found|returned|matched)/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    // Telerik RadGrid paging - look for page count indicator
    const pageMatch = text.match(/page\s+\d+\s+of\s+(\d+)/i);
    if (pageMatch) return parseInt(pageMatch[1], 10) * this.pageSize;

    return 0;
  }

  /**
   * Override search() for Alabama's ASP.NET AJAX postback pattern.
   * Step 1: GET the search page to extract __VIEWSTATE tokens
   * Step 2: POST with search criteria (city, practice area) + tokens
   * Step 3: Parse results and handle Telerik RadGrid pagination
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

      // Step 2: POST search with city filter
      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      // Initial search POST (clicks the "Search" button)
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

      // Extract results HTML from UpdatePanel response
      let resultsHtml = this.extractHtmlFromPartial(response.body);
      let updatedTokens = this.extractTokensFromPartial(response.body);

      // Merge updated tokens
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

      // Yield first page results
      for (const attorney of attorneys) {
        if (options.minYear && attorney.admission_date) {
          const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
          if (year > 0 && year < options.minYear) continue;
        }
        yield this.transformResult(attorney, practiceArea);
      }

      pagesFetched++;

      // Continue pagination if more pages exist
      if (attorneys.length < this.pageSize) {
        log.success(`Completed all results for ${city}`);
        continue;
      }

      page++;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        try {
          await rateLimiter.wait();
          const pageData = this.buildPostbackData(tokens, city, practiceCode, page);
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

        // Extract from UpdatePanel
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

module.exports = new AlabamaScraper();
