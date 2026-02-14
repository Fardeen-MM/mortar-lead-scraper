/**
 * Nova Scotia Barristers' Society Scraper
 *
 * Source: https://members.nsbs.org/LaunchPage.aspx?LoginRedirect=NSBSMemberSearch/Search_Page.aspx
 * Method: ASP.NET/iMIS with ViewState tokens — form POST + Cheerio HTML parsing
 *
 * The NSBS member search is built on the iMIS platform (ASP.NET WebForms).
 * Each request requires __VIEWSTATE and __EVENTVALIDATION tokens extracted from
 * the previous page. Overrides search() for the stateful POST workflow.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NovaScotiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nova-scotia',
      stateCode: 'CA-NS',
      baseUrl: 'https://members.nsbs.org/NSBSMemberSearch/Search_Page.aspx',
      pageSize: 25,
      practiceAreaCodes: {
        'family':                'Family',
        'family law':            'Family',
        'criminal':              'Criminal',
        'criminal defense':      'Criminal',
        'real estate':           'Real Estate',
        'corporate/commercial':  'Corporate Commercial',
        'corporate':             'Corporate Commercial',
        'commercial':            'Corporate Commercial',
        'personal injury':       'Personal Injury',
        'employment':            'Employment',
        'labour':                'Labour',
        'immigration':           'Immigration',
        'estate planning/wills': 'Wills and Estates',
        'estate planning':       'Wills and Estates',
        'wills':                 'Wills and Estates',
        'intellectual property': 'Intellectual Property',
        'civil litigation':      'Civil Litigation',
        'litigation':            'Civil Litigation',
        'tax':                   'Tax',
        'administrative':        'Administrative',
        'environmental':         'Environmental',
        'maritime':              'Maritime',
        'insurance':             'Insurance',
      },
      defaultCities: [
        'Halifax', 'Dartmouth', 'Sydney', 'Truro', 'New Glasgow',
      ],
    });

    this.launchUrl = 'https://members.nsbs.org/LaunchPage.aspx?LoginRedirect=NSBSMemberSearch/Search_Page.aspx';
  }

  /**
   * HTTP POST with URL-encoded form data.
   */
  httpPost(url, data, rateLimiter, contentType = 'application/x-www-form-urlencoded') {
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
          'Accept': 'text/html,application/json,*/*',
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
   * Not used — search() is fully overridden.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ASP.NET workflow`);
  }

  /**
   * Extract ASP.NET ViewState tokens from the page.
   */
  extractViewState($) {
    return {
      viewState: $('input#__VIEWSTATE').val() || '',
      viewStateGenerator: $('input#__VIEWSTATEGENERATOR').val() || '',
      eventValidation: $('input#__EVENTVALIDATION').val() || '',
    };
  }

  /**
   * Parse ASP.NET/iMIS member search results.
   * iMIS typically renders results in a GridView (HTML table with asp:GridView IDs).
   */
  parseResultsPage($) {
    const attorneys = [];

    // iMIS GridView tables — look for typical ASP.NET GridView patterns
    $('table[id*="GridView"] tr, table[id*="grid"] tr, table[id*="results"] tr, .search-results table tr, table.rgMasterTable tr').each((i, el) => {
      const $row = $(el);
      const cells = $row.find('td');
      if (cells.length < 2) return;

      const firstText = $(cells[0]).text().trim();
      if (/^(name|member|last|first|#)$/i.test(firstText)) return;
      if (!firstText || firstText.length < 2) return;

      const nameCell = $(cells[0]);
      const profileLink = nameCell.find('a').attr('href') || '';

      // iMIS layout varies but typically: Name | City | Status
      let fullName = firstText;
      let city = cells.length > 1 ? $(cells[1]).text().trim() : '';
      let status = cells.length > 2 ? $(cells[2]).text().trim() : '';
      let barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';
      let firm = '';
      let phone = '';
      let email = '';

      // Check for phone patterns
      for (let c = 1; c < cells.length; c++) {
        const cellText = $(cells[c]).text().trim();
        if (/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/.test(cellText) && !phone) {
          phone = cellText;
        }
      }

      // Check for email
      const mailtoLink = $row.find('a[href^="mailto:"]');
      if (mailtoLink.length) {
        email = mailtoLink.attr('href').replace('mailto:', '').trim();
      }

      let firstName = '';
      let lastName = '';
      if (fullName.includes(',')) {
        const parts = fullName.split(',').map(s => s.trim());
        lastName = parts[0];
        firstName = parts[1] ? parts[1].split(/\s+/)[0] : '';
      } else {
        const nameParts = this.splitName(fullName);
        firstName = nameParts.firstName;
        lastName = nameParts.lastName;
      }

      const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: displayName,
        firm_name: firm,
        city: city,
        state: 'CA-NS',
        phone,
        email,
        website: '',
        bar_number: barNumber.replace(/^#?\s*/, '').trim(),
        bar_status: status || 'Active',
        profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://members.nsbs.org${profileLink}` : ''),
      });
    });

    // Fallback: general table parsing
    if (attorneys.length === 0) {
      $('table tr').each((i, el) => {
        const $row = $(el);
        const cells = $row.find('td');
        if (cells.length < 2) return;

        const firstText = $(cells[0]).text().trim();
        if (/^(name|member|last|first|#|no)$/i.test(firstText)) return;
        if (!firstText || firstText.length < 2) return;

        const nameCell = $(cells[0]);
        const profileLink = nameCell.find('a').attr('href') || '';

        let fullName = firstText;
        let city = cells.length > 1 ? $(cells[1]).text().trim() : '';
        let status = cells.length > 2 ? $(cells[2]).text().trim() : '';
        let barNumber = cells.length > 3 ? $(cells[3]).text().trim() : '';

        let email = '';
        const mailtoLink = $row.find('a[href^="mailto:"]');
        if (mailtoLink.length) {
          email = mailtoLink.attr('href').replace('mailto:', '').trim();
        }

        let phone = '';
        for (let c = 1; c < cells.length; c++) {
          const cellText = $(cells[c]).text().trim();
          if (/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/.test(cellText) && !phone) {
            phone = cellText;
          }
        }

        // Skip rows that don't look like names
        if (/^\d+$/.test(fullName) || fullName.length > 100) return;

        let firstName = '';
        let lastName = '';
        if (fullName.includes(',')) {
          const parts = fullName.split(',').map(s => s.trim());
          lastName = parts[0];
          firstName = parts[1] ? parts[1].split(/\s+/)[0] : '';
        } else {
          const nameParts = this.splitName(fullName);
          firstName = nameParts.firstName;
          lastName = nameParts.lastName;
        }

        const displayName = fullName.includes(',') ? `${firstName} ${lastName}`.trim() : fullName;

        attorneys.push({
          first_name: firstName,
          last_name: lastName,
          full_name: displayName,
          firm_name: '',
          city: city,
          state: 'CA-NS',
          phone,
          email,
          website: '',
          bar_number: barNumber.replace(/[^0-9A-Za-z]/g, ''),
          bar_status: status || 'Active',
          profile_url: profileLink.startsWith('http') ? profileLink : (profileLink ? `https://members.nsbs.org${profileLink}` : ''),
        });
      });
    }

    return attorneys;
  }

  /**
   * Extract total result count from iMIS page.
   */
  extractResultCount($) {
    const text = $('body').text();

    const matchOf = text.match(/(?:Displaying|Showing|Results?)\s*:?\s*\d+\s*[-–to]+\s*\d+\s+of\s+([\d,]+)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchFound = text.match(/([\d,]+)\s+(?:members?|results?|records?|lawyers?|barristers?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total\s*:?\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Override search() for ASP.NET/iMIS stateful POST workflow.
   * Must first GET the page to obtain ViewState tokens, then POST the search.
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
      log.scrape(`Searching: ${practiceArea || 'all'} lawyers in ${city}, ${this.stateCode}`);

      // Step 1: GET the search page to extract ViewState tokens
      log.info(`Fetching search page for ViewState tokens`);
      let viewState, viewStateGenerator, eventValidation;

      try {
        await rateLimiter.wait();
        const initResponse = await this.httpGet(this.baseUrl, rateLimiter);

        if (initResponse.statusCode !== 200) {
          log.error(`Failed to load search page: status ${initResponse.statusCode}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(initResponse.body)) {
          log.warn(`CAPTCHA detected on search page for ${city} — skipping`);
          yield { _captcha: true, city, page: 0 };
          continue;
        }

        const $init = cheerio.load(initResponse.body);
        const tokens = this.extractViewState($init);
        viewState = tokens.viewState;
        viewStateGenerator = tokens.viewStateGenerator;
        eventValidation = tokens.eventValidation;

        if (!viewState) {
          log.warn(`No ViewState found on search page — ASP.NET form may have changed`);
        }
      } catch (err) {
        log.error(`Failed to load search page: ${err.message}`);
        continue;
      }

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build ASP.NET POST form data with ViewState
        const formData = new URLSearchParams();
        formData.set('__VIEWSTATE', viewState || '');
        if (viewStateGenerator) {
          formData.set('__VIEWSTATEGENERATOR', viewStateGenerator);
        }
        if (eventValidation) {
          formData.set('__EVENTVALIDATION', eventValidation);
        }
        formData.set('ctl00$MainContent$txtCity', city);
        formData.set('ctl00$MainContent$ddlStatus', 'Practising');
        if (practiceCode) {
          formData.set('ctl00$MainContent$ddlPracticeArea', practiceCode);
        }
        if (page === 1) {
          formData.set('ctl00$MainContent$btnSearch', 'Search');
        } else {
          // ASP.NET paging uses __EVENTTARGET for page changes
          formData.set('__EVENTTARGET', 'ctl00$MainContent$GridView1');
          formData.set('__EVENTARGUMENT', `Page$${page}`);
        }

        log.info(`Page ${page} — POST ${this.baseUrl} [City=${city}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData.toString(), rateLimiter);
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

        // Update ViewState for next request
        const newTokens = this.extractViewState($);
        if (newTokens.viewState) viewState = newTokens.viewState;
        if (newTokens.viewStateGenerator) viewStateGenerator = newTokens.viewStateGenerator;
        if (newTokens.eventValidation) eventValidation = newTokens.eventValidation;

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
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        // Check for next page via ASP.NET pager
        const hasNext = $('a').filter((_, el) => {
          const text = $(el).text().trim().toLowerCase();
          const href = $(el).attr('href') || '';
          return text === 'next' || text === '>' || text === '>>' ||
                 href.includes('Page$Next') || href.includes(`Page$${page + 1}`);
        }).length > 0;

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

module.exports = new NovaScotiaScraper();
