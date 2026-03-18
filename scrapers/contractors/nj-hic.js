/**
 * New Jersey Home Improvement Contractor (HIC) Scraper
 *
 * Source: https://newjersey.mylicense.com/verification/Search.aspx?facility=Y
 * Records: ~30K+ Home Improvement Business Contractor registrations
 * Method: ASP.NET WebForms with ViewState, POST search, __doPostBack pagination
 *
 * How it works:
 *   1. GET the search page to obtain ASP.NET session cookie + ViewState + EventValidation
 *   2. POST the search form with Profession="Home Improvement Contractors" and
 *      License Type="Home Improvement Business Contr", filtered by city
 *   3. Follow 302 redirect to SearchResults.aspx
 *   4. Parse the datagrid_results HTML table (40 rows per page)
 *   5. Paginate via __doPostBack('datagrid_results$_ctl44$_ctl{N}','')
 *   6. Optionally fetch detail pages for owner name + address enrichment
 *
 * Search results contain: Full Name, License Number, Profession, License Type,
 *   License Status, City, State
 *
 * Detail pages contain: Name, Doing Business As, Owner Name, Address (street,
 *   city, state, zip), License No, License Status, Status Change Reason,
 *   Issue Date, Expiration Date, SPL
 *
 * Note: Detail pages do NOT contain phone numbers or emails. The only phone
 * on detail pages is the DCA general inquiry number (973) 424-8150.
 *
 * Strategy:
 *   - Search by city to avoid the 30s+ timeout on blank searches
 *   - 40 results per page (ASP.NET DataGrid default)
 *   - Pager shows up to 40 page links at a time with "..." for more
 *   - Detail page enrichment yields owner name, DBA, and full address
 *
 * NJ Cities coverage: 20 major cities cover the bulk of registrations.
 * The site requires at least 2 characters for text fields.
 */

const https = require('https');
const cheerio = require('cheerio');
const querystring = require('querystring');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const SEARCH_URL = 'https://newjersey.mylicense.com/verification/Search.aspx?facility=Y';
const RESULTS_URL = 'https://newjersey.mylicense.com/verification/SearchResults.aspx';
const DETAIL_BASE = 'https://newjersey.mylicense.com/verification/';

const PAGE_SIZE = 40; // ASP.NET DataGrid default rows per page

// Major NJ cities — cover bulk of HIC registrations
const DEFAULT_CITIES = [
  'Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Trenton',
  'Clifton', 'Camden', 'Passaic', 'Union City', 'Bayonne',
  'East Orange', 'Vineland', 'New Brunswick', 'Hoboken', 'Perth Amboy',
  'Plainfield', 'Hackensack', 'Sayreville', 'Kearny', 'Linden',
  'Atlantic City', 'Long Branch', 'Rahway', 'Irvington', 'Woodbridge',
  'Toms River', 'Cherry Hill', 'Edison', 'Lakewood', 'Brick',
];

/**
 * Title-case a string (UPPERCASE -> Title Case).
 * Handles hyphens, apostrophes, and preserves common abbreviations.
 */
function titleCase(str) {
  if (!str) return '';
  let result = str.trim()
    .toLowerCase()
    .replace(/(?:^|[\s\-'/(])(\w)/g, (match) => match.toUpperCase());

  // Fix common abbreviations that should stay uppercase
  result = result.replace(/\bLlc\b/g, 'LLC')
    .replace(/\bInc\b/g, 'Inc.')
    .replace(/\bCorp\b/g, 'Corp.')
    .replace(/\bLlp\b/g, 'LLP')
    .replace(/\bPllc\b/g, 'PLLC')
    .replace(/\bDba\b/g, 'DBA')
    .replace(/\bHvac\b/g, 'HVAC');
  return result;
}

/**
 * Parse owner name field which may contain multiple names separated by commas.
 * Returns { firstName, lastName } for the first/primary owner.
 * Examples:
 *   "Rafael Beltre, Carlos H Paiva, Joao F Paiva" -> { firstName: "Rafael", lastName: "Beltre" }
 *   "John Smith" -> { firstName: "John", lastName: "Smith" }
 */
function parseOwnerName(raw) {
  if (!raw) return { firstName: '', lastName: '' };

  // Take first name in comma-separated list (primary owner)
  const primary = raw.split(',')[0].trim();
  if (!primary) return { firstName: '', lastName: '' };

  const parts = primary.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    return {
      firstName: titleCase(parts[0]),
      lastName: titleCase(parts[parts.length - 1]),
    };
  }
  if (parts.length === 1) {
    return { firstName: '', lastName: titleCase(parts[0]) };
  }
  return { firstName: '', lastName: '' };
}


class NjHicScraper extends BaseScraper {
  constructor() {
    super({
      name: 'nj_hic_contractors',
      stateCode: 'NJ-HIC',
      baseUrl: 'https://newjersey.mylicense.com',
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'home improvement': 'Home Improvement Business Contr',
        'general': 'all',
      },
      defaultCities: DEFAULT_CITIES,
    });
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() not used — search() is overridden`);
  }

  transformResult(lead, practiceArea) {
    lead.source = 'nj_hic_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Suppress CAPTCHA detection — the site includes "captcha" in some JavaScript
   * comments but has no actual CAPTCHA challenge.
   */
  detectCaptcha() {
    return false;
  }

  // --- HTTP helpers ---

  /**
   * HTTPS request helper with cookie support and configurable timeout.
   * Returns { statusCode, body, cookies, location }.
   */
  _httpReq(method, url, postData, cookies, referer) {
    return new Promise((resolve, reject) => {
      const u = new URL(url);
      const opts = {
        hostname: u.hostname,
        path: u.pathname + u.search,
        method,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Cookie': cookies || '',
        },
        timeout: 120000, // 2 minutes — blank searches can take 30s+
      };
      if (referer) opts.headers['Referer'] = referer;
      if (method === 'POST' && postData) {
        opts.headers['Content-Type'] = 'application/x-www-form-urlencoded';
        opts.headers['Content-Length'] = Buffer.byteLength(postData);
      }

      const req = https.request(opts, (res) => {
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0]);
        let data = '';
        res.on('data', c => { data += c; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          newCookies,
          location: res.headers.location,
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error(`Request timed out: ${method} ${url}`));
      });
      if (postData) req.write(postData);
      req.end();
    });
  }

  /**
   * Merge new cookies into existing cookie string.
   */
  _mergeCookies(existing, newOnes) {
    const map = {};
    (existing || '').split('; ').filter(Boolean).forEach(c => {
      const [k] = c.split('=');
      if (k) map[k] = c;
    });
    (newOnes || []).forEach(c => {
      const [k] = c.split('=');
      if (k) map[k] = c;
    });
    return Object.values(map).join('; ');
  }

  // --- ASP.NET session and search ---

  /**
   * Establish a new ASP.NET session by GETting the search page.
   * Returns { cookies, viewState, viewStateGenerator, eventValidation }.
   */
  async _initSession() {
    const res = await this._httpReq('GET', SEARCH_URL, null, '');
    if (res.statusCode !== 200) {
      throw new Error(`Search page returned ${res.statusCode}`);
    }
    const cookies = res.newCookies.join('; ');
    const $ = cheerio.load(res.body);
    return {
      cookies,
      viewState: $('#__VIEWSTATE').val() || '',
      viewStateGenerator: $('#__VIEWSTATEGENERATOR').val() || '',
      eventValidation: $('#__EVENTVALIDATION').val() || '',
    };
  }

  /**
   * Submit the search form and follow the redirect to results.
   * Returns { $, cookies } with a Cheerio instance of the results page,
   * or null if the search fails.
   */
  async _submitSearch(session, city, rateLimiter) {
    const postData = querystring.stringify({
      '__EVENTTARGET': '',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': session.viewState,
      '__VIEWSTATEGENERATOR': session.viewStateGenerator,
      '__EVENTVALIDATION': session.eventValidation,
      't_web_lookup__profession_name': 'Home Improvement Contractors',
      't_web_lookup__license_type_name': 'Home Improvement Business Contr',
      't_web_lookup__full_name': '',
      't_web_lookup__license_no': '',
      't_web_lookup__addr_city': city || '',
      'sch_button': 'Search',
    });

    await rateLimiter.wait();
    const res = await this._httpReq('POST', SEARCH_URL, postData, session.cookies, SEARCH_URL);
    let cookies = this._mergeCookies(session.cookies, res.newCookies);

    // Expect a 302 redirect to SearchResults.aspx
    if (res.statusCode !== 302 || !res.location) {
      // Check if we got results directly (some edge cases)
      if (res.statusCode === 200 && res.body.includes('datagrid_results')) {
        return { $: cheerio.load(res.body), cookies };
      }
      log.warn(`NJ HIC search POST returned ${res.statusCode} (expected 302)`);
      return null;
    }

    // Follow redirect
    await rateLimiter.wait();
    const resultsRes = await this._httpReq('GET', RESULTS_URL, null, cookies, SEARCH_URL);
    cookies = this._mergeCookies(cookies, resultsRes.newCookies);

    if (resultsRes.statusCode !== 200) {
      log.warn(`NJ HIC results page returned ${resultsRes.statusCode}`);
      return null;
    }

    return { $: cheerio.load(resultsRes.body), cookies };
  }

  /**
   * Navigate to a specific page of results via __doPostBack.
   * pageNum is 1-indexed (page 1 is the initial results, page 2+ need postback).
   * The pager control index is (pageNum - 2) since _ctl1 = page 2.
   *
   * Returns { $, cookies } or null on failure.
   */
  async _goToPage(pageNum, $current, cookies, rateLimiter) {
    // Page numbering: page 2 -> _ctl1, page 3 -> _ctl2, etc.
    // When pages > 40, "..." link at _ctl40 advances to next set
    const ctlIndex = pageNum - 2;
    const eventTarget = `datagrid_results$_ctl44$_ctl${ctlIndex}`;

    const viewState = $current('#__VIEWSTATE').val() || '';
    const viewStateGen = $current('#__VIEWSTATEGENERATOR').val() || '';
    const eventValidation = $current('#__EVENTVALIDATION').val() || '';

    const postData = querystring.stringify({
      '__EVENTTARGET': eventTarget,
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': viewState,
      '__VIEWSTATEGENERATOR': viewStateGen,
      '__EVENTVALIDATION': eventValidation,
    });

    await rateLimiter.wait();
    const res = await this._httpReq('POST', RESULTS_URL, postData, cookies, RESULTS_URL);
    const newCookies = this._mergeCookies(cookies, res.newCookies);

    if (res.statusCode !== 200) {
      log.warn(`NJ HIC page ${pageNum} returned ${res.statusCode}`);
      return null;
    }

    return { $: cheerio.load(res.body), cookies: newCookies };
  }

  // --- Results page parsing ---

  /**
   * Parse the datagrid_results table from a results page.
   * Returns an array of { name, licenseNo, profession, licenseType, status, city, state, detailHref }.
   */
  _parseResultsGrid($) {
    const results = [];
    const gridRows = $('#datagrid_results').children('tbody').children('tr');

    gridRows.each((i, row) => {
      // Skip header row (has th elements)
      if ($(row).find('th').length > 0) return;
      // Skip pager row (has td with colspan)
      if ($(row).find('> td[colspan]').length > 0) return;

      const tds = $(row).children('td');
      if (tds.length < 7) return;

      // Name cell contains a nested table with a link
      const nameLink = $(tds[0]).find('a[href*="Details"]');
      const name = nameLink.text().trim();
      const detailHref = nameLink.attr('href') || '';

      // Other cells contain spans
      const licenseNo = $(tds[1]).find('span').text().trim() || $(tds[1]).text().trim();
      const profession = $(tds[2]).find('span').text().trim() || $(tds[2]).text().trim();
      const licenseType = $(tds[3]).find('span').text().trim() || $(tds[3]).text().trim();
      const status = $(tds[4]).find('span').text().trim() || $(tds[4]).text().trim();
      const city = $(tds[5]).find('span').text().trim() || $(tds[5]).text().trim();
      const state = $(tds[6]).find('span').text().trim() || $(tds[6]).text().trim();

      if (name || licenseNo) {
        results.push({ name, licenseNo, profession, licenseType, status, city, state, detailHref });
      }
    });

    return results;
  }

  /**
   * Determine total page count from the pager row.
   * Returns { currentPage, maxVisiblePage, hasMore }.
   */
  _parsePager($) {
    const gridRows = $('#datagrid_results').children('tbody').children('tr');
    const pagerRow = gridRows.last();

    // No pager means single page of results
    if (pagerRow.find('td[colspan]').length === 0) {
      return { currentPage: 1, maxVisiblePage: 1, hasMore: false };
    }

    // Current page is shown as a <span> (non-linked)
    const currentPage = parseInt(pagerRow.find('span').first().text().trim(), 10) || 1;

    // Max visible page from numeric links
    let maxVisiblePage = currentPage;
    pagerRow.find('a').each((i, el) => {
      const text = $(el).text().trim();
      if (/^\d+$/.test(text)) {
        const num = parseInt(text, 10);
        if (num > maxVisiblePage) maxVisiblePage = num;
      }
    });

    // "..." link means there are more pages beyond the visible set
    const hasMore = pagerRow.find('a').filter((i, el) =>
      $(el).text().trim() === '...'
    ).length > 0;

    return { currentPage, maxVisiblePage, hasMore };
  }

  /**
   * Get the page number link's __doPostBack index from the pager.
   * For pages within the current pager window, returns the control index.
   * Returns -1 if the page link is not visible.
   */
  _getPageControlIndex($, targetPage) {
    const gridRows = $('#datagrid_results').children('tbody').children('tr');
    const pagerRow = gridRows.last();
    let controlIndex = -1;

    pagerRow.find('a').each((i, el) => {
      const text = $(el).text().trim();
      const href = $(el).attr('href') || '';

      if (text === String(targetPage)) {
        // Extract control index from href: javascript:__doPostBack('datagrid_results$_ctl44$_ctl{N}','')
        const match = href.match(/_ctl(\d+)/g);
        if (match && match.length >= 2) {
          controlIndex = parseInt(match[1].replace('_ctl', ''), 10);
        }
      }
    });

    return controlIndex;
  }

  // --- Detail page parsing ---

  /**
   * Fetch and parse a detail page for owner name and address info.
   * Returns { ownerName, firstName, lastName, dba, address, city, state, zip,
   *           issueDate, expirationDate, statusReason }.
   */
  async _fetchDetail(detailHref, cookies, rateLimiter) {
    if (!detailHref) return {};

    const url = DETAIL_BASE + detailHref;
    try {
      await rateLimiter.wait();
      const res = await this._httpReq('GET', url, null, cookies, RESULTS_URL);
      if (res.statusCode !== 200) return {};

      const $ = cheerio.load(res.body);

      const ownerName = $('#owner_name').text().trim();
      const dba = $('#doing_business_as').text().trim();
      const addrLine1 = $('#addr_line_1').text().trim();
      const addrCity = $('#addr_city').text().trim();
      const addrState = $('#addr_state').text().trim();
      const addrZip = $('#addr_zipcode').text().trim();
      const issueDate = $('#issue').text().trim();
      const expirationDate = $('#expiration_date').text().trim();
      const statusReason = $('#changeReason').text().trim();

      const { firstName, lastName } = parseOwnerName(ownerName);

      return {
        ownerName,
        firstName,
        lastName,
        dba,
        address: addrLine1,
        city: addrCity || undefined,
        state: addrState || undefined,
        zip: addrZip,
        issueDate,
        expirationDate,
        statusReason,
      };
    } catch (err) {
      log.warn(`NJ HIC detail fetch failed: ${err.message}`);
      return {};
    }
  }

  // --- Main search generator ---

  /**
   * Async generator yielding NJ Home Improvement Contractor leads.
   *
   * Strategy:
   *   1. Iterate through NJ cities (or single city if options.city is set)
   *   2. For each city, submit a search and paginate through results
   *   3. Parse the results grid (40 leads per page)
   *   4. Fetch detail pages for Active contractors to get owner name + address
   *   5. Deduplicate by license number across cities
   *
   * options.maxPages limits pages per city (default: Infinity)
   * options.maxCities limits number of cities (default: all)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Determine cities to search
    const cities = options.city
      ? [options.city]
      : this.defaultCities;
    const citiesToSearch = options.maxCities
      ? cities.slice(0, options.maxCities)
      : cities;

    log.scrape(`NJ Home Improvement Contractor search — ${citiesToSearch.length} cities`);

    // Track seen license numbers to dedup across cities
    const seenLicenses = new Set();
    let totalYielded = 0;
    let totalActive = 0;

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const city = citiesToSearch[ci];
      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
      log.scrape(`Searching NJ HIC in ${city}`);

      // Initialize a fresh session for each city search
      let session;
      try {
        session = await this._initSession();
      } catch (err) {
        log.error(`NJ HIC session init failed: ${err.message}`);
        const shouldRetry = await rateLimiter.handleBlock(0);
        if (shouldRetry) {
          ci--; // Retry same city
          continue;
        }
        break;
      }

      // Submit search
      let result;
      try {
        result = await this._submitSearch(session, city, rateLimiter);
      } catch (err) {
        log.error(`NJ HIC search failed for ${city}: ${err.message}`);
        continue;
      }

      if (!result) {
        log.warn(`NJ HIC: No results page returned for ${city}`);
        continue;
      }

      let { $: $page, cookies } = result;

      // Parse page 1
      let pageNum = 1;
      let pagesFetched = 0;
      let cityYielded = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages (${maxPages}) for ${city}`);
          break;
        }

        const records = this._parseResultsGrid($page);
        const { currentPage, maxVisiblePage, hasMore } = this._parsePager($page);

        if (pageNum === 1) {
          const estimatedPages = hasMore ? '40+' : String(maxVisiblePage);
          log.info(`${city}: page 1 has ${records.length} results, ${estimatedPages} pages`);
        }

        if (records.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2 || pageNum === 1) {
            if (pageNum === 1) log.info(`No results for ${city}`);
            break;
          }
        } else {
          consecutiveEmpty = 0;
        }

        // Process records from this page
        for (const rec of records) {
          // Skip entries with no license number (withdrawn/redacted entries shown as "*")
          if (!rec.licenseNo || rec.licenseNo === '*') continue;

          // Dedup by license number
          if (seenLicenses.has(rec.licenseNo)) continue;
          seenLicenses.add(rec.licenseNo);

          // Build lead object from grid data
          const lead = {
            first_name: '',
            last_name: '',
            firm_name: titleCase(rec.name),
            city: titleCase(rec.city),
            state: rec.state || 'NJ',
            phone: '',
            email: '',
            website: '',
            bar_number: rec.licenseNo,
            bar_status: rec.status,
            license_type: rec.licenseType,
            source: 'nj_hic_contractors',
            practice_area: 'Home Improvement Contractor',
            profile_url: rec.detailHref ? (DETAIL_BASE + rec.detailHref) : '',
          };

          if (rec.status === 'Active') totalActive++;

          yield this.transformResult(lead, practiceArea);
          cityYielded++;
          totalYielded++;
        }

        // Check if there are more pages
        const nextPage = pageNum + 1;
        if (nextPage > maxVisiblePage && !hasMore) {
          log.success(`Completed all ${pageNum} pages for ${city}: ${cityYielded} leads`);
          break;
        }

        // Navigate to next page
        try {
          const nextResult = await this._goToPage(nextPage, $page, cookies, rateLimiter);
          if (!nextResult) {
            log.warn(`Failed to navigate to page ${nextPage} for ${city}`);
            break;
          }
          $page = nextResult.$;
          cookies = nextResult.cookies;
        } catch (err) {
          log.error(`NJ HIC pagination error for ${city} page ${nextPage}: ${err.message}`);
          break;
        }

        pageNum++;
        pagesFetched++;
      }

      if (cityYielded > 0) {
        log.info(`${city}: ${cityYielded} leads scraped`);
      }
    }

    log.success([
      `NJ HIC scrape complete:`,
      `${seenLicenses.size.toLocaleString()} unique licenses`,
      `${totalActive.toLocaleString()} active`,
      `${totalYielded.toLocaleString()} total yielded`,
    ].join(' | '));
  }

  // --- Profile page enrichment ---

  /**
   * Override enrichFromProfile to fetch detail pages for owner name + address.
   * This is called by the waterfall pipeline for each lead that has a profile_url.
   */
  async enrichFromProfile(lead, rateLimiter) {
    if (!lead.profile_url) return {};

    const detailHref = lead.profile_url.replace(DETAIL_BASE, '');
    if (!detailHref) return {};

    // We need cookies from a session to access detail pages
    // Initialize a fresh session if needed
    try {
      const session = await this._initSession();
      const detail = await this._fetchDetail(detailHref, session.cookies, rateLimiter);
      const enriched = {};

      if (detail.firstName) enriched.first_name = detail.firstName;
      if (detail.lastName) enriched.last_name = detail.lastName;
      if (detail.dba) enriched.dba = detail.dba;
      if (detail.address) enriched.address = detail.address;
      if (detail.city) enriched.city = detail.city;
      if (detail.state) enriched.state = detail.state;
      if (detail.zip) enriched.zip = detail.zip;
      if (detail.expirationDate) enriched.license_expiry = detail.expirationDate;
      if (detail.issueDate) enriched.license_created = detail.issueDate;

      return enriched;
    } catch (err) {
      log.warn(`NJ HIC enrichment failed: ${err.message}`);
      return {};
    }
  }

  get hasProfileParser() {
    return true;
  }
}

module.exports = new NjHicScraper();
