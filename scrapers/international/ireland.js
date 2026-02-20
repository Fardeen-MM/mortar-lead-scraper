/**
 * Law Society of Ireland Scraper
 *
 * Source: https://www.lawsociety.ie/find-a-solicitor/Solicitor-Firm-Search
 * Method: HTTP POST with anti-forgery token + Cheerio HTML parsing
 *
 * The Law Society of Ireland "Find a Solicitor" directory is a server-rendered
 * ASP.NET Core form at /find-a-solicitor/Solicitor-Firm-Search. It requires:
 *   1. A GET request to obtain a __RequestVerificationToken and session cookies
 *   2. A POST with form data including the token, search keyword, county, etc.
 *
 * Results are rendered as <section> elements containing <div class="cardcontainer">
 * blocks with solicitor name, firm, phone, email, website, address, and admission year.
 *
 * Each solicitor has an ID prefixed with "S" (e.g., "S26826") embedded in element IDs.
 *
 * Default page size is 10 results. Pagination uses the "pageNo" form field.
 * County-based filtering via "ddlCounty" provides more targeted results.
 *
 * Overrides search() for POST-based workflow with anti-forgery token management.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class IrelandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'ireland',
      stateCode: 'IE',
      baseUrl: 'https://www.lawsociety.ie',
      pageSize: 10,
      practiceAreaCodes: {
        'commercial':           'Commercial',
        'company':              'Company',
        'construction':         'Construction',
        'employment':           'Employment',
        'family':               'Family',
        'partnership':          'Partnership',
        'personal injuries':    'Personal Injuries',
        'personal injury':      'Personal Injuries',
        'probate':              'Probate',
        'property':             'Property',
        'conveyancing':         'Property',
      },
      defaultCities: ['Dublin', 'Cork', 'Galway', 'Limerick', 'Waterford', 'Kilkenny'],
    });

    this.searchPath = '/find-a-solicitor/Solicitor-Firm-Search';
    this.searchUrl = `${this.baseUrl}${this.searchPath}`;

    // Map city names to their corresponding county values for more precise filtering
    this.cityToCounty = {
      'Dublin':     'Dublin',
      'Cork':       'Cork',
      'Galway':     'Galway',
      'Limerick':   'Limerick',
      'Waterford':  'Waterford',
      'Kilkenny':   'Kilkenny',
      'Wexford':    'Wexford',
      'Kerry':      'Kerry',
      'Tipperary':  'Tipperary',
      'Clare':      'Clare',
      'Wicklow':    'Wicklow',
      'Meath':      'Meath',
      'Kildare':    'Kildare',
      'Louth':      'Louth',
      'Donegal':    'Donegal',
      'Mayo':       'Mayo',
      'Sligo':      'Sligo',
      'Carlow':     'Carlow',
      'Cavan':      'Cavan',
      'Laois':      'Laois',
      'Leitrim':    'Leitrim',
      'Longford':   'Longford',
      'Monaghan':   'Monaghan',
      'Offaly':     'Offaly',
      'Roscommon':  'Roscommon',
      'Westmeath':  'Westmeath',
    };
  }

  /**
   * HTTP POST with URL-encoded form data, cookie forwarding, and redirect following.
   */
  httpPost(url, data, rateLimiter, cookies = '', redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const parsed = new URL(url);
      const postData = typeof data === 'string' ? data : new URLSearchParams(data).toString();
      const ua = rateLimiter.getUserAgent();
      const options = {
        hostname: parsed.hostname,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postData),
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': `${this.baseUrl}${this.searchPath}/`,
          'Origin': this.baseUrl,
        },
        timeout: 30000,
      };

      if (cookies) {
        options.headers['Cookie'] = cookies;
      }

      const proto = parsed.protocol === 'https:' ? https : http;
      const req = proto.request(options, (res) => {
        // Collect any Set-Cookie headers
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          // Follow redirect as GET
          return resolve(this.httpGet(redirect, rateLimiter, redirectCount + 1));
        }

        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body,
          cookies: newCookies,
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(postData);
      req.end();
    });
  }

  /**
   * Fetch the search page via GET to extract anti-forgery token and cookies.
   */
  async fetchToken(rateLimiter) {
    log.info('Fetching anti-forgery token from search page...');
    const response = await this.httpGet(`${this.searchUrl}/`, rateLimiter);

    if (response.statusCode !== 200) {
      throw new Error(`Failed to fetch token page: HTTP ${response.statusCode}`);
    }

    // Extract __RequestVerificationToken from the HTML
    const tokenMatch = response.body.match(
      /__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"/
    );
    if (!tokenMatch) {
      throw new Error('Could not find __RequestVerificationToken in page HTML');
    }

    // Extract cookies from the response (handled by httpGet via headers)
    // The cookies are set by the server and we need them for the POST
    const cookieMatch = response.body.match(/set-cookie/i);

    log.info(`Anti-forgery token obtained (${tokenMatch[1].substring(0, 30)}...)`);
    return {
      token: tokenMatch[1],
      cookies: '', // httpGet doesn't return cookies, but the server accepts without them
    };
  }

  /**
   * Fetch token with cookie extraction using raw https request.
   */
  fetchTokenWithCookies(rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        hostname: 'www.lawsociety.ie',
        path: `${this.searchPath}/`,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 15000,
      };

      const req = https.get(options, (res) => {
        // Handle redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          log.info(`Redirect to ${res.headers.location}, following...`);
          // For simplicity, fall back to httpGet
          return resolve(this.fetchToken(rateLimiter).then(result => ({
            ...result,
            cookies: '',
          })));
        }

        // Collect Set-Cookie headers
        const setCookies = res.headers['set-cookie'] || [];
        const cookies = setCookies.map(c => c.split(';')[0]).join('; ');

        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            return reject(new Error(`Token page returned HTTP ${res.statusCode}`));
          }

          const tokenMatch = body.match(
            /__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"/
          );
          if (!tokenMatch) {
            return reject(new Error('Could not find __RequestVerificationToken in page HTML'));
          }

          log.info(`Token obtained (${tokenMatch[1].substring(0, 30)}...), cookies: ${cookies ? 'yes' : 'none'}`);
          resolve({
            token: tokenMatch[1],
            cookies,
          });
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Token request timed out')); });
    });
  }

  /**
   * Build POST form data for a search request.
   */
  buildFormData({ city, county, page, token, sortBy }) {
    const params = new URLSearchParams();
    params.set('txtSearchNameLocation', city);
    params.set('ddLookingFor', 'Solicitor');
    params.set('ddLocatedIn', 'Ireland');
    params.set('ddlCounty', county || '');
    params.set('ddOrganisationType', 'All organisations');
    params.set('ddFirmOrganisationType', 'All organisations');
    params.set('ddFirmPublicOrganisation', 'All organisations');
    params.set('ddPractisingStatus', '');
    params.set('pageNo', String(page));
    params.set('ddSortBy', sortBy || 'Name');
    params.set('isAdvancedSearchOpen', 'true');
    params.set('isFormSubmitted', 'true');
    params.set('SelectedIncludeFirmsAbroadValue', 'false');
    params.set('SelectedOffersRemoteValue', 'false');
    params.set('__RequestVerificationToken', token);
    return params.toString();
  }

  /**
   * Not used directly -- search() is fully overridden for POST-based workflow.
   */
  buildSearchUrl() {
    return this.searchUrl;
  }

  /**
   * Parse solicitor results from the Law Society of Ireland HTML page.
   *
   * Each solicitor result is in a <section> containing a <div class="cardcontainer">.
   * The card structure:
   *   - Name in <div class="font-body2">
   *   - Firm & location in subsequent <div> children
   *   - Admitted year in div[id^="expandedFieldsAdmittedCounty_"]
   *   - Phone in <a href="tel:...">
   *   - Email in <a href="mailto:...">
   *   - Website in div[id^="expandedFieldsWebsite_"] > a[href]
   *   - Address in div[id^="expandedFieldsAddress_"]
   *   - Fax in div[id^="expandedFieldsFax_"]
   *   - Solicitor ID from element IDs like "PanelExpandSolicitorCard_S26826"
   */
  parseResultsPage($) {
    const attorneys = [];

    // Each result is in a <section> with a .cardcontainer inside
    $('section').each((_, sectionEl) => {
      const $section = $(sectionEl);
      const $card = $section.find('.cardcontainer').first();
      if (!$card.length) return;

      // Extract solicitor ID from expand panel ID
      const expandPanel = $section.find('[id^="PanelExpandSolicitorCard_"]').attr('id') || '';
      const solId = expandPanel.replace('PanelExpandSolicitorCard_', '');
      if (!solId) return; // Not a solicitor card (might be a firm card)

      // Extract name from .font-body2 element
      const fullName = $card.find('.font-body2').first().text().trim();
      if (!fullName || fullName.length < 2) return;

      // Parse name
      const { firstName, lastName } = this.splitName(fullName);

      // Extract firm and location from the second <div> child of cardcontainer
      // Structure: <div><div>Firm Name</div><div>City</div></div>
      const $infoDiv = $card.children('div').eq(1);
      const $firmLocationDiv = $infoDiv.find('.margin--bottom__05rem').last();
      const firmLocationDivs = $firmLocationDiv.children('div');
      let firmName = '';
      let city = '';
      if (firmLocationDivs.length >= 2) {
        firmName = $(firmLocationDivs[0]).text().trim();
        city = $(firmLocationDivs[1]).text().trim();
      } else if (firmLocationDivs.length === 1) {
        // Might be just a city or just a firm name
        const text = $(firmLocationDivs[0]).text().trim();
        // Heuristic: if it looks like a county/city name, treat it as city
        if (this.cityToCounty[text]) {
          city = text;
        } else {
          firmName = text;
        }
      }

      // Extract admission year from the visible admission field
      let admissionDate = '';
      const admissionText = $section.find(`[id^="expandedFieldsAdmittedCounty_"]`).text().trim();
      const admMatch = admissionText.match(/Admitted\s+(\d{4})/i);
      if (admMatch) {
        admissionDate = admMatch[1];
      }

      // Extract phone from tel: link
      let phone = '';
      const $phoneLink = $section.find('a[href^="tel:"]').first();
      if ($phoneLink.length) {
        phone = $phoneLink.text().trim();
      }

      // Extract email from mailto: link
      let email = '';
      const $emailLink = $section.find('a[href^="mailto:"]').first();
      if ($emailLink.length) {
        email = $emailLink.attr('href').replace('mailto:', '').trim().replace(/^\s+/, '');
      }

      // Extract website from expandedFieldsWebsite div
      let website = '';
      const $websiteDiv = $section.find(`[id^="expandedFieldsWebsite_"]`);
      const $websiteLink = $websiteDiv.find('a[href]').first();
      if ($websiteLink.length) {
        website = $websiteLink.attr('href') || '';
      }

      // Extract fax
      let fax = '';
      const $faxDiv = $section.find(`[id^="expandedFieldsFax_"]`);
      if ($faxDiv.length) {
        fax = $faxDiv.find('.cardcontainer--icon__label').text().trim();
      }

      // Extract address
      let address = '';
      const $addressDiv = $section.find(`[id^="expandedFieldsAddress_"]`);
      if ($addressDiv.length) {
        address = $addressDiv.find('.cardcontainer--icon__label').html() || '';
        address = address.replace(/<br\s*\/?>/gi, ', ').replace(/<[^>]+>/g, '').trim();
      }

      // Practising status (if available)
      let barStatus = '';
      const $statusDiv = $section.find(`[id^="expandedFieldsPractisingStatus_"]`);
      if ($statusDiv.length) {
        barStatus = $statusDiv.text().trim();
      }
      if (!barStatus) barStatus = 'Practising';

      // PC year notification
      const $pcNotification = $section.find(`[id^="expandedFieldsPcYearNotification_"]`);
      const pcNotification = $pcNotification.text().trim();
      if (pcNotification && !barStatus) {
        barStatus = pcNotification;
      }

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: city,
        state: 'IE',
        phone,
        email,
        website,
        bar_number: solId,
        bar_status: barStatus,
        profile_url: `https://www.lawsociety.ie/find-a-solicitor/Solicitor-Firm-Search/?solicitorId=${solId}`,
        address,
        fax,
        admission_date: admissionDate,
      });
    });

    return attorneys;
  }

  /**
   * Parse a solicitor profile/detail page for additional fields.
   *
   * The Law Society of Ireland does not have standalone profile pages. All
   * solicitor data is embedded in the search results via expandable card
   * sections. The parseResultsPage() method already extracts: phone, email,
   * website, firm_name, address, fax, admission_date, and bar_status.
   *
   * This parseProfilePage handles the case where we re-fetch a solicitor's
   * data by solicitor ID through the search form. The page structure is the
   * same as a normal search results page.
   *
   * @param {CheerioStatic} $ - Cheerio instance of the page
   * @returns {object} Additional fields from the profile
   */
  parseProfilePage($) {
    const result = {};

    // The page structure is identical to search results.
    // Find the first solicitor card section.
    const $section = $('section').first();
    const $card = $section.find('.cardcontainer').first();
    if (!$card.length) return result;

    // Extract phone from tel: link
    const $phoneLink = $section.find('a[href^="tel:"]').first();
    if ($phoneLink.length) {
      const phone = $phoneLink.text().trim();
      if (phone) result.phone = phone;
    }

    // Extract email from mailto: link
    const $emailLink = $section.find('a[href^="mailto:"]').first();
    if ($emailLink.length) {
      const email = $emailLink.attr('href').replace('mailto:', '').trim();
      if (email) result.email = email;
    }

    // Extract website from expandedFieldsWebsite div
    const $websiteDiv = $section.find('[id^="expandedFieldsWebsite_"]');
    const $websiteLink = $websiteDiv.find('a[href]').first();
    if ($websiteLink.length) {
      const website = $websiteLink.attr('href') || '';
      if (website && !this.isExcludedDomain(website)) result.website = website;
    }

    // Extract firm and location
    const $infoDiv = $card.children('div').eq(1);
    const $firmLocationDiv = $infoDiv.find('.margin--bottom__05rem').last();
    const firmLocationDivs = $firmLocationDiv.children('div');
    if (firmLocationDivs.length >= 2) {
      const firmName = $(firmLocationDivs[0]).text().trim();
      if (firmName) result.firm_name = firmName;
    }

    // Extract address
    const $addressDiv = $section.find('[id^="expandedFieldsAddress_"]');
    if ($addressDiv.length) {
      let address = $addressDiv.find('.cardcontainer--icon__label').html() || '';
      address = address.replace(/<br\s*\/?>/gi, ', ').replace(/<[^>]+>/g, '').trim();
      if (address) result.address = address;
    }

    // Extract fax
    const $faxDiv = $section.find('[id^="expandedFieldsFax_"]');
    if ($faxDiv.length) {
      const fax = $faxDiv.find('.cardcontainer--icon__label').text().trim();
      if (fax) result.fax = fax;
    }

    // Extract admission year
    const admissionText = $section.find('[id^="expandedFieldsAdmittedCounty_"]').text().trim();
    const admMatch = admissionText.match(/Admitted\s+(\d{4})/i);
    if (admMatch) result.admission_date = admMatch[1];

    // Remove empty values
    for (const key of Object.keys(result)) {
      if (!result[key]) delete result[key];
    }

    return result;
  }

  /**
   * Override enrichFromProfile because Ireland has no standalone profile pages.
   * Instead, we re-search the Law Society form using the solicitor's name to
   * retrieve the latest data from the expandable card section.
   *
   * Since the search results already contain full contact details, this is only
   * useful for refreshing stale data or filling gaps in previously scraped records.
   *
   * @param {object} lead - The lead object
   * @param {RateLimiter} rateLimiter - Rate limiter instance
   * @returns {object} Additional fields from the profile
   */
  async enrichFromProfile(lead, rateLimiter) {
    if (!lead.bar_number && !lead.full_name) return {};

    // The Law Society of Ireland does not have direct profile URLs.
    // We perform a targeted search using the solicitor's name to find their card.
    try {
      // First, get a fresh anti-forgery token
      const tokenData = await this.fetchTokenWithCookies(rateLimiter);
      const { token, cookies } = tokenData;

      // Search by the solicitor's name
      const searchName = lead.full_name || `${lead.first_name} ${lead.last_name}`.trim();
      if (!searchName) return {};

      const formData = this.buildFormData({
        city: searchName,
        county: '',
        page: 1,
        token,
        sortBy: 'Name',
      });

      await rateLimiter.wait();
      const response = await this.httpPost(this.searchUrl, formData, rateLimiter, cookies);

      if (response.statusCode !== 200) return {};

      const cheerio = require('cheerio');
      const $ = cheerio.load(response.body);

      // Find the specific solicitor by their bar number (solicitor ID)
      if (lead.bar_number) {
        const $targetSection = $(`[id*="${lead.bar_number}"]`).closest('section');
        if ($targetSection.length) {
          const subHtml = $.html($targetSection);
          const $sub = cheerio.load(subHtml);
          return this.parseProfilePage($sub);
        }
      }

      // Fallback: parse the first result if it's the only one
      return this.parseProfilePage($);
    } catch (err) {
      log.warn(`IE: Failed to enrich profile for ${lead.full_name}: ${err.message}`);
      return {};
    }
  }

  /**
   * Extract total result count from the search results page.
   * The count is in <label id="lblNoOfResults"> as:
   *   "1 - 10 of 8619 result(s) for 'Dublin' in solicitor"
   */
  extractResultCount($) {
    const labelText = $('#lblNoOfResults').text().trim();
    if (!labelText) {
      // Also check totalRecordsBtn in pagination form
      const btnText = $('#totalRecordsBtn').text().trim();
      if (btnText) {
        const match = btnText.match(/of\s+([\d,]+)\s+result/i);
        if (match) return parseInt(match[1].replace(/,/g, ''), 10);
      }
      return 0;
    }

    const match = labelText.match(/of\s+([\d,]+)\s+result/i);
    if (match) return parseInt(match[1].replace(/,/g, ''), 10);

    // Fallback: try any number pattern
    const numMatch = labelText.match(/([\d,]+)\s+result/i);
    if (numMatch) return parseInt(numMatch[1].replace(/,/g, ''), 10);

    return 0;
  }

  /**
   * Async generator that yields solicitor records from the Law Society of Ireland.
   *
   * Strategy:
   *   1. GET the search page to obtain anti-forgery token + cookies
   *   2. POST search form with city name, county filter, and token
   *   3. Parse HTML results from cardcontainer sections
   *   4. Paginate using pageNo form field
   *   5. Repeat for each city
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`Unknown practice area "${practiceArea}" for ${this.stateCode} -- searching without filter`);
      log.info(`Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    const seen = new Set();

    // Fetch initial token and cookies
    let tokenData;
    try {
      tokenData = await this.fetchTokenWithCookies(rateLimiter);
    } catch (err) {
      log.error(`Failed to obtain anti-forgery token: ${err.message}`);
      yield { _captcha: true, city: 'N/A', page: 0 };
      return;
    }

    let { token, cookies } = tokenData;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} solicitors in ${city}, ${this.stateCode}`);

      // Map city to county for filtering
      const county = this.cityToCounty[city] || '';

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;

      while (true) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const formData = this.buildFormData({
          city,
          county,
          page,
          token,
          sortBy: 'Name',
        });

        log.info(`Page ${page} -- POST ${this.searchUrl} [City=${city}, County=${county}]`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.searchUrl, formData, rateLimiter, cookies);
        } catch (err) {
          log.error(`Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from ${this.name}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) {
            // Re-fetch token on block, as session may have expired
            try {
              tokenData = await this.fetchTokenWithCookies(rateLimiter);
              token = tokenData.token;
              cookies = tokenData.cookies;
            } catch (tokenErr) {
              log.error(`Failed to refresh token: ${tokenErr.message}`);
            }
            continue;
          }
          break;
        }

        // Handle 400 Bad Request (likely expired token)
        if (response.statusCode === 400) {
          log.warn('Got 400 -- anti-forgery token may have expired, refreshing...');
          try {
            tokenData = await this.fetchTokenWithCookies(rateLimiter);
            token = tokenData.token;
            cookies = tokenData.cookies;
            continue; // Retry with new token
          } catch (tokenErr) {
            log.error(`Failed to refresh token: ${tokenErr.message}`);
            break;
          }
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} -- skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Update cookies from response if provided
        if (response.cookies) {
          cookies = response.cookies;
        }

        // Extract new token from response page for subsequent requests
        const newTokenMatch = response.body.match(
          /__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"/
        );
        if (newTokenMatch) {
          token = newTokenMatch[1];
        }

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} -- skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            log.info(`No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);

          if (totalResults >= 10000) {
            log.warn(`Result count ${totalResults} may be capped -- consider more specific searches for ${city}`);
          }
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages -- stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const attorney of attorneys) {
          // Deduplicate by solicitor ID
          const key = attorney.bar_number || `${attorney.full_name}|${attorney.firm_name}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);

          if (options.minYear && attorney.admission_date) {
            const year = parseInt((attorney.admission_date.match(/\d{4}/) || ['0'])[0], 10);
            if (year > 0 && year < options.minYear) continue;
          }

          yield this.transformResult(attorney, practiceArea);
        }

        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }

      log.success(`Completed searching ${city} (${seen.size} unique solicitors so far)`);
    }
  }
}

module.exports = new IrelandScraper();
