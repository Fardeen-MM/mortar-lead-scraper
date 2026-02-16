/**
 * Virginia State Bar (VSB) Scraper
 *
 * Source: https://vsb.org/Shared_Content/Directory/va-lawyer-directory.aspx
 * Method: ASP.NET async postback (UpdatePanel + RadGrid)
 *
 * The VSB uses an iMIS-based ASP.NET WebForms application with Telerik RadGrid.
 * Search is done via __doPostBack async postbacks.
 *
 * Flow:
 * 1. GET the search page to obtain __VIEWSTATE and hidden form fields
 * 2. POST async postback with search params (last name prefix, status=AIGS)
 * 3. Parse the RadGrid table from the delta response
 * 4. Paginate via __doPostBack targets extracted from pager HTML
 *
 * The VSB search does NOT have a city filter — results are searched by last name
 * and filtered by city client-side. This scraper uses two-letter prefixes per city
 * to keep result sets manageable (e.g., "Sm" instead of "S").
 *
 * Grid columns: BarID, Name, MemberType, LicenseType, Status, City, State, Zip,
 * HasDiscipline, SuspensionType, DateOfLicense, SortCode(hidden)
 *
 * Status filter values: AIGS = In Good Standing, X = Former, I = Inactive, etc.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class VirginiaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'virginia',
      stateCode: 'VA',
      baseUrl: 'https://vsb.org/Shared_Content/Directory/va-lawyer-directory.aspx',
      pageSize: 10, // RadGrid default page size
      practiceAreaCodes: {},
      defaultCities: [
        'Richmond', 'Virginia Beach', 'Norfolk', 'Arlington', 'Alexandria',
        'Newport News', 'Chesapeake', 'Hampton', 'Roanoke', 'Fairfax',
        'Charlottesville', 'Lynchburg', 'McLean', 'Tysons',
      ],
    });

    this.formPrefix = 'ctl01$TemplateBody$WebPartManager1$gwpciVirginiaLawyerSearch$ciVirginiaLawyerSearch$ResultsGrid$Sheet0';
    this.listerPanel = 'ctl01$TemplateBody$WebPartManager1$gwpciVirginiaLawyerSearch$ciVirginiaLawyerSearch$ListerPanel';
    this.gridId = 'ctl01$TemplateBody$WebPartManager1$gwpciVirginiaLawyerSearch$ciVirginiaLawyerSearch$ResultsGrid$Grid1';

    // Two-letter prefixes to limit result sets to ~300–1000 results each.
    // Single-letter "S" returns 20,000 results; "Sm" returns ~400.
    this.lastNamePrefixes = [
      'Aa','Ab','Ac','Ad','Af','Ag','Ah','Ai','Ak','Al','Am','An','Ap','Ar','As','At','Au','Av','Aw','Ay','Az',
      'Ba','Be','Bi','Bl','Bo','Br','Bu','By',
      'Ca','Ce','Ch','Ci','Cl','Co','Cr','Cu',
      'Da','De','Di','Do','Dr','Du','Dw',
      'Ea','Ed','Eg','Ei','El','Em','En','Er','Es','Et','Ev',
      'Fa','Fe','Fi','Fl','Fo','Fr','Fu',
      'Ga','Ge','Gi','Gl','Go','Gr','Gu',
      'Ha','He','Hi','Ho','Hu','Hy',
      'Ib','Id','Il','Im','In','Ir','Is','Iv',
      'Ja','Je','Jo','Ju',
      'Ka','Ke','Kh','Ki','Kl','Kn','Ko','Kr','Ku',
      'La','Le','Li','Lo','Lu','Ly',
      'Ma','Mc','Me','Mi','Mo','Mu','My',
      'Na','Ne','Ni','No','Nu',
      'Ob','Oc','Od','Og','Oh','Ol','Om','On','Op','Or','Os','Ot','Ow',
      'Pa','Pe','Ph','Pi','Pl','Po','Pr','Pu',
      'Qu',
      'Ra','Re','Rh','Ri','Ro','Ru','Ry',
      'Sa','Sc','Se','Sh','Si','Sk','Sl','Sm','Sn','So','Sp','St','Su','Sw','Sy',
      'Ta','Te','Th','Ti','To','Tr','Tu','Ty',
      'Ul','Um','Un','Ur',
      'Va','Ve','Vi','Vo',
      'Wa','We','Wh','Wi','Wo','Wr',
      'Ya','Ye','Yi','Yo','Yu',
      'Za','Ze','Zh','Zi','Zo','Zu',
    ];
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for VSB async postback`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for VSB async postback`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for VSB async postback`);
  }

  /**
   * HTTP GET with cookie support.
   */
  _httpGet(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const req = https.get(url, {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Encoding': 'identity',
        },
        timeout: 20000,
      }, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: setCookies }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * POST async postback (delta request) with form data.
   */
  _asyncPostback(formData, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(this.baseUrl);
      const postBody = new URLSearchParams(formData).toString();
      const bodyBuffer = Buffer.from(postBody, 'utf8');

      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': '*/*',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': bodyBuffer.length,
          'X-Requested-With': 'XMLHttpRequest',
          'X-MicrosoftAjax': 'Delta=true',
          'Referer': this.baseUrl,
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        const setCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: [cookies, setCookies].filter(Boolean).join('; '),
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(bodyBuffer);
      req.end();
    });
  }

  /**
   * Extract all hidden form fields from HTML.
   */
  _extractHiddenFields(html) {
    const fields = {};
    const regex = /<input[^>]*type="hidden"[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g;
    let match;
    while ((match = regex.exec(html)) !== null) {
      fields[match[1]] = match[2];
    }
    return fields;
  }

  /**
   * Extract updated __VIEWSTATE from delta response.
   */
  _extractViewstateFromDelta(deltaBody) {
    const vsMatch = deltaBody.match(/\|hiddenField\|__VIEWSTATE\|([^|]*)\|/);
    return vsMatch ? vsMatch[1] : null;
  }

  /**
   * Extract total result count and pages from the delta response info part.
   * Expected format: "<strong>458</strong> items in <strong>46</strong> pages"
   */
  _extractTotalPages(deltaBody) {
    const infoMatch = deltaBody.match(/<strong>(\d+)<\/strong>\s*items?\s*in\s*<strong>(\d+)<\/strong>\s*pages?/i);
    if (infoMatch) {
      return { totalItems: parseInt(infoMatch[1], 10), totalPages: parseInt(infoMatch[2], 10) };
    }
    return { totalItems: 0, totalPages: 0 };
  }

  /**
   * Parse RadGrid rows from delta response body.
   * Returns array of attorney objects.
   *
   * IMPORTANT: cheerio.load() must use the third argument `false` to prevent
   * wrapping in <html><body>, which strips <td> from orphaned <tr> elements.
   */
  _parseRadGridRows(deltaBody) {
    const attorneys = [];

    // Extract RadGrid HTML from delta response
    const rows = deltaBody.match(/<tr[^>]*class="rg(?:Row|AltRow)[^"]*"[^>]*>.*?<\/tr>/gs) || [];

    for (const rowHtml of rows) {
      // CRITICAL: pass `false` as 3rd arg to prevent cheerio from wrapping
      // in <html><body>, which would strip <td> children from orphaned <tr>
      const $ = cheerio.load(rowHtml, null, false);
      const cells = $('td');
      if (cells.length < 8) continue;

      const barNumber = $(cells[0]).text().trim();
      const fullName = $(cells[1]).text().trim();
      const memberType = $(cells[2]).text().trim();
      const licenseType = $(cells[3]).text().trim();
      const status = $(cells[4]).text().trim();
      const city = $(cells[5]).text().trim();
      const state = $(cells[6]).text().trim();
      const zip = $(cells[7]).text().trim();
      const dateOfLicense = cells.length > 10 ? $(cells[10]).text().trim() : '';

      if (!fullName) continue;

      // Parse name — VSB format: "First Last" or "First Middle Last"
      // Names may include comma-separated suffixes like ", III" or ", Jr."
      let firstName = '';
      let lastName = '';
      // Strip comma-separated suffixes first (e.g., "Aaronson, III" -> "Aaronson")
      const cleaned = fullName.replace(/,\s*(Jr\.?|Sr\.?|II|III|IV|V|Esq\.?)\s*$/i, '').trim();
      const nameParts = cleaned.split(/\s+/).filter(Boolean);
      if (nameParts.length >= 2) {
        firstName = nameParts[0];
        lastName = nameParts[nameParts.length - 1];
        // Handle remaining suffixes without commas (e.g., "John Smith Jr.")
        const suffixes = ['Jr', 'Jr.', 'Sr', 'Sr.', 'II', 'III', 'IV', 'V', 'Esq', 'Esq.'];
        if (suffixes.includes(lastName) && nameParts.length >= 3) {
          lastName = nameParts[nameParts.length - 2];
        }
      } else if (nameParts.length === 1) {
        lastName = nameParts[0];
      }
      // Clean any remaining trailing punctuation
      lastName = lastName.replace(/,\s*$/, '');

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        firm_name: '',
        city: city,
        state: state || 'VA',
        phone: '',
        email: '',
        website: '',
        bar_number: barNumber,
        admission_date: dateOfLicense,
        bar_status: status,
        member_type: memberType,
        license_type: licenseType,
        source: `${this.name}_bar`,
      });
    }

    return attorneys;
  }

  /**
   * Extract __doPostBack pager link targets from the delta response.
   * The HTML uses &#39; for single quotes in href attributes.
   * Returns an array of { page, target } objects.
   */
  _extractPagerTargets(deltaBody) {
    const targets = [];
    // Unescape HTML entities so we can parse __doPostBack targets
    const unescaped = deltaBody.replace(/&#39;/g, "'");
    const pagerRegex = /title="Go to Page (\d+)"[^>]*href="javascript:__doPostBack\('([^']+)'/g;
    let match;
    while ((match = pagerRegex.exec(unescaped)) !== null) {
      targets.push({ page: parseInt(match[1], 10), target: match[2] });
    }
    // Also capture "Next Pages" link (shows "...")
    const nextMatch = unescaped.match(/title="Next Pages"[^>]*href="javascript:__doPostBack\('([^']+)'/);
    if (nextMatch) {
      const lastPage = targets.length > 0 ? targets[targets.length - 1].page : 0;
      targets.push({ page: lastPage + 1, target: nextMatch[1], isNextBatch: true });
    }
    return targets;
  }

  /**
   * Async generator that yields attorney records from the VSB directory.
   * Iterates two-letter last-name prefixes for each city since the search
   * has no city filter. Results are filtered client-side by city name.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);

    if (practiceArea) {
      log.warn(`Virginia bar search does not support practice area filtering — searching all attorneys`);
    }

    // Track seen bar numbers across all cities to deduplicate
    const seenBarNumbers = new Set();

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      let totalForCity = 0;
      const cityLower = city.toLowerCase();

      // In smoke-test mode (maxPages set), use single-letter prefixes with more
      // pages per prefix, and skip city filtering (VSB has no server-side city
      // filter, so filtering client-side with few pages yields almost nothing).
      // The maxPrefixes option limits how many prefixes are tried in test mode.
      let prefixes;
      let maxPagesPerPrefix;
      const isTestMode = !!options.maxPages;
      if (isTestMode) {
        const testPrefixes = ['A','B','C','D','S','M'];
        const maxPfx = options.maxPrefixes || testPrefixes.length;
        prefixes = testPrefixes.slice(0, maxPfx);
        // Allow more pages per prefix in test mode since we only run a few prefixes
        maxPagesPerPrefix = options.maxPages || 30;
      } else {
        prefixes = this.lastNamePrefixes;
        maxPagesPerPrefix = 30;
      }

      for (const prefix of prefixes) {
        // Step 1: GET the search page (fresh session per prefix to avoid stale viewstate)
        let pageResponse;
        try {
          await rateLimiter.wait();
          pageResponse = await this._httpGet(this.baseUrl, rateLimiter);
        } catch (err) {
          log.error(`Failed to load search page: ${err.message}`);
          continue;
        }

        if (pageResponse.statusCode !== 200) {
          log.error(`Search page returned ${pageResponse.statusCode}`);
          continue;
        }

        const sessionCookies = pageResponse.cookies;
        const hiddenFields = this._extractHiddenFields(pageResponse.body);

        if (!hiddenFields.__VIEWSTATE) {
          log.error(`Could not extract __VIEWSTATE`);
          continue;
        }

        // Step 2: POST async postback with search params
        const submitBtn = `${this.formPrefix}$SubmitButton`;
        const form = { ...hiddenFields };
        form['__EVENTTARGET'] = submitBtn;
        form['__EVENTARGUMENT'] = '';
        form[`${this.formPrefix}$Input0$TextBox1`] = ''; // Bar ID#
        form[`${this.formPrefix}$Input1$TextBox1`] = ''; // First Name
        form[`${this.formPrefix}$Input2$TextBox1`] = prefix; // Last Name contains
        form[`${this.formPrefix}$Input3$ctl00$ListBox`] = 'AIGS'; // Status: In Good Standing
        // ScriptManager must reference the ListerPanel update panel
        form['ctl01$ScriptManager1'] = `${this.listerPanel}|${submitBtn}`;
        form['__ASYNCPOST'] = 'true';
        form['IsControlPostBack'] = '1';

        let searchResponse;
        try {
          await rateLimiter.wait();
          searchResponse = await this._asyncPostback(form, rateLimiter, sessionCookies);
        } catch (err) {
          log.error(`Search POST failed for ${city}/${prefix}: ${err.message}`);
          continue;
        }

        if (searchResponse.statusCode !== 200) {
          log.error(`Search POST returned ${searchResponse.statusCode} for ${city}/${prefix}`);
          continue;
        }

        rateLimiter.resetBackoff();

        // Parse results from page 1
        let currentResponse = searchResponse;
        let currentCookies = searchResponse.cookies;
        let currentViewstate = this._extractViewstateFromDelta(searchResponse.body);
        const { totalItems, totalPages } = this._extractTotalPages(searchResponse.body);
        let currentPage = 1;

        if (totalItems > 0 && !options.maxPages) {
          log.info(`Prefix "${prefix}": ${totalItems} results in ${totalPages} pages`);
        }

        // Process all pages for this prefix
        while (true) {
          const allAttorneys = this._parseRadGridRows(currentResponse.body);

          // Filter by city (case-insensitive) — status already filtered server-side via AIGS.
          // In test mode, yield all results since VSB has no server-side city filter
          // and filtering client-side with limited pages discards most results.
          const cityAttorneys = isTestMode
            ? allAttorneys
            : allAttorneys.filter(a => a.city.toLowerCase() === cityLower);

          for (const attorney of cityAttorneys) {
            // Deduplicate by bar number
            if (seenBarNumbers.has(attorney.bar_number)) continue;
            seenBarNumbers.add(attorney.bar_number);

            if (options.minYear && attorney.admission_date) {
              const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
              if (year > 0 && year < options.minYear) continue;
            }
            attorney.practice_area = practiceArea || '';
            delete attorney.member_type;
            delete attorney.license_type;
            yield attorney;
            totalForCity++;
          }

          // Check if we should paginate
          if (currentPage >= totalPages || currentPage >= maxPagesPerPrefix) break;
          if (allAttorneys.length < this.pageSize) break;

          // Find next page target from pager links
          const pagerTargets = this._extractPagerTargets(currentResponse.body);
          const nextTarget = pagerTargets.find(t => t.page === currentPage + 1);

          if (!nextTarget) break;

          // Build page change postback
          const pageForm = { ...hiddenFields };
          if (currentViewstate) pageForm['__VIEWSTATE'] = currentViewstate;
          pageForm['__EVENTTARGET'] = nextTarget.target;
          pageForm['__EVENTARGUMENT'] = '';
          pageForm[`${this.formPrefix}$Input0$TextBox1`] = '';
          pageForm[`${this.formPrefix}$Input1$TextBox1`] = '';
          pageForm[`${this.formPrefix}$Input2$TextBox1`] = prefix;
          pageForm[`${this.formPrefix}$Input3$ctl00$ListBox`] = 'AIGS';
          pageForm['ctl01$ScriptManager1'] = `${this.listerPanel}|${nextTarget.target}`;
          pageForm['__ASYNCPOST'] = 'true';
          pageForm['IsControlPostBack'] = '1';

          let pageResponse2;
          try {
            await rateLimiter.wait();
            pageResponse2 = await this._asyncPostback(pageForm, rateLimiter, currentCookies);
          } catch (err) {
            log.error(`Grid page ${currentPage + 1} failed for ${city}/${prefix}: ${err.message}`);
            break;
          }

          if (pageResponse2.statusCode !== 200) break;

          currentViewstate = this._extractViewstateFromDelta(pageResponse2.body) || currentViewstate;
          currentCookies = pageResponse2.cookies;
          currentResponse = pageResponse2;
          currentPage++;
        }
      }

      if (totalForCity > 0) {
        log.success(`Found ${totalForCity} total results for ${city}`);
      } else {
        log.info(`No results for ${practiceArea || 'all'} in ${city}`);
      }

      // In test mode, only run one city iteration since we yield all results
      // (no city filter) and additional cities would just produce duplicates
      if (isTestMode) break;
    }
  }
}

module.exports = new VirginiaScraper();
