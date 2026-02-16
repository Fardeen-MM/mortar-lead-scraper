/**
 * Connecticut Judicial Branch Attorney Scraper
 *
 * Source: https://www.jud.ct.gov/attorneyfirminquiry/AttorneyFirmInquiry.aspx
 * Method: ASP.NET POST with __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION
 * The search form requires fetching the page first to obtain ViewState tokens,
 * then submitting a POST with the hidden fields plus search parameters.
 *
 * Profile pages: CT has a detail page at /attorneyfirminquiry/JurisDetail.aspx
 * but it is SESSION-DEPENDENT — accessed by clicking "Select" in search results
 * (ASP.NET __doPostBack), which sets server-side session state, then redirects
 * to JurisDetail.aspx. There is no standalone profile URL with a juris number
 * parameter. The detail page shows: juris number, status, admission date,
 * office address (with phone) — most of which is already in search results.
 * The only new field is the phone number. Because the detail page requires
 * an active search session with valid ViewState, parseProfilePage() CANNOT be
 * implemented without maintaining full ASP.NET session state per attorney.
 * The scraper yields profile_url: '' (empty) to indicate this limitation.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class ConnecticutScraper extends BaseScraper {
  constructor() {
    super({
      name: 'connecticut',
      stateCode: 'CT',
      baseUrl: 'https://www.jud.ct.gov/attorneyfirminquiry/AttorneyFirmInquiry.aspx',
      pageSize: 50,
      practiceAreaCodes: {
        'immigration':          'immigration',
        'family':               'family',
        'family law':           'family',
        'criminal':             'criminal',
        'criminal defense':     'criminal',
        'personal injury':      'personal_injury',
        'estate planning':      'estate_planning',
        'estate':               'estate_planning',
        'tax':                  'tax',
        'tax law':              'tax',
        'employment':           'employment',
        'labor':                'labor',
        'bankruptcy':           'bankruptcy',
        'real estate':          'real_estate',
        'civil litigation':     'civil_litigation',
        'business':             'business',
        'corporate':            'corporate',
        'elder':                'elder',
        'intellectual property':'intellectual_property',
        'medical malpractice':  'medical_malpractice',
        'workers comp':         'workers_comp',
        'environmental':        'environmental',
        'construction':         'construction',
        'juvenile':             'juvenile',
        'insurance':            'insurance',
        'securities':           'securities',
      },
      defaultCities: [
        'Hartford', 'New Haven', 'Stamford', 'Bridgeport',
        'Waterbury', 'Norwalk', 'Danbury', 'Greenwich',
      ],
    });
  }

  /**
   * HTTP POST with URL-encoded form data.
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
          'Referer': this.baseUrl,
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
   * Fetch the search page and extract ASP.NET hidden fields (__VIEWSTATE, etc.)
   */
  async fetchViewState(rateLimiter) {
    const response = await this.httpGet(this.baseUrl, rateLimiter);
    if (response.statusCode !== 200) {
      throw new Error(`Failed to fetch ViewState: status ${response.statusCode}`);
    }

    const $ = cheerio.load(response.body);
    const viewState = $('input[name="__VIEWSTATE"]').val() || '';
    const viewStateGenerator = $('input[name="__VIEWSTATEGENERATOR"]').val() || '';
    const eventValidation = $('input[name="__EVENTVALIDATION"]').val() || '';

    if (!viewState) {
      log.warn(`Could not extract __VIEWSTATE from CT search page`);
    }

    // Extract city dropdown options — values are padded with trailing spaces
    // and ASP.NET EventValidation rejects values that don't match exactly
    const cityMap = {};
    $('select[name="ctl00$ContentPlaceHolder1$ddlCityTown"] option').each((i, el) => {
      const rawVal = $(el).attr('value') || '';
      const normalized = rawVal.trim().toUpperCase();
      if (normalized) {
        cityMap[normalized] = rawVal;
      }
    });

    return { viewState, viewStateGenerator, eventValidation, cityMap };
  }

  /**
   * Not used directly — search() is overridden for ASP.NET ViewState handling.
   */
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for ASP.NET POST`);
  }

  parseResultsPage($) {
    const attorneys = [];

    // CT results are in table#ContentPlaceHolder1_GVDspCivInq
    // Each data row has 5 direct <td> cells:
    //   td[0] = Juris number, td[1] = Juris type (A=Attorney)
    //   td[2] = nested table with spans: lblCivInqName, lblAdmittedDT, lblStatus
    //   td[3] = nested table with span: lblFirm (firm name <br> address <br> city, ST zip)
    //   td[4] = Check license link
    const resultsTable = $('table#ContentPlaceHolder1_GVDspCivInq');
    const rows = resultsTable.find('> tbody > tr, > tr');

    rows.each((i, el) => {
      const $row = $(el);
      // Skip header row
      if ($row.find('th').length > 0) return;

      const tds = $row.find('> td');
      if (tds.length < 4) return;

      const jurisNumber = $(tds[0]).text().trim();
      // Skip if juris number is not numeric
      if (!/^\d+$/.test(jurisNumber)) return;

      // Extract name from span
      const nameSpan = $(tds[2]).find('span[id*="lblCivInqName"]');
      const fullName = nameSpan.text().trim();
      if (!fullName || fullName.length < 2) return;

      // Extract admission date
      const admitSpan = $(tds[2]).find('span[id*="lblAdmittedDT"]');
      const admitText = admitSpan.text().trim(); // "(Admitted:12/15/2020)"
      const admitMatch = admitText.match(/Admitted:\s*([\d/]+)/);
      const admissionDate = admitMatch ? admitMatch[1] : '';

      // Skip firm entries — these have "Admitted: N/A" or firm-like names
      if (/N\/A/.test(admitText) || /\b(LAW OFFICES?|LLC|LLP|P\.?C\.?|PLLC|INC|CORP)\b/i.test(fullName)) return;

      // Extract status
      const statusSpan = $(tds[2]).find('span[id*="lblStatus"]');
      const statusText = statusSpan.text().trim(); // "Current Status: Active"
      const statusMatch = statusText.match(/Status:\s*(.+)/i);
      const barStatus = statusMatch ? statusMatch[1].trim() : '';

      // Extract firm name and address from td[3]
      // The span contains: "FIRM NAME <br> STREET <br> CITY, ST  ZIP"
      const firmSpan = $(tds[3]).find('span[id*="lblFirm"]');
      let firmName = '';
      let street = '';
      let cityStateZip = '';

      if (firmSpan.length) {
        // Replace <br> with newline before extracting text
        const firmHtml = firmSpan.html() || '';
        const firmLines = firmHtml.split(/<br\s*\/?>/i).map(s => s.replace(/<[^>]*>/g, '').trim()).filter(Boolean);
        if (firmLines.length >= 1) firmName = firmLines[0].replace(/\s{2,}/g, ' ').trim();
        // City/state/zip is always on the LAST line (addresses may have variable # of lines)
        for (let li = firmLines.length - 1; li >= 1; li--) {
          const line = firmLines[li].replace(/\s{2,}/g, ' ').trim();
          if (/[A-Z]{2}\s+\d{5}/.test(line) || /,\s*[A-Z]{2}\s*$/.test(line)) {
            cityStateZip = line;
            break;
          }
        }
      }

      // Parse city from city/state/zip line (e.g., "HARTFORD, CT 06103")
      let city = '';
      if (cityStateZip) {
        const cszMatch = cityStateZip.match(/^([^,]+),\s*[A-Z]{2}/);
        if (cszMatch) {
          city = cszMatch[1].trim();
        }
      }

      // Parse name — CT returns "FIRST MIDDLE LAST" format (all caps)
      // Convert to title case, and decode HTML entities
      const toTitleCase = (s) => {
        // First decode common HTML entities
        let decoded = s.replace(/&amp;/gi, '&').replace(/&lt;/gi, '<').replace(/&gt;/gi, '>').replace(/&#39;/g, "'").replace(/&quot;/g, '"');
        return decoded.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
      };
      const nameParts = this.splitName(toTitleCase(fullName));

      attorneys.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: toTitleCase(fullName),
        firm_name: toTitleCase(firmName),
        city: toTitleCase(city),
        state: 'CT',
        phone: '',
        email: '',
        website: '',
        bar_number: jurisNumber,
        bar_status: barStatus,
        admission_date: admissionDate,
        profile_url: '',
      });
    });

    return attorneys;
  }

  extractResultCount($) {
    const text = $('body').text();

    const matchFound = text.match(/([\d,]+)\s+(?:attorneys?|results?|records?|members?)\s+found/i);
    if (matchFound) return parseInt(matchFound[1].replace(/,/g, ''), 10);

    const matchOf = text.match(/of\s+([\d,]+)\s+(?:results?|records?|attorneys?)/i);
    if (matchOf) return parseInt(matchOf[1].replace(/,/g, ''), 10);

    const matchShowing = text.match(/Showing\s+\d+\s*[-–]\s*\d+\s+of\s+([\d,]+)/i);
    if (matchShowing) return parseInt(matchShowing[1].replace(/,/g, ''), 10);

    const matchTotal = text.match(/Total\s+(?:Records?|Results?):\s*([\d,]+)/i);
    if (matchTotal) return parseInt(matchTotal[1].replace(/,/g, ''), 10);

    // Count GridView rows as fallback
    const rowCount = $('table[id*="GridView"] tr td, table[id*="grd"] tr td').closest('tr').length;
    if (rowCount > 0) return rowCount;

    return 0;
  }

  /**
   * Override search() to handle ASP.NET ViewState-based POST submissions.
   * First fetches the page to get __VIEWSTATE, then submits the form.
   * Iterates last name prefixes per city for broad coverage since the
   * search form requires name-based input.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`CT bar search does not filter by practice area — searching all attorneys`);
    }

    const cities = this.getCities(options);

    // CT bar requires at least 2 characters for name search.
    // Use 2-letter prefixes for broad coverage of common last name starts.
    const lastNamePrefixes = [
      'Ab', 'Ad', 'Al', 'Am', 'An', 'Ar', 'As', 'Au',
      'Ba', 'Be', 'Bi', 'Bl', 'Bo', 'Br', 'Bu',
      'Ca', 'Ce', 'Ch', 'Cl', 'Co', 'Cr', 'Cu',
      'Da', 'De', 'Di', 'Do', 'Dr', 'Du',
      'Ea', 'Ed', 'El', 'Em', 'En', 'Er', 'Es', 'Ev',
      'Fa', 'Fe', 'Fi', 'Fl', 'Fo', 'Fr', 'Fu',
      'Ga', 'Ge', 'Gi', 'Gl', 'Go', 'Gr', 'Gu',
      'Ha', 'He', 'Hi', 'Ho', 'Hu', 'Hy',
      'Ig', 'In', 'Ir', 'Is',
      'Ja', 'Je', 'Ji', 'Jo', 'Ju',
      'Ka', 'Ke', 'Ki', 'Kl', 'Kn', 'Ko', 'Kr', 'Ku',
      'La', 'Le', 'Li', 'Lo', 'Lu', 'Ly',
      'Ma', 'Mc', 'Me', 'Mi', 'Mo', 'Mu', 'My',
      'Na', 'Ne', 'Ni', 'No', 'Nu',
      'Ob', 'Od', 'Ol', 'Or', 'Os', 'Ow',
      'Pa', 'Pe', 'Ph', 'Pi', 'Pl', 'Po', 'Pr', 'Pu',
      'Qu',
      'Ra', 'Re', 'Ri', 'Ro', 'Ru', 'Ry',
      'Sa', 'Sc', 'Se', 'Sh', 'Si', 'Sl', 'Sm', 'Sn', 'So', 'Sp', 'St', 'Su', 'Sw',
      'Ta', 'Te', 'Th', 'Ti', 'To', 'Tr', 'Tu',
      'Ul', 'Un', 'Ur',
      'Va', 'Ve', 'Vi', 'Vo',
      'Wa', 'We', 'Wh', 'Wi', 'Wo', 'Wr', 'Wu',
      'Ya', 'Ye', 'Yo', 'Yu',
      'Za', 'Ze', 'Zi', 'Zo', 'Zu',
    ];

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`Searching: ${practiceArea || 'all'} attorneys in ${city}, ${this.stateCode}`);

      // Fetch initial ViewState for this city
      let viewStateData;
      try {
        await rateLimiter.wait();
        viewStateData = await this.fetchViewState(rateLimiter);
      } catch (err) {
        log.error(`Failed to fetch ViewState for ${city}: ${err.message}`);
        continue;
      }

      let pagesFetched = 0;

      for (const prefix of lastNamePrefixes) {
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        // Build ASP.NET form data with ViewState
        // ASP.NET WebForms requires ctl00$ContentPlaceHolder1$ prefix on all controls
        const formData = {
          '__VIEWSTATE': viewStateData.viewState,
          '__VIEWSTATEGENERATOR': viewStateData.viewStateGenerator,
          '__EVENTVALIDATION': viewStateData.eventValidation,
          'ctl00$ContentPlaceHolder1$txtCivInqName': prefix,
          'ctl00$ContentPlaceHolder1$txtJurisNo': '',
          'ctl00$ContentPlaceHolder1$ddlCityTown': (viewStateData.cityMap && viewStateData.cityMap[city.toUpperCase()]) || city.toUpperCase(),
          'ctl00$ContentPlaceHolder1$btnSubmit': 'Search',
        };

        log.info(`Searching ${city} — last name prefix "${prefix}" — POST ${this.baseUrl}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpPost(this.baseUrl, formData, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} prefix ${prefix}: ${err.message}`);
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
          log.error(`Unexpected status ${response.statusCode} — skipping prefix ${prefix}`);
          continue;
        }

        rateLimiter.resetBackoff();

        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected for ${city} prefix ${prefix} — skipping`);
          yield { _captcha: true, city, prefix };
          continue;
        }

        const $ = cheerio.load(response.body);

        // Update ViewState from response for subsequent requests
        const newViewState = $('input[name="__VIEWSTATE"]').val();
        const newViewStateGen = $('input[name="__VIEWSTATEGENERATOR"]').val();
        const newEventValidation = $('input[name="__EVENTVALIDATION"]').val();
        if (newViewState) viewStateData.viewState = newViewState;
        if (newViewStateGen) viewStateData.viewStateGenerator = newViewStateGen;
        if (newEventValidation) viewStateData.eventValidation = newEventValidation;

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          continue;
        }

        log.success(`Found ${attorneys.length} results for ${city} prefix "${prefix}"`);

        for (const attorney of attorneys) {
          if (options.minYear && attorney.admission_date) {
            const year = parseInt(attorney.admission_date.match(/\d{4}/)?.[0] || '0', 10);
            if (year > 0 && year < options.minYear) continue;
          }
          yield this.transformResult(attorney, practiceArea);
        }

        pagesFetched++;
      }
    }
  }
}

module.exports = new ConnecticutScraper();
