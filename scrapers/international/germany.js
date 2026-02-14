/**
 * Germany BRAK (Bundesrechtsanwaltskammer) Lawyer Register Scraper
 *
 * Source: https://bravsearch.bea-brak.de/bravsearch/
 * Method: PrimeFaces JSF AJAX POST with session cookies + ViewState
 *
 * The BRAK maintains the "Bundesweites Amtliches Anwaltsverzeichnis"
 * (nationwide official lawyer directory) of all registered lawyers in Germany.
 *
 * The site uses a PrimeFaces 13.x (Jakarta Faces) frontend that requires:
 *   1. GET the search page to establish JSESSIONID + obtain ViewState token
 *   2. POST an AJAX search with city/name/specialization filters
 *   3. Parse the PrimeFaces partial-response XML containing result cards
 *   4. Paginate via PrimeFaces DataGrid paginator AJAX calls
 *
 * Each result card shows: Last name, First name [Title], professional title,
 * firm name, street, postal code + city. Detailed info (phone, email, etc.)
 * requires per-record AJAX detail dialog fetches.
 *
 * The DataGrid returns 6 results per page. The paginator rowCount is capped
 * at 100 pages (600 items), so for cities with very large result sets the
 * scraper narrows searches by appending last-name letter prefixes.
 *
 * Specialization codes (Fachanwaltsbezeichnung) map to German legal specialty
 * titles registered with the bar.
 */

const https = require('https');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

/** Base URL for the BRAK search application */
const BRAK_BASE = 'https://bravsearch.bea-brak.de/bravsearch';

/**
 * Letters used to partition large result sets.
 * When a city query returns >= 600 results (the PrimeFaces paginator cap),
 * the scraper re-issues narrower searches by last-name prefix.
 */
const ALPHABET_PREFIXES = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

class GermanyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'germany',
      stateCode: 'DE-BRAK',
      baseUrl: BRAK_BASE,
      pageSize: 6, // PrimeFaces DataGrid returns 6 cards per page
      practiceAreaCodes: {
        // German Fachanwaltsbezeichnungen mapped to their select option values
        'agricultural':          '0',   // Agrarrecht
        'employment':            '1',   // Arbeitsrecht
        'banking':               '2',   // Bank- und Kapitalmarktrecht
        'construction':          '3',   // Bau- und Architektenrecht
        'inheritance':           '4',   // Erbrecht
        'family':                '5',   // Familienrecht
        'ip':                    '6',   // Gewerblicher Rechtsschutz
        'commercial':            '7',   // Handels- und Gesellschaftsrecht
        'it':                    '8',   // Informationstechnologierecht
        'insolvency':            '24',  // Insolvenz- und Sanierungsrecht
        'insolvency_old':        '9',   // Insolvenzrecht (legacy)
        'international':         '10',  // Internationales Wirtschaftsrecht
        'medical':               '11',  // Medizinrecht
        'tenancy':               '12',  // Miet- und Wohnungseigentumsrecht
        'immigration':           '13',  // Migrationsrecht
        'social':                '14',  // Sozialrecht
        'sports':                '23',  // Sportrecht
        'tax':                   '15',  // Steuerrecht
        'criminal':              '16',  // Strafrecht
        'transport':             '17',  // Transport- und Speditionsrecht
        'media':                 '18',  // Urheber- und Medienrecht
        'procurement':           '19',  // Vergaberecht
        'traffic':               '20',  // Verkehrsrecht
        'insurance':             '21',  // Versicherungsrecht
        'administrative':        '22',  // Verwaltungsrecht
        // Aliases in English
        'corporate':             '7',
        'real estate':           '12',
        'intellectual property': '6',
      },
      defaultCities: [
        'Berlin', 'München', 'Hamburg', 'Frankfurt',
        'Köln', 'Düsseldorf', 'Stuttgart', 'Leipzig',
      ],
    });

    // PrimeFaces paginator rowCount cap (6 items * 100 pages)
    this.paginatorCap = 600;
  }

  // -------------------------------------------------------------------------
  // These base-class methods are not used; search() is fully overridden.
  // -------------------------------------------------------------------------

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used -- search() is overridden`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used -- search() is overridden`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used -- search() is overridden`);
  }

  // -------------------------------------------------------------------------
  // HTTP helpers with cookie/session support
  // -------------------------------------------------------------------------

  /**
   * HTTP GET with cookie jar support. Returns { statusCode, body, cookies }.
   */
  _httpGetWithCookies(url, rateLimiter, cookies = '', redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `https://${parsed.hostname}${redirect}`;
          }
          const newCookies = this._extractSetCookies(res);
          const merged = this._mergeCookies(cookies, newCookies);
          return resolve(this._httpGetWithCookies(redirect, rateLimiter, merged, redirectCount + 1));
        }

        const newCookies = this._extractSetCookies(res);
        const merged = this._mergeCookies(cookies, newCookies);
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: merged }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
  }

  /**
   * HTTP POST (application/x-www-form-urlencoded) with AJAX headers for PrimeFaces.
   */
  _httpPostAjax(url, formBody, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const bodyBuffer = Buffer.from(formBody, 'utf8');
      const options = {
        hostname: parsed.hostname,
        port: 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'application/xml, text/xml, */*; q=0.01',
          'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'Content-Length': bodyBuffer.length,
          'Faces-Request': 'partial/ajax',
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': 'https://bravsearch.bea-brak.de',
          'Referer': `${BRAK_BASE}/index.xhtml`,
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const req = https.request(options, (res) => {
        const newCookies = this._extractSetCookies(res);
        const merged = this._mergeCookies(cookies, newCookies);
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data, cookies: merged }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(bodyBuffer);
      req.end();
    });
  }

  /**
   * Extract Set-Cookie headers into a cookie string.
   */
  _extractSetCookies(res) {
    return (res.headers['set-cookie'] || [])
      .map(c => c.split(';')[0])
      .join('; ');
  }

  /**
   * Merge old and new cookie strings (new values override old).
   */
  _mergeCookies(existing, incoming) {
    if (!existing && !incoming) return '';
    const map = {};
    for (const str of [existing, incoming]) {
      if (!str) continue;
      for (const pair of str.split('; ')) {
        const eq = pair.indexOf('=');
        if (eq > 0) {
          map[pair.substring(0, eq).trim()] = pair.substring(eq + 1).trim();
        }
      }
    }
    return Object.entries(map).map(([k, v]) => `${k}=${v}`).join('; ');
  }

  // -------------------------------------------------------------------------
  // Session management
  // -------------------------------------------------------------------------

  /**
   * Establish a session by GETting the search page.
   * Returns { cookies, viewState, sessionUrl }.
   */
  async _initSession(rateLimiter) {
    log.info('DE-BRAK: Initializing session...');

    const response = await this._httpGetWithCookies(
      `${BRAK_BASE}/`,
      rateLimiter,
    );

    if (response.statusCode !== 200) {
      throw new Error(`Session init failed with status ${response.statusCode}`);
    }

    // Extract ViewState from the HTML
    const $ = cheerio.load(response.body);
    const viewState = $('input[name="jakarta.faces.ViewState"]').val()
      || $('input[name="javax.faces.ViewState"]').val();

    if (!viewState) {
      throw new Error('Could not extract ViewState from search page');
    }

    // Extract session URL (form action contains jsessionid)
    const formAction = $('form#searchForm').attr('action') || '';
    let sessionUrl = `${BRAK_BASE}/index.xhtml`;
    if (formAction.includes('jsessionid')) {
      sessionUrl = `https://bravsearch.bea-brak.de${formAction}`;
    }

    log.info(`DE-BRAK: Session established. ViewState: ${viewState.substring(0, 30)}...`);

    return {
      cookies: response.cookies,
      viewState,
      sessionUrl,
    };
  }

  // -------------------------------------------------------------------------
  // Search + Pagination
  // -------------------------------------------------------------------------

  /**
   * Submit a search query via PrimeFaces AJAX.
   * Returns the AJAX partial-response body and updated session state.
   */
  async _submitSearch(session, rateLimiter, { city, lastName, specialization }) {
    const params = new URLSearchParams();
    params.set('jakarta.faces.partial.ajax', 'true');
    params.set('jakarta.faces.source', 'searchForm:cmdSearch');
    params.set('jakarta.faces.partial.execute', 'searchForm');
    params.set('jakarta.faces.partial.render', 'mainPageContent');
    params.set('searchForm:cmdSearch', 'searchForm:cmdSearch');
    params.set('searchForm', 'searchForm');
    params.set('searchForm:ddLanguage_input', 'de');
    params.set('searchForm:ddAnrede_input', '');
    params.set('searchForm:ddTitel_input', '');
    params.set('searchForm:txtName', lastName || '');
    params.set('searchForm:txtProfTitle', '');
    params.set('searchForm:txtVorname', '');
    params.set('searchForm:txtSpecialization_input', specialization || '');
    params.set('searchForm:txtOfficeName', '');
    params.set('searchForm:txtStrasse', '');
    params.set('searchForm:txtPostal', '');
    params.set('searchForm:txtOrt', city || '');
    params.set('searchForm:ddRAKammer_input', '');
    params.set('jakarta.faces.ViewState', session.viewState);

    const response = await this._httpPostAjax(
      session.sessionUrl,
      params.toString(),
      rateLimiter,
      session.cookies,
    );

    // Update session cookies
    session.cookies = response.cookies;

    // Extract updated ViewState from response
    const newViewState = this._extractViewStateFromPartialResponse(response.body);
    if (newViewState) {
      session.viewState = newViewState;
    }

    return response;
  }

  /**
   * Fetch a specific page of results from the DataGrid.
   * pageNum is 0-based (PrimeFaces convention).
   */
  async _fetchPage(session, rateLimiter, pageNum) {
    const first = pageNum * this.pageSize;

    const params = new URLSearchParams();
    params.set('jakarta.faces.partial.ajax', 'true');
    params.set('jakarta.faces.source', 'resultForm:dlResultList');
    params.set('jakarta.faces.partial.execute', 'resultForm:dlResultList');
    params.set('jakarta.faces.partial.render', 'resultForm:dlResultList');
    params.set('resultForm:dlResultList_pagination', 'true');
    params.set('resultForm:dlResultList_first', String(first));
    params.set('resultForm:dlResultList_rows', String(this.pageSize));
    params.set('resultForm:dlResultList_page', String(pageNum));
    params.set('resultForm', 'resultForm');
    params.set('resultForm:ddLanguage_input', 'de');
    params.set('jakarta.faces.ViewState', session.viewState);

    const response = await this._httpPostAjax(
      `${BRAK_BASE}/index.xhtml`,
      params.toString(),
      rateLimiter,
      session.cookies,
    );

    session.cookies = response.cookies;

    const newViewState = this._extractViewStateFromPartialResponse(response.body);
    if (newViewState) {
      session.viewState = newViewState;
    }

    return response;
  }

  /**
   * Extract the updated ViewState from a PrimeFaces partial-response XML.
   */
  _extractViewStateFromPartialResponse(xml) {
    // The ViewState is in: <update id="j_id1:jakarta.faces.ViewState:0"><![CDATA[...]]></update>
    const match = xml.match(
      /ViewState[^>]*>\s*<!\[CDATA\[([^\]]+)\]\]>/
    );
    return match ? match[1] : null;
  }

  // -------------------------------------------------------------------------
  // Result parsing
  // -------------------------------------------------------------------------

  /**
   * Extract total result count from the search response.
   * Looks for "Anzahl gefundener Einträge: NNN" in the HTML.
   */
  _extractTotalCount(html) {
    const match = html.match(/Anzahl gefundener Eintr[aä]ge:\s*([\d.]+)/i);
    if (match) {
      return parseInt(match[1].replace(/\./g, ''), 10);
    }
    return 0;
  }

  /**
   * Parse result cards from a PrimeFaces partial-response or from the
   * initial search response HTML.
   *
   * Each result card has:
   *   - Header span.resultCardHeader: "LastName, FirstName [Title]"
   *   - Body ul with li items: professional title, [firm name], street, postal+city
   *
   * There are two types of cards:
   *   - Individual lawyer cards (j_idt255 link -> resultDetailForm)
   *   - Firm/BAG cards (j_idt256 link -> resultDetailFormBag) -- we include these
   */
  _parseCards(html) {
    const $ = cheerio.load(html, { xmlMode: false });
    const results = [];

    // Find all result card panels
    $('div.resultCard').each((_, cardEl) => {
      const $card = $(cardEl);

      // Extract name from header
      const headerText = $card.find('span.resultCardHeader').text().trim();
      if (!headerText) return;

      // Parse "LastName, FirstName Title" or just a firm name
      const parsed = this._parseHeaderName(headerText);

      // Extract list items from card body
      const items = [];
      $card.find('.resultCardContentBox ul li').each((_, li) => {
        const text = $(li).text().trim();
        if (text) items.push(text);
      });

      // Parse the list items
      const cardData = this._parseCardItems(items);

      // Determine if this is a firm card (BAG) vs individual
      const isFirmCard = $card.find('a.resultCardDetailLink').attr('onclick')?.includes('resultDetailFormBag');

      results.push({
        first_name: parsed.firstName,
        last_name: parsed.lastName,
        full_name: parsed.fullName,
        title: parsed.title,
        firm_name: cardData.firmName,
        professional_title: cardData.professionalTitle,
        address: cardData.street,
        postal_code: cardData.postalCode,
        city: cardData.city,
        state: 'DE',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: 'Zugelassen',
        profile_url: '',
        is_firm: isFirmCard || false,
        source: 'germany_bar',
      });
    });

    return results;
  }

  /**
   * Parse "LastName, FirstName Title" from the card header.
   *
   * Examples:
   *   "Aalderks, Dirk Johann Dr. jur." -> { lastName: "Aalderks", firstName: "Dirk Johann", title: "Dr. jur." }
   *   "Abbas, Mostafa" -> { lastName: "Abbas", firstName: "Mostafa" }
   *   "abante Rechtsanwaltsgesellschaft mbH & Co. KG" -> firm name (no comma)
   */
  _parseHeaderName(headerText) {
    // Decode HTML entities
    const decoded = this.decodeEntities(headerText);
    const commaIdx = decoded.indexOf(',');

    if (commaIdx < 0) {
      // No comma: this is a firm name, not a person
      return {
        firstName: '',
        lastName: '',
        fullName: decoded.trim(),
        title: '',
      };
    }

    const lastName = decoded.substring(0, commaIdx).trim();
    const rest = decoded.substring(commaIdx + 1).trim();

    // Separate title (Dr., Prof., etc.) from first name
    let firstName = rest;
    let title = '';

    // Common German academic/professional title patterns at end of name
    const titleMatch = rest.match(/^(.+?)\s+((?:Dr\.\s*(?:jur\.|med\.|rer\.\s*nat\.|rer\.\s*pol\.|h\.c\.)*|Prof\.\s*(?:Dr\.\s*(?:jur\.|med\.|rer\.\s*nat\.)*)?|LL\.M\.|LL\.M|M\.A\.|MBA|Dipl\.\s*-?\s*\w+).*?)$/i);
    if (titleMatch) {
      firstName = titleMatch[1].trim();
      title = titleMatch[2].trim();
    }

    const fullName = [firstName, lastName].filter(Boolean).join(' ');

    return { firstName, lastName, fullName, title };
  }

  /**
   * Parse the <ul><li> items from a result card body.
   *
   * Typical patterns:
   *   ["Rechtsanwalt", "Firm Name", "Street 123", "12345 Berlin"]
   *   ["Rechtsanwältin", "Street 123", "12345 Berlin"]
   *   ["Berufsausübungsgesellschaft", "Firm Name", "Street", "12345 City"]
   */
  _parseCardItems(items) {
    let professionalTitle = '';
    let firmName = '';
    let street = '';
    let postalCode = '';
    let city = '';

    if (items.length === 0) return { professionalTitle, firmName, street, postalCode, city };

    // First item is always the professional title
    professionalTitle = items[0];

    // Last item is always "PLZ City" (if we have multiple items)
    if (items.length >= 2) {
      const lastItem = items[items.length - 1];
      const plzMatch = lastItem.match(/^(\d{5})\s+(.+)$/);
      if (plzMatch) {
        postalCode = plzMatch[1];
        city = plzMatch[2];
      } else {
        // Fallback: might be just a city
        city = lastItem;
      }
    }

    // Second-to-last is usually the street
    if (items.length >= 3) {
      street = items[items.length - 2];
    }

    // If there are items between professional title and address, they are firm name
    // items[0] = prof title, items[1..n-2] = firm name / extra info, items[n-1] = city, items[n-2] = street
    if (items.length >= 4) {
      // Items between [1] and [length-2] are firm/kanzlei info
      const firmParts = items.slice(1, items.length - 2);
      firmName = firmParts.join(', ');
    } else if (items.length === 3) {
      // Could be [prof, street, city] or [prof, firm, city] - check if middle looks like street
      const middleItem = items[1];
      if (middleItem.match(/^\d/) || middleItem.match(/str\.|straße|weg|allee|platz|damm|ring|gasse/i)) {
        street = middleItem;
      } else {
        // It's a firm name; street is empty
        firmName = middleItem;
        street = '';
      }
    }

    // Clean up "Kanzlei: " prefix from firm names
    if (firmName.startsWith('Kanzlei:')) {
      firmName = firmName.substring('Kanzlei:'.length).trim();
    }

    return { professionalTitle, firmName, street, postalCode, city };
  }

  // -------------------------------------------------------------------------
  // Main search generator
  // -------------------------------------------------------------------------

  /**
   * Async generator that yields attorney records from the BRAK register.
   *
   * Strategy:
   *   1. Initialize session (GET search page -> cookies + ViewState)
   *   2. For each city:
   *      a. Submit search AJAX POST with city filter
   *      b. Parse result count
   *      c. If result count >= paginator cap (600), split into A-Z prefix searches
   *      d. Paginate through all result pages
   *      e. Parse and yield lawyer records from each page
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (practiceArea && !practiceCode) {
      log.warn(`DE-BRAK: Unknown practice area "${practiceArea}" -- searching without filter`);
      log.info(`Available: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    // Step 1: Initialize session
    let session;
    try {
      await rateLimiter.wait();
      session = await this._initSession(rateLimiter);
    } catch (err) {
      log.error(`DE-BRAK: Failed to initialize session: ${err.message}`);
      return;
    }

    // Step 2: Iterate cities
    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`DE-BRAK: Searching ${practiceArea || 'all'} lawyers in ${city}`);

      // Determine if we need alphabet partitioning
      const totalForCity = await this._getResultCount(
        session, rateLimiter, { city, lastName: '', specialization: practiceCode || '' }
      );

      if (totalForCity === 0) {
        log.info(`DE-BRAK: No results for ${city}`);
        continue;
      }

      if (totalForCity < this.paginatorCap || options.maxPages) {
        // Small enough to paginate directly
        yield* this._scrapeSearchResults(
          session, rateLimiter, city, '', practiceCode || '', totalForCity, options
        );
      } else {
        // Too many results -- split by last-name prefix
        log.info(`DE-BRAK: ${city} has ${totalForCity} results (>= ${this.paginatorCap}). Splitting by last-name prefix A-Z.`);
        for (const letter of ALPHABET_PREFIXES) {
          const letterCount = await this._getResultCount(
            session, rateLimiter, { city, lastName: letter, specialization: practiceCode || '' }
          );

          if (letterCount === 0) {
            continue;
          }

          log.info(`DE-BRAK: ${city} prefix "${letter}": ${letterCount} results`);
          yield* this._scrapeSearchResults(
            session, rateLimiter, city, letter, practiceCode || '', letterCount, options
          );

          if (options.maxPages) break; // For test mode, one letter is enough
        }
      }
    }
  }

  /**
   * Submit a search and return just the total result count.
   * Also re-initializes session if ViewState has expired.
   */
  async _getResultCount(session, rateLimiter, { city, lastName, specialization }) {
    let response;
    try {
      await rateLimiter.wait();
      response = await this._submitSearch(session, rateLimiter, {
        city,
        lastName,
        specialization,
      });
    } catch (err) {
      log.error(`DE-BRAK: Search failed: ${err.message}`);
      // Try re-initializing session
      try {
        await rateLimiter.wait();
        const newSession = await this._initSession(rateLimiter);
        session.cookies = newSession.cookies;
        session.viewState = newSession.viewState;
        session.sessionUrl = newSession.sessionUrl;
        await rateLimiter.wait();
        response = await this._submitSearch(session, rateLimiter, {
          city,
          lastName,
          specialization,
        });
      } catch (retryErr) {
        log.error(`DE-BRAK: Retry also failed: ${retryErr.message}`);
        return 0;
      }
    }

    if (response.statusCode !== 200) {
      log.error(`DE-BRAK: Search returned status ${response.statusCode}`);
      return 0;
    }

    // Check for session expiry (ViewExpired dialog)
    if (response.body.includes('Ansicht abgelaufen') || response.body.includes('ViewExpiredException')) {
      log.warn('DE-BRAK: Session expired, re-initializing...');
      try {
        await rateLimiter.wait();
        const newSession = await this._initSession(rateLimiter);
        session.cookies = newSession.cookies;
        session.viewState = newSession.viewState;
        session.sessionUrl = newSession.sessionUrl;
        await rateLimiter.wait();
        response = await this._submitSearch(session, rateLimiter, {
          city,
          lastName,
          specialization,
        });
      } catch (err) {
        log.error(`DE-BRAK: Re-init failed: ${err.message}`);
        return 0;
      }
    }

    return this._extractTotalCount(response.body);
  }

  /**
   * Generator that performs a search and paginates through all result pages.
   * Assumes the search has already been submitted (by _getResultCount) and
   * the session is on the results page.
   */
  async *_scrapeSearchResults(session, rateLimiter, city, lastName, specialization, totalCount, options) {
    // Re-submit the search to get page 0 results (since _getResultCount already did this,
    // we already have the results page loaded in the session, but we need to re-submit
    // to be sure we're on the right result set for pagination)
    let searchResponse;
    try {
      await rateLimiter.wait();
      searchResponse = await this._submitSearch(session, rateLimiter, {
        city,
        lastName,
        specialization,
      });
    } catch (err) {
      log.error(`DE-BRAK: Search submit failed for ${city}/${lastName}: ${err.message}`);
      return;
    }

    if (searchResponse.statusCode !== 200) {
      log.error(`DE-BRAK: Search returned ${searchResponse.statusCode} for ${city}/${lastName}`);
      return;
    }

    const verifiedTotal = this._extractTotalCount(searchResponse.body);
    if (verifiedTotal === 0) {
      return;
    }

    const effectiveTotal = Math.min(verifiedTotal, this.paginatorCap);
    const totalPages = Math.ceil(effectiveTotal / this.pageSize);
    const suffix = lastName ? ` (prefix "${lastName}")` : '';
    log.success(`DE-BRAK: Found ${verifiedTotal} results for ${city}${suffix} -- paginating ${totalPages} pages`);

    // Parse page 0 from the search response
    const page0Attorneys = this._parseCards(searchResponse.body);
    for (const attorney of page0Attorneys) {
      yield this._transformBrakResult(attorney, city);
    }

    // Pages 1..N
    let pagesFetched = 1;
    for (let pageNum = 1; pageNum < totalPages; pageNum++) {
      if (options.maxPages && pagesFetched >= options.maxPages) {
        log.info(`DE-BRAK: Reached max pages limit (${options.maxPages}) for ${city}${suffix}`);
        break;
      }

      try {
        await rateLimiter.wait();
        const pageResponse = await this._fetchPage(session, rateLimiter, pageNum);

        if (pageResponse.statusCode !== 200) {
          log.warn(`DE-BRAK: Page ${pageNum} returned ${pageResponse.statusCode} -- stopping`);
          break;
        }

        // Check for session expiry
        if (pageResponse.body.includes('Ansicht abgelaufen') || pageResponse.body.includes('ViewExpiredException')) {
          log.warn('DE-BRAK: Session expired during pagination -- stopping');
          break;
        }

        const attorneys = this._parseCards(pageResponse.body);

        if (attorneys.length === 0) {
          log.warn(`DE-BRAK: Page ${pageNum} returned 0 results -- stopping`);
          break;
        }

        for (const attorney of attorneys) {
          yield this._transformBrakResult(attorney, city);
        }

        pagesFetched++;
      } catch (err) {
        log.error(`DE-BRAK: Page ${pageNum} failed: ${err.message}`);
        break;
      }
    }
  }

  /**
   * Post-process a BRAK result before yielding.
   * Fills in the search city if the parsed city is empty (edge case).
   */
  _transformBrakResult(attorney, searchCity) {
    if (!attorney.city && searchCity) {
      attorney.city = searchCity;
    }
    attorney.source = 'germany_bar';
    return attorney;
  }
}

module.exports = new GermanyScraper();
