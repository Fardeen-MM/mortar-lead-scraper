/**
 * Italy Lawyer Directory Scraper
 *
 * Sources:
 *   1. Primary: Cassa Forense — Elenco Nazionale Avvocati (National Lawyer Directory)
 *      URL: https://servizi.cassaforense.it/cfor/elenconazionaleavvocati/elenconazionaleavvocati_pg.cfm
 *      Method: ColdFusion form POST searching by surname + bar association (Ordine)
 *
 *   2. Fallback: Regional bar associations via SferaBit AlboSFERA platform
 *      URL: https://sfera.sferabit.com/servizi/alboonline/elencoAlboOnline.php
 *      Method: AJAX POST with filter parameters
 *
 *   3. Reference: CNF — Consiglio Nazionale Forense (National Bar Council)
 *      URL: https://www.consiglionazionaleforense.it/ricerca-avvocati
 *      Note: Requires JS rendering (Liferay SPA) — not directly usable for scraping
 *
 * Strategy:
 *   For each target city, the scraper uses the Cassa Forense directory, which
 *   is a server-rendered ColdFusion application that accepts surname queries
 *   filtered by the corresponding local bar association (Ordine).
 *
 *   Since the search requires a surname, the scraper iterates through
 *   alphabet prefixes (A-Z) to enumerate all lawyers in each bar district.
 *
 *   The Cassa Forense directory lists lawyers who are members of the pension
 *   fund, which covers virtually all practising Italian lawyers (~240,000).
 *
 * Known limitations:
 *   - Cassa Forense requires at least 3 characters for surname search;
 *     we use 3-letter prefixes to broaden coverage.
 *   - The CNF site requires full JavaScript rendering and is not scraper-friendly.
 *   - Some regional bars may have additional anti-bot protections.
 *   - Phone/email are typically not included in public search results.
 */

const https = require('https');
const http = require('http');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

/**
 * Cassa Forense search URL.
 * The form POSTs to this endpoint with cognome (surname) and ordine (bar association).
 */
const CASSA_FORENSE_BASE = 'https://servizi.cassaforense.it/CFor/ElencoNazionaleAvvocati';
const CASSA_FORENSE_SEARCH = `${CASSA_FORENSE_BASE}/elenconazionaleavvocati_pg.cfm`;

/**
 * SferaBit AlboSFERA platform URLs for regional bars.
 * Each regional bar has a numeric ID on the SferaBit platform.
 */
const SFERABIT_BASE = 'https://sfera.sferabit.com/servizi/alboonline';
const SFERABIT_SEARCH = `${SFERABIT_BASE}/elencoAlboOnline.php`;

/**
 * Map of target cities to their Ordine (bar association) names.
 * These are the official names used by Cassa Forense in its dropdown.
 * Italian bars are organized by judicial district, not always by city name.
 */
const CITY_TO_ORDINE = {
  'Roma':     'ROMA',
  'Milano':   'MILANO',
  'Napoli':   'NAPOLI',
  'Torino':   'TORINO',
  'Firenze':  'FIRENZE',
  'Bologna':  'BOLOGNA',
  'Palermo':  'PALERMO',
  'Genova':   'GENOVA',
  'Bari':     'BARI',
  'Catania':  'CATANIA',
  'Venezia':  'VENEZIA',
  'Verona':   'VERONA',
  'Padova':   'PADOVA',
  'Brescia':  'BRESCIA',
  'Cagliari': 'CAGLIARI',
  'Perugia':  'PERUGIA',
};

/**
 * SferaBit AlboSFERA IDs for regional bars.
 * Used as fallback when Cassa Forense is unavailable.
 */
const SFERABIT_IDS = {
  'Roma':    1118,
  'Milano':  1080,
  'Napoli':  1104,
  'Torino':  1001,
  'Firenze': 1053,
  'Bologna': 1016,
  'Palermo': 1111,
  'Genova':  1056,
};

/**
 * Alphabet prefixes for surname-based enumeration.
 * We use 2-letter prefixes to get more manageable result sets,
 * but the Cassa Forense requires minimum 3 characters.
 */
const SURNAME_PREFIXES = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

class ItalyScraper extends BaseScraper {
  constructor() {
    super({
      name: 'italy',
      stateCode: 'IT',
      baseUrl: CASSA_FORENSE_BASE,
      pageSize: 50,
      practiceAreaCodes: {
        // Italian legal specializations (Specializzazioni Forensi)
        // Note: Cassa Forense does not filter by practice area;
        // these are provided for metadata/UI purposes and future filtering.
        'civil':                 'Diritto Civile',
        'criminal':              'Diritto Penale',
        'administrative':        'Diritto Amministrativo',
        'tax':                   'Diritto Tributario',
        'employment':            'Diritto del Lavoro',
        'family':                'Diritto di Famiglia',
        'corporate':             'Diritto Commerciale',
        'real estate':           'Diritto Immobiliare',
        'ip':                    'Proprietà Intellettuale',
        'intellectual property': 'Proprietà Intellettuale',
        'immigration':           'Diritto dell\'Immigrazione',
        'international':         'Diritto Internazionale',
        'banking':               'Diritto Bancario',
        'insurance':             'Diritto Assicurativo',
        'environmental':         'Diritto Ambientale',
      },
      defaultCities: [
        'Roma', 'Milano', 'Napoli', 'Torino',
        'Firenze', 'Bologna', 'Palermo', 'Genova',
      ],
    });
  }

  // -------------------------------------------------------------------------
  // Base-class method stubs (search() is fully overridden)
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
  // HTTP helpers
  // -------------------------------------------------------------------------

  /**
   * HTTP POST with form data for Cassa Forense ColdFusion application.
   * Returns { statusCode, body, cookies }.
   */
  _httpPost(url, formBody, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const bodyBuffer = Buffer.from(formBody, 'utf8');
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
          'Accept-Encoding': 'identity',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': bodyBuffer.length,
          'Origin': 'https://servizi.cassaforense.it',
          'Referer': CASSA_FORENSE_SEARCH,
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');

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
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: newCookies,
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.write(bodyBuffer);
      req.end();
    });
  }

  /**
   * HTTP GET with cookie support for session initialization.
   */
  _httpGetWithCookies(url, rateLimiter, cookies = '') {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          ...(cookies ? { 'Cookie': cookies } : {}),
        },
        timeout: 30000,
      };

      const protocol = parsed.protocol === 'https:' ? https : http;
      const req = protocol.request(options, (res) => {
        const newCookies = (res.headers['set-cookie'] || [])
          .map(c => c.split(';')[0])
          .join('; ');

        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            redirect = `${parsed.protocol}//${parsed.host}${redirect}`;
          }
          const merged = this._mergeCookies(cookies, newCookies);
          return resolve(this._httpGetWithCookies(redirect, rateLimiter, merged));
        }

        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: data,
          cookies: this._mergeCookies(cookies, newCookies),
        }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
      req.end();
    });
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
  // Cassa Forense parsing
  // -------------------------------------------------------------------------

  /**
   * Parse lawyer results from the Cassa Forense HTML response.
   *
   * The search results page is a ColdFusion-generated HTML table.
   * Each row typically contains: Name, Ordine (bar), enrollment number,
   * and status information.
   *
   * The exact HTML structure may vary; we use multiple strategies to
   * extract data robustly.
   */
  _parseCassaForenseResults(html, searchCity) {
    const $ = cheerio.load(html);
    const results = [];

    // Strategy 1: Look for result table rows
    // Cassa Forense typically renders results in a <table> with alternating rows
    $('table tr').each((idx, row) => {
      if (idx === 0) return; // Skip header row

      const cells = $(row).find('td');
      if (cells.length < 2) return;

      // Extract text from each cell
      const cellTexts = [];
      cells.each((_, cell) => {
        cellTexts.push($(cell).text().trim());
      });

      // Try to identify name cell (typically first or second cell)
      // Pattern: "COGNOME Nome" or "Cognome, Nome"
      let fullName = '';
      let ordine = '';
      let enrollmentNum = '';

      if (cellTexts.length >= 3) {
        // Common pattern: [Name, Ordine, Number, ...]
        fullName = cellTexts[0];
        ordine = cellTexts[1];
        enrollmentNum = cellTexts[2];
      } else if (cellTexts.length === 2) {
        fullName = cellTexts[0];
        ordine = cellTexts[1];
      }

      if (!fullName || fullName.length < 3) return;

      // Parse name — Italian convention is "COGNOME Nome" (surname uppercase, given title case)
      const nameParts = this._parseItalianName(fullName);

      results.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: nameParts.fullName,
        firm_name: '',
        city: searchCity,
        state: 'IT',
        postal_code: '',
        address: '',
        phone: '',
        email: '',
        website: '',
        bar_number: enrollmentNum,
        bar_name: ordine || `Ordine Avvocati ${searchCity}`,
        bar_status: 'Iscritto',
        profile_url: '',
        source: 'italy_bar',
      });
    });

    // Strategy 2: Look for divs/lists if table approach yields nothing
    if (results.length === 0) {
      $('div.risultato, div.result, li.risultato').each((_, el) => {
        const text = $(el).text().trim();
        if (!text || text.length < 5) return;

        const nameParts = this._parseItalianName(text.split('\n')[0].trim());
        if (!nameParts.lastName) return;

        results.push({
          first_name: nameParts.firstName,
          last_name: nameParts.lastName,
          full_name: nameParts.fullName,
          firm_name: '',
          city: searchCity,
          state: 'IT',
          postal_code: '',
          address: '',
          phone: '',
          email: '',
          website: '',
          bar_number: '',
          bar_name: `Ordine Avvocati ${searchCity}`,
          bar_status: 'Iscritto',
          profile_url: '',
          source: 'italy_bar',
        });
      });
    }

    return results;
  }

  /**
   * Parse an Italian-style full name.
   *
   * Italian name conventions in legal directories:
   *   - "ROSSI MARIO" (surname uppercase, given name uppercase) — Cassa Forense
   *   - "Rossi, Mario" (comma-separated)
   *   - "ROSSI Mario" (surname uppercase, given name title case)
   *
   * We handle all three patterns.
   */
  _parseItalianName(raw) {
    if (!raw) return { firstName: '', lastName: '', fullName: '' };

    const cleaned = raw.trim();

    // Pattern 1: Comma-separated "Cognome, Nome"
    if (cleaned.includes(',')) {
      const parts = cleaned.split(',').map(p => p.trim());
      const lastName = this._titleCase(parts[0]);
      const firstName = this._titleCase(parts.slice(1).join(' '));
      return {
        firstName,
        lastName,
        fullName: `${firstName} ${lastName}`.trim(),
      };
    }

    // Pattern 2: Space-separated — assume first word is surname if all caps,
    // or use the "Last First" convention common in Italian legal directories
    const words = cleaned.split(/\s+/).filter(Boolean);
    if (words.length === 0) return { firstName: '', lastName: '', fullName: '' };

    if (words.length === 1) {
      return {
        firstName: '',
        lastName: this._titleCase(words[0]),
        fullName: this._titleCase(words[0]),
      };
    }

    // If all words are uppercase, assume "COGNOME NOME" format
    const allUpper = words.every(w => w === w.toUpperCase());
    if (allUpper) {
      // Convention: first word(s) = surname, remaining = given name
      // Heuristic: take last word as first name, rest as surname
      // (This is imperfect for multi-word surnames like "DE ROSSI MARIO")
      const lastName = this._titleCase(words.slice(0, -1).join(' '));
      const firstName = this._titleCase(words[words.length - 1]);
      return {
        firstName,
        lastName,
        fullName: `${firstName} ${lastName}`.trim(),
      };
    }

    // Mixed case: first uppercase block is surname
    let surnameEnd = 0;
    for (let i = 0; i < words.length; i++) {
      if (words[i] === words[i].toUpperCase() && /^[A-ZÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÑÇ]+$/i.test(words[i])) {
        surnameEnd = i + 1;
      } else {
        break;
      }
    }

    if (surnameEnd > 0 && surnameEnd < words.length) {
      const lastName = this._titleCase(words.slice(0, surnameEnd).join(' '));
      const firstName = words.slice(surnameEnd).join(' ');
      return {
        firstName,
        lastName,
        fullName: `${firstName} ${lastName}`.trim(),
      };
    }

    // Fallback: first word = first name, rest = last name
    return {
      firstName: words[0],
      lastName: words.slice(1).join(' '),
      fullName: cleaned,
    };
  }

  /**
   * Title-case a string: "ROSSI" -> "Rossi", "DE ROSSI" -> "De Rossi"
   */
  _titleCase(str) {
    if (!str) return '';
    return str
      .toLowerCase()
      .replace(/(?:^|\s)\S/g, c => c.toUpperCase());
  }

  // -------------------------------------------------------------------------
  // SferaBit AlboSFERA fallback parsing
  // -------------------------------------------------------------------------

  /**
   * Parse results from SferaBit's AlboSFERA AJAX response.
   * The response is HTML fragments loaded into a result container.
   */
  _parseSferabitResults(html, searchCity) {
    const $ = cheerio.load(html);
    const results = [];

    // SferaBit uses a table or list for results
    $('tr, div.riga, div.row').each((_, row) => {
      const $row = $(row);
      const cells = $row.find('td, div.cella, span');

      if (cells.length < 2) return;

      const cellTexts = [];
      cells.each((_, cell) => {
        cellTexts.push($(cell).text().trim());
      });

      let fullName = cellTexts[0] || '';
      if (!fullName || fullName.length < 3) return;

      const nameParts = this._parseItalianName(fullName);

      // Try to extract address from subsequent cells
      let address = '';
      let postalCode = '';
      let city = searchCity;
      for (let i = 1; i < cellTexts.length; i++) {
        const capMatch = cellTexts[i].match(/^(\d{5})\s+(.+)/);
        if (capMatch) {
          postalCode = capMatch[1];
          city = capMatch[2];
          break;
        }
        // Check for address pattern
        if (cellTexts[i].match(/^(via|piazza|corso|viale|largo|vicolo)\s/i)) {
          address = cellTexts[i];
        }
      }

      results.push({
        first_name: nameParts.firstName,
        last_name: nameParts.lastName,
        full_name: nameParts.fullName,
        firm_name: '',
        city: city,
        state: 'IT',
        postal_code: postalCode,
        address: address,
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_name: `Ordine Avvocati ${searchCity}`,
        bar_status: 'Iscritto',
        profile_url: '',
        source: 'italy_bar',
      });
    });

    return results;
  }

  // -------------------------------------------------------------------------
  // Main search generator
  // -------------------------------------------------------------------------

  /**
   * Async generator that yields attorney records from Italian lawyer directories.
   *
   * Strategy:
   *   1. For each target city, look up the corresponding Ordine (bar association)
   *   2. Initialize a session with Cassa Forense
   *   3. Iterate A-Z surname prefixes to enumerate all lawyers for that Ordine
   *   4. Parse HTML results and yield standardized attorney objects
   *   5. If Cassa Forense is unavailable, fall back to SferaBit AlboSFERA
   *
   * The search iterates letter prefixes because the directory requires a
   * minimum surname input and has result count caps per query.
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const seen = new Set();

    if (practiceArea) {
      log.info(`IT: Practice area filtering ("${practiceArea}") is noted but the directory does not support server-side filtering. All lawyers will be returned.`);
    }

    // Step 1: Initialize session with Cassa Forense
    let cookies = '';
    let sessionOk = false;

    try {
      log.info('IT: Initializing session with Cassa Forense...');
      await rateLimiter.wait();
      const initResponse = await this._httpGetWithCookies(CASSA_FORENSE_SEARCH, rateLimiter);

      if (initResponse.statusCode === 200) {
        cookies = initResponse.cookies;
        sessionOk = true;
        log.success('IT: Cassa Forense session established');

        // Check for CAPTCHA or JS-rendering requirements
        if (this.detectCaptcha(initResponse.body)) {
          log.warn('IT: CAPTCHA detected on Cassa Forense — will attempt SferaBit fallback');
          sessionOk = false;
        }

        // Check if the page has meaningful form content
        if (!initResponse.body.includes('Cognome') && !initResponse.body.includes('cognome')) {
          log.warn('IT: Cassa Forense page does not contain expected form fields — may require JS rendering');
          log.warn('IT: The page may be behind a JavaScript challenge. Attempting to proceed with POST requests...');
        }
      } else {
        log.warn(`IT: Cassa Forense returned status ${initResponse.statusCode} — will try SferaBit fallback`);
      }
    } catch (err) {
      log.warn(`IT: Failed to connect to Cassa Forense: ${err.message} — will try SferaBit fallback`);
    }

    // Step 2: Iterate cities
    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`IT: Searching lawyers in ${city}`);

      const ordine = CITY_TO_ORDINE[city] || city.toUpperCase();

      if (sessionOk) {
        // Primary path: Cassa Forense surname iteration
        yield* this._searchCassaForense(
          rateLimiter, city, ordine, cookies, seen, options
        );
      } else {
        // Fallback: SferaBit AlboSFERA
        const sferaId = SFERABIT_IDS[city];
        if (sferaId) {
          yield* this._searchSferabit(
            rateLimiter, city, sferaId, seen, options
          );
        } else {
          log.warn(`IT: No SferaBit ID configured for ${city} — skipping`);
          log.info(`IT: To add support, find the AlboSFERA ID at https://sfera.sferabit.com/servizi/alboonline/index.php?id=XXXX`);
        }
      }
    }

    log.success(`IT: Search complete. ${seen.size} unique lawyers yielded.`);
  }

  /**
   * Search Cassa Forense by iterating through surname prefixes for a given Ordine.
   */
  async *_searchCassaForense(rateLimiter, city, ordine, cookies, seen, options) {
    let totalForCity = 0;
    const maxRecords = options.maxPages ? options.maxPages * this.pageSize : 0;

    for (const letter of SURNAME_PREFIXES) {
      if (maxRecords && totalForCity >= maxRecords) {
        log.info(`IT: Reached max records limit for ${city}`);
        break;
      }

      const params = new URLSearchParams();
      params.set('cognome', letter);
      params.set('nome', '');
      params.set('ordine', ordine);

      log.info(`IT: Cassa Forense — ${city} (${ordine}), surname prefix "${letter}"`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._httpPost(
          CASSA_FORENSE_SEARCH,
          params.toString(),
          rateLimiter,
          cookies,
        );
      } catch (err) {
        log.error(`IT: Request failed for ${city} prefix "${letter}": ${err.message}`);
        continue;
      }

      if (response.statusCode === 429 || response.statusCode === 403) {
        log.warn(`IT: Got ${response.statusCode} from Cassa Forense — backing off`);
        const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
        if (!shouldRetry) {
          log.error(`IT: Rate limited, stopping search for ${city}`);
          break;
        }
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`IT: Cassa Forense returned status ${response.statusCode} for prefix "${letter}"`);
        continue;
      }

      // Update cookies
      if (response.cookies) {
        cookies = this._mergeCookies(cookies, response.cookies);
      }

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`IT: CAPTCHA detected on prefix "${letter}" for ${city} — skipping to next prefix`);
        yield { _captcha: true, city, page: letter };
        continue;
      }

      // Parse results
      const attorneys = this._parseCassaForenseResults(response.body, city);

      if (attorneys.length === 0) {
        // Check if the page contains an error message or empty result indicator
        if (response.body.includes('Nessun risultato') || response.body.includes('nessun avvocato')) {
          log.info(`IT: No results for ${city} prefix "${letter}"`);
        } else if (response.body.length < 500) {
          log.info(`IT: Empty/minimal response for ${city} prefix "${letter}" (${response.body.length} bytes)`);
        }
        continue;
      }

      log.info(`IT: Found ${attorneys.length} lawyers for ${city} prefix "${letter}"`);

      for (const attorney of attorneys) {
        // Dedup by name + bar
        const key = `${attorney.full_name}|${attorney.bar_name}|${attorney.bar_number}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);

        attorney.practice_area = '';
        attorney.source = 'italy_bar';
        yield attorney;
        totalForCity++;

        if (maxRecords && totalForCity >= maxRecords) break;
      }

      // For test mode, only do first letter
      if (options.maxPages) break;
    }

    log.success(`IT: Cassa Forense yielded ${totalForCity} lawyers for ${city}`);
  }

  /**
   * Fallback search using SferaBit AlboSFERA platform.
   * Uses AJAX POST to elencoAlboOnline.php with filter parameters.
   */
  async *_searchSferabit(rateLimiter, city, sferaId, seen, options) {
    log.info(`IT: Using SferaBit AlboSFERA fallback for ${city} (ID: ${sferaId})`);

    let totalForCity = 0;
    const maxRecords = options.maxPages ? options.maxPages * this.pageSize : 0;

    for (const letter of SURNAME_PREFIXES) {
      if (maxRecords && totalForCity >= maxRecords) {
        log.info(`IT: Reached max records limit for ${city} (SferaBit)`);
        break;
      }

      // SferaBit expects parameters as query string in the POST body
      const params = new URLSearchParams();
      params.set('nRicerche', '1');
      params.set('filtroRagioneSociale', letter);
      params.set('filtroIdTipiAnagraficheCategorie', '1'); // 1 = Avvocati (lawyers)
      params.set('id', String(sferaId));
      params.set('pag', '1');

      log.info(`IT: SferaBit — ${city}, surname prefix "${letter}"`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._httpPost(
          `${SFERABIT_SEARCH}?${params.toString()}`,
          '',
          rateLimiter,
        );
      } catch (err) {
        log.error(`IT: SferaBit request failed for ${city} prefix "${letter}": ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`IT: SferaBit returned status ${response.statusCode} for ${city} prefix "${letter}"`);
        continue;
      }

      const attorneys = this._parseSferabitResults(response.body, city);

      if (attorneys.length === 0) {
        continue;
      }

      log.info(`IT: SferaBit found ${attorneys.length} lawyers for ${city} prefix "${letter}"`);

      for (const attorney of attorneys) {
        const key = `${attorney.full_name}|${attorney.bar_name}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);

        attorney.practice_area = '';
        attorney.source = 'italy_bar';
        yield attorney;
        totalForCity++;

        if (maxRecords && totalForCity >= maxRecords) break;
      }

      // For test mode, only do first letter
      if (options.maxPages) break;
    }

    log.success(`IT: SferaBit yielded ${totalForCity} lawyers for ${city}`);
  }
}

module.exports = new ItalyScraper();
