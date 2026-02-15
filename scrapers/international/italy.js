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
 *     we use common 3-letter Italian surname prefixes for broad coverage.
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
 * Cassa Forense URLs.
 * The main page is used for session init (GET to collect cookies + ordine values).
 * Search results come from an AJAX POST to retRicerca.cfm, not the main page.
 * Pagination uses GET to retRicerca.cfm?start=N&cognome=X&nome=&ordine=Y.
 */
const CASSA_FORENSE_BASE = 'https://servizi.cassaforense.it/CFor/ElencoNazionaleAvvocati';
const CASSA_FORENSE_MAIN = `${CASSA_FORENSE_BASE}/elenconazionaleavvocati_pg.cfm`;
const CASSA_FORENSE_AJAX = `${CASSA_FORENSE_BASE}/retRicerca.cfm`;

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
 * Surname prefixes for enumeration.
 * Cassa Forense requires minimum 3 characters for surname search.
 * We use common Italian 3-letter surname prefixes to get broad coverage.
 * In test mode (maxPages), only the first prefix is used.
 */
const SURNAME_PREFIXES = [
  'Abb', 'Acc', 'Agn', 'Alb', 'Ale', 'Ama', 'And', 'Ang', 'Ann', 'Ant', 'Ard', 'Are',
  'Bar', 'Bas', 'Bel', 'Ben', 'Ber', 'Bia', 'Bon', 'Bor', 'Bra', 'Bri', 'Bru', 'Buo',
  'Cac', 'Cal', 'Cam', 'Cap', 'Car', 'Cas', 'Cat', 'Cav', 'Cel', 'Cer', 'Chi', 'Cia',
  'Col', 'Con', 'Cor', 'Cos', 'Cri', 'Cro', 'Cuc',
  'Dal', 'Dam', 'Dan', 'DeA', 'DeL', 'DeM', 'DeR', 'DeS', 'Del', 'DiB', 'DiM', 'DiP',
  'Esp', 'Fab', 'Fal', 'Fan', 'Far', 'Fas', 'Fer', 'Fil', 'Fio', 'For', 'Fra', 'Fur',
  'Gal', 'Gar', 'Gas', 'Gen', 'Ghi', 'Gia', 'Gio', 'Gir', 'Giu', 'Gra', 'Gre', 'Gri',
  'Gua', 'Gue', 'Imp', 'Ing',
  'Lac', 'Lam', 'Lan', 'Lat', 'Leo', 'Lic', 'Lom', 'Lon', 'Lor', 'Luc',
  'Mac', 'Mag', 'Mai', 'Man', 'Mar', 'Mas', 'Mat', 'Maz', 'Mel', 'Mer', 'Mic', 'Min',
  'Mon', 'Mor', 'Mos', 'Mur', 'Mus',
  'Nap', 'Nar', 'Neg', 'Ner', 'Nic', 'Nob', 'Noc',
  'Oli', 'Orl', 'Pac', 'Pag', 'Pal', 'Pan', 'Pap', 'Par', 'Pas', 'Pel', 'Per', 'Pet',
  'Pia', 'Pic', 'Pie', 'Pin', 'Pir', 'Pis', 'Pol', 'Pom', 'Por', 'Pri', 'Pro', 'Pug',
  'Rag', 'Rai', 'Ram', 'Ran', 'Rav', 'Reg', 'Ric', 'Rig', 'Rin', 'Ris', 'Roc', 'Rom',
  'Ros', 'Rot', 'Rub', 'Rus', 'Sab', 'Sal', 'San', 'Sar', 'Sav', 'Sca', 'Sch', 'Sci',
  'Ser', 'Sil', 'Sim', 'Sol', 'Sor', 'Spa', 'Spi', 'Sta', 'Ste', 'Str',
  'Tab', 'Tar', 'Ter', 'Tic', 'Tor', 'Tra', 'Tri', 'Tro', 'Tur',
  'Val', 'Van', 'Vas', 'Vec', 'Ven', 'Ver', 'Vic', 'Vil', 'Vis', 'Vit', 'Vol',
  'Zam', 'Zan', 'Zap', 'Zuc',
];

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
          'Referer': CASSA_FORENSE_MAIN,
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
  // Cassa Forense AJAX response parsing
  // -------------------------------------------------------------------------

  /**
   * Parse the AJAX HTML fragment returned by retRicerca.cfm.
   *
   * Structure:
   *   <div class="organigramma-block">
   *     <ul class="list list-personas">
   *       <li class="persona">
   *         <h4 class="persona-name">Avv. COGNOME NOME</h4>
   *         <div class="sede persona-sede">
   *           <p class="sede-name">Luogo di nascita : <span class="sede-city">CITY</span></p>
   *           <p class="address sede-address"><b>Data di nascita : </b>**\/**\/Y-YY</p>
   *         </div>
   *         <div class="sede persona-sede">
   *           <p class="sede-name">Consiglio dell'Ordine di <span class="sede-city">ORDINE</span></p>
   *         </div>
   *       </li>
   *     </ul>
   *   </div>
   */
  _parseCassaForenseAjax(html, searchCity) {
    const $ = cheerio.load(html);
    const results = [];

    $('li.persona').each((_, el) => {
      const $el = $(el);

      // Extract name from h4.persona-name: "Avv. COGNOME NOME"
      let rawName = $el.find('h4.persona-name').text().trim();
      if (!rawName || rawName.length < 3) return;

      // Remove "Avv. " prefix
      rawName = rawName.replace(/^Avv\.\s*/i, '').trim();

      const nameParts = this._parseItalianName(rawName);
      if (!nameParts.lastName) return;

      // Extract bar association (Ordine) from second sede block
      let barName = '';
      $el.find('.sede-city').each((i, span) => {
        const parentText = $(span).parent().text();
        if (parentText.includes("Ordine di") || parentText.includes("dell'Ordine")) {
          barName = $(span).text().trim();
        }
      });

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
        bar_name: barName ? `Ordine Avvocati ${barName}` : `Ordine Avvocati ${searchCity}`,
        bar_status: 'Iscritto',
        profile_url: '',
        practice_area: '',
        source: 'italy_bar',
      });
    });

    return results;
  }

  /**
   * Parse lawyer results from the Cassa Forense full-page HTML response (legacy).
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
    let ordineMap = {}; // Map of city name -> padded ordine value from <select>

    try {
      log.info('IT: Initializing session with Cassa Forense...');
      await rateLimiter.wait();
      const initResponse = await this._httpGetWithCookies(CASSA_FORENSE_MAIN, rateLimiter);

      if (initResponse.statusCode === 200) {
        cookies = initResponse.cookies;
        sessionOk = true;
        log.success('IT: Cassa Forense session established');

        // Check for CAPTCHA or JS-rendering requirements
        if (this.detectCaptcha(initResponse.body)) {
          log.warn('IT: CAPTCHA detected on Cassa Forense — will attempt SferaBit fallback');
          sessionOk = false;
        }

        // Extract ordine values from <select name="Ordine"> options
        // The values are padded with spaces to ~50 chars which is required for the search
        const $ = cheerio.load(initResponse.body);
        $('select[name="Ordine"] option').each((_, opt) => {
          const val = $(opt).attr('value') || '';
          const text = val.trim();
          if (text && text !== 'Seleziona Tutti') {
            ordineMap[text] = val; // Store padded value
          }
        });
        log.info(`IT: Extracted ${Object.keys(ordineMap).length} ordine values from form`);
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

      const ordineName = CITY_TO_ORDINE[city] || city.toUpperCase();
      // Use the padded ordine value from the form, or fall back to the name
      const ordineValue = ordineMap[ordineName] || ordineName;

      if (sessionOk) {
        // Primary path: Cassa Forense AJAX search via retRicerca.cfm
        yield* this._searchCassaForense(
          rateLimiter, city, ordineValue, cookies, seen, options
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
   *
   * The search uses the AJAX endpoint retRicerca.cfm (not the main page).
   * Initial search: POST with form fields (cognome, nome, Ordine, hd* copies).
   * Pagination: GET retRicerca.cfm?start=N&cognome=X&nome=&ordine=Y (5 per page).
   */
  async *_searchCassaForense(rateLimiter, city, ordine, cookies, seen, options) {
    let totalForCity = 0;
    const maxRecords = options.maxPages ? options.maxPages * this.pageSize : 0;
    const RESULTS_PER_PAGE = 5; // Cassa Forense returns 5 per AJAX page

    for (const prefix of SURNAME_PREFIXES) {
      if (maxRecords && totalForCity >= maxRecords) {
        log.info(`IT: Reached max records limit for ${city}`);
        break;
      }

      log.info(`IT: Cassa Forense — ${city} (${ordine.trim()}), surname prefix "${prefix}"`);

      // Initial AJAX POST (page 1)
      // Mimic the getquerystring() serialization from the JS form handler
      const formFields = [
        ['cognome', prefix],
        ['nome', ''],
        ['Ordine', ordine],
        ['hdnome', ''],
        ['hdcognome', prefix],
        ['hdordine', ordine],
      ];
      const formBody = formFields
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v || '')}`)
        .join('&');

      let response;
      try {
        await rateLimiter.wait();
        response = await this._httpPost(
          CASSA_FORENSE_AJAX,
          formBody,
          rateLimiter,
          cookies,
        );
      } catch (err) {
        log.error(`IT: Request failed for ${city} prefix "${prefix}": ${err.message}`);
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
        log.warn(`IT: Cassa Forense returned status ${response.statusCode} for prefix "${prefix}"`);
        continue;
      }

      // Update cookies
      if (response.cookies) {
        cookies = this._mergeCookies(cookies, response.cookies);
      }

      // Check for CAPTCHA
      if (this.detectCaptcha(response.body)) {
        log.warn(`IT: CAPTCHA detected on prefix "${prefix}" for ${city} — skipping to next prefix`);
        yield { _captcha: true, city, page: prefix };
        continue;
      }

      // Extract total record count from the AJAX response
      // The response contains: qryTot = NNN; in a <script> block
      // and #totRec with "Record da X a Y di Z"
      let totalResults = 0;
      const qryMatch = response.body.match(/qryTot\s*=\s*(\d+)/);
      if (qryMatch) totalResults = parseInt(qryMatch[1], 10);

      if (totalResults === 0) {
        // No results for this prefix
        if (response.body.length < 200) {
          log.info(`IT: No results for ${city} prefix "${prefix}"`);
        }
        continue;
      }

      const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
      log.info(`IT: Found ${totalResults} lawyers for ${city} prefix "${prefix}" (${totalPages} pages)`);

      // Parse first page results
      const firstPageAttorneys = this._parseCassaForenseAjax(response.body, city);
      for (const attorney of firstPageAttorneys) {
        const key = `${attorney.full_name}|${attorney.bar_name}`.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        yield attorney;
        totalForCity++;
        if (maxRecords && totalForCity >= maxRecords) break;
      }

      if (maxRecords && totalForCity >= maxRecords) break;

      // Paginate through remaining pages (start=6, 11, 16, ...)
      // In test mode, limit to first page
      const maxPagesToFetch = options.maxPages ? 1 : totalPages;
      for (let page = 2; page <= maxPagesToFetch; page++) {
        if (maxRecords && totalForCity >= maxRecords) break;

        const start = (page - 1) * RESULTS_PER_PAGE + 1;
        const pageUrl = `${CASSA_FORENSE_AJAX}?start=${start}&cognome=${encodeURIComponent(prefix)}&nome=&ordine=${encodeURIComponent(ordine)}`;

        let pageResp;
        try {
          await rateLimiter.wait();
          pageResp = await this._httpGetWithCookies(pageUrl, rateLimiter, cookies);
        } catch (err) {
          log.error(`IT: Pagination failed for ${city} prefix "${prefix}" page ${page}: ${err.message}`);
          break;
        }

        if (pageResp.statusCode !== 200) break;
        if (pageResp.cookies) cookies = this._mergeCookies(cookies, pageResp.cookies);

        const pageAttorneys = this._parseCassaForenseAjax(pageResp.body, city);
        if (pageAttorneys.length === 0) break;

        for (const attorney of pageAttorneys) {
          const key = `${attorney.full_name}|${attorney.bar_name}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);
          yield attorney;
          totalForCity++;
          if (maxRecords && totalForCity >= maxRecords) break;
        }
      }

      // For test mode, only do first prefix
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

    for (const prefix of SURNAME_PREFIXES) {
      if (maxRecords && totalForCity >= maxRecords) {
        log.info(`IT: Reached max records limit for ${city} (SferaBit)`);
        break;
      }

      // SferaBit expects parameters as query string in the POST body
      const params = new URLSearchParams();
      params.set('nRicerche', '1');
      params.set('filtroRagioneSociale', prefix);
      params.set('filtroIdTipiAnagraficheCategorie', '1'); // 1 = Avvocati (lawyers)
      params.set('id', String(sferaId));
      params.set('pag', '1');

      log.info(`IT: SferaBit — ${city}, surname prefix "${prefix}"`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._httpPost(
          `${SFERABIT_SEARCH}?${params.toString()}`,
          '',
          rateLimiter,
        );
      } catch (err) {
        log.error(`IT: SferaBit request failed for ${city} prefix "${prefix}": ${err.message}`);
        continue;
      }

      if (response.statusCode !== 200) {
        log.warn(`IT: SferaBit returned status ${response.statusCode} for ${city} prefix "${prefix}"`);
        continue;
      }

      const attorneys = this._parseSferabitResults(response.body, city);

      if (attorneys.length === 0) {
        continue;
      }

      log.info(`IT: SferaBit found ${attorneys.length} lawyers for ${city} prefix "${prefix}"`);

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

      // For test mode, only do first prefix
      if (options.maxPages) break;
    }

    log.success(`IT: SferaBit yielded ${totalForCity} lawyers for ${city}`);
  }
}

module.exports = new ItalyScraper();
