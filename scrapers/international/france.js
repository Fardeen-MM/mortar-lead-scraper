/**
 * France National Lawyer Directory Scraper
 *
 * Source: https://www.data.gouv.fr/datasets/annuaire-des-avocats-de-france
 * Method: CSV download of the entire public lawyer directory, parsed with csv-parser
 *
 * The French government publishes an open-data CSV (Etalab 2.0 license) of all
 * registered lawyers (~79,000 records). This scraper downloads the CSV, parses
 * semicolon-delimited fields, and yields standardized attorney objects filtered
 * by target cities.
 *
 * CSV format: 14 semicolon-separated columns, latin1 (Windows-1252) encoding, ~9MB
 * Actual header names (as of 2026-01):
 *   Barreau, avNom, avPrenom, cbRaisonSociale, cbSiretSiren,
 *   cbAdresse1, cbAdresse2, cbCp, cbVille,
 *   spLibelle1, spLibelle2, spLibelle3, acDateSerment, AvLang
 *
 * City values are ALL CAPS, may include CEDEX suffixes (e.g., "PARIS CEDEX 08").
 */

const https = require('https');
const http = require('http');
const { Readable } = require('stream');
const csvParser = require('csv-parser');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

/** data.gouv.fr API endpoint for discovering the latest CSV URL */
const DATASET_API_URL = 'https://www.data.gouv.fr/api/1/datasets/annuaire-des-avocats-de-france/';

class FranceScraper extends BaseScraper {
  constructor() {
    super({
      name: 'france',
      stateCode: 'FR',
      baseUrl: 'https://www.data.gouv.fr/datasets/annuaire-des-avocats-de-france',
      pageSize: 50,
      practiceAreaCodes: {
        'family':              'Droit de la famille',
        'criminal':            'Droit pénal',
        'real estate':         'Droit immobilier',
        'corporate':           'Droit des sociétés',
        'employment':          'Droit du travail',
        'immigration':         'Droit des étrangers',
        'tax':                 'Droit fiscal',
        'intellectual property': 'Propriété intellectuelle',
      },
      defaultCities: [
        'Paris', 'Lyon', 'Marseille', 'Toulouse',
        'Bordeaux', 'Nantes', 'Strasbourg', 'Lille',
      ],
    });

    this.csvDownloadUrl =
      'https://static.data.gouv.fr/resources/annuaire-des-avocats-de-france/20260114-162250/annuaire-avocats-20260114.csv';
  }

  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden for CSV download`);
  }

  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden for CSV download`);
  }

  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden for CSV download`);
  }

  /**
   * HTTP GET that returns raw binary data (for CSV download).
   * Returns a Buffer body for binary-safe handling so we can decode
   * from latin1 (ANSI) encoding.
   */
  _httpGetRaw(url, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }

      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': '*/*',
          'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 120000, // 120s timeout for ~9MB file
      };

      const protocol = url.startsWith('https') ? https : http;
      const req = protocol.get(url, options, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          let redirect = res.headers.location;
          if (redirect.startsWith('/')) {
            const u = new URL(url);
            redirect = `${u.protocol}//${u.host}${redirect}`;
          }
          return resolve(this._httpGetRaw(redirect, rateLimiter, redirectCount + 1));
        }

        const chunks = [];
        res.on('data', chunk => { chunks.push(chunk); });
        res.on('end', () => resolve({
          statusCode: res.statusCode,
          body: Buffer.concat(chunks),
          contentType: res.headers['content-type'] || '',
        }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  /**
   * Discover the latest CSV URL from the data.gouv.fr API.
   * Falls back to the hardcoded URL if the API is unreachable.
   */
  async _discoverLatestCsvUrl(rateLimiter) {
    try {
      log.info('Querying data.gouv.fr API for latest CSV URL...');
      const response = await this._httpGetRaw(DATASET_API_URL, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`data.gouv.fr API returned status ${response.statusCode}, using hardcoded URL`);
        return this.csvDownloadUrl;
      }

      const data = JSON.parse(response.body.toString('utf-8'));
      const csvResources = (data.resources || []).filter(r =>
        r.format === 'csv' || (r.url && r.url.toLowerCase().endsWith('.csv'))
      );

      if (csvResources.length > 0) {
        // Resources are ordered by date, first = latest
        const latestUrl = csvResources[0].url;
        log.success(`Discovered latest CSV URL: ${latestUrl}`);
        return latestUrl;
      }

      log.warn('No CSV resources found in API response, using hardcoded URL');
      return this.csvDownloadUrl;
    } catch (err) {
      log.warn(`Failed to query data.gouv.fr API: ${err.message} — using hardcoded URL`);
      return this.csvDownloadUrl;
    }
  }

  /**
   * Parse semicolon-delimited CSV content into row objects using csv-parser.
   * Returns a promise that resolves to an array of row objects.
   */
  _parseCsvContent(csvContent) {
    return new Promise((resolve, reject) => {
      const rows = [];
      const stream = Readable.from([csvContent]);

      stream
        .pipe(csvParser({ separator: ';' }))
        .on('data', (row) => {
          rows.push(row);
        })
        .on('end', () => resolve(rows))
        .on('error', reject);
    });
  }

  /**
   * Normalize a raw CSV row into a standard attorney object.
   *
   * Actual CSV headers (as of 2026-01):
   *  0: Barreau          (bar association)
   *  1: avNom            (surname)
   *  2: avPrenom         (given name)
   *  3: cbRaisonSociale  (firm / business name)
   *  4: cbSiretSiren     (business ID)
   *  5: cbAdresse1       (address line 1)
   *  6: cbAdresse2       (address line 2)
   *  7: cbCp             (postal code)
   *  8: cbVille          (city)
   *  9: spLibelle1       (specialization 1)
   * 10: spLibelle2       (specialization 2)
   * 11: spLibelle3       (specialization 3)
   * 12: acDateSerment    (oath date)
   * 13: AvLang           (languages)
   *
   * The get() helper tries multiple possible header names to handle
   * potential future schema changes.
   */
  _normalizeRow(row) {
    // Helper: try multiple field names, return first non-empty match
    const get = (keys) => {
      for (const key of keys) {
        if (row[key] !== undefined && row[key] !== null) return String(row[key]).trim();
        // Case-insensitive fallback (strip underscores, spaces, hyphens)
        const keyNorm = key.toLowerCase().replace(/[_\s-]/g, '');
        const found = Object.keys(row).find(
          k => k.toLowerCase().replace(/[_\s-]/g, '') === keyNorm
        );
        if (found && row[found] !== undefined && row[found] !== null) {
          return String(row[found]).trim();
        }
      }
      return '';
    };

    // Map actual CSV headers AND legacy/alternative names for robustness
    const lastName    = get(['avNom', 'Nom', 'nom', 'NOM']);
    const firstName   = get(['avPrenom', 'Prénom', 'Prenom', 'prénom', 'prenom', 'PRENOM', 'PRÉNOM']);
    const firmName    = get(['cbRaisonSociale', 'Structure', 'structure', 'STRUCTURE']);
    const city        = get(['cbVille', 'Ville', 'ville', 'VILLE']);
    const postalCode  = get(['cbCp', 'Code postal', 'Code Postal', 'CodePostal', 'codepostal', 'CODE POSTAL']);
    const address1    = get(['cbAdresse1', 'Adresse1', 'Adresse 1', 'adresse1']);
    const address2    = get(['cbAdresse2', 'Adresse2', 'Adresse 2', 'adresse2']);
    const siren       = get(['cbSiretSiren', 'SIREN', 'siren', 'Siren', 'SiretSiren']);
    const spec1       = get(['spLibelle1', 'Spécialité 1', 'Specialite 1', 'Spécialité1', 'Specialite1', 'SPECIALITE 1']);
    const spec2       = get(['spLibelle2', 'Spécialité 2', 'Specialite 2', 'Spécialité2', 'Specialite2', 'SPECIALITE 2']);
    const spec3       = get(['spLibelle3', 'Spécialité 3', 'Specialite 3', 'Spécialité3', 'Specialite3', 'SPECIALITE 3']);
    const oathDate    = get(['acDateSerment', 'Date de serment', 'DateSerment', 'Date serment', 'DATE DE SERMENT']);
    const barreau     = get(['Barreau', 'barreau', 'BARREAU']);
    const languages   = get(['AvLang', 'Langues', 'langues', 'LANGUES']);

    const fullName = [firstName, lastName].filter(Boolean).join(' ');

    // Combine address lines
    const fullAddress = [address1, address2].filter(Boolean).join(', ');

    // Combine specializations into a single practice area string
    const specializations = [spec1, spec2, spec3]
      .filter(Boolean)
      .join(', ');

    return {
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      firm_name: firmName,
      city: city,
      state: 'FR',
      postal_code: postalCode,
      address: fullAddress,
      phone: '',
      email: '',
      website: '',
      bar_number: siren,
      bar_name: barreau,
      bar_status: 'Inscrit',
      oath_date: oathDate,
      specializations: specializations,
      languages: languages,
      profile_url: '',
      source: 'france_bar',
    };
  }

  /**
   * Match a practice area filter against the lawyer's specializations.
   * Returns true if any of the lawyer's French specializations match.
   */
  _matchesPracticeArea(attorney, practiceArea) {
    if (!practiceArea) return true;

    const frenchTerm = this.resolvePracticeCode(practiceArea);
    if (!frenchTerm) return true; // Unknown practice area, don't filter

    const specs = (attorney.specializations || '').toLowerCase();
    return specs.includes(frenchTerm.toLowerCase());
  }

  /**
   * Extract the city name from the raw city field for comparison.
   *
   * French CSV city values are ALL CAPS and may include:
   *   - CEDEX suffixes: "PARIS CEDEX 08", "LYON CEDEX 03"
   *   - Arrondissement numbers: "MARSEILLE 01", "LYON 03"
   *   - Numeric city code prefix: "75056 PARIS" (less common now)
   *
   * This method strips CEDEX suffixes, arrondissement numbers, and
   * numeric prefixes so "PARIS CEDEX 08" -> "PARIS" for matching.
   */
  _extractCityName(rawCity) {
    if (!rawCity) return '';
    let cleaned = rawCity.trim();

    // Remove leading numeric city code if present (e.g., "75056 PARIS" -> "PARIS")
    cleaned = cleaned.replace(/^\d+\s+/, '');

    // Remove CEDEX and everything after it (e.g., "PARIS CEDEX 08" -> "PARIS")
    cleaned = cleaned.replace(/\s+CEDEX\b.*/i, '');

    // Remove trailing arrondissement numbers for major cities
    // e.g., "MARSEILLE 01" -> "MARSEILLE", "LYON 03" -> "LYON"
    cleaned = cleaned.replace(/\s+\d{1,2}$/, '');

    return cleaned.trim();
  }

  /**
   * Build a Set of normalized city names for efficient matching.
   * This set includes the lowercase base name of each target city
   * so that "PARIS", "PARIS CEDEX 08", "PARIS 01" all match "Paris".
   */
  _buildCityMatchSet(cities) {
    return new Set(cities.map(c => c.toLowerCase().trim()));
  }

  /**
   * Async generator that yields attorney records.
   *
   * Strategy:
   *  1. Discover the latest CSV URL (API fallback)
   *  2. Download the full CSV from data.gouv.fr
   *  3. Parse semicolon-delimited rows
   *  4. Filter by target cities and optional practice area
   *  5. Yield standardized attorney objects
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const citySet = this._buildCityMatchSet(cities);

    log.info(`Downloading French lawyer directory from data.gouv.fr...`);
    log.info(`Target cities: ${cities.join(', ')}`);

    // --- Resolve the CSV URL (try hardcoded first, fall back to API discovery) ---
    let csvUrl = this.csvDownloadUrl;
    let csvContent = null;

    try {
      await rateLimiter.wait();
      let response = await this._httpGetRaw(csvUrl, rateLimiter);

      // If the hardcoded URL fails (404, 403, etc.), discover the latest via API
      if (response.statusCode !== 200) {
        log.warn(`Hardcoded CSV URL returned ${response.statusCode} — discovering latest URL from API...`);
        csvUrl = await this._discoverLatestCsvUrl(rateLimiter);
        await rateLimiter.wait();
        response = await this._httpGetRaw(csvUrl, rateLimiter);
      }

      if (response.statusCode !== 200) {
        log.error(`CSV download returned status ${response.statusCode}`);
        return;
      }

      // Decode from latin1 (ANSI / Windows-1252) encoding
      csvContent = response.body.toString('latin1');

      // If the content looks garbled in latin1, try UTF-8
      if (!csvContent.includes(';')) {
        log.warn('latin1 decode did not find semicolons, trying UTF-8');
        csvContent = response.body.toString('utf-8');
      }

      const lineCount = csvContent.split('\n').length;
      log.success(`Downloaded CSV — ${lineCount} lines (${(response.body.length / 1024 / 1024).toFixed(1)} MB)`);
    } catch (err) {
      log.error(`Failed to download French lawyer CSV: ${err.message}`);

      // Last resort: try API discovery if hardcoded URL threw an error
      try {
        log.info('Attempting API discovery as fallback...');
        csvUrl = await this._discoverLatestCsvUrl(rateLimiter);
        if (csvUrl !== this.csvDownloadUrl) {
          await rateLimiter.wait();
          const response = await this._httpGetRaw(csvUrl, rateLimiter);
          if (response.statusCode === 200) {
            csvContent = response.body.toString('latin1');
            if (!csvContent.includes(';')) {
              csvContent = response.body.toString('utf-8');
            }
            const lineCount = csvContent.split('\n').length;
            log.success(`Fallback download succeeded — ${lineCount} lines (${(response.body.length / 1024 / 1024).toFixed(1)} MB)`);
          }
        }
      } catch (fallbackErr) {
        log.error(`Fallback also failed: ${fallbackErr.message}`);
      }

      if (!csvContent) return;
    }

    // --- Parse the CSV ---
    let rows;
    try {
      rows = await this._parseCsvContent(csvContent);
    } catch (err) {
      log.error(`Failed to parse CSV: ${err.message}`);
      return;
    }

    if (!rows || rows.length === 0) {
      log.error('CSV parsed but contained no data rows');
      return;
    }

    log.success(`Parsed ${rows.length.toLocaleString()} lawyer records from CSV`);

    // Log actual CSV headers for debugging
    if (rows.length > 0) {
      const headers = Object.keys(rows[0]);
      log.info(`CSV headers: ${headers.join(', ')}`);
    }

    // Report progress
    yield { _cityProgress: { current: 1, total: cities.length } };

    // --- Filter and yield ---
    let yieldCount = 0;
    const maxRecords = options.maxPages ? options.maxPages * this.pageSize : 0;

    for (const row of rows) {
      const attorney = this._normalizeRow(row);

      // Filter by target cities
      const cityName = this._extractCityName(attorney.city);
      if (!cityName || !citySet.has(cityName.toLowerCase())) {
        continue;
      }

      // Normalize the city to title case for clean output
      attorney.city = this._titleCase(cityName);

      // Filter by practice area if specified
      if (practiceArea && !this._matchesPracticeArea(attorney, practiceArea)) {
        continue;
      }

      // Set the requested practice area on the result
      attorney.practice_area = practiceArea || '';

      yield attorney;
      yieldCount++;

      // Respect maxPages as a record limit (pageSize * maxPages)
      if (maxRecords && yieldCount >= maxRecords) {
        log.info(`Reached max records limit (${maxRecords}) from CSV`);
        return;
      }
    }

    log.success(`Yielded ${yieldCount.toLocaleString()} lawyers from CSV for target cities`);
  }

  /**
   * Convert an ALL-CAPS city name to Title Case.
   * "PARIS" -> "Paris", "AIX EN PROVENCE" -> "Aix En Provence"
   */
  _titleCase(str) {
    if (!str) return '';
    return str
      .toLowerCase()
      .replace(/(?:^|\s)\S/g, (c) => c.toUpperCase());
  }
}

module.exports = new FranceScraper();
