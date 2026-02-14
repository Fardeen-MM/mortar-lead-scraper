/**
 * Northern Territory (AU-NT) Law Society -- Legal Practitioners Scraper
 *
 * Source: https://lawsocietynt.asn.au/index.php/current-nt-legal-practitioners/
 * Method: PDF download + text extraction (no API or HTML directory available)
 *
 * The Law Society NT publishes its register of current legal practitioners
 * as downloadable PDF files on a WordPress site. The PDFs are generated
 * from Microsoft Excel and contain structured tabular data:
 *
 *   Index page: /index.php/current-nt-legal-practitioners/
 *   PDFs (updated regularly, e.g. 2025/26 practising certificate year):
 *     - Barristers
 *     - Legal Practitioners A-F
 *     - Legal Practitioners G-L
 *     - Legal Practitioners M-R
 *     - Legal Practitioners S-Z
 *
 * Each PDF contains columns:
 *   Surname | First Name | Type | Firm Name | Firm Street Address | Suburb | Postcode | Telephone
 *
 * Type codes:
 *   B   = Barrister
 *   U   = Unrestricted practising certificate
 *   RBS = Restricted practising certificate (government/corporate employee)
 *
 * Additionally, the site has a "Firms by Area of Law" page with per-area PDFs
 * organized by location (Darwin, Katherine, Alice Springs):
 *   /index.php/nt-law-firms-by-area-of-law/
 *
 * Strategy:
 *   1. Fetch the practitioner index page to discover current PDF URLs
 *   2. Download each PDF
 *   3. Decompress FlateDecode streams and extract text via PDF text operators
 *   4. Parse the columnar data into structured records
 *   5. Yield practitioner records
 *
 * ~400-600 practitioners are registered in the NT.
 */

const https = require('https');
const http = require('http');
const zlib = require('zlib');
const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

class NtScraper extends BaseScraper {
  constructor() {
    super({
      name: 'australia-nt',
      stateCode: 'AU-NT',
      baseUrl: 'https://lawsocietynt.asn.au',
      pageSize: 100,
      practiceAreaCodes: {
        'accident compensation':          'Accident compensation and personal injuries',
        'personal injury':                'Accident compensation and personal injuries',
        'administrative law':             'Administrative law',
        'administrative':                 'Administrative law',
        'adoption':                       'Adoption',
        'aviation':                       'Aviation and aircraft',
        'banking':                        'Banking and finance',
        'banking and finance':            'Banking and finance',
        'bankruptcy':                     'Bankruptcy and insolvency',
        'insolvency':                     'Bankruptcy and insolvency',
        'building':                       'Building and construction',
        'building and construction':      'Building and construction',
        'construction':                   'Building and construction',
        'business':                       'Business and commercial',
        'business and commercial':        'Business and commercial',
        'commercial':                     'Business and commercial',
        'civil':                          'Civil litigation',
        'civil litigation':               'Civil litigation',
        'constitutional':                 'Constitutional law',
        'consumer':                       'Consumer and credit law',
        'consumer law':                   'Consumer and credit law',
        'contract':                       'Contract law',
        'conveyancing':                   'Conveyancing',
        'copyright':                      'Copyright',
        'criminal':                       'Criminal law',
        'criminal law':                   'Criminal law',
        'debt':                           'Debt collection',
        'debt collection':                'Debt collection',
        'dispute resolution':             'Dispute resolution',
        'domestic violence':              'Domestic and family violence',
        'family violence':                'Domestic and family violence',
        'elder':                          'Elder Law',
        'elder law':                      'Elder Law',
        'environment':                    'Environment',
        'environmental':                  'Environment',
        'equity':                         'Equity and trust',
        'equity and trusts':              'Equity and trust',
        'trusts':                         'Equity and trust',
        'family':                         'Family and relationships',
        'family law':                     'Family and relationships',
        'government':                     'Government',
        'employment':                     'Industrial and employment law',
        'industrial':                     'Industrial and employment law',
        'insurance':                      'Insurance',
        'intellectual property':          'Intellectual property',
        'international':                  'International law',
        'international law':              'International law',
        'leases':                         'Leases and real property',
        'real property':                  'Leases and real property',
        'property':                       'Leases and real property',
        'liquor':                         'Liquor licensing',
        'maritime':                       'Maritime law',
        'media':                          'Media and communications',
        'medical negligence':             'Medical negligence',
        'migration':                      'Migration',
        'immigration':                    'Migration',
        'mining':                         'Mining',
        'motor vehicle':                  'Motor vehicle accident compensation',
        'native title':                   'Native Title and land claims',
        'privacy':                        'Privacy',
        'professional negligence':        'Professional negligence',
        'public liability':               'Public liability',
        'sport':                          'Sport law',
        'taxation':                       'Taxation and revenue',
        'tax':                            'Taxation and revenue',
        'tort':                           'TORT (damages and defamation)',
        'defamation':                     'TORT (damages and defamation)',
        'veterans':                       'Veterans entitlements',
        'wills':                          'Wills and estates',
        'estates':                        'Wills and estates',
        'wills and estates':              'Wills and estates',
        'work health':                    'Work health',
      },
      defaultCities: ['Darwin', 'Alice Springs', 'Katherine'],
    });

    this.practitionerIndexUrl = `${this.baseUrl}/index.php/current-nt-legal-practitioners/`;

    // Type code descriptions
    this.typeCodes = {
      'B':   'Barrister',
      'U':   'Unrestricted Practising Certificate',
      'RBS': 'Restricted Practising Certificate',
    };
  }

  // --- BaseScraper overrides (not used since search() is fully overridden) ---

  buildSearchUrl() {
    return this.practitionerIndexUrl;
  }

  parseResultsPage() {
    return [];
  }

  extractResultCount() {
    return 0;
  }

  // --- PDF download and parsing ---

  /**
   * Download a file (PDF) from a URL. Returns a Buffer.
   */
  _downloadFile(url, rateLimiter) {
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'application/pdf,*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      };

      const protocol = url.startsWith('https') ? https : http;

      const makeRequest = (reqUrl, redirectCount = 0) => {
        if (redirectCount > 5) {
          return reject(new Error(`Too many redirects for ${url}`));
        }

        protocol.get(reqUrl, options, (res) => {
          if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            res.resume();
            let redirect = res.headers.location;
            if (redirect.startsWith('/')) {
              const u = new URL(reqUrl);
              redirect = `${u.protocol}//${u.host}${redirect}`;
            }
            return makeRequest(redirect, redirectCount + 1);
          }

          if (res.statusCode !== 200) {
            res.resume();
            return reject(new Error(`HTTP ${res.statusCode} downloading ${url}`));
          }

          const chunks = [];
          res.on('data', chunk => chunks.push(chunk));
          res.on('end', () => resolve(Buffer.concat(chunks)));
          res.on('error', reject);
        }).on('error', reject);
      };

      makeRequest(url);
    });
  }

  /**
   * Extract text from a PDF buffer by decompressing FlateDecode streams
   * and parsing PDF text operators (Tj and TJ).
   *
   * This is a lightweight PDF text extraction that handles the specific
   * format used by the Law Society NT's Excel-generated PDFs without
   * requiring external PDF parsing libraries.
   *
   * @param {Buffer} pdfBuffer - Raw PDF file contents
   * @returns {string[]} Array of text strings in document order
   */
  _extractTextFromPdf(pdfBuffer) {
    const data = pdfBuffer;
    const textParts = [];

    // Find all FlateDecode compressed streams
    const streamRegex = /stream\r?\n/g;
    let match;

    while ((match = streamRegex.exec(data)) !== null) {
      const start = match.index + match[0].length;

      // Find the corresponding endstream
      const endIdx = data.indexOf('endstream', start);
      if (endIdx === -1) continue;

      // Look backwards from endstream for the actual data end (skip whitespace)
      let dataEnd = endIdx;
      while (dataEnd > start && (data[dataEnd - 1] === 0x0A || data[dataEnd - 1] === 0x0D)) {
        dataEnd--;
      }

      const streamData = data.slice(start, dataEnd);

      try {
        const decompressed = zlib.inflateSync(streamData);
        const text = decompressed.toString('latin1');

        // Extract text from TJ arrays: [(text1) kerning (text2)] TJ
        const tjArrayRegex = /\[(.*?)\]\s*TJ/g;
        let tjMatch;
        while ((tjMatch = tjArrayRegex.exec(text)) !== null) {
          const parts = [];
          const partRegex = /\((.*?)\)/g;
          let partMatch;
          while ((partMatch = partRegex.exec(tjMatch[1])) !== null) {
            parts.push(partMatch[1]);
          }
          const line = parts.join('');
          if (line.trim()) {
            textParts.push(line.trim());
          }
        }

        // Extract text from simple Tj operators: (text) Tj
        const tjRegex = /\((.*?)\)\s*Tj/g;
        let simpleMatch;
        while ((simpleMatch = tjRegex.exec(text)) !== null) {
          if (simpleMatch[1].trim()) {
            textParts.push(simpleMatch[1].trim());
          }
        }
      } catch (e) {
        // Not a FlateDecode stream or decompression failed -- skip
      }
    }

    return textParts;
  }

  /**
   * Parse extracted PDF text lines into structured practitioner records.
   *
   * The PDF format is a repeating table with columns:
   *   Surname | First Name | Type | Firm Name | Firm Street Address | Suburb | Postcode | Telephone
   *
   * The challenge is that some fields span multiple lines (especially
   * Firm Name and Firm Street Address). We use heuristics to reconstruct
   * the record boundaries.
   *
   * @param {string[]} textLines - Array of text strings from _extractTextFromPdf
   * @returns {object[]} Array of practitioner objects
   */
  _parsePractitionerLines(textLines) {
    const practitioners = [];

    // Column headers to skip
    const headerFields = new Set([
      'Surname', 'First Name', 'Type', 'Firm Name',
      'Firm Street Address', 'Suburb', 'Postcode', 'Telephone',
    ]);

    // Build a state machine to parse the records
    // Each record starts with a surname (text that doesn't match a known
    // continuation pattern) and is followed by first name, type, firm, address, etc.

    let i = 0;
    while (i < textLines.length) {
      const line = textLines[i];

      // Skip headers
      if (headerFields.has(line)) {
        i++;
        continue;
      }

      // A new record starts with a surname.
      // We detect a surname by checking that:
      // 1. It's a word (not a number, not a postcode)
      // 2. The next line looks like a first name
      // 3. The line after that looks like a type code (B, U, RBS, etc.)

      if (this._looksLikeSurname(line, textLines, i)) {
        const record = this._extractRecord(textLines, i);
        if (record) {
          practitioners.push(record);
          i = record._nextIndex;
          continue;
        }
      }

      i++;
    }

    return practitioners;
  }

  /**
   * Check if a line looks like the start of a new practitioner record (surname).
   */
  _looksLikeSurname(line, allLines, index) {
    // Must be alphabetic (with possible spaces, hyphens, apostrophes)
    if (!/^[A-Za-z][A-Za-z\s'\-]+$/.test(line)) return false;

    // Must not be a type code
    if (['B', 'U', 'RBS'].includes(line)) return false;

    // Must not look like a suburb
    if (/^\d{4}$/.test(line)) return false;

    // Next line should exist and look like a first name
    if (index + 1 >= allLines.length) return false;
    const nextLine = allLines[index + 1];

    // First name is usually a single word or two
    if (!/^[A-Za-z][A-Za-z\s'\-]*$/.test(nextLine)) return false;

    // Check if a type code appears at index+2 (directly after first name).
    // This is the standard position for the type code in a well-formed record.
    if (index + 2 < allLines.length && ['B', 'U', 'RBS'].includes(allLines[index + 2])) {
      return true;
    }

    // If the next-next line is not a type code, check if it looks like a
    // firm name. This handles records where the type code is missing but
    // the firm name follows immediately after the first name.
    if (index + 2 < allLines.length) {
      const afterFirst = allLines[index + 2];
      // If it contains words that look like a firm name, this is likely a record
      if (afterFirst.includes('|') || afterFirst.includes('Pty') ||
          afterFirst.includes('Ltd') || afterFirst.includes('Law') ||
          afterFirst.includes('Legal') || afterFirst.includes('Solicitor') ||
          afterFirst.includes('Department') || afterFirst.includes('Commission') ||
          afterFirst.includes('Agency') || afterFirst.includes('Aboriginal') ||
          afterFirst.includes('Office') || afterFirst.includes('Services') ||
          afterFirst.includes('Counsel') || afterFirst.includes('Chambers')) {
        return true;
      }
    }

    return false;
  }

  /**
   * Extract a complete practitioner record starting from a given index.
   * Returns the record object with a _nextIndex field indicating where
   * the next record starts.
   */
  _extractRecord(lines, startIndex) {
    const headerFields = new Set([
      'Surname', 'First Name', 'Type', 'Firm Name',
      'Firm Street Address', 'Suburb', 'Postcode', 'Telephone',
    ]);

    let i = startIndex;
    const surname = lines[i++];

    // First name
    if (i >= lines.length) return null;
    const firstName = lines[i++];

    // Type code (B, U, RBS) -- may be missing
    let type = '';
    if (i < lines.length && ['B', 'U', 'RBS'].includes(lines[i])) {
      type = lines[i++];
    }

    // If no type code was found and the very next line starts a new record,
    // this practitioner has no firm/address data -- yield a minimal record
    if (!type && i < lines.length && this._looksLikeSurname(lines[i], lines, i)) {
      return {
        first_name: firstName,
        last_name: surname,
        full_name: `${firstName} ${surname}`.trim(),
        firm_name: '',
        city: '',
        state: 'NT',
        zip: '',
        country: 'Australia',
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: '',
        admission_date: '',
        profile_url: '',
        practice_areas: '',
        practitioner_type: '',
        address: '',
        _nextIndex: i,
      };
    }

    // Firm name -- collect until we hit something that looks like an address
    // Firm names often contain '|' separators and can span multiple lines
    let firmName = '';
    const firmParts = [];
    while (i < lines.length) {
      const line = lines[i];

      // Stop if we hit a header
      if (headerFields.has(line)) break;

      // Stop if we hit a postcode (4-digit number)
      if (/^\d{4}$/.test(line)) break;

      // Stop if we hit a phone number pattern
      if (/^\+?\d[\d\s\-()]{6,}$/.test(line)) break;

      // Stop if this looks like the start of the next record
      if (i > startIndex + 3 && this._looksLikeSurname(line, lines, i)) break;

      // Stop if we hit a known suburb
      if (this._isKnownSuburb(line)) {
        // But first check if the firm parts are empty -- this might be the firm name
        if (firmParts.length === 0) {
          firmParts.push(line);
          i++;
          continue;
        }
        break;
      }

      firmParts.push(line);
      i++;

      // Reasonable limit on firm name + address lines
      if (firmParts.length > 8) break;
    }

    // Now parse the firmParts into firm name and street address
    // The firm name comes first, then address lines, then suburb + postcode
    // Firm names typically contain '|' or descriptive text like "Pty Ltd"
    if (firmParts.length > 0) {
      // First part is always the firm name
      firmName = firmParts[0];

      // If there are more parts, they could be additional firm name lines
      // or street address lines. Address lines typically start with a number
      // or contain 'Level', 'Suite', 'PO Box', 'GPO Box', etc.
      // We assign them as part of the address later
    }

    // Suburb
    let suburb = '';
    if (i < lines.length && this._isKnownSuburb(lines[i])) {
      suburb = lines[i++];
    } else if (i < lines.length && /^[A-Za-z][A-Za-z\s]+$/.test(lines[i]) &&
               !headerFields.has(lines[i]) && !['B', 'U', 'RBS'].includes(lines[i])) {
      // Accept any alphabetic string as a suburb if it's followed by a postcode
      if (i + 1 < lines.length && /^\d{4}$/.test(lines[i + 1])) {
        suburb = lines[i++];
      }
    }

    // Postcode
    let postcode = '';
    if (i < lines.length && /^\d{4}$/.test(lines[i])) {
      postcode = lines[i++];
    }

    // Telephone
    let telephone = '';
    if (i < lines.length && /^[\+\d][\d\s\-()]{4,}$/.test(lines[i])) {
      telephone = lines[i++];
      // Phone may span two lines (e.g. area code on one line, number on next)
      if (i < lines.length && /^[\d\s\-()]{4,}$/.test(lines[i]) &&
          telephone.length < 8) {
        telephone += ' ' + lines[i++];
      }
    }

    // Build the street address from remaining firmParts (beyond the first)
    const streetParts = firmParts.slice(1);
    const streetAddress = streetParts.join(', ');

    // Determine bar status from type code
    const barStatus = this.typeCodes[type] || type;

    // Determine city from suburb
    const city = this._titleCase(suburb);

    return {
      first_name: firstName,
      last_name: surname,
      full_name: `${firstName} ${surname}`.trim(),
      firm_name: firmName,
      city: city,
      state: 'NT',
      zip: postcode,
      country: 'Australia',
      phone: this._formatPhone(telephone),
      email: '',
      website: '',
      bar_number: '',
      bar_status: barStatus,
      admission_date: '',
      profile_url: '',
      practice_areas: '',
      practitioner_type: type,
      address: streetAddress,
      _nextIndex: i,
    };
  }

  /**
   * Check if a string is a known NT suburb/locality.
   */
  _isKnownSuburb(text) {
    if (!text) return false;
    const known = [
      'Darwin', 'Alice Springs', 'Katherine', 'Palmerston', 'Casuarina',
      'Stuart Park', 'Nightcliff', 'Parap', 'Winnellie', 'Berrimah',
      'Malak', 'Jingili', 'Larrakeyah', 'The Gardens', 'Fannie Bay',
      'Woolner', 'Tennant Creek', 'Nhulunbuy', 'Jabiru', 'Araluen',
      'Wanguri', 'Millner', 'Rapid Creek', 'Anula', 'Leanyer',
      'Tiwi', 'Moil', 'Coconut Grove', 'Ludmilla', 'Bayview',
      'East Point', 'Sydney', 'Melbourne', 'Brisbane', 'Perth',
      'Adelaide', 'Canberra', 'Hobart', 'Leichardt', 'Leichhardt',
    ];
    return known.some(s => s.toLowerCase() === text.toLowerCase());
  }

  /**
   * Format an Australian phone number.
   */
  _formatPhone(phone) {
    if (!phone) return '';
    // Clean up whitespace
    let cleaned = phone.replace(/\s+/g, ' ').trim();
    // Add area code prefix for NT landlines if missing
    if (/^\d{4}\s*\d{4}$/.test(cleaned) && !cleaned.startsWith('0')) {
      cleaned = '08 ' + cleaned;
    }
    return cleaned;
  }

  /**
   * Convert a string to Title Case.
   */
  _titleCase(str) {
    if (!str) return '';
    return str.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  }

  // --- Core search implementation ---

  /**
   * Async generator that yields practitioner records from the NT Law Society.
   *
   * Strategy:
   *   1. Fetch the practitioner index page to discover current PDF URLs
   *   2. Download and parse each PDF
   *   3. Optionally filter by city/suburb
   *   4. Yield records
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const cities = this.getCities(options);
    const cityFilter = options.city ? new Set(cities.map(c => c.toLowerCase())) : null;
    const seen = new Set();

    log.scrape('AU-NT: Starting Law Society NT practitioner register scrape');

    // Step 1: Fetch the index page to find current PDF URLs
    let pdfUrls;
    try {
      await rateLimiter.wait();
      const response = await this.httpGet(this.practitionerIndexUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.error(`AU-NT: Failed to fetch practitioner index page (HTTP ${response.statusCode})`);
        return;
      }

      if (this.detectCaptcha(response.body)) {
        log.warn('AU-NT: CAPTCHA detected on index page');
        yield { _captcha: true, city: 'index' };
        return;
      }

      pdfUrls = this._extractPdfUrls(response.body);

      if (pdfUrls.length === 0) {
        log.error('AU-NT: No PDF URLs found on the practitioner index page');
        log.warn('AU-NT: The page structure may have changed -- check https://lawsocietynt.asn.au/index.php/current-nt-legal-practitioners/');
        return;
      }

      log.success(`AU-NT: Found ${pdfUrls.length} practitioner PDF(s) to download`);
    } catch (err) {
      log.error(`AU-NT: Failed to fetch index page: ${err.message}`);
      return;
    }

    // Step 2: Download and parse each PDF
    let totalPractitioners = 0;

    for (let pi = 0; pi < pdfUrls.length; pi++) {
      const pdfUrl = pdfUrls[pi];
      yield { _cityProgress: { current: pi + 1, total: pdfUrls.length } };

      log.info(`AU-NT: Downloading PDF ${pi + 1}/${pdfUrls.length}: ${pdfUrl}`);

      try {
        await rateLimiter.wait();
        const pdfBuffer = await this._downloadFile(pdfUrl, rateLimiter);

        log.info(`AU-NT: Downloaded ${(pdfBuffer.length / 1024).toFixed(0)}KB -- extracting text...`);

        // Extract text from the PDF
        const textLines = this._extractTextFromPdf(pdfBuffer);

        if (textLines.length === 0) {
          log.warn(`AU-NT: No text extracted from ${pdfUrl} -- PDF may use an unsupported encoding`);
          continue;
        }

        log.info(`AU-NT: Extracted ${textLines.length} text fragments from PDF`);

        // Parse into structured records
        const practitioners = this._parsePractitionerLines(textLines);

        log.success(`AU-NT: Parsed ${practitioners.length} practitioners from PDF`);

        // Step 3: Yield records, optionally filtering by city
        for (const practitioner of practitioners) {
          // Apply city filter if specified
          if (cityFilter && practitioner.city) {
            if (!cityFilter.has(practitioner.city.toLowerCase())) continue;
          }

          // Dedup by name
          const dedupKey = `${practitioner.last_name}|${practitioner.first_name}|${practitioner.firm_name}`;
          if (seen.has(dedupKey)) continue;
          seen.add(dedupKey);

          // Remove internal _nextIndex field
          delete practitioner._nextIndex;

          // Apply admission year filter if specified
          if (options.minYear && practitioner.admission_date) {
            const yearMatch = practitioner.admission_date.match(/\d{4}/);
            if (yearMatch) {
              const year = parseInt(yearMatch[0], 10);
              if (year > 0 && year < options.minYear) continue;
            }
          }

          totalPractitioners++;
          yield this.transformResult(practitioner, practiceArea);
        }

        // Respect max pages limit for testing
        if (options.maxPages && pi + 1 >= options.maxPages) {
          log.info(`AU-NT: Reached max pages limit (${options.maxPages})`);
          break;
        }

      } catch (err) {
        log.error(`AU-NT: Failed to process PDF ${pdfUrl}: ${err.message}`);
        continue;
      }
    }

    log.success(`AU-NT: Scrape complete -- ${totalPractitioners} practitioners yielded (${seen.size} unique)`);
  }

  /**
   * Extract PDF download URLs from the practitioner index page HTML.
   *
   * Looks for links to PDF files that contain practitioner data.
   * The URLs typically follow patterns like:
   *   /wp-content/uploads/YYYY/MM/YYMMDD-Barristers.pdf
   *   /wp-content/uploads/YYYY/MM/YYMMDD-Legal_Practitioners_A-F.pdf
   *
   * @param {string} html - The index page HTML
   * @returns {string[]} Array of absolute PDF URLs
   */
  _extractPdfUrls(html) {
    const $ = cheerio.load(html);
    const urls = [];
    const seen = new Set();

    $('a[href$=".pdf"]').each((_, el) => {
      const href = $(el).attr('href');
      if (!href) return;

      // Only include PDFs that look like practitioner lists
      const lowerHref = href.toLowerCase();
      if (lowerHref.includes('practitioner') || lowerHref.includes('barrister')) {
        let fullUrl = href;
        if (href.startsWith('/')) {
          fullUrl = `${this.baseUrl}${href}`;
        } else if (!href.startsWith('http')) {
          fullUrl = `${this.baseUrl}/${href}`;
        }

        if (!seen.has(fullUrl)) {
          seen.add(fullUrl);
          urls.push(fullUrl);
        }
      }
    });

    // If no URLs found by href text, try broader matching
    if (urls.length === 0) {
      $('a[href$=".pdf"]').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase();
        if (!href) return;

        if (text.includes('practitioner') || text.includes('barrister') ||
            text.includes('legal') || text.includes('solicitor')) {
          let fullUrl = href;
          if (href.startsWith('/')) {
            fullUrl = `${this.baseUrl}${href}`;
          } else if (!href.startsWith('http')) {
            fullUrl = `${this.baseUrl}/${href}`;
          }

          if (!seen.has(fullUrl)) {
            seen.add(fullUrl);
            urls.push(fullUrl);
          }
        }
      });
    }

    return urls;
  }
}

module.exports = new NtScraper();
