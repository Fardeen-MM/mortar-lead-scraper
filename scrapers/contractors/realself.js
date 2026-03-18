/**
 * RealSelf.com Scraper — Aesthetic/Cosmetic Provider Directory
 *
 * Source: https://www.realself.com
 * Records: Thousands of med spas, plastic surgeons, dermatologists, and aesthetic clinics
 * Method: Puppeteer (PerimeterX bot protection blocks plain HTTP)
 *
 * URL pattern: https://www.realself.com/find/{State}/{City}/{Specialty}?page={N}
 *   State: Full state name (e.g., "Florida", "California", "New-York")
 *   City:  Title-cased, hyphens for spaces (e.g., "Miami", "Los-Angeles", "New-York")
 *   Specialty: Title-cased slug (e.g., "Plastic-Surgeon", "Dermatologist", "Botox")
 *
 * Data extraction: __NEXT_DATA__ embedded JSON in the page
 *   Path: props.pageProps.contentData.searchResultsData[0].results[]
 *   Each result has .fields with: name, phone, website, city, state, address,
 *     specialty, practice_names, rating, review_count, uri, years_experience
 *
 * Pagination: 10 results per page, total from contentData.matches_count
 *
 * Anti-bot: PerimeterX (PXdz588q90) — requires Puppeteer with headless: 'new'
 *   Stealth plugin (puppeteer-extra-plugin-stealth) NOT always needed but used
 *   when available. PerimeterX may block after repeated requests; rate limiting
 *   and browser recycling help.
 *
 * Specialty slugs (confirmed working):
 *   Plastic-Surgeon, Dermatologist, Botox, Dermal-Fillers, Laser-Hair-Removal,
 *   CoolSculpting, Chemical-Peel, Facial-Plastic-Surgeon
 *
 * Note: "Med-Spa" is NOT a valid RealSelf slug; use "Plastic-Surgeon" or
 *   "Dermatologist" for med spa providers.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

// Specialty slug mapping: user-friendly name -> RealSelf URL slug
const PRACTICE_AREA_CODES = {
  'plastic surgery': 'Plastic-Surgeon',
  'plastic surgeon': 'Plastic-Surgeon',
  'dermatology': 'Dermatologist',
  'dermatologist': 'Dermatologist',
  'botox': 'Botox',
  'dermal fillers': 'Dermal-Fillers',
  'fillers': 'Dermal-Fillers',
  'laser hair removal': 'Laser-Hair-Removal',
  'coolsculpting': 'CoolSculpting',
  'chemical peel': 'Chemical-Peel',
  'facial plastic surgeon': 'Facial-Plastic-Surgeon',
  'med spa': 'Plastic-Surgeon',
  'medspa': 'Plastic-Surgeon',
};

// State name mapping for URL construction
const STATE_NAME_MAP = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
  'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
  'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
  'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
  'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
  'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
  'NH': 'New-Hampshire', 'NJ': 'New-Jersey', 'NM': 'New-Mexico', 'NY': 'New-York',
  'NC': 'North-Carolina', 'ND': 'North-Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
  'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode-Island', 'SC': 'South-Carolina',
  'SD': 'South-Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
  'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West-Virginia',
  'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District-of-Columbia',
};

// Default cities to search — major US metros with high aesthetic demand
const DEFAULT_CITIES = [
  { city: 'Miami', state: 'FL' },
  { city: 'Los Angeles', state: 'CA' },
  { city: 'New York', state: 'NY' },
  { city: 'Houston', state: 'TX' },
  { city: 'Dallas', state: 'TX' },
  { city: 'Atlanta', state: 'GA' },
  { city: 'Chicago', state: 'IL' },
  { city: 'Phoenix', state: 'AZ' },
  { city: 'Las Vegas', state: 'NV' },
  { city: 'San Diego', state: 'CA' },
  { city: 'Denver', state: 'CO' },
  { city: 'Nashville', state: 'TN' },
  { city: 'Scottsdale', state: 'AZ' },
  { city: 'Beverly Hills', state: 'CA' },
  { city: 'Tampa', state: 'FL' },
  { city: 'Charlotte', state: 'NC' },
  { city: 'Austin', state: 'TX' },
  { city: 'San Francisco', state: 'CA' },
  { city: 'Seattle', state: 'WA' },
  { city: 'Boston', state: 'MA' },
];

const PER_PAGE = 10;


class RealSelfScraper extends BaseScraper {
  constructor() {
    super({
      name: 'realself',
      stateCode: 'REALSELF',
      baseUrl: 'https://www.realself.com',
      pageSize: PER_PAGE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES.map(c => `${c.city}, ${c.state}`),
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('realself: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('realself: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('realself: extractResultCount() not used — search() overridden'); }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'realself';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Build the RealSelf find URL for a city + specialty.
   *
   * @param {string} cityName - City name (e.g., "Miami", "Los Angeles")
   * @param {string} stateCode - 2-letter state code (e.g., "FL", "CA")
   * @param {string} specialtySlug - Specialty URL slug (e.g., "Plastic-Surgeon")
   * @param {number} page - Page number (1-based)
   * @returns {string} Full URL
   */
  _buildFindUrl(cityName, stateCode, specialtySlug, page = 1) {
    const stateName = STATE_NAME_MAP[stateCode] || stateCode;
    const citySlug = cityName.replace(/\s+/g, '-');
    const base = `https://www.realself.com/find/${stateName}/${citySlug}/${specialtySlug}`;
    return page > 1 ? `${base}?page=${page}` : base;
  }

  /**
   * Parse city and state from a "City, ST" string.
   * @param {string} locationStr - e.g., "Miami, FL" or "Los Angeles, CA"
   * @returns {{ city: string, state: string }}
   */
  _parseLocation(locationStr) {
    const match = locationStr.match(/^(.+),\s*([A-Z]{2})$/);
    if (match) {
      return { city: match[1].trim(), state: match[2] };
    }
    // Try to find in default cities
    const def = DEFAULT_CITIES.find(c =>
      c.city.toLowerCase() === locationStr.toLowerCase()
    );
    if (def) return { city: def.city, state: def.state };
    return { city: locationStr, state: '' };
  }

  /**
   * Extract provider data from __NEXT_DATA__ JSON in the page.
   *
   * @param {string} html - Full page HTML
   * @returns {{ results: object[], totalCount: number, page: number, perPage: number }}
   */
  _extractFromNextData(html) {
    const $ = cheerio.load(html);
    const nextDataEl = $('#__NEXT_DATA__');

    if (!nextDataEl.length) {
      // Fallback: try to find JSON in a script tag containing "searchResultsData"
      let nextDataText = null;
      $('script').each((i, el) => {
        const text = $(el).html() || '';
        if (text.includes('searchResultsData') && text.includes('"props"')) {
          nextDataText = text;
          return false;
        }
      });
      if (!nextDataText) return { results: [], totalCount: 0, page: 0, perPage: PER_PAGE };

      try {
        const data = JSON.parse(nextDataText);
        return this._parseNextData(data);
      } catch (e) {
        return { results: [], totalCount: 0, page: 0, perPage: PER_PAGE };
      }
    }

    try {
      const data = JSON.parse(nextDataEl.html());
      return this._parseNextData(data);
    } catch (e) {
      log.warn(`[RealSelf] Failed to parse __NEXT_DATA__: ${e.message}`);
      return { results: [], totalCount: 0, page: 0, perPage: PER_PAGE };
    }
  }

  /**
   * Parse the Next.js data structure into provider results.
   */
  _parseNextData(data) {
    const contentData = data?.props?.pageProps?.contentData;
    if (!contentData) {
      return { results: [], totalCount: 0, page: 0, perPage: PER_PAGE };
    }

    const totalCount = contentData.matches_count || 0;
    const page = contentData.page || 1;
    const perPage = contentData.perPage || PER_PAGE;

    const searchResultsData = contentData.searchResultsData;
    if (!searchResultsData || !searchResultsData[0] || !searchResultsData[0].results) {
      return { results: [], totalCount, page, perPage };
    }

    return {
      results: searchResultsData[0].results,
      totalCount,
      page,
      perPage,
    };
  }

  /**
   * Convert a RealSelf search result into our lead format.
   *
   * @param {object} result - Result object from searchResultsData
   * @param {string} practiceArea - The searched practice area
   * @returns {object|null} Normalized lead object, or null if invalid
   */
  _resultToLead(result, practiceArea) {
    const f = result.fields || {};

    // Extract fields — RealSelf wraps everything in arrays
    // Decode HTML entities since JSON may contain &amp; etc.
    const decode = (s) => this.decodeEntities(s);
    const name = decode((f.name || [])[0] || '');
    const phone = (f.phone || [])[0] || '';
    const website = (f.website || [])[0] || '';
    const city = decode((f.city || f['addresses.city'] || [])[0] || '');
    const stateRegion = decode((f.state || f['addresses.state_region'] || [])[0] || '');
    const address = decode((f.address || f['addresses.line1'] || [])[0] || '');
    const specialty = decode((f.specialty || [])[0] || '');
    const practiceName = decode((f.practice_names || [])[0] || '');
    const rating = (f.rating || [])[0] || 0;
    const reviewCount = (f.review_count || [])[0] || 0;
    const uri = (f.uri || [])[0] || '';
    const yearsExp = (f.years_experience || [])[0] || 0;
    const country = (f.country || [])[0] || 'US';

    // Skip if no name
    if (!name) return null;

    // Determine if this is a person or a business
    // RealSelf has both individual doctors (with credentials like MD, DO, FAAD)
    // and practice/clinic listings
    const isDoctor = /\b(MD|DO|FAAD|FACS|FAAOS|DDS|DMD|PhD|PA-C|NP|RN|BSN|MSN)\b/i.test(name);

    let firstName = '', lastName = '', firmName = '';

    if (isDoctor) {
      // Strip credentials for name parsing
      const cleanName = name.replace(/,?\s*(MD|DO|FAAD|FACS|FAAOS|DDS|DMD|PhD|PA-C|NP|RN|BSN|MSN|DABPS|DABFM|FACMS|FAACS|FAAP)\b/gi, '').trim();
      const nameParts = this.splitName(cleanName);
      firstName = nameParts.firstName;
      lastName = nameParts.lastName;
      firmName = practiceName || '';
    } else {
      // Business listing (clinic/med spa)
      firmName = name;
    }

    // Convert full state name to abbreviation
    let stateCode = '';
    if (stateRegion) {
      const entry = Object.entries(STATE_NAME_MAP).find(
        ([, fullName]) => fullName.replace(/-/g, ' ').toLowerCase() === stateRegion.toLowerCase()
      );
      stateCode = entry ? entry[0] : stateRegion;
    }

    const profileUrl = uri ? `https://www.realself.com${uri}` : '';

    return {
      first_name: firstName,
      last_name: lastName,
      firm_name: firmName,
      address: address,
      city: city,
      state: stateCode,
      zip: '',
      phone: phone,
      email: '', // RealSelf does not expose email in search results
      website: website,
      bar_number: '',
      bar_status: rating ? `${rating} stars (${reviewCount} reviews)` : '',
      admission_date: '',
      source: 'realself',
      profile_url: profileUrl,
      practice_area: practiceArea || specialty || '',
      // Extra fields
      realself_id: result._id || '',
      realself_specialty: specialty,
      realself_rating: rating,
      realself_review_count: reviewCount,
      realself_years_experience: yearsExp,
      realself_country: country,
    };
  }

  /**
   * Launch Puppeteer browser with stealth if available.
   * @returns {Promise<Browser>}
   */
  async _launchBrowser() {
    let puppeteer;
    try {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      puppeteerExtra.use(StealthPlugin());
      puppeteer = puppeteerExtra;
      log.info('[RealSelf] Using puppeteer-extra with stealth plugin');
    } catch (e) {
      puppeteer = require('puppeteer');
      log.info('[RealSelf] Using standard puppeteer (no stealth)');
    }

    const launchOptions = {
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-blink-features=AutomationControlled',
        '--window-size=1366,768',
      ],
    };

    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    return puppeteer.launch(launchOptions);
  }

  /**
   * Navigate to a URL with Puppeteer and return the page HTML.
   * Handles PerimeterX detection with retry.
   *
   * @param {import('puppeteer').Page} page - Puppeteer page
   * @param {string} url - URL to navigate to
   * @returns {{ html: string, blocked: boolean }}
   */
  async _fetchPage(page, url) {
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      // Wait for __NEXT_DATA__ to appear
      await new Promise(r => setTimeout(r, 3000));

      const html = await page.content();

      // Check for PerimeterX block
      const blocked = html.includes('px-captcha') ||
                      html.includes('Access has been denied') ||
                      html.includes('Please verify you are a human');

      if (blocked) {
        // Wait longer and retry — sometimes PerimeterX auto-resolves
        log.info('[RealSelf] PerimeterX challenge detected, waiting 8s...');
        await new Promise(r => setTimeout(r, 8000));
        const retryHtml = await page.content();
        const stillBlocked = retryHtml.includes('px-captcha') ||
                             retryHtml.includes('Access has been denied');
        return { html: retryHtml, blocked: stillBlocked };
      }

      return { html, blocked: false };
    } catch (err) {
      log.warn(`[RealSelf] Navigation error: ${err.message}`);
      return { html: '', blocked: true };
    }
  }

  /**
   * Main search generator. Uses Puppeteer to scrape RealSelf's provider directory.
   *
   * @param {string} practiceArea - Specialty name (e.g., 'plastic surgery', 'botox')
   * @param {object} options - Search options
   * @param {string} options.city - Specific city (e.g., 'Miami, FL')
   * @param {number} options.maxPages - Limit pages per city (default 10)
   * @param {number} options.maxCities - Limit number of cities
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // Resolve the specialty slug
    const practiceKey = (practiceArea || 'plastic surgery').toLowerCase().trim();
    const specialtySlug = PRACTICE_AREA_CODES[practiceKey] || 'Plastic-Surgeon';

    log.scrape(`[RealSelf] Starting search: specialty=${specialtySlug}, practiceArea=${practiceArea || 'plastic surgery'}`);

    // Determine cities to search
    let citiesToSearch;
    if (options.city) {
      citiesToSearch = [options.city];
    } else {
      citiesToSearch = DEFAULT_CITIES.map(c => `${c.city}, ${c.state}`);
    }
    if (options.maxCities) {
      citiesToSearch = citiesToSearch.slice(0, options.maxCities);
    }

    // Launch Puppeteer
    let browser;
    try {
      browser = await this._launchBrowser();
    } catch (err) {
      log.error(`[RealSelf] Failed to launch Puppeteer: ${err.message}`);
      yield { _captcha: true, city: 'all', reason: 'Puppeteer not available — RealSelf requires browser rendering' };
      return;
    }

    try {
      const page = await browser.newPage();

      // Set viewport and user agent
      await page.setViewport({ width: 1366, height: 768 });
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );

      // Block images/fonts/media to speed up
      await page.setRequestInterception(true);
      page.on('request', req => {
        const resourceType = req.resourceType();
        if (['image', 'font', 'media', 'stylesheet'].includes(resourceType)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      const maxPagesPerCity = options.maxPages || 10;
      const seenIds = new Set();
      let totalYielded = 0;
      let consecutiveBlockedCities = 0;

      for (let ci = 0; ci < citiesToSearch.length; ci++) {
        const locationStr = citiesToSearch[ci];
        const { city: cityName, state: stateCode } = this._parseLocation(locationStr);

        if (!stateCode) {
          log.warn(`[RealSelf] Cannot determine state for "${locationStr}" — skipping`);
          continue;
        }

        yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
        log.scrape(`[RealSelf] Searching "${specialtySlug}" in ${cityName}, ${stateCode}`);

        let pageNum = 1;
        let totalResults = 0;
        let cityYielded = 0;
        let consecutiveEmpty = 0;
        let blocked = false;

        while (pageNum <= maxPagesPerCity) {
          const url = this._buildFindUrl(cityName, stateCode, specialtySlug, pageNum);
          log.info(`[RealSelf] Page ${pageNum} — ${url}`);

          // Rate limit between page fetches
          await rateLimiter.wait();

          const { html, blocked: isBlocked } = await this._fetchPage(page, url);

          if (isBlocked) {
            log.warn(`[RealSelf] Blocked by PerimeterX for ${cityName}, ${stateCode} page ${pageNum}`);
            if (pageNum === 1) {
              yield { _captcha: true, city: cityName, reason: 'PerimeterX bot detection' };
              blocked = true;
            }
            break;
          }

          // Reset consecutive blocked counter on success
          consecutiveBlockedCities = 0;

          // Extract data from __NEXT_DATA__
          const { results, totalCount, perPage } = this._extractFromNextData(html);

          if (pageNum === 1) {
            totalResults = totalCount;
            const totalPages = Math.ceil(totalResults / (perPage || PER_PAGE));
            if (totalResults === 0) {
              log.info(`[RealSelf] No results for "${specialtySlug}" in ${cityName}, ${stateCode}`);
              break;
            }
            log.success(`[RealSelf] Found ${totalResults} providers (${totalPages} pages) in ${cityName}, ${stateCode}`);
          }

          if (results.length === 0) {
            consecutiveEmpty++;
            if (consecutiveEmpty >= 2) {
              log.info(`[RealSelf] 2 consecutive empty pages for ${cityName} — moving on`);
              break;
            }
            pageNum++;
            continue;
          }

          consecutiveEmpty = 0;

          // Convert results to leads
          for (const result of results) {
            const lead = this._resultToLead(result, practiceArea || specialtySlug.replace(/-/g, ' '));
            if (!lead) continue;

            // Dedup by RealSelf ID across cities
            if (lead.realself_id && seenIds.has(lead.realself_id)) continue;
            if (lead.realself_id) seenIds.add(lead.realself_id);

            yield this.transformResult(lead, practiceArea || specialtySlug.replace(/-/g, ' '));
            cityYielded++;
            totalYielded++;
          }

          // Check if we've fetched all pages
          const totalPages = Math.ceil(totalResults / (PER_PAGE));
          if (pageNum >= totalPages) {
            log.success(`[RealSelf] Completed all ${totalPages} pages for ${cityName}`);
            break;
          }

          pageNum++;

          // Polite delay between pages (4-8 seconds — PerimeterX is aggressive)
          const delay = 4000 + Math.random() * 4000;
          await new Promise(r => setTimeout(r, delay));
        }

        if (blocked) {
          consecutiveBlockedCities++;
          if (consecutiveBlockedCities >= 3) {
            log.error('[RealSelf] 3 consecutive cities blocked — stopping to avoid IP ban');
            yield { _captcha: true, city: 'all', reason: 'PerimeterX blocking all requests — try again later' };
            break;
          }
        }

        log.info(`[RealSelf] ${cityName}, ${stateCode}: ${cityYielded} leads`);

        // Polite delay between cities (5-10 seconds)
        if (ci < citiesToSearch.length - 1) {
          const cityDelay = 5000 + Math.random() * 5000;
          await new Promise(r => setTimeout(r, cityDelay));
        }
      }

      log.success(`[RealSelf] Finished — ${totalYielded} total leads from ${citiesToSearch.length} cities`);

    } finally {
      if (browser) {
        try { await browser.close(); } catch (e) { /* ignore */ }
      }
    }
  }
}

module.exports = new RealSelfScraper();
