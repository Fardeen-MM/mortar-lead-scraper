/**
 * Martindale.com Lawyer Directory Scraper
 *
 * Source: https://www.martindale.com/
 * Method: HTTP GET + JSON-LD structured data (server-rendered HTML)
 * Data:   JSON-LD @graph array with LegalService entries
 *
 * URL Pattern:
 *   Browse: https://www.martindale.com/all-lawyers/{city-slug}/{state-slug}/
 *   Pages:  ?page=2, ?page=3, etc.
 *
 * HIGH PRIORITY data source — fully server-rendered, no anti-scraping measures.
 * Covers all US jurisdictions (~30 lawyers per page).
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

/**
 * Map of US state abbreviations to full state names for URL generation.
 */
const STATE_ABBREV_TO_NAME = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas',
  CA: 'California', CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware',
  DC: 'District of Columbia', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii',
  ID: 'Idaho', IL: 'Illinois', IN: 'Indiana', IA: 'Iowa',
  KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine',
  MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota',
  MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska',
  NV: 'Nevada', NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico',
  NY: 'New York', NC: 'North Carolina', ND: 'North Dakota', OH: 'Ohio',
  OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island',
  SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas',
  UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington',
  WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
};

/**
 * Default cities to scrape — major US metros across multiple states.
 * Each entry is { city, stateCode } so we know what state to use in the URL.
 */
const DEFAULT_CITY_ENTRIES = [
  { city: 'New York',      stateCode: 'NY' },
  { city: 'Los Angeles',   stateCode: 'CA' },
  { city: 'Chicago',       stateCode: 'IL' },
  { city: 'Houston',       stateCode: 'TX' },
  { city: 'Phoenix',       stateCode: 'AZ' },
  { city: 'Philadelphia',  stateCode: 'PA' },
  { city: 'San Antonio',   stateCode: 'TX' },
  { city: 'San Diego',     stateCode: 'CA' },
  { city: 'Dallas',        stateCode: 'TX' },
  { city: 'San Jose',      stateCode: 'CA' },
];

class MartindaleScraper extends BaseScraper {
  constructor() {
    super({
      name: 'martindale',
      stateCode: 'MARTINDALE',
      baseUrl: 'https://www.martindale.com',
      pageSize: 30, // ~30 lawyers per page on Martindale
      practiceAreaCodes: {},  // Martindale doesn't use practice area codes in URLs
      defaultCities: DEFAULT_CITY_ENTRIES.map(e => e.city),
      maxConsecutiveEmpty: 2,
    });

    // Store full city entries for state lookup
    this._cityEntries = DEFAULT_CITY_ENTRIES;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Slugify a string for Martindale URLs.
   * "New York" -> "new-york", "San Antonio" -> "san-antonio"
   */
  slugify(str) {
    return (str || '')
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '');
  }

  /**
   * Get the state name for a state code, or return the input if it's already a name.
   */
  getStateName(stateCode) {
    return STATE_ABBREV_TO_NAME[stateCode] || stateCode;
  }

  /**
   * Resolve a city name to its state code from the default entries, or from options.
   */
  resolveStateForCity(city, options = {}) {
    // If options explicitly provides a state, use it
    if (options.state) return options.state;
    if (options.stateCode) return options.stateCode;

    // Look up in default entries
    const entry = this._cityEntries.find(
      e => e.city.toLowerCase() === city.toLowerCase()
    );
    if (entry) return entry.stateCode;

    // If nothing found, caller must provide state
    return null;
  }

  // ---------------------------------------------------------------------------
  // HTTP with better headers for Martindale
  // ---------------------------------------------------------------------------

  httpGet(url, rateLimiter, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > 5) {
        return reject(new Error(`Too many redirects (>5) for ${url}`));
      }
      const https = require('https');
      const http = require('http');
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
          'Referer': 'https://www.martindale.com/',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'same-origin',
          'Upgrade-Insecure-Requests': '1',
        },
        timeout: 30000,
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
          return resolve(this.httpGet(redirect, rateLimiter, redirectCount + 1));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    });
  }

  // ---------------------------------------------------------------------------
  // URL building
  // ---------------------------------------------------------------------------

  /**
   * Build the Martindale browse URL for a city/state/page.
   * Pattern: https://www.martindale.com/all-lawyers/{city-slug}/{state-slug}/?page=N
   */
  buildSearchUrl({ city, stateSlug, page }) {
    const citySlug = this.slugify(city);
    const base = `${this.baseUrl}/all-lawyers/${citySlug}/${stateSlug}/`;
    if (page && page > 1) {
      return `${base}?page=${page}`;
    }
    return base;
  }

  // ---------------------------------------------------------------------------
  // JSON-LD parsing
  // ---------------------------------------------------------------------------

  /**
   * Extract lawyer entries from JSON-LD structured data in the HTML.
   * Martindale embeds <script type="application/ld+json"> with an @graph array.
   */
  parseJsonLd($) {
    const attorneys = [];

    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        const data = JSON.parse(raw);

        // The @graph array contains LegalService (or similar) entries
        const graph = data['@graph'] || [];
        for (const entry of graph) {
          // Accept LegalService, Attorney, LegalService-type entries
          const type = entry['@type'];
          if (!type) continue;
          const types = Array.isArray(type) ? type : [type];
          const isLegal = types.some(t =>
            t === 'LegalService' || t === 'Attorney' || t === 'Lawyer' ||
            t === 'Organization' || t === 'LocalBusiness' || t === 'Person'
          );
          if (!isLegal) continue;

          const fullName = (entry.name || '').trim();
          if (!fullName) continue;

          const address = entry.address || {};
          const { firstName, lastName } = this.splitName(fullName);

          // sameAs and image can be arrays or strings
          const sameAs = Array.isArray(entry.sameAs) ? entry.sameAs[0] : entry.sameAs;
          const image = Array.isArray(entry.image) ? entry.image[0] : entry.image;
          const telephone = Array.isArray(entry.telephone) ? entry.telephone[0] : entry.telephone;

          attorneys.push({
            first_name: firstName,
            last_name: lastName,
            full_name: fullName,
            firm_name: '',  // JSON-LD doesn't clearly separate firm from attorney name
            city: (address.addressLocality || '').trim(),
            state: (address.addressRegion || '').trim(),
            zip: (address.postalCode || '').trim(),
            address: (address.streetAddress || '').trim(),
            country: (address.addressCountry || 'US').trim(),
            phone: (telephone || '').trim(),
            email: '',  // Not available in Martindale JSON-LD
            website: (entry.url || '').trim(),
            profile_url: (sameAs || '').trim(),
            image_url: (image || '').trim(),
            source: 'martindale',
          });
        }
      } catch (err) {
        // JSON parse error — skip this script block
        log.warn(`Failed to parse JSON-LD block: ${err.message}`);
      }
    });

    return attorneys;
  }

  /**
   * Check if a results page has a "next" link or more pages.
   * Returns true if there appears to be a next page.
   */
  hasNextPage($, currentPage) {
    // Check for pagination links — look for a link to page N+1
    const nextPage = currentPage + 1;

    // Look for pagination links containing the next page number
    let found = false;
    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes(`page=${nextPage}`)) {
        found = true;
        return false; // break
      }
    });
    if (found) return true;

    // Also check for "next" link patterns
    const nextLink = $('a[rel="next"], a.next, a:contains("Next"), a:contains("next")');
    if (nextLink.length > 0) return true;

    return false;
  }

  // ---------------------------------------------------------------------------
  // Profile page parsing
  // ---------------------------------------------------------------------------

  /**
   * Parse a Martindale profile page for additional contact info.
   * Profile URLs come from the JSON-LD sameAs field.
   * These pages have detailed attorney info including bio, education, etc.
   */
  parseProfilePage($) {
    const result = {};

    // Try JSON-LD first (Martindale profile pages also have structured data)
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const raw = $(el).html();
        if (!raw) return;
        const data = JSON.parse(raw);

        if (data.telephone) {
          const tel = Array.isArray(data.telephone) ? data.telephone[0] : data.telephone;
          if (tel && !result.phone) result.phone = tel.trim();
        }
        if (data.email && !result.email) {
          result.email = data.email.trim().toLowerCase();
        }
        if (data.url && !result.website) {
          result.website = data.url.trim();
        }
        if (data.description && !result.bio) {
          result.bio = data.description.trim().substring(0, 500);
        }
      } catch {
        // JSON parse error — skip
      }
    });

    // Email from mailto links
    if (!result.email) {
      const mailtoLink = $('a[href^="mailto:"]').first();
      if (mailtoLink.length) {
        result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
      }
    }

    // Phone from page text
    if (!result.phone) {
      const bodyText = $('body').text();
      const phoneMatch = bodyText.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
      if (phoneMatch) result.phone = phoneMatch[1].trim();
    }

    // Website — external links not on martindale.com or other non-firm sites
    if (!result.website) {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        if (!href.includes('martindale.com') && !this.isExcludedDomain(href)) {
          result.website = href;
          return false;
        }
      });
    }

    return result;
  }

  // ---------------------------------------------------------------------------
  // Main search — overrides BaseScraper.search()
  // ---------------------------------------------------------------------------

  /**
   * Async generator that yields attorney records from Martindale.
   *
   * Martindale is a US-wide directory, not a single state bar, so the search
   * logic differs from state bar scrapers:
   * - Each city needs a corresponding state for URL generation
   * - Pagination is detected by checking for next-page links + empty results
   * - Data is extracted from JSON-LD rather than HTML scraping
   *
   * Options:
   *   city      - Single city name (string)
   *   state     - State code for the city (e.g., 'NY', 'CA')
   *   cities    - Array of { city, stateCode } objects
   *   maxPages  - Max pages to fetch per city (for testing)
   */
  async *search(practiceArea, options = {}) {
    const isTestMode = !!(options.maxPages || options.maxCities);
    const rateLimiter = new RateLimiter({
      minDelay: isTestMode ? 2000 : 5000,
      maxDelay: isTestMode ? 4000 : 10000,
    });
    // In test mode, limit retries on 403/429 to avoid long backoff waits
    const maxBlockRetries = isTestMode ? 1 : 3;

    // Build the list of city/state pairs to scrape
    const cityEntries = this._buildCityList(options);

    if (cityEntries.length === 0) {
      log.warn('Martindale: No cities to search (no state provided for custom city)');
      return;
    }

    log.info(`Martindale: Searching ${cityEntries.length} cities`);

    for (let ci = 0; ci < cityEntries.length; ci++) {
      const { city, stateCode } = cityEntries[ci];
      const stateName = this.getStateName(stateCode);
      const stateSlug = this.slugify(stateName);

      // Emit city progress
      yield { _cityProgress: { current: ci + 1, total: cityEntries.length } };
      log.scrape(`Martindale: Searching lawyers in ${city}, ${stateCode}`);

      let page = 1;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let totalYielded = 0;

      while (true) {
        // Check max pages limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`Reached max pages limit (${options.maxPages}) for ${city}, ${stateCode}`);
          break;
        }

        const url = this.buildSearchUrl({ city, stateSlug, page });
        log.info(`Page ${page} — ${url}`);

        // Fetch page
        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} page ${page}: ${err.message}`);
          if (rateLimiter.consecutiveBlocks >= maxBlockRetries) {
            log.warn(`Reached max retries for ${city} — skipping to next city`);
            rateLimiter.resetBackoff();
            break;
          }
          rateLimiter.consecutiveBlocks++;
          const backoffMs = isTestMode ? 3000 : (30000 * rateLimiter.backoffMultiplier);
          rateLimiter.backoffMultiplier *= 2;
          await new Promise(r => setTimeout(r, backoffMs));
          continue;
        }

        // Handle rate limiting / blocking
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from Martindale on page ${page} for ${city}`);
          if (rateLimiter.consecutiveBlocks >= maxBlockRetries) {
            log.warn(`Reached max block retries (${maxBlockRetries}) for ${city} — skipping to next city`);
            rateLimiter.resetBackoff();
            break;
          }
          const backoffMs = isTestMode ? 5000 : (30000 * rateLimiter.backoffMultiplier);
          log.warn(`Backing off ${backoffMs / 1000}s (attempt ${rateLimiter.consecutiveBlocks + 1}/${maxBlockRetries})`);
          rateLimiter.consecutiveBlocks++;
          rateLimiter.backoffMultiplier *= 2;
          await new Promise(r => setTimeout(r, backoffMs));
          continue;
        }

        // 404 means we've gone past the last page
        if (response.statusCode === 404) {
          log.info(`Got 404 on page ${page} for ${city}, ${stateCode} — no more pages`);
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for ${city} page ${page} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        // Parse HTML and extract JSON-LD
        const $ = cheerio.load(response.body);
        const attorneys = this.parseJsonLd($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          log.info(`No attorneys found on page ${page} for ${city} (empty #${consecutiveEmpty})`);
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Yield each attorney record
        for (const attorney of attorneys) {
          attorney.search_city = city;
          attorney.search_state = stateCode;
          attorney.source = 'martindale';
          attorney.practice_area = practiceArea || '';
          totalYielded++;
          yield attorney;
        }

        log.info(`Page ${page}: ${attorneys.length} attorneys (${totalYielded} total for ${city})`);

        // Check if there's a next page
        if (!this.hasNextPage($, page)) {
          log.success(`No more pages after page ${page} for ${city}, ${stateCode}`);
          break;
        }

        page++;
        pagesFetched++;
      }

      log.success(`Completed ${city}, ${stateCode}: ${totalYielded} attorneys across ${page} page(s)`);
    }
  }

  /**
   * Build the list of { city, stateCode } entries to scrape.
   */
  _buildCityList(options) {
    // Option 1: explicit array of city entries
    if (options.cities && Array.isArray(options.cities)) {
      return options.cities;
    }

    // Option 2: single city + state
    if (options.city) {
      const stateCode = this.resolveStateForCity(options.city, options);
      if (!stateCode) {
        log.error(
          `Martindale: Cannot determine state for city "${options.city}". ` +
          `Please provide --state (e.g., --state NY)`
        );
        return [];
      }
      return [{ city: options.city, stateCode }];
    }

    // Option 3: default cities (respect maxCities limit)
    const entries = [...this._cityEntries];
    if (options.maxCities && options.maxCities < entries.length) {
      return entries.slice(0, options.maxCities);
    }
    return entries;
  }

  // ---------------------------------------------------------------------------
  // Override transformResult — not needed since we handle it in search()
  // ---------------------------------------------------------------------------

  transformResult(attorney, practiceArea) {
    attorney.source = 'martindale';
    attorney.practice_area = practiceArea || '';
    return attorney;
  }

  // ---------------------------------------------------------------------------
  // These are required by BaseScraper but not used since we override search()
  // ---------------------------------------------------------------------------

  parseResultsPage($) {
    return this.parseJsonLd($);
  }

  extractResultCount($) {
    // Martindale doesn't show a clear total count; we rely on pagination detection
    // Return a large number so the base paginator doesn't short-circuit
    const attorneys = this.parseJsonLd($);
    return attorneys.length > 0 ? 99999 : 0;
  }
}

module.exports = new MartindaleScraper();
