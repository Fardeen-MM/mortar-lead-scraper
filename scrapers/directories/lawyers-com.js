/**
 * Lawyers.com Lawyer Directory Scraper
 *
 * Source: https://www.lawyers.com/
 * Method: HTTP GET + Cheerio HTML parsing (server-rendered, no anti-scraping)
 * Owner:  Internet Brands (same parent as Martindale)
 *
 * URL Patterns:
 *   By practice area + city: https://www.lawyers.com/{practice-area}/{city}/{state}/law-firms/
 *   Paginated:               https://www.lawyers.com/{practice-area}/{city}/{state}/law-firms/?page=2
 *   All lawyers in a city:   https://www.lawyers.com/all/{city}/{state}/law-firms/
 *
 * Data per listing (from HTML):
 *   - Firm name (<h2> within profile-link)
 *   - Location/serving area (class srl-serving)
 *   - Practice areas (<li> with briefcase icon)
 *   - Phone (class srl-phone, data-ctn-rtn / data-ctn-rtn-alt attributes)
 *   - Attorney name (class attorney within lawyers-at-firms div)
 *   - Attorney specialty/position (class position)
 *   - Review rating (stars)
 *   - Review count
 *   - Profile URL (from href)
 *   - Website link (class srl-website)
 *
 * HIGH PRIORITY data source — fully server-rendered, no Cloudflare, no CAPTCHA.
 * Covers US + Canada with granular practice area categorization.
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

/**
 * Map of US state abbreviations to full state names for URL slug generation.
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
 * Practice area friendly names mapped to Lawyers.com URL slugs.
 */
const PRACTICE_AREA_SLUGS = {
  'family law':              'family-law',
  'personal injury':         'personal-injury',
  'criminal law':            'criminal-law',
  'labor and employment':    'labor-and-employment',
  'estate planning':         'estate-planning',
  'business law':            'business-law',
  'real estate':             'real-estate',
  'immigration':             'immigration',
  'bankruptcy':              'bankruptcy',
  'divorce':                 'divorce',
  'dui':                     'dui-dwi',
  'dui/dwi':                 'dui-dwi',
  'dui-dwi':                 'dui-dwi',
  'tax law':                 'tax-law',
  'intellectual property':   'intellectual-property',
  'medical malpractice':     'medical-malpractice',
  'workers compensation':    'workers-compensation',
  'social security':         'social-security',
  'traffic violations':      'traffic-violations',
  'consumer protection':     'consumer-protection',
  'civil rights':            'civil-rights',
  'insurance':               'insurance',
  'environmental law':       'environmental-law',
  'maritime law':            'maritime-law',
  'securities':              'securities',
  'contracts':               'contracts',
  'construction':            'construction',
  'health care':             'health-care',
  'elder law':               'elder-law',
  'government':              'government',
  'education':               'education',
  'entertainment':           'entertainment',
  'military law':            'military-law',
  'animal law':              'animal-law',
  'collections':             'collections',
  'land use':                'land-use',
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

class LawyersComScraper extends BaseScraper {
  constructor() {
    super({
      name: 'lawyers-com',
      stateCode: 'LAWYERS-COM',
      baseUrl: 'https://www.lawyers.com',
      pageSize: 25, // ~25 listings per page on Lawyers.com
      practiceAreaCodes: PRACTICE_AREA_SLUGS,
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
   * Slugify a string for Lawyers.com URLs.
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
   * Get the state name for a state code.
   */
  getStateName(stateCode) {
    return STATE_ABBREV_TO_NAME[stateCode] || stateCode;
  }

  /**
   * Resolve a city name to its state code from the default entries, or from options.
   */
  resolveStateForCity(city, options = {}) {
    if (options.state) return options.state;
    if (options.stateCode) return options.stateCode;

    const entry = this._cityEntries.find(
      e => e.city.toLowerCase() === city.toLowerCase()
    );
    if (entry) return entry.stateCode;

    return null;
  }

  // ---------------------------------------------------------------------------
  // HTTP with appropriate headers for Lawyers.com
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
          'Referer': 'https://www.lawyers.com/',
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
   * Build the Lawyers.com browse URL.
   * Pattern: https://www.lawyers.com/{practice-area}/{city-slug}/{state-slug}/law-firms/?page=N
   * If no practice area, uses "all" as the slug.
   */
  buildSearchUrl({ city, stateSlug, practiceSlug, page }) {
    const citySlug = this.slugify(city);
    const areaSlug = practiceSlug || 'all';
    const base = `${this.baseUrl}/${areaSlug}/${citySlug}/${stateSlug}/law-firms/`;
    if (page && page > 1) {
      return `${base}?page=${page}`;
    }
    return base;
  }

  // ---------------------------------------------------------------------------
  // HTML parsing — extract listings from Lawyers.com search results
  // ---------------------------------------------------------------------------

  /**
   * Parse a single listing card and extract all available data fields.
   */
  _parseListing($, el) {
    const $el = $(el);

    // --- Firm name (h2 within profile-link) ---
    const firmNameEl = $el.find('.profile-link h2, a.profile-link h2, h2');
    const firmName = firmNameEl.first().text().trim();

    // --- Profile URL (from profile-link href) ---
    const profileLinkEl = $el.find('a.profile-link, .profile-link a, a[href*="/lawyer/"], a[href*="/law-firm/"]');
    let profileUrl = (profileLinkEl.first().attr('href') || '').trim();
    if (profileUrl && profileUrl.startsWith('/')) {
      profileUrl = `${this.baseUrl}${profileUrl}`;
    }

    // --- Location / serving area ---
    const servingText = $el.find('.srl-serving').text().trim();

    // --- Practice areas (li items with briefcase icon or within practice area list) ---
    const practiceAreas = [];
    $el.find('.srl-practice-areas li, .practice-areas li').each((_, li) => {
      const area = $(li).text().trim();
      if (area) practiceAreas.push(area);
    });
    // Fallback: look for spans/text near briefcase icons
    if (practiceAreas.length === 0) {
      $el.find('[class*="briefcase"] + span, [class*="briefcase"] ~ span, li:has([class*="briefcase"])').each((_, li) => {
        const area = $(li).text().trim();
        if (area) practiceAreas.push(area);
      });
    }

    // --- Phone number ---
    const phoneEl = $el.find('.srl-phone, [class*="phone"]');
    let phone = '';
    // Prefer the tracking number attributes
    const rtn = phoneEl.attr('data-ctn-rtn') || '';
    const rtnAlt = phoneEl.attr('data-ctn-rtn-alt') || '';
    if (rtn) {
      phone = rtn.trim();
    } else if (rtnAlt) {
      phone = rtnAlt.trim();
    } else {
      phone = phoneEl.first().text().trim();
    }
    // Clean phone number
    phone = phone.replace(/[^0-9+()-\s]/g, '').trim();

    // --- Attorneys listed at the firm ---
    const attorneys = [];
    $el.find('.lawyers-at-firms .attorney, .attorney').each((_, attyEl) => {
      const name = $(attyEl).text().trim();
      if (name) {
        const positionEl = $(attyEl).closest('div, li').find('.position');
        const position = positionEl.text().trim();
        attorneys.push({ name, position });
      }
    });

    // --- Review rating (stars) ---
    let rating = 0;
    const ratingEl = $el.find('[class*="star"], [class*="rating"]');
    const ratingText = ratingEl.attr('data-rating') || ratingEl.attr('title') || ratingEl.text() || '';
    const ratingMatch = ratingText.match(/([\d.]+)/);
    if (ratingMatch) {
      rating = parseFloat(ratingMatch[1]);
    }
    // Also check for filled star count
    if (!rating) {
      const filledStars = $el.find('.star-filled, .star.active, [class*="star"][class*="full"]').length;
      if (filledStars > 0) rating = filledStars;
    }

    // --- Review count ---
    let reviewCount = 0;
    const reviewEl = $el.find('[class*="review-count"], [class*="reviews"]');
    const reviewText = reviewEl.text() || '';
    const reviewMatch = reviewText.match(/(\d+)\s*review/i);
    if (reviewMatch) {
      reviewCount = parseInt(reviewMatch[1], 10);
    }

    // --- Website link ---
    const websiteEl = $el.find('.srl-website a, a.srl-website, a[class*="website"]');
    let website = (websiteEl.first().attr('href') || '').trim();

    // --- Address parsing ---
    // Try to extract city, state, zip from dedicated address elements first
    const addressEl = $el.find('.srl-address, [class*="address"]');
    const addressText = addressEl.text().trim();

    let city = '';
    let state = '';
    let zip = '';

    if (addressText) {
      // Dedicated address element: parse "City, ST ZIP"
      const csz = this.parseCityStateZip(addressText);
      if (csz.city) {
        city = csz.city;
        state = csz.state;
        zip = csz.zip;
      }
    }

    // Fall back to serving text if no address element found
    if (!city && servingText) {
      // Handle "Serving City, ST and Nearby Areas" — strip the "Serving" prefix
      const servingMatch = servingText.match(/Serving\s+(.+?),\s*([A-Z]{2})/i);
      if (servingMatch) {
        city = servingMatch[1].trim();
        state = servingMatch[2];
      } else {
        // No "Serving" prefix — try plain "City, ST"
        const plainMatch = servingText.match(/^(.+?),\s*([A-Z]{2})/);
        if (plainMatch) {
          city = plainMatch[1].trim();
          state = plainMatch[2];
        }
      }
    }

    return {
      firm_name: firmName,
      profile_url: profileUrl,
      serving_area: servingText,
      practice_areas: practiceAreas,
      phone,
      attorneys,
      rating,
      review_count: reviewCount,
      website,
      city,
      state,
      zip,
    };
  }

  /**
   * Parse all listings from a Lawyers.com search results page.
   * Returns an array of normalized attorney/firm objects.
   */
  parseResultsPage($) {
    const results = [];

    // Lawyers.com listings are typically in search result list items or divs
    const listingSelectors = [
      '.srl-container',          // search result list container items
      '.search-result-list > li',
      '.search-results > li',
      '[class*="search-result"]',
      '.lawyer-listing',
      '.law-firm-listing',
    ];

    let $listings = $();

    // Try each selector until we find listings
    for (const selector of listingSelectors) {
      $listings = $(selector);
      if ($listings.length > 0) break;
    }

    // Fallback: look for any element that contains a profile-link
    if ($listings.length === 0) {
      $listings = $('*:has(> .profile-link), *:has(> a.profile-link)').filter((_, el) => {
        // Make sure this is a listing container, not the whole page
        const tag = (el.tagName || '').toLowerCase();
        return tag === 'li' || tag === 'div' || tag === 'article' || tag === 'section';
      });
    }

    $listings.each((_, el) => {
      try {
        const listing = this._parseListing($, el);
        if (!listing.firm_name && listing.attorneys.length === 0) return; // skip empty

        // Normalize: create one record per attorney if attorneys are listed,
        // otherwise create one record for the firm
        if (listing.attorneys.length > 0) {
          for (const atty of listing.attorneys) {
            const { firstName, lastName } = this.splitName(atty.name);
            results.push({
              first_name: firstName,
              last_name: lastName,
              full_name: atty.name,
              firm_name: listing.firm_name,
              position: atty.position,
              city: listing.city,
              state: listing.state,
              zip: listing.zip,
              phone: listing.phone,
              email: '',  // Not available in listing HTML
              website: listing.website,
              profile_url: listing.profile_url,
              serving_area: listing.serving_area,
              practice_areas: listing.practice_areas.join(', '),
              rating: listing.rating,
              review_count: listing.review_count,
              source: 'lawyers-com',
            });
          }
        } else {
          // No individual attorneys listed — record the firm itself
          results.push({
            first_name: '',
            last_name: '',
            full_name: '',
            firm_name: listing.firm_name,
            position: '',
            city: listing.city,
            state: listing.state,
            zip: listing.zip,
            phone: listing.phone,
            email: '',
            website: listing.website,
            profile_url: listing.profile_url,
            serving_area: listing.serving_area,
            practice_areas: listing.practice_areas.join(', '),
            rating: listing.rating,
            review_count: listing.review_count,
            source: 'lawyers-com',
          });
        }
      } catch (err) {
        log.warn(`Failed to parse listing: ${err.message}`);
      }
    });

    return results;
  }

  /**
   * Extract total result count from the page.
   * Lawyers.com typically shows "N results" or "Showing X-Y of Z" in the header.
   */
  extractResultCount($) {
    // Look for result count text patterns
    const countSelectors = [
      '.result-count',
      '.search-result-count',
      '[class*="result-count"]',
      '.srl-header',
      'h1',
    ];

    for (const selector of countSelectors) {
      const text = $(selector).first().text();
      // Match patterns like "123 results", "Showing 1-25 of 456", "123 Law Firms"
      const match = text.match(/(?:of\s+)?(\d[\d,]*)\s*(?:result|law\s*firm|lawyer|attorney|match)/i);
      if (match) {
        return parseInt(match[1].replace(/,/g, ''), 10);
      }
      // Also try just a standalone number at the beginning
      const numMatch = text.match(/^(\d[\d,]*)\s/);
      if (numMatch) {
        return parseInt(numMatch[1].replace(/,/g, ''), 10);
      }
    }

    // Fallback: count listings on the page and assume there are more
    const listings = this.parseResultsPage($);
    return listings.length > 0 ? 99999 : 0;
  }

  /**
   * Check if there is a next page available.
   */
  hasNextPage($, currentPage) {
    const nextPage = currentPage + 1;

    // Look for pagination link to next page
    let found = false;
    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes(`page=${nextPage}`)) {
        found = true;
        return false; // break
      }
    });
    if (found) return true;

    // Check for "Next" link patterns
    const nextLink = $('a[rel="next"], a.next, a:contains("Next"), a:contains("next"), a.pagination-next, [class*="pagination"] a:contains("Next")');
    if (nextLink.length > 0) return true;

    // Check for right-arrow pagination links
    const arrowLink = $('[class*="pagination"] a:last-child, .pagination a:last-child');
    if (arrowLink.length > 0) {
      const href = arrowLink.attr('href') || '';
      if (href.includes('page=')) return true;
    }

    return false;
  }

  // ---------------------------------------------------------------------------
  // Profile page parsing
  // ---------------------------------------------------------------------------

  /**
   * Parse a Lawyers.com profile page for additional contact info.
   * Profile pages have detailed attorney info, phone, website, etc.
   */
  parseProfilePage($) {
    const result = {};
    const bodyText = $('body').text();

    // Phone — from profile page elements or text
    const phoneEl = $('[class*="phone"], [data-ctn-rtn]');
    const rtn = phoneEl.attr('data-ctn-rtn') || '';
    const rtnAlt = phoneEl.attr('data-ctn-rtn-alt') || '';
    if (rtn) {
      result.phone = rtn.trim();
    } else if (rtnAlt) {
      result.phone = rtnAlt.trim();
    } else {
      const phoneMatch = bodyText.match(/(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})/);
      if (phoneMatch) result.phone = phoneMatch[1].trim();
    }

    // Email from mailto links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
    }

    // Website
    const websiteEl = $('a.srl-website, a[class*="website"], a[data-analytics*="website"]');
    if (websiteEl.length) {
      result.website = (websiteEl.first().attr('href') || '').trim();
    } else {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().toLowerCase().trim();
        if ((text.includes('visit') || text.includes('website')) &&
            !href.includes('lawyers.com') && !href.includes('martindale.com')) {
          result.website = href;
          return false;
        }
      });
    }

    // Firm name
    const firmEl = $('h1, .profile-name, [class*="firm-name"]').first();
    if (firmEl.length) {
      const firmText = firmEl.text().trim();
      if (firmText && firmText.length > 1 && firmText.length < 200) {
        result.firm_name = firmText;
      }
    }

    return result;
  }

  // ---------------------------------------------------------------------------
  // Main search — overrides BaseScraper.search()
  // ---------------------------------------------------------------------------

  /**
   * Async generator that yields attorney/firm records from Lawyers.com.
   *
   * Lawyers.com is a US-wide directory (also covers Canada), so the search
   * logic handles multi-city + practice area URL generation:
   * - Each city needs a corresponding state for the URL
   * - Practice areas map to URL slugs (e.g., "family-law", "personal-injury")
   * - Pagination via ?page=N query parameter
   * - Data is extracted from server-rendered HTML via Cheerio
   *
   * Options:
   *   city      - Single city name (string)
   *   state     - State code for the city (e.g., 'NY', 'CA')
   *   cities    - Array of { city, stateCode } objects
   *   maxPages  - Max pages to fetch per city (for testing)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter({
      minDelay: 5000,  // 5 seconds minimum between requests
      maxDelay: 10000, // 10 seconds maximum
    });

    // Resolve practice area to URL slug
    const practiceSlug = this.resolvePracticeCode(practiceArea);
    if (practiceArea && !practiceSlug) {
      log.warn(
        `Lawyers.com: Unknown practice area "${practiceArea}" — will search all practice areas. ` +
        `Available: ${Object.keys(this.practiceAreaCodes).join(', ')}`
      );
    }

    // Build the list of city/state pairs to scrape
    const cityEntries = this._buildCityList(options);

    if (cityEntries.length === 0) {
      log.warn('Lawyers.com: No cities to search (no state provided for custom city)');
      return;
    }

    log.info(`Lawyers.com: Searching ${cityEntries.length} cities for "${practiceArea || 'all'}" lawyers`);

    for (let ci = 0; ci < cityEntries.length; ci++) {
      const { city, stateCode } = cityEntries[ci];
      const stateName = this.getStateName(stateCode);
      const stateSlug = this.slugify(stateName);

      // Emit city progress
      yield { _cityProgress: { current: ci + 1, total: cityEntries.length } };
      log.scrape(`Lawyers.com: Searching ${practiceArea || 'all'} lawyers in ${city}, ${stateCode}`);

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

        const url = this.buildSearchUrl({ city, stateSlug, practiceSlug, page });
        log.info(`Page ${page} — ${url}`);

        // Fetch page
        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${city} page ${page}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting / blocking
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from Lawyers.com`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        // 404 means we've gone past the last page or invalid URL
        if (response.statusCode === 404) {
          log.info(`Got 404 on page ${page} for ${city}, ${stateCode} — no more pages`);
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`Unexpected status ${response.statusCode} for ${city} page ${page} — skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA (unlikely on Lawyers.com, but safety check)
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on page ${page} for ${city} — skipping`);
          yield { _captcha: true, city, page };
          break;
        }

        // Parse HTML
        const $ = cheerio.load(response.body);
        const records = this.parseResultsPage($);

        if (records.length === 0) {
          consecutiveEmpty++;
          log.info(`No listings found on page ${page} for ${city} (empty #${consecutiveEmpty})`);
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`${this.maxConsecutiveEmpty} consecutive empty pages — stopping pagination for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Yield each record
        for (const record of records) {
          record.search_city = city;
          record.search_state = stateCode;
          record.source = 'lawyers-com';
          record.practice_area = practiceArea || '';
          totalYielded++;
          yield record;
        }

        log.info(`Page ${page}: ${records.length} listings (${totalYielded} total for ${city})`);

        // Check if there's a next page
        if (!this.hasNextPage($, page)) {
          log.success(`No more pages after page ${page} for ${city}, ${stateCode}`);
          break;
        }

        page++;
        pagesFetched++;
      }

      log.success(`Completed ${city}, ${stateCode}: ${totalYielded} listings across ${page} page(s)`);
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
          `Lawyers.com: Cannot determine state for city "${options.city}". ` +
          `Please provide --state (e.g., --state NY)`
        );
        return [];
      }
      return [{ city: options.city, stateCode }];
    }

    // Option 3: default cities
    return [...this._cityEntries];
  }

  // ---------------------------------------------------------------------------
  // Override transformResult
  // ---------------------------------------------------------------------------

  transformResult(attorney, practiceArea) {
    attorney.source = 'lawyers-com';
    attorney.practice_area = practiceArea || '';
    return attorney;
  }
}

module.exports = new LawyersComScraper();
