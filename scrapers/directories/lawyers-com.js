/**
 * Lawyers.com Lawyer Directory Scraper
 *
 * Source: https://www.lawyers.com/
 * Method: HTTP GET + Cheerio HTML parsing (server-rendered via Cloudflare CDN)
 * Owner:  Internet Brands (same parent as Martindale)
 *
 * URL Patterns:
 *   By practice area + city: https://www.lawyers.com/{practice-area}/{city}/{state}/law-firms/
 *   Paginated:               https://www.lawyers.com/{practice-area}/{city}/{state}/law-firms/?page=2
 *   All lawyers in a city:   https://www.lawyers.com/all/{city}/{state}/law-firms/
 *
 * HTML Structure (as of 2026-02):
 *   Listings are within `.search-results-list` as alternating sibling divs:
 *     - `.summary-content`  — firm name, attorney name, serving area, practice areas
 *     - `.contact-info`     — phone (`.srl-phone`) and website (`.srl-website`)
 *   These two sibling divs form one logical listing.
 *
 * Data per listing:
 *   - Firm name (<h2> within a.profile-link)
 *   - Attorney name (a.attorney within .main-attorney)
 *   - Position (.position within .main-attorney)
 *   - Serving area / address (.srl-serving)
 *   - Phone (.srl-phone: data-ctn-rtn-alt attr or inner a[href^="tel:"])
 *   - Website (.srl-website a href)
 *   - Profile URL (a.profile-link href for firm, a.attorney href for attorney)
 *   - Rating (.number within .review-summary-header)
 *   - Review count (.number-of-reviews text)
 *   - Practice areas (text near briefcase icon)
 *
 * IMPORTANT: The page HTML includes the word "recaptcha" in CSS rules for an
 * error form, but this is NOT an actual blocking CAPTCHA. The detectCaptcha()
 * method from BaseScraper must be overridden to avoid false positives.
 *
 * HIGH PRIORITY data source — server-rendered, no blocking CAPTCHA.
 * Covers US + Canada with granular practice area categorization.
 * Used for waterfall cross-reference enrichment (matching by city+name).
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
      pageSize: 31, // ~31 listings per page on Lawyers.com
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
  // CAPTCHA detection override
  // ---------------------------------------------------------------------------

  /**
   * Override BaseScraper.detectCaptcha() because Lawyers.com pages contain
   * the word "recaptcha" in CSS/JS (for an error form), which is NOT an actual
   * blocking CAPTCHA. We check for real blocking signals instead.
   */
  detectCaptcha(body) {
    if (!body) return false;
    const lower = body.toLowerCase();
    // Real CAPTCHA blocking: Cloudflare challenge page or explicit challenge-form
    if (lower.includes('challenge-form')) return true;
    if (lower.includes('cf-challenge-running')) return true;
    if (lower.includes('just a moment') && lower.includes('cloudflare') && body.length < 10000) return true;
    // If we got a real HTML page with content, it's not a CAPTCHA
    return false;
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
   * Parse all listings from a Lawyers.com search results page.
   *
   * The HTML structure uses alternating sibling divs within .search-results-list:
   *   <div class="summary-content">  — firm name, attorney, serving area
   *   <div class="contact-info ..."> — phone (.srl-phone) and website (.srl-website)
   *   <div class="contact-info mobile ..."> — duplicate for mobile (skip)
   *   <div class="summary-content">  — next listing
   *   ...
   *
   * We iterate each .summary-content and grab the immediately following
   * .contact-info sibling (desktop, not mobile) for phone/website data.
   */
  parseResultsPage($) {
    const results = [];
    const summaryContents = $('.search-results-list .summary-content');

    summaryContents.each((_, el) => {
      try {
        const $listing = $(el);
        // Get the next sibling .contact-info (desktop version, not mobile)
        const $contactInfo = $listing.next('.contact-info');

        // --- Firm name ---
        const firmName = $listing.find('a.profile-link h2').first().text().trim();

        // --- Firm profile URL ---
        let firmProfileUrl = ($listing.find('a.profile-link').first().attr('href') || '').trim();
        if (firmProfileUrl && firmProfileUrl.startsWith('//')) {
          firmProfileUrl = `https:${firmProfileUrl}`;
        } else if (firmProfileUrl && firmProfileUrl.startsWith('/')) {
          firmProfileUrl = `${this.baseUrl}${firmProfileUrl}`;
        }

        // --- Main attorney name and URL ---
        const attorneyEl = $listing.find('.main-attorney a.attorney');
        const attorneyName = attorneyEl.text().trim();
        let attorneyUrl = (attorneyEl.attr('href') || '').trim();
        if (attorneyUrl && attorneyUrl.startsWith('//')) {
          attorneyUrl = `https:${attorneyUrl}`;
        } else if (attorneyUrl && attorneyUrl.startsWith('/')) {
          attorneyUrl = `${this.baseUrl}${attorneyUrl}`;
        }

        // --- Position ---
        const position = $listing.find('.main-attorney .position').text().trim();

        // --- Serving area / address ---
        const servingText = $listing.find('.srl-serving').text().trim();

        // --- Practice areas (text from briefcase icon list items) ---
        const practiceAreas = [];
        $listing.find('li:has(use[xlink\\:href="#iconBriefcaseSearch"]) p').each((_, pEl) => {
          const text = $(pEl).text().trim();
          if (text) practiceAreas.push(text);
        });

        // --- Rating ---
        let rating = 0;
        const ratingText = $listing.find('.review-summary-header .number').first().text().trim();
        if (ratingText) {
          const parsed = parseFloat(ratingText);
          if (!isNaN(parsed)) rating = parsed;
        }

        // --- Review count ---
        let reviewCount = 0;
        const reviewText = $listing.find('.number-of-reviews').text().trim();
        const reviewMatch = reviewText.match(/(\d+)\s*review/i);
        if (reviewMatch) {
          reviewCount = parseInt(reviewMatch[1], 10);
        }

        // --- Phone (from contact-info sibling) ---
        let phone = '';
        const phoneEl = $contactInfo.find('.srl-phone');
        // Prefer data-ctn-rtn-alt (the original number, not the tracking number)
        const rtnAlt = phoneEl.attr('data-ctn-rtn-alt') || '';
        if (rtnAlt) {
          phone = rtnAlt.trim();
        } else {
          // Fallback: use the tel: href from the inner <a> element
          const telHref = phoneEl.find('a[href^="tel:"]').attr('href') || '';
          if (telHref) {
            phone = telHref.replace('tel:', '').trim();
          } else {
            // Last fallback: use data-ctn-rtn (tracking number, better than nothing)
            const rtn = phoneEl.attr('data-ctn-rtn') || '';
            if (rtn) phone = rtn.trim();
          }
        }

        // --- Website (from contact-info sibling) ---
        let website = ($contactInfo.find('.srl-website a').attr('href') || '').trim();

        // --- Parse city/state/zip from serving text ---
        const location = this._parseServingText(servingText);

        // --- Split attorney name ---
        const { firstName, lastName } = this.splitName(attorneyName);

        // Skip if we have neither firm name nor attorney name
        if (!firmName && !attorneyName) return;

        results.push({
          first_name: firstName,
          last_name: lastName,
          full_name: attorneyName,
          firm_name: firmName,
          position: position,
          city: location.city,
          state: location.state,
          zip: location.zip,
          address: location.address,
          phone: phone,
          email: '',  // Not available in listing HTML
          website: website,
          profile_url: attorneyUrl || firmProfileUrl,
          firm_profile_url: firmProfileUrl,
          serving_area: servingText,
          practice_areas: practiceAreas.join('; '),
          rating: rating,
          review_count: reviewCount,
          source: 'lawyers-com',
        });
      } catch (err) {
        log.warn(`Lawyers.com: Failed to parse listing: ${err.message}`);
      }
    });

    return results;
  }

  /**
   * Parse the serving/address text to extract city, state, zip, and street address.
   *
   * Lawyers.com uses several formats:
   *   "Serving New York, NY and Statewide"
   *   "546 5th Avenue, 5th Floor, New York, NY 10036"
   *   "745 5th Avenue, Suite 500, New York, NY 10151+7 locations"
   *   "New York, NY"
   */
  _parseServingText(text) {
    if (!text) return { city: '', state: '', zip: '', address: '' };

    // Remove "Serving " prefix
    let cleaned = text.replace(/^Serving\s+/i, '');
    // Remove trailing "+N locations" or "and Nearby Areas" or "and Statewide"
    cleaned = cleaned.replace(/\+\d+\s*location[s]?/i, '').replace(/\s*and\s+(nearby|statewide).*/i, '').trim();

    // Try full address pattern: "Street, City, ST ZIP" or "Street, Floor, City, ST ZIP"
    // Match the last occurrence of "City, ST ZIP" or "City, ST"
    const fullMatch = cleaned.match(/^(.*?),\s*([^,]+),\s*([A-Z]{2})\s*([\d-]*)\s*$/);
    if (fullMatch) {
      return {
        address: fullMatch[1].trim(),
        city: fullMatch[2].trim(),
        state: fullMatch[3],
        zip: (fullMatch[4] || '').trim(),
      };
    }

    // Try "City, ST ZIP"
    const cszMatch = cleaned.match(/^([^,]+),\s*([A-Z]{2})\s*([\d-]*)\s*$/);
    if (cszMatch) {
      return {
        address: '',
        city: cszMatch[1].trim(),
        state: cszMatch[2],
        zip: (cszMatch[3] || '').trim(),
      };
    }

    // Fallback: try to find "City, ST" anywhere in the text
    const fallbackMatch = cleaned.match(/([A-Za-z\s.]+),\s*([A-Z]{2})/);
    if (fallbackMatch) {
      return {
        address: '',
        city: fallbackMatch[1].trim(),
        state: fallbackMatch[2],
        zip: '',
      };
    }

    return { city: '', state: '', zip: '', address: '' };
  }

  /**
   * Extract total result count from the page.
   * Lawyers.com shows "N Results" text on the page.
   */
  extractResultCount($) {
    const bodyText = $('body').text();
    // Match patterns like "4,950 Results", "123 Results"
    const countMatch = bodyText.match(/(\d[\d,]*)\s*Result/i);
    if (countMatch) {
      return parseInt(countMatch[1].replace(/,/g, ''), 10);
    }

    // Fallback: count listings on the page
    const listings = $('.search-results-list .summary-content').length;
    return listings > 0 ? 99999 : 0;
  }

  /**
   * Check if there is a next page available.
   */
  hasNextPage($, currentPage) {
    const nextPage = currentPage + 1;

    // Look for pagination link to next page number
    let found = false;
    $('a[href*="page="]').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes(`page=${nextPage}`)) {
        found = true;
        return false; // break
      }
    });
    if (found) return true;

    // Check for "Next" link
    const nextLink = $('a:contains("Next")');
    if (nextLink.length > 0) {
      const href = nextLink.attr('href') || '';
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

    // Phone — prefer real phone from text, NOT data-ctn-rtn (Lawyers.com call-tracking numbers)
    const phoneMatch = bodyText.match(/(?:Phone|Tel(?:ephone)?|Office|Call)[:\s]*([\d().\s-]{10,})/i);
    if (phoneMatch) {
      const cleaned = phoneMatch[1].replace(/[^\d()-.\s]/g, '').trim();
      if (/\d{3}.*\d{3}.*\d{4}/.test(cleaned)) result.phone = cleaned;
    }
    if (!result.phone) {
      // Fallback: use data-ctn-rtn-alt (original number)
      const phoneEl = $('[data-ctn-rtn-alt]');
      const rtnAlt = phoneEl.attr('data-ctn-rtn-alt') || '';
      if (rtnAlt) {
        result.phone = rtnAlt.trim();
      }
    }

    // Email from mailto links
    const mailtoLink = $('a[href^="mailto:"]').first();
    if (mailtoLink.length) {
      result.email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
    }

    // Website
    const websiteEl = $('a.website-click, a[class*="website"]');
    if (websiteEl.length) {
      const href = (websiteEl.first().attr('href') || '').trim();
      if (href && !href.includes('lawyers.com')) {
        result.website = href;
      }
    }
    if (!result.website) {
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().toLowerCase().trim();
        if ((text.includes('visit') || text.includes('website')) &&
            !href.includes('lawyers.com') && !this.isExcludedDomain(href)) {
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
   * Async generator that yields attorney/firm records from Lawyers.com.
   *
   * Options:
   *   city      - Single city name (string)
   *   state     - State code for the city (e.g., 'NY', 'CA')
   *   cities    - Array of { city, stateCode } objects
   *   maxPages  - Max pages to fetch per city (for testing)
   *   maxCities - Max cities to search (for testing)
   */
  async *search(practiceArea, options = {}) {
    const isTestMode = !!(options.maxPages || options.maxCities);
    const rateLimiter = new RateLimiter({
      minDelay: isTestMode ? 2000 : 5000,
      maxDelay: isTestMode ? 4000 : 10000,
    });
    const maxBlockRetries = isTestMode ? 1 : 3;

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
          log.warn(`Got ${response.statusCode} from Lawyers.com on page ${page} for ${city}`);
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

        // Check for real CAPTCHA/challenge (uses our override, not BaseScraper's)
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA/challenge detected on page ${page} for ${city} — skipping`);
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

    // Option 3: default cities (respect maxCities limit)
    const entries = [...this._cityEntries];
    if (options.maxCities && options.maxCities < entries.length) {
      return entries.slice(0, options.maxCities);
    }
    return entries;
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
