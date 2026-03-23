/**
 * BBB (Better Business Bureau) — Contractor Scraper
 *
 * Source: https://www.bbb.org
 * Method: HTML scraping of category listing pages + profile page enrichment
 * Coverage: US and Canada — 2,000+ results per city/category combo
 *
 * URL patterns:
 *   Listing: https://www.bbb.org/us/{state}/{city}/category/{category-slug}?page={N}
 *   Profile: https://www.bbb.org/us/{state}/{city}/profile/{category}/{slug-ID}
 *
 * Data available from listing pages:
 *   - Business name, phone, address (street, city, state, zip), BBB rating, profile URL
 *
 * Data available from profile pages (enrichment):
 *   - Website, owner/principal names, fax, years in business, categories, license numbers
 *   - NOTE: Email is NOT publicly displayed — BBB uses an "Email this Business" form.
 *     However, websites enable email derivation via the enrichment pipeline.
 *
 * Results per page: ~15 businesses
 * Pagination: ?page=N query parameter
 *
 * Category slugs (confirmed working):
 *   roofing-contractors, plumber, electrician, heating-contractors,
 *   painting-contractors, general-contractor, landscape-contractors
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.bbb.org';
const RESULTS_PER_PAGE = 15;

/**
 * Category slug mapping: practice area -> BBB URL slug
 * These are confirmed working category slugs on bbb.org.
 */
const CATEGORY_SLUGS = {
  'roofing': 'roofing-contractors',
  'painting': 'painting-contractors',
  'plumbing': 'plumber',
  'hvac': 'heating-contractors',
  'electrical': 'electrician',
  'general contractor': 'general-contractor',
  'landscaping': 'landscape-contractors',
  // Veterinary
  'veterinarian': 'veterinarian',
  'vet': 'veterinarian',
  'veterinary': 'veterinarian',
  'animal hospital': 'animal-hospital',
  // Therapy / Counseling
  'therapist': 'counseling',
  'therapy': 'counseling',
  'counseling': 'counseling',
  'psychologist': 'psychologist',
  'counselor': 'counseling',
  'mental health': 'mental-health-services',
  // Other healthcare
  'dentist': 'dentist',
  'chiropractor': 'chiropractor',
  'optometrist': 'optometrist',
  'physical therapy': 'physical-therapist',
  'massage': 'massage-therapist',
};

/**
 * State code -> { abbrev, name } for URL construction.
 * BBB uses lowercase 2-letter state abbreviations in URLs.
 */
const STATE_MAP = {
  'AL': 'al', 'AK': 'ak', 'AZ': 'az', 'AR': 'ar', 'CA': 'ca',
  'CO': 'co', 'CT': 'ct', 'DE': 'de', 'FL': 'fl', 'GA': 'ga',
  'HI': 'hi', 'ID': 'id', 'IL': 'il', 'IN': 'in', 'IA': 'ia',
  'KS': 'ks', 'KY': 'ky', 'LA': 'la', 'ME': 'me', 'MD': 'md',
  'MA': 'ma', 'MI': 'mi', 'MN': 'mn', 'MS': 'ms', 'MO': 'mo',
  'MT': 'mt', 'NE': 'ne', 'NV': 'nv', 'NH': 'nh', 'NJ': 'nj',
  'NM': 'nm', 'NY': 'ny', 'NC': 'nc', 'ND': 'nd', 'OH': 'oh',
  'OK': 'ok', 'OR': 'or', 'PA': 'pa', 'RI': 'ri', 'SC': 'sc',
  'SD': 'sd', 'TN': 'tn', 'TX': 'tx', 'UT': 'ut', 'VT': 'vt',
  'VA': 'va', 'WA': 'wa', 'WV': 'wv', 'WI': 'wi', 'WY': 'wy',
  'DC': 'dc',
};

/**
 * Default cities to search — major US metros with high BBB listing density.
 * Each entry is { city, state } where state is the 2-letter code.
 */
const DEFAULT_CITIES = [
  { city: 'Miami', state: 'FL' },
  { city: 'Houston', state: 'TX' },
  { city: 'Los Angeles', state: 'CA' },
  { city: 'New York', state: 'NY' },
  { city: 'Columbus', state: 'OH' },
  { city: 'Philadelphia', state: 'PA' },
  { city: 'Chicago', state: 'IL' },
  { city: 'Atlanta', state: 'GA' },
  { city: 'Charlotte', state: 'NC' },
  { city: 'Detroit', state: 'MI' },
];


class BbbScraper extends BaseScraper {
  constructor() {
    super({
      name: 'bbb_contractors',
      stateCode: 'BBB',
      baseUrl: BASE_URL,
      pageSize: RESULTS_PER_PAGE,
      practiceAreaCodes: {
        'roofing': 'Roofing Contractors',
        'painting': 'Painting Contractors',
        'plumbing': 'Plumbers',
        'hvac': 'Heating Contractors',
        'electrical': 'Electricians',
        'general contractor': 'General Contractors',
        'landscaping': 'Landscaping',
        'veterinarian': 'Veterinarians',
        'therapist': 'Counseling Services',
        'counseling': 'Counseling Services',
        'psychologist': 'Psychologists',
        'dentist': 'Dentists',
        'chiropractor': 'Chiropractors',
        'massage': 'Massage Therapists',
      },
      defaultCities: DEFAULT_CITIES.map(c => `${c.city}, ${c.state}`),
    });
  }

  /**
   * Override CAPTCHA detection — BBB loads Google reCAPTCHA Enterprise as a
   * precautionary script on every page, but does NOT actually block with a
   * challenge. The base class detectCaptcha() picks up the "recaptcha" string
   * and false-positives. We override to only trigger on actual blocking patterns.
   */
  detectCaptcha(body) {
    // Only flag as CAPTCHA if we see an actual Cloudflare challenge or
    // a blocking interstitial, not just a precautionary reCAPTCHA script tag.
    return body.includes('challenge-form') ||
           body.includes('Just a moment') ||
           body.includes('cf-browser-verification') ||
           body.includes('g-recaptcha" data-sitekey');
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() {
    throw new Error(`${this.name}: buildSearchUrl() is not used — search() is overridden`);
  }
  parseResultsPage() {
    throw new Error(`${this.name}: parseResultsPage() is not used — search() is overridden`);
  }
  extractResultCount() {
    throw new Error(`${this.name}: extractResultCount() is not used — search() is overridden`);
  }

  /**
   * Override transformResult to use 'bbb_contractors' source.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'bbb_contractors';
    lead.practice_area = practiceArea || lead.practice_area || '';
    return lead;
  }

  /**
   * Build the BBB category listing URL.
   *
   * @param {string} stateAbbrev - 2-letter state code (e.g., 'FL')
   * @param {string} citySlug - City name slugified (e.g., 'miami')
   * @param {string} categorySlug - BBB category slug (e.g., 'roofing-contractors')
   * @param {number} page - Page number (1-indexed)
   * @returns {string} Full URL
   */
  _buildListingUrl(stateAbbrev, citySlug, categorySlug, page) {
    const stateSlug = STATE_MAP[stateAbbrev] || stateAbbrev.toLowerCase();
    let url = `${BASE_URL}/us/${stateSlug}/${citySlug}/category/${categorySlug}`;
    if (page > 1) {
      url += `?page=${page}`;
    }
    return url;
  }

  /**
   * Slugify a city name for the BBB URL.
   * "Fort Lauderdale" -> "fort-lauderdale"
   * "St. Petersburg" -> "st-petersburg"
   * "New York" -> "new-york"
   */
  _slugifyCity(city) {
    return city
      .toLowerCase()
      .replace(/\./g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '');
  }

  /**
   * Parse the category listing page for business listings.
   * Each listing has: name, phone, address, rating, profile URL.
   *
   * @param {string} html - Raw HTML of the category listing page
   * @returns {{ results: object[], totalCount: number }}
   */
  _parseListingPage(html) {
    const $ = cheerio.load(html);
    const results = [];

    // Extract total count from "Showing: X results" text
    let totalCount = 0;
    const bodyText = $('body').text();
    const countMatch = bodyText.match(/Showing:\s*([\d,]+)\s*results/i);
    if (countMatch) {
      totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);
    }

    // Find all profile links — they match pattern /us/XX/city/profile/category/slug
    const profileLinks = [];
    $('a[href*="/profile/"]').each((_, el) => {
      const href = $(el).attr('href') || '';
      // Must match the BBB profile URL pattern
      if (!href.match(/\/us\/[a-z]{2}\/[^/]+\/profile\//)) return;
      // Skip /details, /customer-reviews, /complaints sub-pages
      if (/\/(details|customer-reviews|complaints|accreditation-information)\/?(\?|$)/.test(href)) return;
      profileLinks.push({ el, href: href.startsWith('http') ? href : `${BASE_URL}${href}` });
    });

    // Deduplicate by href (same profile may appear multiple times)
    const seenHrefs = new Set();
    const uniqueLinks = [];
    for (const link of profileLinks) {
      // Normalize: strip addressId and trailing slashes for dedup
      const baseHref = link.href.replace(/\/addressId\/\d+/, '').replace(/\/$/, '');
      if (!seenHrefs.has(baseHref)) {
        seenHrefs.add(baseHref);
        uniqueLinks.push(link);
      }
    }

    for (const link of uniqueLinks) {
      try {
        const result = this._extractListingFromContext($, link.el, link.href);
        if (result && result.firm_name) {
          results.push(result);
        }
      } catch (err) {
        // Skip malformed listings silently
      }
    }

    return { results, totalCount };
  }

  /**
   * Extract a business listing by looking at the surrounding text context.
   * BBB listing pages have structured blocks with name, rating, phone, address.
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @param {CheerioElement} linkEl - The profile link element
   * @param {string} profileUrl - The full profile URL
   * @returns {object|null} Lead object or null
   */
  _extractListingFromContext($, linkEl, profileUrl) {
    const $link = $(linkEl);
    const firmName = $link.text().trim();

    // Skip non-business links (e.g., "Get a Quote", "Book Appointment", category links)
    if (!firmName || firmName.length < 3 || firmName.length > 200) return null;
    if (/^(get a quote|book|learn more|see more|read more|view|visit)/i.test(firmName)) return null;
    // Skip "moreservice areas" and other junk text links
    if (/^more\s?service/i.test(firmName)) return null;
    if (/^(service area|accredit|complaint|review|bbb rating)/i.test(firmName)) return null;

    // Walk up to find the containing block (parent container for this listing)
    // Look for the nearest ancestor that contains phone/address info
    let $container = $link.parent();
    for (let i = 0; i < 8; i++) {
      const text = $container.text();
      if (text.includes('BBB Rating') || /\(\d{3}\)\s*\d{3}-\d{4}/.test(text)) break;
      const $parent = $container.parent();
      if ($parent.length === 0) break;
      $container = $parent;
    }

    const containerText = $container.text();

    // Extract phone number
    let phone = '';
    const phoneMatch = containerText.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
    if (phoneMatch) {
      phone = `(${phoneMatch[1]}) ${phoneMatch[2]}-${phoneMatch[3]}`;
    }

    // Extract BBB rating
    let rating = '';
    const ratingMatch = containerText.match(/BBB\s*Rating:\s*([A-F][+-]?)/i);
    if (ratingMatch) {
      rating = ratingMatch[1];
    }

    // Extract address — look for "City, ST ZIP" pattern
    let city = '';
    let state = '';
    let zip = '';
    let address = '';

    // Try to extract from the profile URL first (most reliable for city/state)
    const urlParts = profileUrl.match(/\/us\/([a-z]{2})\/([^/]+)\/profile\//);
    if (urlParts) {
      state = urlParts[1].toUpperCase();
      city = urlParts[2].replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }

    // Try to extract full address from text
    // Pattern: street address line, then "City, ST ZIP"
    const addrMatch = containerText.match(
      /(\d+[^,\n]{3,60}),\s*([A-Za-z\s.]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/
    );
    if (addrMatch) {
      address = addrMatch[1].trim();
      city = addrMatch[2].trim();
      state = addrMatch[3];
      zip = addrMatch[4];
    } else {
      // Try just City, ST ZIP
      const cityStateMatch = containerText.match(/([A-Za-z\s.]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
      if (cityStateMatch) {
        city = cityStateMatch[1].trim();
        state = cityStateMatch[2];
        zip = cityStateMatch[3];
      }
    }

    return {
      first_name: '',
      last_name: '',
      firm_name: firmName,
      address,
      city,
      state,
      zip,
      phone,
      email: '',
      website: '',
      bar_number: '',
      bar_status: rating ? `BBB ${rating}` : '',
      profile_url: profileUrl,
      source: 'bbb_contractors',
    };
  }

  /**
   * Parse a BBB business profile page for additional details.
   * Extracts: website, email (Cloudflare-decoded), owner names, phones, rating.
   *
   * BBB uses Cloudflare email protection — emails are obfuscated in:
   *   1. <a href="/cdn-cgi/l/email-protection#HEX"> links
   *   2. <span data-cfemail="HEX" class="__cf_email__"> spans
   *   3. <a href=".../email-this-business?email=primary"> (BBB form — no real email)
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object} Additional fields
   */
  parseProfilePage($) {
    const text = $('body').text();
    const extra = {};

    // --- Email extraction (Cloudflare-obfuscated) ---

    // Method 1: Cloudflare email protection links (/cdn-cgi/l/email-protection#HEX)
    $('a[href*="email-protection"]').each((_, el) => {
      if (extra.email) return;
      const href = ($(el).attr('href') || '');
      const hexMatch = href.match(/email-protection#([a-f0-9]+)/i);
      if (hexMatch) {
        const decoded = this.decodeCloudflareEmail(hexMatch[1]);
        if (decoded && decoded.includes('@') && !decoded.includes('bbb.org') && !decoded.includes('bbb.com')) {
          extra.email = decoded.toLowerCase();
        }
      }
    });

    // Method 2: Cloudflare-obfuscated spans (data-cfemail attribute)
    if (!extra.email) {
      $('span[data-cfemail], a[data-cfemail]').each((_, el) => {
        if (extra.email) return;
        const encoded = $(el).attr('data-cfemail');
        if (encoded) {
          const decoded = this.decodeCloudflareEmail(encoded);
          if (decoded && decoded.includes('@') && !decoded.includes('bbb.org') && !decoded.includes('bbb.com')) {
            extra.email = decoded.toLowerCase();
          }
        }
      });
    }

    // Method 3: Standard mailto: links
    if (!extra.email) {
      $('a[href^="mailto:"]').each((_, el) => {
        if (extra.email) return;
        const href = ($(el).attr('href') || '').replace('mailto:', '').trim().toLowerCase();
        if (href && !href.includes('bbb.org') && !href.includes('bbb.com') && href.includes('@')) {
          extra.email = href.split('?')[0]; // Strip query params
        }
      });
    }

    // Method 4: Plain-text email in page body (fallback)
    if (!extra.email) {
      const emailMatch = text.match(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/i);
      if (emailMatch) {
        const email = emailMatch[0].toLowerCase();
        if (!email.includes('bbb.org') && !email.includes('bbb.com')) {
          extra.email = email;
        }
      }
    }

    // --- Website extraction ---

    // Primary: "Visit Website" link text
    $('a[href]').each((_, el) => {
      if (extra.website) return;
      const href = ($(el).attr('href') || '').trim();
      const linkText = $(el).text().trim().toLowerCase();
      if (linkText === 'visit website' || linkText.includes('visit website')) {
        if (href && href.startsWith('http') && !href.includes('bbb.org') && !this.isExcludedDomain(href)) {
          extra.website = href;
        }
      }
    });

    // Fallback: first external business-looking URL on the page
    if (!extra.website) {
      $('a[href]').each((_, el) => {
        if (extra.website) return;
        const href = ($(el).attr('href') || '').trim();
        if (
          href.startsWith('http') &&
          !href.includes('bbb.org') &&
          !this.isExcludedDomain(href) &&
          !href.includes('indeed.com') &&
          !href.includes('glassdoor.com') &&
          !href.includes('sharer') &&
          !href.includes('intent/tweet') &&
          !href.includes('shareArticle') &&
          !href.includes('cdn-cgi')
        ) {
          if (/\.(com|net|org|co|us|io|biz|info)/.test(href)) {
            extra.website = href;
          }
        }
      });
    }

    // --- Owner/principal name extraction ---

    // Pattern 1: "Mr./Ms./Mrs./Dr. Name, Title" near management section
    const principalMatch = text.match(
      /(?:Principal|Management|Owner|President|Contact)[^:]*?:\s*(?:Mr\.|Ms\.|Mrs\.|Dr\.)?\s*([A-Z][a-z]+(?:\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)+)/
    );
    if (principalMatch) {
      const parts = principalMatch[1].trim().split(/\s+/);
      if (parts.length >= 2) {
        extra.first_name = parts[0];
        extra.last_name = parts[parts.length - 1];
      }
    }

    // Pattern 2: "Mr./Ms. FirstName LastName, Title" (broader)
    if (!extra.first_name) {
      const ownerPattern = text.match(
        /(?:Mr\.|Ms\.|Mrs\.|Dr\.)\s+([A-Z][a-z]+(?:\s+(?:\([^)]+\)\s+)?[A-Z][a-z]+)+),\s*(?:President|Owner|Manager|CEO|Principal|Founder|Co-Owner|Vice President|Director)/
      );
      if (ownerPattern) {
        const nameParts = ownerPattern[1].replace(/\([^)]+\)\s*/g, '').trim().split(/\s+/);
        if (nameParts.length >= 2) {
          extra.first_name = nameParts[0];
          extra.last_name = nameParts[nameParts.length - 1];
        }
      }
    }

    // --- Additional phone numbers ---
    const phoneMatches = text.match(/\(\d{3}\)\s*\d{3}-\d{4}/g);
    if (phoneMatches && phoneMatches.length > 1) {
      extra.all_phones = [...new Set(phoneMatches)].join('; ');
    }

    // --- BBB rating ---
    const ratingMatch = text.match(/BBB\s*Rating:\s*([A-F][+-]?)/i);
    if (ratingMatch) {
      extra.bar_status = `BBB ${ratingMatch[1]}`;
    }

    return extra;
  }

  /**
   * Check if the scraper has a profile parser (enables waterfall enrichment).
   */
  get hasProfileParser() {
    return true;
  }

  /**
   * Parse a "City, ST" string into city and state components.
   * @param {string} cityInput - e.g., "Miami, FL" or "Miami"
   * @returns {{ city: string, state: string }}
   */
  _parseCityInput(cityInput) {
    const parts = cityInput.split(',').map(s => s.trim());
    if (parts.length >= 2) {
      return { city: parts[0], state: parts[1].toUpperCase() };
    }
    return { city: parts[0], state: '' };
  }

  /**
   * Main search generator — scrapes BBB category listing pages.
   *
   * Strategy:
   *   1. Build category URL from practice area + city + state
   *   2. Paginate through listing pages (15 results each)
   *   3. Parse each listing for business data
   *   4. Profile pages can be enriched via the waterfall pipeline (profile_url is set)
   *
   * @param {string} practiceArea - Practice area to search (e.g., 'roofing')
   * @param {object} options - Search options (maxPages, maxCities, city)
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Resolve category slug
    const practiceKey = (practiceArea || 'roofing').toLowerCase().trim();
    const categorySlug = CATEGORY_SLUGS[practiceKey];

    if (!categorySlug) {
      log.warn(`BBB: Unknown category "${practiceArea}" — available: ${Object.keys(CATEGORY_SLUGS).join(', ')}`);
      log.info('Defaulting to "roofing-contractors"');
    }

    const slug = categorySlug || 'roofing-contractors';

    // Determine cities to search
    let citiesToSearch;
    if (options.city) {
      citiesToSearch = [options.city];
    } else {
      citiesToSearch = this.defaultCities;
    }
    if (options.maxCities) {
      citiesToSearch = citiesToSearch.slice(0, options.maxCities);
    }

    log.scrape(`BBB search: category=${slug}, cities=${citiesToSearch.length}`);

    // Track seen profile URLs to avoid duplicates across cities
    const seenUrls = new Set();
    let totalYielded = 0;

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const cityInput = citiesToSearch[ci];
      const { city, state } = this._parseCityInput(cityInput);

      if (!state) {
        log.warn(`BBB: Skipping "${cityInput}" — no state code (use "City, ST" format)`);
        continue;
      }

      if (!STATE_MAP[state]) {
        log.warn(`BBB: Unknown state "${state}" — skipping ${cityInput}`);
        continue;
      }

      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };

      const citySlug = this._slugifyCity(city);
      log.scrape(`Searching BBB: ${practiceArea || 'roofing'} in ${city}, ${state}`);

      let page = 1;
      let pagesFetched = 0;
      let totalResults = 0;
      let consecutiveEmpty = 0;
      let cityYielded = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${city}, ${state}`);
          break;
        }

        const url = this._buildListingUrl(state, citySlug, slug, page);
        log.info(`Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`BBB request failed for ${city}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting / blocking
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`BBB returned ${response.statusCode} for ${city}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 404) {
          log.warn(`BBB returned 404 for ${city}, ${state} — city/category may not exist`);
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`BBB returned ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA / Cloudflare challenge
        if (this.detectCaptcha(response.body)) {
          log.warn(`CAPTCHA detected on BBB page for ${city} — skipping`);
          yield { _captcha: true, city: `${city}, ${state}`, page };
          break;
        }

        // Parse the listing page
        const { results, totalCount } = this._parseListingPage(response.body);

        if (page === 1 && totalCount > 0) {
          totalResults = totalCount;
          const totalPages = Math.ceil(totalResults / RESULTS_PER_PAGE);
          log.success(`Found ${totalResults.toLocaleString()} results for ${city}, ${state} (est. ${totalPages} pages)`);
        }

        if (results.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            log.info(`2 consecutive empty pages — done with ${city}`);
            break;
          }
          if (page === 1) {
            log.info(`No results for ${practiceArea || 'roofing'} in ${city}, ${state}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        for (const result of results) {
          // Dedup by profile URL across cities
          if (result.profile_url && seenUrls.has(result.profile_url)) continue;
          if (result.profile_url) seenUrls.add(result.profile_url);

          yield this.transformResult(result, practiceArea);
          cityYielded++;
          totalYielded++;
        }

        log.info(`Page ${page}: ${results.length} listings (${cityYielded} total from ${city})`);

        // Check if we've likely exhausted results
        // BBB shows ~15 per page; if we got fewer, we're probably on the last page
        if (results.length < RESULTS_PER_PAGE - 2) {
          log.success(`Completed ${city}, ${state}: ${cityYielded} leads`);
          break;
        }

        // Safety: don't paginate beyond reasonable limits
        const maxPossiblePages = totalResults > 0
          ? Math.ceil(totalResults / RESULTS_PER_PAGE)
          : 200;

        if (page >= maxPossiblePages) {
          log.success(`Completed all ${page} pages for ${city}, ${state}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }

    log.success(`BBB scrape complete — ${totalYielded} total leads from ${seenUrls.size} unique businesses`);
  }
}

module.exports = new BbbScraper();
