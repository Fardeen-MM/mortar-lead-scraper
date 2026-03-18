/**
 * Which? Trusted Traders UK — Consumer-endorsed tradespeople directory
 *
 * Source: https://trustedtraders.which.co.uk
 * Platform: Server-rendered HTML with JSON-LD structured data
 *
 * URL pattern:
 *   Listing: /{trade-plural}-in-{city}/
 *   Pagination: /{trade-plural}-in-{city}/{page}/?continue=1&sort_type=distance
 *   Profile: /businesses/{slug}/
 *
 * Data extraction:
 *   - JSON-LD <script type="application/ld+json"> on listing pages: name, phone, email, geo
 *   - HTML: postcode from <span> near listing cards
 *   - Profile pages: website from <a class="url">, address from JSON-LD
 *   - Result count: <span class="total">N</span>
 *
 * 12 results per page. ~200+ traders per trade per major city.
 * Email AND phone available on listing pages (rare for directories).
 *
 * Trade URL slugs discovered from sitemap:
 *   plumbers, roofers, builders, electricians, painters, painters-and-decorators,
 *   glaziers, carpenters, plasterers, gardeners, bathroom-fitters-and-designers,
 *   kitchen-installation, boiler-installation, damp-proofing-and-timber-preservation,
 *   drain-and-sewer-services, landscape-designers, renewable-energy, etc.
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://trustedtraders.which.co.uk';
const PAGE_SIZE = 12; // 12 results per page

/**
 * Map of user-friendly trade names to URL slugs used by Which? Trusted Traders.
 */
const TRADE_SLUGS = {
  'plumber': 'plumbers',
  'plumbers': 'plumbers',
  'plumbing': 'plumbers',
  'roofer': 'roofers',
  'roofers': 'roofers',
  'roofing': 'roofers',
  'builder': 'builders',
  'builders': 'builders',
  'building': 'builders',
  'electrician': 'electricians',
  'electricians': 'electricians',
  'electrical': 'electricians',
  'painter': 'painters-and-decorators',
  'painters': 'painters-and-decorators',
  'painting': 'painters-and-decorators',
  'decorator': 'painters-and-decorators',
  'decorators': 'painters-and-decorators',
  'painters and decorators': 'painters-and-decorators',
  'glazier': 'glaziers',
  'glaziers': 'glaziers',
  'glazing': 'glaziers',
  'carpenter': 'carpenters',
  'carpenters': 'carpenters',
  'carpentry': 'carpenters',
  'plasterer': 'plasterers',
  'plasterers': 'plasterers',
  'plastering': 'plasterers',
  'gardener': 'gardeners',
  'gardeners': 'gardeners',
  'gardening': 'gardeners',
  'bathroom': 'bathroom-fitters-and-designers',
  'bathroom fitter': 'bathroom-fitters-and-designers',
  'kitchen': 'kitchen-installation',
  'kitchen fitter': 'kitchen-installation',
  'boiler': 'boiler-installation',
  'boiler installation': 'boiler-installation',
  'heating': 'central-heating-installation-and-servicing-',
  'central heating': 'central-heating-installation-and-servicing-',
  'damp proofing': 'damp-proofing-and-timber-preservation',
  'drainage': 'drain-and-sewer-services',
  'landscaper': 'landscape-designers',
  'landscaping': 'landscape-designers',
  'removals': 'removals',
  'renewable energy': 'renewable-energy',
  'solar': 'renewable-energy',
  'alarm': 'burglar-alarms',
  'alarms': 'burglar-alarms',
  'security': 'burglar-alarms',
};

/**
 * Default trades to scrape when no specific trade is requested.
 */
const DEFAULT_TRADES = ['plumbers', 'roofers', 'builders', 'electricians', 'painters-and-decorators'];

/**
 * Major UK cities/regions that Which? Trusted Traders covers.
 */
const UK_CITIES = [
  'london', 'birmingham', 'manchester', 'leeds', 'glasgow',
  'edinburgh-and-lothian', 'bristol', 'sheffield', 'nottingham',
  'liverpool', 'brighton', 'southampton', 'reading', 'plymouth',
  'west-midlands', 'west-yorkshire', 'south-yorkshire', 'greater-manchester',
  'merseyside', 'surrey', 'hertfordshire', 'berkshire', 'buckinghamshire',
  'leicestershire', 'northamptonshire', 'nottinghamshire', 'norfolk-broads',
  'east-sussex', 'west-sussex', 'warwickshire', 'worcestershire',
];

class WhichTradersScraper extends BaseScraper {
  constructor() {
    super({
      name: 'which_traders',
      stateCode: 'WHICH-TRADERS',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: Object.fromEntries(
        Object.entries(TRADE_SLUGS).map(([k, v]) => [k, v])
      ),
      defaultCities: UK_CITIES,
    });
  }

  // --- Base class stubs (not used — search() is overridden) ---
  buildSearchUrl() { throw new Error('which_traders: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('which_traders: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('which_traders: extractResultCount() not used — search() overridden'); }

  /**
   * Resolve a user-supplied trade name to a URL slug.
   * Returns null if not found (will use DEFAULT_TRADES instead).
   */
  _resolveTradeSlug(practiceArea) {
    if (!practiceArea) return null;
    const key = practiceArea.toLowerCase().trim();
    if (TRADE_SLUGS[key]) return TRADE_SLUGS[key];
    // Partial match
    for (const [name, slug] of Object.entries(TRADE_SLUGS)) {
      if (name.includes(key) || key.includes(name)) return slug;
    }
    return null;
  }

  /**
   * Build the listing page URL for a trade + city + page.
   */
  _buildListingUrl(tradeSlug, city, page) {
    if (page <= 1) {
      return `${BASE_URL}/${tradeSlug}-in-${city}/`;
    }
    return `${BASE_URL}/${tradeSlug}-in-${city}/${page}/?continue=1&sort_type=distance`;
  }

  /**
   * Extract JSON-LD structured data from a listing page.
   * Returns array of business objects parsed from <script type="application/ld+json">.
   */
  _extractJsonLd($) {
    const businesses = [];
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const data = JSON.parse($(el).html());
        if (data && data['@type'] === 'HomeAndConstructionBusiness') {
          businesses.push(data);
        }
      } catch (e) {
        // Skip malformed JSON-LD
      }
    });
    return businesses;
  }

  /**
   * Extract total result count from listing page.
   * Looks for: <span class="total">200</span>
   */
  _extractTotal($) {
    const totalText = $('span.total').first().text().trim();
    if (totalText) {
      const num = parseInt(totalText.replace(/,/g, ''), 10);
      if (!isNaN(num)) return num;
    }
    return 0;
  }

  /**
   * Check if a "See more" / next page link exists.
   */
  _hasNextPage($) {
    return $('div.results-more a').length > 0;
  }

  /**
   * Extract postcodes from the listing page HTML.
   * These appear near each card as: <span>SE1 4QG</span>
   * Returns array in order matching the JSON-LD entries.
   */
  _extractPostcodes($) {
    const postcodes = [];
    // Each listing card has a postcode in a span near the location text
    // Pattern: spans containing UK postcodes (e.g., SE1 4QG, E1W 1LP)
    $('span').each((_, el) => {
      const text = $(el).text().trim();
      if (/^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$/i.test(text)) {
        postcodes.push(text.toUpperCase());
      }
    });
    return postcodes;
  }

  /**
   * Format a UK phone number.
   */
  _formatPhone(phone) {
    if (!phone) return '';
    let trimmed = String(phone).trim();
    // Strip all non-digit except leading +
    const digits = trimmed.replace(/[^\d+]/g, '');
    if (!digits) return '';
    // Convert +44 to 0
    if (digits.startsWith('+44')) {
      return '0' + digits.slice(3);
    }
    if (digits.startsWith('44') && digits.length > 10) {
      return '0' + digits.slice(2);
    }
    // If it starts with 0, it's already local format
    if (digits.startsWith('0')) return digits;
    return trimmed;
  }

  /**
   * Extract the profile slug from a Which? Trusted Traders URL.
   */
  _extractSlug(url) {
    if (!url) return '';
    const match = url.match(/\/businesses\/([^/]+)\/?$/);
    return match ? match[1] : '';
  }

  /**
   * Map a JSON-LD business object to a normalized lead.
   */
  _mapBusiness(biz, tradeSlug, postcode) {
    const name = (biz.name || '').trim();
    const phone = this._formatPhone(biz.telephone);
    const email = (biz.email || '').trim().toLowerCase();
    const profileUrl = (biz.url || '').trim();
    const slug = this._extractSlug(profileUrl);

    // Extract location from address if available, otherwise from geo
    let city = '';
    let zip = postcode || '';
    let streetAddress = '';
    if (biz.address) {
      city = (biz.address.addressLocality || '').trim();
      zip = zip || (biz.address.postalCode || '').trim();
      streetAddress = (biz.address.streetAddress || '').trim();
    }

    // Rating info
    let rating = '';
    let reviewCount = 0;
    if (biz.aggregateRating) {
      rating = String(biz.aggregateRating.ratingValue || '');
      reviewCount = biz.aggregateRating.reviewCount || 0;
    }

    // Human-readable trade name from slug
    const tradeName = tradeSlug
      .replace(/-/g, ' ')
      .replace(/\b\w/g, c => c.toUpperCase());

    return {
      first_name: '',
      last_name: '',
      firm_name: name,
      city: city,
      state: 'UK',
      zip: zip,
      address: streetAddress,
      phone: phone,
      email: email,
      website: '', // Will be enriched from profile page if needed
      bar_number: slug,
      bar_status: 'Which? Endorsed',
      source: 'which_traders',
      practice_area: tradeName,
      profile_url: profileUrl,
      rating: rating,
      review_count: String(reviewCount),
      about: (biz.description || '').trim().substring(0, 500),
    };
  }

  /**
   * Fetch a profile page and extract the website URL.
   */
  async _fetchProfileWebsite(profileUrl, rateLimiter) {
    if (!profileUrl) return '';
    try {
      await rateLimiter.wait();
      const resp = await this.httpGet(profileUrl, rateLimiter);
      if (resp.statusCode !== 200) return '';
      const $ = cheerio.load(resp.body);
      // Website is in <a class="url" ...>
      const websiteLink = $('a.url[data-ga-_action="click_trader_website"]').attr('href') ||
                          $('a.url').attr('href') || '';
      if (websiteLink && !this.isExcludedDomain(websiteLink)) {
        return websiteLink.trim();
      }
      return '';
    } catch (e) {
      return '';
    }
  }

  /**
   * Async generator yielding trader leads from Which? Trusted Traders.
   *
   * Strategy:
   * 1. Determine which trades to scrape (from practiceArea or DEFAULT_TRADES)
   * 2. For each trade, iterate through cities
   * 3. For each city, paginate through listing pages
   * 4. Extract JSON-LD data from each page
   * 5. Optionally fetch profile pages for website URLs
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;
    const seen = new Set(); // Dedup by profile slug

    // Determine trades to scrape
    let tradeSlugs;
    const resolvedSlug = this._resolveTradeSlug(practiceArea);
    if (resolvedSlug) {
      tradeSlugs = [resolvedSlug];
    } else if (practiceArea) {
      // If practiceArea was given but couldn't resolve, try using it as a slug directly
      tradeSlugs = [practiceArea.toLowerCase().replace(/\s+/g, '-')];
    } else {
      tradeSlugs = [...DEFAULT_TRADES];
    }

    // Determine cities
    const cities = options.city
      ? [options.city.toLowerCase().replace(/\s+/g, '-')]
      : [...this.defaultCities];
    const citiesToSearch = options.maxCities
      ? cities.slice(0, options.maxCities)
      : cities;

    log.scrape(`Which? Trusted Traders: trades=${tradeSlugs.join(',')} cities=${citiesToSearch.length}`);

    let totalYielded = 0;

    for (const tradeSlug of tradeSlugs) {
      for (let ci = 0; ci < citiesToSearch.length; ci++) {
        const city = citiesToSearch[ci];
        yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
        log.scrape(`Searching ${tradeSlug} in ${city}`);

        let page = 1;
        let pagesFetched = 0;
        let totalResults = 0;
        let cityYielded = 0;

        while (true) {
          if (pagesFetched >= maxPages) {
            log.info(`Reached max pages limit (${maxPages}) for ${city}`);
            break;
          }

          const url = this._buildListingUrl(tradeSlug, city, page);
          log.info(`Page ${page} — ${url}`);

          let response;
          try {
            await rateLimiter.wait();
            response = await this.httpGet(url, rateLimiter);
          } catch (err) {
            log.error(`Request failed for ${city}: ${err.message}`);
            const shouldRetry = await rateLimiter.handleBlock(0);
            if (shouldRetry) continue;
            break;
          }

          // Handle errors
          if (response.statusCode === 429 || response.statusCode === 403) {
            log.warn(`Got ${response.statusCode} from Which? Trusted Traders`);
            const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
            if (shouldRetry) continue;
            break;
          }

          if (response.statusCode === 404) {
            log.info(`No ${tradeSlug} listings found for ${city} (404)`);
            break;
          }

          if (response.statusCode === 500) {
            log.warn(`Server error (500) for ${tradeSlug} in ${city} — skipping`);
            break;
          }

          if (response.statusCode !== 200) {
            log.error(`Unexpected status ${response.statusCode} for ${city}`);
            break;
          }

          rateLimiter.resetBackoff();

          const $ = cheerio.load(response.body);

          // Get total on first page
          if (page === 1) {
            totalResults = this._extractTotal($);
            if (totalResults === 0) {
              log.info(`No results for ${tradeSlug} in ${city}`);
              break;
            }
            const totalPages = Math.ceil(totalResults / PAGE_SIZE);
            log.success(`Found ${totalResults} ${tradeSlug} in ${city} (${totalPages} pages)`);
          }

          // Extract JSON-LD businesses
          const businesses = this._extractJsonLd($);
          const postcodes = this._extractPostcodes($);

          if (businesses.length === 0) {
            log.info(`No JSON-LD data on page ${page} for ${city}`);
            break;
          }

          for (let i = 0; i < businesses.length; i++) {
            const biz = businesses[i];
            const slug = this._extractSlug(biz.url);

            // Dedup by slug
            if (slug && seen.has(slug)) continue;
            if (slug) seen.add(slug);

            const postcode = postcodes[i] || '';
            const lead = this._mapBusiness(biz, tradeSlug, postcode);

            // Skip if no firm name
            if (!lead.firm_name) continue;

            yield this.transformResult(lead, practiceArea || tradeSlug);
            cityYielded++;
            totalYielded++;
          }

          log.info(`Page ${page}: ${businesses.length} businesses (${cityYielded} from this city)`);

          // Check if there's a next page
          if (!this._hasNextPage($)) {
            log.success(`Completed ${tradeSlug} in ${city}: ${cityYielded} leads`);
            break;
          }

          // Check if we've exhausted results
          const totalPages = Math.ceil(totalResults / PAGE_SIZE);
          if (page >= totalPages) {
            log.success(`Completed all ${totalPages} pages for ${tradeSlug} in ${city}`);
            break;
          }

          page++;
          pagesFetched++;
        }
      }
    }

    log.success(`Which? Trusted Traders scrape complete — ${totalYielded} total leads (${seen.size} unique businesses)`);
  }
}

module.exports = new WhichTradersScraper();
