/**
 * Yelp Home Services Scraper
 *
 * Source: https://www.yelp.com + Yelp Fusion API v3
 * Records: Millions of home service business listings with phone numbers, websites, ratings
 *
 * Two approaches:
 *   A. Yelp Fusion API (preferred) — requires YELP_API_KEY env var
 *      - GET https://api.yelp.com/v3/businesses/search
 *      - 50 results per request, up to 1000 per search (offset limit)
 *      - Returns: name, phone, address, rating, categories, url, coordinates
 *      - Rate limits: 300 calls/day on Starter plan, QPS limits
 *
 *   B. HTML scraping (fallback) — Yelp returns 403 for direct HTML requests
 *      - Yelp aggressively blocks scrapers (Cloudflare, bot detection)
 *      - Falls back to placeholder yield if no API key is set
 *
 * Yelp category aliases (home services):
 *   roofing, painters, plumbing, hvac, electricians, contractors, landscaping
 *
 * Pagination: limit=50, offset=0..950 (max offset 1000)
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');
const https = require('https');

// Yelp Fusion API category aliases for home services
const CATEGORY_ALIASES = {
  'roofing': 'roofing',
  'painting': 'painters',
  'painters': 'painters',
  'plumbing': 'plumbing',
  'plumber': 'plumbing',
  'hvac': 'hvac',
  'heating': 'hvac',
  'cooling': 'hvac',
  'air conditioning': 'hvac',
  'electrical': 'electricians',
  'electrician': 'electricians',
  'general contractor': 'contractors',
  'general': 'contractors',
  'contractor': 'contractors',
  'contractors': 'contractors',
  'landscaping': 'landscaping',
  'landscape': 'landscaping',
  'handyman': 'handyman',
  'fencing': 'fences',
  'garage doors': 'garagedoorservices',
  'windows': 'windowsinstallation',
  'siding': 'siding',
  'flooring': 'flooring',
  'insulation': 'insulationinstallation',
  'masonry': 'masonry_concrete',
  'demolition': 'demolitionservices',
  'tree services': 'treeservices',
  'pressure washing': 'pressurewashers',
  'paving': 'paving',
};

// Default cities to search — major US metros with high Yelp density
const DEFAULT_CITIES = [
  'Miami, FL',
  'Houston, TX',
  'Los Angeles, CA',
  'New York, NY',
  'Dallas, TX',
  'Phoenix, AZ',
  'Atlanta, GA',
  'Denver, CO',
  'Chicago, IL',
  'Seattle, WA',
  'Tampa, FL',
  'Charlotte, NC',
  'San Diego, CA',
  'Las Vegas, NV',
  'Portland, OR',
  'Nashville, TN',
  'Austin, TX',
  'San Antonio, TX',
  'Columbus, OH',
  'Philadelphia, PA',
];

// Max results per Yelp API request
const API_PAGE_SIZE = 50;

// Max offset allowed by Yelp Fusion API
const MAX_API_OFFSET = 1000;


class YelpScraper extends BaseScraper {
  constructor() {
    super({
      name: 'yelp_contractors',
      stateCode: 'YELP',
      baseUrl: 'https://api.yelp.com',
      pageSize: API_PAGE_SIZE,
      practiceAreaCodes: CATEGORY_ALIASES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('yelp: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('yelp: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('yelp: extractResultCount() not used — search() overridden'); }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'yelp_contractors';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Make an authenticated request to the Yelp Fusion API.
   *
   * @param {string} endpoint - API path (e.g., '/v3/businesses/search')
   * @param {object} params - Query parameters
   * @param {string} apiKey - Yelp API key
   * @returns {Promise<object>} Parsed JSON response
   */
  _apiRequest(endpoint, params, apiKey) {
    return new Promise((resolve, reject) => {
      const query = new URLSearchParams(params).toString();
      const url = `https://api.yelp.com${endpoint}?${query}`;

      const options = {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Accept': 'application/json',
          'User-Agent': 'MortarLeadScraper/1.0',
        },
        timeout: 15000,
      };

      const req = https.get(url, options, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          if (res.statusCode === 200) {
            try {
              resolve({ statusCode: 200, data: JSON.parse(data) });
            } catch (err) {
              reject(new Error(`Yelp API: Failed to parse JSON — ${err.message}`));
            }
          } else if (res.statusCode === 429) {
            // Rate limited — extract headers for logging
            const remaining = res.headers['ratelimit-remaining'] || '?';
            const resetTime = res.headers['ratelimit-resettime'] || '?';
            resolve({ statusCode: 429, remaining, resetTime, body: data });
          } else {
            resolve({ statusCode: res.statusCode, body: data });
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Yelp API request timed out')); });
    });
  }

  /**
   * Convert a Yelp API business object to our lead format.
   *
   * @param {object} biz - Yelp business object from API response
   * @param {string} practiceArea - The searched practice area
   * @returns {object} Normalized lead object
   */
  _bizToLead(biz, practiceArea) {
    const loc = biz.location || {};
    const categories = (biz.categories || []).map(c => c.title).join(', ');

    return {
      first_name: '',
      last_name: '',
      firm_name: biz.name || '',
      address: (loc.address1 || ''),
      city: loc.city || '',
      state: loc.state || '',
      zip: loc.zip_code || '',
      phone: biz.phone || biz.display_phone || '',
      email: '',
      website: '', // Yelp API does not return business websites directly
      bar_number: '',
      bar_status: biz.rating ? `${biz.rating} stars (${biz.review_count || 0} reviews)` : '',
      profile_url: biz.url || '',
      source: 'yelp_contractors',
      practice_area: practiceArea || categories || '',
      // Extra fields for enrichment/filtering
      yelp_id: biz.id || '',
      yelp_rating: biz.rating || 0,
      yelp_review_count: biz.review_count || 0,
      yelp_price: biz.price || '',
      yelp_categories: categories,
    };
  }

  /**
   * Search via Yelp Fusion API.
   *
   * @param {string} category - Yelp category alias (e.g., 'roofing')
   * @param {string} location - Location string (e.g., 'Miami, FL')
   * @param {string} apiKey - Yelp Fusion API key
   * @param {number} maxOffset - Max offset to paginate to
   * @param {RateLimiter} rateLimiter - Rate limiter
   * @param {string} practiceArea - Original practice area name
   * @yields {object} Lead objects
   */
  async *_searchViaApi(category, location, apiKey, maxOffset, rateLimiter, practiceArea) {
    let offset = 0;
    let totalResults = 0;
    let pageNum = 0;

    while (offset < maxOffset) {
      const params = {
        categories: category,
        location: location,
        limit: API_PAGE_SIZE,
        offset: offset,
        sort_by: 'review_count', // Most reviewed first for best data quality
      };

      log.info(`[Yelp API] offset=${offset}, location=${location}, category=${category}`);

      let response;
      try {
        await rateLimiter.wait();
        response = await this._apiRequest('/v3/businesses/search', params, apiKey);
      } catch (err) {
        log.error(`[Yelp API] Request failed: ${err.message}`);
        break;
      }

      // Handle rate limiting
      if (response.statusCode === 429) {
        log.warn(`[Yelp API] Rate limited — remaining: ${response.remaining}, resets: ${response.resetTime}`);
        // Wait 60 seconds and retry once
        log.info('[Yelp API] Waiting 60s before retry...');
        await new Promise(r => setTimeout(r, 60000));
        try {
          response = await this._apiRequest('/v3/businesses/search', params, apiKey);
        } catch (err) {
          log.error(`[Yelp API] Retry failed: ${err.message}`);
          break;
        }
        if (response.statusCode === 429) {
          log.error('[Yelp API] Still rate limited after retry — stopping');
          break;
        }
      }

      if (response.statusCode === 400) {
        log.warn(`[Yelp API] Bad request for location "${location}" — ${response.body}`);
        break;
      }

      if (response.statusCode === 401) {
        log.error('[Yelp API] Unauthorized — check YELP_API_KEY');
        break;
      }

      if (response.statusCode !== 200) {
        log.error(`[Yelp API] Unexpected status ${response.statusCode}`);
        break;
      }

      const data = response.data;
      const businesses = data.businesses || [];

      if (pageNum === 0) {
        totalResults = data.total || 0;
        const effectiveTotal = Math.min(totalResults, MAX_API_OFFSET);
        const totalPages = Math.ceil(effectiveTotal / API_PAGE_SIZE);
        log.success(`[Yelp API] ${totalResults.toLocaleString()} total results for "${category}" in ${location} (${totalPages} pages accessible)`);
      }

      if (businesses.length === 0) {
        log.info(`[Yelp API] No more results at offset ${offset}`);
        break;
      }

      for (const biz of businesses) {
        if (!biz.name) continue;
        yield this._bizToLead(biz, practiceArea);
      }

      offset += API_PAGE_SIZE;
      pageNum++;

      // Respect Yelp's offset ceiling
      if (offset >= MAX_API_OFFSET) {
        log.info(`[Yelp API] Reached max offset (${MAX_API_OFFSET}) for ${location}`);
        break;
      }

      // Don't exceed total results
      if (offset >= totalResults) {
        break;
      }
    }
  }

  /**
   * Main search generator.
   *
   * Strategy:
   *   1. If YELP_API_KEY is set, use the Fusion API (reliable, structured data)
   *   2. If no API key, yield a placeholder/captcha signal
   *
   * The API approach is strongly preferred — Yelp blocks HTML scraping with 403/CAPTCHA.
   *
   * @param {string} practiceArea - Category name (e.g., 'roofing', 'plumbing')
   * @param {object} options - Search options
   * @param {string} options.city - Specific city to search (e.g., 'Miami, FL')
   * @param {number} options.maxPages - Limit pages per city
   * @param {number} options.maxCities - Limit number of cities
   */
  async *search(practiceArea, options = {}) {
    const apiKey = process.env.YELP_API_KEY;
    const rateLimiter = new RateLimiter();

    // Resolve the Yelp category alias
    const practiceKey = (practiceArea || 'roofing').toLowerCase().trim();
    const categoryAlias = CATEGORY_ALIASES[practiceKey] || practiceKey;

    log.scrape(`[Yelp] Starting search: category=${categoryAlias}, practiceArea=${practiceArea || 'roofing'}`);

    // --- Check for API key ---
    if (!apiKey) {
      log.warn('[Yelp] No YELP_API_KEY env var set — Yelp blocks HTML scraping with 403');
      log.info('[Yelp] Get a free API key at https://www.yelp.com/developers/v3/manage_app');
      log.info('[Yelp] Then set YELP_API_KEY=your_key_here in your environment');
      yield { _captcha: true, city: 'all', reason: 'No YELP_API_KEY — get one free at yelp.com/developers' };
      return;
    }

    // --- API-based search ---

    // Determine cities to search
    let citiesToSearch = [...DEFAULT_CITIES];
    if (options.city) {
      citiesToSearch = [options.city];
    }
    if (options.maxCities) {
      citiesToSearch = citiesToSearch.slice(0, options.maxCities);
    }

    // Calculate max offset per city based on maxPages
    const maxPagesPerCity = options.maxPages || 20; // Default: 20 pages (1000 results)
    const maxOffset = Math.min(maxPagesPerCity * API_PAGE_SIZE, MAX_API_OFFSET);

    const seenIds = new Set();
    let totalYielded = 0;

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const location = citiesToSearch[ci];

      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
      log.scrape(`[Yelp] Searching "${categoryAlias}" in ${location}`);

      let cityYielded = 0;

      for await (const lead of this._searchViaApi(categoryAlias, location, apiKey, maxOffset, rateLimiter, practiceArea)) {
        // Dedup by Yelp ID across cities
        if (lead.yelp_id && seenIds.has(lead.yelp_id)) continue;
        if (lead.yelp_id) seenIds.add(lead.yelp_id);

        yield this.transformResult(lead, practiceArea || categoryAlias);
        cityYielded++;
        totalYielded++;
      }

      log.info(`[Yelp] ${location}: ${cityYielded} leads`);

      // Polite delay between cities (2-4 seconds)
      if (ci < citiesToSearch.length - 1) {
        await new Promise(r => setTimeout(r, 2000 + Math.random() * 2000));
      }
    }

    log.success(`[Yelp] Finished — ${totalYielded} total leads from ${citiesToSearch.length} cities`);
  }
}

module.exports = new YelpScraper();
