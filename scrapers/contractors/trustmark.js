/**
 * TrustMark UK — Government-endorsed quality scheme for tradespeople
 *
 * Source: https://www.trustmark.org.uk
 * API Docs: https://github.com/trustmark-2005-ltd/TrustMark-BusinessSearch-Integration
 * Platform: REST JSON API (requires x-api-key header)
 *
 * Endpoint: GET https://api.trustmark.org.uk/business-profiles
 * Auth: x-api-key header (env var TRUSTMARK_API_KEY)
 * Rate limit: 50,000 requests/month
 *
 * Parameters:
 *   location        — postcode, town name, or local authority (geocoded server-side)
 *   tradeCodes      — numeric trade code(s), OR logic when multiple
 *   pageIndex       — zero-indexed page number (default 0)
 *   pageSize        — results per page (max 100, default 10)
 *   tradingStandardsApproved — boolean filter
 *   nationalBusinesses — boolean filter
 *   countries       — England, Northern Ireland, Scotland, Wales
 *   regions         — East Midlands, London, North West, etc.
 *
 * Response shape:
 *   { results: [ { id, companyName, tmln, address: { town, county, postcode },
 *     trades: [ { tradeCode, name, description } ], phoneNumbers: [], websites: [],
 *     aboutInformation, tradingStandardsApproved, nationalBusiness,
 *     communicationsPlatformLink } ], totalItemsCount }
 *
 * ~18,000 registered businesses covering 156+ trade types across the UK.
 * Phone and website are available; email is NOT — but websites enable
 * email derivation via the enrichment pipeline.
 */

const https = require('https');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const API_HOST = 'api.trustmark.org.uk';
const API_PATH = '/business-profiles';
const PAGE_SIZE = 100; // max allowed by API

/**
 * Known TrustMark numeric trade codes mapped from friendly practice area names.
 * These codes are defined in the TrustMark Data Dictionary (WorkType sheet).
 * The API also supports location-only search (no tradeCodes param) which
 * returns all trades in that area.
 *
 * Common codes discovered from API documentation examples and public references:
 *   24 = Plumbing
 *   91 = Loft Insulation
 *   93 = Cavity Wall Insulation
 *
 * Since the full code list requires API access to enumerate, we support two modes:
 *   1. If a practiceArea matches a known code, filter by that tradeCode
 *   2. Otherwise, search by location only and post-filter results by trade name
 */
const TRADE_CODE_MAP = {
  // Codes confirmed from API docs / public references
  'plumbing': 24,
  'loft insulation': 91,
  'cavity wall insulation': 93,
};

/**
 * Trade name patterns for post-filtering when we don't have the numeric code.
 * These are matched case-insensitively against the trade name strings returned
 * by the API in the trades[].name field.
 */
const TRADE_NAME_PATTERNS = {
  'plumbing': ['plumbing', 'plumber'],
  'electrical': ['electri'],
  'roofing': ['roof'],
  'painting': ['paint', 'decorat'],
  'general builder': ['general build', 'general construct'],
  'landscaping': ['landscap', 'garden'],
  'heating': ['heat', 'boiler', 'gas'],
  'kitchen': ['kitchen'],
  'bathroom': ['bathroom'],
  'insulation': ['insulat'],
  'windows': ['window', 'glazing', 'glass'],
  'carpentry': ['carpent', 'joiner', 'joinery'],
  'plastering': ['plaster', 'render'],
  'tiling': ['til'],
  'flooring': ['floor'],
  'damp proofing': ['damp'],
  'drainage': ['drain'],
  'solar': ['solar', 'photovoltaic', 'pv'],
  'extensions': ['extension', 'loft conver'],
  'fencing': ['fenc'],
  'guttering': ['gutter', 'fascia', 'soffit'],
  'locksmith': ['locksmith', 'lock'],
  'pest control': ['pest'],
  'scaffolding': ['scaffold'],
  'demolition': ['demolit'],
  'air conditioning': ['air condition', 'hvac'],
  'fire protection': ['fire'],
  'security': ['security', 'alarm', 'cctv'],
};

class TrustMarkScraper extends BaseScraper {
  constructor() {
    super({
      name: 'trustmark_contractors',
      stateCode: 'TRUSTMARK',
      baseUrl: `https://${API_HOST}`,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'plumbing': 'Plumbing',
        'electrical': 'Electrical',
        'roofing': 'Roofing',
        'painting': 'Painting and Decorating',
        'general builder': 'General Building',
        'landscaping': 'Landscaping',
        'heating': 'Heating',
        'kitchen': 'Kitchen Fitting',
        'bathroom': 'Bathroom Fitting',
        'insulation': 'Insulation',
        'windows': 'Windows and Glazing',
        'carpentry': 'Carpentry and Joinery',
        'plastering': 'Plastering',
        'tiling': 'Tiling',
        'flooring': 'Flooring',
        'damp proofing': 'Damp Proofing',
        'drainage': 'Drainage',
        'solar': 'Solar and Renewables',
        'extensions': 'Extensions',
        'fencing': 'Fencing',
        'guttering': 'Guttering and Fascias',
        'locksmith': 'Locksmith',
        'pest control': 'Pest Control',
        'scaffolding': 'Scaffolding',
        'demolition': 'Demolition',
        'air conditioning': 'Air Conditioning',
        'fire protection': 'Fire Protection',
        'security': 'Security Systems',
      },
      defaultCities: [
        'London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow',
        'Liverpool', 'Edinburgh', 'Bristol', 'Sheffield', 'Newcastle',
      ],
    });
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
   * HTTPS GET against the TrustMark API.
   * Requires TRUSTMARK_API_KEY environment variable.
   */
  _apiGet(queryString, rateLimiter) {
    return new Promise((resolve, reject) => {
      const apiKey = process.env.TRUSTMARK_API_KEY;
      if (!apiKey) {
        return reject(new Error(
          'TRUSTMARK_API_KEY env var is required. ' +
          'Apply at https://github.com/trustmark-2005-ltd/TrustMark-BusinessSearch-Integration ' +
          '(contact data@trustmark.org.uk for a Data Sharing Agreement)'
        ));
      }

      const fullPath = queryString ? `${API_PATH}?${queryString}` : API_PATH;

      const req = https.request({
        hostname: API_HOST,
        path: fullPath,
        method: 'GET',
        headers: {
          'x-api-key': apiKey,
          'User-Agent': rateLimiter.getUserAgent(),
          'Accept': 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 30000,
      }, (res) => {
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('TrustMark API request timed out')); });
      req.end();
    });
  }

  /**
   * Build the API query string for a given location + optional trade filter.
   *
   * @param {object} params
   * @param {string} params.location — city/town/postcode
   * @param {number|null} params.tradeCode — numeric trade code (if known)
   * @param {number} params.pageIndex — zero-indexed page
   * @param {number} params.pageSize — results per page (max 100)
   * @returns {string} URL query string
   */
  _buildQueryString({ location, tradeCode, pageIndex, pageSize }) {
    const params = new URLSearchParams();

    if (location) {
      params.set('location', location);
    }

    if (tradeCode) {
      params.set('tradeCodes', String(tradeCode));
    }

    params.set('pageIndex', String(pageIndex));
    params.set('pageSize', String(pageSize));

    return params.toString();
  }

  /**
   * Check if a business's trades match the requested practice area.
   * Used for post-filtering when we search by location only.
   *
   * @param {object[]} trades — array of { tradeCode, name, description }
   * @param {string} practiceArea — user-supplied practice area string
   * @returns {boolean}
   */
  _matchesTrade(trades, practiceArea) {
    if (!practiceArea || !trades || trades.length === 0) return true;

    const key = practiceArea.toLowerCase().trim();
    const patterns = TRADE_NAME_PATTERNS[key];

    if (!patterns) {
      // No pattern defined — try a simple substring match on trade names
      return trades.some(t => {
        const name = (t.name || '').toLowerCase();
        const desc = (t.description || '').toLowerCase();
        return name.includes(key) || desc.includes(key);
      });
    }

    return trades.some(t => {
      const name = (t.name || '').toLowerCase();
      const desc = (t.description || '').toLowerCase();
      return patterns.some(p => name.includes(p) || desc.includes(p));
    });
  }

  /**
   * Extract the best phone number from the phoneNumbers array.
   * Prefers landline-looking numbers (01/02/03) over mobile (07).
   */
  _pickPhone(phoneNumbers) {
    if (!phoneNumbers || phoneNumbers.length === 0) return '';
    if (phoneNumbers.length === 1) return phoneNumbers[0].trim();

    // Prefer landline
    const landline = phoneNumbers.find(p => /^0[123]/.test(p.trim()));
    if (landline) return landline.trim();

    return phoneNumbers[0].trim();
  }

  /**
   * Extract the best website from the websites array.
   * Filters out social media and other non-business sites.
   */
  _pickWebsite(websites) {
    if (!websites || websites.length === 0) return '';

    for (const url of websites) {
      const trimmed = (url || '').trim();
      if (!trimmed) continue;

      // Skip social media / non-business URLs
      if (this.isExcludedDomain(trimmed)) continue;

      // Ensure it has a protocol
      if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://')) {
        return `https://${trimmed}`;
      }
      return trimmed;
    }

    // Fallback to first URL if all were excluded
    const first = (websites[0] || '').trim();
    if (!first) return '';
    if (!first.startsWith('http://') && !first.startsWith('https://')) {
      return `https://${first}`;
    }
    return first;
  }

  /**
   * Format a UK phone number. Normalizes common formats.
   */
  _formatPhone(phone) {
    if (!phone) return '';
    const trimmed = phone.trim();
    // Already formatted nicely — return as-is
    if (/^0\d{2,4}\s?\d{3,4}\s?\d{3,4}$/.test(trimmed)) return trimmed;
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
    return trimmed;
  }

  /**
   * Build a comma-separated string of trade names for the practice_area field.
   */
  _tradeNames(trades) {
    if (!trades || trades.length === 0) return '';
    return trades.map(t => t.name || '').filter(Boolean).join(', ');
  }

  /**
   * Map a TrustMark API result to a normalized lead object.
   *
   * @param {object} biz — API result object
   * @param {string} practiceArea — user-supplied practice area
   * @returns {object} lead
   */
  _mapBusiness(biz, practiceArea) {
    const companyName = (biz.companyName || '').trim();
    const tmln = (biz.tmln || '').trim();
    const town = (biz.address && biz.address.town) ? biz.address.town.trim() : '';
    const county = (biz.address && biz.address.county) ? biz.address.county.trim() : '';
    const postcode = (biz.address && biz.address.postcode) ? biz.address.postcode.trim() : '';

    const phone = this._formatPhone(this._pickPhone(biz.phoneNumbers));
    const website = this._pickWebsite(biz.websites);
    const trades = this._tradeNames(biz.trades);
    const approved = biz.tradingStandardsApproved ? 'Trading Standards Approved' : 'TrustMark Registered';

    return {
      first_name: '',
      last_name: '',
      firm_name: companyName,
      city: town,
      state: 'UK',
      county,
      zip: postcode,
      phone,
      website,
      email: '', // Not available from API — derived via enrichment pipeline
      bar_number: tmln,
      bar_status: approved,
      source: 'trustmark_contractors',
      practice_area: practiceArea || trades,
      about: (biz.aboutInformation || '').trim(),
      // Extra fields for enrichment/display
      all_phones: (biz.phoneNumbers || []).map(p => this._formatPhone(p)).filter(Boolean).join('; '),
      all_websites: (biz.websites || []).join('; '),
      all_trades: trades,
      national_business: biz.nationalBusiness ? 'Yes' : 'No',
      trustmark_id: (biz.id || '').trim(),
    };
  }

  /**
   * Async generator that yields contractor leads from the TrustMark API.
   *
   * Strategy:
   * 1. If the practiceArea maps to a known numeric tradeCode, use it for filtering
   * 2. Otherwise, search by location only and post-filter by trade name matching
   * 3. Iterate through defaultCities (or options.city)
   * 4. Paginate via pageIndex (zero-indexed) until totalItemsCount exhausted
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const maxPages = options.maxPages || Infinity;

    // Check for API key early
    if (!process.env.TRUSTMARK_API_KEY) {
      log.error('TRUSTMARK_API_KEY env var is required');
      log.info('Apply at: https://github.com/trustmark-2005-ltd/TrustMark-BusinessSearch-Integration');
      log.info('Contact: data@trustmark.org.uk for a Data Sharing Agreement');
      yield {
        _captcha: true,
        city: 'all',
        reason: 'TRUSTMARK_API_KEY env var not set — API key required (contact data@trustmark.org.uk)',
      };
      return;
    }

    // Resolve trade code (numeric) if we have one
    const practiceKey = (practiceArea || '').toLowerCase().trim();
    const tradeCode = TRADE_CODE_MAP[practiceKey] || null;
    const needsPostFilter = practiceArea && !tradeCode;

    if (practiceArea && !tradeCode) {
      log.info(`No numeric tradeCode for "${practiceArea}" — will search all trades and post-filter by name`);
    }

    // Determine cities to search
    const cities = options.city
      ? [options.city]
      : this.defaultCities;
    const citiesToSearch = options.maxCities
      ? cities.slice(0, options.maxCities)
      : cities;

    log.scrape(`TrustMark search: trade=${practiceArea || 'all'}${tradeCode ? ` (code ${tradeCode})` : ''}, cities=${citiesToSearch.length}`);

    // Track seen TMLNs to avoid duplicates across cities (businesses serve multiple areas)
    const seenTmlns = new Set();

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const city = citiesToSearch[ci];
      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
      log.scrape(`Searching TrustMark businesses near ${city}${practiceArea ? ` (${practiceArea})` : ''}`);

      let pageIndex = 0;
      let pagesFetched = 0;
      let totalItems = null;
      let cityYielded = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${city}`);
          break;
        }

        const qs = this._buildQueryString({
          location: city,
          tradeCode,
          pageIndex,
          pageSize: PAGE_SIZE,
        });

        log.info(`Page ${pageIndex + 1} (index ${pageIndex}) — ${city}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this._apiGet(qs, rateLimiter);
        } catch (err) {
          log.error(`TrustMark API request failed: ${err.message}`);
          if (err.message.includes('TRUSTMARK_API_KEY')) {
            yield { _captcha: true, city: 'all', reason: err.message };
            return;
          }
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429) {
          log.warn('TrustMark API rate limit hit (429)');
          const shouldRetry = await rateLimiter.handleBlock(429);
          if (shouldRetry) continue;
          break;
        }

        // Handle auth errors
        if (response.statusCode === 401 || response.statusCode === 403) {
          log.error(`TrustMark API auth error (${response.statusCode}) — check TRUSTMARK_API_KEY`);
          yield {
            _captcha: true,
            city: 'all',
            reason: `API returned ${response.statusCode} — invalid or expired TRUSTMARK_API_KEY`,
          };
          return;
        }

        if (response.statusCode === 400) {
          log.warn(`TrustMark API bad request (400) for ${city}`);
          try {
            const errBody = JSON.parse(response.body);
            log.warn(`Validation errors: ${JSON.stringify(errBody.errors || errBody)}`);
          } catch (_) { /* ignore parse errors */ }
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`TrustMark API returned ${response.statusCode} for ${city}`);
          break;
        }

        rateLimiter.resetBackoff();

        let data;
        try {
          data = JSON.parse(response.body);
        } catch (err) {
          log.error(`Failed to parse TrustMark API response: ${err.message}`);
          break;
        }

        // Validate response shape
        if (!data || !Array.isArray(data.results)) {
          log.error(`Unexpected TrustMark API response shape for ${city}`);
          break;
        }

        // Log total on first page
        if (pageIndex === 0) {
          totalItems = data.totalItemsCount || 0;
          const totalPages = Math.ceil(totalItems / PAGE_SIZE);
          log.success(`Found ${totalItems.toLocaleString()} businesses near ${city} (${totalPages} pages)`);
          if (totalItems === 0) break;
        }

        const results = data.results;
        if (results.length === 0) {
          log.info(`No more results for ${city} at page ${pageIndex + 1}`);
          break;
        }

        for (const biz of results) {
          // Dedup by TMLN across cities
          const tmln = (biz.tmln || '').trim();
          if (tmln && seenTmlns.has(tmln)) continue;
          if (tmln) seenTmlns.add(tmln);

          // Post-filter by trade name if no numeric code was available
          if (needsPostFilter && !this._matchesTrade(biz.trades, practiceArea)) {
            continue;
          }

          const lead = this._mapBusiness(biz, practiceArea);
          // Skip businesses without a company name
          if (!lead.firm_name) continue;

          yield this.transformResult(lead, practiceArea);
          cityYielded++;
        }

        log.info(`Got ${results.length} results for ${city} (${cityYielded} leads from this city)`);

        // Check if we've exhausted results
        const totalFetched = (pageIndex + 1) * PAGE_SIZE;
        if (totalItems !== null && totalFetched >= totalItems) {
          log.success(`Completed ${city}: ${cityYielded} leads (${totalItems} total businesses)`);
          break;
        }

        // Fewer than a full page means we're done
        if (results.length < PAGE_SIZE) {
          log.success(`Completed ${city}: ${cityYielded} leads`);
          break;
        }

        pageIndex++;
        pagesFetched++;
      }
    }

    log.success(`TrustMark scrape complete — ${seenTmlns.size} unique businesses found`);
  }
}

module.exports = new TrustMarkScraper();
