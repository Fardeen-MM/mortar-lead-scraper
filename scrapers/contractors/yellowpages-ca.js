/**
 * YellowPages.ca Scraper
 *
 * Source: https://www.yellowpages.ca/search/si/{page}/{category}/{city}+{province}
 * Records: Canada's largest business directory — dentists, medical spas, contractors, etc.
 *
 * Fields per listing (SERP):
 *   - Business name (h3.listing__name, itemprop="name")
 *   - Phone (data-phone attribute on .mlr__item--phone)
 *   - Website (redirect URL in .mlr__item--website a[href] — extract real URL from ?redirect= param)
 *   - Full address (itemprop="streetAddress", "addressLocality", "addressRegion", "postalCode")
 *   - Categories (.listing__headings a)
 *   - Profile URL (.listing__name--link href)
 *
 * Pagination: /search/si/{page}/ — 35 results per page, up to ~100+ pages
 * Result count: .resultCount text "(N Result(s))"
 * Next page link: a.pageButton with text "Next"
 *
 * Profile pages: JSON-LD with schema.org LocalBusiness (telephone, address, openingHours)
 * Emails: NOT shown on search results or profile pages (confirmed via testing)
 *
 * Architecture: Extends BaseScraper, overrides search() directly.
 *   - Iterates cities x categories
 *   - Parses HTML with Cheerio
 *   - Extracts real website URL from YP redirect wrapper
 */

const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');
const cheerio = require('cheerio');

// YellowPages.ca heading strings — maps friendly name to URL-safe category
const PRACTICE_AREA_CODES = {
  // Healthcare / Aesthetic
  'dentist': 'Dentists',
  'dentists': 'Dentists',
  'medical spa': 'Medical+Spas',
  'med spa': 'Medical+Spas',
  'medspa': 'Medical+Spas',
  'chiropractor': 'Chiropractors',
  'chiropractors': 'Chiropractors',
  'dermatologist': 'Dermatologists',
  'dermatologists': 'Dermatologists',
  'optometrist': 'Optometrists',
  'optometrists': 'Optometrists',
  'family doctor': 'Family+Physicians',
  'physician': 'Family+Physicians',
  'physiotherapy': 'Physiotherapists',
  'massage therapy': 'Massage+Therapists',
  'veterinarian': 'Veterinarians',
  'pharmacy': 'Pharmacies',
  'psychologist': 'Psychologists',
  // Home Services / Contractors
  'roofing': 'Roofing+Contractors',
  'roofer': 'Roofing+Contractors',
  'plumbing': 'Plumbing+Contractors',
  'plumber': 'Plumbing+Contractors',
  'painting': 'Painting+Contractors',
  'painter': 'Painting+Contractors',
  'hvac': 'Heating+Contractors',
  'heating': 'Heating+Contractors',
  'electrical': 'Electricians',
  'electrician': 'Electricians',
  'general contractor': 'General+Contractors',
  'contractor': 'General+Contractors',
  'landscaping': 'Landscaping+Contractors',
  'landscaper': 'Landscaping+Contractors',
  'paving': 'Paving+Contractors',
  'concrete': 'Concrete+Contractors',
  'flooring': 'Flooring+Materials-Retail',
  'fencing': 'Fence-Retailers',
  'garage door': 'Garage+Doors',
  'windows': 'Windows',
  'siding': 'Siding+Contractors',
  'tree service': 'Tree+Service',
  'pest control': 'Pest+Control+Services',
  'moving': 'Moving+Services',
  // Professional services
  'lawyer': 'Lawyers',
  'accountant': 'Accountants',
  'real estate': 'Real+Estate+Agents',
  'insurance': 'Insurance',
  'financial advisor': 'Financial+Planning+Consultants',
  'architect': 'Architects',
};

// Major Canadian cities formatted as City+Province
const DEFAULT_CITIES = [
  'Toronto+ON',
  'Montreal+QC',
  'Vancouver+BC',
  'Calgary+AB',
  'Ottawa+ON',
  'Edmonton+AB',
  'Winnipeg+MB',
  'Halifax+NS',
  'Hamilton+ON',
  'Mississauga+ON',
];

const PAGE_SIZE = 35; // YP.ca shows 35 listings per page
const BASE_URL = 'https://www.yellowpages.ca';


class YellowPagesCaScraper extends BaseScraper {
  constructor() {
    super({
      name: 'yellowpages_ca',
      stateCode: 'YP-CA',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: PRACTICE_AREA_CODES,
      defaultCities: DEFAULT_CITIES,
    });
  }

  // search() is fully overridden — these are not used
  buildSearchUrl() { throw new Error('YP-CA: buildSearchUrl() not used — search() overridden'); }
  parseResultsPage() { throw new Error('YP-CA: parseResultsPage() not used — search() overridden'); }
  extractResultCount() { throw new Error('YP-CA: extractResultCount() not used — search() overridden'); }

  /**
   * Override transformResult to set source field.
   */
  transformResult(lead, practiceArea) {
    lead.source = 'yellowpages_ca';
    if (practiceArea && !lead.practice_area) {
      lead.practice_area = practiceArea;
    }
    return lead;
  }

  /**
   * Extract the real website URL from a YP redirect wrapper.
   * YP wraps all outgoing website links as: /gourl/HASH?redirect=ENCODED_URL
   *
   * @param {string} href - The /gourl/... link
   * @returns {string} The decoded real URL, or empty string
   */
  _extractRealWebsite(href) {
    if (!href) return '';
    try {
      // Handle both relative and absolute URLs
      const fullUrl = href.startsWith('/') ? `${BASE_URL}${href}` : href;
      const url = new URL(fullUrl);
      const redirect = url.searchParams.get('redirect');
      if (redirect) {
        // Filter out social media links
        if (this.isExcludedDomain(redirect)) return '';
        return redirect;
      }
    } catch (e) {
      // Not a valid URL
    }
    return '';
  }

  /**
   * Parse a single listing element into a lead object.
   *
   * @param {CheerioElement} el - The .listing element
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {object|null} Lead object or null if invalid
   */
  _parseListing($, el) {
    const listing = $(el);

    // Business name
    const nameLink = listing.find('.listing__name--link');
    const firmName = nameLink.text().trim();
    if (!firmName) return null;

    // Address components (schema.org microdata)
    const street = listing.find('[itemprop="streetAddress"]').first().text().trim();
    const city = listing.find('[itemprop="addressLocality"]').first().text().trim();
    const province = listing.find('[itemprop="addressRegion"]').first().text().trim();
    const postalCode = listing.find('[itemprop="postalCode"]').first().text().trim();

    // Phone — from data-phone attribute or text content
    const phoneEl = listing.find('.mlr__item--phone');
    let phone = phoneEl.find('[data-phone]').attr('data-phone') || '';
    if (!phone) {
      // Extract from text: "Phone Number 416-xxx-xxxx"
      const phoneText = phoneEl.text().replace(/Phone\s*Number/gi, '').trim();
      const phoneMatch = phoneText.match(/[\d()-]{7,}/);
      if (phoneMatch) phone = phoneMatch[0];
    }

    // Website — extract real URL from redirect wrapper
    const websiteEl = listing.find('.mlr__item--website');
    const websiteHref = websiteEl.find('a.mlr__item__cta').attr('href') ||
                        websiteEl.find('a').attr('href') || '';
    const website = this._extractRealWebsite(websiteHref);

    // Email — YP.ca does not show emails, but check anyway for future changes
    let email = '';
    const mailtoLink = listing.find('a[href^="mailto:"]');
    if (mailtoLink.length) {
      email = mailtoLink.attr('href').replace('mailto:', '').split('?')[0].trim();
    }

    // Profile URL
    let profileUrl = nameLink.attr('href') || '';
    if (profileUrl && profileUrl.startsWith('/')) {
      profileUrl = `${BASE_URL}${profileUrl}`;
    }
    // Strip query params from profile URL for cleaner dedup
    if (profileUrl.includes('?')) {
      profileUrl = profileUrl.split('?')[0];
    }

    // Categories / headings
    const headings = listing.find('.listing__headings a').map((i, a) => $(a).text().trim()).get();

    // Listing ID from data-analytics
    const merchantUrl = listing.find('.jsGoToMp').attr('data-merchanturl') || '';
    const listingIdMatch = merchantUrl.match(/\/(\d+)\.html/);
    const listingId = listingIdMatch ? listingIdMatch[1] : '';

    return {
      first_name: '',
      last_name: '',
      firm_name: firmName,
      address: street,
      city: city,
      state: province,
      zip: postalCode,
      phone: phone,
      email: email,
      website: website,
      bar_number: listingId ? `YP-${listingId}` : '',
      bar_status: '',
      profile_url: profileUrl,
      source: 'yellowpages_ca',
      practice_area: headings.join(', '),
      yp_listing_id: listingId,
    };
  }

  /**
   * Extract total result count from the search page.
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {number} Total result count
   */
  _extractTotalResults($) {
    // Pattern: ".resultCount" contains "(3855 Result(s))"
    const countText = $('.resultCount').text();
    const match = countText.match(/\(?([\d,]+)\s+Result/i);
    if (match) {
      return parseInt(match[1].replace(/,/g, ''), 10);
    }
    return 0;
  }

  /**
   * Check if there's a next page link.
   *
   * @param {CheerioStatic} $ - Cheerio instance
   * @returns {boolean} True if next page exists
   */
  _hasNextPage($) {
    // The "Next" button has class .pageButton and text containing "Next"
    const nextBtn = $('a.pageButton').filter((i, el) => {
      return $(el).text().includes('Next');
    });
    return nextBtn.length > 0;
  }

  /**
   * Main search generator.
   *
   * Iterates through cities and categories, paginating through results.
   * Parses HTML with Cheerio and yields normalized leads.
   *
   * @param {string} practiceArea - Category name (e.g., 'dentist', 'roofing')
   * @param {object} options - Search options
   * @param {string} options.city - Specific city to search (e.g., 'Toronto+ON')
   * @param {number} options.maxPages - Limit pages per city
   * @param {number} options.maxCities - Limit number of cities
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();

    // Resolve category
    const practiceKey = (practiceArea || 'dentist').toLowerCase().trim();
    const category = PRACTICE_AREA_CODES[practiceKey] || practiceArea || 'Dentists';

    log.scrape(`[YP-CA] Starting search: category=${category}, practiceArea=${practiceArea || 'dentist'}`);

    // Determine cities
    let citiesToSearch = [...DEFAULT_CITIES];
    if (options.city) {
      citiesToSearch = [options.city];
    }
    if (options.maxCities) {
      citiesToSearch = citiesToSearch.slice(0, options.maxCities);
    }

    const maxPagesPerCity = options.maxPages || 10; // Default: 10 pages per city (350 results)
    const seenIds = new Set();
    let totalYielded = 0;

    for (let ci = 0; ci < citiesToSearch.length; ci++) {
      const cityCode = citiesToSearch[ci];

      yield { _cityProgress: { current: ci + 1, total: citiesToSearch.length } };
      log.scrape(`[YP-CA] Searching "${category}" in ${cityCode}`);

      let page = 1;
      let totalResults = 0;
      let cityYielded = 0;
      let consecutiveEmpty = 0;

      while (page <= maxPagesPerCity) {
        const url = `${BASE_URL}/search/si/${page}/${encodeURIComponent(category)}/${cityCode}`;
        log.info(`[YP-CA] Page ${page} — ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`[YP-CA] Request failed: ${err.message}`);
          break;
        }

        // Handle rate limiting / blocks
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`[YP-CA] Got ${response.statusCode} — backing off`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`[YP-CA] Unexpected status ${response.statusCode} for ${cityCode} page ${page}`);
          break;
        }

        // Check for Cloudflare / CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`[YP-CA] CAPTCHA/challenge detected for ${cityCode}`);
          yield { _captcha: true, city: cityCode, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total result count on first page
        if (page === 1) {
          totalResults = this._extractTotalResults($);
          if (totalResults === 0) {
            log.info(`[YP-CA] No results for "${category}" in ${cityCode}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / PAGE_SIZE);
          log.success(`[YP-CA] ${totalResults.toLocaleString()} results (${totalPages} pages) for "${category}" in ${cityCode}`);
        }

        // Parse all listings on this page — collect into array (can't yield inside each())
        const listings = $('.listing');
        const pageLeads = [];

        listings.each((i, el) => {
          const lead = this._parseListing($, el);
          if (!lead || !lead.firm_name) return;

          // Dedup by listing ID or firm_name+city
          const dedupKey = lead.yp_listing_id || `${lead.firm_name}|${lead.city}`;
          if (seenIds.has(dedupKey)) return;
          seenIds.add(dedupKey);

          pageLeads.push(lead);
        });

        // Yield collected leads
        for (const lead of pageLeads) {
          yield this.transformResult(lead, practiceArea || category);
          cityYielded++;
          totalYielded++;
        }

        if (pageLeads.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 2) {
            log.warn(`[YP-CA] 2 consecutive empty pages — stopping for ${cityCode}`);
            break;
          }
        } else {
          consecutiveEmpty = 0;
        }

        // Check if there's a next page
        if (!this._hasNextPage($)) {
          log.info(`[YP-CA] No next page — finished ${cityCode} at page ${page}`);
          break;
        }

        // Check if we've exceeded total results
        if (page * PAGE_SIZE >= totalResults) {
          log.info(`[YP-CA] Reached end of results for ${cityCode}`);
          break;
        }

        page++;
      }

      log.info(`[YP-CA] ${cityCode}: ${cityYielded} leads`);

      // Polite delay between cities (3-5 seconds)
      if (ci < citiesToSearch.length - 1) {
        await new Promise(r => setTimeout(r, 3000 + Math.random() * 2000));
      }
    }

    log.success(`[YP-CA] Finished — ${totalYielded} total leads from ${citiesToSearch.length} cities`);
  }
}

module.exports = new YellowPagesCaScraper();
