/**
 * FMB (Federation of Master Builders) UK — Vetted builder directory
 *
 * Source: https://www.fmb.org.uk/find-a-builder.html
 * Platform: Server-rendered HTML with AJAX loadMore endpoint
 *
 * Search endpoint: GET /page-types/find_builder_search/loadMore/
 *   ?postcode=XX&page=N&tradename=
 *   Returns 10 HTML <article> cards per page with pagination nav
 *
 * Profile pages: GET /builder/{slug}.html
 *   Contains website link, city ("Based in X"), social links, trade categories
 *
 * ~6,200 vetted builders across the UK.
 * Listing page has: firm_name, phone, slug
 * Profile page adds: website, city, trades
 */

const cheerio = require('cheerio');
const BaseScraper = require('../base-scraper');
const { log } = require('../../lib/logger');
const { RateLimiter } = require('../../lib/rate-limiter');

const BASE_URL = 'https://www.fmb.org.uk';
const LOAD_MORE_PATH = '/page-types/find_builder_search/loadMore/';
const PAGE_SIZE = 10; // loadMore returns 10 per page

// Major UK postcodes spanning different regions to maximise coverage.
// FMB search is radius-based from each postcode, so we pick spread-out anchors.
const UK_POSTCODES = [
  // England
  'SW1A 1AA', // London (Central)
  'E1 6AN',   // London (East)
  'M1 1AE',   // Manchester
  'B1 1BB',   // Birmingham
  'LS1 1UR',  // Leeds
  'L1 8JQ',   // Liverpool
  'BS1 1HT',  // Bristol
  'S1 2BJ',   // Sheffield
  'NE1 4ST',  // Newcastle
  'NG1 5FW',  // Nottingham
  'CB2 1TN',  // Cambridge
  'OX1 1BX',  // Oxford
  'BN1 1AE',  // Brighton
  'EX1 1QA',  // Exeter
  'BA1 1SU',  // Bath
  'CT1 2EH',  // Canterbury
  'YO1 7HH',  // York
  'PL1 1EA',  // Plymouth
  'NR1 3JU',  // Norwich
  'SO14 7DU', // Southampton
  'CV1 1FY',  // Coventry
  'LE1 5PH',  // Leicester
  // Scotland
  'EH1 1RE',  // Edinburgh
  'G1 1DU',   // Glasgow
  'AB10 1AQ', // Aberdeen
  'DD1 1DA',  // Dundee
  'IV1 1HR',  // Inverness
  // Wales
  'CF10 1EP', // Cardiff
  'SA1 1DP',  // Swansea
  'LL57 1DQ', // Bangor
  // Northern Ireland
  'BT1 5GS',  // Belfast
];

class FMBScraper extends BaseScraper {
  constructor() {
    super({
      name: 'fmb_builders',
      stateCode: 'FMB',
      baseUrl: BASE_URL,
      pageSize: PAGE_SIZE,
      practiceAreaCodes: {
        'builder': 'Builder',
        'extensions': 'Extensions+and+conversions',
        'loft conversions': 'Loft+conversions',
        'kitchens': 'Kitchens',
        'bathrooms': 'Bathrooms',
        'roofing': 'Roofing',
        'plastering': 'Plastering',
        'carpentry': 'Carpentry',
        'brickwork': 'Brickwork',
        'landscaping': 'Landscaping',
        'painting': 'Painting+and+decorating',
        'damp proofing': 'Damp+proofing',
        'electrical': 'Electrical',
        'plumbing': 'Plumbing',
        'drainage': 'Drainage',
        'flooring': 'Flooring',
        'fencing': 'Fencing',
        'guttering': 'Guttering',
        'insulation': 'Insulation',
        'windows': 'Windows',
        'conservatories': 'Conservatories',
        'demolition': 'Demolition',
        'disabled adaptation': 'Disabled%2Felderly+home+adaptation',
        'garages': 'Garages',
        'new builds': 'New+build+homes',
        'swimming pools': 'Swimming+pools',
      },
      defaultCities: UK_POSTCODES,
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
   * Decode HTML entity-encoded characters like &#x3a; &#x2f; etc.
   */
  _decodeHtmlEntities(str) {
    if (!str) return '';
    return str
      .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
      .replace(/&#(\d+);/g, (_, dec) => String.fromCharCode(parseInt(dec, 10)))
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"');
  }

  /**
   * Parse the loadMore HTML response into an array of builder stubs.
   * Each stub has: firm_name, phone, slug, profile_url
   */
  _parseListingPage(html) {
    const $ = cheerio.load(html);
    const results = [];

    $('article.article-list-item').each((_, el) => {
      const $el = $(el);

      // Company name + profile link
      const $titleLink = $el.find('a.article-title-link');
      const firmName = ($titleLink.text() || '').trim();
      const rawHref = $titleLink.attr('href') || '';
      // Extract slug from href like /builder/some-builder-name.html?...
      const slugMatch = rawHref.match(/\/builder\/([^.?]+)/);
      const slug = slugMatch ? slugMatch[1] : '';

      // Phone number from tel: link
      const $phoneLink = $el.find('a[data-fab-track="phone-number"]');
      let phone = '';
      if ($phoneLink.length) {
        const telHref = this._decodeHtmlEntities($phoneLink.attr('href') || '');
        phone = telHref.replace(/^tel:/, '').trim();
        if (!phone) {
          phone = $phoneLink.text().trim();
        }
      }

      // Description
      const description = $el.find('.article-teaser p').text().trim();

      if (firmName && slug) {
        results.push({
          firm_name: firmName,
          phone,
          slug,
          profile_url: `${BASE_URL}/builder/${slug}.html`,
          about: description,
        });
      }
    });

    return results;
  }

  /**
   * Extract total result count from the loadMore pagination.
   * The last pagination link data-page attribute gives total pages.
   */
  _extractTotalPages(html) {
    const $ = cheerio.load(html);
    let maxPage = 1;
    $('a.search-result-pagination-link[data-page]').each((_, el) => {
      const p = parseInt($(el).attr('data-page'), 10);
      if (p > maxPage) maxPage = p;
    });
    return maxPage;
  }

  /**
   * Fast HTTP GET with 10-second timeout (shorter than base 15s).
   */
  _httpGetFast(url, rateLimiter) {
    const https = require('https');
    const http = require('http');
    return new Promise((resolve, reject) => {
      const ua = rateLimiter.getUserAgent();
      const options = {
        headers: {
          'User-Agent': ua,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Accept-Encoding': 'identity',
          'Connection': 'keep-alive',
        },
        timeout: 10000,
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
          return resolve(this._httpGetFast(redirect, rateLimiter));
        }
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Profile request timed out (10s)')); });
    });
  }

  /**
   * Fetch a builder profile page and extract website, city, and trades.
   */
  async _fetchProfileDetails(slug, rateLimiter) {
    const url = `${BASE_URL}/builder/${slug}.html`;
    try {
      const response = await this._httpGetFast(url, rateLimiter);
      if (response.statusCode !== 200) {
        log.warn(`Profile ${slug} returned ${response.statusCode}`);
        return {};
      }

      const $ = cheerio.load(response.body);
      const details = {};

      // Website: <a data-fab-track="website" href="...">Visit website</a>
      const $websiteLink = $('a[data-fab-track="website"]').first();
      if ($websiteLink.length) {
        const rawWebsite = this._decodeHtmlEntities($websiteLink.attr('href') || '');
        if (rawWebsite && !this.isExcludedDomain(rawWebsite)) {
          details.website = rawWebsite.startsWith('http') ? rawWebsite : `https://${rawWebsite}`;
        }
      }

      // City: <p class="fab-detail-profile-text"><span class="fmb-icons ico-fmb-pin"></span>Based in London</p>
      const $locationText = $('p.fab-detail-profile-text').first();
      if ($locationText.length) {
        const locText = $locationText.text().trim();
        const cityMatch = locText.match(/Based in\s+(.+)/i);
        if (cityMatch) {
          details.city = cityMatch[1].trim();
        }
      }

      // Postcode from og:description: "... in or around NW4 4DJ"
      const ogDesc = $('meta[property="og:description"]').attr('content') || '';
      const pcMatch = ogDesc.match(/around\s+([A-Z]{1,2}\d[\dA-Z]?\s*\d[A-Z]{2})/i);
      if (pcMatch) {
        details.zip = pcMatch[1].trim();
      }

      // Trades from og:description: "offering Bathrooms,Brickwork,Builder,..."
      const tradesMatch = ogDesc.match(/offering\s+(.+?)\s+in\s+or\s+around/i);
      if (tradesMatch) {
        details.all_trades = tradesMatch[1].trim();
      }

      return details;
    } catch (err) {
      log.warn(`Failed to fetch profile for ${slug}: ${err.message}`);
      return {};
    }
  }

  /**
   * Format a UK phone number.
   */
  _formatPhone(phone) {
    if (!phone) return '';
    const trimmed = phone.trim();
    const digits = trimmed.replace(/[^\d+]/g, '');
    if (!digits) return '';
    if (digits.startsWith('+44')) {
      return '0' + digits.slice(3);
    }
    if (digits.startsWith('44') && digits.length > 10) {
      return '0' + digits.slice(2);
    }
    return trimmed;
  }

  /**
   * Simple 1-2s delay for profile fetches (FMB is not rate-limiting aggressive).
   */
  _quickDelay() {
    const ms = 1000 + Math.random() * 1500; // 1-2.5s
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Async generator — yields builder leads from FMB.
   *
   * Strategy:
   * 1. Search by postcode across major UK regions
   * 2. Paginate via loadMore endpoint (10 per page)
   * 3. Fetch profile pages for website + city (unless options.skipProfiles)
   * 4. Dedup by slug across postcodes
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter({ minDelay: 2000, maxDelay: 4000 });
    const maxPages = options.maxPages || Infinity;
    const skipProfiles = options.skipProfiles || false;
    const seenSlugs = new Set();

    // Resolve trade filter
    const practiceKey = (practiceArea || '').toLowerCase().trim();
    const tradeName = this.practiceAreaCodes[practiceKey] || '';

    // Determine postcodes to search
    const postcodes = options.city
      ? [options.city]
      : this.defaultCities;
    const postcodesToSearch = options.maxCities
      ? postcodes.slice(0, options.maxCities)
      : postcodes;

    log.scrape(`FMB search: trade=${practiceArea || 'all'}, postcodes=${postcodesToSearch.length}`);

    for (let pi = 0; pi < postcodesToSearch.length; pi++) {
      const postcode = postcodesToSearch[pi];
      yield { _cityProgress: { current: pi + 1, total: postcodesToSearch.length } };
      log.scrape(`Searching FMB builders near ${postcode}${practiceArea ? ` (${practiceArea})` : ''}`);

      let page = 1;
      let pagesFetched = 0;
      let totalPages = null;
      let postcodeYielded = 0;

      while (true) {
        if (pagesFetched >= maxPages) {
          log.info(`Reached max pages limit (${maxPages}) for ${postcode}`);
          break;
        }

        const url = `${BASE_URL}${LOAD_MORE_PATH}?postcode=${encodeURIComponent(postcode)}&page=${page}&tradename=${tradeName}`;
        log.info(`Page ${page} — ${postcode}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`Request failed for ${postcode} page ${page}: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`Got ${response.statusCode} from FMB`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`FMB returned ${response.statusCode} for ${postcode} page ${page}`);
          break;
        }

        rateLimiter.resetBackoff();

        // Parse results
        const stubs = this._parseListingPage(response.body);

        // Get total pages on first page
        if (page === 1) {
          totalPages = this._extractTotalPages(response.body);
          if (stubs.length === 0) {
            log.info(`No results for ${postcode}`);
            break;
          }
          log.success(`Found results near ${postcode} (~${totalPages} pages)`);
        }

        if (stubs.length === 0) {
          log.info(`No more results for ${postcode} at page ${page}`);
          break;
        }

        // Process each builder
        for (const stub of stubs) {
          // Dedup by slug across postcodes
          if (seenSlugs.has(stub.slug)) continue;
          seenSlugs.add(stub.slug);

          // Fetch profile page for website + city (unless skipped)
          let profileDetails = {};
          if (!skipProfiles) {
            await this._quickDelay();
            profileDetails = await this._fetchProfileDetails(stub.slug, rateLimiter);
          }

          const lead = {
            first_name: '',
            last_name: '',
            firm_name: stub.firm_name,
            city: profileDetails.city || '',
            state: 'UK',
            zip: profileDetails.zip || '',
            phone: this._formatPhone(stub.phone),
            website: profileDetails.website || '',
            email: '',
            bar_number: '',
            bar_status: 'FMB Vetted',
            source: 'fmb_builders',
            practice_area: practiceArea || profileDetails.all_trades || '',
            about: stub.about || '',
            profile_url: stub.profile_url,
            all_trades: profileDetails.all_trades || '',
          };

          // Skip entries without a firm name
          if (!lead.firm_name) continue;

          yield this.transformResult(lead, practiceArea);
          postcodeYielded++;
        }

        log.info(`Page ${page}: ${stubs.length} cards, ${postcodeYielded} new leads from ${postcode}`);

        // Check if we've reached the last page
        if (totalPages && page >= totalPages) {
          log.success(`Completed all ${totalPages} pages for ${postcode}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }

    log.success(`FMB scrape complete — ${seenSlugs.size} unique builders found`);
  }
}

module.exports = new FMBScraper();
