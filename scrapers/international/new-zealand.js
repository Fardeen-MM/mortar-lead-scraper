/**
 * New Zealand Law Society Scraper
 *
 * Source: https://www.lawsociety.org.nz/for-the-public/find-a-lawyer/
 * Method: HTTP GET + Cheerio (server-rendered HTML with query-param pagination)
 *
 * The NZ Law Society directory is a SilverStripe CMS site. The search accepts
 * query parameters: Regions, PracticeAreas, Keyword, page, pageSize (fixed at 10).
 * Each result links to a profile page at /register/<slug>/?glh=1 which contains
 * detailed contact information (email, phone, firm, address, admission date).
 *
 * Strategy:
 *   1. Fetch paginated listing pages to collect lawyer names + profile URLs
 *   2. Fetch each profile page to extract full contact details
 *   3. Yield standardised attorney records
 *
 * NZ uses "Regions" not cities. The defaultCities map to NZ regions:
 *   Auckland -> Auckland, Wellington -> Wellington, Christchurch -> Canterbury,
 *   Hamilton -> Waikato, Tauranga -> Bay of Plenty, Dunedin -> Otago
 */

const BaseScraper = require('../base-scraper');
const cheerio = require('cheerio');
const { log } = require('../../lib/logger');
const { RateLimiter, sleep } = require('../../lib/rate-limiter');

// Map user-friendly city names to NZ Law Society region values
const CITY_TO_REGION = {
  'Auckland':     'Auckland',
  'Wellington':   'Wellington',
  'Christchurch': 'Canterbury',
  'Hamilton':     'Waikato',
  'Tauranga':     'Bay of Plenty',
  'Dunedin':      'Otago',
  'Nelson':       'Nelson',
  'Napier':       'Hawke\'s Bay',
  'Hastings':     'Hawke\'s Bay',
  'Palmerston North': 'Manawat\u016b-Whanganui',
  'Invercargill': 'Southland',
  'New Plymouth':  'Taranaki',
  'Rotorua':      'Bay of Plenty',
  'Whangarei':    'Northland',
  'Queenstown':   'Otago',
  'Blenheim':     'Marlborough',
};

class NewZealandScraper extends BaseScraper {
  constructor() {
    super({
      name: 'new-zealand',
      stateCode: 'NZ',
      baseUrl: 'https://www.lawsociety.org.nz/for-the-public/find-a-lawyer/',
      pageSize: 10, // Server always returns 10 results per page
      practiceAreaCodes: {
        'acc':                  'AC',
        'administrative':       'AP',
        'public':               'AP',
        'arbitration':          'AR',
        'banking':              'BF',
        'bank/finance':         'BF',
        'finance':              'BF',
        'civil litigation':     'CL',
        'litigation':           'CL',
        'company/commercial':   'CC',
        'commercial':           'CC',
        'corporate':            'CC',
        'coronial':             'coronialLaw',
        'criminal':             'CR',
        'criminal law':         'CR',
        'employment':           'EM',
        'family':               'FM',
        'family law':           'FM',
        'health':               'HE',
        'immigration':          'IM',
        'in-house':             'IH',
        'insurance':            'IN',
        'intellectual property': 'IP',
        'lending':              'LA',
        'media':                'mediaLaw',
        'mediation':            'MD',
        'privacy':              'PL',
        'property':             'PR',
        'resource management':  'RM',
        'real estate':          'SR',
        'tax':                  'TX',
        'treaty':               'TM',
        'trusts':               'TE',
        'trusts and estates':   'TE',
        'estates':              'TE',
      },
      defaultCities: ['Auckland', 'Wellington', 'Christchurch', 'Hamilton', 'Tauranga', 'Dunedin'],
    });
  }

  /**
   * Resolve a city name to the NZ region parameter value.
   */
  cityToRegion(city) {
    if (CITY_TO_REGION[city]) return CITY_TO_REGION[city];
    // If user passes a region name directly, pass it through
    const regionValues = Object.values(CITY_TO_REGION);
    if (regionValues.includes(city)) return city;
    // Fallback: use as-is (the server may still match)
    return city;
  }

  /**
   * Build the search URL for a listing page.
   * The NZ site uses: Regions, PracticeAreas, page, pageSize as query params.
   */
  buildSearchUrl({ city, practiceCode, page }) {
    const params = new URLSearchParams();
    if (city) {
      params.set('Regions', this.cityToRegion(city));
    }
    if (practiceCode) {
      params.set('PracticeAreas', practiceCode);
    }
    params.set('page', String(page || 1));
    params.set('pageSize', '10');
    return `${this.baseUrl}?${params.toString()}`;
  }

  /**
   * Parse the listing page to extract lawyer names, profile URLs,
   * firms, locations, and practice areas.
   */
  parseResultsPage($) {
    const attorneys = [];

    $('li.c-lawyer-list-glh__item').each((_, el) => {
      const $el = $(el);

      // Name + profile URL
      const nameLink = $el.find('a.c-lawyer-list-glh__link');
      const fullName = nameLink.text().trim();
      const profilePath = nameLink.attr('href') || '';
      const profileUrl = profilePath
        ? `https://www.lawsociety.org.nz${profilePath}`
        : '';

      // Firm / practice name
      const firmName = $el.find('.c-lawyer-list-glh__practice-name').text().trim();

      // Location (e.g., "Auckland, North Shore")
      const location = $el.find('.c-lawyer-list-glh__location-name').text().trim();
      const locationParts = location.split(',').map(s => s.trim());
      const region = locationParts[0] || '';
      const district = locationParts[1] || '';

      // Practice areas
      const areas = [];
      $el.find('.c-lawyer-list-glh__law-areas-list-item').each((_, li) => {
        const text = $(li).text().trim();
        if (text && !text.startsWith('+')) areas.push(text);
      });

      const { firstName, lastName } = this.splitName(fullName);

      attorneys.push({
        first_name: firstName,
        last_name: lastName,
        full_name: fullName,
        firm_name: firmName,
        city: region,
        state: 'NZ',
        district,
        phone: '',
        email: '',
        website: '',
        bar_number: '',
        bar_status: '',
        practice_areas_list: areas.join('; '),
        profile_url: profileUrl,
      });
    });

    return attorneys;
  }

  /**
   * Extract total result count from the data-total attribute.
   */
  extractResultCount($) {
    const total = $('[data-total]').first().attr('data-total');
    if (total) return parseInt(total, 10);
    return 0;
  }

  /**
   * Fetch a single lawyer's profile page and extract detailed contact info.
   */
  async fetchProfileDetails(profileUrl, rateLimiter) {
    try {
      // Use shorter delay for profile pages (1-2s) vs full 5-10s for search pages
      await sleep(1000 + Math.random() * 1000);
      const response = await this.httpGet(profileUrl, rateLimiter);

      if (response.statusCode !== 200) {
        log.warn(`NZ: Profile fetch returned ${response.statusCode} for ${profileUrl}`);
        return {};
      }

      if (this.detectCaptcha(response.body)) {
        log.warn(`NZ: CAPTCHA on profile page ${profileUrl}`);
        return {};
      }

      const $ = cheerio.load(response.body);
      const details = {};

      // Extract all dt/dd pairs
      $('dt').each((_, dtEl) => {
        const $dt = $(dtEl);
        const $dd = $dt.next('dd');
        if (!$dd.length) return;

        const label = $dt.text().replace(/\s+/g, ' ').trim().toLowerCase();
        const ddHtml = $dd.html() || '';
        const ddText = $dd.text().replace(/\s+/g, ' ').trim();

        if (label.includes('email')) {
          const mailMatch = ddHtml.match(/mailto:([^"]+)/);
          if (mailMatch) details.email = mailMatch[1].trim();
        } else if (label.includes('telephone') || label.includes('phone')) {
          const telMatch = ddHtml.match(/tel:([^"]+)/);
          if (telMatch) details.phone = telMatch[1].trim();
        } else if (label.includes('workplace') || label.includes('firm')) {
          details.workplace = ddText;
        } else if (label.includes('preferred name')) {
          details.preferred_name = ddText;
        } else if (label.includes('regulatory') || label.includes('practising')) {
          details.regulatory = ddText;
          // Extract certificate type (Barrister, Barrister & Solicitor, etc.)
          const certMatch = ddText.match(/as a (Barrister(?: & Solicitor)?)/i);
          if (certMatch) details.bar_status = certMatch[1];
          // Check if current
          if (ddText.toLowerCase().includes('currently holds')) {
            details.bar_status = (details.bar_status || '') + ' (Current)';
          }
        } else if (label.includes('admitted')) {
          details.admission_date = ddText;
        } else if (label.includes('post') || label.includes('address')) {
          details.address = ddText;
        } else if (label.includes('website') || label.includes('web')) {
          const urlMatch = ddHtml.match(/href="(https?:\/\/[^"]+)"/);
          if (urlMatch) details.website = urlMatch[1];
        }
      });

      // Parse workplace to extract firm name
      if (details.workplace) {
        // Patterns: "Sole Practitioner at <firm>", "Partner at <firm>", "<role> at <firm>"
        const atMatch = details.workplace.match(/(?:at|with)\s+(.+)/i);
        if (atMatch) {
          details.firm_name = atMatch[1].trim();
        } else {
          details.firm_name = details.workplace;
        }
      }

      return details;
    } catch (err) {
      log.error(`NZ: Error fetching profile ${profileUrl}: ${err.message}`);
      return {};
    }
  }

  /**
   * Override the main search generator.
   * The NZ scraper uses a two-phase approach:
   *   Phase 1: Paginate through listing pages to get lawyer summaries
   *   Phase 2: Optionally fetch profile pages for detailed contact info
   *
   * By default, it fetches profile details. Pass options.skipProfiles = true
   * to only get listing-level data (faster but less complete).
   */
  async *search(practiceArea, options = {}) {
    const rateLimiter = new RateLimiter();
    const practiceCode = this.resolvePracticeCode(practiceArea);

    if (!practiceCode && practiceArea) {
      log.warn(`NZ: Unknown practice area "${practiceArea}" -- searching without filter`);
      log.info(`NZ: Available areas: ${Object.keys(this.practiceAreaCodes).join(', ')}`);
    }

    const cities = this.getCities(options);
    const skipProfiles = options.skipProfiles || false;

    for (let ci = 0; ci < cities.length; ci++) {
      const city = cities[ci];
      const region = this.cityToRegion(city);
      yield { _cityProgress: { current: ci + 1, total: cities.length } };
      log.scrape(`NZ: Searching ${practiceArea || 'all'} lawyers in ${city} (region: ${region})`);

      let page = 1;
      let totalResults = 0;
      let pagesFetched = 0;
      let consecutiveEmpty = 0;
      let profileFetchCount = 0;
      const maxProfileFetches = options.maxPages ? 3 : Infinity; // Limit in test mode

      while (true) {
        // Check max pages limit
        if (options.maxPages && pagesFetched >= options.maxPages) {
          log.info(`NZ: Reached max pages limit (${options.maxPages}) for ${city}`);
          break;
        }

        const url = this.buildSearchUrl({ city, practiceCode, page });
        log.info(`NZ: Page ${page} -- ${url}`);

        let response;
        try {
          await rateLimiter.wait();
          response = await this.httpGet(url, rateLimiter);
        } catch (err) {
          log.error(`NZ: Request failed: ${err.message}`);
          const shouldRetry = await rateLimiter.handleBlock(0);
          if (shouldRetry) continue;
          break;
        }

        // Handle rate limiting
        if (response.statusCode === 429 || response.statusCode === 403) {
          log.warn(`NZ: Got ${response.statusCode}`);
          const shouldRetry = await rateLimiter.handleBlock(response.statusCode);
          if (shouldRetry) continue;
          break;
        }

        if (response.statusCode !== 200) {
          log.error(`NZ: Unexpected status ${response.statusCode} -- skipping`);
          break;
        }

        rateLimiter.resetBackoff();

        // Check for CAPTCHA
        if (this.detectCaptcha(response.body)) {
          log.warn(`NZ: CAPTCHA detected on page ${page} for ${city}`);
          yield { _captcha: true, city, page };
          break;
        }

        const $ = cheerio.load(response.body);

        // Get total count on first page
        if (page === 1) {
          totalResults = this.extractResultCount($);
          if (totalResults === 0) {
            log.info(`NZ: No results for ${practiceArea || 'all'} in ${city}`);
            break;
          }
          const totalPages = Math.ceil(totalResults / this.pageSize);
          log.success(`NZ: Found ${totalResults.toLocaleString()} results (${totalPages} pages) for ${city}`);
        }

        const attorneys = this.parseResultsPage($);

        if (attorneys.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= this.maxConsecutiveEmpty) {
            log.warn(`NZ: ${this.maxConsecutiveEmpty} consecutive empty pages -- stopping for ${city}`);
            break;
          }
          page++;
          pagesFetched++;
          continue;
        }

        consecutiveEmpty = 0;

        // Optionally enrich each attorney with profile details
        for (const attorney of attorneys) {
          if (!skipProfiles && attorney.profile_url && profileFetchCount < maxProfileFetches) {
            profileFetchCount++;
            log.info(`NZ: Fetching profile for ${attorney.full_name}`);
            const details = await this.fetchProfileDetails(attorney.profile_url, rateLimiter);

            if (details.email) attorney.email = details.email;
            if (details.phone) attorney.phone = details.phone;
            if (details.firm_name) attorney.firm_name = details.firm_name;
            if (details.website) attorney.website = details.website;
            if (details.bar_status) attorney.bar_status = details.bar_status;
            if (details.admission_date) attorney.admission_date = details.admission_date;
            if (details.address) attorney.address = details.address;
            if (details.preferred_name) attorney.preferred_name = details.preferred_name;
          }

          // Apply min year filter
          if (options.minYear && attorney.admission_date) {
            const yearMatch = attorney.admission_date.match(/\d{4}/);
            if (yearMatch) {
              const year = parseInt(yearMatch[0], 10);
              if (year > 0 && year < options.minYear) continue;
            }
          }

          yield this.transformResult(attorney, practiceArea);
        }

        // Check if we've reached the last page
        const totalPages = Math.ceil(totalResults / this.pageSize);
        if (page >= totalPages) {
          log.success(`NZ: Completed all ${totalPages} pages for ${city}`);
          break;
        }

        page++;
        pagesFetched++;
      }
    }
  }
}

module.exports = new NewZealandScraper();
